// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    public class ReplayTimestampTracker(int aofSublogCount)
    {
        const int Size = 1 << 15;
        readonly int aofSublogCount = aofSublogCount;
        static ReplayTimestampTracker()
        {
            if ((Size & (Size - 1)) != 0)
                throw new InvalidOperationException($"Size ({Size}) must be a power of 2");
        }

        SingleWriterMultiReaderLock updateTimestampLock;

        ConcurrentQueue<ReadSessionWaiter>[] waitQs = InitializeWaitQs(aofSublogCount);

        static ConcurrentQueue<ReadSessionWaiter>[] InitializeWaitQs(int aofSublogCount)
            => [.. Enumerable.Range(0, aofSublogCount).Select(_ => new ConcurrentQueue<ReadSessionWaiter>())];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static void Hash(ref SpanByte key, long aofSublogCount, out long hash, out long sublogIdx, out long keyOffset)
        {
            hash = (long)GarnetLog.Hash(ref key);
            Hash(aofSublogCount, hash, out sublogIdx, out keyOffset);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static void Hash(long aofSublogCount, long hash, out long sublogIdx, out long keyOffset)
        {
            sublogIdx = hash % aofSublogCount;
            keyOffset = hash & (Size - 1);
        }

        readonly long[][] timestamps = InitializeTimestamps(aofSublogCount, Size);

        private static long[][] InitializeTimestamps(int aofSublogCount, int size)
            => [.. Enumerable.Range(0, aofSublogCount).Select(_ =>
            {
                var array = GC.AllocateArray<long>(size, pinned: true);
                Array.Clear(array);
                return array;
            })];

        /// <summary>
        /// Update timestamp for all keys corresponding to sublog.
        /// NOTE: This is triggered by the RefreshSublogTail task when a sublog has stalled
        /// (i.e. failed to keep moving its timestamp forward) due to lack of new data being added.
        /// In that case we need to move the timestamp forward to avoid a deadlock in the consistent read protocol
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="timestamp"></param>
        /// <seealso cref="T:Garnet.cluster.AofSyncDriver.RefreshSublogTail"/>
        public void UpdateSublogTimestamp(int sublogIdx, long timestamp)
        {
            var array = timestamps[sublogIdx];
            const int length = Size;

            try
            {
                // Bulk update of sublog timestamps should happen rarely
                // Hence, we acquire a write-lock which has implicitly lower priority (compared to a read-lock)
                updateTimestampLock.WriteLock();
                if (Avx2.IsSupported)
                {
                    var timestampVector = Vector256.Create(timestamp);
                    for (var i = 0; i < length; i += 16)
                    {
                        unsafe
                        {
                            fixed (long* ptr = &array[i])
                            {
                                // Load 4 vectors (16 longs total)
                                var currentVector1 = Avx.LoadVector256(ptr);
                                var currentVector2 = Avx.LoadVector256(ptr + 4);
                                var currentVector3 = Avx.LoadVector256(ptr + 8);
                                var currentVector4 = Avx.LoadVector256(ptr + 12);

                                // Vectorized max operations on 4 vectors in parallel
                                var maxVector1 = Vector256.Max(timestampVector, currentVector1);
                                var maxVector2 = Vector256.Max(timestampVector, currentVector2);
                                var maxVector3 = Vector256.Max(timestampVector, currentVector3);
                                var maxVector4 = Vector256.Max(timestampVector, currentVector4);

                                // Store 4 vectors (16 longs total)
                                Avx.Store(ptr, maxVector1);
                                Avx.Store(ptr + 4, maxVector2);
                                Avx.Store(ptr + 8, maxVector3);
                                Avx.Store(ptr + 12, maxVector4);
                            }
                        }
                    }
                }
                else if (Sse2.IsSupported)
                {
                    var timestampVector = Vector128.Create(timestamp);
                    for (var i = 0; i < length; i += 8)
                    {
                        unsafe
                        {
                            fixed (long* ptr = &array[i])
                            {
                                // Load 4 vectors (8 longs total)
                                var currentVector1 = Sse2.LoadVector128(ptr);
                                var currentVector2 = Sse2.LoadVector128(ptr + 2);
                                var currentVector3 = Sse2.LoadVector128(ptr + 4);
                                var currentVector4 = Sse2.LoadVector128(ptr + 6);

                                // Vectorized max operations on 4 vectors in parallel
                                var maxVector1 = Vector128.Max(timestampVector, currentVector1);
                                var maxVector2 = Vector128.Max(timestampVector, currentVector2);
                                var maxVector3 = Vector128.Max(timestampVector, currentVector3);
                                var maxVector4 = Vector128.Max(timestampVector, currentVector4);

                                // Store 4 vectors (8 longs total)
                                Sse2.Store(ptr, maxVector1);
                                Sse2.Store(ptr + 2, maxVector2);
                                Sse2.Store(ptr + 4, maxVector3);
                                Sse2.Store(ptr + 6, maxVector4);
                            }
                        }
                    }
                }
                else
                {
                    for (var i = 0; i < length; i += 8)
                    {
                        array[i] = Math.Max(timestamp, array[i]);
                        array[i + 1] = Math.Max(timestamp, array[i + 1]);
                        array[i + 2] = Math.Max(timestamp, array[i + 2]);
                        array[i + 3] = Math.Max(timestamp, array[i + 3]);
                        array[i + 4] = Math.Max(timestamp, array[i + 4]);
                        array[i + 5] = Math.Max(timestamp, array[i + 5]);
                        array[i + 6] = Math.Max(timestamp, array[i + 6]);
                        array[i + 7] = Math.Max(timestamp, array[i + 7]);
                    }
                }
            }
            finally
            {
                updateTimestampLock.WriteUnlock();
            }

            SignalWaiters(sublogIdx);
        }

        /// <summary>
        /// Update key timestamp when replaying the AOF at replica and release any waiters.
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="key"></param>
        /// <param name="timestamp"></param>
        public void UpdateKeyTimestamp(int sublogIdx, ref SpanByte key, long timestamp)
        {
            try
            {
                // It is expected that updates to individual key timestamp is a frequent operation
                // Hence the acquisition of a read-lock that contents only with the bulk update of timestamps (see UpdateSublogTimestamp)
                updateTimestampLock.ReadLock();
                Hash(ref key, aofSublogCount, out _, out var _sublogIdx, out var keyOffset);
                Debug.Assert(sublogIdx == _sublogIdx);
                timestamps[sublogIdx][keyOffset] = Math.Max(timestamp, timestamps[sublogIdx][keyOffset]);
                // TODO: monotonic update is needed only if each sublog is replayed in parallel.
                // Utility.MonotonicUpdate(ref timestamps[sublogIdx][keyOffset], timestamp, out _);
            }
            finally
            {
                updateTimestampLock.ReadUnlock();
            }

            SignalWaiters(sublogIdx);
        }

        void SignalWaiters(int sublogIdx)
        {
            var waiterList = new List<ReadSessionWaiter>();
            while (waitQs[sublogIdx].TryDequeue(out var waiter))
            {
                Hash(aofSublogCount, waiter.hash, out _, out var keyOffset);

                // If we have replayed beyond waited timestamp release
                // the waiter otherwise put it in back-up list to re-insert later
                if (waiter.waitForTimestamp <= timestamps[sublogIdx][keyOffset])
                    waiter.eventSlim.Set();
                else
                    waiterList.Add(waiter);
            }

            // Re-insert any waiters that have not been released yet
            foreach (var waiter in waiterList)
                waitQs[sublogIdx].Enqueue(waiter);
        }

        /// <summary>
        /// Check timestamps or reads you are about to read
        /// NOTE: Reading and validating one key at a time is not transactional
        /// but it is ok since it is no worse than what we currently support (i.e. MGET)
        /// </summary>
        /// <param name="replicaReadContext"></param>
        /// <param name="parseState"></param>
        /// <param name="csvi"></param>
        public void WaitForConsistentRead(ref ReplicaReadContext replicaReadContext, ref SessionParseState parseState, ref ClusterSlotVerificationInput csvi, ReadSessionWaiter readSessionWaiter)
        {
            for (var i = csvi.firstKey; i < csvi.lastKey; i += csvi.step)
            {
                var key = parseState.GetArgSliceByRef(i).SpanByte;
                Hash(ref key, aofSublogCount, out var hash, out var sublogIdx, out var keyOffset);

                // If first read initialize context
                if (replicaReadContext.lastSublogIdx == -1)
                {
                    replicaReadContext.lastSublogIdx = sublogIdx;
                    replicaReadContext.maximumTimestamp = timestamps[sublogIdx][keyOffset];
                    continue;
                }

                // If reading from same sublog then don't need to check for freshness
                if (replicaReadContext.lastSublogIdx == sublogIdx)
                    continue;

                // Here we have to wait for replay to catch up
                if (replicaReadContext.maximumTimestamp > timestamps[sublogIdx][keyOffset])
                {
                    // Before adding to the waitQ set timestamp and hash of key we are waiting for
                    readSessionWaiter.waitForTimestamp = replicaReadContext.maximumTimestamp;
                    readSessionWaiter.hash = hash;

                    // Enqueue waiter and wait
                    waitQs[sublogIdx].Enqueue(readSessionWaiter);
                    readSessionWaiter.eventSlim.Wait();

                    // Reset waiter for next iteration
                    readSessionWaiter.eventSlim.Reset();
                }

                // If timestamp of current key is after maximum timestamp we can safely read the key
                Debug.Assert(replicaReadContext.maximumTimestamp <= timestamps[sublogIdx][keyOffset]);
                replicaReadContext.lastSublogIdx = sublogIdx;
                replicaReadContext.maximumTimestamp = timestamps[sublogIdx][keyOffset];
            }
        }
    }
}