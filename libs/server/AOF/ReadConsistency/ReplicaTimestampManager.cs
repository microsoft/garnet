// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// This implements the replica timestamp manager to track the timestamps of keys for all sessions in a given replica.
    /// The class maintains a 2D-array of size [SublogCount][KeyOffsetCount + 1] which tracks the timestamps for a set of keys per sublog
    /// Every data replay operation will update this map on per key basis using a hash function to determine the keyOffset.
    /// This is done to mitigate the read delay for the following scenario if we only maintain a maximum timestamp per sublog
    ///     time: ----(k1,t1)---(k2,t2)---(k3,t3)----
    ///     s1: (k1,t1) (k3,t3)
    ///     s2: (k2,t2)
    ///     read k1 (at t3 because s1 has replayed until k3)
    ///     read k2 (at t2 have to wait because reading k1 established that we are in t3 though no updates have arrived for k1 by t3)
    /// </summary>
    /// <param name="appendOnlyFile"></param>
    /// <param name="nextVersion"></param>
    public class ReplicaTimestampManager(long nextVersion, GarnetAppendOnlyFile appendOnlyFile)
    {
        public long CurrentVersion { get; private set; } = nextVersion;
        public const int KeyOffsetCount = (1 << 15) + 1;
        const int MaxSublogTimestampOffset = KeyOffsetCount - 1;
        readonly GarnetAppendOnlyFile appendOnlyFile = appendOnlyFile;
        static ReplicaTimestampManager()
        {
            var size = KeyOffsetCount - 1;
            if ((size & (size - 1)) != 0)
                throw new InvalidOperationException($"Size ({KeyOffsetCount - 1}) must be a power of 2");
        }

        ConcurrentQueue<ReadSessionWaiter>[] waitQs = InitializeWaitQs(appendOnlyFile.Log.Size);

        static ConcurrentQueue<ReadSessionWaiter>[] InitializeWaitQs(int aofSublogCount)
            => [.. Enumerable.Range(0, aofSublogCount).Select(_ => new ConcurrentQueue<ReadSessionWaiter>())];

        long[][] timestamps = InitializeTimestamps(appendOnlyFile.Log.Size, KeyOffsetCount);

        private static long[][] InitializeTimestamps(int aofSublogCount, int size)
            => [.. Enumerable.Range(0, aofSublogCount).Select(_ =>
            {
                var array = GC.AllocateArray<long>(size, pinned: true);
                Array.Clear(array);
                return array;
            })];

        /// <summary>
        /// Get timestamp frontier which is the maximum between the key timestamp and the sublog maximum timestamp
        /// </summary>
        /// <returns></returns>
        long GetKeyTimestampFrontier(int sublogIdx, int keyOffset) => Math.Max(timestamps[sublogIdx][keyOffset], timestamps[MaxSublogTimestampOffset][keyOffset]);

        /// <summary>
        /// Get timestamp for actual key
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="keyOffset"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long GetActualKeyTimestamp(int sublogIdx, int keyOffset) => timestamps[sublogIdx][keyOffset];

        /// <summary>
        /// Update timestamp for all keys corresponding to sublog.
        /// This is triggered by RefreshSublogTail, TxnReplay or CustomProcReplay.
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="timestamp"></param>
        /// <seealso cref="T:Garnet.cluster.AofSyncDriver.RefreshSublogTail"/>
        public void UpdateSublogTimestamp(int sublogIdx, long timestamp)
        {
            _ = Utility.MonotonicUpdate(ref timestamps[sublogIdx][MaxSublogTimestampOffset], timestamp, out _);
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
            appendOnlyFile.Log.HashKey(ref key, out _, out var _sublogIdx, out var keyOffset);
            if (sublogIdx != _sublogIdx)
                throw new GarnetException("Sublog index does not match key mapping!");
            _ = Utility.MonotonicUpdate(ref timestamps[sublogIdx][keyOffset], timestamp, out _);
            SignalWaiters(sublogIdx);
        }

        /// <summary>
        /// Update timestamp for provided sublogIdx and keyOffset
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="keyOffset"></param>
        /// <param name="timestamp"></param>
        public void UpdateKeyTimestamp(int sublogIdx, int keyOffset, long timestamp)
        {
            _ = Utility.MonotonicUpdate(ref timestamps[sublogIdx][keyOffset], timestamp, out _);
            SignalWaiters(sublogIdx);
        }

        /// <summary>
        /// Signal any readers waiting for timestamp to progress
        /// </summary>
        /// <param name="sublogIdx"></param>
        void SignalWaiters(int sublogIdx)
        {
            var waiterList = new List<ReadSessionWaiter>();
            while (waitQs[sublogIdx].TryDequeue(out var waiter))
            {
                // If timestamp has not progressed enough will re-add this waiter to the waitQ
                if (waiter.waitForTimestamp > GetKeyTimestampFrontier(waiter.sublogIdx, waiter.keyOffset))
                    waiterList.Add(waiter);
                else
                    // Signal for waiter to proceed
                    waiter.eventSlim.Set();
            }

            // Re-insert any waiters that have not been released yet
            foreach (var waiter in waiterList)
                waitQs[sublogIdx].Enqueue(waiter);
        }

        /// <summary>
        /// Ensure read consistency when using a sharded log
        /// NOTE: Reading and validating one key at a time is not transactional
        /// but it is ok since it is no worse than what we currently support (i.e. MGET)
        /// </summary>
        /// <param name="replicaReadSessionContext"></param>
        /// <param name="parseState"></param>
        /// <param name="csvi"></param>
        public void EnsureConsistentRead(ref ReplicaReadSessionContext replicaReadSessionContext, ref SessionParseState parseState, ref ClusterSlotVerificationInput csvi, ReadSessionWaiter readSessionWaiter)
        {
            // If first time calling or version has been bumped reset read context
            // NOTE: version changes every time replica is reset and a attached to a new primary
            if(replicaReadSessionContext.sessionVersion == -1 || replicaReadSessionContext.sessionVersion != CurrentVersion)
            {
                replicaReadSessionContext.sessionVersion = CurrentVersion;
                replicaReadSessionContext.lastSublogIdx = -1;
                replicaReadSessionContext.maximumSessionTimestamp = 0;
            }

            for (var i = csvi.firstKey; i < csvi.lastKey; i += csvi.step)
            {
                var key = parseState.GetArgSliceByRef(i).SpanByte;
                appendOnlyFile.Log.HashKey(ref key, out _, out var sublogIdx, out var keyOffset);

                // If first read initialize context
                if (replicaReadSessionContext.lastSublogIdx == -1)
                {
                    replicaReadSessionContext.lastSublogIdx = sublogIdx;
                    replicaReadSessionContext.maximumSessionTimestamp = GetActualKeyTimestamp(sublogIdx, keyOffset);
                    continue;
                }

                // Here we have to wait for replay to catch up
                // Don't have to wait if reading from same sublog or maximumSessionTimestamp is behind the sublog frontier timestamp
                if (replicaReadSessionContext.lastSublogIdx != sublogIdx && replicaReadSessionContext.maximumSessionTimestamp > GetKeyTimestampFrontier(sublogIdx, keyOffset))
                {
                    // Before adding to the waitQ set timestamp and reader associated information
                    readSessionWaiter.waitForTimestamp = replicaReadSessionContext.maximumSessionTimestamp;
                    readSessionWaiter.sublogIdx = (byte)sublogIdx;
                    readSessionWaiter.keyOffset = keyOffset;

                    // Enqueue waiter and wait
                    waitQs[sublogIdx].Enqueue(readSessionWaiter);
                    readSessionWaiter.eventSlim.Wait();

                    // Reset waiter for next iteration
                    readSessionWaiter.eventSlim.Reset();
                }

                // If timestamp of current key is after maximum timestamp we can safely read the key
                Debug.Assert(replicaReadSessionContext.maximumSessionTimestamp <= timestamps[sublogIdx][keyOffset]);
                replicaReadSessionContext.lastSublogIdx = sublogIdx;
                replicaReadSessionContext.maximumSessionTimestamp = GetActualKeyTimestamp(sublogIdx, keyOffset);
            }
        }
    }
}