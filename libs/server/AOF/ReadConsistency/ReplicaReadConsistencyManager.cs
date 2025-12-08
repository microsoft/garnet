// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// This implements the replica sequence manager to track the sequence number of keys for all sessions in a given replica.
    /// The class maintains a 2D-array of size [SublogCount][KeyOffsetCount + 1] which tracks the timestamps for a set of keys per sublog
    /// Every data replay operation will update this map on per key basis using a hash function to determine the keyOffset.
    /// This is done to mitigate the read delay for the following scenario if we only maintain a maximum timestamp per sublog
    ///     time: ----(k1,t1)---(k2,t2)---(k3,t3)----
    ///     s1: (k1,t1) (k3,t3)
    ///     s2: (k2,t2)
    ///     read k1 (at t3 because s1 has replayed until k3)
    ///     read k2 (at t2 have to wait because reading k1 established that we are in t3 though no updates have arrived for k1 by t3)
    /// </summary>
    /// <param name="nextVersion"></param>
    /// <param name="appendOnlyFile"></param>
    /// <param name="serverOptions"></param>
    /// <param name="logger"></param>
    public class ReplicaReadConsistencyManager(long nextVersion, GarnetAppendOnlyFile appendOnlyFile, GarnetServerOptions serverOptions, ILogger logger = null)
    {
        public long CurrentVersion { get; private set; } = nextVersion;
        public const int KeyOffsetCount = (1 << 15) + 1;
        const int SublogMaxKeySequenceNumberOffset = KeyOffsetCount - 1;
        readonly GarnetAppendOnlyFile appendOnlyFile = appendOnlyFile;
        readonly GarnetServerOptions serverOptions = serverOptions;
        readonly ILogger logger = logger;

        long[][] activeSequenceNumbers = InitializeTimestamps(appendOnlyFile.Log.Size, KeyOffsetCount);

        /// <summary>
        /// Get snapshot of maximum replayed timestamp for all sublogs
        /// </summary>
        /// <returns></returns>
        public AofAddress GetSublogMaxKeySequenceNumber()
        {
            var maxKeySeqNumVector = AofAddress.Create(appendOnlyFile.Log.Size, 0);
            for (var i = 0; i < maxKeySeqNumVector.Length; i++)
                maxKeySeqNumVector[i] = activeSequenceNumbers[i][SublogMaxKeySequenceNumberOffset];
            return maxKeySeqNumVector;
        }

        static ReplicaReadConsistencyManager()
        {
            var size = KeyOffsetCount - 1;
            if ((size & (size - 1)) != 0)
                throw new InvalidOperationException($"Size ({KeyOffsetCount - 1}) must be a power of 2");
        }

        ConcurrentQueue<ReadSessionWaiter>[] waitQs = InitializeWaitQs(appendOnlyFile.Log.Size);

        static ConcurrentQueue<ReadSessionWaiter>[] InitializeWaitQs(int aofSublogCount)
            => [.. Enumerable.Range(0, aofSublogCount).Select(_ => new ConcurrentQueue<ReadSessionWaiter>())];

        private static long[][] InitializeTimestamps(int aofSublogCount, int size)
            => [.. Enumerable.Range(0, aofSublogCount).Select(_ =>
            {
                var array = GC.AllocateArray<long>(size, pinned: true);
                Array.Clear(array);
                return array;
            })];

        /// <summary>
        /// Get sequence number frontier which is the maximum between the key sequence number and the sublog maximum sequence number
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="keyOffset"></param>
        /// <returns></returns>
        long GetFrontierSequenceNumber(int sublogIdx, int keyOffset)
            => Math.Max(activeSequenceNumbers[sublogIdx][keyOffset], activeSequenceNumbers[sublogIdx][SublogMaxKeySequenceNumberOffset]);

        /// <summary>
        /// Get sequence number for provided key offset
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="keyOffset"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long GetKeySequenceNumber(int sublogIdx, int keyOffset)
            => activeSequenceNumbers[sublogIdx][keyOffset];

        /// <summary>
        /// Get maximum sequence number
        /// NOTE: used on reset/failover to update sequence number start offset for replica that is taking over
        /// </summary>
        /// <returns></returns>
        public long GetMaximumSequenceNumber() => activeSequenceNumbers.SelectMany(sublogArray => sublogArray).Max();

        /// <summary>
        /// Update sequence number for all keys corresponding to sublog.
        /// This is triggered by RefreshSublogTail, TxnReplay or CustomProcReplay.
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="sequenceNumber"></param>
        /// <seealso cref="T:Garnet.cluster.AofSyncDriver.RefreshSublogTail"/>
        public void UpdateSublogSequencenumber(int sublogIdx, long sequenceNumber)
        {
            // logger?.LogError("+sn: {sn} idx: {sublogIdx}", sequenceNumber, sublogIdx);
            _ = Utility.MonotonicUpdate(ref activeSequenceNumbers[sublogIdx][SublogMaxKeySequenceNumberOffset], sequenceNumber, out _);
            SignalWaiters(sublogIdx);
        }

        /// <summary>
        /// Update key sequence number when replaying the AOF at replica and release any waiters.
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="key"></param>
        /// <param name="sequenceNumber"></param>
        public void UpdateKeySequenceNumber(int sublogIdx, ref PinnedSpanByte key, long sequenceNumber)
        {
            appendOnlyFile.Log.HashKey(ref key, out _, out var _sublogIdx, out var keyOffset);
            Debug.Assert(sublogIdx == _sublogIdx);
            // logger?.LogError("*sn: {sn} idx: {sublogIdx} key: {keyOffset}", sequenceNumber, sublogIdx, keyOffset);
            _ = Utility.MonotonicUpdate(ref activeSequenceNumbers[sublogIdx][keyOffset], sequenceNumber, out _);
            _ = Utility.MonotonicUpdate(ref activeSequenceNumbers[sublogIdx][SublogMaxKeySequenceNumberOffset], sequenceNumber, out _);
            SignalWaiters(sublogIdx);
        }

        /// <summary>
        /// Update sequenceNumber for provided sublogIdx and keyOffset
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="keyOffset"></param>
        /// <param name="sequenceNumber"></param>
        public void UpdateKeySequenceNumber(int sublogIdx, int keyOffset, long sequenceNumber)
        {
            _ = Utility.MonotonicUpdate(ref activeSequenceNumbers[sublogIdx][keyOffset], sequenceNumber, out _);
            _ = Utility.MonotonicUpdate(ref activeSequenceNumbers[sublogIdx][SublogMaxKeySequenceNumberOffset], sequenceNumber, out _);
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
                if (waiter.waitForTimestamp > GetFrontierSequenceNumber(waiter.sublogIdx, waiter.keyOffset))
                    waiterList.Add(waiter);
                else
                    // Signal for waiter to proceed
                    waiter.Set();
            }

            // Re-insert any waiters that have not been released yet
            foreach (var waiter in waiterList)
                waitQs[sublogIdx].Enqueue(waiter);
        }

        /// <summary>
        /// Ensure consistent read protocol for network command
        /// NOTE: Reading and validating one key at a time is not transactional
        /// but it is ok since it is no worse than what we currently support (i.e. MGET)
        /// </summary>
        /// <param name="replicaReadSessionContext"></param>
        /// <param name="parseState"></param>
        /// <param name="csvi"></param>
        public void MultiKeyConsistentRead(ref ReplicaReadSessionContext replicaReadSessionContext, ref SessionParseState parseState, ref ClusterSlotVerificationInput csvi, ReadSessionWaiter readSessionWaiter)
        {
            // If first time calling or version has been bumped reset read context
            // NOTE: version changes every time replica is reset and a attached to a new primary
            if (replicaReadSessionContext.sessionVersion == -1 || replicaReadSessionContext.sessionVersion != CurrentVersion)
            {
                replicaReadSessionContext.sessionVersion = CurrentVersion;
                replicaReadSessionContext.lastSublogIdx = -1;
                replicaReadSessionContext.maximumSessionSequenceNumber = 0;
            }

            for (var i = csvi.firstKey; i < csvi.lastKey; i += csvi.step)
            {
                var key = parseState.GetArgSliceByRef(i).Span;
                ConsistentReadKey(key, ref replicaReadSessionContext, readSessionWaiter);
            }
        }

        /// <summary>
        /// Enforce consistent read protocol for key batch
        /// NOTE: Reading and validating one key at a time is not transactional
        /// but it is ok since it is no worse than what we currently support (i.e. MGET)
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="replicaReadSessionContext"></param>
        /// <param name="readSessionWaiter"></param>
        public void MultiKeyConsistentRead(List<byte[]> keys, ref ReplicaReadSessionContext replicaReadSessionContext, ReadSessionWaiter readSessionWaiter)
        {
            for (var i = 0; i < keys.Count; i++)
                ConsistentReadKey(keys[i].AsSpan(), ref replicaReadSessionContext, readSessionWaiter);
        }

        /// <summary>
        /// Consistent read protocol implementation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="replicaReadSessionContext"></param>
        /// <param name="readSessionWaiter"></param>
        public void ConsistentReadKey(Span<byte> key, ref ReplicaReadSessionContext replicaReadSessionContext, ReadSessionWaiter readSessionWaiter)
        {
            appendOnlyFile.Log.Hash(key, out _, out var sublogIdx, out var keyOffset);

            // If first time calling or version has been bumped reset read context
            // NOTE: version changes every time replica is reset and a attached to a new primary
            if (replicaReadSessionContext.sessionVersion == -1 || replicaReadSessionContext.sessionVersion != CurrentVersion)
            {
                replicaReadSessionContext.sessionVersion = CurrentVersion;
                replicaReadSessionContext.lastSublogIdx = -1;
                replicaReadSessionContext.maximumSessionSequenceNumber = 0;
            }

            // If first read initialize context
            if (replicaReadSessionContext.lastSublogIdx == -1)
            {
                replicaReadSessionContext.lastSublogIdx = sublogIdx;
                replicaReadSessionContext.maximumSessionSequenceNumber = GetKeySequenceNumber(sublogIdx, keyOffset);
                return;
            }

            // Here we have to wait for replay to catch up
            // Don't have to wait if reading from same sublog or maximumSessionTimestamp is behind the sublog frontier timestamp
            if (replicaReadSessionContext.lastSublogIdx != sublogIdx && replicaReadSessionContext.maximumSessionSequenceNumber > GetFrontierSequenceNumber(sublogIdx, keyOffset))
            {
                // Before adding to the waitQ set timestamp and reader associated information
                readSessionWaiter.waitForTimestamp = replicaReadSessionContext.maximumSessionSequenceNumber;
                readSessionWaiter.sublogIdx = (byte)sublogIdx;
                readSessionWaiter.keyOffset = keyOffset;

                // Enqueue waiter and wait
                waitQs[sublogIdx].Enqueue(readSessionWaiter);
                readSessionWaiter.Wait(serverOptions.ReplicaSyncTimeout);

                // Reset waiter for next iteration
                readSessionWaiter.Reset();
            }

            // If timestamp of current key is after maximum timestamp we can safely read the key
            replicaReadSessionContext.lastSublogIdx = sublogIdx;
            replicaReadSessionContext.maximumSessionSequenceNumber = Math.Max(replicaReadSessionContext.maximumSessionSequenceNumber, GetKeySequenceNumber(sublogIdx, keyOffset));
        }

        /// <summary>
        /// This method implements part of the consistent read protocol for a single key when shared AOF is enabled.
        /// NOTE:
        ///     This method waits until the log sequence number of the associated key is lesser or equal than the maximum session log sequence number.
        ///     It executes before store.Read is processed to ensure that the log sequence number of the associated key is ahead of the last read in accordance to the consistent read protocol
        ///     The replica read context is updated (<seealso cref="T:Garnet.server.ReplicaReadConsistencyManager.ConsistentReadKeyUpdate"/>) after the actual store.Read call to ensure that we don't underestimate the true log sequence number.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="replicaReadSessionContext"></param>
        /// <param name="readSessionWaiter"></param>
        public void ConsistentReadKeyPrepare(ReadOnlySpan<byte> key, ref ReplicaReadSessionContext replicaReadSessionContext, ReadSessionWaiter readSessionWaiter)
        {
            appendOnlyFile.Log.Hash(key, out _, out var sublogIdx, out var keyOffset);

            // If first time calling or version has been bumped reset read context
            // NOTE: version changes every time replica is reset and a attached to a new primary
            if (replicaReadSessionContext.sessionVersion == -1 || replicaReadSessionContext.sessionVersion != CurrentVersion)
            {
                replicaReadSessionContext.sessionVersion = CurrentVersion;
                replicaReadSessionContext.lastSublogIdx = -1;
                replicaReadSessionContext.maximumSessionSequenceNumber = 0;
            }

            // If first read initialize context
            if (replicaReadSessionContext.lastSublogIdx == -1)
            {
                // Store for future update
                replicaReadSessionContext.lastSublogIdx = sublogIdx;
                replicaReadSessionContext.lastKeyOffset = keyOffset;
                return;
            }

            // Here we have to wait for replay to catch up
            // Don't have to wait if reading from same sublog or maximumSessionTimestamp is behind the sublog frontier timestamp
            if (replicaReadSessionContext.lastSublogIdx != sublogIdx && replicaReadSessionContext.maximumSessionSequenceNumber > GetFrontierSequenceNumber(sublogIdx, keyOffset))
            {
                // Before adding to the waitQ set timestamp and reader associated information
                readSessionWaiter.waitForTimestamp = replicaReadSessionContext.maximumSessionSequenceNumber;
                readSessionWaiter.sublogIdx = (byte)sublogIdx;
                readSessionWaiter.keyOffset = keyOffset;

                logger?.LogError("Paused [{last}] {msn} > [{current}] {fsn}",
                    replicaReadSessionContext.lastSublogIdx,
                    replicaReadSessionContext.maximumSessionSequenceNumber,
                    sublogIdx,
                    GetFrontierSequenceNumber(sublogIdx, keyOffset));

                // Enqueue waiter and wait
                waitQs[sublogIdx].Enqueue(readSessionWaiter);
                readSessionWaiter.Wait(TimeSpan.FromSeconds(1000));

                logger?.LogError("Resumed [{last}] {msn} > [{current}] {fsn}",
                    replicaReadSessionContext.lastSublogIdx,
                    replicaReadSessionContext.maximumSessionSequenceNumber,
                    sublogIdx,
                    GetFrontierSequenceNumber(sublogIdx, keyOffset));

                // Reset waiter for next iteration
                readSessionWaiter.Reset();
            }

            // Store for future update
            replicaReadSessionContext.lastSublogIdx = sublogIdx;
            replicaReadSessionContext.lastKeyOffset = keyOffset;
        }

        /// <summary>
        /// This method implements part of the consistent read protocol for a single key when shared AOF is enabled.
        /// NOTE:
        ///     This method is used to update the log sequence number after store.Read was processed.
        ///     This is done to ensure that the log sequence number tracked by the ReadConsistencyManager is an overestimate of the actual sequence number since
        ///     we cannot be certain at prepare phase what is the actual sequence number.
        /// </summary>
        /// <param name="replicaReadSessionContext"></param>
        public void ConsistentReadSequenceNumberUpdate(ref ReplicaReadSessionContext replicaReadSessionContext)
        {
            var sublogIdx = replicaReadSessionContext.lastSublogIdx;
            var keyOffset = replicaReadSessionContext.lastKeyOffset;
            replicaReadSessionContext.maximumSessionSequenceNumber = Math.Max(
                replicaReadSessionContext.maximumSessionSequenceNumber, GetKeySequenceNumber(sublogIdx, keyOffset));
        }
    }
}