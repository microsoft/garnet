// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using Microsoft.Extensions.Logging;

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
    /// <param name="currentVersion"></param>
    /// <param name="appendOnlyFile"></param>
    /// <param name="serverOptions"></param>
    /// <param name="logger"></param>
    public class ReplicaReadConsistencyStateManager(long currentVersion, GarnetAppendOnlyFile appendOnlyFile, GarnetServerOptions serverOptions, ILogger logger = null)
    {
        public long CurrentVersion { get; private set; } = currentVersion;
        readonly GarnetServerOptions serverOptions = serverOptions;
        readonly ILogger logger = logger;

        readonly SublogReplayState[] srs = [.. Enumerable.Range(0, serverOptions.AofPhysicalSublogCount).Select(_ => new SublogReplayState(serverOptions))];

        public long MaxSequenceNumber => srs.Select(sublog => sublog.Max).Max();

        /// <summary>
        /// Get snapshot of maximum replayed timestamp for all sublogs
        /// </summary>
        /// <returns></returns>
        public AofAddress GetSublogMaxKeySequenceNumber()
        {
            var maxKeySeqNumVector = AofAddress.Create(appendOnlyFile.Log.Size, 0);
            for (var i = 0; i < maxKeySeqNumVector.Length; i++)
                maxKeySeqNumVector[i] = srs[i].GetSublogMaxSequenceNumber();
            return maxKeySeqNumVector;
        }

        /// <summary>
        /// Get frontier sequence number for provided hash
        /// NOTE: Frontier sequence number is maximum sequence number between key specific sequence number and maximum observed sublog sequence number
        /// </summary>
        /// <param name="hash"></param>
        /// <returns></returns>
        long GetSublogFrontierSequenceNumber(long hash)
        {
            var sublogIdx = (byte)(hash % serverOptions.AofPhysicalSublogCount);
            return srs[sublogIdx].GetSublogFrontierSequenceNumber(hash);
        }

        /// <summary>
        /// Get key specific sequence number for provided hash
        /// </summary>
        /// <param name="hash"></param>
        /// <returns></returns>
        long GetKeySequenceNumber(long hash)
        {
            var sublogIdx = (byte)(hash % serverOptions.AofPhysicalSublogCount);
            return srs[sublogIdx].GetKeySequenceNumber(hash);
        }

        /// <summary>
        /// Update max sequence number of physical sublog associated with the specified sublogIdx.
        /// </summary>
        /// <param name="sublogIdx"></param>
        public void UpdateSublogMaxSequenceNumber(int sublogIdx)
            => srs[sublogIdx].UpdateMaxSequenceNumber();

        /// <summary>
        /// Update max sequence number of physical sublog associated with the specified sublogIdx.
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="sequenceNumber"></param>
        public void UpdateSublogMaxSequenceNumber(int sublogIdx, long sequenceNumber)
            => srs[sublogIdx].UpdateMaxSequenceNumber(sequenceNumber);

        /// <summary>
        /// Update sequence number for provided key (called after replay)
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="key"></param>
        /// <param name="sequenceNumber"></param>
        public void UpdateKeySequenceNumber(int sublogIdx, ReadOnlySpan<byte> key, long sequenceNumber)
        {
            var keyHash = GarnetLog.HASH(key);
            srs[sublogIdx].UpdateKeySequenceNumber(keyHash, sequenceNumber);
        }

        /// <summary>
        /// Update sequence number for provided key hash (called after replay)
        /// </summary>
        /// <param name="keyHash"></param>
        /// <param name="sequenceNumber"></param>
        public void UpdateKeySequenceNumber(long keyHash, long sequenceNumber)
        {
            srs[keyHash].UpdateKeySequenceNumber(keyHash, sequenceNumber);
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
            var hash = GarnetLog.HASH(key);
            var sublogIdx = (byte)(hash % serverOptions.AofPhysicalSublogCount);
            var replayIdx = (byte)(hash % serverOptions.AofReplaySubtaskCount);

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
                goto updateContext;

            // Here we have to wait for replay to catch up
            // Don't have to wait if reading from same sublog or maximumSessionTimestamp is behind the sublog frontier timestamp
            if (replicaReadSessionContext.lastSublogIdx != sublogIdx && replicaReadSessionContext.lastReplayIdx != replayIdx && replicaReadSessionContext.maximumSessionSequenceNumber > GetSublogFrontierSequenceNumber(hash))
            {
                // Before adding to the waitQ set timestamp and reader associated information
                readSessionWaiter.rrsc = new ReplicaReadSessionContext()
                {
                    lastHash = hash,
                    lastSublogIdx = sublogIdx,
                    lastReplayIdx = replayIdx,
                    maximumSessionSequenceNumber = replicaReadSessionContext.maximumSessionSequenceNumber
                };

                //logger?.LogError("Paused [{last}] {msn} > [{current}] {fsn}",
                //    replicaReadSessionContext.lastSublogIdx,
                //    replicaReadSessionContext.maximumSessionSequenceNumber,
                //    sublogIdx,
                //    GetSublogFrontierSequenceNumber(hash));

                // Enqueue waiter and wait
                srs[sublogIdx].AddWaiter(hash, readSessionWaiter);
                readSessionWaiter.Wait(TimeSpan.FromSeconds(1000));

                //logger?.LogError("Resumed [{last}] {msn} > [{current}] {fsn}",
                //    replicaReadSessionContext.lastSublogIdx,
                //    replicaReadSessionContext.maximumSessionSequenceNumber,
                //    sublogIdx,
                //    GetSublogFrontierSequenceNumber(hash));

                // Reset waiter for next iteration
                readSessionWaiter.Reset();
            }

        updateContext:
            // Store for future update
            replicaReadSessionContext.lastSublogIdx = sublogIdx;
            replicaReadSessionContext.lastReplayIdx = replayIdx;
            replicaReadSessionContext.lastHash = hash;
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
            replicaReadSessionContext.maximumSessionSequenceNumber = Math.Max(
                replicaReadSessionContext.maximumSessionSequenceNumber, GetKeySequenceNumber(replicaReadSessionContext.lastHash));
        }
    }
}