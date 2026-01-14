// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;

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
    public class ReadConsistencyManager(long currentVersion, GarnetAppendOnlyFile appendOnlyFile, GarnetServerOptions serverOptions)
    {
        public long CurrentVersion { get; private set; } = currentVersion;
        readonly GarnetServerOptions serverOptions = serverOptions;

        readonly VirtualSublogReplayState[] vsrs = [.. Enumerable.Range(0, serverOptions.AofVirtualSublogCount).Select(_ => new VirtualSublogReplayState())];

        /// <summary>
        /// Get max sequence number across all sublogs
        /// </summary>
        public long MaxSequenceNumber => vsrs.Max(sublog => sublog.Max);

        /// <summary>
        /// Get snapshot of maximum replayed timestamp for all sublogs
        /// </summary>
        /// <returns></returns>
        public AofAddress GetSublogMaxKeySequenceNumber()
        {
            var physicalSublogCount = serverOptions.AofPhysicalSublogCount;
            var replayTaskCount = serverOptions.AofReplayTaskCount;
            var maxKeySeqNumVector = AofAddress.Create(physicalSublogCount, 0);
            for (var sublog = 0; sublog < physicalSublogCount; sublog++)
            {
                for (var rt = 0; rt < replayTaskCount; rt++)
                    maxKeySeqNumVector[sublog] = Math.Max(maxKeySeqNumVector[sublog], vsrs[appendOnlyFile.GetVirtualSublogIdx(sublog, rt)].Max);
            }
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
            var sublogIdx = (byte)(hash % serverOptions.AofVirtualSublogCount);
            return vsrs[sublogIdx].GetFrontierSequenceNumber(hash);
        }

        /// <summary>
        /// Get key specific sequence number for provided hash
        /// </summary>
        /// <param name="hash"></param>
        /// <returns></returns>
        long GetKeySequenceNumber(long hash)
        {
            var sublogIdx = (byte)(hash % serverOptions.AofVirtualSublogCount);
            return vsrs[sublogIdx].GetKeySequenceNumber(hash);
        }

        /// <summary>
        /// Update max sequence number of physical sublog associated with the specified sublogIdx.
        /// NOTE: This will update all virtual sublogs max sequence number
        /// </summary>
        /// <param name="sublogIdx"></param>
        public void UpdateSublogMaxSequenceNumber(int sublogIdx)
        {
            var replayTaskCount = serverOptions.AofReplayTaskCount;
            var globalMaxSequenceNumber = 0L;

            // Get maximum value across all virtual sublogs
            for (var rt = 0; rt < replayTaskCount; rt++)
                globalMaxSequenceNumber = Math.Max(globalMaxSequenceNumber, vsrs[appendOnlyFile.GetVirtualSublogIdx(sublogIdx, rt)].Max);

            // Update virtual sublog maximum value to ensure time moves forward when replaying with multiple tasks
            for (var rt = 0; rt < replayTaskCount; rt++)
                vsrs[appendOnlyFile.GetVirtualSublogIdx(sublogIdx, rt)].UpdateMaxSequenceNumber(globalMaxSequenceNumber);
        }

        /// <summary>
        /// Update max sequence number of virtual sublog associated with the specified virtual sublogIdx.
        /// </summary>
        /// <param name="virtualSublogIdx"></param>
        /// <param name="sequenceNumber"></param>
        public void UpdateVirtualSublogMaxSequenceNumber(int virtualSublogIdx, long sequenceNumber)
            => vsrs[virtualSublogIdx].UpdateMaxSequenceNumber(sequenceNumber);

        /// <summary>
        /// Update key sequence number of virtual sublog associated with the specified virtual sublogIdx.
        /// </summary>
        /// <param name="virtualSublogIdx"></param>
        /// <param name="key"></param>
        /// <param name="sequenceNumber"></param>
        public void UpdateVirtualSublogKeySequenceNumber(int virtualSublogIdx, ReadOnlySpan<byte> key, long sequenceNumber)
        {
            var keyHash = GarnetLog.HASH(key);
            vsrs[virtualSublogIdx].UpdateKeySequenceNumber(keyHash, sequenceNumber);
        }

        /// <summary>
        /// Update key sequence number of virtual sublog associated with the specified keyHash.
        /// </summary>
        /// <param name="keyHash"></param>
        /// <param name="sequenceNumber"></param>
        public void UpdateVirtualSublogKeySequenceNumber(long keyHash, long sequenceNumber)
        {
            var sublogIdx = (byte)(keyHash % serverOptions.AofVirtualSublogCount);
            vsrs[sublogIdx].UpdateKeySequenceNumber(keyHash, sequenceNumber);
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
            var virtualSublogIdx = (short)(hash % serverOptions.AofVirtualSublogCount);

            // If first time calling or version has been bumped reset read context
            // NOTE: version changes every time replica is reset and a attached to a new primary
            if (replicaReadSessionContext.sessionVersion == -1 || replicaReadSessionContext.sessionVersion != CurrentVersion)
            {
                replicaReadSessionContext.sessionVersion = CurrentVersion;
                replicaReadSessionContext.lastVirtualSublogIdx = -1;
                replicaReadSessionContext.maximumSessionSequenceNumber = 0;
            }

            // If first read initialize context
            if (replicaReadSessionContext.lastVirtualSublogIdx == -1)
                goto updateContext;

            // Here we have to wait for replay to catch up
            // Don't have to wait if reading from same sublog or maximumSessionTimestamp is behind the sublog frontier timestamp
            if (replicaReadSessionContext.lastVirtualSublogIdx != virtualSublogIdx && replicaReadSessionContext.maximumSessionSequenceNumber > GetSublogFrontierSequenceNumber(hash))
            {
                // Before adding to the waitQ set timestamp and reader associated information
                readSessionWaiter.rrsc = new ReplicaReadSessionContext()
                {
                    lastHash = hash,
                    lastVirtualSublogIdx = virtualSublogIdx,
                    maximumSessionSequenceNumber = replicaReadSessionContext.maximumSessionSequenceNumber
                };

                // Enqueue waiter and wait
                vsrs[virtualSublogIdx].AddWaiter(readSessionWaiter);
                readSessionWaiter.Wait(TimeSpan.FromSeconds(1000));

                // Reset waiter for next iteration
                readSessionWaiter.Reset();
            }

        updateContext:
            // Store for future update
            replicaReadSessionContext.lastVirtualSublogIdx = virtualSublogIdx;
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