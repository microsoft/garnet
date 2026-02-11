// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading;

namespace Garnet.server
{
    /// <summary>
    /// Manages read consistency for append-only file operations, tracking sequence numbers and ensuring consistent
    /// reads across virtual sublogs and keys.
    /// </summary>
    /// <param name="currentVersion"></param>
    /// <param name="appendOnlyFile"></param>
    /// <param name="serverOptions"></param>
    public class ReadConsistencyManager(long currentVersion, GarnetAppendOnlyFile appendOnlyFile, GarnetServerOptions serverOptions)
    {
        /// <summary>
        /// Read consistency manager version.
        /// </summary>
        public long CurrentVersion { get; private set; } = currentVersion;
        readonly GarnetServerOptions serverOptions = serverOptions;

        readonly VirtualSublogReplayState[] vsrs = [.. Enumerable.Range(0, serverOptions.AofVirtualSublogCount).Select(_ => new VirtualSublogReplayState())];

        /// <summary>
        /// Get max sequence number across all sublogs
        /// </summary>
        public long MaxSequenceNumber => vsrs.Max(sublog => sublog.Max);

        /// <summary>
        /// Get sequence number for provided key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="frontier"></param>
        /// <returns></returns>
        public long GetKeySequenceNumber(ReadOnlySpan<byte> key, bool frontier = false)
        {
            var hash = GarnetLog.HASH(key);
            return frontier ? GetSublogFrontierSequenceNumber(hash) : GetKeySequenceNumber(hash);
        }

        /// <summary>
        /// Get snapshot of maximum replayed timestamp for all physical sublogs
        /// </summary>
        /// <returns></returns>
        public AofAddress GetPhysicalSublogMaxReplayedSequenceNumber()
        {
            var physicalSublogCount = serverOptions.AofPhysicalSublogCount;
            var replayTaskCount = serverOptions.AofReplayTaskCount;
            var maxKeySeqNumVector = AofAddress.Create(physicalSublogCount, 0);
            for (var physicalSublogIdx = 0; physicalSublogIdx < physicalSublogCount; physicalSublogIdx++)
            {
                for (var rt = 0; rt < replayTaskCount; rt++)
                    maxKeySeqNumVector[physicalSublogIdx] = Math.Max(maxKeySeqNumVector[physicalSublogIdx], vsrs[appendOnlyFile.GetVirtualSublogIdx(physicalSublogIdx, rt)].Max);
            }
            return maxKeySeqNumVector;
        }

        /// <summary>
        /// Get frontier sequence number for provided hash
        /// NOTE: Frontier sequence number is maximum sequence number between key specific sequence number and maximum observed sublog sequence number
        /// </summary>
        /// <param name="keyHash"></param>
        /// <returns></returns>
        long GetSublogFrontierSequenceNumber(long keyHash)
        {
            var sublogIdx = (byte)(keyHash % serverOptions.AofVirtualSublogCount);
            return vsrs[sublogIdx].GetFrontierSequenceNumber(keyHash);
        }

        /// <summary>
        /// Get key specific sequence number for provided hash
        /// </summary>
        /// <param name="keyHash"></param>
        /// <returns></returns>
        long GetKeySequenceNumber(long keyHash)
        {
            var sublogIdx = (byte)(keyHash % serverOptions.AofVirtualSublogCount);
            return vsrs[sublogIdx].GetKeySequenceNumber(keyHash);
        }

        /// <summary>
        /// Update physical sublog max sequence number
        /// </summary>
        /// <param name="physicalSublogIdx"></param>
        /// <param name="sequenceNumber"></param>
        public void UpdatePhysicalSublogMaxSequenceNumber(int physicalSublogIdx, long sequenceNumber)
        {
            var replayTaskCount = serverOptions.AofReplayTaskCount;
            // Update virtual sublog maximum value for all virtual sublogs
            for (var rt = 0; rt < replayTaskCount; rt++)
                vsrs[appendOnlyFile.GetVirtualSublogIdx(physicalSublogIdx, rt)].UpdateMaxSequenceNumber(sequenceNumber);
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
            var virtualSublogIdx = (byte)(keyHash % serverOptions.AofVirtualSublogCount);
            vsrs[virtualSublogIdx].UpdateKeySequenceNumber(keyHash, sequenceNumber);
        }

        /// <summary>
        /// This method implements part of the consistent read protocol for a single key when shared AOF is enabled.
        /// NOTE:
        ///     This method waits until the log sequence number of the associated key is lesser or equal than the maximum session log sequence number.
        ///     It executes before store.Read is processed to ensure that the log sequence number of the associated key is ahead of the last read in accordance to the consistent read protocol
        ///     The replica read context is updated (<seealso cref="T:Garnet.server.ReplicaReadConsistencyManager.ConsistentReadSequenceNumberUpdate"/>) after the actual store.Read call to ensure that we don't underestimate the true log sequence number.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="replicaReadSessionContext"></param>
        /// <param name="ct"></param>
        public void ConsistentReadKeyPrepare(ReadOnlySpan<byte> key, ref ReplicaReadSessionContext replicaReadSessionContext, CancellationToken ct)
        {
            var hash = GarnetLog.HASH(key);
            var virtualSublogIdx = (short)(hash % serverOptions.AofVirtualSublogCount);

            // If first time calling or version has been bumped reset read context
            // NOTE: Version changes every time replica is reset and a attached to a new primary.
            // When a batch of read commands executes, it all happens under epoch protection, hence version change will not affect read prefix consistency
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
            if (replicaReadSessionContext.lastVirtualSublogIdx != virtualSublogIdx)
            {
                // Optimistic check without lock
                while (replicaReadSessionContext.maximumSessionSequenceNumber >= GetSublogFrontierSequenceNumber(hash))
                {
                    vsrs[virtualSublogIdx].WaitForSequenceNumber(
                        hash,
                        replicaReadSessionContext.maximumSessionSequenceNumber,
                        ct).Wait(ct);
                }
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