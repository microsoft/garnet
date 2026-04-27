// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Runtime.CompilerServices;
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
            => vsrs[appendOnlyFile.Log.GetVirtualSublogIdx(keyHash)].GetFrontierSequenceNumber(keyHash);

        /// <summary>
        /// Get key specific sequence number for provided hash
        /// </summary>
        /// <param name="keyHash"></param>
        /// <returns></returns>
        long GetKeySequenceNumber(long keyHash)
            => vsrs[appendOnlyFile.Log.GetVirtualSublogIdx(keyHash)].GetKeySequenceNumber(keyHash);

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
        /// <param name="keyHash"></param>
        /// <param name="sequenceNumber"></param>
        public void UpdateVirtualSublogKeySequenceNumber(int virtualSublogIdx, long keyHash, long sequenceNumber)
            => vsrs[virtualSublogIdx].UpdateKeySequenceNumber(keyHash, sequenceNumber);

        /// <summary>
        /// Update key sequence number of virtual sublog associated with the specified keyHash.
        /// </summary>
        /// <param name="keyHash"></param>
        /// <param name="sequenceNumber"></param>
        public void UpdateVirtualSublogKeySequenceNumber(long keyHash, long sequenceNumber)
            => vsrs[appendOnlyFile.Log.GetVirtualSublogIdx(keyHash)].UpdateKeySequenceNumber(keyHash, sequenceNumber);

        /// <summary>
        /// Ensures that the specified replica read session context is synchronized with the current session version.
        /// </summary>
        /// <param name="replicaReadSessionContext">A reference to the session context to check and update.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CheckConsistencyManagerVersion(ref ReplicaReadSessionContext replicaReadSessionContext)
        {
            // If first time calling or version has been bumped reset read context
            // NOTE: Version changes every time replica is reset and a attached to a new primary.
            // When a batch of read commands executes, it all happens under epoch protection, hence version change will not affect read prefix consistency
            if (replicaReadSessionContext.sessionVersion == -1 || replicaReadSessionContext.sessionVersion != CurrentVersion)
            {
                replicaReadSessionContext.sessionVersion = CurrentVersion;
                replicaReadSessionContext.lastVirtualSublogIdx = -1;
                replicaReadSessionContext.maximumSessionSequenceNumber = 0;
            }
        }

        /// <summary>
        /// Verify key freshness before allowing reads.
        /// </summary>
        /// <param name="keyHash"></param>
        /// <param name="replicaReadSessionContext"></param>
        /// <param name="ct"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void VerifyKeyFreshness(long keyHash, ref ReplicaReadSessionContext replicaReadSessionContext, CancellationToken ct)
        {
            var virtualSublogIdx = appendOnlyFile.Log.GetVirtualSublogIdx(keyHash);

            // Here we have to wait for replay to catch up
            // Don't have to wait if reading from same sublog or maximumSessionTimestamp is behind the sublog frontier timestamp
            if (replicaReadSessionContext.lastVirtualSublogIdx != -1 && replicaReadSessionContext.lastVirtualSublogIdx != virtualSublogIdx)
            {
                // Optimistic check without lock
                while (replicaReadSessionContext.maximumSessionSequenceNumber >= GetSublogFrontierSequenceNumber(keyHash))
                {
                    vsrs[virtualSublogIdx].WaitForSequenceNumber(
                        keyHash,
                        replicaReadSessionContext.maximumSessionSequenceNumber,
                        ct);
                }
            }

            // Store for future update
            replicaReadSessionContext.lastVirtualSublogIdx = (short)virtualSublogIdx;
            replicaReadSessionContext.lastHash = keyHash;
        }

        /// <summary>
        /// This method implements part of the consistent read protocol for a single key when shared AOF is enabled.
        /// NOTE:
        ///     This method waits until the log sequence number of the associated key is lesser or equal than the maximum session log sequence number.
        ///     It executes before store.Read is processed to ensure that the log sequence number of the associated key is ahead of the last read in accordance to the consistent read protocol
        ///     The replica read context is updated (<seealso cref="T:Garnet.server.ReplicaReadConsistencyManager.ConsistentReadSequenceNumberUpdate"/>) after the actual store.Read call to ensure that we don't underestimate the true log sequence number.
        /// </summary>
        /// <param name="hash"></param>
        /// <param name="replicaReadSessionContext"></param>
        /// <param name="ct"></param>
        public void BeforeConsistentReadKey(long hash, ref ReplicaReadSessionContext replicaReadSessionContext, CancellationToken ct)
        {
            // Check version
            CheckConsistencyManagerVersion(ref replicaReadSessionContext);

            // Verify key freshness
            VerifyKeyFreshness(hash, ref replicaReadSessionContext, ct);
        }

        /// <summary>
        /// This method implements part of the consistent read protocol for a single key when shared AOF is enabled.
        /// NOTE:
        ///     This method is used to update the log sequence number after store.Read was processed.
        ///     This is done to ensure that the log sequence number tracked by the ReadConsistencyManager is an overestimate of the actual sequence number since
        ///     we cannot be certain at prepare phase what is the actual sequence number.
        /// </summary>
        /// <param name="replicaReadSessionContext"></param>
        public void AfterConsistentReadKey(ref ReplicaReadSessionContext replicaReadSessionContext)
        {
            replicaReadSessionContext.maximumSessionSequenceNumber = Math.Max(
                replicaReadSessionContext.maximumSessionSequenceNumber, GetKeySequenceNumber(replicaReadSessionContext.lastHash));
        }

        /// <summary>
        /// Verify key freshness and keep track hash and maximum session sequence number to check for updates after batch read.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="batchReadContext"></param>
        /// <param name="ct"></param>
        /// <param name="hash"></param>
        public void BeforeConsistentReadKeyBatch(ReadOnlySpan<byte> key, ref ReplicaReadSessionContext batchReadContext, CancellationToken ct, out long hash)
        {
            // Verify key freshness
            hash = GarnetLog.HASH(key);
            VerifyKeyFreshness(hash, ref batchReadContext, ct);

            // Keep track of max sequence number to check for updates after batch read.
            batchReadContext.maximumSessionSequenceNumber = Math.Max(
                batchReadContext.maximumSessionSequenceNumber, GetKeySequenceNumber(batchReadContext.lastHash));
        }

        /// <summary>
        /// Validate that key sequence number has not progressed beyond the snapshot used for batch key read.
        /// </summary>
        /// <param name="hash"></param>
        /// <param name="batchReadContext"></param>
        /// <returns></returns>
        public bool AfterConsistentReadKeyBatch(long hash, ref ReplicaReadSessionContext batchReadContext)
        {
            var keySequenceNumber = GetKeySequenceNumber(hash);
            var mSSN = batchReadContext.maximumSessionSequenceNumber;
            // NOTE: Read key batch is prefix consistent at boundary because maximumSessionSequenceNumber (mSSN) == maxof(batch key sequence numbers)
            // and freshness check would have prevented boundary read of the corresponding key.
            // In other words, T_k (timestamp of key k) < T_f (frontier timestamp where read was allowed to proceed) and because mSSN == max of all T_k in the batch
            // mSSN < T_f, hence time has advanced beyond the point where it is safe to read.
            return keySequenceNumber <= mSSN;
        }
    }
}