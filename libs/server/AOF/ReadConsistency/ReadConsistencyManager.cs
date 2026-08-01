// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
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
        /// Maximum allowed drift (in sequence-number units) between leading and trailing sublog
        /// before the reader will trigger a replay-side synchronization barrier. -1 disables the
        /// barrier so the reader never activates a round.
        /// </summary>
        readonly long replayDriftThreshold = serverOptions.AofReplayDriftThreshold;

        /// <summary>
        /// Whether the reader bounds replay drift at all: false when the barrier is disabled
        /// (threshold -1) or there is a single virtual sublog (no cross-sublog drift to bound).
        /// </summary>
        readonly bool driftBoundingEnabled = serverOptions.AofReplayDriftThreshold >= 0 && serverOptions.AofVirtualSublogCount > 1;

        /// <summary>
        /// Cooperative barrier used to bound inter-virtual-sublog replay drift. The reader activates it
        /// on demand when it observes a large drift while about to wait; replay threads align on it via
        /// per-record CheckAndWait calls. One participant per virtual sublog (one replay thread each).
        /// </summary>
        public readonly ReplayAlignBarrier replayBarrier = new(serverOptions.AofVirtualSublogCount, serverOptions.AofReplayBarrierSpinUs);

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
        /// Gets the maximum replayed sequence number for a single physical sublog
        /// by reading the max across all its virtual sublogs.
        /// </summary>
        /// <param name="physicalSublogIdx">Physical sublog index.</param>
        /// <returns>The maximum sequence number observed across all virtual sublogs for this physical sublog.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPhysicalSublogMax(int physicalSublogIdx)
        {
            var replayTaskCount = serverOptions.AofReplayTaskCount;
            var startIdx = appendOnlyFile.GetVirtualSublogIdx(physicalSublogIdx, 0);
            long max = 0;
            for (var rt = 0; rt < replayTaskCount; rt++)
                max = Math.Max(max, Volatile.Read(ref vsrs[startIdx + rt].MaxRef));
            return max;
        }

        public string GetPhysicalSublogMaxSequenceVector()
        {
            StringBuilder stringBuilder = new();
            var sublogCount = serverOptions.AofPhysicalSublogCount;
            _ = stringBuilder.Append(GetPhysicalSublogMax(0));
            for (var s = 1; s < sublogCount; s++)
            {
                _ = stringBuilder.Append(',');
                _ = stringBuilder.Append(GetPhysicalSublogMax(s));
            }
            return stringBuilder.ToString();
        }

        public unsafe string GetPhysicalSublogMaxDriftSequenceVector()
        {
            var sublogCount = serverOptions.AofPhysicalSublogCount;
            var physicalSublogMaxSequenceVector = stackalloc long[sublogCount];

            var maxSequenceNumber = 0L;
            for (var s = 0; s < sublogCount; s++)
            {
                physicalSublogMaxSequenceVector[s] = GetPhysicalSublogMax(s);
                maxSequenceNumber = Math.Max(maxSequenceNumber, physicalSublogMaxSequenceVector[s]);
            }
            StringBuilder stringBuilder = new();
            _ = stringBuilder.Append(maxSequenceNumber - physicalSublogMaxSequenceVector[0]);
            for (var s = 1; s < sublogCount; s++)
            {
                _ = stringBuilder.Append(',');
                _ = stringBuilder.Append(maxSequenceNumber - physicalSublogMaxSequenceVector[s]);
            }
            return stringBuilder.ToString();
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
        /// Advances an idle virtual sublog from an in-band primary pulse and registers a
        /// non-blocking arrival at any active replay-alignment round.
        /// </summary>
        /// <param name="virtualSublogIdx"></param>
        /// <param name="sequenceNumber"></param>
        public void AdvanceVirtualSublogTime(int virtualSublogIdx, long sequenceNumber)
        {
            vsrs[virtualSublogIdx].UpdateMaxSequenceNumber(sequenceNumber);
            replayBarrier.CheckAndArrive(virtualSublogIdx, vsrs[virtualSublogIdx].Max);
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
        {
            // Publish this sublog's frontier (max) eagerly -- BEFORE parking and before the key
            // sketch entry. This makes barrier arrival deterministic from replay alone: a replay
            // thread that crosses an active round's target on THIS record arrives on this record via
            // the CheckAndWait below (which now sees the just-published max), instead of only on a
            // subsequent record or an idle-sublog time pulse. Without the eager publish, a lagging
            // sublog that crosses the target on its final pending record and then goes idle never
            // registers its arrival through CheckAndWait, so the round can only complete if a pulse
            // happens to advance it -- and if that pulse is delayed, every arrived participant spins
            // forever (AofReplayBarrierSpinUs < 0) and replay deadlocks. Publishing the max early is
            // safe for readers: a far-ahead frontier only makes a reader's prepare gate pass more
            // easily; it never feeds a session clock (which is drawn from the key sketch entry).
            vsrs[virtualSublogIdx].UpdateMaxSequenceNumber(sequenceNumber);

            // Pause this replay thread when it has run ahead of an active round's target, bounding
            // drift from the lagging sublogs. Fast path is a single Volatile.Read + compare when no
            // round is active.
            replayBarrier.CheckAndWait(virtualSublogIdx, vsrs[virtualSublogIdx].Max);

            // Publish the key's sketch entry AFTER the park (deferred-KRT order): while parked, a
            // reader touching this key still sees its previous value and advances its session
            // sequence number only to that value, never to the just-published frontier.
            vsrs[virtualSublogIdx].UpdateKeySequenceNumber(keyHash, sequenceNumber);
        }

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
                replicaReadSessionContext.ResetCachedSublogMax();
            }
        }

        /// <summary>
        /// Verify key freshness before allowing reads.
        /// </summary>
        /// <param name="keyHash"></param>
        /// <param name="replicaReadSessionContext"></param>
        /// <param name="timeout"></param>
        /// <param name="ct"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void VerifyKeyFreshness(long keyHash, ref ReplicaReadSessionContext replicaReadSessionContext, TimeSpan timeout, CancellationToken ct)
        {
            var virtualSublogIdx = appendOnlyFile.Log.GetVirtualSublogIdx(keyHash);
            var initOrSameSublog = replicaReadSessionContext.lastVirtualSublogIdx == -1 || replicaReadSessionContext.lastVirtualSublogIdx == virtualSublogIdx;
            var mssn = replicaReadSessionContext.maximumSessionSequenceNumber;

            // Here we have to wait for replay to catch up
            // Don't have to wait if reading from same sublog or maximumSessionTimestamp is behind the sublog frontier timestamp
            if (!initOrSameSublog && mssn >= replicaReadSessionContext.cachedSublogMax[virtualSublogIdx])
            {
                // Refresh cached view
                var sketchMaxValue = vsrs[virtualSublogIdx].Max;
                replicaReadSessionContext.cachedSublogMax[virtualSublogIdx] = sketchMaxValue;

                // Optimistic check without lock
                if (mssn >= sketchMaxValue)
                {
                    // About to wait. If the replay-side drift is large enough to be worth bounding, install a barrier round
                    BoundReplayDrift();

                    vsrs[virtualSublogIdx].WaitForSequenceNumber(mssn, timeout, ct);
                    // Refresh after wait
                    replicaReadSessionContext.cachedSublogMax[virtualSublogIdx] = vsrs[virtualSublogIdx].Max;
                }
            }

            // Store for future update
            replicaReadSessionContext.lastVirtualSublogIdx = (short)virtualSublogIdx;
            replicaReadSessionContext.lastHash = keyHash;
        }

        /// <summary>
        /// Scan all virtual sublogs' current max sequence numbers; if the spread exceeds
        /// <see cref="replayDriftThreshold"/>, install a barrier round at the leader's value so that
        /// replayers pause once they reach it and the laggards have time to catch up.
        /// Only invoked on the slow path (when the reader is about to actually wait).
        /// </summary>
        void BoundReplayDrift()
        {
            if (!driftBoundingEnabled) return;
            // A round already in progress is bounding the drift; skip the scan.
            if (replayBarrier.IsActive) return;

            var virtualSublogCount = serverOptions.AofVirtualSublogCount;
            long minFrontier = long.MaxValue, maxFrontier = long.MinValue;
            for (var v = 0; v < virtualSublogCount; v++)
            {
                var frontier = vsrs[v].Max;
                if (frontier < minFrontier) minFrontier = frontier;
                if (frontier > maxFrontier) maxFrontier = frontier;
            }
            if (maxFrontier - minFrontier <= replayDriftThreshold) return;
            replayBarrier.TryActivate(maxFrontier);
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
        /// <param name="timeout"></param>
        /// <param name="ct"></param>
        public void PreSingleKeyConsistentRead(long hash, ref ReplicaReadSessionContext replicaReadSessionContext, TimeSpan timeout, CancellationToken ct)
        {
            // Check version
            CheckConsistencyManagerVersion(ref replicaReadSessionContext);

            // Verify key freshness
            VerifyKeyFreshness(hash, ref replicaReadSessionContext, timeout, ct);
        }

        /// <summary>
        /// This method implements part of the consistent read protocol for a single key when shared AOF is enabled.
        /// NOTE:
        ///     This method is used to update the log sequence number after store.Read was processed.
        ///     This is done to ensure that the log sequence number tracked by the ReadConsistencyManager is an overestimate of the actual sequence number since
        ///     we cannot be certain at prepare phase what is the actual sequence number.
        /// </summary>
        /// <param name="replicaReadSessionContext"></param>
        public void PostSingleKeyConsistentRead(ref ReplicaReadSessionContext replicaReadSessionContext)
        {
            replicaReadSessionContext.maximumSessionSequenceNumber = Math.Max(
                replicaReadSessionContext.maximumSessionSequenceNumber, GetKeySequenceNumber(replicaReadSessionContext.lastHash));
        }

        /// <summary>
        /// Verify key freshness and keep track hash and maximum session sequence number to check for updates after batch read.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="batchReadContext"></param>
        /// <param name="timeout"></param>
        /// <param name="ct"></param>
        /// <param name="hash"></param>
        public void PreBatchKeyConsistentRead(ReadOnlySpan<byte> key, ref ReplicaReadSessionContext batchReadContext, TimeSpan timeout, CancellationToken ct, out long hash)
        {
            // Verify key freshness
            hash = GarnetLog.HASH(key);
            VerifyKeyFreshness(hash, ref batchReadContext, timeout, ct);

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
        public bool PostBatchKeyConsistentReadValidate(long hash, ref ReplicaReadSessionContext batchReadContext)
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