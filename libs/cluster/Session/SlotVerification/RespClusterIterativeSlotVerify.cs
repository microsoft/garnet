// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        ClusterSlotVerificationResult cachedVerificationResult;
        ClusterConfig configSnapshot;
        bool initialized;

        /// <summary>
        /// Reset cache slot verification result
        /// </summary>
        public void ResetCachedSlotVerificationResult()
        {
            configSnapshot = clusterProvider.clusterManager.CurrentConfig;
            initialized = false;
        }

        /// <inheritdoc/>
        public bool NetworkIterativeSlotVerify(PinnedSpanByte keySlice, ref ClusterSlotVerificationInput csvi)
        {
            ClusterSlotVerificationResult verifyResult;

            // If it is the first verification initialize the result cache
            if (!initialized)
            {
                verifyResult = SingleKeySlotVerify(ref configSnapshot, ref keySlice, csvi.readOnly, csvi.sessionAsking > 0, csvi.waitForStableSlot, csvi.canWait);
                cachedVerificationResult = verifyResult;
                initialized = true;
                return Verified(verifyResult.state, ref csvi);
            }

            // If slot verification failed return early in order to capture the first error
            if (cachedVerificationResult.state != SlotVerifiedState.OK)
                return false;

            verifyResult = SingleKeySlotVerify(ref configSnapshot, ref keySlice, csvi.readOnly, csvi.sessionAsking > 0, csvi.waitForStableSlot, csvi.canWait);

            // Check if slot changes between keys
            if (verifyResult.slot != cachedVerificationResult.slot)
            {
                cachedVerificationResult = new(SlotVerifiedState.CROSSSLOT, cachedVerificationResult.slot);
                return false;
            }

            if (verifyResult.state is SlotVerifiedState.WAIT or SlotVerifiedState.OFFLOAD)
            {
                cachedVerificationResult = verifyResult;
                return Verified(verifyResult.state, ref csvi);
            }

            // Check if any key might have moved
            if (verifyResult.state != cachedVerificationResult.state)
            {
                cachedVerificationResult = new(SlotVerifiedState.TRYAGAIN, cachedVerificationResult.slot);
                return false;
            }

            return verifyResult.state == SlotVerifiedState.OK;

            static bool Verified(SlotVerifiedState state, ref ClusterSlotVerificationInput csvi)
            {
                csvi.mustWait = state == SlotVerifiedState.WAIT;
                csvi.mustOffload = state == SlotVerifiedState.OFFLOAD;
                return state == SlotVerifiedState.OK;
            }
        }

        /// <summary>
        /// Write cached slot verification message to output
        /// </summary>
        /// <param name="output"></param>
        public void WriteCachedSlotVerificationMessage(ref MemoryResult<byte> output)
        {
            if (cachedVerificationResult.state != SlotVerifiedState.OK)
            {
                var errorMessage = GetSlotVerificationMessage(configSnapshot, cachedVerificationResult);
                _ = RespWriteUtils.TryWriteError(errorMessage, ref output);
            }
        }
    }
}