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

        /// <summary>
        /// Iterative verify slot for a single key
        /// </summary>
        /// <remarks>
        /// Holds the calling thread while a key is being migrated. It runs from a custom transaction procedure's
        /// <c>Prepare</c>, which is user code in the middle of a call and cannot be replayed after a wait.
        /// </remarks>
        /// <param name="keySlice"></param>
        /// <param name="readOnly"></param>
        /// <param name="SessionAsking"></param>
        /// <returns></returns>
        public bool NetworkIterativeSlotVerify(PinnedSpanByte keySlice, bool readOnly, byte SessionAsking, bool waitForStableSlot)
        {
            ClusterSlotVerificationResult verifyResult;

            // If it is the first verification initialize the result cache
            if (!initialized)
            {
                verifyResult = SingleKeySlotVerify(ref configSnapshot, ref keySlice, readOnly, SessionAsking > 0, waitForStableSlot, canWait: false);
                cachedVerificationResult = verifyResult;
                initialized = true;
                return verifyResult.state == SlotVerifiedState.OK;
            }

            // If slot verification failed return early in order to capture the first error
            if (cachedVerificationResult.state != SlotVerifiedState.OK)
                return false;

            verifyResult = SingleKeySlotVerify(ref configSnapshot, ref keySlice, readOnly, SessionAsking > 0, waitForStableSlot, canWait: false);

            // Check if slot changes between keys
            if (verifyResult.slot != cachedVerificationResult.slot)
            {
                cachedVerificationResult = new(SlotVerifiedState.CROSSSLOT, cachedVerificationResult.slot);
                return false;
            }

            // Check if any key might have moved
            if (verifyResult.state != cachedVerificationResult.state)
            {
                cachedVerificationResult = new(SlotVerifiedState.TRYAGAIN, cachedVerificationResult.slot);
                return false;
            }

            return verifyResult.state == SlotVerifiedState.OK;
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