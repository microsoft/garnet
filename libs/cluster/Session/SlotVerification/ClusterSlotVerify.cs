// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool Exists(PinnedSpanByte keySlice)
            => basicGarnetApi.EXISTS(keySlice) == GarnetStatus.OK;

        private ClusterSlotVerificationResult SingleKeySlotVerify(ref ClusterConfig config, ref PinnedSpanByte keySlice, bool readOnly, bool SessionAsking, bool waitForStableSlot, int slot = -1)
        {
            Debug.Assert(!waitForStableSlot || (waitForStableSlot && !readOnly), "Shouldn't see Vector Set writes and readonly at same time");

            return readOnly ? SingleKeyReadSlotVerify(ref config, ref keySlice) : SingleKeyReadWriteSlotVerify(waitForStableSlot, ref config, ref keySlice);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ClusterSlotVerificationResult SingleKeyReadSlotVerify(ref ClusterConfig config, ref PinnedSpanByte keySlice)
            {
                var _slot = slot == -1 ? HashSlotUtils.HashSlot(keySlice) : (ushort)slot;
                var IsLocal = config.IsLocal(_slot);
                var state = config.GetState(_slot);

                // If local check we can serve request or redirect with ask
                if (IsLocal)
                {
                    // Only if a node IsLocal we might be able to serve a request.
                    // So we check here if the node is recovering
                    if (clusterProvider.replicationManager.IsRecovering)
                    {
                        return config.LocalNodeRole switch
                        {
                            NodeRole.REPLICA => new(SlotVerifiedState.MOVED, _slot), // If replica is recovering redirect request to primary.
                            NodeRole.PRIMARY => new(SlotVerifiedState.CLUSTERDOWN, _slot), // If primary is recovering slots unavailable, respond with CLUSTERDOWN.
                            NodeRole.UNASSIGNED => new(SlotVerifiedState.CLUSTERDOWN, _slot), // This should never happen, adding only for completeness.
                            _ => new(SlotVerifiedState.CLUSTERDOWN, _slot) // This should never happen, adding only for completeness.
                        };
                    }

                    return state switch
                    {
                        SlotState.STABLE => new(SlotVerifiedState.OK, _slot), // If slot in stable state then serve request
                        SlotState.MIGRATING => CanOperateOnKey(ref keySlice, _slot, readOnly: true) ? new(SlotVerifiedState.OK, _slot) : new(SlotVerifiedState.ASK, _slot), // Can serve request only if key exists
                        _ => new(SlotVerifiedState.CLUSTERDOWN, _slot)
                    };
                }
                else
                {
                    return state switch
                    {
                        SlotState.STABLE => new(SlotVerifiedState.MOVED, _slot), // If local slot in stable state and not local redirect to primary
                        SlotState.IMPORTING => SessionAsking ? new(SlotVerifiedState.OK, _slot) : new(SlotVerifiedState.MOVED, _slot), // If it is in importing state serve request only if asking flag is set else redirect
                        _ => new(SlotVerifiedState.CLUSTERDOWN, _slot) // If not local and any other state respond with CLUSTERDOWN
                    };
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ClusterSlotVerificationResult SingleKeyReadWriteSlotVerify(bool waitForStableSlot, ref ClusterConfig config, ref PinnedSpanByte keySlice)
            {
                var _slot = slot == -1 ? HashSlotUtils.HashSlot(keySlice) : (ushort)slot;

            tryAgain:
                var IsLocal = config.IsLocal(_slot, readWriteSession: readWriteSession);
                var state = config.GetState(_slot);

                if (waitForStableSlot && state is SlotState.IMPORTING or SlotState.MIGRATING)
                {
                    WaitForSlotToStabalize(_slot, keySlice, ref config);
                    goto tryAgain;
                }

                // Redirect r/w requests towards primary
                if (config.LocalNodeRole == NodeRole.REPLICA && !readWriteSession)
                    return new(SlotVerifiedState.MOVED, _slot);

                if (IsLocal)
                {
                    if (clusterProvider.replicationManager.IsRecovering)
                    {
                        return config.LocalNodeRole switch
                        {
                            NodeRole.REPLICA => new(SlotVerifiedState.MOVED, _slot), // Never happens because replica does not serve writes and for this reason IsLocal will always be false
                            NodeRole.PRIMARY => new(SlotVerifiedState.CLUSTERDOWN, _slot), // If primary is recovering slots unavailable, respond with CLUSTERDOWN.
                            NodeRole.UNASSIGNED => new(SlotVerifiedState.CLUSTERDOWN, _slot), // This should never happen, adding only for completeness.
                            _ => new(SlotVerifiedState.CLUSTERDOWN, _slot) // This should never happen, adding only for completeness.
                        };
                    }

                    return state switch
                    {
                        SlotState.STABLE => new(SlotVerifiedState.OK, _slot), // If slot in stable state then serve request
                        SlotState.MIGRATING => CanOperateOnKey(ref keySlice, _slot, readOnly: false) ? new(SlotVerifiedState.OK, _slot) : new(SlotVerifiedState.ASK, _slot), // Can serve request only if key exists
                        _ => new(SlotVerifiedState.CLUSTERDOWN, _slot)
                    };
                }
                else
                {
                    return state switch
                    {
                        SlotState.STABLE => new(SlotVerifiedState.MOVED, _slot), // If local slot in stable state and not local redirect to primary
                        SlotState.IMPORTING => SessionAsking ? new(SlotVerifiedState.OK, _slot) : new(SlotVerifiedState.MOVED, _slot), // If it is in importing state serve request only if asking flag is set else redirect
                        _ => new(SlotVerifiedState.CLUSTERDOWN, _slot) // If not local and any other state respond with CLUSTERDOWN
                    };
                }
            }

            bool CanOperateOnKey(ref PinnedSpanByte key, int slot, bool readOnly)
            {
                // For both read and read/write ops we need to ensure that key will not be removed
                // while we try to operate on it so we will delay the corresponding operation
                // as long as the key is being actively migrated
                while (!clusterProvider.migrationManager.CanAccessKey(key, slot, readOnly))
                {
                    ReleaseCurrentEpoch();
                    Thread.Yield();
                    AcquireCurrentEpoch();
                }
                return Exists(key);
            }


            void WaitForSlotToStabalize(ushort slot, PinnedSpanByte keySlice, ref ClusterConfig config)
            {
                // For Vector Set ops specifically, we need a slot to be stable (or faulted, but not migrating) before writes can proceed
                //
                // This isn't key specific because we can't know the Vector Sets being migrated in advance, only that the slot is moving

                do
                {
                    ReleaseCurrentEpoch();
                    _ = Thread.Yield();
                    AcquireCurrentEpoch();

                    config = clusterProvider.clusterManager.CurrentConfig;
                }
                while (config.GetState(slot) is SlotState.IMPORTING or SlotState.MIGRATING);
            }
        }

        ClusterSlotVerificationResult MultiKeySlotVerify(ClusterConfig config, ref SessionParseState parseState, ref ClusterSlotVerificationInput csvi, bool isTxn, bool waitForStableSlot)
        {
            // Find the first valid key and initialize slot/result
            var specIndex = 0;
            // If slot verification is called from transaction manager, parse state contains consecutive keys so we can skip key search
            (int firstIdx, int lastIdx, int step) searchArgs = isTxn ? (0, parseState.Count - 1, 1) : default;
            while (specIndex < csvi.keySpecs?.Length &&
                   !parseState.TryGetKeySearchArgsFromSimpleKeySpec(csvi.keySpecs[specIndex], csvi.isSubCommand, out searchArgs))
                specIndex++;

            if (specIndex == csvi.keySpecs?.Length && !isTxn)
                return default;

            ref var firstKey = ref parseState.GetArgSliceByRef(searchArgs.firstIdx);
            var firstSlot = HashSlotUtils.HashSlot(firstKey);
            var firstSlotVerifyResult = SingleKeySlotVerify(ref config, ref firstKey, csvi.readOnly, csvi.sessionAsking > 0, waitForStableSlot, firstSlot);

            // Verify remaining keys from the first spec (starting from second key)
            var verifyResult = VerifyKeysInRange(ref config, ref parseState, ref csvi, searchArgs.firstIdx + searchArgs.step,
                searchArgs.lastIdx, searchArgs.step, firstSlot, waitForStableSlot, ref firstSlotVerifyResult);
            if (verifyResult.state != SlotVerifiedState.OK)
                return verifyResult;

            // Verify keys from remaining specs
            for (specIndex++; specIndex < csvi.keySpecs?.Length; specIndex++)
            {
                if (!parseState.TryGetKeySearchArgsFromSimpleKeySpec(csvi.keySpecs[specIndex], csvi.isSubCommand, out searchArgs))
                    continue;

                verifyResult = VerifyKeysInRange(ref config, ref parseState, ref csvi, searchArgs.firstIdx,
                    searchArgs.lastIdx, searchArgs.step, firstSlot, waitForStableSlot, ref firstSlotVerifyResult);
                if (verifyResult.state != SlotVerifiedState.OK)
                    return verifyResult;
            }

            return firstSlotVerifyResult;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ClusterSlotVerificationResult VerifyKeysInRange(ref ClusterConfig config, ref SessionParseState parseState, ref ClusterSlotVerificationInput csvi,
            int startIdx, int lastIdx, int step, ushort firstSlot, bool waitForStableSlot, ref ClusterSlotVerificationResult verifyResult)
        {
            for (var i = startIdx; i <= lastIdx; i += step)
            {
                ref var key = ref parseState.GetArgSliceByRef(i);
                var slot = HashSlotUtils.HashSlot(key);
                var result = SingleKeySlotVerify(ref config, ref key, csvi.readOnly, csvi.sessionAsking > 0, waitForStableSlot, slot);

                if (slot != firstSlot)
                    return new(SlotVerifiedState.CROSSSLOT, firstSlot);
                if (result.state != verifyResult.state)
                    return new(SlotVerifiedState.TRYAGAIN, firstSlot);
            }

            return default;
        }
    }
}