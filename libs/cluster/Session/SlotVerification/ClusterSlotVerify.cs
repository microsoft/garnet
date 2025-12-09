// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Garnet.server;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool Exists(ref ArgSlice keySlice)
            => basicGarnetApi.EXISTS(keySlice, StoreType.All) == GarnetStatus.OK;

        private bool CheckIfKeyExists(byte[] key)
        {
            fixed (byte* keyPtr = key)
            {
                var keySlice = new ArgSlice(keyPtr, key.Length);
                return Exists(ref keySlice);
            }
        }

        private ClusterSlotVerificationResult SingleKeySlotVerify(ref ClusterConfig config, ref ArgSlice keySlice, bool readOnly, byte SessionAsking, bool waitForStableSlot, int slot = -1)
        {
            Debug.Assert(!waitForStableSlot || (waitForStableSlot && !readOnly), "Shouldn't see Vector Set writes and readonly at same time");

            var ret = readOnly ? SingleKeyReadSlotVerify(ref config, ref keySlice) : SingleKeyReadWriteSlotVerify(waitForStableSlot, ref config, ref keySlice);

            return ret;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ClusterSlotVerificationResult SingleKeyReadSlotVerify(ref ClusterConfig config, ref ArgSlice keySlice)
            {
                var _slot = slot == -1 ? ArgSliceUtils.HashSlot(ref keySlice) : (ushort)slot;
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
                        SlotState.IMPORTING => SessionAsking > 0 ? new(SlotVerifiedState.OK, _slot) : new(SlotVerifiedState.MOVED, _slot), // If it is in importing state serve request only if asking flag is set else redirect
                        _ => new(SlotVerifiedState.CLUSTERDOWN, _slot) // If not local and any other state respond with CLUSTERDOWN
                    };
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ClusterSlotVerificationResult SingleKeyReadWriteSlotVerify(bool waitForStableSlot, ref ClusterConfig config, ref ArgSlice keySlice)
            {
                var _slot = slot == -1 ? ArgSliceUtils.HashSlot(ref keySlice) : (ushort)slot;

            tryAgain:
                var IsLocal = config.IsLocal(_slot, readWriteSession: readWriteSession);
                var state = config.GetState(_slot);

                if (waitForStableSlot && state is SlotState.IMPORTING or SlotState.MIGRATING)
                {
                    WaitForSlotToStabalize(_slot, ref keySlice, ref config);
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
                        SlotState.IMPORTING => SessionAsking > 0 ? new(SlotVerifiedState.OK, _slot) : new(SlotVerifiedState.MOVED, _slot), // If it is in importing state serve request only if asking flag is set else redirect
                        _ => new(SlotVerifiedState.CLUSTERDOWN, _slot) // If not local and any other state respond with CLUSTERDOWN
                    };
                }
            }

            bool CanOperateOnKey(ref ArgSlice key, int slot, bool readOnly)
            {
                // For both read and read/write ops we need to ensure that key will not be removed
                // while we try to operate on it so we will delay the corresponding operation
                // as long as the key is being actively migrated
                while (!clusterProvider.migrationManager.CanAccessKey(ref key, slot, readOnly))
                {
                    ReleaseCurrentEpoch();
                    Thread.Yield();
                    AcquireCurrentEpoch();
                }
                return Exists(ref key);
            }

            void WaitForSlotToStabalize(ushort slot, ref ArgSlice keySlice, ref ClusterConfig config)
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

        ClusterSlotVerificationResult MultiKeySlotVerify(ClusterConfig config, ref Span<ArgSlice> keys, bool readOnly, byte sessionAsking, bool waitForStableSlot, int count)
        {
            var _end = count < 0 ? keys.Length : count;
            var slot = ArgSliceUtils.HashSlot(ref keys[0]);
            var verifyResult = SingleKeySlotVerify(ref config, ref keys[0], readOnly, sessionAsking, waitForStableSlot, slot);

            for (var i = 1; i < _end; i++)
            {
                var _slot = ArgSliceUtils.HashSlot(ref keys[i]);
                var _verifyResult = SingleKeySlotVerify(ref config, ref keys[i], readOnly, sessionAsking, waitForStableSlot, _slot);

                // Check if slot changes between keys
                if (_slot != slot)
                    return new(SlotVerifiedState.CROSSSLOT, slot);

                // Check if state of key changes
                if (_verifyResult.state != verifyResult.state)
                    return new(SlotVerifiedState.TRYAGAIN, slot);
            }

            return verifyResult;
        }

        ClusterSlotVerificationResult MultiKeySlotVerify(ClusterConfig config, ref SessionParseState parseState, ref ClusterSlotVerificationInput csvi)
        {
            ref var key = ref parseState.GetArgSliceByRef(csvi.firstKey);
            var slot = ArgSliceUtils.HashSlot(ref key);
            var verifyResult = SingleKeySlotVerify(ref config, ref key, csvi.readOnly, csvi.sessionAsking, csvi.waitForStableSlot, slot);
            var secondKey = csvi.firstKey + csvi.step;

            for (var i = secondKey; i < csvi.lastKey; i += csvi.step)
            {
                if (csvi.keyNumOffset == i)
                    continue;
                key = ref parseState.GetArgSliceByRef(i);
                var _slot = ArgSliceUtils.HashSlot(ref key);
                var _verifyResult = SingleKeySlotVerify(ref config, ref key, csvi.readOnly, csvi.sessionAsking, csvi.waitForStableSlot, _slot);

                // Check if slot changes between keys
                if (_slot != slot)
                    return new(SlotVerifiedState.CROSSSLOT, slot);

                // Check if any key might have moved
                if (_verifyResult.state != verifyResult.state)
                    return new(SlotVerifiedState.TRYAGAIN, slot);
            }

            return verifyResult;
        }
    }
}