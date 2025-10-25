// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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

        private ClusterSlotVerificationResult SingleKeySlotVerify(ref ClusterConfig config, ref PinnedSpanByte keySlice, bool readOnly, byte SessionAsking, int slot = -1)
        {
            return readOnly ? SingleKeyReadSlotVerify(ref config, ref keySlice) : SingleKeyReadWriteSlotVerify(ref config, ref keySlice);

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
                        SlotState.IMPORTING => SessionAsking > 0 ? new(SlotVerifiedState.OK, _slot) : new(SlotVerifiedState.MOVED, _slot), // If it is in importing state serve request only if asking flag is set else redirect
                        _ => new(SlotVerifiedState.CLUSTERDOWN, _slot) // If not local and any other state respond with CLUSTERDOWN
                    };
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ClusterSlotVerificationResult SingleKeyReadWriteSlotVerify(ref ClusterConfig config, ref PinnedSpanByte keySlice)
            {
                var _slot = slot == -1 ? HashSlotUtils.HashSlot(keySlice) : (ushort)slot;
                var IsLocal = config.IsLocal(_slot, readWriteSession: readWriteSession);
                var state = config.GetState(_slot);

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
        }

        ClusterSlotVerificationResult MultiKeySlotVerify(ClusterConfig config, ref Span<PinnedSpanByte> keys, bool readOnly, byte sessionAsking, int count)
        {
            var _end = count < 0 ? keys.Length : count;
            var slot = HashSlotUtils.HashSlot(keys[0]);
            var verifyResult = SingleKeySlotVerify(ref config, ref keys[0], readOnly, sessionAsking, slot);

            for (var i = 1; i < _end; i++)
            {
                var _slot = HashSlotUtils.HashSlot(keys[i]);
                var _verifyResult = SingleKeySlotVerify(ref config, ref keys[i], readOnly, sessionAsking, _slot);

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
            var slot = HashSlotUtils.HashSlot(key);
            var verifyResult = SingleKeySlotVerify(ref config, ref key, csvi.readOnly, csvi.sessionAsking, slot);
            var secondKey = csvi.firstKey + csvi.step;

            for (var i = secondKey; i < csvi.lastKey; i += csvi.step)
            {
                if (csvi.keyNumOffset == i)
                    continue;
                key = ref parseState.GetArgSliceByRef(i);
                var _slot = HashSlotUtils.HashSlot(key);
                var _verifyResult = SingleKeySlotVerify(ref config, ref key, csvi.readOnly, csvi.sessionAsking, _slot);

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