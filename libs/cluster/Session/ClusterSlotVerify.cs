﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.server;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        private bool CanOperateOnKey(int slot, ArgSlice key, bool readOnly)
        {
            // For both read and read/write ops we need to ensure that key will not be removed
            // while we try to operate on it so we will delay the corresponding operation
            // as long as the key is being actively migrated
            while (!clusterProvider.migrationManager.CanModifyKey(slot, key, readOnly))
            {
                ReleaseCurrentEpoch();
                Thread.Yield();
                AcquireCurrentEpoch();
            }
            return CheckIfKeyExists(key);
        }

        private bool CheckIfKeyExists(byte[] key)
        {
            fixed (byte* keyPtr = key)
                return CheckIfKeyExists(new ArgSlice(keyPtr, key.Length));
        }

        private bool CheckIfKeyExists(ArgSlice keySlice)
            => basicGarnetApi.EXISTS(keySlice, StoreType.All) == GarnetStatus.OK;

        /// <summary>
        /// Checks if the given key maps to a slot owned by this node.
        /// </summary>
        /// <param name="keySlice"></param>
        /// <param name="readOnly"></param>
        /// <param name="SessionAsking"></param>
        /// <returns>True if key maps to slot not owned by current node otherwise false.</returns>
        public bool CheckSingleKeySlotVerify(ArgSlice keySlice, bool readOnly, byte SessionAsking)
        {
            var config = clusterProvider.clusterManager.CurrentConfig;
            return SingleKeySlotVerify(config, ref keySlice, readOnly, SessionAsking).state == SlotVerifiedState.OK;
        }

        private ClusterSlotVerificationResult SingleKeySlotVerify(ClusterConfig config, ref ArgSlice keySlice, bool readOnly, byte SessionAsking, int slot = -1)
        {
            return readOnly ? SingleKeyReadSlotVerify(config, ref keySlice, SessionAsking, slot) : SingleKeyReadWriteSlotVerify(config, ref keySlice, SessionAsking, slot);

            ClusterSlotVerificationResult SingleKeyReadSlotVerify(ClusterConfig config, ref ArgSlice keySlice, byte SessionAsking, int slot = -1)
            {
                var _slot = slot == -1 ? ArgSliceUtils.HashSlot(ref keySlice) : (ushort)slot;
                var IsLocal = config.IsLocal(_slot);
                var state = config.GetState(_slot);

                // If local check we can serve request or redirect with ask
                if (IsLocal)
                {
                    // Only if a node IsLocal we might be able to serve a request.
                    // So we check here if the node is recovering
                    if (clusterProvider.replicationManager.Recovering)
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
                        SlotState.MIGRATING => CanOperateOnKey(slot, keySlice, readOnly: true) ? new(SlotVerifiedState.OK, _slot) : new(SlotVerifiedState.ASK, _slot), // Can serve request only if key exists
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

            ClusterSlotVerificationResult SingleKeyReadWriteSlotVerify(ClusterConfig config, ref ArgSlice keySlice, byte SessionAsking, int slot = -1)
            {
                var _slot = slot == -1 ? ArgSliceUtils.HashSlot(ref keySlice) : (ushort)slot;
                var IsLocal = config.IsLocal(_slot, readCommand: readWriteSession);
                var state = config.GetState(_slot);

                // Redirect r/w requests towards primary
                if (config.LocalNodeRole == NodeRole.REPLICA)
                    return new(SlotVerifiedState.MOVED, _slot);

                if (IsLocal)
                {
                    if (clusterProvider.replicationManager.Recovering)
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
                        SlotState.MIGRATING => CanOperateOnKey(slot, keySlice, readOnly: false) ? new(SlotVerifiedState.OK, _slot) : new(SlotVerifiedState.ASK, _slot), // Can serve request only if key exists
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
        }

        ClusterSlotVerificationResult MultiKeySlotVerify(ClusterConfig config, ref Span<ArgSlice> keys, bool readOnly, byte sessionAsking, int count)
        {
            var _end = count < 0 ? keys.Length : count;
            var slot = ArgSliceUtils.HashSlot(ref keys[0]);
            var verifyResult = SingleKeySlotVerify(config, ref keys[0], readOnly, sessionAsking);

            for (var i = 1; i < _end; i++)
            {
                var _slot = ArgSliceUtils.HashSlot(ref keys[i]);
                var _verifyResult = SingleKeySlotVerify(config, ref keys[i], readOnly, sessionAsking);

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
            var verifyResult = SingleKeySlotVerify(config, ref key, csvi.readOnly, csvi.sessionAsking, slot);
            var stride = csvi.firstKey + csvi.step;

            for (var i = stride; i < csvi.lastKey; i += stride)
            {
                key = ref parseState.GetArgSliceByRef(i);
                var _slot = ArgSliceUtils.HashSlot(ref key);
                var _verifyResult = SingleKeySlotVerify(config, ref key, csvi.readOnly, csvi.sessionAsking, slot);

                // Check if slot changes between keys
                if (_slot != slot)
                    return new(SlotVerifiedState.CROSSSLOT, slot);

                // Check if state of key changes
                if (_verifyResult.state != verifyResult.state)
                    return new(SlotVerifiedState.TRYAGAIN, slot);
            }

            return verifyResult;
        }
    }
}