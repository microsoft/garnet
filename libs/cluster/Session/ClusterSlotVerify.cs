// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using Garnet.common;
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
            => SingleKeySlotVerify(keySlice, readOnly, SessionAsking).state == SlotVerifiedState.OK;

        private ClusterSlotVerificationResult SingleKeySlotVerify(ArgSlice keySlice, bool readOnly, byte SessionAsking)
        {
            var config = clusterProvider.clusterManager.CurrentConfig;
            return readOnly ? SingleKeyReadSlotVerify(config, keySlice, SessionAsking) : SingleKeyReadWriteSlotVerify(config, keySlice, SessionAsking);
        }

        private ClusterSlotVerificationResult SingleKeyReadSlotVerify(ClusterConfig config, ArgSlice keySlice, byte SessionAsking, int slot = -1)
        {
            var _slot = slot == -1 ? ArgSliceUtils.HashSlot(keySlice) : (ushort)slot;
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

        private ClusterSlotVerificationResult SingleKeyReadWriteSlotVerify(ClusterConfig config, ArgSlice keySlice, byte SessionAsking, int slot = -1)
        {
            var _slot = slot == -1 ? ArgSliceUtils.HashSlot(keySlice) : (ushort)slot;
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

        static ClusterSlotVerificationResult ArrayCrosslotVerify(int keyCount, ref byte* ptr, byte* endPtr, bool interleavedKeys, out bool retVal, out byte* keyPtr, out int ksize)
        {
            retVal = false;
            var crossSlot = false;
            keyPtr = null;
            ksize = 0;

            byte* valPtr = null;
            var vsize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, endPtr))
                return new(SlotVerifiedState.OK, 0);

            // Skip value if key values are interleaved
            if (interleavedKeys)
                if (!RespReadUtils.ReadPtrWithLengthHeader(ref valPtr, ref vsize, ref ptr, endPtr))
                    return new(SlotVerifiedState.OK, 0);

            var slot = HashSlotUtils.HashSlot(keyPtr, ksize);

            for (var c = 1; c < keyCount; c++)
            {
                keyPtr = null;
                ksize = 0;

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, endPtr))
                    return new(SlotVerifiedState.OK, slot);

                // Skip value if key values are interleaved
                if (interleavedKeys)
                    if (!RespReadUtils.ReadPtrWithLengthHeader(ref valPtr, ref vsize, ref ptr, endPtr))
                        return new(SlotVerifiedState.OK, 0);

                var _slot = HashSlotUtils.HashSlot(keyPtr, ksize);
                crossSlot |= (_slot != slot);
            }

            retVal = true;
            return crossSlot ? new(SlotVerifiedState.CROSSLOT, slot) : new(SlotVerifiedState.OK, slot);
        }

        ClusterSlotVerificationResult KeyArraySlotVerify(ClusterConfig config, int keyCount, ref byte* ptr, byte* endPtr, bool readOnly, bool interleavedKeys, byte SessionAsking, out bool retVal)
        {
            var vres = ArrayCrosslotVerify(keyCount, ref ptr, endPtr, interleavedKeys, out retVal, out byte* keyPtr, out int ksize);
            if (!retVal) return new(SlotVerifiedState.OK, 0);

            if (vres.state == SlotVerifiedState.CROSSLOT)
                return vres;
            else
            {
                return readOnly
                    ? SingleKeyReadSlotVerify(config, new ArgSlice(keyPtr, ksize), SessionAsking, vres.slot)
                    : SingleKeyReadWriteSlotVerify(config, new ArgSlice(keyPtr, ksize), SessionAsking, vres.slot);
            }
        }

        static ClusterSlotVerificationResult ArrayCrossSlotVerify(ref ArgSlice[] keys, int count)
        {
            var _offset = 0;
            var _end = count < 0 ? keys.Length : count;

            var slot = ArgSliceUtils.HashSlot(keys[_offset]);
            var crossSlot = false;
            for (var i = _offset; i < _end; i++)
            {
                var _slot = ArgSliceUtils.HashSlot(keys[i]);

                if (_slot != slot)
                {
                    crossSlot = true;
                    break;
                }
            }

            return crossSlot
                ? new(SlotVerifiedState.CROSSLOT, slot)
                : new(SlotVerifiedState.OK, slot);
        }

        ClusterSlotVerificationResult KeyArraySlotVerify(ClusterConfig config, ref ArgSlice[] keys, bool readOnly, byte SessionAsking, int count)
        {
            var vres = ArrayCrossSlotVerify(ref keys, count);
            if (vres.state == SlotVerifiedState.CROSSLOT)
                return vres;
            else
            {
                return readOnly
                    ? SingleKeyReadSlotVerify(config, keys[0], SessionAsking, vres.slot)
                    : SingleKeyReadWriteSlotVerify(config, keys[0], SessionAsking, vres.slot);
            }
        }
    }
}