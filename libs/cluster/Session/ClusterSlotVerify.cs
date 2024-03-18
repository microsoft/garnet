// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        /// <summary>
        /// Returns true if key maps to slot not owned by current node otherwise false.
        /// </summary>
        public bool CheckSingleKeySlotVerify(ArgSlice keySlice, bool readOnly, byte SessionAsking)
            => SingleKeySlotVerify(keySlice, readOnly, SessionAsking).state == SlotVerifiedState.OK;

        ClusterSlotVerificationResult SingleKeySlotVerify(ArgSlice keySlice, bool readOnly, byte SessionAsking)
        {
            var config = clusterProvider.clusterManager.CurrentConfig;
            return readOnly ? SingleKeyReadSlotVerify(config, keySlice, SessionAsking) : SingleKeyReadWriteSlotVerify(config, keySlice, SessionAsking);
        }

        ClusterSlotVerificationResult SingleKeyReadSlotVerify(ClusterConfig config, ArgSlice keySlice, byte SessionAsking, int slot = -1)
        {
            var _slot = slot == -1 ? ArgSliceUtils.HashSlot(keySlice) : (ushort)slot;
            bool IsLocal = config.IsLocal(_slot);
            SlotState state = config.GetState(_slot);

            // If local, then slot in not stable state
            if (IsLocal)
            {
                // TODO: make sure other Read locations add this new logic
                if (clusterProvider.replicationManager.recovering)
                {
                    // If we are a replica, let primary handle the request
                    if (config.GetLocalNodeRole() == NodeRole.REPLICA)
                        return new(SlotVerifiedState.MOVED, _slot);
                    else // Else report cluster down
                        return new(SlotVerifiedState.CLUSTERDOWN, _slot);
                }

                if (state == SlotState.STABLE)
                    return new(SlotVerifiedState.OK, _slot);

                //if key migrating and it exists serve read request
                if (state == SlotState.MIGRATING)
                    if (CheckIfKeyExists(keySlice))
                        return new(SlotVerifiedState.OK, _slot);
                    else
                        return new(SlotVerifiedState.ASK, _slot);
                else
                    return new(SlotVerifiedState.CLUSTERDOWN, _slot);
            }
            else
            {
                //if stable state and not local redirect to PRIMARY node
                if (state == SlotState.STABLE)
                    return new(SlotVerifiedState.MOVED, _slot);
                else if (state == SlotState.IMPORTING)
                {
                    //if importing state respond to query only if preceded by asking
                    if (SessionAsking > 0)
                        return new(SlotVerifiedState.OK, _slot);
                    // if importing state and not asking redirect to source node
                    else
                        return new(SlotVerifiedState.MOVED, _slot);
                }
                //if offline respond with clusterdown
                else
                    return new(SlotVerifiedState.CLUSTERDOWN, _slot);
            }
        }

        ClusterSlotVerificationResult SingleKeyReadWriteSlotVerify(ClusterConfig config, ArgSlice keySlice, byte SessionAsking, int slot = -1)
        {
            var _slot = slot == -1 ? ArgSliceUtils.HashSlot(keySlice) : (ushort)slot;
            bool IsLocal = config.IsLocal(_slot, readCommand: readWriteSession);
            SlotState state = config.GetState(_slot);

            //Redirect r/w requests towards primary
            if (config.GetLocalNodeRole() == NodeRole.REPLICA)
                return new(SlotVerifiedState.MOVED, _slot);

            if (IsLocal && state == SlotState.STABLE) return new(SlotVerifiedState.OK, _slot);

            if (IsLocal)
            {
                if (state == SlotState.MIGRATING)
                    //if key migrating and it exists cannot server write request
                    if (CheckIfKeyExists(keySlice))
                        return new(SlotVerifiedState.MIGRATING, _slot);
                    //if key migrating can redirect with ask to target node
                    else
                        return new(SlotVerifiedState.ASK, _slot);
                else
                    return new(SlotVerifiedState.CLUSTERDOWN, _slot);
            }
            else
            {
                //if stable state and not local redirect to PRIMARY node
                if (state == SlotState.STABLE)
                    return new(SlotVerifiedState.MOVED, _slot);
                else if (state == SlotState.IMPORTING)
                {
                    //if importing state respond to query only if preceeded by asking
                    if (SessionAsking > 0)
                        return new(SlotVerifiedState.OK, _slot);
                    // if importing state and not asking redirect to source node
                    else
                        return new(SlotVerifiedState.MOVED, _slot);
                }
                //if offline respond with clusterdown
                else
                    return new(SlotVerifiedState.CLUSTERDOWN, _slot);
            }
        }

        ClusterSlotVerificationResult ArrayCrosslotVerify(int keyCount, ref byte* ptr, byte* endPtr, bool interleavedKeys, out bool retVal, out byte* keyPtr, out int ksize)
        {
            retVal = false;
            bool crossSlot = false;
            keyPtr = null;
            ksize = 0;

            byte* valPtr = null;
            int vsize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, endPtr))
                return new(SlotVerifiedState.OK, 0);

            //skip value if key values are interleaved
            if (interleavedKeys)
                if (!RespReadUtils.ReadPtrWithLengthHeader(ref valPtr, ref vsize, ref ptr, endPtr))
                    return new(SlotVerifiedState.OK, 0);

            var slot = NumUtils.HashSlot(keyPtr, ksize);

            for (int c = 1; c < keyCount; c++)
            {
                keyPtr = null;
                ksize = 0;

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, endPtr))
                    return new(SlotVerifiedState.OK, slot);

                //skip value if key values are interleaved
                if (interleavedKeys)
                    if (!RespReadUtils.ReadPtrWithLengthHeader(ref valPtr, ref vsize, ref ptr, endPtr))
                        return new(SlotVerifiedState.OK, 0);

                var _slot = NumUtils.HashSlot(keyPtr, ksize);
                crossSlot |= (_slot != slot);
            }

            retVal = true;
            if (crossSlot)
                return new(SlotVerifiedState.CROSSLOT, slot);
            else
                return new(SlotVerifiedState.OK, slot);
        }

        ClusterSlotVerificationResult KeyArraySlotVerify(ClusterConfig config, int keyCount, ref byte* ptr, byte* endPtr, bool readOnly, bool interleavedKeys, byte SessionAsking, out bool retVal)
        {
            var vres = ArrayCrosslotVerify(keyCount, ref ptr, endPtr, interleavedKeys, out retVal, out byte* keyPtr, out int ksize);
            if (!retVal) return new(SlotVerifiedState.OK, 0);

            if (vres.state == SlotVerifiedState.CROSSLOT)
                return vres;
            else
                if (readOnly)
                return SingleKeyReadSlotVerify(config, new ArgSlice(keyPtr, ksize), SessionAsking, vres.slot);
            else
                return SingleKeyReadWriteSlotVerify(config, new ArgSlice(keyPtr, ksize), SessionAsking, vres.slot);
        }

        ClusterSlotVerificationResult ArrayCrossSlotVerify(ref ArgSlice[] keys, int count)
        {
            var _offset = 0;
            var _end = count < 0 ? keys.Length : count;

            var slot = ArgSliceUtils.HashSlot(keys[_offset]);
            bool crossSlot = false;
            for (int i = _offset; i < _end; i++)
            {
                var _slot = ArgSliceUtils.HashSlot(keys[i]);

                if (_slot != slot)
                {
                    crossSlot = true;
                    break;
                }
            }

            if (crossSlot)
                return new(SlotVerifiedState.CROSSLOT, slot);
            else
                return new(SlotVerifiedState.OK, slot);
        }

        ClusterSlotVerificationResult KeyArraySlotVerify(ClusterConfig config, ref ArgSlice[] keys, bool readOnly, byte SessionAsking, int count)
        {
            var vres = ArrayCrossSlotVerify(ref keys, count);
            if (vres.state == SlotVerifiedState.CROSSLOT)
                return vres;
            else
            {
                if (readOnly)
                    return SingleKeyReadSlotVerify(config, keys[0], SessionAsking, vres.slot);
                else
                    return SingleKeyReadWriteSlotVerify(config, keys[0], SessionAsking, vres.slot);
            }
        }
    }
}