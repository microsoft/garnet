// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    using BasicGarnetApi = GarnetApi<BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions>, BasicContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>>;

    /// <summary>
    /// Cluster manager
    /// </summary>
    internal sealed partial class ClusterManager : IDisposable
    {
        /// <summary>
        /// Add slots to local worker
        /// </summary>
        /// <param name="slots"></param>
        /// <param name="slotAssigned"></param>
        /// <returns></returns>
        public bool TryAddSlots(List<int> slots, out int slotAssigned)
        {
            slotAssigned = -1;
            while (true)
            {
                var current = currentConfig;
                if (current.NumWorkers == 0) return false;

                var newConfig = currentConfig.AddSlots(slots, out var slot);
                if (slot != -1)
                {
                    slotAssigned = slot;
                    return false;
                }
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
            logger?.LogTrace("ADD SLOTS {slots}", GetRange(slots));
            return true;
        }

        /// <summary>
        /// Remove ownernship of slots. Slot state transition to OFFLINE.
        /// </summary>
        /// <param name="slots"></param>
        /// <param name="notLocalSlot"></param>
        /// <returns></returns>
        public bool TryRemoveSlots(List<int> slots, out int notLocalSlot)
        {
            notLocalSlot = -1;

            while (true)
            {
                var current = currentConfig;
                if (current.NumWorkers == 0) return false;

                var newConfig = currentConfig.RemoveSlots(slots, out var slot);
                if (slot != -1)
                {
                    notLocalSlot = slot;
                    return false;
                }
                newConfig = newConfig.BumpLocalNodeConfigEpoch();
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
            logger?.LogTrace("REMOVE SLOTS {slots}", String.Join(",", slots));
            return true;
        }

        /// <summary>
        /// Prepare node for migration of slot to node with specified node Id.
        /// </summary>
        /// <param name="slot"></param>
        /// <param name="nodeid"></param>
        /// <param name="resp"></param>
        /// <returns></returns>
        public bool PrepareSlotForMigration(int slot, string nodeid, out ReadOnlySpan<byte> resp)
        {
            while (true)
            {
                var current = currentConfig;
                int migratingWorkerId = current.GetWorkerIdFromNodeId(nodeid);

                if (migratingWorkerId == 0)
                {
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR I don't know about node {nodeid}\r\n"));
                    return false;
                }

                if (current.GetLocalNodeId().Equals(nodeid))
                {
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes("-ERR Can't MIGRATE to myself\r\n"));
                    return false;
                }

                if (current.GetNodeRoleFromNodeId(nodeid) != NodeRole.PRIMARY)
                {
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Target node {nodeid} is not a master node.\r\n"));
                    return false;
                }

                if (!current.IsLocal((ushort)slot))
                {
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR I'm not the owner of hash slot {slot}\r\n"));
                    return false;
                }

                if (current.GetState((ushort)slot) != SlotState.STABLE)
                {
                    var _migratingNodeId = current.GetNodeIdFromSlot((ushort)slot);
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Slot already scheduled for migration from {_migratingNodeId}\r\n"));
                    return false;
                }

                // Slot is conditionally assigned to target node
                // Redirection logic should be aware of this and not consider this slot as part of target node until migration completes
                // Cluster status queries should also be aware of this implicit assignment and return this node as the current owner
                // The above is only true for the primary that owns this slot and this configuration change is not propagated through gossip.
                var newConfig = current.UpdateSlotState(slot, migratingWorkerId, SlotState.MIGRATING);
                if (newConfig == null)
                {
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Slot already in that state.\r\n"));
                    return false;
                }

                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();

            logger?.LogInformation("MIGRATE {slot} TO {currentConfig.GetWorkerAddressFromNodeId(nodeid)}", slot, currentConfig.GetWorkerAddressFromNodeId(nodeid));
            resp = CmdStrings.RESP_OK;
            return true;
        }

        /// <summary>
        /// Change list of slots to migrating state
        /// </summary>
        /// <param name="slots"></param>
        /// <param name="nodeid"></param>
        /// <param name="resp"></param>
        /// <returns></returns>
        public bool PrepareSlotsForMigration(HashSet<int> slots, string nodeid, out ReadOnlySpan<byte> resp)
        {
            while (true)
            {
                var current = currentConfig;
                int migratingWorkerId = current.GetWorkerIdFromNodeId(nodeid);

                //Check migrating worker is a known valid worker
                if (migratingWorkerId == 0)
                {
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR I don't know about node {nodeid}\r\n"));
                    return false;
                }

                //Check if nodeid is different from local node
                if (current.GetLocalNodeId().Equals(nodeid))
                {
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes("-ERR Can't MIGRATE to myself\r\n"));
                    return false;
                }

                //Check if local node is primary
                if (current.GetNodeRoleFromNodeId(nodeid) != NodeRole.PRIMARY)
                {
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Target node {nodeid} is not a master node.\r\n"));
                    return false;
                }

                foreach (var slot in slots)
                {
                    //Check if slot is owned by local node
                    if (!current.IsLocal((ushort)slot))
                    {
                        resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR I'm not the owner of hash slot {slot}\r\n"));
                        return false;
                    }

                    //Check node state is stable
                    if (current.GetState((ushort)slot) != SlotState.STABLE)
                    {
                        var _migratingNodeId = current.GetNodeIdFromSlot((ushort)slot);
                        resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Slot already scheduled for migration from {_migratingNodeId}\r\n"));
                        return false;
                    }
                }

                // Slot is conditionally assigned to target node
                // Redirection logic should be aware of this and not consider this slot as part of target node until migration completes
                // Cluster status queries should also be aware of this implicit assignment and return this node as the current owner
                // The above is only true for the primary that owns this slot and this configuration change is not propagated through gossip.
                var newConfig = currentConfig.UpdateMultiSlotState(slots, migratingWorkerId, SlotState.MIGRATING);
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();

            logger?.LogInformation("MIGRATE {slot} TO {migrating node}", String.Join(' ', slots), currentConfig.GetWorkerAddressFromNodeId(nodeid));
            resp = CmdStrings.RESP_OK;
            return true;
        }

        /// <summary>
        /// Prepare node for import of slot from node with specified nodeid.
        /// </summary>
        /// <param name="slot"></param>
        /// <param name="nodeid"></param>
        /// <param name="resp"></param>
        /// <returns></returns>       
        public bool PrepareSlotForImport(int slot, string nodeid, out ReadOnlySpan<byte> resp)
        {
            while (true)
            {
                var current = currentConfig;
                int importingWorkerId = current.GetWorkerIdFromNodeId(nodeid);
                if (importingWorkerId == 0)
                {
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR I don't know about node {nodeid}\r\n"));
                    return false;
                }

                if (current.GetLocalNodeRole() != NodeRole.PRIMARY)
                {
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Importing node {current.GetLocalNodeRole()} is not a master node.\r\n"));
                    return false;
                }

                if (current.IsLocal((ushort)slot, readCommand: false))
                {
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR This is a local hash slot {slot} and is already imported\r\n"));
                    return false;
                }

                string sourceNodeId = current.GetNodeIdFromSlot((ushort)slot);
                if (sourceNodeId == null || !sourceNodeId.Equals(nodeid))
                {
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Slot {slot} is not owned by {nodeid}\r\n"));
                    return false;
                }

                if (current.GetState((ushort)slot) != SlotState.STABLE)
                {
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Slot already scheduled for import from {nodeid}\r\n"));
                    return false;
                }

                var newConfig = current.UpdateSlotState(slot, importingWorkerId, SlotState.IMPORTING);
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();

            logger?.LogInformation("IMPORT {slot} FROM {currentConfig.GetWorkerAddressFromNodeId(nodeid)}", slot, currentConfig.GetWorkerAddressFromNodeId(nodeid));
            resp = CmdStrings.RESP_OK;
            return true;
        }

        /// <summary>
        /// Prepare node for import of slots from node with specified nodeid.
        /// </summary>
        /// <param name="slots"></param>
        /// <param name="nodeid"></param>
        /// <param name="resp"></param>
        /// <returns></returns>
        public bool PrepareSlotsForImport(HashSet<int> slots, string nodeid, out ReadOnlySpan<byte> resp)
        {
            resp = CmdStrings.RESP_OK;
            while (true)
            {
                var current = currentConfig;
                int importingWorkerId = current.GetWorkerIdFromNodeId(nodeid);
                //Check importing nodeId is valid
                if (importingWorkerId == 0)
                {
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR I don't know about node {nodeid}\r\n"));
                    return false;
                }

                //Check local node is a primary
                if (current.GetLocalNodeRole() != NodeRole.PRIMARY)
                {
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Importing node {current.GetLocalNodeRole()} is not a master node.\r\n"));
                    return false;
                }

                //Check validity of slots
                foreach (var slot in slots)
                {
                    //can only import remote slots
                    if (current.IsLocal((ushort)slot, readCommand: false))
                    {
                        resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR This is a local hash slot {slot} and is already imported\r\n"));
                        return false;
                    }

                    //check if node is owned by node
                    string sourceNodeId = current.GetNodeIdFromSlot((ushort)slot);
                    if (sourceNodeId == null || !sourceNodeId.Equals(nodeid))
                    {
                        resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Slot {slot} is not owned by {nodeid}\r\n"));
                        return false;
                    }

                    //check if slot is in stable state
                    if (current.GetState((ushort)slot) != SlotState.STABLE)
                    {
                        resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Slot already scheduled for import from {nodeid}\r\n"));
                        return false;
                    }
                }

                var newConfig = currentConfig.UpdateMultiSlotState(slots, importingWorkerId, SlotState.IMPORTING);
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }

            logger?.LogInformation("IMPORT {slot} FROM {importingNode}", String.Join(' ', slots), currentConfig.GetWorkerAddressFromNodeId(nodeid));
            resp = CmdStrings.RESP_OK;
            return true;
        }

        /// <summary>
        /// Change ownership of slot to node.
        /// </summary>
        /// <param name="slot"></param>
        /// <param name="nodeid"></param>
        /// <param name="resp"></param>
        /// <returns></returns>
        public bool PrepareSlotForOwnershipChange(int slot, string nodeid, out ReadOnlySpan<byte> resp)
        {
            resp = CmdStrings.RESP_OK;
            var current = currentConfig;
            int workerId = current.GetWorkerIdFromNodeId(nodeid);
            if (workerId == 0)
            {
                resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR I don't know about node {nodeid}\r\n"));
                return false;
            }

            if (current.GetState((ushort)slot) == SlotState.MIGRATING)
            {
                while (true)
                {
                    current = currentConfig;
                    workerId = current.GetWorkerIdFromNodeId(nodeid);
                    var newConfig = currentConfig.UpdateSlotState(slot, workerId, SlotState.STABLE);

                    if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                        break;
                }
                FlushConfig();
                logger?.LogInformation("SLOT {slot} IMPORTED TO {nodeid}", slot, currentConfig.GetWorkerAddressFromNodeId(nodeid));
                return true;
            }
            else if (current.GetState((ushort)slot) == SlotState.IMPORTING)
            {
                if (!current.GetLocalNodeId().Equals(nodeid))
                {
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Input nodeid {nodeid} different from local nodeid {CurrentConfig.GetLocalNodeId()}.\r\n"));
                    return false;
                }

                while (true)
                {
                    current = currentConfig;
                    var newConfig = currentConfig.UpdateSlotState(slot, 1, SlotState.STABLE);
                    newConfig = newConfig.BumpLocalNodeConfigEpoch();

                    if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                        break;
                }
                FlushConfig();
                logger?.LogInformation("SLOT {slot} IMPORTED FROM {nodeid}", slot, currentConfig.GetWorkerAddressFromNodeId(nodeid));
                return true;
            }
            return true;
        }

        /// <summary>
        /// Change ownership of slots to node.
        /// </summary>
        /// <param name="slots"></param>
        /// <param name="nodeid"></param>
        /// <param name="resp"></param>
        /// <returns></returns>
        public bool PrepareSlotsForOwnershipChange(HashSet<int> slots, string nodeid, out ReadOnlySpan<byte> resp)
        {
            resp = CmdStrings.RESP_OK;
            while (true)
            {
                var current = currentConfig;
                int workerId = current.GetWorkerIdFromNodeId(nodeid);
                if (workerId == 0)
                {
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR I don't know about node {nodeid}\r\n"));
                    return false;
                }

                var newConfig = currentConfig.UpdateMultiSlotState(slots, workerId, SlotState.STABLE);
                if (current.GetLocalNodeId().Equals(nodeid)) newConfig = newConfig.BumpLocalNodeConfigEpoch();
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }

            FlushConfig();
            logger?.LogInformation("SLOT {slot} IMPORTED TO {endpoint}", slots, currentConfig.GetWorkerAddressFromNodeId(nodeid));
            return true;
        }

        /// <summary>
        /// Reset slot state to stable
        /// </summary>
        /// <param name="slot"></param>       
        /// <param name="resp"></param>
        /// <returns></returns>
        public bool ResetSlotState(int slot, out ReadOnlySpan<byte> resp)
        {
            resp = CmdStrings.RESP_OK;
            var current = currentConfig;
            var slotState = current.GetState((ushort)slot);
            if (slotState == SlotState.MIGRATING || slotState == SlotState.IMPORTING)
            {
                while (true)
                {
                    current = currentConfig;
                    slotState = current.GetState((ushort)slot);
                    var workerId = slotState == SlotState.MIGRATING ? 1 : currentConfig.GetWorkerIdFromSlot((ushort)slot);
                    var newConfig = currentConfig.UpdateSlotState(slot, workerId, SlotState.STABLE);
                    if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                        break;
                }
                FlushConfig();
            }
            return true;
        }

        /// <summary>
        /// Reset local slot state to stable
        /// </summary>
        /// <param name="slots"></param>
        /// <param name="resp"></param>
        /// <returns></returns>
        public bool ResetSlotsState(HashSet<int> slots, out ReadOnlySpan<byte> resp)
        {
            resp = CmdStrings.RESP_OK;
            foreach (var slot in slots)
                if (!ResetSlotState(slot, out resp))
                    return false;
            return true;
        }

        /// <summary>
        /// Check if slot is in importing state.
        /// </summary>
        /// <param name="slot"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsImporting(ushort slot) => currentConfig.GetState(slot) == SlotState.IMPORTING;

        /// <summary>
        /// Methods used to cleanup keys for given slot collection in main store
        /// </summary>
        /// <param name="BasicGarnetApi"></param>
        /// <param name="slots"></param>
        public unsafe void DeleteKeysInSlotsFromMainStore(BasicGarnetApi BasicGarnetApi, HashSet<int> slots)
        {
            using var iter = BasicGarnetApi.IterateMainStore();
            while (iter.GetNext(out var recordInfo))
            {
                ref SpanByte key = ref iter.GetKey();
                var s = NumUtils.HashSlot(key.ToPointer(), key.Length);
                if (slots.Contains(s))
                    BasicGarnetApi.DELETE(ref key, StoreType.Main);
            }
        }

        /// <summary>
        /// Methods used to cleanup keys for given slot collection in object store
        /// </summary>
        /// <param name="BasicGarnetApi"></param>
        /// <param name="slots"></param>
        public unsafe void DeleteKeysInSlotsFromObjectStore(BasicGarnetApi BasicGarnetApi, HashSet<int> slots)
        {
            using var iterObject = BasicGarnetApi.IterateObjectStore();
            while (iterObject.GetNext(out var recordInfo))
            {
                ref var key = ref iterObject.GetKey();
                ref var value = ref iterObject.GetValue();
                var s = NumUtils.HashSlot(key);
                if (slots.Contains(s))
                    BasicGarnetApi.DELETE(key, StoreType.Object);
            }
        }
    }
}