// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    using BasicGarnetApi = GarnetApi<BasicContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>,
        BasicContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>,
        BasicContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions,
            /* UnifiedStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>>;

    /// <summary>
    /// Cluster manager
    /// </summary>
    internal sealed partial class ClusterManager : IDisposable
    {
        /// <summary>
        /// Try to add slots to local worker
        /// </summary>
        /// <param name="slots">Slot list</param>
        /// <param name="slotAssigned">Slot number of already assigned slot</param>
        /// <returns>True on success, false otherwise</returns>
        public bool TryAddSlots(HashSet<int> slots, out int slotAssigned)
        {
            slotAssigned = -1;
            while (true)
            {
                var current = currentConfig;
                if (current.NumWorkers == 0) return false;

                if (!current.TryAddSlots(slots, out var slot, out var newConfig))
                {
                    slotAssigned = slot;
                    return false;
                }
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
            logger?.LogTrace("[Processed] AddSlots {slots}", GetRange([.. slots]));
            return true;
        }

        /// <summary>
        /// Try to remove ownership of slots. Slot state transition to OFFLINE.
        /// </summary>
        /// <param name="slots">Slot list</param>
        /// <param name="notLocalSlot">The slot number that is not local.</param>
        /// <returns><see langword="false"/> if a slot provided is not local; otherwise <see langword="true"/>.</returns>
        public bool TryRemoveSlots(HashSet<int> slots, out int notLocalSlot)
        {
            notLocalSlot = -1;

            while (true)
            {
                var current = currentConfig;
                if (current.NumWorkers == 0) return false;

                if (!current.TryRemoveSlots(slots, out var slot, out var newConfig) &&
                    slot != -1)
                {
                    notLocalSlot = slot;
                    return false;
                }
                newConfig = newConfig.BumpLocalNodeConfigEpoch();
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
            logger?.LogTrace("[Processed] RemoveSlots {slots}", GetRange([.. slots]));
            return true;
        }

        /// <summary>
        /// Try to prepare node for migration of slot to node with specified node Id.
        /// </summary>
        /// <param name="slot">Slot to change state</param>
        /// <param name="nodeid">Migration target node-id</param>
        /// <param name="errorMessage">Error message</param>
        /// <returns>True on success, false otherwise</returns>
        public bool TryPrepareSlotForMigration(int slot, string nodeid, out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = default;
            while (true)
            {
                var current = currentConfig;
                var migratingWorkerId = current.GetWorkerIdFromNodeId(nodeid);

                if (migratingWorkerId == 0)
                {
                    errorMessage = Encoding.ASCII.GetBytes($"ERR I don't know about node {nodeid}");
                    return false;
                }

                if (current.LocalNodeId.Equals(nodeid, StringComparison.OrdinalIgnoreCase))
                {
                    errorMessage = CmdStrings.RESP_ERR_GENERIC_MIGRATE_TO_MYSELF;
                    return false;
                }

                if (current.GetNodeRoleFromNodeId(nodeid) != NodeRole.PRIMARY)
                {
                    errorMessage = Encoding.ASCII.GetBytes($"ERR Target node {nodeid} is not a master node.");
                    return false;
                }

                if (!current.IsLocal((ushort)slot))
                {
                    errorMessage = Encoding.ASCII.GetBytes($"ERR I'm not the owner of hash slot {slot}");
                    return false;
                }

                if (current.GetState((ushort)slot) != SlotState.STABLE)
                {
                    var _migratingNodeId = current.GetNodeIdFromSlot((ushort)slot);
                    errorMessage = Encoding.ASCII.GetBytes($"ERR Slot already scheduled for migration from {_migratingNodeId}");
                    return false;
                }

                // Slot is conditionally assigned to target node
                // Redirection logic should be aware of this and not consider this slot as part of target node until migration completes
                // Cluster status queries should also be aware of this implicit assignment and return this node as the current owner
                // The above is only true for the primary that owns this slot and this configuration change is not propagated through gossip.
                var newConfig = current.UpdateSlotState(slot, migratingWorkerId, SlotState.MIGRATING);
                if (newConfig == null)
                {
                    errorMessage = CmdStrings.RESP_ERR_GENERIC_SLOTSTATE_TRANSITION;
                    return false;
                }

                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
            logger?.LogTrace("[Processed] SetSlot MIGRATING {slot} TO {nodeId}", slot, nodeid);
            return true;
        }

        /// <summary>
        /// Try to change list of slots to migrating state
        /// </summary>
        /// <param name="slots">Slot list</param>
        /// <param name="nodeid">Migration target node-id</param>
        /// <param name="errorMessage">Error message</param>
        /// <returns>True on success, false otherwise</returns>
        public bool TryPrepareSlotsForMigration(HashSet<int> slots, string nodeid, out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = default;
            while (true)
            {
                var current = currentConfig;
                var migratingWorkerId = current.GetWorkerIdFromNodeId(nodeid);

                // Check migrating worker is a known valid worker
                if (migratingWorkerId == 0)
                {
                    errorMessage = Encoding.ASCII.GetBytes($"ERR I don't know about node {nodeid}");
                    return false;
                }

                // Check if node-id is different from local node
                if (current.LocalNodeId.Equals(nodeid, StringComparison.OrdinalIgnoreCase))
                {
                    errorMessage = CmdStrings.RESP_ERR_GENERIC_MIGRATE_TO_MYSELF;
                    return false;
                }

                // Check if local node is primary
                if (current.GetNodeRoleFromNodeId(nodeid) != NodeRole.PRIMARY)
                {
                    errorMessage = Encoding.ASCII.GetBytes($"ERR Target node {nodeid} is not a master node.");
                    return false;
                }

                foreach (var slot in slots)
                {
                    // Check if slot is owned by local node
                    if (!current.IsLocal((ushort)slot))
                    {
                        errorMessage = Encoding.ASCII.GetBytes($"ERR I'm not the owner of hash slot {slot}");
                        return false;
                    }

                    // Check node state is stable
                    if (current.GetState((ushort)slot) != SlotState.STABLE)
                    {
                        var _migratingNodeId = current.GetNodeIdFromSlot((ushort)slot);
                        errorMessage = Encoding.ASCII.GetBytes($"ERR Slot already scheduled for migration from {_migratingNodeId}");
                        return false;
                    }
                }

                // Slot is conditionally assigned to target node
                // Redirection logic should be aware of this and not consider this slot as part of target node until migration completes
                // Cluster status queries should also be aware of this implicit assignment and return this node as the current owner
                // The above is only true for the primary that owns this slot and this configuration change is not propagated through gossip.
                var newConfig = current.UpdateMultiSlotState(slots, migratingWorkerId, SlotState.MIGRATING);
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
            logger?.LogTrace("[Processed] SetSlotsRange MIGRATING {slot} TO {nodeId}", GetRange([.. slots]), nodeid);
            return true;
        }

        /// <summary>
        /// Try to prepare node for import of slot from node with specified nodeid.
        /// </summary>
        /// <param name="slot">Slot list</param>
        /// <param name="nodeid">Importing source node-id</param>
        /// <param name="errorMessage">Error message</param>
        /// <returns>True on success, false otherwise</returns>       
        public bool TryPrepareSlotForImport(int slot, string nodeid, out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = default;
            while (true)
            {
                var current = currentConfig;
                var importingWorkerId = current.GetWorkerIdFromNodeId(nodeid);
                if (importingWorkerId == 0)
                {
                    errorMessage = Encoding.ASCII.GetBytes($"ERR I don't know about node {nodeid}");
                    return false;
                }

                if (current.LocalNodeRole != NodeRole.PRIMARY)
                {
                    errorMessage = Encoding.ASCII.GetBytes($"ERR Importing node {current.LocalNodeRole} is not a master node.");
                    return false;
                }

                if (current.IsLocal((ushort)slot, readWriteSession: false))
                {
                    errorMessage = Encoding.ASCII.GetBytes($"ERR This is a local hash slot {slot} and is already imported");
                    return false;
                }

                var sourceNodeId = current.GetNodeIdFromSlot((ushort)slot);
                if (sourceNodeId == null || !sourceNodeId.Equals(nodeid, StringComparison.OrdinalIgnoreCase))
                {
                    errorMessage = Encoding.ASCII.GetBytes($"ERR Slot {slot} is not owned by {nodeid}");
                    return false;
                }

                if (current.GetState((ushort)slot) != SlotState.STABLE)
                {
                    errorMessage = Encoding.ASCII.GetBytes($"ERR Slot already scheduled for import from {nodeid}");
                    return false;
                }

                var newConfig = current.UpdateSlotState(slot, importingWorkerId, SlotState.IMPORTING);
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
            logger?.LogTrace("[Processed] SetSlot IMPORTING {slot} TO {nodeId}", slot, nodeid);
            return true;
        }

        /// <summary>
        /// Try to prepare node for import of slots from node with specified nodeid.
        /// </summary>
        /// <param name="slots">Slot list</param>
        /// <param name="nodeid">Migration target node-id</param>
        /// <param name="errorMessage">Error message</param>
        /// <returns>True on success, false otherwise</returns>
        public bool TryPrepareSlotsForImport(HashSet<int> slots, string nodeid, out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = default;
            while (true)
            {
                var current = currentConfig;
                var importingWorkerId = current.GetWorkerIdFromNodeId(nodeid);
                // Check importing nodeId is valid
                if (importingWorkerId == 0)
                {
                    errorMessage = Encoding.ASCII.GetBytes($"ERR I don't know about node {nodeid}");
                    return false;
                }

                // Check local node is a primary
                if (current.LocalNodeRole != NodeRole.PRIMARY)
                {
                    errorMessage = Encoding.ASCII.GetBytes($"ERR Importing node {current.LocalNodeRole} is not a master node.");
                    return false;
                }

                // Check validity of slots
                foreach (var slot in slots)
                {
                    // Can only import remote slots
                    if (current.IsLocal((ushort)slot, readWriteSession: false))
                    {
                        errorMessage = Encoding.ASCII.GetBytes($"ERR This is a local hash slot {slot} and is already imported");
                        return false;
                    }

                    // Check if node is owned by node
                    var sourceNodeId = current.GetNodeIdFromSlot((ushort)slot);
                    if (sourceNodeId == null || !sourceNodeId.Equals(nodeid, StringComparison.OrdinalIgnoreCase))
                    {
                        errorMessage = Encoding.ASCII.GetBytes($"ERR Slot {slot} is not owned by {nodeid}");
                        return false;
                    }

                    // Check if slot is in stable state
                    if (current.GetState((ushort)slot) != SlotState.STABLE)
                    {
                        errorMessage = Encoding.ASCII.GetBytes($"ERR Slot already scheduled for import from {nodeid}");
                        return false;
                    }
                }

                var newConfig = current.UpdateMultiSlotState(slots, importingWorkerId, SlotState.IMPORTING);
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
            logger?.LogTrace("[Processed] SetSlotsRange IMPORTING {slot} TO {nodeId}", GetRange([.. slots]), nodeid);
            return true;
        }

        /// <summary>
        /// Try to change ownership of slot to node.
        /// </summary>
        /// <param name="slot">Slot list</param>
        /// <param name="nodeid">Importing source node-id</param>
        /// <param name="errorMesage">Error message</param>
        /// <returns>True on success, false otherwise</returns>
        public bool TryPrepareSlotForOwnershipChange(int slot, string nodeid, out ReadOnlySpan<byte> errorMesage)
        {
            errorMesage = default;
            var current = currentConfig;
            var workerId = current.GetWorkerIdFromNodeId(nodeid);
            if (workerId == 0)
            {
                errorMesage = Encoding.ASCII.GetBytes($"ERR I don't know about node {nodeid}");
                return false;
            }

            if (current.GetState((ushort)slot) is SlotState.MIGRATING)
            {
                while (true)
                {
                    current = currentConfig;
                    workerId = current.GetWorkerIdFromNodeId(nodeid);
                    var newConfig = current.UpdateSlotState(slot, workerId, SlotState.STABLE);

                    if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                        break;
                }
                FlushConfig();
                logger?.LogTrace("[Processed] SetSlot {slot} MIGRATED TO {nodeId}", slot, nodeid);
                return true;
            }
            else if (current.GetState((ushort)slot) is SlotState.IMPORTING)
            {
                if (!current.LocalNodeId.Equals(nodeid, StringComparison.OrdinalIgnoreCase))
                {
                    errorMesage = Encoding.ASCII.GetBytes($"ERR Input nodeid {nodeid} different from local nodeid {CurrentConfig.LocalNodeId}.");
                    return false;
                }

                while (true)
                {
                    current = currentConfig;
                    var newConfig = current.UpdateSlotState(slot, 1, SlotState.STABLE).BumpLocalNodeConfigEpoch();

                    if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                        break;
                }
                logger?.LogWarning("Bumped Epoch ({LocalNodeConfigEpoch}) [{LocalIp}:{LocalPort},{LocalNodeId}]", currentConfig.LocalNodeConfigEpoch, currentConfig.LocalNodeIp, currentConfig.LocalNodePort, currentConfig.LocalNodeIdShort);
                FlushConfig();
                return true;
            }
            else
            {
                while (true)
                {
                    current = currentConfig;
                    workerId = current.GetWorkerIdFromNodeId(nodeid);
                    var newConfig = current.UpdateSlotState(slot, workerId, SlotState.STABLE);

                    if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                        break;
                }
                FlushConfig();
                logger?.LogTrace("[Processed] SetSlot {slot} FORCED TO {nodeId}", slot, nodeid);
            }
            return true;
        }

        /// <summary>
        /// Try to change ownership of slots to node.
        /// </summary>
        /// <param name="slots">SLot list</param>
        /// <param name="nodeid">The id of the new owner node.</param>
        /// <param name="errorMessage">Error message</param>
        /// <returns>True on success, false otherwise</returns>
        public bool TryPrepareSlotsForOwnershipChange(HashSet<int> slots, string nodeid, out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = default;
            while (true)
            {
                var current = currentConfig;
                var workerId = current.GetWorkerIdFromNodeId(nodeid);
                if (workerId == 0)
                {
                    errorMessage = Encoding.ASCII.GetBytes($"ERR I don't know about node {nodeid}");
                    return false;
                }

                var newConfig = current.UpdateMultiSlotState(slots, workerId, SlotState.STABLE);
                if (current.LocalNodeId.Equals(nodeid, StringComparison.OrdinalIgnoreCase)) newConfig = newConfig.BumpLocalNodeConfigEpoch();
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            logger?.LogWarning("Bumped Epoch ({LocalNodeConfigEpoch}) [{LocalIp}:{LocalPort},{LocalNodeId}]", currentConfig.LocalNodeConfigEpoch, currentConfig.LocalNodeIp, currentConfig.LocalNodePort, currentConfig.LocalNodeIdShort);
            FlushConfig();
            return true;
        }

        /// <summary>
        /// Reset slot state to <see cref="SlotState.STABLE"/>
        /// </summary>
        /// <param name="slot">Slot id to reset state</param>
        public void TryResetSlotState(int slot)
        {
            var current = currentConfig;
            var slotState = current.GetState((ushort)slot);
            if (slotState is SlotState.MIGRATING or SlotState.IMPORTING)
            {
                while (true)
                {
                    current = currentConfig;
                    slotState = current.GetState((ushort)slot);
                    var workerId = slotState == SlotState.MIGRATING ? 1 : current.GetWorkerIdFromSlot((ushort)slot);
                    var newConfig = current.UpdateSlotState(slot, workerId, SlotState.STABLE);
                    if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                        break;
                }
                FlushConfig();
            }
        }

        /// <summary>
        /// Reset local slot state to <see cref="SlotState.STABLE"/>
        /// </summary>
        /// <param name="slots">Slot list</param>
        public void TryResetSlotState(HashSet<int> slots)
        {
            while (true)
            {
                var current = currentConfig;
                var newConfig = current.ResetMultiSlotState(slots);
                if (Interlocked.CompareExchange(ref currentConfig, newConfig, current) == current)
                    break;
            }
            FlushConfig();
        }

        /// <summary>
        /// Methods used to cleanup keys for given slot collection
        /// </summary>
        /// <param name="basicGarnetApi"></param>
        /// <param name="slots">Slot list</param>
        public static unsafe void DeleteKeysInSlots(BasicGarnetApi basicGarnetApi, HashSet<int> slots)
        {
            using var iter = basicGarnetApi.IterateStore();
            while (iter.GetNext())
            {
                var key = iter.Key;
                var s = HashSlotUtils.HashSlot(key);
                if (slots.Contains(s))
                    _ = basicGarnetApi.DELETE(PinnedSpanByte.FromPinnedSpan(key));
            }
        }
    }
}