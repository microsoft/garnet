// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Garnet.cluster
{
    /// <summary>
    /// Slot state.
    /// Persisted in the cluster config (ClusterConfigSerializer) and gossiped, so values are
    /// EXPLICIT and APPEND-ONLY: never change/reorder an existing value; add new states at the end.
    /// </summary>
    public enum SlotState : byte
    {
        // IMPORTANT: Any changes to the values of this enum should be reflected in its parser (SessionParseStateExtensions.TryGetSlotState)

        /// <summary>
        /// Slot not assigned
        /// </summary>
        OFFLINE = 0x0,
        /// <summary>
        /// Slot assigned and ready to be used.
        /// </summary>
        STABLE = 0x1,
        /// <summary>
        /// Slot is being moved to another node.
        /// </summary>
        MIGRATING = 0x2,
        /// <summary>
        /// Reverse of migrating, preparing node to receive commands for that slot.
        /// </summary>
        IMPORTING = 0x3,
        /// <summary>
        /// Slot in FAIL state.
        /// </summary>
        FAIL = 0x4,
        /// <summary>
        /// Not a slot state. Used with SETSLOT
        /// </summary>
        NODE = 0x5,
        /// <summary>
        /// Invalid slot state
        /// </summary>
        INVALID = 0x6,
    }

    /// <summary>
    /// Hashslot info
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    public struct HashSlot
    {
        /// <summary>
        /// WorkerId of slot owner.
        /// </summary>
        [FieldOffset(0)]
        public ushort _workerId;

        /// <summary>
        /// State of this slot.
        /// </summary>
        [FieldOffset(2)]
        public SlotState _state;

        /// <summary>
        /// Slot in migrating state points to target node though still owned by local node until migration completes.
        /// </summary>
        public ushort workerId => _state == SlotState.MIGRATING ? (ushort)1 : _workerId;
    }
}