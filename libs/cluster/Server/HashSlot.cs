// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;
using Garnet.common;
using Garnet.server;

namespace Garnet.cluster
{
    /// <summary>
    /// NodeRole identifier
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
        STABLE,
        /// <summary>
        /// Slot is being moved to another node.
        /// </summary>
        MIGRATING,
        /// <summary>
        /// Reverse of migrating, preparing node to receive commands for that slot.
        /// </summary>
        IMPORTING,
        /// <summary>
        /// Slot in FAIL state.
        /// </summary>
        FAIL,
        /// <summary>
        /// Not a slot state. Used with SETSLOT
        /// </summary>
        NODE,
        /// <summary>
        /// Invalid slot state
        /// </summary>
        INVALID,
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