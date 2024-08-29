// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;

namespace Garnet.cluster
{
    /// <summary>
    /// NodeRole identifier
    /// </summary>
    public enum SlotState : byte
    {
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
    /// Extension methods for <see cref="SlotState"/>.
    /// </summary>
    public static class SloteStateExtensions
    {
        /// <summary>
        /// Validate that the given <see cref="SlotState"/> is legal, and _could_ have come from the given <see cref="ReadOnlySpan{T}"/>.
        /// 
        /// TODO: Long term we can kill this and use <see cref="IUtf8SpanParsable{ClientType}"/> instead of <see cref="Enum.TryParse{TEnum}(string?, bool, out TEnum)"/>
        /// and avoid extra validation.  See: https://github.com/dotnet/runtime/issues/81500 .
        /// </summary>
        public static bool IsValid(this SlotState type, ReadOnlySpan<byte> fromSpan)
        {
            return type != SlotState.INVALID && type != SlotState.OFFLINE && Enum.IsDefined(type) && !fromSpan.ContainsAnyInRange((byte)'0', (byte)'9');
        }
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