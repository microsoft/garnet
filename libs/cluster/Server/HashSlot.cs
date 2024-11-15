// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using Garnet.common;

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
    /// Utility methods for <see cref="SlotState"/>.
    /// </summary>
    public static class SlotStateUtils
    {
        /// <summary>
        /// Parse slot state from span
        /// </summary>
        /// <param name="input">ReadOnlySpan input to parse</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        public static bool TryParseSlotState(ReadOnlySpan<byte> input, out SlotState value)
        {
            value = default;

            if (input.EqualsUpperCaseSpanIgnoringCase("OFFLINE"u8))
                value = SlotState.OFFLINE;
            else if (input.EqualsUpperCaseSpanIgnoringCase("STABLE"u8))
                value = SlotState.STABLE;
            else if (input.EqualsUpperCaseSpanIgnoringCase("MIGRATING"u8))
                value = SlotState.MIGRATING;
            else if (input.EqualsUpperCaseSpanIgnoringCase("IMPORTING"u8))
                value = SlotState.IMPORTING;
            else if (input.EqualsUpperCaseSpanIgnoringCase("FAIL"u8))
                value = SlotState.FAIL;
            else if (input.EqualsUpperCaseSpanIgnoringCase("NODE"u8))
                value = SlotState.NODE;
            else if (input.EqualsUpperCaseSpanIgnoringCase("INVALID"u8))
                value = SlotState.INVALID;
            else return false;

            return true;
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