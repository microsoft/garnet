// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;

namespace Garnet.cluster
{
    /// <summary>
    /// Extension methods for <see cref="SessionParseState"/>.
    /// </summary>
    public static class SessionParseStateExtensions
    {
        /// <summary>
        /// Parse slot state from parse state at specified index
        /// </summary>
        /// <param name="parseState">The parse state</param>
        /// <param name="idx">The argument index</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        public static bool TryGetSlotState(this SessionParseState parseState, int idx, out SlotState value)
        {
            value = default;
            var sbArg = parseState.GetArgSliceByRef(idx).ReadOnlySpan;

            if (sbArg.EqualsUpperCaseSpanIgnoringCase("OFFLINE"u8))
                value = SlotState.OFFLINE;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("STABLE"u8))
                value = SlotState.STABLE;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("MIGRATING"u8))
                value = SlotState.MIGRATING;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("IMPORTING"u8))
                value = SlotState.IMPORTING;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("FAIL"u8))
                value = SlotState.FAIL;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("NODE"u8))
                value = SlotState.NODE;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("INVALID"u8))
                value = SlotState.INVALID;
            else return false;

            return true;
        }

        /// <summary>
        /// Parse failover option from parse state at specified index
        /// </summary>
        /// <param name="parseState">The parse state</param>
        /// <param name="idx">The argument index</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        public static bool TryGetFailoverOption(this SessionParseState parseState, int idx, out FailoverOption value)
        {
            value = default;
            var sbArg = parseState.GetArgSliceByRef(idx).ReadOnlySpan;

            if (sbArg.EqualsUpperCaseSpanIgnoringCase("DEFAULT"u8))
                value = FailoverOption.DEFAULT;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("INVALID"u8))
                value = FailoverOption.INVALID;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("TO"u8))
                value = FailoverOption.TO;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("FORCE"u8))
                value = FailoverOption.FORCE;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("ABORT"u8))
                value = FailoverOption.ABORT;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("TIMEOUT"u8))
                value = FailoverOption.TIMEOUT;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("TAKEOVER"u8))
                value = FailoverOption.TAKEOVER;
            else return false;

            return true;
        }
    }
}