// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Text;

namespace Garnet.common
{
    /// <summary>
    /// Failover option flags
    /// </summary>
    public enum FailoverOption : byte
    {
        /// <summary>
        /// Internal use only
        /// </summary>
        DEFAULT,
        /// <summary>
        /// Internal use only
        /// </summary>
        INVALID,

        /// <summary>
        /// Failover endpoint input marker
        /// </summary>
        TO,
        /// <summary>
        /// Force failover flag
        /// </summary>
        FORCE,
        /// <summary>
        /// Issue abort of ongoing failover
        /// </summary>
        ABORT,
        /// <summary>
        /// Timeout marker
        /// </summary>
        TIMEOUT,
        /// <summary>
        /// Issue takeover without consensus to replica
        /// </summary>
        TAKEOVER
    }

    /// <summary>
    /// Utils for info command
    /// </summary>
    public static class FailoverUtils
    {
        static readonly byte[][] infoSections = Enum.GetValues<FailoverOption>()
            .Select(x => Encoding.ASCII.GetBytes($"${x.ToString().Length}\r\n{x}\r\n")).ToArray();

        /// <summary>
        /// Return resp formatted failover option
        /// </summary>
        /// <param name="failoverOption"></param>
        /// <returns></returns>
        public static byte[] GetRespFormattedFailoverOption(FailoverOption failoverOption)
            => infoSections[(int)failoverOption];

        /// <summary>
        /// Parse failover option from span
        /// </summary>
        /// <param name="input">ReadOnlySpan input to parse</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        public static bool TryParseFailoverOption(ReadOnlySpan<byte> input, out FailoverOption value)
        {
            value = default;

            if (input.EqualsUpperCaseSpanIgnoringCase("DEFAULT"u8))
                value = FailoverOption.DEFAULT;
            else if (input.EqualsUpperCaseSpanIgnoringCase("INVALID"u8))
                value = FailoverOption.INVALID;
            else if (input.EqualsUpperCaseSpanIgnoringCase("TO"u8))
                value = FailoverOption.TO;
            else if (input.EqualsUpperCaseSpanIgnoringCase("FORCE"u8))
                value = FailoverOption.FORCE;
            else if (input.EqualsUpperCaseSpanIgnoringCase("ABORT"u8))
                value = FailoverOption.ABORT;
            else if (input.EqualsUpperCaseSpanIgnoringCase("TIMEOUT"u8))
                value = FailoverOption.TIMEOUT;
            else if (input.EqualsUpperCaseSpanIgnoringCase("TAKEOVER"u8))
                value = FailoverOption.TAKEOVER;
            else return false;

            return true;
        }
    }
}