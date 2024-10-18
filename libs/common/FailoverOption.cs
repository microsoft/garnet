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
    }

    /// <summary>
    /// Extension methods for <see cref="FailoverOption"/>.
    /// </summary>
    public static class FailoverOptionExtensions
    {
        /// <summary>
        /// Validate that the given <see cref="FailoverOption"/> is legal, and _could_ have come from the given <see cref="ReadOnlySpan{T}"/>.
        /// 
        /// TODO: Long term we can kill this and use <see cref="IUtf8SpanParsable{ClientType}"/> instead of <see cref="Enum.TryParse{TEnum}(string?, bool, out TEnum)"/>
        /// and avoid extra validation.  See: https://github.com/dotnet/runtime/issues/81500 .
        /// </summary>
        public static bool IsValid(this FailoverOption type, ReadOnlySpan<byte> fromSpan)
        {
            return type != FailoverOption.DEFAULT && type != FailoverOption.INVALID && Enum.IsDefined(type) && !fromSpan.ContainsAnyInRange((byte)'0', (byte)'9');
        }
    }
}