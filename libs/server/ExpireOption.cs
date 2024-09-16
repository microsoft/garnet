// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    /// <summary>
    /// Expire option
    /// </summary>
    public enum ExpireOption : byte
    {
        /// <summary>
        /// None
        /// </summary>
        None = 0,
        /// <summary>
        /// Set expiry only when the key has no expiry
        /// </summary>
        NX = 1 << 0,
        /// <summary>
        /// Set expiry only when the key has an existing expiry 
        /// </summary>
        XX = 1 << 1,
        /// <summary>
        /// Set expiry only when the new expiry is greater than current one
        /// </summary>
        GT = 1 << 2,
        /// <summary>
        /// Set expiry only when the new expiry is less than current one
        /// </summary>
        LT = 1 << 3,
        /// <summary>
        /// Set expiry only when the key has an existing expiry and the new expiry is greater than current one
        /// </summary>
        XXGT = XX | GT,
        /// <summary>
        /// Set expiry only when the key has an existing expiry and the new expiry is less than current one
        /// </summary>
        XXLT = XX | LT,
    }

    /// <summary>
    /// Extension methods for <see cref="ExpireOption"/>.
    /// </summary>
    public static class ExpireOptionExtensions
    {
        /// <summary>
        /// Validate that the given <see cref="ExpireOption"/> is legal, and _could_ have come from the given <see cref="ArgSlice"/>.
        /// 
        /// TODO: Long term we can kill this and use <see cref="IUtf8SpanParsable{ClientType}"/> instead of <see cref="Enum.TryParse{TEnum}(string?, bool, out TEnum)"/>
        /// and avoid extra validation.  See: https://github.com/dotnet/runtime/issues/81500 .
        /// </summary>
        public static bool IsValid(this ExpireOption type, ref ArgSlice fromSlice)
        {
            return type != ExpireOption.None && Enum.IsDefined(type) && !fromSlice.ReadOnlySpan.ContainsAnyInRange((byte)'0', (byte)'9');
        }
    }
}