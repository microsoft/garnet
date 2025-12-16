// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;

namespace Tsavorite.core
{
    /// <summary>
    /// ArgSlice Comparer. This implements <see cref="IEqualityComparer{PinnedSpanByte}"/>, not <see cref="IKeyComparer"/>,
    /// as it is not used for Tsavorite keys directly (they are converted to <see cref="ReadOnlySpan{_byte_}"/>).
    /// </summary>
    public sealed class PinnedSpanByteComparer : IEqualityComparer<PinnedSpanByte>
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly PinnedSpanByteComparer Instance = new();

        /// <inheritdoc />
        public bool Equals(PinnedSpanByte x, PinnedSpanByte y) => x.Equals(y);

        /// <inheritdoc />
        public unsafe int GetHashCode([DisallowNull] PinnedSpanByte obj)
        {
            fixed (byte* ptr = obj.Span)
                return (int)HashBytes(ptr, obj.Length);
        }

        static unsafe long HashBytes(byte* pbString, int len)
        {
            const long magicno = 40343;
            char* pwString = (char*)pbString;
            int cbBuf = len / 2;
            ulong hashState = (ulong)len;

            for (int i = 0; i < cbBuf; i++, pwString++)
                hashState = magicno * hashState + *pwString;

            if ((len & 1) > 0)
            {
                byte* pC = (byte*)pwString;
                hashState = magicno * hashState + *pC;
            }

            return (long)BitOperations.RotateRight(magicno * hashState, 4);
        }
    }
}