// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Equality comparer for <see cref="SpanByte"/>
    /// </summary>
    public struct SpanByteComparer : IKeyComparer
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly SpanByteComparer Instance = new();

        /// <inheritdoc />
        public readonly unsafe long GetHashCode64(SpanByte spanByte) => StaticGetHashCode64(spanByte);

        /// <summary>
        /// Get 64-bit hash code
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe long StaticGetHashCode64(SpanByte spanByte)
        {
            if (spanByte.Serialized)
            {
                var ptr = (byte*)Unsafe.AsPointer(ref spanByte);
                return Utility.HashBytes(ptr + sizeof(int), spanByte.Length);
            }
            else
            {
                var ptr = (byte*)spanByte.Pointer;
                return Utility.HashBytes(ptr, spanByte.Length);
            }
        }

        /// <inheritdoc />
        public readonly unsafe bool Equals(SpanByte k1, SpanByte k2) => StaticEquals(k1, k2);

        /// <summary>
        /// Equality comparison
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe bool StaticEquals(SpanByte k1, SpanByte k2)
        {
            return k1.AsReadOnlySpan().SequenceEqual(k2.AsReadOnlySpan())
                && (k1.MetadataSize == k2.MetadataSize);
        }
    }
}