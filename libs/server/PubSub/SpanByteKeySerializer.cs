// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Serializer for SpanByte.
    /// </summary>
    public sealed unsafe class SpanByteKeySerializer : IKeySerializer
    {
        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref SpanByte ReadKeyByRef(ref byte* src)
        {
            ref var ret = ref Unsafe.AsRef<SpanByte>(src);
            src += ret.TotalSize;
            return ref ret;
        }

        /// <inheritdoc />
        public bool Match(SpanByte k, bool asciiKey, SpanByte pattern, bool asciiPattern)
        {
            if (asciiKey && asciiPattern)
            {
                return GlobUtils.Match(pattern.ToPointer(), pattern.Length, k.ToPointer(), k.Length);
            }

            if (pattern.Length > k.Length)
                return false;
            return pattern.AsReadOnlySpan().SequenceEqual(k.AsReadOnlySpan().Slice(0, pattern.Length));
        }
    }
}