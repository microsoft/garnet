// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Serializer for SpanByte. Used only on server-side.
    /// </summary>
    public sealed unsafe class SpanByteKeySerializer : IKeyInputSerializer<SpanByte, SpanByte>
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref SpanByte ReadInputByRef(ref byte* src)
        {
            ref var ret = ref Unsafe.AsRef<SpanByte>(src);
            src += ret.TotalSize;
            return ref ret;
        }

        /// <inheritdoc />
        public bool Match(ref SpanByte k, bool asciiKey, ref SpanByte pattern, bool asciiPattern)
        {
            if (asciiKey && asciiPattern)
            {
                return GlobUtils.Match(pattern.ToPointer(), pattern.LengthWithoutMetadata, k.ToPointer(), k.LengthWithoutMetadata);
            }

            if (pattern.LengthWithoutMetadata > k.LengthWithoutMetadata)
                return false;
            return pattern.AsReadOnlySpan().SequenceEqual(k.AsReadOnlySpan().Slice(0, pattern.LengthWithoutMetadata));
        }
    }
}