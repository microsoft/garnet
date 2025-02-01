// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Serializer for SpanByte.
    /// </summary>
    public sealed unsafe class SpanByteKeySerializer
    {
        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref SpanByte ReadByRef(ref byte* src)
        {
            ref var ret = ref Unsafe.AsRef<SpanByte>(src);
            src += ret.TotalSize;
            return ref ret;
        }

        public void Skip(ref byte* src)
        {
            src += Unsafe.AsRef<SpanByte>(src).TotalSize;
        }

        /// <inheritdoc />
        public bool Match(ref SpanByte k, ref SpanByte pattern)
        {
            return GlobUtils.Match(pattern.ToPointer(), pattern.LengthWithoutMetadata, k.ToPointer(), k.LengthWithoutMetadata);
        }
    }
}