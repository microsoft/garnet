// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Serializer for SpanByte. Used only on server-side.
    /// </summary>
    public sealed unsafe class SpanByteServerSerializer : IServerSerializer<SpanByte, SpanByte, SpanByte, SpanByteAndMemory>
    {
        readonly int keyLength;
        readonly int valueLength;

        [ThreadStatic]
        static SpanByteAndMemory output;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="maxKeyLength">Max key length</param>
        /// <param name="maxValueLength">Max value length</param>
        public SpanByteServerSerializer(int maxKeyLength = 512, int maxValueLength = 512)
        {
            keyLength = maxKeyLength;
            valueLength = maxValueLength;
        }

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
        public ref SpanByte ReadValueByRef(ref byte* src)
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
        public bool Write(ref SpanByte k, ref byte* dst, int length)
        {
            if (k.Length > length) return false;

            *(int*)dst = k.Length;
            dst += sizeof(int);
            var dest = new SpanByte(k.Length, (IntPtr)dst);
            k.CopyTo(ref dest);
            dst += k.Length;
            return true;
        }


        /// <inheritdoc />
        public bool Write(ref SpanByteAndMemory k, ref byte* dst, int length)
        {
            if (k.Length > length) return false;

            var dest = new SpanByte(length, (IntPtr)dst);
            if (k.IsSpanByte)
                k.SpanByte.CopyTo(ref dest);
            else
                k.AsMemoryReadOnlySpan().CopyTo(dest.AsSpan());
            return true;
        }

        /// <inheritdoc />
        public ref SpanByteAndMemory AsRefOutput(byte* src, int length)
        {
            output = SpanByteAndMemory.FromFixedSpan(new Span<byte>(src, length));
            return ref output;
        }

        /// <inheritdoc />
        public void SkipOutput(ref byte* src) => src += (*(int*)src) + sizeof(int);

        /// <inheritdoc />
        public int GetLength(ref SpanByteAndMemory o) => o.Length;
    }
}