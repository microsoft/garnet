// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Tsavorite.kvbench
{
    /// <summary>
    /// 8-byte key holding a single long value. Stored in a stack/array of 16-byte
    /// slots (padding for cache-line alignment of subsequent value bytes inside the
    /// log record). Only the first 8 bytes are part of the key for hash + equality.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = DataSize)]
    public struct KvKey : IKey
    {
        internal const int DataSize = 16;

        [FieldOffset(0)]
        public long Value;

        [FieldOffset(sizeof(long))]
        public int padding1, padding2;

        public override readonly string ToString() => "{ " + Value + " }";

        public readonly bool IsPinned => false;

        public unsafe ReadOnlySpan<byte> KeyBytes
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new(Unsafe.AsPointer(ref this), sizeof(long));
        }

        public readonly bool HasNamespace => false;

        public readonly ReadOnlySpan<byte> NamespaceBytes => [];
    }
}
