// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Tsavorite.test
{
    public readonly unsafe struct TestSpanByteKey : IKey
    {
        private readonly byte[] arr;
        private readonly void* ptr;
        private readonly int len;

        public readonly bool IsPinned => arr == null;

        public readonly ReadOnlySpan<byte> KeyBytes => arr == null ? new(ptr, len) : arr.AsSpan();

        /// <inheritdoc/>
        public bool HasNamespace => false;

        /// <inheritdoc/>
        public ReadOnlySpan<byte> NamespaceBytes => [];

        private TestSpanByteKey(byte[] arr, void* ptr, int len)
        {
            this.arr = arr;
            this.ptr = ptr;
            this.len = len;
        }

        public static TestSpanByteKey FromPinnedSpan(ReadOnlySpan<byte> key)
        {
            var ptr = Unsafe.AsPointer(ref MemoryMarshal.GetReference(key));
            var len = key.Length;
            return new(null, ptr, len);
        }

        public static TestSpanByteKey CopySpan(ReadOnlySpan<byte> key)
        {
            var arr = key.ToArray();
            return new(arr, null, arr.Length);
        }

        public static TestSpanByteKey FromPointer(byte* ptr, int len) => new(null, ptr, len);

        public static TestSpanByteKey FromArray(byte[] array) => new(array, null, array.Length);
    }
}