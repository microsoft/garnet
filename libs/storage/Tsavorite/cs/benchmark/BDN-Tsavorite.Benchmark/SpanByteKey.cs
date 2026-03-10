// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if !NET9_0_OR_GREATER
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
#endif
using Tsavorite.core;

namespace BenchmarkDotNetTests
{
    public readonly
#if NET9_0_OR_GREATER
        ref
#endif
        struct SpanByteKey : IKey
    {
#if !NET9_0_OR_GREATER
        private readonly unsafe void* ptr;
        private readonly int len;
#endif

        /// <summary>
        /// In benchmarks, we don't wait for pending operations to complete so the span isn't fixed.
        /// </summary>
        public readonly bool IsPinned => false;

        /// <inheritdoc/>
        public readonly bool IsEmpty => false;

#if NET9_0_OR_GREATER
        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> KeyBytes { get; }
#else
        /// <inheritdoc/>        
        public readonly unsafe ReadOnlySpan<byte> KeyBytes => new(ptr, len);
#endif

        public SpanByteKey(ReadOnlySpan<byte> keyBytes)
        {
#if NET9_0_OR_GREATER
            KeyBytes = keyBytes;
#else
            unsafe
            {
                ptr = Unsafe.AsPointer(ref MemoryMarshal.GetReference(keyBytes));
                len = keyBytes.Length;
            }
#endif
        }

        /// <inheritdoc/>
        public bool HasNamespace => false;

        /// <inheritdoc/>
        public ReadOnlySpan<byte> NamespaceBytes => [];
    }
}