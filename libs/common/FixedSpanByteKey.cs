// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
#if !NET9_0_OR_GREATER
using System.Runtime.InteropServices;
#endif
using Tsavorite.core;

namespace Garnet.common
{
    /// <summary>
    /// Key type which wraps a <see cref="ReadOnlySpan{Byte}"/>.
    /// 
    /// In addition to the span being pinned during, it must also be "fixed" - that is unmoving and not-reused over the whole lifetime of a Tsavorite operation.
    /// This is inclusive of asynchronous completions.
    /// </summary>
    public readonly
#if NET9_0_OR_GREATER
        ref
#endif
        struct FixedSpanByteKey : IKey
    {
#if !NET9_0_OR_GREATER
        private readonly unsafe void* ptr;
        private readonly int len;
#endif

        /// <inheritdoc/>
        public readonly bool IsPinned
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => true;
        }

        /// <inheritdoc/>
        public readonly bool IsEmpty
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => false;
        }

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> KeyBytes
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if NET9_0_OR_GREATER
            get; 
#else
            get
            {
                unsafe
                {
                    return new(ptr, len);
                }
            }
#endif
        }

        /// <inheritdoc/>
        public readonly bool HasNamespace
        {
            get => false;
        }

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> NamespaceBytes
        {
            get
            {
                Debug.Fail("Should never be called on FixedSpanByteKey");
                return [];
            }
        }

        private FixedSpanByteKey(ReadOnlySpan<byte> key)
        {
#if NET9_0_OR_GREATER
            KeyBytes = key;
#else
            unsafe
            {
                ptr = Unsafe.AsPointer(ref MemoryMarshal.GetReference(key));
            }
            len = key.Length;
#endif
        }

        /// <inheritdoc/>
        public override readonly string ToString() => SpanByte.ToShortString(KeyBytes);

        /// <summary>
        /// Convert a pinned and "fixed" (data will be unchanged and unmoving until after any async ops complete) <see cref="ReadOnlySpan{Byte}"/> to a <see cref="FixedSpanByteKey"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static explicit operator FixedSpanByteKey(ReadOnlySpan<byte> key)
        => new(key);

        /// <summary>
        /// Convert a pinned and "fixed" (data will be unchanged and unmoving until after any async ops complete) <see cref="PinnedSpanByte"/> to a <see cref="FixedSpanByteKey"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static explicit operator FixedSpanByteKey(PinnedSpanByte key)
        => new(key.ReadOnlySpan);
    }
}