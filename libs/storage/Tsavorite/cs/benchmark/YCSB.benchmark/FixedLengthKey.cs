// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Tsavorite.benchmark
{
    [StructLayout(LayoutKind.Explicit, Size = sizeof(long))]
    public struct FixedLengthKey : IKey
    {
        [FieldOffset(0)]
        public long value;

        // Not always pinned, so don't assume it is
        public readonly bool IsPinned => false;

        [UnscopedRef]
        public readonly ReadOnlySpan<byte> KeyBytes => MemoryMarshal.Cast<long, byte>(new ReadOnlySpan<long>(in value));

        public override readonly string ToString() => "{ " + value + " }";

        // Only call this for stack-based structs, not the ones in the *_keys vectors
        public unsafe ReadOnlySpan<byte> AsReadOnlySpan() => new(Unsafe.AsPointer(ref this), sizeof(long));

        public struct Comparer : IKeyComparer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public readonly long GetHashCode64<TKey>(TKey key)
                where TKey : IKey
#if NET9_0_OR_GREATER
                    , allows ref struct
#endif
                => Utility.GetHashCode(key.KeyBytes.AsRef<FixedLengthKey>().value);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Equals<TFirstKey, TSecondKey>(TFirstKey key1, TSecondKey key2)
                where TFirstKey : IKey
#if NET9_0_OR_GREATER
                    , allows ref struct
#endif
                where TSecondKey : IKey
#if NET9_0_OR_GREATER
                    , allows ref struct
#endif
                => key1.KeyBytes.AsRef<FixedLengthKey>().value == key2.KeyBytes.AsRef<FixedLengthKey>().value;
        }

        /// <inheritdoc/>
        public bool HasNamespace => false;

        /// <inheritdoc/>
        public ReadOnlySpan<byte> NamespaceBytes => [];
    }
}