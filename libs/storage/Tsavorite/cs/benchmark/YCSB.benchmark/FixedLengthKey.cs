// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Tsavorite.benchmark
{
    [StructLayout(LayoutKind.Explicit, Size = sizeof(long))]
    public struct FixedLengthKey
    {
        [FieldOffset(0)]
        public long value;

        public override string ToString() => "{ " + value + " }";

        // Only call this for stack-based structs, not the ones in the *_keys vectors
        public unsafe SpanByte AsSpanByte() => new(sizeof(long), (nint)Unsafe.AsPointer(ref this));

        public struct Comparer : IKeyComparer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public readonly long GetHashCode64(SpanByte key) => Utility.GetHashCode(key.AsRef<FixedLengthKey>().value);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Equals(SpanByte key1, SpanByte key2) => key1.AsRef<FixedLengthKey>().value == key2.AsRef<FixedLengthKey>().value;
        }
    }
}