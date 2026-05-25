// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#define EIGHT_BYTE_VALUE
//#define FIXED_SIZE_VALUE
//#define FIXED_SIZE_VALUE_WITH_LOCK

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tsavorite.benchmark
{
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct FixedLengthValue
    {
        public const int Size = 8;

        // Only call this for stack-based structs, not the ones in the *_keys vectors
        public unsafe Span<byte> AsSpan() => new(Unsafe.AsPointer(ref this), sizeof(long));

        [FieldOffset(0)]
        public long value;
    }
}