// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#define EIGHT_BYTE_VALUE
//#define FIXED_SIZE_VALUE
//#define FIXED_SIZE_VALUE_WITH_LOCK

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Tsavorite.benchmark
{
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct FixedLengthValue
    {
        public const int Size = 8;

        public unsafe SpanByte AsSpanByte() => new(sizeof(long), (nint)Unsafe.AsPointer(ref this));

        [FieldOffset(0)]
        public long value;
    }
}