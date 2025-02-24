// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Tsavorite.benchmark
{
    [StructLayout(LayoutKind.Explicit, Size = sizeof(long))]
    public struct KeySpanByte
    {
        public const int kTotalKeySize = 12;

        [FieldOffset(0)]
        public long value;

        // Only call this for stack-based structs, not the ones in the *_keys vectors
        public override string ToString() => "{ " + value + " }";

        public unsafe SpanByte AsSpanByte() => new(sizeof(long), (nint)Unsafe.AsPointer(ref this));
    }
}