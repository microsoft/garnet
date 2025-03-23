// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

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

        public unsafe ReadOnlySpan<byte> AsReadOnlySpan() => new(Unsafe.AsPointer(ref this), sizeof(long));
    }
}