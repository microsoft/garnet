// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Tsavorite.benchmark
{
    [StructLayout(LayoutKind.Explicit, Size = SpanByteYcsbBenchmark.kKeySize)]
    public struct KeySpanByte
    {
        [FieldOffset(0)]
        public int length;
        [FieldOffset(4)]
        public long value;
    }
}