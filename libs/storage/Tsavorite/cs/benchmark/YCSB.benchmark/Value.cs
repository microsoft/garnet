// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#define EIGHT_BYTE_VALUE
//#define FIXED_SIZE_VALUE
//#define FIXED_SIZE_VALUE_WITH_LOCK

using System.Runtime.InteropServices;

namespace Tsavorite.benchmark
{
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct Value
    {
        [FieldOffset(0)]
        public long value;
    }
}