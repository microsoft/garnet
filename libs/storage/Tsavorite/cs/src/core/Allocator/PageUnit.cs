// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    struct PageUnit
    {
        public byte[] value;
        public long pointer;
    }

    [StructLayout(LayoutKind.Explicit)]
    internal struct FullPageStatus
    {
        [FieldOffset(0)]
        public long LastFlushedUntilAddress;
        [FieldOffset(8)]
        public long Dirty;
    }

    [StructLayout(LayoutKind.Explicit)]
    internal struct PageOffset
    {
        [FieldOffset(0)]
        public int Offset;
        [FieldOffset(4)]
        public int Page;
        [FieldOffset(0)]
        public long PageAndOffset;
    }
}