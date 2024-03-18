// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Garnet.server
{
    [StructLayout(LayoutKind.Explicit, Size = 14)]
    struct AofHeader
    {
        [FieldOffset(0)]
        public AofEntryType opType;
        [FieldOffset(1)]
        public byte type;
        [FieldOffset(2)]
        public long version;
        [FieldOffset(10)]
        public int sessionID;
    }
}