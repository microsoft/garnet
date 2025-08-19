// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System.Runtime.InteropServices;

namespace Tsavorite.benchmark
{
    [StructLayout(LayoutKind.Explicit, Size = 16)]
    struct LogHeader
    {
        [FieldOffset(0)]
        public byte version;
        [FieldOffset(1)]
        public byte padding;
        [FieldOffset(2)]
        public byte type;
        [FieldOffset(3)]
        public byte procId;
        [FieldOffset(4)]
        public long storeVersion;
        [FieldOffset(12)]
        public int sessionID;
        [FieldOffset(1)]
        public byte unsafeTruncateLog;
        [FieldOffset(3)]
        public byte databaseId;
    }
}
