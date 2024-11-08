// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Garnet.server
{
    // DEPRECATED: Only used for backwards compatability when reading from old AOF in legacy mode, do not use for enqueueing in AOF.
    // Refer to AofHeader to see new format.
    [StructLayout(LayoutKind.Explicit, Size = 14)]
    struct LegacyAofHeader
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