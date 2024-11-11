// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Garnet.server
{
    [StructLayout(LayoutKind.Explicit, Size = 15)]
    struct AofHeader
    {
        // UPDATE THIS VERSION WHENEVER THE LAYOUT OF THIS STRUCT CHANGES
        const byte AOF_HEADER_VERSION = 1;

        [FieldOffset(0)]
        public byte aofHeaderVersion;
        [FieldOffset(1)]
        public AofEntryType opType;
        [FieldOffset(2)]
        public byte type;
        [FieldOffset(3)]
        public long version;
        [FieldOffset(11)]
        public int sessionID;

        public AofHeader(AofEntryType opType, byte type, long version, int sessionID)
        {
            this.aofHeaderVersion = AOF_HEADER_VERSION;
            this.opType = opType;
            this.type = type;
            this.version = version;
            this.sessionID = sessionID;
        }

        public AofHeader(AofEntryType opType, long version, int sessionID)
        {
            this.aofHeaderVersion = AOF_HEADER_VERSION;
            this.opType = opType;
            this.version = version;
            this.sessionID = sessionID;
        }
    }
}