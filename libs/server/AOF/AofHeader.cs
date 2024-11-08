// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Garnet.server
{
    [StructLayout(LayoutKind.Explicit, Size = 18)]
    struct AofHeader
    {
        /*
        First byte of AofHeader starts from an int such that the MSB of the int is SET
        We use the MSB to differentiate if we are interacting with a legacy or the new AofHeader  
        Future AOF format updates should increment version from 128.
        */
        const int AofFormatVersion = 1 << 7;

        [FieldOffset(0)]
        public int AofHeaderFormatVersion;

        [FieldOffset(4)]
        public AofEntryType opType;

        // Custom trasaction procedure ID
        [FieldOffset(5)]
        public byte type;

        // Version for the record, associated with the record
        [FieldOffset(6)]
        public long version;

        // Used to map transactions to RespServerSessions that they came from
        [FieldOffset(14)]
        public int sessionID;

        public AofHeader(AofEntryType opType, long version, int sessionID)
        {
            AofHeaderFormatVersion = AofFormatVersion;
            this.opType = opType;
            this.version = version;
            this.sessionID = sessionID;
        }

        public AofHeader(AofEntryType opType, byte type, long version, int sessionID)
        {
            AofHeaderFormatVersion = AofFormatVersion;
            this.opType = opType;
            this.type = type;
            this.version = version;
            this.sessionID = sessionID;
        }
    }
}