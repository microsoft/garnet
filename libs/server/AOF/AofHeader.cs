// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Garnet.server
{
    [StructLayout(LayoutKind.Explicit, Size = 15)]
    struct AofHeader
    {
        /*
        First byte of AofHeader starts from an int such that the MSB of the int is SET
        We use the MSB to differentiate if we are interacting with a legacy or the new AofHeader  
        Future AOF format updates should increment version from 128.
        PLEASE UPDATE THIS IF THE LAYOUT OF THIS FILE IS CHANGED
        
        Note: When we are removing legacy support for old AOF log we can set the version to start from 0.
        */
        const byte AofFormatVersion = 1 << 7;

        [FieldOffset(0)]
        public byte AofHeaderFormatVersion;

        [FieldOffset(1)]
        public AofEntryType opType;

        // Custom trasaction procedure ID
        [FieldOffset(2)]
        public byte type;

        // Version for the record, associated with the record
        [FieldOffset(3)]
        public long version;

        // Used to map transactions to RespServerSessions that they came from
        [FieldOffset(11)]
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