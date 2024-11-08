// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Garnet.server
{
    [StructLayout(LayoutKind.Explicit, Size = 15)]
    struct AofHeader
    {
        // UPDATE THIS IF YOU CHANGE LAYOUT OF THIS STRUCT, this cannot exceed 127, as long as we are using MSB as heuristic to check for legacy format
        const byte CURRENT_AOF_FORMAT_VERSION = 1;

        /*
        We use the MSB to differentiate if we are interacting with a legacy or the new AofHeader  
        Note: When we are removing legacy support for old AOF log we can remove this and it's usage completely
        */
        const byte ONLY_MSB_SET_MASK = 0b1000_0000;

        const byte MSB_IGNORED_BYTE_MASK = 0b0111_1111;

        public byte AofHeaderFormatVersion
        {
            get
            {
                // only use the first 7 bits of the byte to give the value
                return (byte)(this._aofHeaderFormatVersion & MSB_IGNORED_BYTE_MASK);
            }

            set
            {
                // ignore the MSB and let the user only set the first 7 bits before the MSB
                _aofHeaderFormatVersion = (byte)((_aofHeaderFormatVersion & ONLY_MSB_SET_MASK) | (value & MSB_IGNORED_BYTE_MASK));
            }
        }

        [FieldOffset(0)]
        byte _aofHeaderFormatVersion = ONLY_MSB_SET_MASK;

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


        /// <summary>
        /// ctors are used for creating an instance when writing, having the MSB being set in the ctor will not affect the value when reading from disk
        /// </summary>
        /// <param name="opType"></param>
        /// <param name="version"></param>
        /// <param name="sessionID"></param>
        public AofHeader(AofEntryType opType, long version, int sessionID)
        {
            this.AofHeaderFormatVersion = CURRENT_AOF_FORMAT_VERSION;
            this.opType = opType;
            this.version = version;
            this.sessionID = sessionID;
        }

        /// <summary>
        /// ctors are used for creating an instance when writing, having the MSB being set in the ctor will not affect the value when reading from disk
        /// </summary>
        /// <param name="opType"></param>
        /// <param name="type"></param>
        /// <param name="version"></param>
        /// <param name="sessionID"></param>
        public AofHeader(AofEntryType opType, byte type, long version, int sessionID)
        {
            this.AofHeaderFormatVersion = CURRENT_AOF_FORMAT_VERSION;
            this.opType = opType;
            this.type = type;
            this.version = version;
            this.sessionID = sessionID;
        }

        /// <summary>
        /// Checks if the MSB is not set of the first pointer, this indicates it is an older AOF format
        /// </summary>
        /// <param name="ptr"></param>
        /// <returns></returns>
        public static unsafe bool IsLegacyFormat(byte* ptr) => (*ptr & ONLY_MSB_SET_MASK) == 0;

    }
}