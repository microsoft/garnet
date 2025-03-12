// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Garnet.server
{
    [StructLayout(LayoutKind.Explicit, Size = 16)]
    struct AofHeader
    {
        // Important: Update version number whenever any of the following change:
        // * Layout, size, contents of this struct
        // * Any of the AofEntryType or AofStoreType enums' existing value mappings
        // * SpanByte format or header
        const byte AofHeaderVersion = 2;

        /// <summary>
        /// Version of AOF
        /// </summary>
        [FieldOffset(0)]
        public byte aofHeaderVersion;
        /// <summary>
        /// Padding, for alignment and future use
        /// </summary>
        [FieldOffset(1)]
        public byte padding;
        /// <summary>
        /// Type of operation
        /// </summary>
        [FieldOffset(2)]
        public AofEntryType opType;
        /// <summary>
        /// Procedure ID
        /// </summary>
        [FieldOffset(3)]
        public byte procedureId;
        /// <summary>
        /// Store version
        /// </summary>
        [FieldOffset(4)]
        public long storeVersion;
        /// <summary>
        /// Session ID
        /// </summary>
        [FieldOffset(12)]
        public int sessionID;
        /// <summary>
        /// Unsafe truncate log (used with FLUSH command)
        /// </summary>
        [FieldOffset(1)]
        public byte unsafeTruncateLog;
        /// <summary>
        /// Database ID (used with FLUSH command)
        /// </summary>
        [FieldOffset(3)]
        public byte databaseId;

        public AofHeader()
        {
            this.aofHeaderVersion = AofHeaderVersion;
        }
    }
}