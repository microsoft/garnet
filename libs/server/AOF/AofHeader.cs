// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Used for sharded log to add a timestamp and logAccessCounter
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 26)]
    struct AofExtendedHeader
    {
        /// <summary>
        /// Reserved sublog replay task id for coordinated operations (i.e. txn, custom-txn, )
        /// </summary>
        internal const int RESERVED_SUBTASK_ID = 255;

        /// <summary>
        /// AofHeader used with singleLog
        /// </summary>
        [FieldOffset(0)]
        public AofHeader header;

        /// <summary>
        /// Used for multilog operations
        /// </summary>
        [FieldOffset(16)]
        public long sequenceNumber;

        /// <summary>
        /// Used for synchronizing sublog replay
        /// </summary>
        [FieldOffset(24)]
        public byte sublogAccessCount;

        /// <summary>
        /// Used for marking an entry for replay to a specific subtask
        /// </summary>
        [FieldOffset(25)]
        public byte subtaskIdx;

        /// <summary>
        /// AofExtendedHeader constructor
        /// </summary>
        /// <param name="aofHeader"></param>
        /// <param name="sequenceNumber"></param>
        /// <param name="sublogAccessCount"></param>
        public AofExtendedHeader(AofHeader aofHeader, long sequenceNumber, byte sublogAccessCount)
        {
            header = aofHeader;
            header.padding = AofHeader.ShardedLogFlag;
            this.sequenceNumber = sequenceNumber;
            this.sublogAccessCount = sublogAccessCount;
        }

        /// <summary>
        /// Tests whether this is an extended header by looking at the padding first bit
        /// </summary>
        public readonly bool IsExtendedHeader => (header.padding & 0x1) == 0x1;

        /// <summary>
        /// Throws exception if AofHeader is not of AofExtendedType
        /// </summary>
        /// <exception cref="GarnetException"></exception>
        public readonly void ThrowIfNotExtendedHeader()
        {
            if (!IsExtendedHeader)
                throw new GarnetException("AofHeader not of AofExtendedHeader type!");
        }
    };

    [StructLayout(LayoutKind.Explicit, Size = 16)]
    struct AofHeader
    {
        // Important: Update version number whenever any of the following change:
        // * Layout, size, contents of this struct
        // * Any of the AofEntryType or AofStoreType enums' existing value mappings
        // * SpanByte format or header
        const byte AofHeaderVersion = 2;

        /// <summary>
        /// 0-bit in padding is used to indicate that the log contains AofExtendedHeader
        /// </summary>
        internal const byte ShardedLogFlag = 1;

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
        /// Transaction ID
        /// </summary>
        [FieldOffset(12)]
        public int txnID;
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