// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;
using Garnet.common;

namespace Garnet.server
{
    internal enum AofHeaderType : byte
    {
        BasicHeader,
        ShardedHeader,
        TransactionHeader
    }

    /// <summary>
    /// Used for coordinated operations
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = AofTransactionHeader.TotalSize)]
    struct AofTransactionHeader
    {
        public const int TotalSize = AofShardedHeader.TotalSize + 1;

        /// <summary>
        /// AofShardedHeader used with multi-log
        /// </summary>
        [FieldOffset(0)]
        public AofShardedHeader shardedHeader;

        /// <summary>
        /// Used for synchronizing sublog replay
        /// </summary>
        [FieldOffset(AofShardedHeader.TotalSize)]
        public byte sublogAccessCount;
    }

    /// <summary>
    /// Used for sharded log to add a k
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = AofShardedHeader.TotalSize)]
    struct AofShardedHeader
    {
        public const int TotalSize = AofHeader.TotalSize + 8 + 1;

        /// <summary>
        /// AofHeader used with singleLog
        /// </summary>
        [FieldOffset(0)]
        public AofHeader basicHeader;

        /// <summary>
        /// Used for multilog operations
        /// </summary>
        [FieldOffset(AofHeader.TotalSize)]
        public long sequenceNumber;

        /// <summary>
        /// Used for marking an entry for replay to a specific subtask
        /// </summary>
        [FieldOffset(AofHeader.TotalSize + 8)]
        public byte keyDigest;
    };

    /// <summary>
    /// Basic AOF header
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = TotalSize)]
    struct AofHeader
    {
        public static unsafe byte* SkipHeader(byte* entryPtr)
        {
            var header = *(AofHeader*)entryPtr;
            var headerType = (AofHeaderType)header.padding;
            return headerType switch
            {
                AofHeaderType.BasicHeader => entryPtr + TotalSize,
                AofHeaderType.ShardedHeader => entryPtr + AofShardedHeader.TotalSize,
                AofHeaderType.TransactionHeader => entryPtr + AofTransactionHeader.TotalSize,
                _ => throw new GarnetException($"Type not supported {headerType}"),
            };
        }

        public const int TotalSize = 16;

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