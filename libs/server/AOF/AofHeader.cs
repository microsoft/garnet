// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.InteropServices;
using Garnet.common;

namespace Garnet.server
{
    internal enum AofHeaderType : byte
    {
        BasicHeader = 0,
        ShardedHeader = 1,
        TransactionHeader = 2
    }

    /// <summary>
    /// Used for coordinated operations
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = TotalSize)]
    unsafe struct AofTransactionHeader
    {
        public const int TotalSize = AofShardedHeader.TotalSize + 2 + 32;
        // maximum 256 replay tasks per physical sublog, hence 32 bytes bitmap
        public const int ReplayTaskAccessVectorBytes = 32;

        /// <summary>
        /// AofShardedHeader used with multi-log
        /// </summary>
        [FieldOffset(0)]
        public AofShardedHeader shardedHeader;

        /// <summary>
        /// Used for synchronizing virtual sublog replay
        /// NOTE: This stores the total number of replay tasks that participate in a given transaction.
        /// </summary>
        [FieldOffset(AofShardedHeader.TotalSize)]
        public short participantCount;

        /// <summary>
        /// Used to track replay task participating in the txn
        /// </summary>
        [FieldOffset(AofShardedHeader.TotalSize + 2)]
        public fixed byte replayTaskAccessVector[ReplayTaskAccessVectorBytes];
    }

    /// <summary>
    /// Used for sharded log to add a k
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = TotalSize)]
    struct AofShardedHeader
    {
        public const int TotalSize = AofHeader.TotalSize + 8;

        /// <summary>
        /// Basic AOF header used with single log.
        /// </summary>
        [FieldOffset(0)]
        public AofHeader basicHeader;

        /// <summary>
        /// Used with multi-log to implement read consistency protocol.
        /// </summary>
        [FieldOffset(AofHeader.TotalSize)]
        public long sequenceNumber;
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
            var headerType = header.HeaderType;
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
        // Version 3 repurposes the flags byte as a bitfield containing the header type
        // plus chunked-record and unsafe-truncate markers.
        const byte AofHeaderVersion = 3;

        /// <summary>
        /// Bits in <see cref="flags"/> that identify the <see cref="AofHeaderType"/>
        /// </summary>
        internal const byte AofHeaderTypeMask = 0b0011;

        /// <summary>
        /// Bit in <see cref="flags"/> that indicates that the record is chunked
        /// </summary>
        internal const byte ChunkedRecordFlag = 0b0100;

        /// <summary>
        /// Bit in <see cref="flags"/> that indicates Unsafe truncate log (used with FLUSH command)
        /// </summary>
        internal const byte UnsafeTruncateLogFlag = 0b1000;

        /// <summary>
        /// Version of AOF
        /// </summary>
        [FieldOffset(0)]
        public byte aofHeaderVersion;
        /// <summary>
        /// Flags, for current and future use
        /// </summary>
        [FieldOffset(1)]
        public byte flags;
        /// <summary>
        /// Type of operation
        /// </summary>
        [FieldOffset(2)]
        public AofEntryType opType;

        /// <summary>
        /// Procedure ID; union with <see cref="databaseId"/>
        /// </summary>
        [FieldOffset(3)]
        public byte procedureId;
        /// <summary>
        /// Database ID (used with FLUSH command); union with <see cref="procedureId"/>
        /// </summary>
        [FieldOffset(3)]
        public byte databaseId;

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
        public bool UnsafeTruncateLog
        {
            get => (flags & UnsafeTruncateLogFlag) != 0;
            set
            {
                if (value)
                    flags |= UnsafeTruncateLogFlag;
                else
                    flags = (byte)(flags & ~UnsafeTruncateLogFlag);
            }
        }

        public AofHeaderType HeaderType
        {
            get => (AofHeaderType)(flags & AofHeaderTypeMask);
            set
            {
                Debug.Assert((int)value <= AofHeaderTypeMask, $"value {value} does not fit in AofHeaderTypeMask");
                flags = (byte)((flags & ~AofHeaderTypeMask) | (byte)value);
            }
        }

        public AofHeader()
        {
            flags = 0;
            aofHeaderVersion = AofHeaderVersion;
        }
    }
}