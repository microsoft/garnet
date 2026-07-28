// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.InteropServices;
using Garnet.common;

namespace Garnet.server
{
    // AOF Header Hierarchy
    //
    // The header type determines the wire format of each AOF entry based on the log topology:
    //
    //   AofHeader (16B) — Base header for all entries. Used standalone with single-log mode.
    //       │
    //       ├── AofShardedHeader (24B) = AofHeader + sequenceNumber
    //       │       Used for per-key entries in multi-physical-log (sharded) mode.
    //       │       The sequence number enables cross-sublog ordering.
    //       │
    //       ├── AofSingleLogTransactionHeader (50B) = AofHeader + participantCount + replayTaskAccessVector
    //       │       Used for coordinated/broadcast operations (transactions, checkpoints, flush)
    //       │       in single-physical-log + multi-replay mode. Uses log addresses for ordering
    //       │       instead of embedded sequence numbers, saving 8B per entry.
    //       │
    //       └── AofShardedLogTransactionHeader (58B) = AofShardedHeader + participantCount + replayTaskAccessVector
    //               Used for coordinated/broadcast operations in multi-physical-log (sharded) mode.
    //               Embeds a sequence number (via AofShardedHeader) for cross-sublog ordering.
    //
    // Selection logic:
    //   Single log (1 physical, 1 replay task)  → BasicHeader
    //   Single physical log, multi-replay       → BasicHeader (per-key), SingleLogTransactionHeader (broadcast)
    //   Multi physical log, multi-replay        → ShardedHeader (per-key), ShardedLogTransactionHeader (broadcast)
    //
    // There are also chunked variants of non-Transaction header types, used when a large object value is split across multiple AOF entries.
    internal enum AofHeaderType : byte
    {
        BasicHeader = 0,
        ShardedHeader = 1,
        SingleLogTransactionHeader = 2,
        ShardedLogTransactionHeader = 3,

        // Chunked variants of the non-transction types above, used when a large object value is split across multiple AOF entries
        // (see the Aof*ChunkHeader structs). Each chunked variant has the same low two bits as its non-chunked counterpart, with the
        // ChunkedRecordFlag bit (0b0100) set, so it occupies the third bit of the (now 3-bit) AofHeaderTypeMask.
        BasicChunkHeader = 4,                            // BasicHeader | ChunkedRecordFlag
        ShardedChunkHeader = 5,                          // ShardedHeader | ChunkedRecordFlag
    }

    /// <summary>
    /// Used for coordinated operations
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = TotalSize)]
    unsafe struct AofShardedLogTransactionHeader
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
    /// Used for single-physical-log with multi-replay to carry transaction participant info
    /// without embedding a sequence number (log addresses are used instead).
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = TotalSize)]
    unsafe struct AofSingleLogTransactionHeader
    {
        public const int TotalSize = AofHeader.TotalSize + 2 + AofShardedLogTransactionHeader.ReplayTaskAccessVectorBytes;

        /// <summary>
        /// Basic AOF header
        /// </summary>
        [FieldOffset(0)]
        public AofHeader basicHeader;

        /// <summary>
        /// Used for synchronizing virtual sublog replay
        /// NOTE: This stores the total number of replay tasks that participate in a given transaction.
        /// </summary>
        [FieldOffset(AofHeader.TotalSize)]
        public short participantCount;

        /// <summary>
        /// Used to track replay task participating in the txn
        /// </summary>
        [FieldOffset(AofHeader.TotalSize + 2)]
        public fixed byte replayTaskAccessVector[AofShardedLogTransactionHeader.ReplayTaskAccessVectorBytes];
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
                AofHeaderType.ShardedLogTransactionHeader => entryPtr + AofShardedLogTransactionHeader.TotalSize,
                AofHeaderType.SingleLogTransactionHeader => entryPtr + AofSingleLogTransactionHeader.TotalSize,
                AofHeaderType.BasicChunkHeader => entryPtr + AofBasicChunkHeader.TotalSize,
                AofHeaderType.ShardedChunkHeader => entryPtr + AofShardedChunkHeader.TotalSize,
                _ => throw new GarnetException($"Type not supported {headerType}"),
            };
        }

        /// <summary>
        /// Like <see cref="SkipHeader"/>, but returns a reference to the embedded <see cref="AofChunkHeader"/> of a chunk record
        /// (its lengths/objectId/keyHash), for <see cref="AofHeaderType.BasicChunkHeader"/> and
        /// <see cref="AofHeaderType.ShardedChunkHeader"/>.
        /// </summary>
        public static unsafe ref AofChunkHeader GetChunkedHeaderRef(byte* entryPtr)
        {
            var headerType = ((AofHeader*)entryPtr)->HeaderType;
            if (headerType == AofHeaderType.BasicChunkHeader)
                return ref ((AofBasicChunkHeader*)entryPtr)->chunkHeader;
            if (headerType == AofHeaderType.ShardedChunkHeader)
                return ref ((AofShardedChunkHeader*)entryPtr)->chunkHeader;
            throw new GarnetException($"Type is not a chunk header: {headerType}");
        }

        public const int TotalSize = 16;

        // Important: Update AofHeaderVersion whenever any of the following change:
        // * Layout, size, contents of this struct
        // * Any of the AofEntryType or AofStoreType enums' existing value mappings
        // * SpanByte format or header
        // * The persisted-value numbering of any enum serialized into an entry payload:
        //   RespCommand (RespInputHeader.cmd), the object sub-operation enums (RespInputHeader.SubId:
        //   HashOperation/ListOperation/SetOperation/SortedSetOperation), GarnetObjectType
        //   (RespInputHeader.type), or RespInputFlags.
        // * The layout of RespInputHeader itself (e.g. which byte holds cmd/type/subId/flags).
        // Version 3 repurposes the flags byte as a bitfield containing the header type
        // plus chunked-record and unsafe-truncate markers.
        // Version 4 makes the RespCommand write block dense/explicit (writes-first) and moves the
        // object sub-operation id (SubId) from the low 5 bits of the flags byte into its own header
        // byte; AofProcessor remaps v3 entries on replay.
        internal const byte AofHeaderVersion = 4;

        /// <summary>
        /// Highest AOF header version this build can read/replay. Entries with a higher version were
        /// written by a newer Garnet version and cannot be safely interpreted.
        /// </summary>
        internal const byte MaxSupportedAofHeaderVersion = AofHeaderVersion;

        /// <summary>
        /// Bits in <see cref="flags"/> that identify the <see cref="AofHeaderType"/>.
        /// Three bits wide: the low two bits select the base header type, and the third bit
        /// (<see cref="ChunkedRecordFlag"/>) selects the chunked variant of that base type.
        /// </summary>
        internal const byte AofHeaderTypeMask = 0b0111;

        /// <summary>
        /// Bit in <see cref="flags"/> that indicates that the record is chunked. This is the high
        /// bit of <see cref="AofHeaderTypeMask"/>, so a chunked <see cref="AofHeaderType"/> value is
        /// its non-chunked counterpart with this bit set.
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

        /// <summary>True if this record is one chunk of a larger, chunked logical record.</summary>
        public readonly bool IsChunked => (flags & ChunkedRecordFlag) != 0;

        public AofHeader()
        {
            flags = 0;
            aofHeaderVersion = AofHeaderVersion;
        }
    }

    /// <summary>
    /// Chunked variant of <see cref="AofHeader"/>: a basic header immediately followed by an
    /// <see cref="AofChunkHeader"/>. Used when a large value is split across multiple AOF entries.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = TotalSize)]
    struct AofBasicChunkHeader
    {
        public const int TotalSize = AofHeader.TotalSize + AofChunkHeader.TotalSize;

        /// <summary>Byte offset of the chunk's objectId within this header (the only field patched per-chunk at write time).</summary>
        public const int ObjectIdOffset = AofHeader.TotalSize + AofChunkHeader.ObjectIdOffset;

        /// <summary>
        /// Basic AOF header.
        /// </summary>
        [FieldOffset(0)]
        public AofHeader basicHeader;

        /// <summary>
        /// Chunk framing for this entry.
        /// </summary>
        [FieldOffset(AofHeader.TotalSize)]
        public AofChunkHeader chunkHeader;
    }

    /// <summary>
    /// Chunked variant of <see cref="AofShardedHeader"/>: a sharded header immediately followed by an
    /// <see cref="AofChunkHeader"/>.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = TotalSize)]
    struct AofShardedChunkHeader
    {
        public const int TotalSize = AofShardedHeader.TotalSize + AofChunkHeader.TotalSize;

        /// <summary>Byte offset of the chunk's objectId within this header (the only field patched per-chunk at write time).</summary>
        public const int ObjectIdOffset = AofShardedHeader.TotalSize + AofChunkHeader.ObjectIdOffset;

        /// <summary>
        /// Sharded AOF header.
        /// </summary>
        [FieldOffset(0)]
        public AofShardedHeader shardedHeader;

        /// <summary>
        /// Chunk framing for this entry.
        /// </summary>
        [FieldOffset(AofShardedHeader.TotalSize)]
        public AofChunkHeader chunkHeader;
    }
}