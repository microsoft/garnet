// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server.BTreeIndex;
using Tsavorite.core;

namespace Garnet.server
{
    public enum StreamTrimOpts
    {
        MAXLEN,
        MINID,
        NONE
    }

    public enum XADDOpts
    {
        NOMKSTREAM,
        NONE
    }

    public enum ParsedStreamEntryID
    {
        VALID,
        INVALID,
        NOT_GREATER,
    }

    // This is the layout that is put in the log for each stream entry. The numPairs field
    // doubles as a kind discriminator: non-negative values are data records carrying that
    // many field-value pairs, negative values are control records (see ControlRecordKind).
    [StructLayout(LayoutKind.Sequential, Pack = 1, Size = 20)]
    public struct StreamLogEntryHeader
    {
        public StreamID id;
        public int numPairs;
    }

    /// <summary>
    /// Negative <see cref="StreamLogEntryHeader.numPairs"/> sentinels for control records.
    /// XDEL writes a tombstone (an arbitrary entry can be deleted from the middle of the
    /// stream, so we can't represent it by truncating the log prefix). XTRIM is *not* logged
    /// here — it always drops the oldest entries, which is a contiguous log prefix, so
    /// <c>log.TruncateUntil</c> is the persistence mechanism for it.
    /// </summary>
    internal static class ControlRecordKind
    {
        // XDEL marker: header.id = the deleted entry's id, payload empty.
        public const int Tombstone = -1;
    }

    /// <summary>
    /// Server-global configuration needed to re-open a stream's per-stream log when the object store
    /// deserializes a <see cref="StreamObject"/> (the deserialization factory only receives a
    /// <see cref="BinaryReader"/>). Streams are not per-database, so a single ambient configuration
    /// suffices. Set once at startup by <see cref="StreamManager"/>.
    /// </summary>
    internal static class StreamObjectConfig
    {
        internal static string StreamsRootDir;
        internal static long DefaultPageSize = 4096;
        internal static long DefaultMemorySize = 1L << 24;
        internal static bool WaitForCommit;

        internal static void Configure(string streamsRootDir, long pageSize, long memorySize, bool waitForCommit)
        {
            StreamsRootDir = streamsRootDir;
            DefaultPageSize = pageSize;
            DefaultMemorySize = memorySize;
            WaitForCommit = waitForCommit;
        }
    }

    public class StreamObject : GarnetObjectBase
    {
        // Rough heap-overhead estimate reported to the object store (refined as the BTree/log grow).
        const long StreamHeapOverhead = 4096;

        readonly IDevice device;
        readonly TsavoriteLog log;
        readonly BTree index;
        readonly bool waitForCommit;
        readonly string streamsRootDir;
        readonly string streamDirName;
        StreamID lastId;
        long totalEntriesAdded;
        SingleWriterMultiReaderLock _lock;
        // Set by Dispose. Read by session caches to evict stale references after FLUSHDB/FLUSHALL.
        // volatile because Dispose may run on the FLUSHDB thread while another session reads from
        // its own cache without taking the stream lock.
        volatile bool disposed;

        /// <summary>True once <see cref="Dispose"/> has run. Used by session-level caches to
        /// detect and evict references that were invalidated by FLUSHDB/FLUSHALL.</summary>
        public bool IsDisposed => disposed;

        /// <summary>Consumer groups attached to this stream, keyed by group name.</summary>
        readonly Dictionary<string, ConsumerGroup> consumerGroups = new(StringComparer.Ordinal);

        public StreamID LastId
        {
            get
            {
                // Need locking to prevent torn reads from AddEntry
                _lock.ReadLock();
                try
                {
                    return lastId;
                }
                finally
                {
                    _lock.ReadUnlock();
                }
            }
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="streamsRootDir">Root directory under which all stream subdirectories live.
        ///     If null, the stream is in-memory only (NullDevice) — no durability.</param>
        /// <param name="streamDirName">Per-stream subdirectory name (typically a hex-encoded key).
        ///     If null, the stream is in-memory only.</param>
        /// <param name="pageSize">Page size of the log used for the stream.</param>
        /// <param name="memorySize">Memory budget for the log.</param>
        /// <param name="waitForCommit">When true, every write (XADD, XDEL, XTRIM) synchronously
        ///     flushes and waits for the commit to complete before returning. This mirrors the
        ///     server-wide <c>--wait-for-commit</c> AOF behaviour for the stream's own log.</param>
        /// <param name="recover">If true and a disk-backed log exists at the path, recover the
        ///     log and rebuild the in-memory BTree by scanning all entries.</param>
        public StreamObject(string streamsRootDir, string streamDirName, long pageSize, long memorySize, bool waitForCommit = false, bool recover = false)
            : base(StreamHeapOverhead)
        {
            this.streamsRootDir = streamsRootDir;
            this.streamDirName = streamDirName;
            if (streamsRootDir == null || streamDirName == null)
            {
                device = new NullDevice();
            }
            else
            {
                var streamDir = Path.Combine(streamsRootDir, streamDirName);
                Directory.CreateDirectory(streamDir);
                device = Devices.CreateLogDevice(Path.Combine(streamDir, "streamLog"), preallocateFile: false);
            }
            // TsavoriteLog auto-recovers when TryRecoverLatest=true (the default), so simply
            // re-opening a log device with prior commits replays its in-memory state.
            // SafeTailAddress is now refreshed on-demand via RefreshSafeTailAddress() (Tsavorite PR
            // #1720); there is no longer a background-refresh-frequency setting.
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSize = pageSize, MemorySize = memorySize });
            index = new BTree((uint)BTreeNode.PAGE_SIZE);
            totalEntriesAdded = 0;
            lastId = default;
            this.waitForCommit = waitForCommit;
            _lock = new SingleWriterMultiReaderLock();

            if (recover)
            {
                RebuildIndexFromLog();
            }
        }

        /// <summary>
        /// Reconstruct a stream from its serialized object-store form. Reads the per-stream identity,
        /// metadata, and consumer-group state from the blob, re-opens the per-stream log (using the
        /// server-global <see cref="StreamObjectConfig"/>), and rebuilds the BTree index.
        /// </summary>
        public StreamObject(BinaryReader reader)
            : base(reader, StreamHeapOverhead)
        {
            _lock = new SingleWriterMultiReaderLock();

            // Per-stream identity. The log directory name is persisted; the root dir, page/memory
            // sizes, and wait-for-commit are server-global and come from the ambient config.
            var hasDir = reader.ReadBoolean();
            streamDirName = hasDir ? reader.ReadString() : null;
            waitForCommit = reader.ReadBoolean();
            streamsRootDir = StreamObjectConfig.StreamsRootDir;

            if (streamsRootDir == null || streamDirName == null)
            {
                device = new NullDevice();
            }
            else
            {
                var streamDir = Path.Combine(streamsRootDir, streamDirName);
                Directory.CreateDirectory(streamDir);
                device = Devices.CreateLogDevice(Path.Combine(streamDir, "streamLog"), preallocateFile: false);
            }
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSize = StreamObjectConfig.DefaultPageSize, MemorySize = StreamObjectConfig.DefaultMemorySize });
            index = new BTree((uint)BTreeNode.PAGE_SIZE);

            // Authoritative metadata from the blob (totalEntriesAdded must survive log trims, which
            // RebuildIndexFromLog cannot recompute since trimmed records are gone).
            var blobLastId = ReadStreamID(reader);
            var blobTotalEntriesAdded = reader.ReadInt64();

            DeserializeConsumerGroups(reader);

            // Phase 1: rebuild the index by scanning the recovered log. A future phase serializes the
            // BTree into the blob (with the covered tail) and replays only the post-checkpoint suffix.
            RebuildIndexFromLog();

            lastId = blobLastId;
            totalEntriesAdded = blobTotalEntriesAdded;
        }

        /// <inheritdoc />
        public override byte Type => (byte)GarnetObjectType.Stream;

        /// <summary>
        /// Streams are mutated in place through StreamManager rather than copied; the per-stream log
        /// and device cannot be duplicated, so Clone shares this single owning instance.
        /// </summary>
        public override IHeapObject Clone() => this;

        /// <summary>
        /// Stream commands are dispatched directly via StreamManager / RespServerSession, not through
        /// the generic object Operate path used by Hash/List/Set/SortedSet.
        /// </summary>
        public override bool Operate(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
            => throw new NotSupportedException("Stream operations are dispatched via StreamManager, not the object Operate path.");

        /// <summary>
        /// Streams do not participate in generic key-space SCAN over members.
        /// </summary>
        public override unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = default, int patternLength = 0, bool isNoValue = false)
        {
            items = new List<byte[]>();
            cursor = 0;
        }

        /// <inheritdoc />
        public override void DoSerialize(BinaryWriter writer)
        {
            base.DoSerialize(writer);

            // Per-stream identity needed to re-open the log on deserialize.
            var hasDir = streamDirName != null;
            writer.Write(hasDir);
            if (hasDir)
                writer.Write(streamDirName);
            writer.Write(waitForCommit);

            // Stream metadata. Take the read lock to avoid torn reads against a concurrent AddEntry.
            _lock.ReadLock();
            try
            {
                WriteStreamID(writer, lastId);
                writer.Write(totalEntriesAdded);
                SerializeConsumerGroups(writer);
            }
            finally
            {
                _lock.ReadUnlock();
            }
        }

        /// <summary>Serialize all consumer groups (group cursor + entries-read + PEL + consumers).</summary>
        void SerializeConsumerGroups(BinaryWriter writer)
        {
            writer.Write(consumerGroups.Count);
            foreach (var group in consumerGroups.Values)
            {
                writer.Write(group.Name);
                WriteStreamID(writer, group.LastDeliveredId);
                writer.Write(group.EntriesRead);

                // Pending Entries List.
                writer.Write(group.PEL.Count);
                foreach (var pending in group.PEL.Values)
                {
                    WriteStreamID(writer, pending.Id);
                    writer.Write(pending.ConsumerName);
                    writer.Write(pending.DeliveryTime);
                    writer.Write(pending.DeliveryCount);
                }

                // Consumers (their PendingIds are rebuilt from the PEL on deserialize).
                writer.Write(group.Consumers.Count);
                foreach (var consumer in group.Consumers.Values)
                {
                    writer.Write(consumer.Name);
                    writer.Write(consumer.SeenTime);
                }
            }
        }

        /// <summary>Reconstruct consumer groups written by <see cref="SerializeConsumerGroups"/>.</summary>
        void DeserializeConsumerGroups(BinaryReader reader)
        {
            var groupCount = reader.ReadInt32();
            for (var g = 0; g < groupCount; g++)
            {
                var name = reader.ReadString();
                var lastDeliveredId = ReadStreamID(reader);
                var entriesRead = reader.ReadInt64();
                var group = new ConsumerGroup(name, lastDeliveredId, entriesRead);

                var pelCount = reader.ReadInt32();
                for (var p = 0; p < pelCount; p++)
                {
                    var id = ReadStreamID(reader);
                    var consumerName = reader.ReadString();
                    var deliveryTime = reader.ReadInt64();
                    var deliveryCount = reader.ReadInt32();
                    group.PEL[id] = new PendingEntry(id, consumerName, deliveryTime) { DeliveryCount = deliveryCount };
                }

                var consumerCount = reader.ReadInt32();
                for (var c = 0; c < consumerCount; c++)
                {
                    var consumerName = reader.ReadString();
                    var seenTime = reader.ReadInt64();
                    group.Consumers[consumerName] = new StreamConsumer(consumerName, seenTime);
                }

                // Rebuild each consumer's PendingIds index from the group PEL.
                foreach (var pending in group.PEL.Values)
                {
                    if (group.Consumers.TryGetValue(pending.ConsumerName, out var consumer))
                        consumer.PendingIds.Add(pending.Id);
                }

                consumerGroups[name] = group;
            }
        }

        static void WriteStreamID(BinaryWriter writer, StreamID id)
        {
            Span<byte> buf = stackalloc byte[16];
            MemoryMarshal.Write(buf, in id);
            writer.Write(buf);
        }

        static StreamID ReadStreamID(BinaryReader reader)
        {
            Span<byte> buf = stackalloc byte[16];
            var read = reader.Read(buf);
            if (read != buf.Length)
                throw new EndOfStreamException("Truncated StreamID in serialized stream object");
            return MemoryMarshal.Read<StreamID>(buf);
        }

        /// <summary>
        /// Walk the recovered log from BeginAddress to TailAddress and replay every record into
        /// the BTree, preserving deletes and trims via control-record markers. Called once during
        /// recovery before the stream is exposed to traffic, so no locking is needed.
        /// </summary>
        unsafe void RebuildIndexFromLog()
        {
            long begin = log.BeginAddress;
            long end = log.TailAddress;
            if (begin >= end) return;

            using var iter = log.Scan(begin, end, scanUncommitted: true);
            while (iter.GetNext(out byte[] entry, out _, out long currentAddress))
            {
                if (entry == null || entry.Length < sizeof(StreamLogEntryHeader)) continue;
                StreamLogEntryHeader header = MemoryMarshal.Read<StreamLogEntryHeader>(
                    new ReadOnlySpan<byte>(entry, 0, sizeof(StreamLogEntryHeader)));

                if (header.numPairs == ControlRecordKind.Tombstone)
                {
                    // XDEL of an entry that was later trimmed will fail to find anything in the
                    // BTree (its data record was pruned by log truncation). That's fine — Delete
                    // returns false and the recovered state already excludes the entry.
                    index.Delete((byte*)Unsafe.AsPointer(ref header.id.idBytes[0]));
                }
                else if (header.numPairs >= 0)
                {
                    // Data record.
                    index.InsertIntoTail((byte*)Unsafe.AsPointer(ref header.id.idBytes[0]), new Value(currentAddress));
                    lastId = header.id;
                    totalEntriesAdded++;
                }
                // Else: unknown negative kind. Skip defensively — could be a legacy marker.
            }
        }

        /// <summary>
        /// Asynchronously commit all pending log entries to disk. Does not block AddEntry/Read/Trim
        /// — the BTree continues to operate while pages flush in the background.
        /// </summary>
        public ValueTask CommitAsync(CancellationToken token = default)
        {
            // For NullDevice, commit is a no-op and returns immediately.
            return log.CommitAsync(cookie: null, token: token);
        }

        /// <summary>
        /// Increment the stream ID
        /// </summary>
        /// <param name="incrementedID">carries the incremented stream id</param>
        public void IncrementID(ref StreamID incrementedID)
        {
            var originalMs = lastId.getMS();
            var originalSeq = lastId.getSeq();

            if (originalMs == long.MaxValue)
            {
                incrementedID = default;
                return;
            }

            var newMs = originalMs;
            var newSeq = originalSeq + 1;

            // if seq overflows, increment timestamp and reset seq
            if (newSeq == 0)
            {
                newMs += 1;
                newSeq = 0;
            }

            incrementedID.setMS(newMs);
            incrementedID.setSeq(newSeq);

        }

        /// <summary>
        /// Generate the next stream ID
        /// </summary>
        /// <returns>StreamID generated</returns>
        public void GenerateNextID(ref StreamID id)
        {
            ulong timestamp = (ulong)Stopwatch.GetTimestamp() / (ulong)(Stopwatch.Frequency / 1000);

            // read existing timestamp in big endian format
            var lastTs = lastId.getMS();
            // if this is the first entry or timestamp is greater than last added entry
            if (totalEntriesAdded == 0 || timestamp > lastTs)
            {
                // this will write timestamp in big endian format
                id.setMS(timestamp);
                id.setSeq(0);
                return;
            }
            // if timestamp is same as last added entry, increment the sequence number
            // if seq overflows, increment timestamp and reset the sequence number
            IncrementID(ref id);
        }

        unsafe ParsedStreamEntryID parseIDString(PinnedSpanByte idSlice, ref StreamID id)
        {
            // if we have to auto-generate the whole ID
            if (*idSlice.ptr == '*' && idSlice.length == 1)
            {
                GenerateNextID(ref id);
                return ParsedStreamEntryID.VALID;
            }

            var lastIdDecodedTs = lastId.getMS();

            // parse user-defined ID
            // can be of following formats:
            // 1. ts (seq = 0)
            // 2. ts-* (auto-generate seq number)
            // 3. ts-seq

            // last character is a *
            if (*(idSlice.ptr + idSlice.length - 1) == '*')
            {
                // has to be of format ts-*,  check if '-' is the preceding character
                if (*(idSlice.ptr + idSlice.length - 2) != '-')
                {
                    return ParsedStreamEntryID.INVALID;
                }
                // parse the timestamp
                // slice the id to remove the last two characters
                var slicedId = PinnedSpanByte.FromPinnedPointer(idSlice.ptr, idSlice.length - 2);
                var idEnd = idSlice.ptr + idSlice.length - 2;
                if (!RespReadUtils.ReadUlong(out ulong timestamp, ref idSlice.ptr, idEnd))
                {
                    return ParsedStreamEntryID.INVALID;
                }

                // check if timestamp is greater than last added entry's decoded ts
                if (totalEntriesAdded != 0 && timestamp < lastIdDecodedTs)
                {
                    return ParsedStreamEntryID.NOT_GREATER;
                }
                else if (totalEntriesAdded != 0 && timestamp == lastIdDecodedTs)
                {
                    IncrementID(ref id);
                }
                else
                {
                    id.setMS(timestamp);
                    id.setSeq(0);
                }
            }
            else
            {
                // find index of '-' in the id
                int index = -1;
                for (int i = 0; i < idSlice.length; i++)
                {
                    if (*(idSlice.ptr + i) == '-')
                    {
                        index = i;
                        break;
                    }
                }
                // if '-' is not found, format should be just ts
                if (index == -1)
                {
                    if (!RespReadUtils.ReadUlong(out ulong timestamp, ref idSlice.ptr, idSlice.ptr + idSlice.length))
                    {
                        return ParsedStreamEntryID.INVALID;
                    }
                    // check if timestamp is greater than last added entry
                    if (totalEntriesAdded != 0 && timestamp < lastIdDecodedTs)
                    {
                        return ParsedStreamEntryID.NOT_GREATER;
                    }
                    else if (totalEntriesAdded != 0 && timestamp == lastIdDecodedTs)
                    {
                        IncrementID(ref id);
                    }
                    else
                    {
                        id.setMS(timestamp);
                        id.setSeq(0);
                    }
                }
                else
                {
                    // parse the timestamp
                    // slice the id to remove everything after '-'
                    var slicedId = PinnedSpanByte.FromPinnedPointer(idSlice.ptr, index);
                    var slicedSeq = PinnedSpanByte.FromPinnedPointer(idSlice.ptr + index + 1, idSlice.length - index - 1);
                    if (!RespReadUtils.ReadUlong(out ulong timestamp, ref idSlice.ptr, idSlice.ptr + index))
                    {
                        return ParsedStreamEntryID.INVALID;
                    }
                    var seqBegin = idSlice.ptr + index + 1;
                    var seqEnd = idSlice.ptr + idSlice.length;
                    if (!RespReadUtils.ReadUlong(out ulong seq, ref seqBegin, seqEnd))
                    {
                        return ParsedStreamEntryID.INVALID;
                    }

                    if (totalEntriesAdded != 0 && timestamp < lastIdDecodedTs)
                    {
                        return ParsedStreamEntryID.NOT_GREATER;
                    }
                    else if (totalEntriesAdded != 0 && timestamp == lastIdDecodedTs)
                    {
                        if (seq <= lastId.seq)
                        {
                            return ParsedStreamEntryID.INVALID;
                        }
                    }
                    // use ID and seq given by user
                    // encode while storing
                    id.setMS(timestamp);
                    id.setSeq(seq);
                }
            }

            return ParsedStreamEntryID.VALID;
        }

        /// <summary>
        /// Adds an entry or item to the stream
        /// </summary>
        public unsafe void AddEntry(PinnedSpanByte idSlice, int numPairs, ReadOnlySpan<byte> rawFieldValuePairs, ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            StreamID id = default;
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output);

            // take a lock to ensure thread safety
            _lock.WriteLock();
            try
            {
                var parsedIDStatus = parseIDString(idSlice, ref id);
                if (parsedIDStatus == ParsedStreamEntryID.INVALID)
                {
                    writer.WriteError(CmdStrings.RESP_ERR_XADD_INVALID_STREAM_ID);
                    return;
                }
                else if (parsedIDStatus == ParsedStreamEntryID.NOT_GREATER)
                {
                    writer.WriteError(CmdStrings.RESP_ERR_XADD_ID_NOT_GREATER);
                    return;
                }

                // add the entry to the log
                StreamLogEntryHeader header = new StreamLogEntryHeader
                {
                    id = id,
                    numPairs = numPairs,
                };

                log.Enqueue<StreamLogEntryHeader>(header, item: rawFieldValuePairs, out long returnedLogicalAddr);

                if (waitForCommit)
                    log.Commit(spinWait: true);

                // BTree append-only insert. parseIDString already enforces strict monotonic IDs,
                // so this never collides with an existing key.
                index.InsertIntoTail((byte*)Unsafe.AsPointer(ref id.idBytes[0]), new Value(returnedLogicalAddr));

                // copy encoded ms and seq
                lastId.ms = id.ms;
                lastId.seq = id.seq;

                totalEntriesAdded++;

                ulong idMS = id.getMS();
                ulong idSeq = id.getSeq();
                Span<byte> outputBuffer = stackalloc byte[(NumUtils.MaximumFormatInt64Length * 2) + 1];
                int len = NumUtils.WriteInt64((long)idMS, outputBuffer);
                outputBuffer[len++] = (byte)'-';
                len += NumUtils.WriteInt64((long)idSeq, outputBuffer.Slice(len));

                writer.WriteBulkString(outputBuffer.Slice(0, len));
            }
            finally
            {
                // log.Commit();
                _lock.WriteUnlock();
            }

        }

        /// <summary>
        /// Get current length of the stream (number of non-tombstoned entries in the stream)
        /// </summary>
        /// <returns>length of stream</returns>
        public ulong Length()
        {
            ulong len;
            _lock.ReadLock();
            try
            {
                len = index.ValidCount;
            }
            finally
            {
                _lock.ReadUnlock();
            }
            return len;
        }

        /// <summary>
        /// Deletes an entry from the stream
        /// </summary>
        /// <param name="idSlice">id of the stream entry to delete</param>
        /// <returns>true if entry was deleted successfully</returns>
        public unsafe bool DeleteEntry(PinnedSpanByte idSlice)
        {
            if (!parseCompleteID(idSlice, out StreamID entryID))
            {
                return false;
            }
            bool deleted;
            _lock.WriteLock();
            try
            {
                deleted = index.Delete((byte*)Unsafe.AsPointer(ref entryID.idBytes[0]));
                if (deleted)
                {
                    // Persist a tombstone marker so recovery doesn't resurrect this entry.
                    var marker = new StreamLogEntryHeader { id = entryID, numPairs = ControlRecordKind.Tombstone };
                    log.Enqueue<StreamLogEntryHeader>(marker, item: [], out _);

                    if (waitForCommit)
                        log.Commit(spinWait: true);
                }
            }
            finally
            {
                _lock.WriteUnlock();
            }
            return deleted;
        }


        // Read the last entry in the stream and into output
        internal unsafe void ReadLastEntry(ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var writer = new RespMemoryWriter(respProtocolVersion, ref output);
            try
            {
                _lock.ReadLock();
                try
                {
                    if (index.ValidCount == 0)
                    {
                        writer.WriteNull();
                        return;
                    }

                    // BTree retains tombstones until trim, so use LastAlive() to skip them.
                    var lastEntry = index.LastAlive();
                    if (!lastEntry.Value.Valid)
                    {
                        writer.WriteNull();
                        return;
                    }

                    long addressOnLog = lastEntry.Value.address;

                    using (var iter = log.Scan(addressOnLog, addressOnLog + 1, scanUncommitted: true))
                    {
                        if (iter.GetNext(out byte[] entry, out _, out _, out _))
                        {
                            // Wrap in array of 1 entry
                            writer.WriteArrayLength(1);
                            WriteEntryToWriter(entry, ref writer);
                        }
                        else
                        {
                            writer.WriteNull();
                        }
                    }
                }
                finally
                {
                    _lock.ReadUnlock();
                }
            }
            finally
            {
                writer.Dispose();
            }
        }

        /// <summary>
        /// Read entries from the stream from given range
        /// </summary>
        /// <param name="min">start of range</param>
        /// <param name="max">end of range</param>
        /// <param name="limit">threshold to scanning</param>
        /// <param name="output"></param>
        public unsafe void ReadRange(string min, string max, int limit, ref SpanByteAndMemory output, byte respProtocolVersion, bool isReverse = false)
        {
            var writer = new RespMemoryWriter(respProtocolVersion, ref output);
            try
            {
                _lock.ReadLock();
                try
                {
                    if (index.ValidCount == 0)
                    {
                        return;
                    }

                    // Sentinels for "-" and "+". StreamID is stored big-endian, so byte-wise
                    // comparison (used by BTree) gives the same ordering as numeric comparison.
                    StreamID minStreamID = new StreamID(0UL, 0UL);
                    StreamID maxStreamID = new StreamID(ulong.MaxValue, ulong.MaxValue);

                    StreamID startID, endID;
                    if (min == "-")
                    {
                        startID = minStreamID;
                    }
                    else if (min == "+")
                    {
                        startID = maxStreamID;
                    }
                    else if (!ParseStreamIDFromString(min, out startID))
                    {
                        return;
                    }

                    if (max == "+")
                    {
                        endID = maxStreamID;
                    }
                    else if (max == "-")
                    {
                        endID = minStreamID;
                    }
                    else if (!ParseStreamIDFromString(max, out endID))
                    {
                        return;
                    }

                    // BTree.Get asserts ordering. For reverse, start (larger) >= end (smaller).
                    int cmp = startID.CompareTo(endID);
                    if (isReverse ? cmp < 0 : cmp > 0)
                    {
                        writer.WriteArrayLength(0);
                        return;
                    }

                    byte* startPtr = (byte*)Unsafe.AsPointer(ref startID.idBytes[0]);
                    byte* endPtr = (byte*)Unsafe.AsPointer(ref endID.idBytes[0]);
                    int actualLimit = limit > 0 ? limit : -1;

                    int validCount = index.Get(startPtr, endPtr, out Value startVal, out Value endVal,
                        out List<Value> tombstones, actualLimit, isReverse);

                    if (validCount == 0)
                    {
                        writer.WriteArrayLength(0);
                        return;
                    }

                    HashSet<long> tombstoneAddrs = null;
                    if (tombstones != null && tombstones.Count > 0)
                    {
                        tombstoneAddrs = new HashSet<long>(tombstones.Count);
                        foreach (var t in tombstones)
                        {
                            tombstoneAddrs.Add(t.address);
                        }
                    }

                    // For reverse: startVal is at the higher address, endVal at the lower.
                    // For forward: startVal is at the lower address, endVal at the higher.
                    long scanStart = (isReverse ? endVal.address : startVal.address);
                    long scanEnd = (isReverse ? startVal.address : endVal.address);

                    // After XTRIM, BTree leaves still reference addresses below the new
                    // log.BeginAddress. Clamp so log.Scan never tries to read truncated pages.
                    long beginAddr = log.BeginAddress;
                    if (scanStart < beginAddr) scanStart = beginAddr;
                    if (scanEnd < beginAddr)
                    {
                        // Whole requested window was truncated; nothing to emit.
                        writer.WriteArrayLength(0);
                        return;
                    }

                    writer.WriteArrayLength(validCount);

                    if (isReverse)
                    {
                        // Log scans are forward-only; buffer entries and emit reversed.
                        var entries = new List<byte[]>(validCount);
                        using (var iter = log.Scan(scanStart, scanEnd + 1, scanUncommitted: true))
                        {
                            while (iter.GetNext(out byte[] entry, out _, out long currentAddress))
                            {
                                if (tombstoneAddrs != null && tombstoneAddrs.Contains(currentAddress))
                                    continue;
                                if (IsControlRecord(entry)) continue;
                                entries.Add(entry);
                                if (entries.Count >= validCount)
                                    break;
                            }
                        }
                        for (int i = entries.Count - 1; i >= 0; i--)
                        {
                            WriteEntryToWriter(entries[i], ref writer);
                        }
                    }
                    else
                    {
                        using (var iter = log.Scan(scanStart, scanEnd + 1, scanUncommitted: true))
                        {
                            int written = 0;
                            while (iter.GetNext(out byte[] entry, out _, out long currentAddress))
                            {
                                if (tombstoneAddrs != null && tombstoneAddrs.Contains(currentAddress))
                                    continue;
                                if (IsControlRecord(entry)) continue;
                                WriteEntryToWriter(entry, ref writer);
                                written++;
                                if (written >= validCount)
                                    break;
                            }
                        }
                    }
                }
                finally
                {
                    _lock.ReadUnlock();
                }
            }
            finally
            {
                writer.Dispose();
            }
        }

        /// <summary>
        /// Trims the stream based on the specified options.
        /// </summary>
        /// <param name="trimArg">length or ID specifying the threshold</param>
        /// <param name="optType">MAXLEN or MINID</param>
        /// <param name="entriesTrimmed">number of keys trimmed</param>
        /// <returns></returns>
        public unsafe bool Trim(PinnedSpanByte trimArg, StreamTrimOpts optType, out ulong entriesTrimmed, bool approximate = false)
        {
            _lock.WriteLock();
            try
            {
                Value newHead = default;
                switch (optType)
                {
                    case StreamTrimOpts.MAXLEN:
                        if (!RespReadUtils.ReadUlong(out ulong maxLen, ref trimArg.ptr, trimArg.ptr + trimArg.length))
                        {
                            entriesTrimmed = 0;
                            return false;
                        }
                        index.TrimByLength(maxLen, out entriesTrimmed, out newHead, out _, out _, approximate);
                        break;
                    case StreamTrimOpts.MINID:
                        if (!parseCompleteID(trimArg, out StreamID minID))
                        {
                            entriesTrimmed = 0;
                            return false;
                        }
                        index.TrimByID((byte*)Unsafe.AsPointer(ref minID.idBytes[0]),
                            out entriesTrimmed, out newHead, out _, out _);
                        break;
                    default:
                        entriesTrimmed = 0;
                        break;
                }

                if (entriesTrimmed > 0)
                {
                    // XTRIM always drops the oldest entries (a contiguous prefix of both the
                    // BTree key order and the log address order, since IDs and log addresses
                    // both grow monotonically). Truncating the log past the new head IS the
                    // persistence of the trim — recovery just won't see the dropped entries.
                    // If everything was trimmed, push past the current tail so nothing replays.
                    long target = newHead.Valid ? newHead.address : log.TailAddress;
                    log.TruncateUntil(target);

                    if (waitForCommit)
                        log.Commit(spinWait: true);
                }
                // Note: BTree leaves still reference tombstoned entries at addresses below the
                // new BeginAddress. Range reads handle this by clamping scanStart to BeginAddress
                // before calling log.Scan — see ReadRange.
            }
            finally
            {
                _lock.WriteUnlock();
            }
            return true;
        }

        /// <summary>
        /// True if the log entry is a control record (tombstone or trim marker) rather than a
        /// data entry. Control records can appear inside a forward log scan window between two
        /// data entries' addresses, so range-read paths must filter them out.
        /// </summary>
        static unsafe bool IsControlRecord(ReadOnlySpan<byte> entryBytes)
        {
            if (entryBytes.Length < sizeof(StreamLogEntryHeader)) return false;
            var header = MemoryMarshal.Read<StreamLogEntryHeader>(entryBytes.Slice(0, sizeof(StreamLogEntryHeader)));
            return header.numPairs < 0;
        }

        unsafe void WriteEntryToWriter(ReadOnlySpan<byte> entryBytes, ref RespMemoryWriter writer)
        {
            // each response entry is an array of two items: ID and array of key-value pairs
            writer.WriteArrayLength(2);

            // Read the first 20 bytes into our StreamLogEntryHeader struct
            StreamLogEntryHeader streamLogEntryHeader = MemoryMarshal.Read<StreamLogEntryHeader>(entryBytes.Slice(0, sizeof(StreamLogEntryHeader)));
            StreamID entryID = streamLogEntryHeader.id;

            // first item in the array is the ID
            WriteStreamIdToWriter(entryID, ref writer);

            // Second item is an array so write the subarray length
            int numPairs = streamLogEntryHeader.numPairs;
            writer.WriteArrayLength(numPairs);

            // this is a serialized ReadOnlySpan<byte> of field-value pairs, we want to copy it directly into the writer
            int serializedSpanLength = MemoryMarshal.Read<int>(entryBytes.Slice(sizeof(StreamLogEntryHeader)));
            int valueOffset = sizeof(StreamLogEntryHeader) + sizeof(int);
            ReadOnlySpan<byte> value = entryBytes.Slice(valueOffset, serializedSpanLength);
            writer.WriteDirect(value);
        }


        unsafe bool parseCompleteID(PinnedSpanByte idSlice, out StreamID streamID)
        {
            streamID = default;
            // complete ID is of the format ts-seq in input where both ts and seq are ulong
            // find the index of '-' in the id
            int index = -1;
            for (int i = 0; i < idSlice.length; i++)
            {
                if (*(idSlice.ptr + i) == '-')
                {
                    index = i;
                    break;
                }
            }
            // parse the timestamp
            if (!RespReadUtils.ReadUlong(out ulong timestamp, ref idSlice.ptr, idSlice.ptr + index))
            {
                return false;
            }

            // after reading the timestamp, the pointer will be at the '-' character
            var seqBegin = idSlice.ptr + 1;
            // parse the sequence number
            if (!RespReadUtils.ReadUlong(out ulong seq, ref seqBegin, idSlice.ptr + idSlice.length - 1))
            {
                return false;
            }
            streamID.setMS(timestamp);
            streamID.setSeq(seq);
            return true;
        }

        public static bool ParseCompleteStreamIDFromString(ReadOnlySpan<char> idString, out StreamID id)
        {
            id = default;
            int hyphenIdx = -1;
            for (int i = 0; i < idString.Length; i++)
            {
                if (idString[i] == '-')
                {
                    if (hyphenIdx != -1)
                    {
                        // more than 1 occurence of hypen
                        return false;
                    }
                    hyphenIdx = i;
                }
            }

            // no occurence of hypen
            if (hyphenIdx == -1)
                return false;

            if (!ulong.TryParse(idString.Slice(0, hyphenIdx), out ulong timestamp))
            {
                return false;
            }
            if (!ulong.TryParse(idString.Slice(hyphenIdx + 1), out ulong seq))
            {
                return false;
            }

            id.setMS(timestamp);
            id.setSeq(seq);
            return true;
        }

        public static bool ParseStreamIDFromString(ReadOnlySpan<char> idString, out StreamID id)
        {
            id = default;
            if (idString == "-" || idString == "+")
            {
                return false;
            }
            if (!idString.Contains('-'))
            {
                if (!ulong.TryParse(idString, out ulong ms))
                {
                    return false;
                }
                id.setMS(ms);
                id.setSeq(0);
                return true;
            }
            return ParseCompleteStreamIDFromString(idString, out id);
        }

        // Util to write without doing temp heap allocations
        private static void WriteStreamIdToWriter(StreamID id, ref RespMemoryWriter writer)
        {
            Span<byte> outputBuffer = stackalloc byte[(NumUtils.MaximumFormatInt64Length * 2) + 1];
            ulong idMS = id.getMS();
            ulong idSeq = id.getSeq();
            int len = NumUtils.WriteInt64((long)idMS, outputBuffer);
            outputBuffer[len++] = (byte)'-';
            len += NumUtils.WriteInt64((long)idSeq, outputBuffer.Slice(len));
            writer.WriteBulkString(outputBuffer.Slice(0, len));
        }

        #region Consumer Group Operations

        static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        /// <summary>
        /// Create a consumer group on this stream.
        /// </summary>
        /// <param name="groupName">Name of the group.</param>
        /// <param name="startId">The ID after which new entries will be delivered.
        /// Use <c>0-0</c> to deliver all existing entries, or pass the stream's last ID
        /// for "$" semantics.</param>
        /// <param name="entriesRead">Initial entries-read counter (-1 = unknown).</param>
        /// <returns>True if created; false if a group with that name already exists.</returns>
        public bool CreateGroup(string groupName, StreamID startId, long entriesRead)
        {
            _lock.WriteLock();
            try
            {
                if (consumerGroups.ContainsKey(groupName))
                    return false;
                consumerGroups[groupName] = new ConsumerGroup(groupName, startId, entriesRead);
                return true;
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }

        /// <summary>
        /// Destroy a consumer group. Returns true if the group existed.
        /// </summary>
        public bool DestroyGroup(string groupName)
        {
            _lock.WriteLock();
            try
            {
                return consumerGroups.Remove(groupName);
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }

        /// <summary>
        /// Set the last-delivered-id for a consumer group.
        /// </summary>
        public bool SetGroupId(string groupName, StreamID id, long entriesRead)
        {
            _lock.WriteLock();
            try
            {
                if (!consumerGroups.TryGetValue(groupName, out var group))
                    return false;
                group.LastDeliveredId = id;
                if (entriesRead >= 0)
                    group.EntriesRead = entriesRead;
                return true;
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }

        /// <summary>
        /// Explicitly create a consumer in a group. Returns false if it already exists.
        /// Returns null (via out bool?) if the group doesn't exist.
        /// </summary>
        public bool? CreateConsumer(string groupName, string consumerName)
        {
            _lock.WriteLock();
            try
            {
                if (!consumerGroups.TryGetValue(groupName, out var group))
                    return null;
                return group.CreateConsumer(consumerName, NowMs());
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }

        /// <summary>
        /// Delete a consumer from a group. Returns the number of pending entries removed,
        /// or -1 if the group doesn't exist.
        /// </summary>
        public int DeleteConsumer(string groupName, string consumerName)
        {
            _lock.WriteLock();
            try
            {
                if (!consumerGroups.TryGetValue(groupName, out var group))
                    return -1;
                return group.DeleteConsumer(consumerName);
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }

        /// <summary>
        /// Read entries from the stream for a consumer group (XREADGROUP).
        /// When <paramref name="id"/> is "&gt;", delivers new entries after the group's
        /// last-delivered-id. Otherwise, returns pending entries for the consumer starting
        /// from the given ID.
        /// </summary>
        /// <param name="groupName">Consumer group name.</param>
        /// <param name="consumerName">Consumer name (auto-created if needed).</param>
        /// <param name="id">"&gt;" for new entries, or a specific ID for pending re-read.</param>
        /// <param name="count">Max entries to return (-1 = unlimited).</param>
        /// <param name="noAck">If true, do not add to PEL on new delivery.</param>
        /// <param name="output">RESP output buffer.</param>
        /// <param name="respProtocolVersion">RESP protocol version.</param>
        /// <returns>True if group was found, false otherwise.</returns>
        public unsafe bool ReadGroup(string groupName, string consumerName, string id,
            int count, bool noAck, ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var writer = new RespMemoryWriter(respProtocolVersion, ref output);
            try
            {
                _lock.WriteLock();
                try
                {
                    if (!consumerGroups.TryGetValue(groupName, out var group))
                        return false;

                    long nowMs = NowMs();
                    var consumer = group.GetOrCreateConsumer(consumerName, nowMs);

                    if (id == ">")
                    {
                        ReadGroupNewEntries(group, consumer, count, noAck, nowMs, ref writer);
                    }
                    else
                    {
                        ReadGroupPendingEntries(group, consumer, id, count, ref writer);
                    }
                    return true;
                }
                finally
                {
                    _lock.WriteUnlock();
                }
            }
            finally
            {
                writer.Dispose();
            }
        }

        /// <summary>
        /// Deliver new entries (after group.LastDeliveredId) to the consumer.
        /// </summary>
        unsafe void ReadGroupNewEntries(ConsumerGroup group, StreamConsumer consumer,
            int count, bool noAck, long nowMs, ref RespMemoryWriter writer)
        {
            // Find entries after LastDeliveredId
            StreamID startID = group.LastDeliveredId;
            // We want entries strictly after LastDeliveredId. Increment seq to get exclusive start.
            ulong startSeq = startID.getSeq();
            if (startSeq < ulong.MaxValue)
            {
                startID.setSeq(startSeq + 1);
            }
            else
            {
                ulong startMs = startID.getMS();
                if (startMs < ulong.MaxValue)
                {
                    startID.setMS(startMs + 1);
                    startID.setSeq(0);
                }
                else
                {
                    // At the absolute maximum ID, nothing can follow
                    writer.WriteArrayLength(0);
                    return;
                }
            }

            StreamID endID = new StreamID(ulong.MaxValue, ulong.MaxValue);
            int actualLimit = count > 0 ? count : -1;

            byte* startPtr = (byte*)Unsafe.AsPointer(ref startID.idBytes[0]);
            byte* endPtr = (byte*)Unsafe.AsPointer(ref endID.idBytes[0]);

            int validCount = index.Get(startPtr, endPtr, out Value startVal, out Value endVal,
                out List<Value> tombstones, actualLimit, reverse: false);

            if (validCount == 0)
            {
                writer.WriteArrayLength(0);
                return;
            }

            HashSet<long> tombstoneAddrs = null;
            if (tombstones != null && tombstones.Count > 0)
            {
                tombstoneAddrs = new HashSet<long>(tombstones.Count);
                foreach (var t in tombstones)
                    tombstoneAddrs.Add(t.address);
            }

            long scanStart = startVal.address;
            long scanEnd = endVal.address;
            long beginAddr = log.BeginAddress;
            if (scanStart < beginAddr) scanStart = beginAddr;
            if (scanEnd < beginAddr)
            {
                writer.WriteArrayLength(0);
                return;
            }

            // Collect entries to deliver
            var entries = new List<(StreamID id, byte[] data)>(validCount);
            using (var iter = log.Scan(scanStart, scanEnd + 1, scanUncommitted: true))
            {
                while (iter.GetNext(out byte[] entry, out _, out long currentAddress))
                {
                    if (tombstoneAddrs != null && tombstoneAddrs.Contains(currentAddress))
                        continue;
                    if (IsControlRecord(entry)) continue;

                    var header = MemoryMarshal.Read<StreamLogEntryHeader>(
                        new ReadOnlySpan<byte>(entry, 0, sizeof(StreamLogEntryHeader)));
                    entries.Add((header.id, entry));
                    if (entries.Count >= validCount) break;
                }
            }

            writer.WriteArrayLength(entries.Count);
            foreach (var (entryId, entryData) in entries)
            {
                WriteEntryToWriter(entryData, ref writer);

                // Update group state
                group.LastDeliveredId = entryId;
                group.EntriesRead++;

                if (!noAck)
                {
                    group.AddPendingEntry(entryId, consumer, nowMs);
                }
            }
        }

        /// <summary>
        /// Return pending entries for the consumer starting at the given ID.
        /// </summary>
        unsafe void ReadGroupPendingEntries(ConsumerGroup group, StreamConsumer consumer,
            string idStr, int count, ref RespMemoryWriter writer)
        {
            StreamID startID;
            if (idStr == "0" || idStr == "0-0")
            {
                startID = new StreamID(0, 0);
            }
            else if (!ParseCompleteStreamIDFromString(idStr, out startID))
            {
                writer.WriteArrayLength(0);
                return;
            }

            // Walk consumer's pending IDs from startID
            var pendingView = consumer.PendingIds.GetViewBetween(startID, new StreamID(ulong.MaxValue, ulong.MaxValue));
            var pendingIds = new List<StreamID>(pendingView);

            if (count > 0 && pendingIds.Count > count)
                pendingIds.RemoveRange(count, pendingIds.Count - count);

            if (pendingIds.Count == 0)
            {
                writer.WriteArrayLength(0);
                return;
            }

            // Read each pending entry from the log
            writer.WriteArrayLength(pendingIds.Count);
            foreach (var pendingId in pendingIds)
            {
                if (!group.PEL.TryGetValue(pendingId, out var pe))
                    continue;

                // Update delivery metadata
                pe.DeliveryCount++;
                pe.DeliveryTime = NowMs();

                // Look up address from BTree and read from log
                var lookupId = pendingId;
                byte* keyPtr = (byte*)Unsafe.AsPointer(ref lookupId.idBytes[0]);
                var result = index.Get(keyPtr);
                if (!result.Valid)
                {
                    // Entry was trimmed/deleted; still in PEL but data gone
                    writer.WriteNull();
                    continue;
                }

                long addr = result.address;
                using var iter = log.Scan(addr, addr + 1, scanUncommitted: true);
                if (iter.GetNext(out byte[] entry, out _, out _))
                {
                    WriteEntryToWriter(entry, ref writer);
                }
                else
                {
                    writer.WriteNull();
                }
            }
        }

        /// <summary>
        /// Acknowledge entries in a consumer group (XACK).
        /// </summary>
        /// <returns>Number of entries acknowledged, or -1 if group not found.</returns>
        public int Acknowledge(string groupName, ReadOnlySpan<StreamID> ids)
        {
            _lock.WriteLock();
            try
            {
                if (!consumerGroups.TryGetValue(groupName, out var group))
                    return -1;
                return group.Acknowledge(ids, NowMs());
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }

        /// <summary>
        /// Get pending entries summary or detail (XPENDING).
        /// </summary>
        /// <param name="groupName">Consumer group name.</param>
        /// <param name="start">Start ID for range query (null for summary form).</param>
        /// <param name="end">End ID for range query.</param>
        /// <param name="count">Max entries to return.</param>
        /// <param name="minIdleTime">Minimum idle time filter in ms (-1 = no filter).</param>
        /// <param name="consumerFilter">Filter by consumer name (null = all consumers).</param>
        /// <param name="output">RESP output buffer.</param>
        /// <param name="respProtocolVersion">RESP protocol version.</param>
        /// <returns>True if group found, false otherwise.</returns>
        public bool GetPending(string groupName, string start, string end,
            int count, long minIdleTime, string consumerFilter,
            ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var writer = new RespMemoryWriter(respProtocolVersion, ref output);
            try
            {
                _lock.ReadLock();
                try
                {
                    if (!consumerGroups.TryGetValue(groupName, out var group))
                        return false;

                    long nowMs = NowMs();

                    if (start == null)
                    {
                        // Summary form
                        WritePendingSummary(group, nowMs, ref writer);
                    }
                    else
                    {
                        // Detail form
                        WritePendingDetail(group, start, end, count, minIdleTime, consumerFilter, nowMs, ref writer);
                    }
                    return true;
                }
                finally
                {
                    _lock.ReadUnlock();
                }
            }
            finally
            {
                writer.Dispose();
            }
        }

        void WritePendingSummary(ConsumerGroup group, long nowMs, ref RespMemoryWriter writer)
        {
            int total = group.PEL.Count;
            writer.WriteArrayLength(4);
            writer.WriteInt64(total);

            if (total == 0)
            {
                writer.WriteNull();
                writer.WriteNull();
                writer.WriteArrayLength(0);
                return;
            }

            // Min and max IDs in PEL
            WriteStreamIdToWriter(group.PEL.Keys[0], ref writer);
            WriteStreamIdToWriter(group.PEL.Keys[group.PEL.Count - 1], ref writer);

            // Per-consumer counts
            var consumerCounts = new Dictionary<string, int>(StringComparer.Ordinal);
            foreach (var pe in group.PEL.Values)
            {
                consumerCounts.TryGetValue(pe.ConsumerName, out int c);
                consumerCounts[pe.ConsumerName] = c + 1;
            }

            writer.WriteArrayLength(consumerCounts.Count);
            foreach (var kvp in consumerCounts)
            {
                writer.WriteArrayLength(2);
                writer.WriteBulkString(System.Text.Encoding.UTF8.GetBytes(kvp.Key));
                writer.WriteBulkString(System.Text.Encoding.UTF8.GetBytes(kvp.Value.ToString()));
            }
        }

        void WritePendingDetail(ConsumerGroup group, string startStr, string endStr,
            int count, long minIdleTime, string consumerFilter, long nowMs,
            ref RespMemoryWriter writer)
        {
            StreamID startID = startStr == "-" ? new StreamID(0, 0) : default;
            StreamID endID = endStr == "+" ? new StreamID(ulong.MaxValue, ulong.MaxValue) : default;
            if (startStr != "-" && !ParseStreamIDFromString(startStr, out startID))
            {
                writer.WriteArrayLength(0);
                return;
            }
            if (endStr != "+" && !ParseStreamIDFromString(endStr, out endID))
            {
                writer.WriteArrayLength(0);
                return;
            }

            var results = new List<PendingEntry>();
            foreach (var kvp in group.PEL)
            {
                if (kvp.Key.CompareTo(startID) < 0) continue;
                if (kvp.Key.CompareTo(endID) > 0) break;

                var pe = kvp.Value;
                if (consumerFilter != null && pe.ConsumerName != consumerFilter) continue;
                if (minIdleTime >= 0 && (nowMs - pe.DeliveryTime) < minIdleTime) continue;

                results.Add(pe);
                if (count > 0 && results.Count >= count) break;
            }

            writer.WriteArrayLength(results.Count);
            foreach (var pe in results)
            {
                writer.WriteArrayLength(4);
                WriteStreamIdToWriter(pe.Id, ref writer);
                writer.WriteBulkString(Encoding.UTF8.GetBytes(pe.ConsumerName));
                writer.WriteInt64(nowMs - pe.DeliveryTime);
                writer.WriteInt64(pe.DeliveryCount);
            }
        }

        /// <summary>
        /// Claim pending entries (XCLAIM).
        /// </summary>
        /// <returns>List of claimed entry data, or null if group not found.</returns>
        public unsafe bool ClaimEntries(string groupName, string consumerName, long minIdleTime,
            StreamID[] ids, long? idleMs, long? timeMs, int? retryCount, bool force, bool justId,
            ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var writer = new RespMemoryWriter(respProtocolVersion, ref output);
            try
            {
                _lock.WriteLock();
                try
                {
                    if (!consumerGroups.TryGetValue(groupName, out var group))
                        return false;

                    long nowMs = NowMs();
                    var consumer = group.GetOrCreateConsumer(consumerName, nowMs);
                    var claimed = new List<(StreamID id, byte[] data)>();

                    foreach (var id in ids)
                    {
                        if (!group.PEL.TryGetValue(id, out var pe))
                        {
                            if (!force) continue;
                            // FORCE: create a new PEL entry even if it doesn't exist
                            pe = new PendingEntry(id, consumerName, nowMs);
                            group.PEL[id] = pe;
                            consumer.PendingIds.Add(id);
                        }
                        else
                        {
                            long idle = nowMs - pe.DeliveryTime;
                            if (idle < minIdleTime && !force) continue;

                            // Transfer ownership: remove from old consumer
                            if (group.Consumers.TryGetValue(pe.ConsumerName, out var oldConsumer))
                            {
                                oldConsumer.PendingIds.Remove(id);
                            }

                            pe.ConsumerName = consumerName;
                            consumer.PendingIds.Add(id);
                        }

                        // Apply optional overrides
                        pe.DeliveryTime = timeMs ?? (idleMs.HasValue ? (nowMs - idleMs.Value) : nowMs);
                        if (retryCount.HasValue)
                            pe.DeliveryCount = retryCount.Value;
                        else
                            pe.DeliveryCount++;

                        if (!justId)
                        {
                            // Read entry data from log
                            var lookupId = id;
                            byte* keyPtr = (byte*)Unsafe.AsPointer(ref lookupId.idBytes[0]);
                            var result = index.Get(keyPtr);
                            if (result.Valid)
                            {
                                long addr = result.address;
                                using var iter = log.Scan(addr, addr + 1, scanUncommitted: true);
                                if (iter.GetNext(out byte[] entry, out _, out _))
                                {
                                    claimed.Add((id, entry));
                                    continue;
                                }
                            }
                        }
                        claimed.Add((id, null));
                    }

                    writer.WriteArrayLength(claimed.Count);
                    foreach (var (claimedId, data) in claimed)
                    {
                        if (justId)
                        {
                            WriteStreamIdToWriter(claimedId, ref writer);
                        }
                        else if (data != null)
                        {
                            WriteEntryToWriter(data, ref writer);
                        }
                        else
                        {
                            writer.WriteNull();
                        }
                    }
                    return true;
                }
                finally
                {
                    _lock.WriteUnlock();
                }
            }
            finally
            {
                writer.Dispose();
            }
        }

        /// <summary>
        /// Auto-claim idle pending entries (XAUTOCLAIM).
        /// </summary>
        public unsafe bool AutoClaim(string groupName, string consumerName, long minIdleTime,
            StreamID start, int count, bool justId,
            ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var writer = new RespMemoryWriter(respProtocolVersion, ref output);
            try
            {
                _lock.WriteLock();
                try
                {
                    if (!consumerGroups.TryGetValue(groupName, out var group))
                        return false;

                    long nowMs = NowMs();
                    var consumer = group.GetOrCreateConsumer(consumerName, nowMs);
                    var claimed = new List<(StreamID id, byte[] data)>();
                    var deletedIds = new List<StreamID>();
                    StreamID nextCursor = new StreamID(0, 0);
                    bool hasMore = false;

                    // Scan PEL from start
                    int scanned = 0;
                    foreach (var kvp in group.PEL)
                    {
                        if (kvp.Key.CompareTo(start) < 0) continue;

                        if (scanned >= count)
                        {
                            nextCursor = kvp.Key;
                            hasMore = true;
                            break;
                        }

                        var pe = kvp.Value;
                        long idle = nowMs - pe.DeliveryTime;
                        if (idle < minIdleTime)
                        {
                            scanned++;
                            continue;
                        }

                        // Check if entry still exists in the stream
                        var lookupId = kvp.Key;
                        byte* keyPtr = (byte*)Unsafe.AsPointer(ref lookupId.idBytes[0]);
                        var result = index.Get(keyPtr);
                        if (!result.Valid)
                        {
                            deletedIds.Add(kvp.Key);
                            scanned++;
                            continue;
                        }

                        // Transfer ownership
                        if (group.Consumers.TryGetValue(pe.ConsumerName, out var oldConsumer))
                        {
                            oldConsumer.PendingIds.Remove(kvp.Key);
                        }

                        pe.ConsumerName = consumerName;
                        pe.DeliveryTime = nowMs;
                        pe.DeliveryCount++;
                        consumer.PendingIds.Add(kvp.Key);

                        if (!justId)
                        {
                            long addr = result.address;
                            using var iter = log.Scan(addr, addr + 1, scanUncommitted: true);
                            if (iter.GetNext(out byte[] entry, out _, out _))
                            {
                                claimed.Add((kvp.Key, entry));
                            }
                            else
                            {
                                claimed.Add((kvp.Key, null));
                            }
                        }
                        else
                        {
                            claimed.Add((kvp.Key, null));
                        }
                        scanned++;
                    }

                    // Remove deleted entries from PEL
                    foreach (var did in deletedIds)
                    {
                        if (group.PEL.TryGetValue(did, out var dpe))
                        {
                            if (group.Consumers.TryGetValue(dpe.ConsumerName, out var dc))
                                dc.PendingIds.Remove(did);
                            group.PEL.Remove(did);
                        }
                    }

                    // Write response: [nextCursor, claimedEntries, deletedIds]
                    writer.WriteArrayLength(3);

                    // Next cursor (0-0 if scan complete)
                    if (!hasMore)
                        nextCursor = new StreamID(0, 0);
                    WriteStreamIdToWriter(nextCursor, ref writer);

                    // Claimed entries
                    writer.WriteArrayLength(claimed.Count);
                    foreach (var (claimedId, data) in claimed)
                    {
                        if (justId)
                        {
                            WriteStreamIdToWriter(claimedId, ref writer);
                        }
                        else if (data != null)
                        {
                            WriteEntryToWriter(data, ref writer);
                        }
                        else
                        {
                            writer.WriteNull();
                        }
                    }

                    // Deleted IDs
                    writer.WriteArrayLength(deletedIds.Count);
                    foreach (var did in deletedIds)
                    {
                        WriteStreamIdToWriter(did, ref writer);
                    }

                    return true;
                }
                finally
                {
                    _lock.WriteUnlock();
                }
            }
            finally
            {
                writer.Dispose();
            }
        }

        /// <summary>
        /// Get stream info (XINFO STREAM).
        /// </summary>
        public void GetStreamInfo(ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var writer = new RespMemoryWriter(respProtocolVersion, ref output);
            try
            {
                _lock.ReadLock();
                try
                {
                    // Return as a flat array of field-value pairs (like Redis XINFO STREAM)
                    writer.WriteArrayLength(14);

                    writer.WriteBulkString("length"u8);
                    writer.WriteInt64((long)index.ValidCount);

                    writer.WriteBulkString("radix-tree-keys"u8);
                    writer.WriteInt64(0); // N/A for BTree

                    writer.WriteBulkString("radix-tree-nodes"u8);
                    writer.WriteInt64(0); // N/A for BTree

                    writer.WriteBulkString("last-generated-id"u8);
                    WriteStreamIdToWriter(lastId, ref writer);

                    writer.WriteBulkString("groups"u8);
                    writer.WriteInt64(consumerGroups.Count);

                    writer.WriteBulkString("entries-added"u8);
                    writer.WriteInt64(totalEntriesAdded);

                    writer.WriteBulkString("recorded-first-entry-id"u8);
                    if (index.ValidCount > 0)
                    {
                        var first = index.FirstAlive();
                        if (first.Value.Valid)
                            WriteStreamIdToWriter(MemoryMarshal.Read<StreamID>(first.Key.AsSpan()), ref writer);
                        else
                            WriteStreamIdToWriter(new StreamID(0, 0), ref writer);
                    }
                    else
                    {
                        WriteStreamIdToWriter(new StreamID(0, 0), ref writer);
                    }
                }
                finally
                {
                    _lock.ReadUnlock();
                }
            }
            finally
            {
                writer.Dispose();
            }
        }

        /// <summary>
        /// Get consumer groups info (XINFO GROUPS).
        /// </summary>
        public bool GetGroupsInfo(ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var writer = new RespMemoryWriter(respProtocolVersion, ref output);
            try
            {
                _lock.ReadLock();
                try
                {
                    writer.WriteArrayLength(consumerGroups.Count);
                    foreach (var group in consumerGroups.Values)
                    {
                        writer.WriteArrayLength(10);

                        writer.WriteBulkString("name"u8);
                        writer.WriteBulkString(System.Text.Encoding.UTF8.GetBytes(group.Name));

                        writer.WriteBulkString("consumers"u8);
                        writer.WriteInt64(group.Consumers.Count);

                        writer.WriteBulkString("pending"u8);
                        writer.WriteInt64(group.PEL.Count);

                        writer.WriteBulkString("last-delivered-id"u8);
                        WriteStreamIdToWriter(group.LastDeliveredId, ref writer);

                        writer.WriteBulkString("entries-read"u8);
                        writer.WriteInt64(group.EntriesRead);
                    }
                    return true;
                }
                finally
                {
                    _lock.ReadUnlock();
                }
            }
            finally
            {
                writer.Dispose();
            }
        }

        /// <summary>
        /// Get consumers info for a group (XINFO CONSUMERS).
        /// </summary>
        public bool GetConsumersInfo(string groupName, ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var writer = new RespMemoryWriter(respProtocolVersion, ref output);
            try
            {
                _lock.ReadLock();
                try
                {
                    if (!consumerGroups.TryGetValue(groupName, out var group))
                        return false;

                    long nowMs = NowMs();
                    writer.WriteArrayLength(group.Consumers.Count);
                    foreach (var consumer in group.Consumers.Values)
                    {
                        writer.WriteArrayLength(6);

                        writer.WriteBulkString("name"u8);
                        writer.WriteBulkString(System.Text.Encoding.UTF8.GetBytes(consumer.Name));

                        writer.WriteBulkString("pending"u8);
                        writer.WriteInt64(consumer.PendingIds.Count);

                        writer.WriteBulkString("idle"u8);
                        writer.WriteInt64(nowMs - consumer.SeenTime);
                    }
                    return true;
                }
                finally
                {
                    _lock.ReadUnlock();
                }
            }
            finally
            {
                writer.Dispose();
            }
        }

        /// <summary>
        /// Read entries from the stream after the given ID (for XREAD, non-group).
        /// </summary>
        public unsafe void ReadAfter(StreamID afterId, int count, ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            // Compute exclusive start: afterId + 1
            StreamID startID = afterId;
            ulong seq = startID.getSeq();
            if (seq < ulong.MaxValue)
            {
                startID.setSeq(seq + 1);
            }
            else
            {
                ulong ms = startID.getMS();
                if (ms < ulong.MaxValue)
                {
                    startID.setMS(ms + 1);
                    startID.setSeq(0);
                }
                else
                {
                    using var w = new RespMemoryWriter(respProtocolVersion, ref output);
                    w.WriteArrayLength(0);
                    return;
                }
            }

            StreamID endID = new StreamID(ulong.MaxValue, ulong.MaxValue);
            // Reuse ReadRange infrastructure
            string startStr = $"{startID.getMS()}-{startID.getSeq()}";
            string endStr = "+";
            ReadRange(startStr, endStr, count, ref output, respProtocolVersion, isReverse: false);
        }

        #endregion Consumer Group Operations

        /// <inheritdoc/>
        public override void Dispose()
        {
            // Idempotent: BTree.Deallocate is a native free that would crash on a second call,
            // and a session cache may still hold a reference to a disposed StreamObject until
            // its FIFO eviction reclaims it or the next lookup detects IsDisposed and evicts.
            // Guard with the same flag the cache uses for staleness detection.
            if (disposed) return;

            // Publish the disposed flag *before* releasing native memory so any concurrent reader
            // that beat us to a cache lookup at least has a chance to observe it on its next
            // operation. Subsequent cache-hit code paths must check IsDisposed and re-resolve
            // through StreamManager.
            disposed = true;
            try
            {
                index.Deallocate();
                log.Dispose();
                device.Dispose();

                // Release the heavy managed graph (consumer groups, PELs, PendingEntry instances,
                // consumer-name strings, etc.) so it becomes GC-eligible immediately even if the
                // wrapper is pinned by a stale SessionStreamCache entry that never sees another
                // lookup for this key. Without this, a DELed stream that had a large pending list
                // would keep all of that managed state alive until the session disconnects or
                // FIFO-evicts the cache entry.
                consumerGroups.Clear();
            }
            finally
            { }
        }
    }
}