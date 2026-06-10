// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Concurrent;
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

    /// <summary>
    /// Stream sub-operations dispatched through <see cref="StreamObject.Operate"/> when a stream is
    /// stored as an <see cref="IGarnetObject"/> in the unified object store. Values must fit in the
    /// low <see cref="RespInputHeader.FlagMask"/> bits (0-31).
    /// </summary>
    public enum StreamOperation : byte
    {
        XADD,
        XLEN,
        XRANGE,
        XREVRANGE,
        XDEL,
        XTRIM,
        XSETID,
        XREAD,
        XLAST,
        XINFO_STREAM,
        XINFO_GROUPS,
        XINFO_CONSUMERS,
        XGROUP_CREATE,
        XGROUP_DESTROY,
        XGROUP_CREATECONSUMER,
        XGROUP_DELCONSUMER,
        XGROUP_SETID,
        XREADGROUP,
        XACK,
        XPENDING,
        XCLAIM,
        XAUTOCLAIM,
    }

    /// <summary>Options bitmask carried in <see cref="ObjectInput.arg1"/> for stream operations.</summary>
    [Flags]
    internal enum StreamAddOptions
    {
        None = 0,
        NoMkStream = 1,
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
    /// Server-global stream configuration and lifecycle registry. Streams are first-class objects in
    /// the unified store but are not per-database, so a single ambient holder suffices. It supplies the
    /// per-stream log settings needed when the object-store deserialization factory re-opens a stream
    /// (it only receives a <see cref="BinaryReader"/>), and tracks live stream instances so
    /// FLUSHDB/FLUSHALL can close their per-stream log handles before deleting the on-disk directories
    /// (the unified store retains the objects after a flush, so GC alone won't close the handles).
    /// Configured once at startup by <c>StoreWrapper</c>.
    /// </summary>
    internal static class StreamObjectConfig
    {
        internal static string StreamsRootDir;
        internal static long DefaultPageSize = 4096;
        internal static long DefaultMemorySize = 1L << 24;

        static readonly ConcurrentDictionary<StreamObject, byte> liveStreams = new();

        internal static void Configure(string streamsRootDir, long pageSize, long memorySize)
        {
            StreamsRootDir = streamsRootDir;
            DefaultPageSize = pageSize;
            DefaultMemorySize = memorySize;
        }

        internal static void Register(StreamObject stream) => liveStreams.TryAdd(stream, 0);

        internal static void Unregister(StreamObject stream) => liveStreams.TryRemove(stream, out _);

        /// <summary>
        /// FLUSHDB/FLUSHALL cleanup: close every live stream's per-stream log/device handle, then delete
        /// all per-stream on-disk directories. Streams are not per-database, so flushing any database
        /// wipes the entire stream namespace.
        /// </summary>
        internal static void FlushAll()
        {
            foreach (var stream in liveStreams.Keys)
            {
                try
                {
                    stream.Dispose();
                }
                catch
                {
                    // Best-effort: a failed dispose must not block the remaining cleanup.
                }
            }
            liveStreams.Clear();

            if (StreamsRootDir == null || !Directory.Exists(StreamsRootDir))
                return;

            foreach (var dir in Directory.EnumerateDirectories(StreamsRootDir))
            {
                try
                {
                    Directory.Delete(dir, recursive: true);
                }
                catch
                {
                    // Best-effort: leftover directories are reclaimed on the next FLUSH.
                }
            }
        }
    }

    public class StreamObject : GarnetObjectBase
    {
        // Rough heap-overhead estimate reported to the object store (refined as the BTree/log grow).
        const long StreamHeapOverhead = 4096;

        readonly IDevice device;
        readonly TsavoriteLog log;
        readonly BTree index;
        readonly string streamsRootDir;
        readonly string streamDirName;
        StreamID lastId;
        long totalEntriesAdded;
        // Set by Dispose. volatile because Dispose may run on the FLUSHDB/FLUSHALL thread while
        // another session concurrently observes the flag (e.g. via IsDisposed).
        volatile bool disposed;

        /// <summary>True once <see cref="Dispose"/> has run. Lets callers detect a stream whose
        /// per-stream log/BTree were released by DEL/EXPIRE or FLUSHDB/FLUSHALL.</summary>
        public bool IsDisposed => disposed;

        /// <summary>Consumer groups attached to this stream, keyed by group name.</summary>
        readonly Dictionary<string, ConsumerGroup> consumerGroups = new(StringComparer.Ordinal);

        public StreamID LastId
        {
            get
            {
                return lastId;
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
        /// <param name="recover">If true and a disk-backed log exists at the path, recover the
        ///     log and rebuild the in-memory BTree by scanning all entries.</param>
        public StreamObject(string streamsRootDir, string streamDirName, long pageSize, long memorySize, bool recover = false)
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

            if (recover)
            {
                RebuildIndexFromLog();
            }

            StreamObjectConfig.Register(this);
        }

        /// <summary>
        /// Reconstruct a stream from its serialized object-store form. Reads the per-stream identity,
        /// metadata, and consumer-group state from the blob, re-opens the per-stream log (using the
        /// server-global <see cref="StreamObjectConfig"/>), and rebuilds the BTree index.
        /// </summary>
        public StreamObject(BinaryReader reader)
            : base(reader, StreamHeapOverhead)
        {

            // Per-stream identity. The log directory name is persisted; the root dir and page/memory
            // sizes are server-global and come from the ambient config.
            var hasDir = reader.ReadBoolean();
            streamDirName = hasDir ? reader.ReadString() : null;
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

            StreamObjectConfig.Register(this);
        }

        /// <inheritdoc />
        public override byte Type => (byte)GarnetObjectType.Stream;

        /// <summary>
        /// Create a fresh stream for the given key using the server-global <see cref="StreamObjectConfig"/>.
        /// The per-stream log directory is derived from the hex-encoded key so it is filesystem-safe and
        /// reversible. Used by the object-store RMW initial-update path, where the record key is available.
        /// </summary>
        internal static StreamObject CreateForKey(ReadOnlySpan<byte> key)
        {
            var rootDir = StreamObjectConfig.StreamsRootDir;
            var dirName = rootDir != null ? Convert.ToHexString(key) : null;
            return new StreamObject(rootDir, dirName, StreamObjectConfig.DefaultPageSize, StreamObjectConfig.DefaultMemorySize);
        }

        /// <summary>
        /// Streams are mutated in place rather than copied; the per-stream log and device cannot be
        /// duplicated, so Clone shares this single owning instance.
        /// </summary>
        public override IHeapObject Clone() => this;

        /// <summary>
        /// Dispatch a stream sub-operation when the stream is stored as an object in the unified store.
        /// Args arrive via <paramref name="input"/>'s parse state (index 0 = first arg after the key);
        /// RESP output is written into <paramref name="output"/>'s buffer. Returns true on success.
        /// </summary>
        public override bool Operate(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            switch (input.header.StreamOp)
            {
                case StreamOperation.XADD:
                    OperateXAdd(ref input, ref output, respProtocolVersion);
                    return true;
                case StreamOperation.XLEN:
                    OperateXLen(ref output, respProtocolVersion);
                    return true;
                case StreamOperation.XRANGE:
                case StreamOperation.XREVRANGE:
                    OperateXRange(ref input, ref output, respProtocolVersion);
                    return true;
                case StreamOperation.XDEL:
                    OperateXDel(ref input, ref output, respProtocolVersion);
                    return true;
                case StreamOperation.XTRIM:
                    OperateXTrim(ref input, ref output, respProtocolVersion);
                    return true;
                case StreamOperation.XLAST:
                    ReadLastEntry(ref output.SpanByteAndMemory, respProtocolVersion);
                    return true;
                case StreamOperation.XGROUP_CREATE:
                    OperateXGroupCreate(ref input, ref output, respProtocolVersion);
                    return true;
                case StreamOperation.XGROUP_SETID:
                    OperateXGroupSetId(ref input, ref output, respProtocolVersion);
                    return true;
                case StreamOperation.XGROUP_DESTROY:
                    OperateXGroupDestroy(ref input, ref output, respProtocolVersion);
                    return true;
                case StreamOperation.XGROUP_CREATECONSUMER:
                    OperateXGroupCreateConsumer(ref input, ref output, respProtocolVersion);
                    return true;
                case StreamOperation.XGROUP_DELCONSUMER:
                    OperateXGroupDelConsumer(ref input, ref output, respProtocolVersion);
                    return true;
                case StreamOperation.XINFO_STREAM:
                    GetStreamInfo(ref output.SpanByteAndMemory, respProtocolVersion);
                    return true;
                case StreamOperation.XINFO_GROUPS:
                    GetGroupsInfo(ref output.SpanByteAndMemory, respProtocolVersion);
                    return true;
                case StreamOperation.XINFO_CONSUMERS:
                    OperateXInfoConsumers(ref input, ref output, respProtocolVersion);
                    return true;
                case StreamOperation.XACK:
                    OperateXAck(ref input, ref output, respProtocolVersion);
                    return true;
                case StreamOperation.XPENDING:
                    OperateXPending(ref input, ref output, respProtocolVersion);
                    return true;
                case StreamOperation.XCLAIM:
                    OperateXClaim(ref input, ref output, respProtocolVersion);
                    return true;
                case StreamOperation.XAUTOCLAIM:
                    OperateXAutoClaim(ref input, ref output, respProtocolVersion);
                    return true;
                case StreamOperation.XREADGROUP:
                    OperateXReadGroup(ref input, ref output, respProtocolVersion);
                    return true;
                case StreamOperation.XREAD:
                    OperateXRead(ref input, ref output, respProtocolVersion);
                    return true;
                default:
                    throw new NotSupportedException($"Stream operation {input.header.StreamOp} is not yet dispatched through Operate.");
            }
        }

        /// <summary>XRANGE/XREVRANGE: parseState = [key, start, end, (COUNT, n)].</summary>
        void OperateXRange(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var isReverse = input.header.StreamOp == StreamOperation.XREVRANGE;
            var startId = input.parseState.GetArgSliceByRef(1).ToString();
            var endId = input.parseState.GetArgSliceByRef(2).ToString();
            var count = -1;
            if (input.parseState.Count > 3)
            {
                var countStr = input.parseState.GetArgSliceByRef(4).ToString();
                if (!int.TryParse(countStr, out count))
                {
                    using var w = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);
                    w.WriteError(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                    return;
                }
            }
            ReadRange(startId, endId, count, ref output.SpanByteAndMemory, respProtocolVersion, isReverse);
        }

        /// <summary>XDEL: parseState = [key, id, id, ...]. Replies with the count deleted.</summary>
        void OperateXDel(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var deleted = 0;
            for (var i = 1; i < input.parseState.Count; i++)
            {
                if (DeleteEntry(input.parseState.GetArgSliceByRef(i)))
                    deleted++;
            }
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);
            writer.WriteInt64(deleted);
        }

        /// <summary>XTRIM: parseState = [key, MAXLEN|MINID, [~], threshold].</summary>
        void OperateXTrim(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var trimType = input.parseState.GetArgSliceByRef(1).ToString().ToUpper();
            var approximate = false;
            var trimArgIndex = 2;
            if (input.parseState.Count > 3 && input.parseState.GetArgSliceByRef(2).ToString() == "~")
            {
                approximate = true;
                trimArgIndex++;
            }
            var trimArg = input.parseState.GetArgSliceByRef(trimArgIndex);
            var optType = trimType switch
            {
                "MAXLEN" => StreamTrimOpts.MAXLEN,
                "MINID" => StreamTrimOpts.MINID,
                _ => StreamTrimOpts.NONE,
            };
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);
            if (!Trim(trimArg, optType, out var entriesTrimmed, approximate))
                writer.WriteError(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
            else
                writer.WriteInt64((long)entriesTrimmed);
        }

        /// <summary>
        /// XADD via the object path. parseState layout: [id, field1, value1, ...]. Re-encodes the
        /// field/value tokens into the RESP bulk-string payload the read path expects and appends.
        /// </summary>
        void OperateXAdd(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var idSlice = input.parseState.GetArgSliceByRef(0);
            var numPairs = input.parseState.Count - 1;
            var payload = EncodeFieldValuePairs(ref input.parseState, 1, out var rented, out var payloadLength);
            try
            {
                AddEntry(idSlice, numPairs, payload.Slice(0, payloadLength), ref output.SpanByteAndMemory, respProtocolVersion);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        void OperateXLen(ref ObjectOutput output, byte respProtocolVersion)
        {
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);
            writer.WriteInt64((long)Length());
        }

        /// <summary>XINFO CONSUMERS: parseState = [CONSUMERS, key, group].</summary>
        void OperateXInfoConsumers(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var groupName = input.parseState.GetArgSliceByRef(2).ToString();
            if (!GetConsumersInfo(groupName, ref output.SpanByteAndMemory, respProtocolVersion))
                WriteStreamError(ref output, respProtocolVersion, "NOGROUP No such consumer group"u8);
        }

        /// <summary>XACK: parseState = [key, group, id, id, ...].</summary>
        void OperateXAck(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var groupName = input.parseState.GetArgSliceByRef(1).ToString();
            var ids = new StreamID[input.parseState.Count - 2];
            for (var i = 0; i < ids.Length; i++)
            {
                if (!ParseCompleteStreamIDFromString(input.parseState.GetArgSliceByRef(i + 2).ToString(), out ids[i]))
                {
                    WriteStreamError(ref output, respProtocolVersion, CmdStrings.RESP_ERR_XADD_INVALID_STREAM_ID);
                    return;
                }
            }

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);
            var acked = Acknowledge(groupName, ids);
            if (acked < 0)
                writer.WriteError("NOGROUP No such consumer group"u8);
            else
                writer.WriteInt64(acked);
        }

        /// <summary>XPENDING: parseState = [key, group, ([IDLE ms] start end count [consumer])].</summary>
        void OperateXPending(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var groupName = input.parseState.GetArgSliceByRef(1).ToString();
            string start = null, end = null, consumerFilter = null;
            var count = -1;
            long minIdleTime = -1;

            if (input.parseState.Count > 2)
            {
                var argIdx = 2;
                var opt = input.parseState.GetArgSliceByRef(argIdx).ToString().ToUpperInvariant();
                if (opt == "IDLE" && argIdx + 1 < input.parseState.Count)
                {
                    if (!long.TryParse(input.parseState.GetArgSliceByRef(argIdx + 1).ToString(), out minIdleTime))
                    {
                        WriteStreamError(ref output, respProtocolVersion, CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                        return;
                    }
                    argIdx += 2;
                }
                if (argIdx + 2 < input.parseState.Count)
                {
                    start = input.parseState.GetArgSliceByRef(argIdx).ToString();
                    end = input.parseState.GetArgSliceByRef(argIdx + 1).ToString();
                    if (!int.TryParse(input.parseState.GetArgSliceByRef(argIdx + 2).ToString(), out count))
                    {
                        WriteStreamError(ref output, respProtocolVersion, CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                        return;
                    }
                    argIdx += 3;
                }
                if (argIdx < input.parseState.Count)
                    consumerFilter = input.parseState.GetArgSliceByRef(argIdx).ToString();
            }

            if (!GetPending(groupName, start, end, count, minIdleTime, consumerFilter, ref output.SpanByteAndMemory, respProtocolVersion))
                WriteStreamError(ref output, respProtocolVersion, "NOGROUP No such consumer group"u8);
        }

        /// <summary>XCLAIM: parseState = [key, group, consumer, min-idle, id..., options...].</summary>
        void OperateXClaim(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var groupName = input.parseState.GetArgSliceByRef(1).ToString();
            var consumerName = input.parseState.GetArgSliceByRef(2).ToString();
            if (!long.TryParse(input.parseState.GetArgSliceByRef(3).ToString(), out var minIdleTime))
            {
                WriteStreamError(ref output, respProtocolVersion, CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                return;
            }

            var idList = new List<StreamID>();
            long? idleMs = null, timeMs = null;
            int? retryCount = null;
            var force = false;
            var justId = false;
            var argIdx = 4;
            while (argIdx < input.parseState.Count)
            {
                var argStr = input.parseState.GetArgSliceByRef(argIdx).ToString();
                var upper = argStr.ToUpperInvariant();
                if (upper is "IDLE" or "TIME" or "RETRYCOUNT" or "FORCE" or "JUSTID")
                    break;
                if (!ParseCompleteStreamIDFromString(argStr, out var id))
                {
                    WriteStreamError(ref output, respProtocolVersion, CmdStrings.RESP_ERR_XADD_INVALID_STREAM_ID);
                    return;
                }
                idList.Add(id);
                argIdx++;
            }
            while (argIdx < input.parseState.Count)
            {
                var opt = input.parseState.GetArgSliceByRef(argIdx).ToString().ToUpperInvariant();
                switch (opt)
                {
                    case "IDLE":
                        if (argIdx + 1 >= input.parseState.Count || !long.TryParse(input.parseState.GetArgSliceByRef(argIdx + 1).ToString(), out var idle))
                        { WriteStreamError(ref output, respProtocolVersion, CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR); return; }
                        idleMs = idle; argIdx += 2; break;
                    case "TIME":
                        if (argIdx + 1 >= input.parseState.Count || !long.TryParse(input.parseState.GetArgSliceByRef(argIdx + 1).ToString(), out var time))
                        { WriteStreamError(ref output, respProtocolVersion, CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR); return; }
                        timeMs = time; argIdx += 2; break;
                    case "RETRYCOUNT":
                        if (argIdx + 1 >= input.parseState.Count || !int.TryParse(input.parseState.GetArgSliceByRef(argIdx + 1).ToString(), out var retry))
                        { WriteStreamError(ref output, respProtocolVersion, CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR); return; }
                        retryCount = retry; argIdx += 2; break;
                    case "FORCE": force = true; argIdx++; break;
                    case "JUSTID": justId = true; argIdx++; break;
                    default: WriteStreamError(ref output, respProtocolVersion, CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR); return;
                }
            }

            if (!ClaimEntries(groupName, consumerName, minIdleTime, idList.ToArray(), idleMs, timeMs, retryCount, force, justId, ref output.SpanByteAndMemory, respProtocolVersion))
                WriteStreamError(ref output, respProtocolVersion, "NOGROUP No such consumer group"u8);
        }

        /// <summary>XAUTOCLAIM: parseState = [key, group, consumer, min-idle, start, (COUNT n)(JUSTID)].</summary>
        void OperateXAutoClaim(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var groupName = input.parseState.GetArgSliceByRef(1).ToString();
            var consumerName = input.parseState.GetArgSliceByRef(2).ToString();
            if (!long.TryParse(input.parseState.GetArgSliceByRef(3).ToString(), out var minIdleTime))
            {
                WriteStreamError(ref output, respProtocolVersion, CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                return;
            }
            if (!ParseStreamIDFromString(input.parseState.GetArgSliceByRef(4).ToString(), out var start))
            {
                WriteStreamError(ref output, respProtocolVersion, CmdStrings.RESP_ERR_XADD_INVALID_STREAM_ID);
                return;
            }

            var count = 100;
            var justId = false;
            var argIdx = 5;
            while (argIdx < input.parseState.Count)
            {
                var opt = input.parseState.GetArgSliceByRef(argIdx).ToString().ToUpperInvariant();
                if (opt == "COUNT" && argIdx + 1 < input.parseState.Count)
                {
                    if (!int.TryParse(input.parseState.GetArgSliceByRef(argIdx + 1).ToString(), out count))
                    { WriteStreamError(ref output, respProtocolVersion, CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR); return; }
                    argIdx += 2;
                }
                else if (opt == "JUSTID")
                {
                    justId = true; argIdx++;
                }
                else
                {
                    WriteStreamError(ref output, respProtocolVersion, CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR); return;
                }
            }

            if (!AutoClaim(groupName, consumerName, minIdleTime, start, count, justId, ref output.SpanByteAndMemory, respProtocolVersion))
                WriteStreamError(ref output, respProtocolVersion, "NOGROUP No such consumer group"u8);
        }

        /// <summary>
        /// XREADGROUP single-stream slice: parseState = [group, consumer, id]; arg1 = NOACK flag,
        /// arg2 = count. Writes the entries sub-array; sets result1 = 1 if the group was found, else 0.
        /// </summary>
        void OperateXReadGroup(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var groupName = input.parseState.GetArgSliceByRef(0).ToString();
            var consumerName = input.parseState.GetArgSliceByRef(1).ToString();
            var id = input.parseState.GetArgSliceByRef(2).ToString();
            var found = ReadGroup(groupName, consumerName, id, input.arg2, input.arg1 != 0, ref output.SpanByteAndMemory, respProtocolVersion);
            output.result1 = found ? 1 : 0;
        }

        /// <summary>XREAD single-stream slice: parseState = [id]; arg2 = count. Writes the entries array.</summary>
        void OperateXRead(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var idStr = input.parseState.GetArgSliceByRef(0).ToString();
            StreamID afterId;
            if (idStr == "$")
                afterId = new StreamID(ulong.MaxValue, ulong.MaxValue);
            else if (!ParseStreamIDFromString(idStr, out afterId))
            {
                WriteStreamError(ref output, respProtocolVersion, CmdStrings.RESP_ERR_XADD_INVALID_STREAM_ID);
                return;
            }
            ReadAfter(afterId, input.arg2, ref output.SpanByteAndMemory, respProtocolVersion);
        }

        static void WriteStreamError(ref ObjectOutput output, byte respProtocolVersion, scoped ReadOnlySpan<byte> error)
        {
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);
            writer.WriteError(error);
        }

        /// <summary>XGROUP CREATE: parseState = [CREATE, key, group, id, (MKSTREAM | ENTRIESREAD n)...].</summary>
        void OperateXGroupCreate(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var groupName = input.parseState.GetArgSliceByRef(2).ToString();
            var idStr = input.parseState.GetArgSliceByRef(3).ToString();
            long entriesRead = -1;
            for (var i = 4; i < input.parseState.Count; i++)
            {
                var opt = input.parseState.GetArgSliceByRef(i).ToString().ToUpperInvariant();
                if (opt == "ENTRIESREAD" && i + 1 < input.parseState.Count)
                {
                    _ = long.TryParse(input.parseState.GetArgSliceByRef(i + 1).ToString(), out entriesRead);
                    i++;
                }
                // MKSTREAM was applied at creation time (carried in arg1); ignore here.
            }

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);
            StreamID startId;
            if (idStr == "$")
            {
                startId = default;
            }
            else if (idStr is "0" or "0-0")
            {
                startId = new StreamID(0, 0);
                if (entriesRead < 0) entriesRead = 0;
            }
            else if (!ParseCompleteStreamIDFromString(idStr, out startId))
            {
                writer.WriteError(CmdStrings.RESP_ERR_XADD_INVALID_STREAM_ID);
                return;
            }

            if (CreateGroup(groupName, startId, entriesRead))
                writer.WriteSimpleString("OK"u8);
            else
                writer.WriteError("ERR no such key"u8);
        }

        /// <summary>XGROUP SETID: parseState = [SETID, key, group, id, (ENTRIESREAD n)].</summary>
        void OperateXGroupSetId(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var groupName = input.parseState.GetArgSliceByRef(2).ToString();
            var idStr = input.parseState.GetArgSliceByRef(3).ToString();
            long entriesRead = -1;
            if (input.parseState.Count >= 6)
            {
                var opt = input.parseState.GetArgSliceByRef(4).ToString().ToUpperInvariant();
                if (opt == "ENTRIESREAD")
                    _ = long.TryParse(input.parseState.GetArgSliceByRef(5).ToString(), out entriesRead);
            }

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);
            StreamID id;
            if (idStr == "$")
            {
                id = default;
            }
            else if (!ParseCompleteStreamIDFromString(idStr, out id))
            {
                writer.WriteError(CmdStrings.RESP_ERR_XADD_INVALID_STREAM_ID);
                return;
            }

            if (SetGroupId(groupName, id, entriesRead))
                writer.WriteSimpleString("OK"u8);
            else
                writer.WriteError("NOGROUP No such consumer group"u8);
        }

        /// <summary>XGROUP DESTROY: parseState = [DESTROY, key, group].</summary>
        void OperateXGroupDestroy(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var groupName = input.parseState.GetArgSliceByRef(2).ToString();
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);
            writer.WriteInt64(DestroyGroup(groupName) ? 1 : 0);
        }

        /// <summary>XGROUP CREATECONSUMER: parseState = [CREATECONSUMER, key, group, consumer].</summary>
        void OperateXGroupCreateConsumer(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var groupName = input.parseState.GetArgSliceByRef(2).ToString();
            var consumerName = input.parseState.GetArgSliceByRef(3).ToString();
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);
            var result = CreateConsumer(groupName, consumerName);
            if (result == null)
                writer.WriteError("NOGROUP No such consumer group"u8);
            else
                writer.WriteInt64(result.Value ? 1 : 0);
        }

        /// <summary>XGROUP DELCONSUMER: parseState = [DELCONSUMER, key, group, consumer].</summary>
        void OperateXGroupDelConsumer(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var groupName = input.parseState.GetArgSliceByRef(2).ToString();
            var consumerName = input.parseState.GetArgSliceByRef(3).ToString();
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);
            var removed = DeleteConsumer(groupName, consumerName);
            if (removed < 0)
                writer.WriteError("NOGROUP No such consumer group"u8);
            else
                writer.WriteInt64(removed);
        }

        /// <summary>
        /// Re-encode the field/value tokens in <paramref name="parseState"/> (from <paramref name="startIdx"/>)
        /// as a contiguous sequence of RESP bulk strings (<c>$len\r\n&lt;bytes&gt;\r\n</c>) — the exact
        /// payload layout the stream read path copies verbatim. Rents a buffer from the shared pool;
        /// the caller must return <paramref name="rented"/>.
        /// </summary>
        static Span<byte> EncodeFieldValuePairs(ref SessionParseState parseState, int startIdx, out byte[] rented, out int length)
        {
            // Over-estimate each token's serialized size: '$' + up to 20 length digits + CRLF + data + CRLF.
            long size = 0;
            for (var i = startIdx; i < parseState.Count; i++)
                size += 25 + parseState.GetArgSliceByRef(i).Length;

            rented = ArrayPool<byte>.Shared.Rent((int)size);
            var off = 0;
            for (var i = startIdx; i < parseState.Count; i++)
            {
                var span = parseState.GetArgSliceByRef(i).ReadOnlySpan;
                rented[off++] = (byte)'$';
                off += NumUtils.WriteInt64(span.Length, rented.AsSpan(off));
                rented[off++] = (byte)'\r';
                rented[off++] = (byte)'\n';
                span.CopyTo(rented.AsSpan(off));
                off += span.Length;
                rented[off++] = (byte)'\r';
                rented[off++] = (byte)'\n';
            }
            length = off;
            return rented.AsSpan(0, off);
        }

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

            // Stream metadata.
            // Flush the per-stream log so entries up to the serialized metadata are durable on disk.
            // Recovery re-opens this log and rebuilds the BTree from it; uncommitted tail records
            // would be lost on restart. No-op for in-memory (NullDevice) streams.
            log.Commit(spinWait: true);

            WriteStreamID(writer, lastId);
            writer.Write(totalEntriesAdded);
            SerializeConsumerGroups(writer);
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

        /// <summary>
        /// Get current length of the stream (number of non-tombstoned entries in the stream)
        /// </summary>
        /// <returns>length of stream</returns>
        public ulong Length()
        {
            ulong len;
            len = index.ValidCount;
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
            deleted = index.Delete((byte*)Unsafe.AsPointer(ref entryID.idBytes[0]));
            if (deleted)
            {
                // Persist a tombstone marker so recovery doesn't resurrect this entry.
                var marker = new StreamLogEntryHeader { id = entryID, numPairs = ControlRecordKind.Tombstone };
                log.Enqueue<StreamLogEntryHeader>(marker, item: [], out _);
            }
            return deleted;
        }


        // Read the last entry in the stream and into output
        internal unsafe void ReadLastEntry(ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var writer = new RespMemoryWriter(respProtocolVersion, ref output);
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
            }
            // Note: BTree leaves still reference tombstoned entries at addresses below the
            // new BeginAddress. Range reads handle this by clamping scanStart to BeginAddress
            // before calling log.Scan — see ReadRange.
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
            if (consumerGroups.ContainsKey(groupName))
                return false;
            consumerGroups[groupName] = new ConsumerGroup(groupName, startId, entriesRead);
            return true;
        }

        /// <summary>
        /// Destroy a consumer group. Returns true if the group existed.
        /// </summary>
        public bool DestroyGroup(string groupName)
        {
            return consumerGroups.Remove(groupName);
        }

        /// <summary>
        /// Set the last-delivered-id for a consumer group.
        /// </summary>
        public bool SetGroupId(string groupName, StreamID id, long entriesRead)
        {
            if (!consumerGroups.TryGetValue(groupName, out var group))
                return false;
            group.LastDeliveredId = id;
            if (entriesRead >= 0)
                group.EntriesRead = entriesRead;
            return true;
        }

        /// <summary>
        /// Explicitly create a consumer in a group. Returns false if it already exists.
        /// Returns null (via out bool?) if the group doesn't exist.
        /// </summary>
        public bool? CreateConsumer(string groupName, string consumerName)
        {
            if (!consumerGroups.TryGetValue(groupName, out var group))
                return null;
            return group.CreateConsumer(consumerName, NowMs());
        }

        /// <summary>
        /// Delete a consumer from a group. Returns the number of pending entries removed,
        /// or -1 if the group doesn't exist.
        /// </summary>
        public int DeleteConsumer(string groupName, string consumerName)
        {
            if (!consumerGroups.TryGetValue(groupName, out var group))
                return -1;
            return group.DeleteConsumer(consumerName);
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
            if (!consumerGroups.TryGetValue(groupName, out var group))
                return -1;
            return group.Acknowledge(ids, NowMs());
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

        /// <summary>Number of consumer groups attached to this stream. For tests/diagnostics.</summary>
        internal int ConsumerGroupCount => consumerGroups.Count;

        /// <summary>
        /// Delete this stream's on-disk directory (per-stream log + segment files). Call only on actual
        /// key removal (DEL / UNLINK / FLUSH) — never on eviction, where the data must survive on disk.
        /// <see cref="Dispose"/> must run first so the device/log file handles are closed.
        /// </summary>
        internal void DeleteOnDiskData()
        {
            if (streamsRootDir == null || streamDirName == null)
                return;
            try
            {
                var dir = Path.Combine(streamsRootDir, streamDirName);
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
            }
            catch
            {
                // Best-effort cleanup; a leftover directory is reclaimed on the next FLUSH or by recovery skipping it.
            }
        }

        /// <summary>Try to get a consumer group by name. For tests/diagnostics.</summary>
        internal bool TryGetConsumerGroupForTest(string name, out ConsumerGroup group) => consumerGroups.TryGetValue(name, out group);

        /// <inheritdoc/>
        public override void Dispose()
        {
            // Idempotent: BTree.Deallocate is a native free that would crash on a second call.
            // The unified store may still hold a reference to a disposed StreamObject (e.g. after a
            // FLUSHDB that evicts records without firing OnDispose), so guard with this flag.
            if (disposed) return;

            // Publish the disposed flag *before* releasing native memory so any concurrent reader
            // observes it and re-resolves through the store on its next operation.
            disposed = true;
            StreamObjectConfig.Unregister(this);
            try
            {
                index.Deallocate();
                log.Dispose();
                device.Dispose();

                // Release the heavy managed graph (consumer groups, PELs, PendingEntry instances,
                // consumer-name strings, etc.) so it becomes GC-eligible immediately. Without this,
                // a DELed stream that had a large pending list would keep all of that managed state
                // alive until the store record itself is reclaimed.
                consumerGroups.Clear();
            }
            finally
            { }
        }
    }
}