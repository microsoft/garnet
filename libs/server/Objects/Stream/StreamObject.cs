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
        /// Dispose every live stream's in-memory resources (BTree, per-stream <see cref="TsavoriteLog"/> and
        /// its <see cref="Tsavorite.core.LightEpoch"/>, device handle) and clear the registry, leaving the
        /// on-disk data intact. Called on graceful server shutdown so a subsequent recovery can re-open the
        /// logs. Streams are a process-global namespace, so this disposes streams across all databases.
        /// </summary>
        internal static void DisposeAll()
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
        }

        /// <summary>
        /// FLUSHDB cleanup for a single database: dispose that database's live streams (closing each
        /// per-stream log/device handle) and delete only its on-disk subtree (streamsRootDir/db{dbId}).
        /// Streams are per-database, so other databases' streams are left untouched.
        /// </summary>
        internal static void FlushDatabase(int dbId)
        {
            foreach (var stream in liveStreams.Keys)
            {
                if (stream.DbId != dbId)
                    continue;
                try
                {
                    // Dispose unregisters the stream from liveStreams.
                    stream.Dispose();
                }
                catch
                {
                    // Best-effort: a failed dispose must not block the remaining cleanup.
                }
            }

            if (StreamsRootDir == null)
                return;
            var dbDir = Path.Combine(StreamsRootDir, StreamObject.DbDirName(dbId));
            if (!Directory.Exists(dbDir))
                return;
            try
            {
                Directory.Delete(dbDir, recursive: true);
            }
            catch
            {
                // Best-effort: leftover directories are reclaimed on the next FLUSH.
            }
        }

        /// <summary>
        /// FLUSHALL cleanup: dispose every live stream's per-stream log/device handle, then delete all
        /// per-database on-disk subtrees under the streams root.
        /// </summary>
        internal static void FlushAll()
        {
            DisposeAll();

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

    public partial class StreamObject : GarnetObjectBase
    {
        // Rough heap-overhead estimate reported to the object store (refined as the BTree/log grow).
        const long StreamHeapOverhead = 4096;

        readonly IDevice device;
        readonly TsavoriteLog log;
        readonly BTree index;
        readonly string streamsRootDir;
        readonly string streamDirName;

        // Database this stream belongs to. Streams are per-database (like every other key), so the
        // per-stream on-disk log is namespaced by db id under streamsRootDir; this is persisted in the
        // blob so deserialize re-opens the correct directory, and used to scope FLUSHDB cleanup.
        readonly int dbId;
        StreamID lastId;
        long totalEntriesAdded;

        /// <summary>Database this stream belongs to. Used to scope FLUSHDB cleanup.</summary>
        internal int DbId => dbId;
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
        public StreamObject(string streamsRootDir, string streamDirName, long pageSize, long memorySize, int dbId = 0, bool recover = false)
            : base(StreamHeapOverhead)
        {
            this.streamsRootDir = streamsRootDir;
            this.streamDirName = streamDirName;
            this.dbId = dbId;
            if (streamsRootDir == null || streamDirName == null)
            {
                device = new NullDevice();
            }
            else
            {
                var streamDir = Path.Combine(streamsRootDir, DbDirName(dbId), streamDirName);
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

            // Per-stream identity. The log directory name + db id are persisted; the root dir and
            // page/memory sizes are server-global and come from the ambient config.
            var hasDir = reader.ReadBoolean();
            streamDirName = hasDir ? reader.ReadString() : null;
            dbId = reader.ReadInt32();
            streamsRootDir = StreamObjectConfig.StreamsRootDir;

            if (streamsRootDir == null || streamDirName == null)
            {
                device = new NullDevice();
            }
            else
            {
                var streamDir = Path.Combine(streamsRootDir, DbDirName(dbId), streamDirName);
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

            // Rebuild the index by scanning the recovered log. In-memory (NullDevice) streams have no
            // durable log, so there is nothing to rebuild — and scanning a fresh NullDevice log would
            // fault on uninitialized pages. On-disk streams rebuild from their committed log.
            if (streamsRootDir != null && streamDirName != null)
                RebuildIndexFromLog();

            lastId = blobLastId;
            totalEntriesAdded = blobTotalEntriesAdded;

            StreamObjectConfig.Register(this);
        }

        /// <inheritdoc />
        public override byte Type => (byte)GarnetObjectType.Stream;

        /// <summary>
        /// Create a fresh stream for the given key in the given database using the server-global
        /// <see cref="StreamObjectConfig"/>. The per-stream log directory is derived from the db id and
        /// the hex-encoded key so it is filesystem-safe, reversible, and isolated per database. Used by
        /// the object-store RMW initial-update path, where the record key and db id are available.
        /// </summary>
        internal static StreamObject CreateForKey(ReadOnlySpan<byte> key, int dbId)
        {
            var rootDir = StreamObjectConfig.StreamsRootDir;
            var dirName = rootDir != null ? Convert.ToHexString(key) : null;
            return new StreamObject(rootDir, dirName, StreamObjectConfig.DefaultPageSize, StreamObjectConfig.DefaultMemorySize, dbId);
        }

        /// <summary>Per-database subdirectory name under the streams root (e.g. "db0").</summary>
        internal static string DbDirName(int dbId) => $"db{dbId}";

        /// <summary>
        /// A stream owns a per-stream log + device + native BTree that cannot be duplicated, so it is a
        /// single-owner resource and <see cref="Clone"/> returns the same instance rather than copying.
        /// This is sound because the store transfers ownership rather than aliasing: on an RMW CopyUpdate,
        /// Tsavorite shallow-clones the value into the destination record and then clears the source
        /// record's reference (DisposeReason.CopyUpdated), so exactly one live record references the
        /// instance at any time. Eviction/DEL therefore dispose it safely (see GarnetRecordTriggers).
        /// NOTE: enabling the read cache for streams would break this invariant (the read-cache copy and
        /// the main-log record would both reference this instance while the main-log record can be
        /// evicted+disposed); streams are not read-cached.
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
                AddEntry(idSlice, numPairs, payload.Slice(0, payloadLength), ref output.SpanByteAndMemory, respProtocolVersion, out var idWasAutoGenerated, out var resolvedIdLength);

                // Freeze a server-generated id into the command that is logged to the AOF /
                // propagated to replicas, so replay reproduces the same id instead of generating a
                // new one from the (different) replay-time clock.
                if (idWasAutoGenerated)
                    FreezeAutoGeneratedId(ref input, resolvedIdLength);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        /// <summary>
        /// Rewrite the XADD id argument (parseState slot 0) from its server-generation token
        /// ('*' / 'ms-*') to the concrete resolved "ms-seq" id held in <c>resolvedIdAscii</c>.
        /// This runs after <see cref="AddEntry"/> and before the object-store RMW callback logs the
        /// input, so the AOF/replication record is deterministic: replay parses the concrete id
        /// verbatim instead of generating a fresh one. Safe because same-key RMWs serialize under
        /// the record lock and the logger copies the bytes synchronously immediately afterward.
        /// </summary>
        unsafe void FreezeAutoGeneratedId(ref ObjectInput input, int resolvedIdLength)
        {
            var idPtr = (byte*)Unsafe.AsPointer(ref resolvedIdAscii[0]);
            input.parseState.SetArgument(0, PinnedSpanByte.FromPinnedPointer(idPtr, resolvedIdLength));
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
        /// arg2 = count. Writes the entries sub-array; sets result1 = number of entries written, or -1
        /// if the group was not found (so the caller can emit NOGROUP and omit empty "&gt;" streams).
        /// </summary>
        void OperateXReadGroup(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            var groupName = input.parseState.GetArgSliceByRef(0).ToString();
            var consumerName = input.parseState.GetArgSliceByRef(1).ToString();
            var id = input.parseState.GetArgSliceByRef(2).ToString();
            output.result1 = ReadGroup(groupName, consumerName, id, input.arg2, input.arg1 != 0, ref output.SpanByteAndMemory, respProtocolVersion);
        }

        /// <summary>XREAD single-stream slice: parseState = [id]; arg2 = count. Writes the entries array
        /// and sets result1 = number of entries found (0 if none, so the caller can omit empty streams).</summary>
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
            output.result1 = ReadAfter(afterId, input.arg2, ref output.SpanByteAndMemory, respProtocolVersion);
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
            writer.Write(dbId);

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
                    // Split the id into the timestamp (before '-') and sequence (after '-')
                    // parts. RespReadUtils.ReadUlong advances the `ref` pointer it is given, so
                    // parse each part through its own local pointer to avoid mutating idSlice.ptr
                    // (which would shift the sequence parse off the end of the id and into the
                    // following command argument).
                    var slicedId = PinnedSpanByte.FromPinnedPointer(idSlice.ptr, index);
                    var slicedSeq = PinnedSpanByte.FromPinnedPointer(idSlice.ptr + index + 1, idSlice.length - index - 1);

                    var tsPtr = slicedId.ptr;
                    if (!RespReadUtils.ReadUlong(out ulong timestamp, ref tsPtr, slicedId.ptr + slicedId.length))
                    {
                        return ParsedStreamEntryID.INVALID;
                    }
                    var seqPtr = slicedSeq.ptr;
                    if (!RespReadUtils.ReadUlong(out ulong seq, ref seqPtr, slicedSeq.ptr + slicedSeq.length))
                    {
                        return ParsedStreamEntryID.INVALID;
                    }

                    if (totalEntriesAdded != 0 && timestamp < lastIdDecodedTs)
                    {
                        return ParsedStreamEntryID.NOT_GREATER;
                    }
                    else if (totalEntriesAdded != 0 && timestamp == lastIdDecodedTs)
                    {
                        // Compare against the decoded sequence — lastId.seq holds the
                        // big-endian-encoded bytes, not the logical value.
                        if (seq <= lastId.getSeq())
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
            // parse the timestamp and sequence through their own local pointers: RespReadUtils
            // .ReadUlong advances the ref pointer it is given, so reusing idSlice.ptr would shift
            // the sequence parse past the id slice into adjacent memory.
            var tsPtr = idSlice.ptr;
            if (!RespReadUtils.ReadUlong(out ulong timestamp, ref tsPtr, idSlice.ptr + index))
            {
                return false;
            }

            var seqPtr = idSlice.ptr + index + 1;
            // parse the sequence number
            if (!RespReadUtils.ReadUlong(out ulong seq, ref seqPtr, idSlice.ptr + idSlice.length))
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
                var dir = Path.Combine(streamsRootDir, DbDirName(dbId), streamDirName);
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

        /// <summary>
        /// Releases the per-stream log/device handles and frees the native BTree, leaving any on-disk
        /// data intact (use <see cref="DeleteOnDiskData"/> to also remove the directory). Invoked
        /// deterministically by every lifecycle path: DEL/UNLINK/EXPIRE and main-log eviction via
        /// GarnetRecordTriggers (OnDispose / OnEvict), FLUSHDB/FLUSHALL, and graceful shutdown
        /// (StreamObjectConfig). As a safety net for any unforeseen abandonment, the native BTree
        /// memory is also reclaimed by <c>~BTree()</c> and the device's OS handles by SafeFileHandle
        /// finalizers, so no unmanaged resource leaks even if Dispose is somehow missed.
        /// </summary>
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