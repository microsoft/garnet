// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
using Garnet.server.BfTreeInterop;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Provides methods for managing range index operations, including replication and handling of AOF (Append-Only
    /// File) entries for create, set, and delete operations.
    /// </summary>
    public sealed partial class RangeIndexManager
    {
        /// <summary>
        /// Sentinel placed in <c>StringInput.arg1</c> of the migration-publish <see cref="RespCommand.RICREATE"/>
        /// RMW so <c>MainSessionFunctions.WriteLogRMW</c> skips adding to AOF: the range index stream
        /// is the single AOF source of truth for a migrated key. Used only in the migration code path.
        /// </summary>
        internal const long StreamedPublishLogArg = long.MinValue;

        // Bit flags packed into the range index stream chunk AOF entry's StringInput.arg1.
        private const long StreamChunkIsLastFlag = 1L;
        private const long StreamChunkIsFirstFlag = 2L;

        /// <summary>
        /// Max size (bytes) of each <see cref="AofEntryType.RangeIndexStreamChunk"/> AOF entry used to
        /// replicate a migrated index. Tests may override it to exercise the multi-chunk path.
        /// </summary>
        private int rangeIndexAofStreamChunkSize = DefaultMigrationChunkSize;

        internal void SetAofStreamChunkSize(int chunkSize)
        {
            if (chunkSize < RangeIndexChunkedSerializer.MinChunkSize)
                throw new ArgumentOutOfRangeException(nameof(chunkSize), chunkSize, $"Range index AOF stream chunk size must be at least {RangeIndexChunkedSerializer.MinChunkSize} bytes.");
            rangeIndexAofStreamChunkSize = chunkSize;
        }

        /// <summary>
        /// In-progress AOF-stream reassembly state, keyed by RangeIndex key. During migration of a RangeIndex,
        /// the serialized BfTree file is streamed into the AOF as a sequence of chunked <see cref="AofEntryType.RangeIndexStreamChunk"/>.
        /// Replicas replaying the AOF may receive these chunks interleaved with unrelated AOF entries, so we need to track
        /// per-key state to correctly reassemble the stream.
        /// </summary>
        private readonly ConcurrentDictionary<byte[], StreamReassemblyState> rangeIndexAofStreamReassembly = new(ByteArrayComparer.Instance);

        /// <summary>Number of in-progress per-key range index stream reassemblies.</summary>
        internal int PendingStreamReassemblyCount => rangeIndexAofStreamReassembly.Count;

        /// <summary>
        /// Per-key AOF RangeIndex stream reassembly state: the deserializer reassembling the stream plus the
        /// <see cref="RangeIndexReplicationActivities.ReassemblyActivity"/> tracing it.
        /// </summary>
        private sealed class StreamReassemblyState(RangeIndexChunkedDeserializer deserializer, RangeIndexReplicationActivities.ReassemblyActivity activity)
        {
            internal readonly RangeIndexChunkedDeserializer deserializer = deserializer;
            internal readonly RangeIndexReplicationActivities.ReassemblyActivity activity = activity;
        }

        /// <summary>
        /// Log RI.SET to AOF via direct enqueue (no synthetic RMW).
        /// Skipped when <paramref name="storedProcMode"/> is true (stored procedure logs as a unit).
        /// </summary>
        internal void ReplicateRangeIndexSet(PinnedSpanByte key, PinnedSpanByte field, PinnedSpanByte value,
            GarnetAppendOnlyFile appendOnlyFile, long version, int sessionId, bool storedProcMode)
        {
            if (appendOnlyFile == null || storedProcMode) return;

            var replicateParseState = new SessionParseState();
            replicateParseState.InitializeWithArguments(field, value);
            var input = new StringInput(RespCommand.RISET, ref replicateParseState);
            input.header.flags |= RespInputFlags.Deterministic;

            appendOnlyFile.Log.Enqueue(
                AofEntryType.StoreRMW,
                version,
                sessionId,
                key.ReadOnlySpan,
                ref input,
                out _);
        }

        /// <summary>
        /// Log RI.DEL to AOF via direct enqueue (no synthetic RMW).
        /// Skipped when <paramref name="storedProcMode"/> is true (stored procedure logs as a unit).
        /// </summary>
        internal void ReplicateRangeIndexDel(PinnedSpanByte key, PinnedSpanByte field,
            GarnetAppendOnlyFile appendOnlyFile, long version, int sessionId, bool storedProcMode)
        {
            if (appendOnlyFile == null || storedProcMode) return;

            var replicateParseState = new SessionParseState();
            replicateParseState.InitializeWithArgument(field);
            var input = new StringInput(RespCommand.RIDEL, ref replicateParseState);
            input.header.flags |= RespInputFlags.Deterministic;

            appendOnlyFile.Log.Enqueue(
                AofEntryType.StoreRMW,
                version,
                sessionId,
                key.ReadOnlySpan,
                ref input,
                out _);
        }

        /// <summary>
        /// Handle RI.CREATE replay from AOF.
        /// </summary>
        /// <remarks>
        /// The AOF entry contains the serialized stub bytes (including a stale TreeHandle
        /// from the original process). This method:
        /// <list type="number">
        /// <item>Extracts BfTree configuration from the stale stub.</item>
        /// <item>Creates a fresh BfTree instance with a new native pointer.</item>
        /// <item>Replaces the stale TreeHandle in the stub bytes with the new pointer.</item>
        /// <item>Lets the normal RMW path (InitialUpdater) create the store record.</item>
        /// </list>
        /// If the key already exists (e.g., AOF replay of a duplicate RI.CREATE after
        /// checkpoint recovery), the RMW returns <c>InPlaceUpdated</c> and the fresh
        /// BfTree is disposed.
        /// </remarks>
        /// <param name="session">The storage session for issuing the RMW.</param>
        /// <param name="key">The Garnet key being created.</param>
        /// <param name="input">The RMW input containing the stub bytes in parseState.</param>
        internal unsafe void HandleRangeIndexCreateReplay(StorageSession session, ReadOnlySpan<byte> key, ref StringInput input)
        {
            var stubSpan = input.parseState.GetArgSliceByRef(0).Span;
            if (stubSpan.Length != IndexSizeBytes)
                throw new GarnetException($"Corrupt RI.CREATE AOF entry: stub size {stubSpan.Length}, expected {IndexSizeBytes}");

            ref var stub = ref Unsafe.As<byte, RangeIndexStub>(ref MemoryMarshal.GetReference(stubSpan));

            // Create a fresh BfTree from the config in the stub
            BfTreeService bfTree;
            try
            {
                bfTree = CreateBfTree(
                    (StorageBackendType)stub.StorageBackend, key,
                    stub.CacheSize, stub.MinRecordSize, stub.MaxRecordSize,
                    stub.MaxKeyLen, stub.LeafPageSize);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Failed to recreate BfTree during AOF replay");
                return;
            }

            // Replace stale handle with fresh one in the stub bytes
            stub.TreeHandle = bfTree.NativePtr;
            stub.ResetFlags();

            // Let the normal RMW path create the record from the updated stub bytes
            var output = new StringOutput();
            var pinnedKey = PinnedSpanByte.FromPinnedSpan(key);
            var status = session.stringBasicContext.RMW((FixedSpanByteKey)pinnedKey, ref input, ref output);
            if (status.IsPending)
                StorageSession.CompletePendingForSession(ref status, ref output, ref session.stringBasicContext);

            if (status.Record.Created)
            {
                var keyHash = session.stringBasicContext.GetKeyHash((FixedSpanByteKey)pinnedKey);
                RegisterIndex(bfTree, keyHash, key);
            }
            else
            {
                bfTree.Dispose();
            }
        }

        /// <summary>
        /// Handle RI.SET replay from AOF. Acquires a shared lock, reads the stub to get
        /// the live BfTree pointer, then performs the native insert.
        /// </summary>
        /// <param name="session">The storage session for reading the stub.</param>
        /// <param name="key">The Garnet key of the RangeIndex.</param>
        /// <param name="input">The RMW input containing field and value in parseState.</param>
        internal void HandleRangeIndexSetReplay(StorageSession session, ReadOnlySpan<byte> key, ref StringInput input)
        {
            var field = input.parseState.GetArgSliceByRef(0);
            var value = input.parseState.GetArgSliceByRef(1);

            var pinnedKey = PinnedSpanByte.FromPinnedSpan(key);
            var inputCopy = input;
            inputCopy.arg1 = default;
            Span<byte> stubSpan = stackalloc byte[IndexSizeBytes];

            using (ReadRangeIndex(session, pinnedKey, ref inputCopy, stubSpan, out var status))
            {
                if (status != GarnetStatus.OK) return;
                var treePtr = ReadIndex(stubSpan).TreeHandle;
                if (treePtr == nint.Zero) return;

                var insertResult = BfTreeService.InsertByPtr(treePtr, field, value);
                if (insertResult != BfTreeInsertResult.Success)
                {
                    logger?.LogError("RI.SET AOF replay: native insert rejected ({result}) for a {keyLen}-byte key with field {fieldLen}B and value {valueLen}B; primary/replica divergence.", insertResult, key.Length, field.Length, value.Length);
                    throw new GarnetException($"RI.SET AOF replay failed: native insert returned {insertResult}");
                }
            }
        }

        /// <summary>
        /// Handle RI.DEL replay from AOF. Acquires a shared lock, reads the stub to get
        /// the live BfTree pointer, then performs the native delete.
        /// </summary>
        /// <param name="session">The storage session for reading the stub.</param>
        /// <param name="key">The Garnet key of the RangeIndex.</param>
        /// <param name="input">The RMW input containing the field in parseState.</param>
        internal void HandleRangeIndexDelReplay(StorageSession session, ReadOnlySpan<byte> key, ref StringInput input)
        {
            var field = input.parseState.GetArgSliceByRef(0);

            var pinnedKey = PinnedSpanByte.FromPinnedSpan(key);
            var inputCopy = input;
            inputCopy.arg1 = default;
            Span<byte> stubSpan = stackalloc byte[IndexSizeBytes];

            using (ReadRangeIndex(session, pinnedKey, ref inputCopy, stubSpan, out var status))
            {
                if (status != GarnetStatus.OK) return;
                var treePtr = ReadIndex(stubSpan).TreeHandle;
                if (treePtr == nint.Zero) return;

                var deleteResult = BfTreeService.DeleteByPtr(treePtr, field);
                if (deleteResult != BfTreeDeleteResult.Success)
                {
                    logger?.LogError("RI.DEL AOF replay: native delete rejected ({result}) for a {keyLen}-byte key with field {fieldLen}B; primary/replica divergence.", deleteResult, key.Length, field.Length);
                    throw new GarnetException($"RI.DEL AOF replay failed: native delete returned {deleteResult}");
                }
            }
        }

        /// <summary>
        /// Stream a migrated RangeIndex's serialized BfTree file into the AOF as a sequence of
        /// chunked <see cref="AofEntryType.RangeIndexStreamChunk"/> entries.
        /// </summary>
        internal void ReplicateRangeIndexStream(ReadOnlySpan<byte> key, ReadOnlySpan<byte> stub, string filePath,
            GarnetAppendOnlyFile appendOnlyFile, long version, int sessionId, int chunkSize)
        {
            if (appendOnlyFile == null)
            {
                logger?.LogWarning("ReplicateRangeIndexStream called with null appendOnlyFile");
                return;
            }

            var streamActivity = RangeIndexReplicationActivities.StreamActivity.StartActivity(chunkSize);
            byte[] destBuffer = ArrayPool<byte>.Shared.Rent(chunkSize);
            try
            {
                var fileLen = new FileInfo(filePath).Length;
                streamActivity.OnFileLength(fileLen);
                var serializer = new RangeIndexChunkedSerializer(key.ToArray(), stub.ToArray(), fileLen);
                var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: RangeIndexMigrationReader.DefaultFileReadBufferSize);
                using var reader = new RangeIndexMigrationReader(serializer, fs, tempFilePath: null, logger);

                var isFirst = true;
                while (!reader.IsComplete)
                {
                    var written = reader.ReadNextChunk(destBuffer.AsSpan(0, chunkSize));

                    // ReadNextChunk always makes progress while the !reader.IsComplete (it returns 0
                    // only when already complete, which the loop guard prevents).
                    if (written == 0)
                    {
                        streamActivity.OnError("ZeroLengthChunkFromReader");
                        logger?.LogError("ReplicateRangeIndexStream: reader returned zero-length chunk with a {Size}-byte buffer while the stream is incomplete for key {key}", chunkSize, Encoding.UTF8.GetString(key));
                        throw new GarnetException("ReplicateRangeIndexStream: reader returned zero-length chunk while the stream is incomplete");
                    }

                    EnqueueRangeIndexStreamChunk(appendOnlyFile, version, sessionId, key, destBuffer.AsSpan(0, written), isFirst, reader.IsComplete);
                    streamActivity.OnChunkEnqueued(written);
                    isFirst = false;
                }
            }
            catch (Exception ex)
            {
                streamActivity.OnError(ex.ToString());
                throw;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(destBuffer);
                streamActivity.EndAndLog(logger, key);
            }
        }

        /// <summary>Enqueue a single <see cref="AofEntryType.RangeIndexStreamChunk"/> chunk to the AOF.</summary>
        private unsafe void EnqueueRangeIndexStreamChunk(GarnetAppendOnlyFile appendOnlyFile, long version, int sessionId,
            ReadOnlySpan<byte> key, ReadOnlySpan<byte> chunk, bool isFirst, bool isLast)
        {
            fixed (byte* chunkPtr = chunk)
            {
                var chunkSlice = PinnedSpanByte.FromPinnedPointer(chunkPtr, chunk.Length);
                var parseState = new SessionParseState();
                parseState.InitializeWithArgument(chunkSlice);
                var input = new StringInput(RespCommand.NONE, ref parseState,
                    arg1: (isLast ? StreamChunkIsLastFlag : 0) | (isFirst ? StreamChunkIsFirstFlag : 0),
                    flags: RespInputFlags.Deterministic);

                appendOnlyFile.Log.Enqueue(AofEntryType.RangeIndexStreamChunk, version, sessionId, key, ref input, out _);
            }
        }

        /// <summary>
        /// Handle a <see cref="AofEntryType.RangeIndexStreamChunk"/> chunk on AOF replay (replica replication or
        /// crash recovery). Feeds the chunk into the per-key <see cref="RangeIndexChunkedDeserializer"/>;
        /// once the stream completes, publishes the reassembled BfTree via <see cref="PublishMigratedIndex"/>.
        /// </summary>
        internal void HandleRangeIndexStreamReplay(StorageSession session, ReadOnlySpan<byte> key, ref StringInput input)
        {
            var chunk = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
            var isLast = (input.arg1 & StreamChunkIsLastFlag) != 0;
            var isFirst = (input.arg1 & StreamChunkIsFirstFlag) != 0;
            ProcessStreamChunk(session, key, chunk, isFirst, isLast);
        }

        /// <summary>
        /// Core range index stream reassembly step: reset stale state on a stream's first chunk, feed the chunk
        /// to the per-key deserializer, and publish when the stream completes.
        /// </summary>
        internal void ProcessStreamChunk(StorageSession session, ReadOnlySpan<byte> key, ReadOnlySpan<byte> chunk, bool isFirst, bool isLast)
        {
            var keyArr = key.ToArray();

            // A new stream's first chunk supersedes any incomplete reassembly for the same key.
            if (isFirst)
                RemoveAndDisposeStreamReassembly(keyArr, "NewStreamReceived");

            var state = rangeIndexAofStreamReassembly.GetOrAdd(keyArr, _ => new StreamReassemblyState(new RangeIndexChunkedDeserializer(DeriveTempMigrationPath(), logger), RangeIndexReplicationActivities.ReassemblyActivity.StartActivity()));
            var deserializer = state.deserializer;
            state.activity.OnChunkReceived(chunk.Length);

            if (!deserializer.ProcessChunk(chunk) || deserializer.HasError)
            {
                logger?.LogError("HandleRangeIndexStreamReplay: failed to process range index stream chunk for key {key}", Encoding.UTF8.GetString(key));
                RemoveAndDisposeStreamReassembly(keyArr, "ChunkProcessingError");
                throw new GarnetException($"HandleRangeIndexStreamReplay: failed to process range index stream chunk for key {Encoding.UTF8.GetString(key)}");
            }

            if (deserializer.IsComplete)
            {
                // TODO(RangeIndex): Propagate replaceOption in the migrated stream
                PublishMigratedIndexResult publishResult;
                if (ExceptionInjectionHelper.TriggerCondition(ExceptionInjectionType.RangeIndex_Replay_Force_Publish_Failure))
                    publishResult = PublishMigratedIndexResult.Failed;
                else
                    publishResult = PublishMigratedIndex(deserializer.Key, deserializer.Stub, deserializer.TempPath, replaceOption: false, ref session.stringBasicContext, session.functionsState.appendOnlyFile);

                state.activity.OnPublishResult(publishResult);

                if (publishResult == PublishMigratedIndexResult.Failed)
                {
                    logger?.LogError("HandleRangeIndexStreamReplay: PublishMigratedIndex failed during AOF replay for key {key}", Encoding.UTF8.GetString(key));
                    RemoveAndDisposeStreamReassembly(keyArr, "PublishFailed");
                    throw new GarnetException($"HandleRangeIndexStreamReplay: PublishMigratedIndex failed during AOF replay for key {Encoding.UTF8.GetString(key)}");
                }

                RemoveAndDisposeStreamReassembly(keyArr, "Complete");
                return;
            }

            if (isLast)
            {
                // Final-chunk flag set but the deserializer did not reach completion — the stream is malformed/truncated.
                logger?.LogError("HandleRangeIndexStreamReplay: final range index stream chunk flag set but stream is incomplete for key {key}", Encoding.UTF8.GetString(key));
                RemoveAndDisposeStreamReassembly(keyArr, "FinalChunkButDeserializerIncomplete");
                throw new GarnetException($"HandleRangeIndexStreamReplay: final range index stream chunk flag set but stream is incomplete for key {Encoding.UTF8.GetString(key)}");
            }
        }

        /// <summary>
        /// Dispose and drop any in-progress AOF stream reassembly state - called on disposal.
        /// </summary>
        internal void DisposeIncompleteStreamReassembly()
        {
            foreach (var key in rangeIndexAofStreamReassembly.Keys)
            {
                logger?.LogWarning("DisposeIncompleteStreamReassembly: discarding incomplete range index stream reassembly for key {key}", Encoding.UTF8.GetString(key));
                RemoveAndDisposeStreamReassembly(key, "CleanupIncomplete");
            }
        }

        private void RemoveAndDisposeStreamReassembly(byte[] key, string reason)
        {
            if (rangeIndexAofStreamReassembly.TryRemove(key, out var state))
            {
                state.activity.EndAndLog(logger, key, reason);
                state.deserializer.Dispose();
            }
        }
    }
}