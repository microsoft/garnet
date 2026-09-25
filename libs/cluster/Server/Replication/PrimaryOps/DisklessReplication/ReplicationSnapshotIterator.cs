// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Threading;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;
using Tsavorite.core.Allocator.ObjectSerialization;

namespace Garnet.cluster
{
    internal sealed unsafe class SnapshotIteratorManager : IChunkedObjectSerializerConsumer
    {
        public readonly ReplicationSyncManager replicationSyncManager;
        public readonly CancellationToken cancellationToken;
        public readonly ILogger logger;

        public StoreSnapshotIterator StoreSnapshotIterator;

        // For serialization from LogRecord to DiskLogRecord
        SpanByteAndMemory serializationOutput;
        GarnetObjectSerializer valueObjectSerializer;
        MemoryPool<byte> memoryPool;

        // Reused chunk writer for records too large for a single send buffer; this manager is its consumer (fans chunks to all sessions).
        readonly ChunkedObjectSerializer<SnapshotIteratorManager> chunker;
        // Max record/chunk payload that fits one send buffer after the per-batch header and per-chunk framing overhead.
        readonly int maxSendBufferContentSize;

        readonly ReplicaSyncSession[] sessions;
        readonly int numSessions;

        bool firstRead = false;
        long currentFlushEventCount = 0;
        long lastFlushEventCount = 0;

        // Whether the record currently being serialized may be emitted as a whole LogRecord if it fits one send buffer (set by
        // WriteRecord for a prefix-free object record); Consume detects "fit one buffer" from the chunker's isStart+isComplete.
        bool wholeRecordEmittable;

        AofAddress CheckpointCoveredAddress { get; set; }

        public SnapshotIteratorManager(ReplicationSyncManager replicationSyncManager, CancellationToken cancellationToken, ILogger logger = null)
        {
            this.replicationSyncManager = replicationSyncManager;
            this.cancellationToken = cancellationToken;
            this.logger = logger;

            sessions = replicationSyncManager.Sessions;
            numSessions = replicationSyncManager.NumSessions;

            CheckpointCoveredAddress = replicationSyncManager.ClusterProvider.storeWrapper.appendOnlyFile.Log.TailAddress;
            for (var i = 0; i < numSessions; i++)
            {
                if (!replicationSyncManager.IsActive(i)) continue;
                sessions[i].checkpointCoveredAofAddress = CheckpointCoveredAddress;
            }

            StoreSnapshotIterator = new StoreSnapshotIterator(this);

            memoryPool = MemoryPool<byte>.Shared;
            valueObjectSerializer = new(customCommandManager: default);

            // Base the chunk size on the AOF-sync client's own send buffer (GetAofSyncNetworkBufferSettings), NOT the inclusive
            // networkBufferSettings (whose sendBufferSize is the max across all replication clients and is larger than this one).
            maxSendBufferContentSize = replicationSyncManager.ClusterProvider.replicationManager.GetAofSyncNetworkBufferSettings.MaxSendBufferContentSize;
            // The ring holds one drained chunk; keep it below maxSendBufferContentSize so a chunk plus its framing always fits a freshly flushed buffer.
            chunker = new ChunkedObjectSerializer<SnapshotIteratorManager>(this, bufferSize: Math.Max(1024, maxSendBufferContentSize - 64));
        }

        /// <summary>
        /// Check if stream is progressing
        /// </summary>
        /// <returns></returns>
        public bool IsProgressing()
        {
            var flushEventCount = currentFlushEventCount;
            if (flushEventCount > lastFlushEventCount)
            {
                return true;
            }
            else
            {
                lastFlushEventCount = flushEventCount;
                return false;
            }
        }

        public bool OnStart(Guid checkpointToken, long currentVersion, long targetVersion)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger?.LogError("{method} cancellationRequested", nameof(OnStart));
                return false;
            }

            // reset progress counter
            lastFlushEventCount = currentFlushEventCount = 0;

            for (var i = 0; i < numSessions; i++)
            {
                if (!replicationSyncManager.IsActive(i))
                    continue;
                sessions[i].InitializeIterationBuffer();
                sessions[i].currentStoreVersion = targetVersion;
            }

            logger?.LogTrace("{OnStart} {token} {currentVersion} {targetVersion}",
                nameof(OnStart), checkpointToken, currentVersion, targetVersion);

            return true;
        }

        /// <summary>
        /// Serialize and send one record to all active replica sessions. A record that fits one send buffer is sent whole (type
        /// <see cref="MigrationRecordSpanType.LogRecord"/>): a fully-inline record directly, and a prefix-free object record
        /// (inline key + object value) via <c>Consume</c> when it drains in one piece. Anything larger — an inline record
        /// too big for the buffer, an overflow key/value, or a large object value — is streamed as
        /// <see cref="MigrationRecordSpanType.ChunkedLogRecord"/> chunks so records larger than the send buffer are supported.
        /// </summary>
        public bool WriteRecord<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (!firstRead)
            {
                logger?.LogTrace("Start Streaming {key}", srcLogRecord.Key.ToString());
                firstRead = true;
            }

            // We may be sending to multiple replicas, so serialize once to a local buffer (fast path) or drain once through the
            // shared chunker (chunked path), copying to each network buffer as we go rather than re-serializing per replica.
            // RoundUp(ActualSize) to Constants.kRecordAlignment (8); Constants is internal to Tsavorite.core.
            var alignedInlineRecordSize = (srcLogRecord.ActualSize + 7) & ~7;
            if (srcLogRecord.DataHeader.RecordIsInline && alignedInlineRecordSize <= maxSendBufferContentSize)
            {
                DiskLogRecord.DirectCopyInlinePortionOfRecord(in srcLogRecord, alignedInlineRecordSize, estimatedTotalSize: alignedInlineRecordSize,
                    maxHeapAllocationSize: alignedInlineRecordSize, memoryPool, ref serializationOutput);
                return FanOutRecordSpan(serializationOutput.MemorySpan.Slice(0, alignedInlineRecordSize), MigrationRecordSpanType.LogRecord);
            }

            // Chunked path: DiskLogRecord.Serialize drains through the chunker to this consumer's Consume, which fans each chunk
            // out to all sessions with the continuation flag set until the record's final chunk. A prefix-free object record
            // (inline key + object value: its chunk stream is exactly the [inline][object] whole-record layout) that fits the
            // chunker's ring arrives as one complete drain, which Consume emits as a whole LogRecord so the receiver deserializes
            // it directly from the buffer instead of via a single-chunk reassembly.
            wholeRecordEmittable = !srcLogRecord.DataHeader.RecordIsInline && srcLogRecord.DataHeader.KeyIsInline && srcLogRecord.DataHeader.ValueIsObject;
            DiskLogRecord.Serialize(in srcLogRecord, valueObjectSerializer, chunker, this);
            return !cancellationToken.IsCancellationRequested;
        }

        /// <summary>
        /// Fan a whole record span out to all active sessions in lockstep. Because every session receives the same byte stream
        /// their buffers stay in lockstep: a record fits in all of them or in none, so a full buffer flushes all and retries.
        /// </summary>
        bool FanOutRecordSpan(ReadOnlySpan<byte> span, MigrationRecordSpanType type)
        {
            while (true)
            {
                if (cancellationToken.IsCancellationRequested)
                    return false;

                var needToFlush = false;
                for (var i = 0; i < numSessions; i++)
                {
                    if (!replicationSyncManager.IsActive(i))
                        continue;
                    sessions[i].SetClusterSyncHeader();
                    if (!sessions[i].TryWriteRecordSpan(span, type, out var task))
                    {
                        sessions[i].SetFlushTask(task);
                        needToFlush = true;
                    }
                }

                if (!needToFlush)
                    return true;

                AsyncUtils.BlockingWait(replicationSyncManager.WaitForFlushAsync());
                currentFlushEventCount++;
            }
        }

        /// <summary>
        /// Fan one record chunk out to all active sessions in lockstep (see <see cref="FanOutRecordSpan"/>), setting the
        /// continuation flag per <paramref name="moreChunksFollow"/>. Returns the number of bytes consumed (the whole span).
        /// </summary>
        int FanOutChunk(ReadOnlySpan<byte> span, bool moreChunksFollow)
        {
            while (true)
            {
                if (cancellationToken.IsCancellationRequested)
                    return span.Length; // Abort: drop the chunk; WriteRecord returns false and the sync fails.

                var needToFlush = false;
                for (var i = 0; i < numSessions; i++)
                {
                    if (!replicationSyncManager.IsActive(i))
                        continue;
                    sessions[i].SetClusterSyncHeader();
                    if (!sessions[i].TryWriteChunkedRecordSpan(span, moreChunksFollow, out var task))
                    {
                        sessions[i].SetFlushTask(task);
                        needToFlush = true;
                    }
                }

                if (!needToFlush)
                    return span.Length;

                AsyncUtils.BlockingWait(replicationSyncManager.WaitForFlushAsync());
                currentFlushEventCount++;
            }
        }

        /// <inheritdoc/>
        int IChunkedObjectSerializerConsumer.Consume<TContext>(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, bool isStart, bool isComplete, TContext context)
        {
            // A prefix-free object record that fit the chunker's ring arrives as one complete, contiguous drain (isStart and
            // isComplete on the same drain): send it as a whole LogRecord so the receiver deserializes it directly from the
            // buffer (no single-chunk reassembly).
            if (isStart && wholeRecordEmittable && isComplete && second.IsEmpty)
            {
                _ = FanOutRecordSpan(first, MigrationRecordSpanType.LogRecord);
                return first.Length;
            }

            // Each drained run is one chunk to every session; the continuation flag is clear only on the final run of the record.
            var consumed = FanOutChunk(first, moreChunksFollow: !second.IsEmpty || !isComplete);
            if (!second.IsEmpty)
                consumed += FanOutChunk(second, moreChunksFollow: !isComplete);
            return consumed;
        }

        /// <inheritdoc/>
        int IChunkedObjectSerializerConsumer.Consume<TContext, TKey, TInput>(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, bool isStart, bool isComplete, TKey key, ref TInput input, TContext context)
            => throw new NotSupportedException("The network chunk path serializes only the value; the key is sent inline.");

        public void OnStop(bool completed, long numberOfRecords, long targetVersion)
        {
            // Flush remaining data
            for (var i = 0; i < numSessions; i++)
            {
                if (replicationSyncManager.IsActive(i))
                    sessions[i].SendAndResetIterationBuffer();
            }

            // Wait for flush and response to complete
            AsyncUtils.BlockingWait(replicationSyncManager.WaitForFlushAsync());

            logger?.LogTrace("{OnStop} {numberOfRecords} {targetVersion}",
                nameof(OnStop), numberOfRecords, targetVersion);

            // Reset read marker
            firstRead = false;

            serializationOutput.Dispose();
        }
    }

    internal sealed unsafe class StoreSnapshotIterator(SnapshotIteratorManager snapshotIteratorManager) :
        IStreamingSnapshotIteratorFunctions
    {
        long targetVersion;

        public bool OnStart(Guid checkpointToken, long currentVersion, long targetVersion)
        {
            this.targetVersion = targetVersion;
            return snapshotIteratorManager.OnStart(checkpointToken, currentVersion, targetVersion);
        }

        public bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords)
            where TSourceLogRecord : ISourceLogRecord
        {
            return snapshotIteratorManager.WriteRecord(in srcLogRecord, recordMetadata, numberOfRecords);
        }

        public void OnException(Exception exception, long numberOfRecords)
            => snapshotIteratorManager.logger?.LogError(exception, $"{nameof(StoreSnapshotIterator)}");

        public void OnStop(bool completed, long numberOfRecords)
            => snapshotIteratorManager.OnStop(completed, numberOfRecords, targetVersion);
    }
}