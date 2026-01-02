// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Threading;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe class SnapshotIteratorManager
    {
        public readonly ReplicationSyncManager replicationSyncManager;
        public readonly CancellationToken cancellationToken;
        public readonly ILogger logger;

        public StoreSnapshotIterator StoreSnapshotIterator;

        // For serialization from LogRecord to DiskLogRecord
        SpanByteAndMemory serializationOutput;
        GarnetObjectSerializer valueObjectSerializer;
        MemoryPool<byte> memoryPool;

        readonly ReplicaSyncSession[] sessions;
        readonly int numSessions;

        bool firstRead = false;
        long currentFlushEventCount = 0;
        long lastFlushEventCount = 0;

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

        public bool StringReader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (!firstRead)
            {
                var key = srcLogRecord.Key;
                var value = srcLogRecord.ValueSpan;
                logger?.LogTrace("Start Streaming {key} {value}", key.ToString(), value.ToString());
                firstRead = true;
            }

            // Note: We may be sending to multiple replicas, so serialize LogRecords to a local then copy to the multiple network buffers
            // rather than issuing multiple serialization calls.
            _ = DiskLogRecord.Serialize(in srcLogRecord, maxHeapAllocationSize: -1, valueObjectSerializer: default, memoryPool, ref serializationOutput);

            var needToFlush = false;
            while (true)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    logger?.LogError("{method} cancellationRequested", nameof(OnStart));
                    return false;
                }

                // Write key value pair to network buffer
                for (var i = 0; i < numSessions; i++)
                {
                    if (!replicationSyncManager.IsActive(i))
                        continue;

                    // Initialize header if necessary
                    sessions[i].SetClusterSyncHeader();

                    // Try to write to network buffer. If failed we need to retry
                    if (!sessions[i].TryWriteRecordSpan(serializationOutput.MemorySpan, out var task))
                    {
                        sessions[i].SetFlushTask(task);
                        needToFlush = true;
                    }
                }

                if (!needToFlush)
                    break;

                // Wait for flush to complete for all and retry to enqueue previous keyValuePair above
                replicationSyncManager.WaitForFlush().GetAwaiter().GetResult();
                currentFlushEventCount++;
                needToFlush = false;
            }

            return true;
        }

        public bool ObjectReader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (!firstRead)
            {
                var key = srcLogRecord.Key;
                var value = srcLogRecord.ValueObject;
                logger?.LogTrace("Start Streaming {key} {value}", key.ToString(), value.ToString());
                firstRead = true;
            }

            // Note: We may be sending to multiple replicas, so cannot serialize LogRecords directly to the network buffer
            var maxHeapAllocationSize = replicationSyncManager.ClusterProvider.replicationManager.networkBufferSettings.sendBufferSize;
            var recordSize = DiskLogRecord.Serialize(in srcLogRecord, maxHeapAllocationSize, valueObjectSerializer, memoryPool, ref serializationOutput);

            var needToFlush = false;
            while (true)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    logger?.LogError("{method} cancellationRequested", nameof(OnStart));
                    return false;
                }

                // Write key value pair to network buffer
                for (var i = 0; i < numSessions; i++)
                {
                    if (!replicationSyncManager.IsActive(i))
                        continue;

                    // Initialize header if necessary
                    sessions[i].SetClusterSyncHeader();

                    // Try to write to network buffer. If failed we need to retry
                    if (!sessions[i].TryWriteRecordSpan(serializationOutput.MemorySpan.Slice(0, recordSize), out var task))
                    {
                        sessions[i].SetFlushTask(task);
                        needToFlush = true;
                    }
                }

                if (!needToFlush)
                    break;

                // Wait for flush to complete for all and retry to enqueue previous keyValuePair above
                replicationSyncManager.WaitForFlush().GetAwaiter().GetResult();
                currentFlushEventCount++;
            }

            return true;
        }

        public void OnStop(bool completed, long numberOfRecords, long targetVersion)
        {
            // Flush remaining data
            for (var i = 0; i < numSessions; i++)
            {
                if (replicationSyncManager.IsActive(i))
                    sessions[i].SendAndResetIterationBuffer();
            }

            // Wait for flush and response to complete
            replicationSyncManager.WaitForFlush().GetAwaiter().GetResult();

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
            return srcLogRecord.Info.ValueIsObject
                ? snapshotIteratorManager.ObjectReader(in srcLogRecord, recordMetadata, numberOfRecords)
                : snapshotIteratorManager.StringReader(in srcLogRecord, recordMetadata, numberOfRecords);
        }

        public void OnException(Exception exception, long numberOfRecords)
            => snapshotIteratorManager.logger?.LogError(exception, $"{nameof(StoreSnapshotIterator)}");

        public void OnStop(bool completed, long numberOfRecords)
            => snapshotIteratorManager.OnStop(completed, numberOfRecords, targetVersion);
    }
}