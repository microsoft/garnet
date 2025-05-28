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
        public readonly TimeSpan timeout;
        public readonly CancellationToken cancellationToken;
        public readonly ILogger logger;

        public MainStoreSnapshotIterator mainStoreSnapshotIterator;
        public ObjectStoreSnapshotIterator objectStoreSnapshotIterator;

        // For serialization from LogRecord to DiskLogRecord
        SpanByteAndMemory serializationOutput;
        GarnetObjectSerializer valueObjectSerializer;
        MemoryPool<byte> memoryPool;

        readonly ReplicaSyncSession[] sessions;
        readonly int numSessions;

        bool firstRead = false;
        long currentFlushEventCount = 0;
        long lastFlushEventCount = 0;

        public long CheckpointCoveredAddress { get; private set; }

        public SnapshotIteratorManager(ReplicationSyncManager replicationSyncManager, CancellationToken cancellationToken, ILogger logger = null)
        {
            this.replicationSyncManager = replicationSyncManager;
            this.cancellationToken = cancellationToken;
            this.logger = logger;

            sessions = replicationSyncManager.Sessions;
            numSessions = replicationSyncManager.NumSessions;

            CheckpointCoveredAddress = replicationSyncManager.ClusterProvider.storeWrapper.appendOnlyFile.TailAddress;
            for (var i = 0; i < numSessions; i++)
            {
                if (!replicationSyncManager.IsActive(i)) continue;
                sessions[i].checkpointCoveredAofAddress = CheckpointCoveredAddress;
            }

            mainStoreSnapshotIterator = new MainStoreSnapshotIterator(this);
            if (!replicationSyncManager.ClusterProvider.serverOptions.DisableObjects)
                objectStoreSnapshotIterator = new ObjectStoreSnapshotIterator(this);

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

        public bool OnStart(Guid checkpointToken, long currentVersion, long targetVersion, bool isMainStore)
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
                if (isMainStore)
                    sessions[i].currentStoreVersion = targetVersion;
                else
                    sessions[i].currentObjectStoreVersion = targetVersion;
            }

            logger?.LogTrace("{OnStart} {store} {token} {currentVersion} {targetVersion}",
                nameof(OnStart), isMainStore ? "MAIN STORE" : "OBJECT STORE", checkpointToken, currentVersion, targetVersion);

            return true;
        }

        public bool StringReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (!firstRead)
            {
                var key = srcLogRecord.Key;
                var value = srcLogRecord.ValueSpan;
                logger?.LogTrace("Start Streaming {key} {value}", key.ToString(), value.ToString());
                firstRead = true;
            }

            // Note: We may be sending to multiple replicas, so cannot serialize LogRecords directly to the network buffer
            DiskLogRecord.Serialize(in srcLogRecord, valueSerializer: null, ref serializationOutput, memoryPool);

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
                    sessions[i].SetClusterSyncHeader(isMainStore: true);

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

        public bool ObjectReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords)
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
            DiskLogRecord.Serialize(in srcLogRecord, valueObjectSerializer, ref serializationOutput, memoryPool);

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
                    sessions[i].SetClusterSyncHeader(isMainStore: false);

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
            }

            return true;
        }

        public void OnStop(bool completed, long numberOfRecords, bool isMainStore, long targetVersion)
        {
            // Flush remaining data
            for (var i = 0; i < numSessions; i++)
            {
                if (replicationSyncManager.IsActive(i))
                    sessions[i].SendAndResetIterationBuffer();
            }

            // Wait for flush and response to complete
            replicationSyncManager.WaitForFlush().GetAwaiter().GetResult();

            logger?.LogTrace("{OnStop} {store} {numberOfRecords} {targetVersion}",
                nameof(OnStop), isMainStore ? "MAIN STORE" : "OBJECT STORE", numberOfRecords, targetVersion);

            // Reset read marker
            firstRead = false;

            serializationOutput.Dispose();
        }
    }

    internal sealed unsafe class MainStoreSnapshotIterator(SnapshotIteratorManager snapshotIteratorManager) :
        IStreamingSnapshotIteratorFunctions
    {
        readonly SnapshotIteratorManager snapshotIteratorManager = snapshotIteratorManager;
        long targetVersion;

        public bool OnStart(Guid checkpointToken, long currentVersion, long targetVersion)
        {
            this.targetVersion = targetVersion;
            return snapshotIteratorManager.OnStart(checkpointToken, currentVersion, targetVersion, isMainStore: true);
        }

        public bool Reader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords)
            where TSourceLogRecord : ISourceLogRecord
            => snapshotIteratorManager.StringReader(ref srcLogRecord, recordMetadata, numberOfRecords);

        public void OnException(Exception exception, long numberOfRecords)
            => snapshotIteratorManager.logger?.LogError(exception, $"{nameof(MainStoreSnapshotIterator)}");

        public void OnStop(bool completed, long numberOfRecords)
            => snapshotIteratorManager.OnStop(completed, numberOfRecords, isMainStore: true, targetVersion);
    }

    internal sealed unsafe class ObjectStoreSnapshotIterator(SnapshotIteratorManager snapshotIteratorManager) :
        IStreamingSnapshotIteratorFunctions
    {
        readonly SnapshotIteratorManager snapshotIteratorManager = snapshotIteratorManager;
        long targetVersion;

        public bool OnStart(Guid checkpointToken, long currentVersion, long targetVersion)
        {
            this.targetVersion = targetVersion;
            return snapshotIteratorManager.OnStart(checkpointToken, currentVersion, targetVersion, isMainStore: false);
        }

        public bool Reader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, RecordMetadata recordMetadata, long numberOfRecords)
            where TSourceLogRecord : ISourceLogRecord
            => snapshotIteratorManager.ObjectReader(ref srcLogRecord, recordMetadata, numberOfRecords);

        public void OnException(Exception exception, long numberOfRecords)
            => snapshotIteratorManager.logger?.LogError(exception, $"{nameof(ObjectStoreSnapshotIterator)}");

        public void OnStop(bool completed, long numberOfRecords)
            => snapshotIteratorManager.OnStop(completed, numberOfRecords, isMainStore: false, targetVersion);
    }
}