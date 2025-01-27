// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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

        readonly ReplicaSyncSession[] sessions;
        readonly int numSessions;

        public SnapshotIteratorManager(ReplicationSyncManager replicationSyncManager, TimeSpan timeout, CancellationToken cancellationToken, ILogger logger = null)
        {
            this.replicationSyncManager = replicationSyncManager;
            this.timeout = timeout;
            this.cancellationToken = cancellationToken;
            this.logger = logger;

            var sessionStore = replicationSyncManager.GetSessionStore;
            sessions = sessionStore.GetSessions();
            numSessions = sessionStore.GetNumSessions();

            mainStoreSnapshotIterator = new MainStoreSnapshotIterator(this);
            if (!replicationSyncManager.ClusterProvider.serverOptions.DisableObjects)
                objectStoreSnapshotIterator = new ObjectStoreSnapshotIterator(this);
        }

        public static bool IsActive(ReplicaSyncSession[] session, int offset)
        {
            // Check if session is null if an error occurred earlier and session was broken
            if (session[offset] == null)
                return false;

            // Check if connection is still healthy
            if (!session[offset].IsConnected)
            {
                session[offset].SetStatus(SyncStatus.FAILED, "Connection broken");
                session[offset] = null;
                return false;
            }
            return true;
        }

        public bool OnStart(Guid checkpointToken, long currentVersion, long targetVersion, bool isMainStore)
        {
            if (cancellationToken.IsCancellationRequested)
                return false;

            for (var i = 0; i < numSessions; i++)
            {
                if (!IsActive(sessions, i)) continue;
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

        void WaitForFlushAll()
        {
            // Wait for flush to complete for all and retry to enqueue previous keyValuePair above
            for (var i = 0; i < numSessions; i++)
            {
                if (!IsActive(sessions, i)) continue;
                sessions[i].WaitForFlush().GetAwaiter().GetResult();
            }
        }

        public bool Reader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords)
        {
            var needToFlush = false;
            while (true)
            {
                if (cancellationToken.IsCancellationRequested)
                    return false;

                // Write key value pair to network buffer
                for (var i = 0; i < numSessions; i++)
                {
                    if (!IsActive(sessions, i)) continue;

                    // Initialize header if necessary
                    sessions[i].SetClusterSyncHeader(isMainStore: true);

                    // Try to write to network buffer. If failed we need to retry
                    if (!sessions[i].TryWriteKeyValueSpanByte(ref key, ref value, out var task))
                    {
                        sessions[i].SetFlushTask(task, timeout, cancellationToken);
                        needToFlush = true;
                    }
                }

                if (!needToFlush) break;

                // Wait for flush to complete for all and retry to enqueue previous keyValuePair above
                WaitForFlushAll();
                needToFlush = false;
            }

            return true;
        }

        public bool Reader(ref byte[] key, ref IGarnetObject value, RecordMetadata recordMetadata, long numberOfRecords)
        {
            var needToFlush = false;
            var objectData = GarnetObjectSerializer.Serialize(value);
            while (true)
            {
                if (cancellationToken.IsCancellationRequested)
                    return false;

                // Write key value pair to network buffer
                for (var i = 0; i < numSessions; i++)
                {
                    if (!IsActive(sessions, i)) continue;

                    // Initialize header if necessary
                    sessions[i].SetClusterSyncHeader(isMainStore: false);

                    // Try to write to network buffer. If failed we need to retry
                    if (!sessions[i].TryWriteKeyValueByteArray(key, objectData, value.Expiration, out var task))
                    {
                        sessions[i].SetFlushTask(task, timeout, cancellationToken);
                        needToFlush = true;
                    }
                }

                if (!needToFlush) break;

                // Wait for flush to complete for all and retry to enqueue previous keyValuePair above
                WaitForFlushAll();
            }

            return true;
        }

        public void OnStop(bool completed, long numberOfRecords, bool isMainStore, long targetVersion)
        {
            // Flush remaining data
            for (var i = 0; i < numSessions; i++)
            {
                if (!IsActive(sessions, i)) continue;
                sessions[i].SendAndResetIterationBuffer(timeout, cancellationToken);
            }

            // Wait for flush and response to complete
            WaitForFlushAll();

            // Enqueue version change commit
            replicationSyncManager.ClusterProvider.storeWrapper.EnqueueCommit(isMainStore, targetVersion);

            logger?.LogTrace("{OnStop} {store} {numberOfRecords} {targetVersion}",
                nameof(OnStop), isMainStore ? "MAIN STORE" : "OBJECT STORE", numberOfRecords, targetVersion);
        }
    }

    internal sealed unsafe class MainStoreSnapshotIterator(SnapshotIteratorManager snapshotIteratorManager) :
        IStreamingSnapshotIteratorFunctions<SpanByte, SpanByte>
    {
        readonly SnapshotIteratorManager snapshotIteratorManager = snapshotIteratorManager;
        long targetVersion;

        public bool OnStart(Guid checkpointToken, long currentVersion, long targetVersion)
        {
            this.targetVersion = targetVersion;
            return snapshotIteratorManager.OnStart(checkpointToken, currentVersion, targetVersion, isMainStore: true);
        }

        public bool Reader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords)
            => snapshotIteratorManager.Reader(ref key, ref value, recordMetadata, numberOfRecords);

        public void OnException(Exception exception, long numberOfRecords)
            => snapshotIteratorManager.logger?.LogError(exception, $"{nameof(MainStoreSnapshotIterator)}");

        public void OnStop(bool completed, long numberOfRecords)
            => snapshotIteratorManager.OnStop(completed, numberOfRecords, isMainStore: true, targetVersion);
    }

    internal sealed unsafe class ObjectStoreSnapshotIterator(SnapshotIteratorManager snapshotIteratorManager) :
        IStreamingSnapshotIteratorFunctions<byte[], IGarnetObject>
    {
        readonly SnapshotIteratorManager snapshotIteratorManager = snapshotIteratorManager;
        long targetVersion;

        public bool OnStart(Guid checkpointToken, long currentVersion, long targetVersion)
        {
            this.targetVersion = targetVersion;
            return snapshotIteratorManager.OnStart(checkpointToken, currentVersion, targetVersion, isMainStore: false);
        }

        public bool Reader(ref byte[] key, ref IGarnetObject value, RecordMetadata recordMetadata, long numberOfRecords)
            => snapshotIteratorManager.Reader(ref key, ref value, recordMetadata, numberOfRecords);

        public void OnException(Exception exception, long numberOfRecords)
            => snapshotIteratorManager.logger?.LogError(exception, $"{nameof(ObjectStoreSnapshotIterator)}");

        public void OnStop(bool completed, long numberOfRecords)
            => snapshotIteratorManager.OnStop(completed, numberOfRecords, isMainStore: false, targetVersion);
    }
}