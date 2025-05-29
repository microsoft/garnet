﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationManager : IDisposable
    {
        #region manageInMemoryCheckpointStore
        public bool InitializeCheckpointStore()
        {
            checkpointStore.Initialize();
            if (checkpointStore.GetLatestCheckpointEntryFromMemory(out var cEntry))
            {
                aofTaskStore.UpdateTruncatedUntil(cEntry.GetMinAofCoveredAddress());
                cEntry.RemoveReader();
                return true;
            }
            return false;
        }

        /// <summary>
        ///  Keep trying to acquire main store metadata until it settles
        /// </summary>
        /// <param name="entry">CheckpointEntry to retrieve metadata for</param>
        /// <param name="hlog_size">LogFileInfo to return</param>
        /// <param name="index_size">Index size in bytes to return</param>
        public bool TryAcquireSettledMetadataForMainStore(CheckpointEntry entry, out LogFileInfo hlog_size, out long index_size)
        {
            hlog_size = default;
            index_size = -1;
            try
            {
                hlog_size = storeWrapper.store.GetLogFileSize(entry.metadata.storeHlogToken);
                index_size = storeWrapper.store.GetIndexFileSize(entry.metadata.storeIndexToken);
                return true;
            }
            catch
            {
                logger?.LogError("Waiting for main store metadata to settle");
                return false;
            }
        }

        /// <summary>
        ///  Keep trying to acquire object store metadata until it settles
        /// </summary>
        /// <param name="entry">CheckpointEntry to retrieve metadata for</param>
        /// <param name="hlog_size">LogFileInfo to return</param>
        /// <param name="index_size">Index size in bytes to return</param>
        public bool TryAcquireSettledMetadataForObjectStore(CheckpointEntry entry, out LogFileInfo hlog_size, out long index_size)
        {
            hlog_size = default;
            index_size = -1;
            try
            {
                hlog_size = storeWrapper.objectStore.GetLogFileSize(entry.metadata.objectStoreHlogToken);
                index_size = storeWrapper.objectStore.GetIndexFileSize(entry.metadata.objectStoreIndexToken);
                return true;
            }
            catch
            {
                logger?.LogError("Waiting for object store metadata to settle");
                return false;
            }
        }

        /// <summary>
        /// Add new checkpoint entry to the in-memory store
        /// </summary>
        /// <param name="entry"></param>
        /// <param name="fullCheckpoint"></param>
        public void AddCheckpointEntry(CheckpointEntry entry, bool fullCheckpoint)
            => checkpointStore.AddCheckpointEntry(entry, fullCheckpoint);

        public void PurgeAllCheckpointsExceptEntry(CheckpointEntry except)
            => checkpointStore.PurgeAllCheckpointsExceptEntry(except);

        public bool GetLatestCheckpointEntryFromMemory(out CheckpointEntry cEntry)
            => checkpointStore.GetLatestCheckpointEntryFromMemory(out cEntry);

        public CheckpointEntry GetLatestCheckpointEntryFromDisk()
            => checkpointStore.GetLatestCheckpointEntryFromDisk();

        public string GetLatestCheckpointFromMemoryInfo()
            => checkpointStore.GetLatestCheckpointFromMemoryInfo();

        public string GetLatestCheckpointFromDiskInfo()
            => checkpointStore.GetLatestCheckpointFromDiskInfo();
        #endregion

        public long StoreCurrentSafeAofAddress => clusterProvider.GetReplicationLogCheckpointManager(StoreType.Main).CurrentSafeAofAddress;
        public long ObjectStoreCurrentSafeAofAddress => clusterProvider.serverOptions.DisableObjects ? -1 : clusterProvider.GetReplicationLogCheckpointManager(StoreType.Object).CurrentSafeAofAddress;

        public long StoreRecoveredSafeAofTailAddress => clusterProvider.GetReplicationLogCheckpointManager(StoreType.Main).RecoveredSafeAofAddress;
        public long ObjectStoreRecoveredSafeAofTailAddress => clusterProvider.serverOptions.DisableObjects ? -1 : clusterProvider.GetReplicationLogCheckpointManager(StoreType.Object).RecoveredSafeAofAddress;

        /// <summary>
        /// Update current aof address for pending commit.
        /// This is necessary to recover safe aof address along with the commit information.
        /// </summary>
        /// <param name="safeAofTailAddress"></param>
        public void UpdateCommitSafeAofAddress(long safeAofTailAddress)
        {
            clusterProvider.GetReplicationLogCheckpointManager(StoreType.Main).CurrentSafeAofAddress = safeAofTailAddress;
            if (!clusterProvider.serverOptions.DisableObjects)
                clusterProvider.GetReplicationLogCheckpointManager(StoreType.Object).CurrentSafeAofAddress = safeAofTailAddress;
        }

        /// <summary>
        /// Update replicationId for both stores to use for signing future checkpoints
        /// Should be called only at initialization of replication manager and during a failover
        /// </summary>
        public void SetPrimaryReplicationId()
        {
            clusterProvider.GetReplicationLogCheckpointManager(StoreType.Main).CurrentReplicationId = PrimaryReplId;
            if (!clusterProvider.serverOptions.DisableObjects)
                clusterProvider.GetReplicationLogCheckpointManager(StoreType.Object).CurrentReplicationId = PrimaryReplId;
        }
    }
}