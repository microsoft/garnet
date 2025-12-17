// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
            if (checkpointStore.TryGetLatestCheckpointEntryFromMemory(out var cEntry))
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
            index_size = -1L;
            try
            {
                hlog_size = storeWrapper.store.GetLogFileSize(entry.metadata.storeHlogToken);
                index_size = storeWrapper.store.GetIndexFileSize(entry.metadata.storeIndexToken);
                return true;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Waiting for main store metadata to settle");
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

        public bool TryGetLatestCheckpointEntryFromMemory(out CheckpointEntry cEntry)
            => checkpointStore.TryGetLatestCheckpointEntryFromMemory(out cEntry);

        public CheckpointEntry GetLatestCheckpointEntryFromDisk()
            => checkpointStore.GetLatestCheckpointEntryFromDisk();

        public string GetLatestCheckpointFromMemoryInfo()
            => checkpointStore.GetLatestCheckpointFromMemoryInfo();

        public string GetLatestCheckpointFromDiskInfo()
            => checkpointStore.GetLatestCheckpointFromDiskInfo();
        #endregion

        public long StoreCurrentSafeAofAddress => clusterProvider.storeWrapper.StoreCheckpointManager.CurrentSafeAofAddress;

        public long StoreRecoveredSafeAofTailAddress => clusterProvider.storeWrapper.StoreCheckpointManager.RecoveredSafeAofAddress;

        /// <summary>
        /// Update current aof address for pending commit.
        /// This is necessary to recover safe aof address along with the commit information.
        /// </summary>
        /// <param name="safeAofTailAddress"></param>
        public void UpdateCommitSafeAofAddress(long safeAofTailAddress)
        {
            clusterProvider.storeWrapper.StoreCheckpointManager.CurrentSafeAofAddress = safeAofTailAddress;
        }

        /// <summary>
        /// Update replicationId for both stores to use for signing future checkpoints
        /// Should be called only at initialization of replication manager and during a failover
        /// </summary>
        public void SetPrimaryReplicationId()
        {
            clusterProvider.storeWrapper.StoreCheckpointManager.CurrentHistoryId = PrimaryReplId;
        }
    }
}