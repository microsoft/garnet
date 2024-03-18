// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.server;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationManager : IDisposable
    {
        #region manageInMemoryCheckpointStore
        public void InitializeCheckpointStore()
        {
            checkpointStore.Initialize();
            var cEntry = checkpointStore.GetLatestCheckpointEntryFromMemory();
            aofTaskStore.UpdateTruncatedUntil(cEntry.GetMinAofCoveredAddress());
            cEntry.RemoveReader();
        }

        public void AddCheckpointEntry(CheckpointEntry entry, StoreType storeType, bool fullCheckpoint)
            => checkpointStore.AddCheckpointEntry(entry, storeType, fullCheckpoint);

        public void PurgeAllCheckpointsExceptEntry(CheckpointEntry except)
            => checkpointStore.PurgeAllCheckpointsExceptEntry(except);

        public CheckpointEntry GetLatestCheckpointEntryFromMemory()
            => checkpointStore.GetLatestCheckpointEntryFromMemory();

        public CheckpointEntry GetLatestCheckpointEntryFromDisk()
            => checkpointStore.GetLatestCheckpointEntryFromDisk();
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

        public void SetPrimaryReplicationId()
        {
            clusterProvider.GetReplicationLogCheckpointManager(StoreType.Main).PrimaryReplicationId = PrimaryReplId;
            if (!clusterProvider.serverOptions.DisableObjects)
                clusterProvider.GetReplicationLogCheckpointManager(StoreType.Object).PrimaryReplicationId = PrimaryReplId;
        }
    }
}