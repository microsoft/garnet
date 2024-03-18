// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal class CheckpointStore
    {
        readonly StoreWrapper storeWrapper;
        readonly ClusterProvider clusterProvider;
        volatile CheckpointEntry head;
        volatile CheckpointEntry tail;
        bool safelyRemoveOutdated;
        readonly ILogger logger;

        public CheckpointStore(StoreWrapper storeWrapper, ClusterProvider clusterProvider, bool safelyRemoveOutdated, ILogger logger = null)
        {
            this.storeWrapper = storeWrapper;
            this.clusterProvider = clusterProvider;
            this.safelyRemoveOutdated = safelyRemoveOutdated;
            this.head = this.tail = null;
            this.logger = logger;

            if (safelyRemoveOutdated)
                PurgeAllCheckpointsExceptEntry();
        }

        /// <summary>
        /// Initialize in-memory checkpoint store.
        /// Assume during initialization checkpoint requests will not be issued/processed.
        /// </summary>
        public void Initialize()
        {
            head = tail = GetLatestCheckpointEntryFromDisk();

            if (tail.storeVersion == -1 && tail.objectStoreVersion == -1) head = tail = null;

            //This purge does not check for active readers
            //1. If primary is initializing then we will not have any active readers since not connections are established at recovery
            //2. If replica is initializing during failover we dot this before allowing other replicas to attach.
            if (safelyRemoveOutdated)
                PurgeAllCheckpointsExceptEntry(tail);
        }

        /// <summary>
        /// Wait for replicas to finish reading
        /// </summary>
        public void WaitForReplicas()
        {
            if (head == tail) return;
            var curr = head;
            while (curr != null)
            {
                while (!curr.TrySuspendReaders()) Thread.Yield();
                curr = curr.next;
            }
        }

        /// <summary>
        /// Method used at initialization to purge any orphan checkpoints except the latest checkpoint
        /// </summary>
        /// <param name="entry"></param>
        public void PurgeAllCheckpointsExceptEntry(CheckpointEntry entry = null)
        {
            entry ??= GetLatestCheckpointEntryFromDisk();
            if (entry == null) return;
            LogCheckpointEntry("Purge all except", entry);
            PurgeAllCheckpointsExceptTokens(StoreType.Main, entry.storeHlogToken, entry.storeIndexToken);
            if (!clusterProvider.serverOptions.DisableObjects)
                PurgeAllCheckpointsExceptTokens(StoreType.Object, entry.objectStoreHlogToken, entry.objectStoreIndexToken);
        }

        public void PurgeAllCheckpointsExceptTokens(StoreType storeType, Guid logToken, Guid indexToken)
        {
            var ckptManager = clusterProvider.GetReplicationLogCheckpointManager(storeType);

            //Delete log checkpoints
            foreach (var toDeletelogToken in ckptManager.GetLogCheckpointTokens())
            {
                if (!toDeletelogToken.Equals(logToken))
                {
                    logger.LogTrace("Deleting log token {toDeletelogToken}", toDeletelogToken);
                    ckptManager.DeleteLogCheckpoint(toDeletelogToken);
                }
            }

            //Delete index checkpoints
            foreach (var toDeleteIndexToken in ckptManager.GetIndexCheckpointTokens())
            {
                if (!toDeleteIndexToken.Equals(indexToken))
                {
                    logger.LogTrace("Deleting index token {toDeleteIndexToken}", toDeleteIndexToken);
                    ckptManager.DeleteIndexCheckpoint(toDeleteIndexToken);
                }
            }
        }

        /// <summary>
        /// Add new checkpoint entry to in memory list.
        /// Assume that addition of new entries executes at completion of a checkpoint.
        /// Since there can be not concurrent checkpoints this method is not thread safe.
        /// </summary>
        /// <param name="entry"></param>
        /// <param name="storeType"></param>
        /// <param name="fullCheckpoint"></param>
        public void AddCheckpointEntry(CheckpointEntry entry, StoreType storeType, bool fullCheckpoint = false)
        {
            //If not full checkpoint index checkpoint will be the one of the previous checkpoint
            if (!fullCheckpoint)
            {
                var lastEntry = tail;
                Debug.Assert(lastEntry != null);

                entry.storeIndexToken = lastEntry.storeIndexToken;
                entry.objectStoreIndexToken = lastEntry.objectStoreIndexToken;
            }

            //Assume we don't have multiple writers so it is safe to update the tail directly
            if (tail == null)
                head = tail = entry;
            else
            {
                if (storeType == StoreType.Main)
                {
                    entry.objectStoreVersion = tail.objectStoreVersion;
                    entry.objectStoreHlogToken = tail.objectStoreHlogToken;
                    entry.objectStoreIndexToken = tail.objectStoreIndexToken;
                    entry.objectCheckpointCoveredAofAddress = tail.storeCheckpointCoveredAofAddress;
                    entry.objectStorePrimaryReplId = tail.objectStorePrimaryReplId;
                }

                if (storeType == StoreType.Object)
                {
                    entry.storeVersion = tail.storeVersion;
                    entry.storeHlogToken = tail.storeHlogToken;
                    entry.storeIndexToken = tail.storeIndexToken;
                    entry.storeCheckpointCoveredAofAddress = tail.objectCheckpointCoveredAofAddress;
                    entry.storePrimaryReplId = tail.storePrimaryReplId;
                }

                tail.next = entry;
                tail = entry;
            }

            LogCheckpointEntry("Added new checkpoint entry in-memory", tail);

            if (safelyRemoveOutdated)
                DeleteOutdatedCheckpoints();
        }

        /// <summary>
        /// Method used to delete outdated checkpoints from in-memory list of checkpoint entries
        /// </summary>
        private void DeleteOutdatedCheckpoints()
        {
            //If only one in-memory checkpoint return
            if (head == tail) return;

            logger?.LogTrace("Try safe delete in-memory outdated checkpoints");
            var curr = head;
            while (curr != null && curr != tail)
            {
                LogCheckpointEntry("Trying to suspend readers for checkpoint entry", curr);
                //if cannot suspend readers for this entry
                if (!curr.TrySuspendReaders()) break;

                LogCheckpointEntry("Deleting checkpoint entry", curr);
                //Below check each checkpoint token separately if it is eligible for deletion
                if (CanDeleteToken(curr, CheckpointFileType.STORE_HLOG))
                    clusterProvider.GetReplicationLogCheckpointManager(StoreType.Main).DeleteLogCheckpoint(curr.storeHlogToken);

                if (CanDeleteToken(curr, CheckpointFileType.STORE_INDEX))
                    clusterProvider.GetReplicationLogCheckpointManager(StoreType.Main).DeleteIndexCheckpoint(curr.storeIndexToken);

                if (!clusterProvider.serverOptions.DisableObjects)
                {
                    if (CanDeleteToken(curr, CheckpointFileType.OBJ_STORE_HLOG))
                        clusterProvider.GetReplicationLogCheckpointManager(StoreType.Object).DeleteLogCheckpoint(curr.objectStoreHlogToken);

                    if (CanDeleteToken(curr, CheckpointFileType.OBJ_STORE_INDEX))
                        clusterProvider.GetReplicationLogCheckpointManager(StoreType.Object).DeleteIndexCheckpoint(curr.objectStoreIndexToken);
                }

                //At least one token can always be deleted thus invalidating the in-memory entry
                var next = curr.next;
                curr.next = null;
                curr = next;
            }

            //Update head after delete
            head = curr;
        }

        /// <summary>
        /// Check if specific token can be delete by scanning through the entries of the in memory checkpoint store.
        /// If token is not shared we can return immediately and delete it
        /// If token is shared try suspending the addition of new readers
        ///     if failed suspending new readers cannot delete token
        ///     else we need to move up the chain of entries and ensure the token is not shared with other entries
        /// </summary>
        /// <param name="toDelete"></param>
        /// <param name="fileType"></param>
        /// <returns></returns>
        private bool CanDeleteToken(CheckpointEntry toDelete, CheckpointFileType fileType)
        {
            var curr = toDelete.next;
            while (curr != null && curr != tail)
            {
                //Token can be deleted when curr entry and toDelete do not share it
                if (!curr.ContainsSharedToken(toDelete, fileType))
                    return true;

                //If token is shared then try to suspend addition of new readers
                if (!curr.TrySuspendReaders())
                    return false;

                //If suspend new readers succeeds continue up the chain to find how far token is being used
                curr = curr.next;
            }

            //Here we reached the tail so we can delete the token only if it is not shared with the tail
            return !curr.ContainsSharedToken(toDelete, fileType);
        }

        /// <summary>
        /// Return latest checkpoint entry and increment readers counter.
        /// Caller is responsible for releasing reader by calling removeReader on entry
        /// </summary>
        /// <returns></returns>
        public CheckpointEntry GetLatestCheckpointEntryFromMemory()
        {
            var _tail = tail;
            if (_tail == null)
            {
                var cEntry = new CheckpointEntry()
                {
                    storeCheckpointCoveredAofAddress = 0,
                    objectCheckpointCoveredAofAddress = clusterProvider.serverOptions.DisableObjects ? long.MaxValue : 0
                };
                cEntry.TryAddReader();
                return cEntry;
            }
            while (true)
            {
                //Attempt to TryAddReader to ref of tail entry and return if successful
                if (_tail.TryAddReader()) return _tail;
                Thread.Yield();

                //Update reference to latest tail entry and continue trying to add a new reader
                _tail = tail;
            }
        }

        /// <summary>
        /// Return latest checkpoint entry from disk by scanning all available tokens
        /// </summary>
        /// <returns></returns>
        public CheckpointEntry GetLatestCheckpointEntryFromDisk()
        {
            Guid objectStoreHLogToken = default;
            Guid objectStoreIndexToken = default;
            storeWrapper.store.GetLatestCheckpointTokens(out var storeHLogToken, out var storeIndexToken);
            storeWrapper.objectStore?.GetLatestCheckpointTokens(out objectStoreHLogToken, out objectStoreIndexToken);
            var (storeCheckpointCoveredAofAddress, storePrimaryReplId) = GetCheckpointCookieMetadata(StoreType.Main, storeHLogToken);
            var (objectCheckpointCoveredAofAddress, objectStorePrimaryReplId) = objectStoreHLogToken == default ? (long.MaxValue, null) : GetCheckpointCookieMetadata(StoreType.Object, objectStoreHLogToken);

            CheckpointEntry entry = new()
            {
                storeVersion = storeHLogToken == default ? -1 : storeWrapper.store.GetLatestCheckpointVersion(),
                storeHlogToken = storeHLogToken,
                storeIndexToken = storeIndexToken,
                storeCheckpointCoveredAofAddress = storeCheckpointCoveredAofAddress,
                storePrimaryReplId = storePrimaryReplId,

                objectStoreVersion = objectStoreHLogToken == default ? -1 : storeWrapper.objectStore.GetLatestCheckpointVersion(),
                objectStoreHlogToken = objectStoreHLogToken,
                objectStoreIndexToken = objectStoreIndexToken,
                objectCheckpointCoveredAofAddress = objectCheckpointCoveredAofAddress,
                objectStorePrimaryReplId = objectStorePrimaryReplId,
                _lock = new SingleWriterMultiReaderLock()
            };
            return entry;
        }

        private (long, string) GetCheckpointCookieMetadata(StoreType storeType, Guid fileToken)
        {
            if (fileToken == default) return (0, null);
            var ckptManager = clusterProvider.GetReplicationLogCheckpointManager(storeType);
            var pageSizeBits = storeType == StoreType.Main ? clusterProvider.serverOptions.PageSizeBits() : clusterProvider.serverOptions.ObjectStorePageSizeBits();
            using (var deltaFileDevice = ckptManager.GetDeltaLogDevice(fileToken))
            {
                if (deltaFileDevice is not null)
                {
                    deltaFileDevice.Initialize(-1);
                    if (deltaFileDevice.GetFileSize(0) > 0)
                    {
                        var deltaLog = new DeltaLog(deltaFileDevice, pageSizeBits, -1);
                        deltaLog.InitializeForReads();
                        return ckptManager.GetCheckpointCookieMetadata(fileToken, deltaLog, true, -1);
                    }
                }
            }
            return ckptManager.GetCheckpointCookieMetadata(fileToken, null, false, -1);
        }

        public void LogCheckpointEntry(string msg, CheckpointEntry entry)
        {
            logger?.LogTrace("{msg} {storeVersion} {storeHlogToken} {storeIndexToken} {objectStoreVersion} {objectStoreHlogToken} {objectStoreIndexToken}",
                msg,
                entry.storeVersion,
                entry.storeHlogToken,
                entry.storeIndexToken,
                entry.objectStoreVersion,
                entry.objectStoreHlogToken,
                entry.objectStoreIndexToken);
        }
    }
}