// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed class CheckpointStore
    {
        readonly StoreWrapper storeWrapper;
        readonly ClusterProvider clusterProvider;
        volatile CheckpointEntry head;
        volatile CheckpointEntry tail;
        readonly bool safelyRemoveOutdated;
        readonly ILogger logger;

        public CheckpointStore(StoreWrapper storeWrapper, ClusterProvider clusterProvider, bool safelyRemoveOutdated, ILogger logger = null)
        {
            this.storeWrapper = storeWrapper;
            this.clusterProvider = clusterProvider;
            this.safelyRemoveOutdated = safelyRemoveOutdated;
            this.head = this.tail = null;
            this.logger = logger;

            // When recovering, defer the orphan purge to Initialize() so that nothing is deleted before it is known
            // what recovery was able to read. Otherwise this is the only purge, so run it now.
            if (safelyRemoveOutdated && !clusterProvider.serverOptions.Recover)
                PurgeAllCheckpointsExceptEntry();
        }

        /// <summary>
        /// Initialize in-memory checkpoint store.
        /// Assume during initialization checkpoint requests will not be issued/processed.
        /// </summary>
        public void Initialize()
        {
            head = tail = GetLatestCheckpointEntryFromDisk();

            if (tail.metadata.storeVersion == -1)
            {
                head = tail = null;
            }
            else
            {
                clusterProvider.storeWrapper.StoreCheckpointManager.SetRecoveredSafeAofAddress(ref tail.metadata.storeCheckpointCoveredAofAddress);
                clusterProvider.storeWrapper.StoreCheckpointManager.RecoveredHistoryId = tail.metadata.storePrimaryReplId;
            }

            // This purge does not check for active readers
            // 1. If primary is initializing then we will not have any active readers since not connections are established at recovery
            // 2. If replica is initializing during failover we do this before allowing other replicas to attach.
            // Note that a null tail here means nothing valid was found, and PurgeAllCheckpointsExceptEntry declines
            // to delete anything in that case rather than treating default tokens as the selection to retain.
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
        /// <param name="entry">CheckpointEntry</param>
        public void PurgeAllCheckpointsExceptEntry(CheckpointEntry entry = null)
        {
            entry ??= GetLatestCheckpointEntryFromDisk();
            if (entry == null) return;
            logger?.LogCheckpointEntry(LogLevel.Trace, nameof(PurgeAllCheckpointsExceptEntry), entry);
            PurgeAllCheckpointsExceptTokens(entry.metadata.storeHlogToken, entry.metadata.storeIndexToken);

            void PurgeAllCheckpointsExceptTokens(Guid logToken, Guid indexToken)
            {
                // A default token selects nothing to keep, which would delete every checkpoint on disk. That is
                // never the intent: it means no valid checkpoint was identified, so the artifacts must be preserved.
                if (logToken == default)
                {
                    logger?.LogWarning("Skipping checkpoint purge because no valid HybridLog checkpoint was identified to retain");
                    return;
                }

                var ckptManager = clusterProvider.ReplicationLogCheckpointManager;

                // Delete log checkpoints
                foreach (var toDeletelogToken in ckptManager.GetLogCheckpointTokens())
                {
                    if (!toDeletelogToken.Equals(logToken))
                    {
                        logger?.LogTrace("Deleting log token {toDeletelogToken}", toDeletelogToken);
                        ckptManager.DeleteLogCheckpoint(toDeletelogToken);
                    }
                }

                // Delete index checkpoints
                foreach (var toDeleteIndexToken in ckptManager.GetIndexCheckpointTokens())
                {
                    if (!toDeleteIndexToken.Equals(indexToken))
                    {
                        logger?.LogTrace("Deleting index token {toDeleteIndexToken}", toDeleteIndexToken);
                        ckptManager.DeleteIndexCheckpoint(toDeleteIndexToken);
                    }
                }
            }
        }

        /// <summary>
        /// Add new checkpoint entry to in memory list.
        /// Assume that addition of new entries executes at completion of a checkpoint.
        /// Since there can be not concurrent checkpoints this method is not thread safe.
        /// </summary>
        /// <param name="entry"></param>
        /// <param name="fullCheckpoint"></param>
        public void AddCheckpointEntry(CheckpointEntry entry, bool fullCheckpoint = false)
        {
            // If not full checkpoint index checkpoint will be the one of the previous checkpoint
            if (!fullCheckpoint)
            {
                var lastEntry = tail ?? throw new GarnetException($"Checkpoint history unavailable, need full checkpoint for {entry}");
                entry.metadata.storeIndexToken = lastEntry.metadata.storeIndexToken;
            }

            _ = ValidateCheckpointEntry(entry);

            if (tail == null)
                head = tail = entry;
            else
            {
                // We don't have multiple writers because this method is called under the CheckpointLock
                // So it is safe to update in-place.
                tail.next = entry;
                tail = tail.next;
            }

            logger?.LogCheckpointEntry(LogLevel.Trace, nameof(AddCheckpointEntry), entry);

            if (safelyRemoveOutdated)
                DeleteOutdatedCheckpoints();

            bool ValidateCheckpointEntry(CheckpointEntry entry)
            {
                try
                {
                    if (!clusterProvider.replicationManager.TryAcquireSettledMetadataForMainStore(entry, out _, out _))
                        throw new GarnetException("Failed to validate main store metadata at insertion");

                    return true;
                }
                catch (Exception ex)
                {
                    logger?.LogCheckpointEntry(LogLevel.Error, ex.Message, entry);
                    return false;
                }
            }
        }

        /// <summary>
        /// Method used to delete outdated checkpoints from in-memory list of checkpoint entries
        /// </summary>
        private void DeleteOutdatedCheckpoints()
        {
            // If only one in-memory checkpoint return
            if (head == tail) return;

            logger?.LogTrace("Try safe delete in-memory outdated checkpoints");
            var curr = head;
            while (curr != null && curr != tail)
            {
                logger?.LogCheckpointEntry(LogLevel.Trace, nameof(DeleteOutdatedCheckpoints), curr);

                // If cannot suspend readers for this entry
                if (!curr.TrySuspendReaders()) break;

                logger?.LogTrace("Suspended readers and deleting outdated checkpoint!");

                // Below check each checkpoint token separately if it is eligible for deletion
                if (!CanDeleteToken(curr, CheckpointFileType.STORE_HLOG))
                    break;
                clusterProvider.ReplicationLogCheckpointManager.DeleteLogCheckpoint(curr.metadata.storeHlogToken);

                if (CanDeleteToken(curr, CheckpointFileType.STORE_INDEX))
                    clusterProvider.ReplicationLogCheckpointManager.DeleteIndexCheckpoint(curr.metadata.storeIndexToken);

                logger?.LogCheckpointEntry(LogLevel.Warning, "Deleting outdated checkpoint", curr);

                // At least one token can always be deleted thus invalidating the in-memory entry
                var next = curr.next;
                curr.next = null;
                curr = next;
            }

            // Safely truncate hlog segments up to the oldest active checkpoint's begin address.
            // This is safe because curr is the oldest entry still referenced by active readers.
            if (curr != null)
            {
                try
                {
                    var hlogSize = storeWrapper.store.GetLogFileSize(curr.metadata.storeHlogToken);
                    storeWrapper.store.Log.ShiftBeginAddress(hlogSize.hybridLogFileStartAddress, truncateLog: true);
                }
                catch (Exception ex)
                {
                    logger?.LogWarning(ex, "Skipping hlog truncation for checkpoint {token} because checkpoint metadata could not be read",
                                           curr.metadata.storeHlogToken);
                }
            }

            // Update head after delete
            head = curr;
            logger?.LogCheckpointEntry(LogLevel.Trace, "Current Head", head);

            bool CanDeleteToken(CheckpointEntry toDelete, CheckpointFileType fileType)
            {
                var curr = toDelete.next;
                while (curr != null && curr != tail)
                {
                    // If current entry does not contain a shared token it is safe to delete because
                    // either all previous entries would have contained a shared token and would be locked from readers
                    // in order for the loop to reach the point where this entry is being inspected or it is the first entry being inspected.
                    if (!curr.ContainsSharedToken(toDelete, fileType))
                        return true;

                    // Here we know that the entry contains a shared token so we try to suspend access to readers.
                    // If we fail we can immediately return that we cannot delete this token 
                    if (!curr.TrySuspendReaders())
                        return false;

                    // If suspend new readers succeeds continue up the chain to find how far token is being used
                    curr = curr.next;
                }

                Debug.Assert(curr == tail);
                // Tail is checked separately because we don't want to lock readers, just check for shared tokens.
                return !curr.ContainsSharedToken(toDelete, fileType);
            }
        }

        /// <summary>
        /// Return a lease for the latest checkpoint entry.
        /// </summary>
        /// <returns></returns>
        public bool TryAcquireLatestCheckpoint(out CheckpointLease<CheckpointEntry> lease)
        {
            lease = null;
            var _tail = tail;
            if (_tail == null)
            {
                var emptyEntry = new CheckpointEntry
                {
                    metadata = new(storeWrapper.serverOptions.AofPhysicalSublogCount)
                };
                if (!emptyEntry.TryAddReader())
                    return false;
                lease = new CheckpointLease<CheckpointEntry>(emptyEntry, emptyEntry.RemoveReader);
                return true;
            }

            if (!_tail.TryAddReader())
                return false;

            lease = new CheckpointLease<CheckpointEntry>(_tail, _tail.RemoveReader);
            return true;
        }

        /// <summary>
        /// Return latest checkpoint entry from disk by scanning all available tokens. When nothing valid is found
        /// the returned entry carries default tokens and <c>storeVersion == -1</c>, meaning "no checkpoint here";
        /// callers must not treat that as a checkpoint selection.
        /// </summary>
        /// <returns></returns>
        public CheckpointEntry GetLatestCheckpointEntryFromDisk()
        {
            var storeCheckpointCoveredAofAddress = server.AofAddress.Create(clusterProvider.serverOptions.AofPhysicalSublogCount, 0);
            storeWrapper.store.GetLatestCheckpointTokens(out var storeHLogToken, out var storeIndexToken, out var storeVersion);
            GetCheckpointCookieMetadata(StoreType.Main, storeHLogToken, ref storeCheckpointCoveredAofAddress, out var storePrimaryReplId);

            CheckpointEntry entry = new()
            {
                metadata = new(storeWrapper.serverOptions.AofPhysicalSublogCount)
                {
                    storeVersion = storeVersion,
                    storeHlogToken = storeHLogToken,
                    storeIndexToken = storeIndexToken,
                    storeCheckpointCoveredAofAddress = storeCheckpointCoveredAofAddress,
                    storePrimaryReplId = storePrimaryReplId,
                }
            };
            return entry;

            void GetCheckpointCookieMetadata(StoreType storeType, Guid fileToken, ref AofAddress recoveredSafeAofAddress, out string RecoveredReplicationId)
            {
                RecoveredReplicationId = null;
                if (fileToken == default) return;
                var ckptManager = clusterProvider.ReplicationLogCheckpointManager;

                ckptManager.GetCheckpointCookieMetadata(fileToken, ref recoveredSafeAofAddress, out RecoveredReplicationId);
            }
        }

        /// <summary>
        /// Get latest checkpoint from memory info
        /// </summary>
        /// <returns></returns>
        public string GetLatestCheckpointFromMemoryInfo()
        {
            try
            {
                var _tail = tail;
                if (_tail == null)
                    return "(empty)";
                return _tail.ToString();
            }
            catch
            {
                return "(empty)";
            }
        }

        /// <summary>
        /// Get latest checkpoint from memory info
        /// </summary>
        /// <returns></returns>
        public string GetLatestCheckpointFromDiskInfo()
        {
            try
            {
                var cEntry = GetLatestCheckpointEntryFromDisk();
                if (cEntry == null)
                    return "(empty)";
                return cEntry.ToString();
            }
            catch
            {
                return "(empty)";
            }
        }
    }
}