// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Tracks standalone checkpoints and prevents leased files from being deleted.
    /// </summary>
    internal sealed class StandaloneCheckpointStore
    {
        sealed class Entry(CheckpointMetadata metadata)
        {
            internal readonly CheckpointMetadata Metadata = metadata;
            internal int Readers;
        }
        readonly object sync = new();
        readonly GarnetCheckpointManager checkpointManager;
        readonly List<Entry> entries = [];

        internal StandaloneCheckpointStore(GarnetCheckpointManager checkpointManager)
        {
            this.checkpointManager = checkpointManager;
        }

        /// <summary>
        /// Initializes the catalogue with the checkpoint recovered at startup.
        /// </summary>
        internal void Initialize(CheckpointMetadata metadata)
        {
            if (metadata == null || metadata.storeHlogToken == default)
                return;

            lock (sync)
            {
                entries.Clear();
                entries.Add(new Entry(metadata));

                foreach (var token in checkpointManager.GetLogCheckpointTokens())
                {
                    if (token != metadata.storeHlogToken)
                        checkpointManager.DeleteLogCheckpoint(token);
                }
                foreach (var token in checkpointManager.GetIndexCheckpointTokens())
                {
                    if (token != metadata.storeIndexToken)
                        checkpointManager.DeleteIndexCheckpoint(token);
                }
            }
        }

        /// <summary>
        /// Adds a completed checkpoint and removes unleased obsolete checkpoint files.
        /// </summary>
        internal void AddCheckpoint(
            bool fullCheckpoint,
            long storeVersion,
            Guid checkpointToken,
            in AofAddress coveredAofAddress,
            string historyId)
        {
            lock (sync)
            {
                var indexToken = checkpointToken;
                if (!fullCheckpoint)
                {
                    if (entries.Count == 0)
                        throw new GarnetException("Checkpoint history unavailable; a full checkpoint is required");
                    indexToken = entries[^1].Metadata.storeIndexToken;
                }

                entries.Add(new Entry(new CheckpointMetadata(coveredAofAddress.Length)
                {
                    storeVersion = storeVersion,
                    storeHlogToken = checkpointToken,
                    storeIndexToken = indexToken,
                    storeCheckpointCoveredAofAddress = coveredAofAddress,
                    storePrimaryReplId = historyId
                }));

                DeleteUnleasedCheckpoints();
            }
        }

        /// <summary>
        /// Attempts to lease the latest completed checkpoint.
        /// </summary>
        internal bool TryAcquireLatest(out CheckpointLease<CheckpointMetadata> lease)
        {
            lock (sync)
            {
                lease = null;
                if (entries.Count == 0)
                    return false;

                var entry = entries[^1];
                entry.Readers++;
                lease = new CheckpointLease<CheckpointMetadata>(entry.Metadata, () => Release(entry));
                return true;
            }
        }

        void Release(Entry entry)
        {
            lock (sync)
            {
                if (entry.Readers <= 0)
                    throw new GarnetException("Checkpoint lease released without an active reader");
                entry.Readers--;
                DeleteUnleasedCheckpoints();
            }
        }

        void DeleteUnleasedCheckpoints()
        {
            for (var index = 0; index < entries.Count - 1;)
            {
                var entry = entries[index];
                if (entry.Readers != 0)
                {
                    index++;
                    continue;
                }

                entries.RemoveAt(index);
                if (!ContainsLogToken(entry.Metadata.storeHlogToken))
                    checkpointManager.DeleteLogCheckpoint(entry.Metadata.storeHlogToken);
                if (!ContainsIndexToken(entry.Metadata.storeIndexToken))
                    checkpointManager.DeleteIndexCheckpoint(entry.Metadata.storeIndexToken);
            }
        }

        bool ContainsLogToken(Guid token)
        {
            foreach (var entry in entries)
            {
                if (entry.Metadata.storeHlogToken == token)
                    return true;
            }
            return false;
        }

        bool ContainsIndexToken(Guid token)
        {
            foreach (var entry in entries)
            {
                if (entry.Metadata.storeIndexToken == token)
                    return true;
            }
            return false;
        }
    }
}