// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// What checkpoint recovery found on disk and what it managed to recover, for a single database.
    /// </summary>
    public struct CheckpointRecoveryOutcome
    {
        /// <summary>
        /// Store version recovered from a checkpoint, or zero if no checkpoint was recovered.
        /// </summary>
        public long StoreVersion;

        /// <summary>
        /// Number of HybridLog checkpoint tokens present on disk when recovery scanned for one.
        /// </summary>
        public int CandidateTokenCount;

        /// <summary>
        /// Number of those tokens whose metadata could not be read.
        /// </summary>
        public int UnreadableTokenCount;

        /// <summary>
        /// Storage-slot to logical-database mapping recorded in the recovered checkpoint, as
        /// <c>DatabaseMapping[storageSlot] = logicalDatabaseId</c>, or null if it recorded none.
        /// Every database records the whole mapping, so one checkpoint describes the full permutation.
        /// </summary>
        public int[] DatabaseMapping;

        /// <summary>
        /// Swap epoch that <see cref="DatabaseMapping"/> belongs to. Databases are checkpointed
        /// individually and can therefore disagree; the highest epoch wins.
        /// </summary>
        public long SwapEpoch;

        /// <summary>
        /// Checkpoint format version of the recovered checkpoint, or zero if none was recovered.
        /// </summary>
        public int CheckpointVersion;

        /// <summary>
        /// True if a checkpoint was recovered into the store.
        /// </summary>
        public readonly bool CheckpointRecovered => StoreVersion > 0;

        /// <summary>
        /// True if checkpoint tokens exist on disk but none of them yielded a usable checkpoint. A checkpointed
        /// prefix of the database therefore exists that recovery was unable to load, which is distinct from a
        /// fresh start where no checkpoint was ever written.
        /// </summary>
        public readonly bool CheckpointTokensRejected => !CheckpointRecovered && CandidateTokenCount > 0;
    }
}