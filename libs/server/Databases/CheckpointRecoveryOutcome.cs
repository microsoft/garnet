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