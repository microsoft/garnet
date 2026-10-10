// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Checkpoint type
    /// </summary>
    public enum CheckpointType
    {
        /// <summary>
        /// Take separate snapshot of in-memory portion of log (default)
        /// </summary>
        Snapshot,

        /// <summary>
        /// Flush current log by moving the read-only boundary to the tail; the log grows faster than with a Snapshot
        /// </summary>
        FoldOver,

        /// <summary>
        /// Yield a stream of key-value records in version (v), that can be used to rebuild the store
        /// </summary>
        StreamingSnapshot,
    }

    /// <summary>
    /// Checkpoint-related settings
    /// </summary>
    internal class CheckpointSettings
    {
        /// <summary>
        /// Checkpoint manager
        /// </summary>
        public ICheckpointManager CheckpointManager = null;

        /// <summary>
        /// Use specified directory for storing and retrieving checkpoints
        /// using local storage device.
        /// </summary>
        public string CheckpointDir = null;

        /// <summary>
        /// Whether Tsavorite should remove outdated checkpoints automatically
        /// </summary>
        public bool RemoveOutdated = false;

        /// <summary>
        /// Whether we should throttle the disk IO for checkpoints (one write at a time, wait between each write) and issue IO from separate task (-1 = throttling disabled)
        /// </summary>
        public int ThrottleCheckpointFlushDelayMs = -1;
    }
}