// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Outcome of a checkpoint request issued against one or more logical databases.
    /// </summary>
    public enum CheckpointStatus
    {
        /// <summary>
        /// The checkpoint completed and its data is durable. For a background checkpoint this means the checkpoint
        /// was successfully started; its outcome is reported through the database's last save time and status.
        /// </summary>
        Success,

        /// <summary>
        /// A checkpoint is already in progress for at least one of the requested databases, so no new checkpoint
        /// was started. Nothing was written and no existing checkpoint was invalidated.
        /// </summary>
        AlreadyInProgress,

        /// <summary>
        /// The checkpoint was started but did not complete, so nothing durable was written.
        /// </summary>
        Failed,
    }

    /// <summary>
    /// Outcome of a single database's checkpoint attempt.
    /// </summary>
    internal readonly struct CheckpointResult
    {
        /// <summary>
        /// True if the checkpoint completed and its data is durable. Only then may the database's last save time
        /// be advanced; advancing it for a failed checkpoint reports data as durable that was never written.
        /// </summary>
        public bool IsSuccessful { get; init; }

        /// <summary>
        /// Store tail address covered by a full checkpoint, or null for an incremental checkpoint. Null is also
        /// returned for a failed checkpoint, which is why it cannot by itself signal failure.
        /// </summary>
        public long? StoreTailAddress { get; init; }

        /// <summary>
        /// A result denoting a checkpoint that did not complete.
        /// </summary>
        public static CheckpointResult Failed => default;

        /// <summary>
        /// Creates a result denoting a completed checkpoint.
        /// </summary>
        /// <param name="storeTailAddress">Store tail address covered by a full checkpoint, or null for an incremental checkpoint.</param>
        /// <returns>A successful result.</returns>
        public static CheckpointResult Succeeded(long? storeTailAddress) =>
            new() { IsSuccessful = true, StoreTailAddress = storeTailAddress };
    }
}