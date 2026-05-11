// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Identifies the checkpoint lifecycle point at which <see cref="IRecordTriggers.OnCheckpoint"/> is called.
    /// </summary>
    public enum CheckpointTrigger
    {
        /// <summary>
        /// PREPARE → IN_PROGRESS transition (version shift from v to v+1).
        /// The application should set a barrier to prevent v+1 operations
        /// from modifying external resources.
        /// </summary>
        VersionShift,

        /// <summary>
        /// WAIT_FLUSH phase, after all v threads have completed and before the
        /// snapshot flush begins. The application should snapshot external resources
        /// and clear the barrier set during <see cref="VersionShift"/>.
        /// </summary>
        FlushBegin,

        /// <summary>
        /// REST phase, after the checkpoint is fully persisted. The application
        /// should clean up outdated external checkpoint artifacts.
        /// </summary>
        CheckpointCompleted
    }
}