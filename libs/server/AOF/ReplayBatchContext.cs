// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Replay work item used with recover/replication replay.
    /// </summary>
    /// <param name="replayTasks"></param>
    public unsafe class ReplayBatchContext(int replayTasks)
    {
        /// <summary>
        /// Record pointer.
        /// </summary>
        public byte* Record;
        /// <summary>
        /// Record length.
        /// </summary>
        public int RecordLength;
        /// <summary>
        /// Represents the current address value for a given TsavoriteLog page.
        /// </summary>
        public long CurrentAddress;
        /// <summary>
        /// Represents the next address value for a given TsavoriteLog page.
        /// </summary>
        public long NextAddress;
        /// <summary>
        /// Whether replay occurs under epoch protections.
        /// </summary>
        public bool IsProtected;
        /// <summary>
        /// Leader barrier to coordinate replication offset update.
        /// </summary>
        public LeaderFollowerBarrier LeaderFollowerBarrier = new(replayTasks);
    }
}