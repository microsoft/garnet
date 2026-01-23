// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Replay work item used with recover/replication replay.
    /// </summary>
    /// <param name="replayTasks"></param>
    public unsafe class ReplayWorkItem(int replayTasks)
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
        /// Signals when work is ready to be processed.
        /// </summary>
        public SemaphoreSlim WorkReady = new(0);
        /// <summary>
        /// Signal processing completion.
        /// </summary>
        public ManualResetEventSlim WorkCompleted = new(true);
        /// <summary>
        /// Leader barrier to coordinate replication offset update.
        /// </summary>
        public LeaderBarier LeaderBarrier = new(replayTasks);

        /// <summary>
        /// Reset signals associated with this instance.
        /// </summary>
        public void Reset()
        {
            WorkCompleted.Reset();
            LeaderBarrier.Reset();
        }
    }
}
