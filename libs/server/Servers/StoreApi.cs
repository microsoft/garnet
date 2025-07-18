// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;

namespace Garnet.server
{
    /// <summary>
    /// Store API
    /// </summary>
    public sealed class StoreApi(StoreWrapper storeWrapper)
    {
        /// <summary>
        /// Commit AOF
        /// </summary>
        /// <param name="spinWait">True if should wait until all commits complete</param>
        /// <returns>false if the commit was ignored due to node state or config</returns>
        public bool CommitAOF(bool spinWait = false)
        {
            if (storeWrapper.clusterProvider != null)
            {
                if (!storeWrapper.clusterProvider.PreventRoleChange())
                {
                    return false;
                }

                try
                {
                    if (storeWrapper.clusterProvider.IsReplica())
                    {
                        return false;
                    }

                    // We must always wait for commits to complete if we're
                    // holding the role lock
                    return storeWrapper.CommitAOF(spinWait: true);
                }
                finally
                {
                    storeWrapper.clusterProvider.AllowRoleChange();
                }
            }

            return storeWrapper.CommitAOF(spinWait);
        }

        /// <summary>
        /// Wait for commit
        /// </summary>
        public ValueTask WaitForCommitAsync(CancellationToken token = default) => storeWrapper.WaitForCommitAsync(token: token);

        /// <summary>
        /// Wait for commit
        /// </summary>
        public void WaitForCommit() => storeWrapper.WaitForCommit();

        /// <summary>
        /// Commit AOF
        /// </summary>
        public ValueTask CommitAOFAsync(CancellationToken token) => storeWrapper.CommitAOFAsync(token: token);

        /// <summary>
        /// Flush DB (delete all keys)
        /// Optionally truncate log on disk. This is a destructive operation. Instead take a checkpoint if you are using checkpointing, as
        /// that will safely truncate the log on disk after the checkpoint.
        /// </summary>
        public void FlushDB(int dbId = 0, bool unsafeTruncateLog = false) =>
            storeWrapper.FlushDatabase(unsafeTruncateLog, dbId);
    }
}