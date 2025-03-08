// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;

namespace Garnet.server
{
    /// <summary>
    /// Store API
    /// </summary>
    public class StoreApi
    {
        readonly StoreWrapper storeWrapper;

        /// <summary>
        /// Construct new Store API instance
        /// </summary>
        /// <param name="storeWrapper"></param>
        public StoreApi(StoreWrapper storeWrapper)
        {
            this.storeWrapper = storeWrapper;
        }

        /// <summary>
        /// Commit AOF
        /// </summary>
        /// <param name="spinWait"></param>
        public void CommitAOF(bool spinWait = false) => storeWrapper.CommitAOF(spinWait);

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