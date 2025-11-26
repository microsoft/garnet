// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
        /// Helper to prevent certain operations from happening on a node which is currently a replica.
        /// </summary>
        private sealed class PreventRoleChangeLock(StoreApi storeApi) : IDisposable
        {
            /// <inheritdoc/>
            public void Dispose()
            {
                storeApi.storeWrapper.clusterProvider.AllowRoleChange();

                _ = Interlocked.Exchange(ref storeApi.reusableLock, this);
            }
        }

        private readonly StoreWrapper storeWrapper = storeWrapper;

        private PreventRoleChangeLock reusableLock;

        /// <summary>
        /// Helper for checking if node is currently a replica.
        /// 
        /// Only call under a <see cref="PreventRoleChangeLock"/> using acquired via <see cref="PreventRoleChange"/>.
        /// </summary>
        private bool IsReplica
        => storeWrapper.clusterProvider?.IsReplica() ?? false;

        /// <summary>
        /// Commit AOF
        /// </summary>
        /// <param name="spinWait">True if should wait until all commits complete</param>
        /// <returns>false if the commit was ignored due to node state or config</returns>
        public bool CommitAOF(bool spinWait = false)
        {
            using (PreventRoleChange(out var acquired))
            {
                if (!acquired || IsReplica)
                {
                    return false;
                }

                // If we're in a cluster, we need to wait for the commit to succeed no matter what
                var effectiveSpinWait = spinWait || storeWrapper.clusterProvider != null;
                return storeWrapper.CommitAOF(effectiveSpinWait);
            }
        }

        /// <summary>
        /// Wait for commit
        /// </summary>
        /// <returns>false if the commit was ignored due to node state or config</returns>
        public async ValueTask<bool> WaitForCommitAsync(CancellationToken token = default)
        {
            using (PreventRoleChange(out var acquired))
            {
                if (!acquired || IsReplica)
                {
                    return false;
                }

                return await storeWrapper.WaitForCommitAsync(token: token);
            }
        }

        /// <summary>
        /// Wait for commit
        /// </summary>
        /// <returns>false if the commit was ignored due to node state or config</returns>
        public bool WaitForCommit()
        {
            using (PreventRoleChange(out var acquired))
            {
                if (!acquired || IsReplica)
                {
                    return false;
                }

                return storeWrapper.WaitForCommit();
            }
        }

        /// <summary>
        /// Commit AOF
        /// </summary>
        /// <returns>false if the commit was ignored due to node state or config</returns>
        public async ValueTask<bool> CommitAOFAsync(CancellationToken token)
        {
            using (PreventRoleChange(out var acquired))
            {
                if (!acquired || IsReplica)
                {
                    return false;
                }

                return await storeWrapper.CommitAOFAsync(token: token);
            }
        }

        /// <summary>
        /// Flush DB (delete all keys)
        /// Optionally truncate log on disk. This is a destructive operation. Instead take a checkpoint if you are using checkpointing, as
        /// that will safely truncate the log on disk after the checkpoint.
        /// </summary>
        /// <returns>false if the commit was ignored due to node state or config</returns>
        public bool FlushDB(int dbId = 0, bool unsafeTruncateLog = false)
        {
            using (PreventRoleChange(out var acquired))
            {
                if (!acquired || IsReplica)
                {
                    return false;
                }

                storeWrapper.FlushDatabase(unsafeTruncateLog, dbId);

                return true;
            }
        }

        /// <summary>
        /// Take checkpoint for all active databases
        /// </summary>
        /// <param name="background">True if method can return before checkpoint is taken</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>false if checkpoint was skipped due to node state or another checkpoint in progress</returns>
        public bool TakeCheckpoint(bool background = false, CancellationToken token = default)
        {
            using (PreventRoleChange(out var acquired))
            {
                if (!acquired || IsReplica)
                {
                    return false;
                }

                return storeWrapper.TakeCheckpoint(background, logger: null, token: token);
            }
        }

        /// <summary>
        /// Check if storage tier is enabled
        /// </summary>
        public bool IsStorageTierEnabled => storeWrapper.serverOptions.EnableStorageTier;

        /// <summary>
        /// Check if AOF is enabled
        /// </summary>
        public bool IsAOFEnabled => storeWrapper.serverOptions.EnableAOF;

        /// <summary>
        /// Helper to disable role changes during a using block.
        /// 
        /// <paramref name="acquired"/> is set if, and only if, the role will not change until the return is disposed.
        /// </summary>
        private PreventRoleChangeLock PreventRoleChange(out bool acquired)
        {
            var cluster = storeWrapper.clusterProvider;

            if (cluster == null)
            {
                // True here because, if we're not in a cluster, we have in fact prevented a role change
                acquired = true;
                return null;
            }

            if (!cluster.PreventRoleChange())
            {
                acquired = false;
                return null;
            }

            acquired = true;

            // Assumption is if called once, we'll be called repeatedly, but not concurrently
            // 
            // So try and keep a reusable instance around
            var lockRet = Interlocked.Exchange(ref reusableLock, null);
            lockRet ??= new(this);

            return lockRet;
        }
    }
}