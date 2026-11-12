// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class FailoverSession : IDisposable
    {
        /// <summary>
        /// How long the replication offset may stay unchanged before checking that the replication stream is still active
        /// </summary>
        static readonly TimeSpan ReplicationLivenessCheckInterval = TimeSpan.FromSeconds(1);

        /// <summary>
        /// Upper bound for querying the primary's replication state when diagnosing a failed sync
        /// </summary>
        static readonly TimeSpan PrimaryStateQueryTimeout = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Connection to primary if reachable
        /// </summary>
        GarnetClient primaryClient = null;

        /// <summary>
        /// Helper method to establish connection towards remote node
        /// </summary>
        /// <param name="nodeId">Id of node to create connection for</param>
        /// <returns></returns>
        private Task<GarnetClient> CreateConnectionAsync(string nodeId)
            => CreateConnectionAsync(nodeId, failoverTimeout, cts.Token);

        /// <summary>
        /// Helper method to establish connection towards remote node
        /// </summary>
        /// <param name="nodeId">Id of node to create connection for</param>
        /// <param name="timeout">Upper bound for establishing the connection</param>
        /// <param name="token">Token used to cancel connection establishment</param>
        /// <returns></returns>
        private async Task<GarnetClient> CreateConnectionAsync(string nodeId, TimeSpan timeout, CancellationToken token)
        {
            var endpoint = oldConfig.GetEndpointFromNodeId(nodeId);
            var client = new GarnetClient(
                endpoint,
                clusterProvider.serverOptions.TlsOptions?.TlsClientOptions,
                authUsername: clusterProvider.ClusterUsername,
                authPassword: clusterProvider.ClusterPassword, epoch: epoch, logger: logger);

            try
            {
                if (!client.IsConnected)
                    await client.ReconnectAsync((int)Math.Min(timeout.TotalMilliseconds, int.MaxValue), token).ConfigureAwait(false);

                return client;
            }
            catch (Exception ex)
            {
                client?.Dispose();
                logger?.LogError(ex, "ReplicaFailoverSession.CreateConnection");
                return null;
            }
        }

        /// <summary>
        /// Acquire a connection to the node identified by given node-id.
        /// </summary>
        /// <param name="nodeId"></param>
        /// <returns></returns>
        private Task<GarnetClient> GetConnectionAsync(string nodeId)
            => CreateConnectionAsync(nodeId);

        /// <summary>
        /// Send stop writes message to PRIMARY
        /// </summary>
        /// <returns>True on success, false otherwise</returns>
        private async Task<bool> PauseWritesAndWaitForSyncAsync()
        {
            var primaryId = oldConfig.LocalNodePrimaryId;
            var client = await GetConnectionAsync(primaryId).ConfigureAwait(false);
            try
            {
                if (client == null)
                {
                    logger?.LogError("Failed to initialize connection to primary {primaryId}", primaryId);
                    return false;
                }

                // Cache connection for use with next operations
                primaryClient = client;

                // Issue stop writes to the primary
                status = FailoverStatus.ISSUING_PAUSE_WRITES;
                var localIdBytes = Encoding.ASCII.GetBytes(oldConfig.LocalNodeId);

                var resp = await client.ExecuteClusterFailStopWritesAsync(localIdBytes).WaitAsync(failoverTimeout, cts.Token).ConfigureAwait(false);
                var primaryReplicationOffset = AofAddress.FromString(resp);

                // Wait for replica to catch up
                status = FailoverStatus.WAITING_FOR_SYNC;
#if DEBUG
                await ExceptionInjectionHelper.ResetAndWaitAsync(ExceptionInjectionType.Failover_Pause_Waiting_For_Sync).ConfigureAwait(false);
#endif
                return await WaitForReplicationSyncAsync(primaryReplicationOffset).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "PauseWritesAndWaitForSync Error");
                return false;
            }
        }

        /// <summary>
        /// Wait until the local replication offset reaches the offset the primary returned on FAILSTOPWRITES.
        /// The primary has stopped writes and demoted itself at this point, so a lost replication stream cannot be
        /// re-established; the wait fails early when the stream is gone or the offset stops advancing.
        /// </summary>
        /// <param name="targetOffset">Primary AOF tail address returned by FAILSTOPWRITES</param>
        /// <returns>True if the replica caught up, false otherwise</returns>
        private async Task<bool> WaitForReplicationSyncAsync(AofAddress targetOffset)
        {
            var lastOffset = clusterProvider.replicationManager.ReplicationOffset;
            var lastProgressTimestamp = Stopwatch.GetTimestamp();
            var lastLivenessCheckTimestamp = lastProgressTimestamp;

            while (true)
            {
                var currentOffset = clusterProvider.replicationManager.ReplicationOffset;
                if (!targetOffset.AnyGreater(currentOffset))
                    return true;

                string reason = null;
                if (cts.Token.IsCancellationRequested)
                {
                    reason = "failover was cancelled";
                }
                else if (FailoverTimeout)
                {
                    reason = $"failover timeout of {failoverTimeout} was reached";
                }
                else if (!currentOffset.EqualsAll(lastOffset))
                {
                    lastOffset = currentOffset;
                    lastProgressTimestamp = Stopwatch.GetTimestamp();
                }
                else if (Stopwatch.GetElapsedTime(lastProgressTimestamp) >= ReplicationLivenessCheckInterval &&
                         Stopwatch.GetElapsedTime(lastLivenessCheckTimestamp) >= ReplicationLivenessCheckInterval)
                {
                    lastLivenessCheckTimestamp = Stopwatch.GetTimestamp();
                    if (!IsReplicationStreamActive())
                        reason = "replication stream from the primary is not active";
                    else if (clusterTimeout > TimeSpan.Zero && Stopwatch.GetElapsedTime(lastProgressTimestamp) >= clusterTimeout)
                        reason = $"replication offset made no progress for {clusterTimeout}";
                }

                if (reason != null)
                {
                    await LogReplicationSyncStateAsync(reason, targetOffset, currentOffset, lastProgressTimestamp).ConfigureAwait(false);
                    return false;
                }

                await Task.Yield();
            }
        }

        /// <summary>
        /// Whether any session is currently receiving the AOF stream from the primary.
        /// </summary>
        private bool IsReplicationStreamActive()
        {
            foreach (var server in clusterProvider.storeWrapper.Servers)
            {
                if (server is not GarnetServerBase garnetServer)
                    continue;

                foreach (var session in garnetServer.ActiveClusterSessions())
                {
                    if (session.IsReplicating)
                        return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Log the state of replication when the wait for sync stops before the replica caught up.
        /// </summary>
        private async Task LogReplicationSyncStateAsync(string reason, AofAddress targetOffset, AofAddress replicaOffset, long lastProgressTimestamp)
        {
            var replicationManager = clusterProvider.replicationManager;
            var linkStatus = clusterProvider.clusterManager.GetPrimaryLinkStatus(clusterProvider.clusterManager.CurrentConfig);
            var primaryState = await GetPrimaryReplicationStateAsync().ConfigureAwait(false);

            logger?.LogError(
                "Stopped waiting for replication sync: {reason}. " +
                "TargetOffset (primary tail at FAILSTOPWRITES): {targetOffset}, ReplicaOffset: {replicaOffset}, LastProgress: {sinceProgress} ago, " +
                "ReplicationStreamActive: {streamActive}, Recovering: {recovering} ({recoveryStatus}), PrimaryGossipLink: {linkStatus} (last io {lastIo}s ago), " +
                "Primary: {primaryState}",
                reason,
                targetOffset,
                replicaOffset,
                Stopwatch.GetElapsedTime(lastProgressTimestamp),
                IsReplicationStreamActive(),
                replicationManager.IsRecovering,
                replicationManager.currentRecoveryStatus,
                linkStatus[0].Value,
                linkStatus[1].Value,
                primaryState);
        }

        /// <summary>
        /// Query the primary for its replication state, or describe why it cannot be reached.
        /// </summary>
        private async Task<string> GetPrimaryReplicationStateAsync()
        {
            if (primaryClient == null || !primaryClient.IsConnected)
                return "unreachable (connection to primary is closed)";

            try
            {
                var info = await primaryClient.ExecuteForStringResultAsync("INFO", ["REPLICATION"]).WaitAsync(PrimaryStateQueryTimeout).ConfigureAwait(false);
                var lines = info.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

                // A demoted primary reports its replica-side offset; its AOF tail is the target offset since writes are stopped
                var isPrimary = Array.IndexOf(lines, "role:master") >= 0;
                var state = new StringBuilder();
                foreach (var line in lines)
                {
                    if (line.StartsWith("role:", StringComparison.Ordinal) ||
                        line.StartsWith("connected_slaves:", StringComparison.Ordinal) ||
                        line.StartsWith("sync_driver_count:", StringComparison.Ordinal) ||
                        (isPrimary && line.StartsWith("master_repl_offset:", StringComparison.Ordinal)) ||
                        (line.StartsWith("slave", StringComparison.Ordinal) && line.Length > 5 && char.IsAsciiDigit(line[5])))
                    {
                        if (state.Length > 0)
                            _ = state.Append(", ");
                        _ = state.Append(line);
                    }
                }
                return state.ToString();
            }
            catch (Exception ex)
            {
                // Drop the connection so a later reset of the primary starts from a fresh one
                primaryClient.Dispose();
                primaryClient = null;
                return $"unreachable ({ex.GetType().Name}: {ex.Message})";
            }
        }

        /// <summary>
        /// Perform series of steps to update local config and take ownership of primary slots.
        /// </summary>
        private async Task<bool> TakeOverAsPrimaryAsync()
        {
            // Take over as primary and inform old primary
            status = FailoverStatus.TAKING_OVER_AS_PRIMARY;
            var acquiredLock = false;

            try
            {
                // Exception injection point for testing: simulates TakeOverAsPrimary failure
                // after PauseWritesAndWaitForSync has already sent failstopwrites to the primary.
                ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Failover_Fail_TakeOverAsPrimary);

                // Make replica syncing unavailable by setting recovery flag
                if (!clusterProvider.replicationManager.BeginRecovery(RecoveryStatus.ClusterFailover, upgradeLock: false))
                {
                    logger?.LogWarning($"{nameof(TakeOverAsPrimaryAsync)}: {{logMessage}}", Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_CANNOT_ACQUIRE_RECOVERY_LOCK));
                    return false;
                }
                acquiredLock = true;
                _ = await clusterProvider.BumpAndWaitForEpochTransitionAsync(cts.Token).ConfigureAwait(false);
                cts.Token.ThrowIfCancellationRequested();

                // Take over slots from old primary
                if (!clusterProvider.clusterManager.TryTakeOverForPrimary())
                {
                    logger?.LogWarning($"{nameof(TakeOverAsPrimaryAsync)}: {{logMessage}}", Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_CANNOT_TAKEOVER_FROM_PRIMARY));
                    return false;
                }

                // Update replicationIds and replicationOffset2
                clusterProvider.replicationManager.TryUpdateForFailover();

                // Cancel active replication tasks
                clusterProvider.replicationManager.ResetReplicaReplayDriverStore();

                // Update sequence number generator for sharded log if needed
                if (clusterProvider.serverOptions.AofPhysicalSublogCount > 1)
                {
                    clusterProvider.storeWrapper.appendOnlyFile.ResetSequenceNumberGenerator();
                }

                // Initialize checkpoint history
                if (!clusterProvider.replicationManager.InitializeCheckpointStore())
                    logger?.LogWarning("Failed acquiring latest memory checkpoint metadata at {method}", nameof(TakeOverAsPrimaryAsync));

                _ = clusterProvider.BumpAndWaitForEpochTransitionAsync().ConfigureAwait(false);

                // Resume all background maintenance that were possibly shutdown when this node became a replica
                clusterProvider.storeWrapper.StartPrimaryTasks();
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "{method}", nameof(TakeOverAsPrimaryAsync));
                throw;
            }
            finally
            {
                // Disable recovering as now this node has become a primary or failed in its attempt earlier
                if (acquiredLock)
                    clusterProvider.replicationManager.EndRecovery(RecoveryStatus.NoRecovery, downgradeLock: false);
            }

            return true;
        }

        /// <summary>
        /// Issue gossip and attach request to replica
        /// </summary>
        /// <param name="replicaId">Replica-id to issue gossip and attache request</param>
        /// <returns></returns>
        private async Task BroadcastConfigAndRequestAttachAsync(string replicaId)
        {
            // Force async
            await Task.Yield();

            var oldPrimaryId = oldConfig.LocalNodePrimaryId;
            var newConfig = clusterProvider.clusterManager.CurrentConfig;
            var client = oldPrimaryId.Equals(replicaId) && primaryClient != null
                ? primaryClient
                : await GetConnectionAsync(replicaId).ConfigureAwait(false);

            try
            {
                if (client == null)
                {
                    logger?.LogError("Failed to initialize connection to replica {replicaId}", replicaId);
                    return;
                }

                // Force send updated config to replica
                GossipVersionNegotiation negotiation = new(client, clusterProvider.clusterManager);
                using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
                timeout.CancelAfter(failoverTimeout);
                ClusterConfig other = await negotiation.ExchangeAsync(newConfig, withMeet: false, replicaId, timeout.Token).ConfigureAwait(false);

                try
                {
                    var current = clusterProvider.clusterManager.CurrentConfig;
                    if (other != null)
                    {
                        // Check if gossip is from a node that is known and trusted before merging
                        if (current.IsKnown(other.LocalNodeId))
                            _ = clusterProvider.clusterManager.TryMerge(other);
                        else
                            logger?.LogWarning("Received gossip from unknown node: {node-id}", other.LocalNodeId);
                    }
                }
                catch (Exception ex)
                {
                    logger?.LogCritical(ex, "IssueAttachReplicas faulted");
                }

                var (localAddress, localPort) = oldConfig.GetWorkerAddress(ClusterConfig.LOCAL_WORKER_ID);

                // Ask replica to attach and sync
                var replicaOfResp = await client.ReplicaOf(localAddress, localPort).WaitAsync(failoverTimeout, cts.Token).ConfigureAwait(false);

                // Check if response for attach succeeded
                if (!replicaOfResp.Equals("OK"))
                    logger?.LogWarning("IssueAttachReplicas Error: {replicaId} {replicaOfResp}", replicaId, replicaOfResp);
            }
            finally
            {
                client?.Dispose();
            }
        }

        /// <summary>
        /// Issue attach message to remote replicas
        /// </summary>
        /// <returns></returns>
        private async Task IssueAttachReplicasAsync()
        {
            // Get information of local node from newConfig
            var newConfig = clusterProvider.clusterManager.CurrentConfig;
            // Get replica ids for old primary from old configuration
            var oldPrimaryId = oldConfig.LocalNodePrimaryId;
            var replicaIds = newConfig.GetReplicaIds(oldPrimaryId);
            var attachReplicaTasks = new List<Task>();

            // A coordinated primary-initiated handoff demotes the old primary before requesting takeover.
            if (option is FailoverOption.DEFAULT ||
                newConfig.GetReplicaIds(newConfig.LocalNodeId).Contains(oldPrimaryId))
            {
                replicaIds.Add(oldPrimaryId);
            }

            // Issue gossip and attach request to replicas
            foreach (var replicaId in replicaIds)
            {
                try
                {
                    attachReplicaTasks.Add(BroadcastConfigAndRequestAttachAsync(replicaId));
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "IssueAttachReplicas Error");
                }
            }

            // Wait for tasks to complete
            if (attachReplicaTasks.Count > 0)
            {
                try
                {
                    await Task.WhenAll(attachReplicaTasks).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger?.LogWarning(ex, "WaitingForAttachToComplete Error");
                }
            }
        }

        /// <summary>
        /// Returns true if failstopwrites was confirmed by the primary and the primary's
        /// config was modified (slots given up, role changed to replica). Used to determine
        /// whether the primary needs to be reset on failover failure.
        /// </summary>
        private bool PrimaryNeedsReset()
            => status is FailoverStatus.WAITING_FOR_SYNC or FailoverStatus.TAKING_OVER_AS_PRIMARY;

        /// <summary>
        /// Restore the old primary after a failed failover that it had already accepted.
        /// The failover token is not used because it is already cancelled when the failover was aborted.
        /// Attempts are retried with capped exponential backoff until the primary acknowledges the reset,
        /// the reset budget (<see cref="GarnetServerOptions.FailoverResetPrimaryTimeout"/>) expires, or the server shuts down.
        /// A lost connection to the primary is re-established on each attempt.
        /// </summary>
        /// <param name="shutdownToken">Token signalled on server shutdown</param>
        /// <returns>True if the primary acknowledged the reset, false otherwise</returns>
        private async Task<bool> ResetPrimaryAsync(CancellationToken shutdownToken)
        {
            var primaryId = oldConfig.LocalNodePrimaryId;
            var budget = TimeSpan.FromSeconds(Math.Max(1, clusterProvider.serverOptions.FailoverResetPrimaryTimeout));
            var attemptTimeout = clusterTimeout > TimeSpan.Zero ? clusterTimeout : failoverTimeout;
            if (attemptTimeout > budget)
                attemptTimeout = budget;
            var backoff = new ExponentialBackoff(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(5));

            using var budgetCts = CancellationTokenSource.CreateLinkedTokenSource(shutdownToken);
            budgetCts.CancelAfter(budget);

            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    if (primaryClient == null || !primaryClient.IsConnected)
                    {
                        primaryClient?.Dispose();
                        primaryClient = await CreateConnectionAsync(primaryId, attemptTimeout, budgetCts.Token).ConfigureAwait(false);
                    }

                    if (primaryClient != null)
                    {
                        using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(budgetCts.Token);
                        attemptCts.CancelAfter(attemptTimeout);
                        _ = await primaryClient.ExecuteClusterFailStopWritesAsync(Array.Empty<byte>(), attemptCts.Token).ConfigureAwait(false);
                        logger?.LogWarning("Reset primary {primaryId} after failed failover (attempt {attempt})", primaryId, attempt);
                        return true;
                    }
                }
                catch (OperationCanceledException) when (budgetCts.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger?.LogWarning(ex, "Attempt {attempt} to reset primary {primaryId} failed", attempt, primaryId);
                    primaryClient?.Dispose();
                    primaryClient = null;
                }

                try
                {
                    await Task.Delay(backoff.RecordFailure(), budgetCts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }

            logger?.LogError("Failed to reset primary {primaryId} after failed failover within {budget} ({reason}) — cluster may be in an incoherent state",
                primaryId, budget, shutdownToken.IsCancellationRequested ? "server shutdown" : "reset budget expired");
            return false;
        }

        /// <summary>
        /// REPLICA main failover task
        /// </summary>
        /// <param name="shutdownToken">Token signalled on server shutdown; used to bound the primary reset on failure</param>
        /// <returns></returns>
        public async Task<bool> BeginAsyncReplicaFailoverAsync(CancellationToken shutdownToken = default)
        {
            // FORCE and TAKEOVER skip primary coordination and replication catch-up.
            // An external coordinator authorizes and serializes promotions; Garnet does not perform voting.
            var failoverSucceeded = false;
            try
            {
                // Issue stop writes and on ack wait for replica to catch up
                if (option is FailoverOption.DEFAULT && !await PauseWritesAndWaitForSyncAsync().ConfigureAwait(false))
                {
                    return false;
                }

                // Transition to primary role
                if (!await TakeOverAsPrimaryAsync().ConfigureAwait(false))
                {
                    return false;
                }
                failoverSucceeded = true;

                // Attach to old replicas, and old primary if DEFAULT option
                await IssueAttachReplicasAsync().ConfigureAwait(false);

                await clusterProvider.storeWrapper.SuspendReplicaOnlyTasksAsync();
                clusterProvider.storeWrapper.StartPrimaryTasks();

                return true;
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "BeginAsyncReplicaFailover Error");
                return false;
            }
            finally
            {
                // If failstopwrites was confirmed by the primary (status reached WAITING_FOR_SYNC
                // or beyond) but the failover did not succeed, reset the primary back to its
                // original state. Without this, the primary has already given up its slots
                // (via TryStopWrites) but the replica never claimed them, leaving the cluster
                // in an incoherent state where no node owns the slots.
                if (PrimaryNeedsReset() && !failoverSucceeded)
                {
                    logger?.LogWarning("Attempting to reset primary after failed failover");
                    _ = await ResetPrimaryAsync(shutdownToken).ConfigureAwait(false);
                }

                primaryClient?.Dispose();
                status = FailoverStatus.NO_FAILOVER;
            }
        }
    }
}