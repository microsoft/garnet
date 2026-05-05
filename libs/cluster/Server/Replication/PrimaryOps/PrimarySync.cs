// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading.Tasks;

#if DEBUG
using Garnet.common;
#endif
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class ReplicationManager : IDisposable
    {
        readonly ReplicaSyncSessionTaskStore replicaSyncSessionTaskStore;

        /// <summary>
        /// Try attach sync session from replica (unified path for both diskless and disk-based modes).
        /// </summary>
        /// <param name="replicaSyncMetadata"></param>
        /// <returns></returns>
        public async Task<(bool Success, ReadOnlyMemory<byte> ErrorMessage)> TryBeginDisklessSyncAsync(SyncMetadata replicaSyncMetadata)
        {
            ReadOnlyMemory<byte> errorMessage = default;

            if (!replicationSyncManager.AddReplicaSyncSession(replicaSyncMetadata, out var replicaSyncSession))
            {
                replicaSyncSession?.Dispose();
                errorMessage = CmdStrings.RESP_ERR_CREATE_SYNC_SESSION_ERROR.ToArray();
                logger?.LogError("{errorMessage}", Encoding.ASCII.GetString(errorMessage.Span));
            }

#if DEBUG
            await ExceptionInjectionHelper.ResetAndWaitAsync(ExceptionInjectionType.Replication_InProgress_During_Diskless_Replica_Attach_Sync).ConfigureAwait(false);
#endif

            var status = await replicationSyncManager.ReplicationSyncDriverAsync(replicaSyncSession).ConfigureAwait(false);
            if (status.syncStatus == SyncStatus.FAILED)
                errorMessage = Encoding.ASCII.GetBytes(status.error);

            return (true, errorMessage);
        }

        /// <summary>
        /// Begin disk-based replica sync session.
        /// Routes through the unified broadcast coordinator or legacy per-replica path based on configuration.
        /// </summary>
        public Task<(bool Success, ReadOnlyMemory<byte> ErrorMessage)> TryBeginDiskbasedSyncAsync(
            string replicaNodeId,
            string replicaAssignedPrimaryId,
            CheckpointEntry replicaCheckpointEntry,
            AofAddress replicaAofBeginAddress,
            AofAddress replicaAofTailAddress)
        {
            if (clusterProvider.serverOptions.ReplicaDiskbasedBroadcast)
                return TryBeginDiskbasedBroadcastSyncAsync(replicaNodeId, replicaAssignedPrimaryId, replicaCheckpointEntry, replicaAofBeginAddress, replicaAofTailAddress);

            return TryBeginDiskbasedLegacySyncAsync(replicaNodeId, replicaAssignedPrimaryId, replicaCheckpointEntry, replicaAofBeginAddress, replicaAofTailAddress);
        }

        /// <summary>
        /// Unified broadcast coordinator path for disk-based sync.
        /// </summary>
        async Task<(bool Success, ReadOnlyMemory<byte> ErrorMessage)> TryBeginDiskbasedBroadcastSyncAsync(
            string replicaNodeId,
            string replicaAssignedPrimaryId,
            CheckpointEntry replicaCheckpointEntry,
            AofAddress replicaAofBeginAddress,
            AofAddress replicaAofTailAddress)
        {
            ReadOnlyMemory<byte> errorMessage = default;

            if (!replicationSyncManager.AddDiskbasedReplicaSyncSession(replicaNodeId, replicaAssignedPrimaryId, replicaCheckpointEntry, replicaAofBeginAddress, replicaAofTailAddress, out var replicaSyncSession))
            {
                replicaSyncSession?.Dispose();
                errorMessage = CmdStrings.RESP_ERR_CREATE_SYNC_SESSION_ERROR.ToArray();
                logger?.LogError("{errorMessage}", Encoding.ASCII.GetString(errorMessage.Span));
                return (false, errorMessage);
            }

#if DEBUG
            await ExceptionInjectionHelper.ResetAndWaitAsync(ExceptionInjectionType.Replication_InProgress_During_DiskBased_Replica_Attach_Sync).ConfigureAwait(false);
#endif

            var status = await replicationSyncManager.ReplicationSyncDriverAsync(replicaSyncSession).ConfigureAwait(false);
            if (status.syncStatus == SyncStatus.FAILED)
            {
                errorMessage = Encoding.ASCII.GetBytes(status.error);
                return (false, errorMessage);
            }

            errorMessage = CmdStrings.RESP_OK.ToArray();
            return (true, errorMessage);
        }

        /// <summary>
        /// Legacy per-replica disk-based sync path.
        /// </summary>
        Task<(bool Success, ReadOnlyMemory<byte> ErrorMessage)> TryBeginDiskbasedLegacySyncAsync(
            string replicaNodeId,
            string replicaAssignedPrimaryId,
            CheckpointEntry replicaCheckpointEntry,
            AofAddress replicaAofBeginAddress,
            AofAddress replicaAofTailAddress)
        {
            ReadOnlyMemory<byte> errorMessage = default;

            if (!replicaSyncSessionTaskStore.TryAddReplicaSyncSession(replicaNodeId, replicaAssignedPrimaryId, replicaCheckpointEntry, replicaAofBeginAddress, replicaAofTailAddress))
            {
                errorMessage = CmdStrings.RESP_ERR_CREATE_SYNC_SESSION_ERROR.ToArray();
                logger?.LogError("{errorMessage}", Encoding.ASCII.GetString(errorMessage.Span));
                return Task.FromResult((false, errorMessage));
            }

            return ReplicaSyncSessionBackgroundTaskAsync(replicaNodeId);

            async Task<(bool Success, ReadOnlyMemory<byte> ErrorMessage)> ReplicaSyncSessionBackgroundTaskAsync(string replicaId)
            {
                try
                {
                    ReadOnlyMemory<byte> errorMessage = default;

                    if (!replicaSyncSessionTaskStore.TryGetSession(replicaId, out var session))
                    {
                        errorMessage = CmdStrings.RESP_ERR_RETRIEVE_SYNC_SESSION_ERROR.ToArray();
                        logger?.LogError("{errorMessage}", Encoding.ASCII.GetString(errorMessage.Span));
                        return (false, errorMessage);
                    }

#if DEBUG
                    await ExceptionInjectionHelper.ResetAndWaitAsync(ExceptionInjectionType.Replication_InProgress_During_DiskBased_Replica_Attach_Sync).ConfigureAwait(false);
#endif

                    if (!await session.SendCheckpointAsync().ConfigureAwait(false))
                    {
                        errorMessage = Encoding.ASCII.GetBytes(session.errorMsg);
                        return (false, errorMessage);
                    }

                    errorMessage = CmdStrings.RESP_OK.ToArray();
                    return (true, errorMessage);
                }
                finally
                {
                    if (!replicaSyncSessionTaskStore.TryRemove(replicaId))
                        logger?.LogError("Unable to remove replica sync session for remote node {replicaId}", replicaId);
                }
            }
        }
    }
}