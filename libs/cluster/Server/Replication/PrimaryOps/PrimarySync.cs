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
        /// Try attach sync session from replica
        /// </summary>
        /// <param name="replicaSyncMetadata"></param>
        /// <returns></returns>
        public async Task<(bool Success, ReadOnlyMemory<byte> ErrorMessage)> TryBeginDisklessSyncAsync(SyncMetadata replicaSyncMetadata)
        {
            ReadOnlyMemory<byte> errorMessage = default;
            if (clusterProvider.serverOptions.ReplicaDisklessSync)
            {
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
            }

            return (true, errorMessage);
        }

        /// <summary>
        /// Begin background replica sync session
        /// </summary>
        /// <param name="replicaNodeId">Node-id of replica that is currently attaching</param>
        /// <param name="replicaAssignedPrimaryId">Primary-id of replica that is currently attaching</param>
        /// <param name="replicaCheckpointEntry">Most recent checkpoint entry at replica</param>
        /// <param name="replicaAofBeginAddress">AOF begin address at replica</param>
        /// <param name="replicaAofTailAddress">AOF tail address at replica</param>
        /// <returns></returns>
        public Task<(bool Success, ReadOnlyMemory<byte> ErrorMessage)> TryBeginDiskbasedSyncAsync(
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