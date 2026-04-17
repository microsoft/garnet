// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading.Tasks;
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
        public async Task<(bool Success, ReadOnlyMemory<byte> ErrorMessage)> TryAttachSyncAsync(SyncMetadata replicaSyncMetadata)
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

                var status = await replicationSyncManager.ReplicationSyncDriverAsync(replicaSyncSession).ConfigureAwait(false);
                if (status.syncStatus == SyncStatus.FAILED)
                    errorMessage = Encoding.ASCII.GetBytes(status.error);
            }

            return (true, errorMessage);
        }

        /// <summary>
        /// Start sync of remote replica from this primary
        /// </summary>
        /// <param name="replicaNodeId"></param>
        /// <param name="replicaAssignedPrimaryId"></param>
        /// <param name="replicaCheckpointEntry"></param>
        /// <param name="replicaAofBeginAddress"></param>
        /// <param name="replicaAofTailAddress"></param>
        /// <returns></returns>
        public Task<(bool Success, ReadOnlyMemory<byte> ErrorMessage)> TryBeginPrimarySyncAsync(
            string replicaNodeId,
            string replicaAssignedPrimaryId,
            CheckpointEntry replicaCheckpointEntry,
            long replicaAofBeginAddress,
            long replicaAofTailAddress)
        {
            return TryBeginDiskSyncAsync(replicaNodeId, replicaAssignedPrimaryId, replicaCheckpointEntry, replicaAofBeginAddress, replicaAofTailAddress);
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
        public Task<(bool Success, ReadOnlyMemory<byte> ErrorMessage)> TryBeginDiskSyncAsync(
            string replicaNodeId,
            string replicaAssignedPrimaryId,
            CheckpointEntry replicaCheckpointEntry,
            long replicaAofBeginAddress,
            long replicaAofTailAddress)
        {
            if (!replicaSyncSessionTaskStore.TryAddReplicaSyncSession(replicaNodeId, replicaAssignedPrimaryId, replicaCheckpointEntry, replicaAofBeginAddress, replicaAofTailAddress))
            {
                var errorMessage = CmdStrings.RESP_ERR_CREATE_SYNC_SESSION_ERROR.ToArray();
                logger?.LogError("{errorMessage}", Encoding.ASCII.GetString(errorMessage));
                return Task.FromResult((false, (ReadOnlyMemory<byte>)errorMessage.AsMemory()));
            }

            return ReplicaSyncSessionBackgroundTaskAsync(replicaNodeId);

            async Task<(bool Success, ReadOnlyMemory<byte> ErrorMessage)> ReplicaSyncSessionBackgroundTaskAsync(string replicaId)
            {
                try
                {
                    if (!replicaSyncSessionTaskStore.TryGetSession(replicaId, out var session))
                    {
                        var errorMessage = CmdStrings.RESP_ERR_RETRIEVE_SYNC_SESSION_ERROR.ToArray();
                        logger?.LogError("{errorMessage}", Encoding.ASCII.GetString(errorMessage));
                        return (false, (ReadOnlyMemory<byte>)errorMessage.AsMemory());
                    }

                    if (!await session.SendCheckpoint().ConfigureAwait(false))
                    {
                        var errorMessage = Encoding.ASCII.GetBytes(session.errorMsg);
                        return (false, (ReadOnlyMemory<byte>)errorMessage.AsMemory());
                    }

                    return (true, default);
                }
                finally
                {
                    _ = replicaSyncSessionTaskStore.TryRemove(replicaId);
                }
            }
        }
    }
}