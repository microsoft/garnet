// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
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
        /// <param name="errorMessage"></param>
        /// <returns></returns>
        public bool TryAttachSync(SyncMetadata replicaSyncMetadata, out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = [];
            if (clusterProvider.serverOptions.ReplicaDisklessSync)
            {
                if (!replicationSyncManager.AddReplicaSyncSession(replicaSyncMetadata, out var replicaSyncSession))
                {
                    replicaSyncSession?.Dispose();
                    errorMessage = CmdStrings.RESP_ERR_CREATE_SYNC_SESSION_ERROR;
                    logger?.LogError("{errorMessage}", Encoding.ASCII.GetString(errorMessage));
                }

                var status = replicationSyncManager.ReplicationSyncDriver(replicaSyncSession).GetAwaiter().GetResult();
                if (status.syncStatus == SyncStatus.FAILED)
                    errorMessage = Encoding.ASCII.GetBytes(status.error);
            }

            return true;
        }

        /// <summary>
        /// Start sync of remote replica from this primary
        /// </summary>
        /// <param name="replicaNodeId"></param>
        /// <param name="replicaAssignedPrimaryId"></param>
        /// <param name="replicaCheckpointEntry"></param>
        /// <param name="replicaAofBeginAddress"></param>
        /// <param name="replicaAofTailAddress"></param>
        /// <param name="errorMessage"></param>
        /// <returns></returns>
        public bool TryBeginPrimarySync(
            string replicaNodeId,
            string replicaAssignedPrimaryId,
            CheckpointEntry replicaCheckpointEntry,
            long replicaAofBeginAddress,
            long replicaAofTailAddress,
            out ReadOnlySpan<byte> errorMessage)
        {
            return TryBeginDiskSync(replicaNodeId, replicaAssignedPrimaryId, replicaCheckpointEntry, replicaAofBeginAddress, replicaAofTailAddress, out errorMessage);
        }

        /// <summary>
        /// Begin background replica sync session
        /// </summary>
        /// <param name="replicaNodeId">Node-id of replica that is currently attaching</param>
        /// <param name="replicaAssignedPrimaryId">Primary-id of replica that is currently attaching</param>
        /// <param name="replicaCheckpointEntry">Most recent checkpoint entry at replica</param>
        /// <param name="replicaAofBeginAddress">AOF begin address at replica</param>
        /// <param name="replicaAofTailAddress">AOF tail address at replica</param>
        /// <param name="errorMessage">The ASCII encoded error message if the method returned <see langword="false"/>; otherwise <see langword="default"/></param>
        /// <returns></returns>
        public bool TryBeginDiskSync(
            string replicaNodeId,
            string replicaAssignedPrimaryId,
            CheckpointEntry replicaCheckpointEntry,
            long replicaAofBeginAddress,
            long replicaAofTailAddress,
            out ReadOnlySpan<byte> errorMessage)
        {
            if (!replicaSyncSessionTaskStore.TryAddReplicaSyncSession(replicaNodeId, replicaAssignedPrimaryId, replicaCheckpointEntry, replicaAofBeginAddress, replicaAofTailAddress))
            {
                errorMessage = CmdStrings.RESP_ERR_CREATE_SYNC_SESSION_ERROR;
                logger?.LogError("{errorMessage}", Encoding.ASCII.GetString(errorMessage));
                return false;
            }

            return ReplicaSyncSessionBackgroundTask(replicaNodeId, out errorMessage);

            bool ReplicaSyncSessionBackgroundTask(string replicaId, out ReadOnlySpan<byte> errorMessage)
            {
                try
                {
                    if (!replicaSyncSessionTaskStore.TryGetSession(replicaId, out var session))
                    {
                        errorMessage = CmdStrings.RESP_ERR_RETRIEVE_SYNC_SESSION_ERROR;
                        logger?.LogError("{errorMessage}", Encoding.ASCII.GetString(errorMessage));
                        return false;
                    }

                    if (!session.SendCheckpoint().GetAwaiter().GetResult())
                    {
                        errorMessage = Encoding.ASCII.GetBytes(session.errorMsg);
                        return false;
                    }

                    errorMessage = CmdStrings.RESP_OK;
                    return true;
                }
                finally
                {
                    replicaSyncSessionTaskStore.TryRemove(replicaId);
                }
            }
        }
    }
}