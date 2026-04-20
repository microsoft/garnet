// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
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
        /// <param name="errorMessage"></param>
        /// <returns></returns>
        public bool TryBeginDisklessSync(SyncMetadata replicaSyncMetadata, out ReadOnlySpan<byte> errorMessage)
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

#if DEBUG
                ExceptionInjectionHelper.ResetAndWaitAsync(ExceptionInjectionType.Replication_InProgress_During_Diskless_Replica_Attach_Sync).GetAwaiter().GetResult();
#endif

                var status = replicationSyncManager.ReplicationSyncDriver(replicaSyncSession).GetAwaiter().GetResult();
                if (status.syncStatus == SyncStatus.FAILED)
                    errorMessage = Encoding.ASCII.GetBytes(status.error);
            }

            return true;
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
        public bool TryBeginDiskbasedSync(
            string replicaNodeId,
            string replicaAssignedPrimaryId,
            CheckpointEntry replicaCheckpointEntry,
            AofAddress replicaAofBeginAddress,
            AofAddress replicaAofTailAddress,
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

#if DEBUG
                    ExceptionInjectionHelper.ResetAndWaitAsync(ExceptionInjectionType.Replication_InProgress_During_DiskBased_Replica_Attach_Sync).GetAwaiter().GetResult();
#endif

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
                    if (!replicaSyncSessionTaskStore.TryRemove(replicaId))
                        logger?.LogError("Unable to remove replica sync session for remote node {replicaId}", replicaId);
                }
            }
        }
    }
}