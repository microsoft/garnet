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
        /// Begin background replica sync session
        /// </summary>
        /// <param name="errorMessage">The ASCII encoded error message if the method returned <see langword="false"/>; otherwise <see langword="default"/></param>
        public bool TryBeginReplicaSyncSession(string remoteNodeId, string remote_primary_replid, CheckpointEntry remoteEntry, long replicaAofBeginAddress, long replicaAofTailAddress, out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = default;
            if (!replicaSyncSessionTaskStore.TryAddReplicaSyncSession(remoteNodeId, remote_primary_replid, remoteEntry, replicaAofBeginAddress, replicaAofTailAddress))
            {
                errorMessage = CmdStrings.RESP_ERR_CREATE_SYNC_SESSION_ERROR;
                logger?.LogError("{errorMessage}", Encoding.ASCII.GetString(errorMessage));
                return false;
            }

            if (!ReplicaSyncSessionBackgroundTask(remoteNodeId, out errorMessage))
            {
                return false;
            }
            return true;
        }

        private bool ReplicaSyncSessionBackgroundTask(string replicaId, out ReadOnlySpan<byte> errorMessage)
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

        public void DisposeReplicaSyncSessionTasks()
            => replicaSyncSessionTaskStore.Dispose();
    }
}