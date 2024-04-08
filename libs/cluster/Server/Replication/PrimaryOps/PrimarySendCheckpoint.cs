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
        /// Begin background replica sync session
        /// </summary>
        /// <param name="errorMessage">The ASCII encoded error message if the method returned <see langword="false"/>; otherwise <see langword="default"/></param>
        public bool TryBeginReplicaSyncSession(string remoteNodeId, string remote_primary_replid, CheckpointEntry remoteEntry, long replicaAofBeginAddress, long replicaAofTailAddress, out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = default;
            if (!replicaSyncSessionTaskStore.TryAddReplicaSyncSession(remoteNodeId, remote_primary_replid, remoteEntry, replicaAofBeginAddress, replicaAofTailAddress))
            {
                errorMessage = "PRIMARY-ERR failed creating replica sync session task."u8;
                logger?.LogError(Encoding.ASCII.GetString(errorMessage));
                return false;
            }

            var errorMsg = ReplicaSyncSessionBackgroundTask(remoteNodeId).GetAwaiter().GetResult();
            if (errorMsg != null)
            {
                errorMessage = Encoding.UTF8.GetBytes(errorMsg);
                return false;
            }
            return true;
        }

        private async Task<string> ReplicaSyncSessionBackgroundTask(string replicaId)
        {
            try
            {
                if (!replicaSyncSessionTaskStore.TryGetSession(replicaId, out var session))
                {
                    var msg = "PRIMARY-ERR Failed retrieving replica sync session.";
                    logger?.LogError(msg);
                    return msg;
                }
                return await session.SendCheckpoint();
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