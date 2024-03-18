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
        /// <param name="remoteNodeId"></param>
        /// <param name="remote_primary_replid"></param>
        /// <param name="remoteEntry"></param>
        /// <param name="replicaAofBeginAddress"></param>
        /// <param name="replicaAofTailAddress"></param>
        /// <returns></returns>
        public ReadOnlySpan<byte> BeginReplicaSyncSession(string remoteNodeId, string remote_primary_replid, CheckpointEntry remoteEntry, long replicaAofBeginAddress, long replicaAofTailAddress)
        {
            if (!replicaSyncSessionTaskStore.TryAddReplicaSyncSession(remoteNodeId, remote_primary_replid, remoteEntry, replicaAofBeginAddress, replicaAofTailAddress))
            {
                var message = "-PRIMARY-ERR: failed creating replica sync session task.\r\n";
                logger?.LogError(message);
                return Encoding.ASCII.GetBytes(message);
            }

            var errorMsg = ReplicaSyncSessionBackgroundTask(remoteNodeId).GetAwaiter().GetResult();
            return errorMsg.Length > 0 ? Encoding.ASCII.GetBytes(errorMsg) : CmdStrings.RESP_OK;
        }

        private async Task<string> ReplicaSyncSessionBackgroundTask(string replicaId)
        {
            try
            {
                if (!replicaSyncSessionTaskStore.TryGetSession(replicaId, out var session))
                {
                    var msg = "-PRIMARY-ERR Failed retrieving replica sync session.\r\n";
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