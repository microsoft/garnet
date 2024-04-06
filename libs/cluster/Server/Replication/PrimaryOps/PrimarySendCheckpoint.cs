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
                logger?.LogError("{errorMsg}", Encoding.ASCII.GetString(CmdStrings.RESP_CREATE_SYNC_SESSION_ERROR));
                return CmdStrings.RESP_CREATE_SYNC_SESSION_ERROR;
            }

            return ReplicaSyncSessionBackgroundTask(remoteNodeId);
        }

        private ReadOnlySpan<byte> ReplicaSyncSessionBackgroundTask(string replicaId)
        {
            try
            {
                if (!replicaSyncSessionTaskStore.TryGetSession(replicaId, out var session))
                {
                    logger?.LogError("{errorMsg}", Encoding.ASCII.GetString(CmdStrings.RESP_RETRIEVE_SYNC_SESSION_ERROR));
                    return CmdStrings.RESP_RETRIEVE_SYNC_SESSION_ERROR;
                }

                var resp = session.SendCheckpoint().GetAwaiter().GetResult();
                return Encoding.ASCII.GetBytes(resp);
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