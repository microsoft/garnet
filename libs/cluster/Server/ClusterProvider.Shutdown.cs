// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;

namespace Garnet.cluster
{
    public sealed partial class ClusterProvider
    {
        /// <inheritdoc />
        public void WaitForReplicaSync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            if (!IsPrimary()) return;
            var endpoints = clusterManager.CurrentConfig.GetLocalNodeReplicaEndpoints();
            if (endpoints.Count == 0) return;

            using var timeoutCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCancellation.CancelAfter(timeout);
            var token = timeoutCancellation.Token;
            var offset = replicationManager.ReplicationOffset;
            var clients = new List<GarnetClient>(endpoints.Count);
            var tasks = new List<Task>(endpoints.Count);
            try
            {
                foreach (var endpoint in endpoints)
                {
                    var client = new GarnetClient(endpoint, serverOptions.TlsOptions?.TlsClientOptions,
                        authUsername: ClusterUsername, authPassword: ClusterPassword);
                    clients.Add(client);
                    tasks.Add(client.ConnectAsync(token).ContinueWith(connected =>
                    {
                        AsyncUtils.BlockingWait(connected);
                        return client.ExecuteClusterFailReplicationOffsetAsync(offset, token);
                    }, token, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default).Unwrap());
                }
                AsyncUtils.BlockingWait(Task.WhenAll(tasks).WaitAsync(token));
            }
            catch (Exception) when (!cancellationToken.IsCancellationRequested)
            {
                // Replica synchronization is best effort and cannot prevent a bounded shutdown.
            }
            finally
            {
                foreach (var client in clients) client.Dispose();
            }
            cancellationToken.ThrowIfCancellationRequested();
        }
    }
}