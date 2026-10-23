// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Net;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server.TLS;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal class GarnetClusterConnectionStore
    {
        readonly ILogger logger;
        readonly LightEpoch epoch;
        GarnetServerNode[] connections;
        Dictionary<string, int> connectionMap;
        int numConnection;
        bool _disposed;

        SingleWriterMultiReaderLock _lock;

        public int Count => numConnection;

        public LightEpoch Epoch => epoch;

        /// <summary>
        /// Aggregates current and configured-maximum accounted memory across all gossip clients.
        /// </summary>
        /// <returns>Active and maximum accounted memory in bytes.</returns>
        public (long ActiveMemoryUsageBytes, long MaxMemoryUsageBytes) GetMemoryUsage()
        {
            long activeMemoryUsageBytes = 0;
            long maxMemoryUsageBytes = 0;
            try
            {
                _lock.ReadLock();
                if (_disposed)
                    return default;

                for (var i = 0; i < numConnection; i++)
                {
                    var (clientActiveMemoryUsageBytes, clientMaxMemoryUsageBytes) = connections[i].GetClientMemoryUsage();
                    activeMemoryUsageBytes = long.CreateSaturating((Int128)activeMemoryUsageBytes + clientActiveMemoryUsageBytes);
                    maxMemoryUsageBytes = long.CreateSaturating((Int128)maxMemoryUsageBytes + clientMaxMemoryUsageBytes);
                }
            }
            finally
            {
                _lock.ReadUnlock();
            }

            return (activeMemoryUsageBytes, maxMemoryUsageBytes);
        }

        /// <summary>
        /// Connection store for cluster gossip connections.
        /// </summary>
        /// <param name="initialSize">Size for array of connection (auto-grows as connections are added).</param>
        /// <param name="logger">Logger instance</param>
        public GarnetClusterConnectionStore(LightEpoch epoch, int initialSize = 1, ILogger logger = null)
        {
            this.epoch = epoch;
            this.logger = logger;
            connections = new GarnetServerNode[initialSize];
            connectionMap = [];
            numConnection = 0;
        }

        /// <summary>
        /// Dispose connections
        /// </summary>
        private void UnsafeDisposeConnections()
        {
            for (var i = 0; i < numConnection; i++)
                connections[i].Dispose();
            numConnection = 0;
            Array.Clear(connections);
            connectionMap.Clear();
        }

        /// <summary>
        /// Dispose cluster connection store.
        /// </summary>
        public void Dispose()
        {
            try
            {
                _lock.WriteLock();
                UnsafeDisposeConnections();
                _disposed = true;
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }

        /// <summary>
        /// Close all connections
        /// </summary>
        public void CloseAll()
        {
            try
            {
                _lock.WriteLock();
                if (_disposed) return;
                UnsafeDisposeConnections();
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "GarnetConnectionStore.CloseAll");
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }

        /// <summary>
        /// Linear search of the array of connections without read-lock protection.
        /// Caller responsible for locking.
        /// </summary>
        /// <param name="nodeId">Node-id to search for.</param>
        /// <param name="conn">Connection object returned on success otherwise null.</param>
        /// <returns>True if connection is found otherwise false.</returns>
        private bool UnsafeGetConnection(string nodeId, out GarnetServerNode conn)
        {
            conn = null;
            if (_disposed) return false;

            // Find offset in connection array
            if (!connectionMap.TryGetValue(nodeId, out var offset))
                return false;

            // Find connection based on offset
            conn = connections[offset];
            return true;
        }

        /// <summary>
        /// Add new connection by expanding base array if necessary
        /// </summary>
        /// <param name="conn"></param>
        /// <returns></returns>
        private bool UnsafeAddConnection(GarnetServerNode conn)
        {
            if (numConnection == connections.Length)
            {
                var oldArray = connections;
                var newArray = new GarnetServerNode[connections.Length * 2];
                Array.Copy(oldArray, newArray, oldArray.Length);
                Array.Clear(oldArray);
                connections = newArray;
            }

            // Try add connection offset mapping
            if (!connectionMap.TryAdd(conn.NodeId, numConnection)) return false;

            // Try add connection to offset
            connections[numConnection++] = conn;
            return true;
        }

        private async Task AcquireWriteLockAsync()
        {
            while (!_lock.TryWriteLock())
                await Task.Yield();
        }

        /// <summary>
        /// Add new GarnetServerNode to the connection store.
        /// </summary>
        /// <param name="conn">Connection object to add.</param>
        /// <returns>True on success, false otherwise</returns>
        public async Task<bool> AddConnectionAsync(GarnetServerNode conn)
        {
            // Test hook: pause a pending add before the write lock is taken, so a CONFIG SET can republish the
            // GOSSIP_CLIENT snapshot and let its swap run to completion (the swap takes this same lock) while
            // this add is still parked. The swap then never sees this not-yet-added node, so when the add is
            // released the stale-rebuild branch below is the only thing that can give it the new options.
            // No-op outside DEBUG / when not enabled.
            await ExceptionInjectionHelper.ResetAndWaitAsync(ExceptionInjectionType.Cluster_Gossip_Pause_Before_AddConnection).ConfigureAwait(false);

            // A connection built outside the write lock (e.g. during a CLUSTER MEET handshake) can carry a
            // GOSSIP_CLIENT configuration that a swap superseded while it was being established. That node was
            // not in the store when the swap repointed the live set, so it would persist with stale options.
            // Detect it under the lock and insert a replacement built from the current configuration instead.
            // The superseded node has finished its handshake, holds no outstanding requests, and was never
            // advertised, so it is disposed immediately once the lock is released - no graceful drain or
            // background task is needed, matching the synchronous reclamation the MEET caller already performs
            // on its failure paths.
            GarnetServerNode retire = null;
            try
            {
                await AcquireWriteLockAsync().ConfigureAwait(false);

                if (_disposed) return false;

                // Check if connection exists and fail if it does
                if (UnsafeGetConnection(conn.NodeId, out _)) return false;

                if (conn.IsGossipClientConfigStale)
                {
                    retire = conn;
                    conn = conn.CreateReplacement();
                }

                // Add new connection
                if (!UnsafeAddConnection(conn))
                {
                    // Add failed: discard any replacement we built and let the caller dispose its original.
                    retire = retire != null ? conn : null;
                    return false;
                }

                return true;
            }
            finally
            {
                _lock.WriteUnlock();
                retire?.Dispose();
            }
        }

        /// <summary>
        /// Get or add a connection to the store.
        /// </summary>
        /// <param name="clusterProvider"></param>
        /// <param name="endpoint">The cluster node endpoint</param>
        /// <param name="tlsOptions"></param>
        /// <param name="nodeId"></param>
        /// <param name="logger"></param>
        /// <returns>True if connection was added, false otherwise</returns>
        public async ValueTask<(bool, GarnetServerNode)> GetOrAddAsync(ClusterProvider clusterProvider, IPEndPoint endpoint, IGarnetTlsOptions tlsOptions, string nodeId, ILogger logger = null)
        {
            GarnetServerNode conn = null;
            try
            {
                await AcquireWriteLockAsync().ConfigureAwait(false);

                // Fail on disposed
                if (_disposed) return (false, conn);

                // Check if connection exists and return it the connection if it does
                // Return false because the connection was not added
                if (UnsafeGetConnection(nodeId, out conn)) return (false, conn);

                // Create connection to be added. Built under the write lock, so it observes the current
                // GOSSIP_CLIENT configuration and can never be stale (no version check needed here).
                conn = new GarnetServerNode(clusterProvider, endpoint, tlsOptions?.TlsClientOptions, epoch, logger: logger)
                {
                    NodeId = nodeId
                };

                // Add new connection
                return (UnsafeAddConnection(conn), conn);
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }

        /// <summary>
        /// Remove GarnetServerNode connection object from store.
        /// </summary>
        /// <param name="nodeId">Node-id to search for.</param>
        /// <returns>True on successful removal of connection otherwise false.</returns>
        public async Task<bool> TryRemoveConnectionAsync(string nodeId)
        {
            try
            {
                await AcquireWriteLockAsync().ConfigureAwait(false);

                // Fail on disposed
                if (_disposed) return false;

                // Search for connection and fail if it does not exist
                if (!UnsafeGetConnection(nodeId, out var conn))
                    return false;

                // Search for offset mapping fail if it does not exist
                if (!connectionMap.TryGetValue(conn.NodeId, out var offset))
                    return false;

                // Remove nodeId -> offset mapping
                if (!connectionMap.Remove(conn.NodeId))
                    return false;

                // Swap tail entry to offset entry's position
                var tail = numConnection - 1;
                connections[offset] = connections[tail];
                connections[numConnection - 1] = null;
                numConnection--;
                // Update nodeId -> offset mapping
                if (offset != tail)
                    connectionMap[connections[offset].NodeId] = offset;

                conn.Dispose();

            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "GarnetConnectionStore.TryRemove");
            }
            finally
            {
                _lock.WriteUnlock();
            }

            return true;
        }

        /// <summary>
        /// Atomically swaps every live gossip connection for a replacement built from the current runtime
        /// configuration, then retires the previous connections in the background. The connectionMap offsets are
        /// preserved, so a selector (<see cref="GetConnection"/>, <see cref="GetConnectionAtOffset"/>,
        /// <see cref="GetRandomConnection"/>) observes the replacement as soon as the swap completes. A caller
        /// that already captured a previous connection keeps using it; its in-flight work drains through the
        /// previous connection's graceful disposal rather than being aborted.
        /// </summary>
        /// <param name="grace">
        /// Delay applied once, after every connection has been repointed, before the previous connections begin
        /// graceful disposal. It widens the window for callers that selected a previous connection just before
        /// the swap to finish issuing their requests against it.
        /// </param>
        public void SwapAllConnections(TimeSpan grace)
        {
            GarnetServerNode[] retired;
            int retiredCount;
            try
            {
                _lock.WriteLock();
                if (_disposed) return;

                retiredCount = numConnection;
                retired = new GarnetServerNode[retiredCount];
                for (var i = 0; i < retiredCount; i++)
                {
                    var previous = connections[i];
                    // connectionMap[nodeId] -> i is unchanged, so selectors immediately observe the replacement.
                    connections[i] = previous.CreateReplacement();
                    retired[i] = previous;
                }
            }
            finally
            {
                _lock.WriteUnlock();
            }

            if (retiredCount == 0)
                return;

            // Apply the grace window once for the whole collection, then retire every previous connection.
            _ = Task.Run(async () =>
            {
                if (grace > TimeSpan.Zero)
                    await Task.Delay(grace).ConfigureAwait(false);

                var retirements = new Task[retiredCount];
                for (var i = 0; i < retiredCount; i++)
                {
                    var previous = retired[i];
                    retirements[i] = Task.Run(async () =>
                    {
                        try
                        {
                            await previous.DisposeGracefullyAsync().ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            logger?.LogWarning(ex, "Error retiring gossip connection during configuration swap");
                        }
                    });
                }

                await Task.WhenAll(retirements).ConfigureAwait(false);
            });
        }

        /// <summary>
        /// Get connection object corresponding to provided node-id.
        /// </summary>
        /// <param name="nodeId">Node-id to search for.</param>
        /// <param name="conn">Connection object returned on success otherwise null.</param>
        /// <returns></returns>
        public bool GetConnection(string nodeId, out GarnetServerNode conn)
        {
            conn = null;
            try
            {
                _lock.ReadLock();
                // Return false if connection was not found
                if (!UnsafeGetConnection(nodeId, out conn))
                    return false;
            }
            finally
            {
                _lock.ReadUnlock();
            }
            return true;
        }

        /// <summary>
        /// Retrieve connection at given offset.
        /// </summary>
        /// <param name="offset">Offset in array of connections.</param>
        /// <param name="conn">Connection object to return on success.</param>
        /// <returns>True if offset within range else false.</returns>
        public bool GetConnectionAtOffset(uint offset, out GarnetServerNode conn)
        {
            conn = null;
            try
            {
                _lock.ReadLock();
                if (_disposed) return false;

                if (offset < numConnection)
                {
                    conn = connections[offset];
                    return true;
                }
            }
            finally
            {
                _lock.ReadUnlock();
            }
            return false;
        }

        /// <summary>
        /// Pick a random connection object to retrieve.
        /// </summary>
        /// <param name="conn">Connection retrieved.</param>
        /// <returns>True on success otherwise false.</returns>
        public bool GetRandomConnection(out GarnetServerNode conn)
        {
            conn = null;
            try
            {
                _lock.ReadLock();
                if (_disposed) return false;
                if (numConnection == 0) return false;

                var offset = RandomNumberGenerator.GetInt32(0, numConnection);
                conn = connections[offset];
                return true;
            }
            finally
            {
                _lock.ReadUnlock();
            }
        }

        /// <summary>
        /// Populate metrics related to link connection status.
        /// </summary>
        /// <param name="nodeId">Node-id to search for.</param>
        /// <param name="info">Connection info corresponding to specified connection.</param>
        /// <returns></returns>
        public bool GetConnectionInfo(string nodeId, out ConnectionInfo info)
        {
            try
            {
                _lock.ReadLock();
                // Return connection information if connection exists
                if (UnsafeGetConnection(nodeId, out var conn))
                {
                    info = conn.GetConnectionInfo();
                    return true;
                }
                info = new ConnectionInfo();
            }
            finally
            {
                _lock.ReadUnlock();
            }

            return false;
        }
    }
}