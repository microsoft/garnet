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
        readonly LightEpoch garnetClientEpoch;
        GarnetServerNode[] connections;
        Dictionary<string, int> connectionMap;
        int numConnection;
        bool _disposed;

        SingleWriterMultiReaderLock _lock;

        public int Count => numConnection;

        public LightEpoch GarnetClientEpoch => garnetClientEpoch;

        /// <summary>
        /// Connection store for cluster gossip connections.
        /// </summary>
        /// <param name="initialSize">Size for array of connection (auto-grows as connections are added).</param>
        /// <param name="logger">Logger instance</param>
        public GarnetClusterConnectionStore(int initialSize = 1, ILogger logger = null)
        {
            this.logger = logger;
            this.garnetClientEpoch = new LightEpoch();
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
        public async Task<bool> AddConnection(GarnetServerNode conn)
        {
            try
            {
                await AcquireWriteLockAsync();

                if (_disposed) return false;

                // Check if connection exists and fail if it does
                if (UnsafeGetConnection(conn.NodeId, out _)) return false;

                // Add new connection
                return UnsafeAddConnection(conn);
            }
            finally
            {
                _lock.WriteUnlock();
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
        public async Task<(bool, GarnetServerNode)> GetOrAdd(ClusterProvider clusterProvider, IPEndPoint endpoint, IGarnetTlsOptions tlsOptions, string nodeId, ILogger logger = null)
        {
            GarnetServerNode conn = null;
            try
            {
                await AcquireWriteLockAsync();

                // Fail on disposed
                if (_disposed) return (false, conn);

                // Check if connection exists and return it the connection if it does
                // Return false because the connection was not added
                if (UnsafeGetConnection(nodeId, out conn)) return (false, conn);

                // Create connection to be added
                conn = new GarnetServerNode(clusterProvider, endpoint, tlsOptions?.TlsClientOptions, garnetClientEpoch, logger: logger)
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
        public async Task<bool> TryRemoveConnection(string nodeId)
        {
            try
            {
                await AcquireWriteLockAsync();

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