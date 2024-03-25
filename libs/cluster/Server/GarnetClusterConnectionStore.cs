// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Security.Cryptography;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal class GarnetClusterConnectionStore
    {
        readonly ILogger logger;
        GarnetServerNode[] connections;
        int numConnection;
        bool _disposed;

        SingleWriterMultiReaderLock _lock;

        public int Count => numConnection;

        /// <summary>
        /// Connection store for cluster gossip connections.
        /// </summary>
        /// <param name="initialSize">Size for array of connection (auto-grows as connections are added).</param>
        /// <param name="logger">Logger instance</param>
        public GarnetClusterConnectionStore(int initialSize = 1, ILogger logger = null)
        {
            this.logger = logger;
            connections = new GarnetServerNode[initialSize];
            numConnection = 0;
        }

        /// <summary>
        /// Dispose cluster connection store.
        /// </summary>
        public void Dispose()
        {
            try
            {
                _lock.WriteLock();
                _disposed = true;
                for (int i = 0; i < numConnection; i++)
                    connections[i].Dispose();
                numConnection = 0;
                Array.Clear(connections);
            }
            finally
            {
                _lock.WriteUnlock();
            }
        }

        /// <summary>
        /// Add new GarnetServerNode to the connection store.
        /// </summary>
        /// <param name="conn">Connection object to add.</param>
        /// <returns>True on success, false otherwise</returns>
        public bool AddConnection(GarnetServerNode conn)
        {
            try
            {
                _lock.WriteLock();

                if (_disposed) return false;

                // Iterate array of existing connections
                for (int i = 0; i < numConnection; i++)
                {
                    var _conn = connections[i];
                    if (_conn.nodeid == conn.nodeid)
                    {
                        return false;
                    }
                }

                if (numConnection == connections.Length)
                {
                    var oldArray = connections;
                    var newArray = new GarnetServerNode[connections.Length * 2];
                    Array.Copy(oldArray, newArray, oldArray.Length);
                    Array.Clear(oldArray);
                    connections = newArray;
                }
                connections[numConnection++] = conn;
            }
            finally
            {
                _lock.WriteUnlock();
            }

            return true;
        }

        /// <summary>
        /// Remove GarnetServerNode connection object from store.
        /// </summary>
        /// <param name="nodeId">Node-id to search for.</param>
        /// <returns>True on successful removal of connection otherwise false.</returns>
        public bool TryRemove(string nodeId)
        {
            try
            {
                _lock.WriteLock();

                if (_disposed) return false;
                for (int i = 0; i < numConnection; i++)
                {
                    var _conn = connections[i];
                    if (nodeId == _conn.nodeid)
                    {
                        connections[i] = null;
                        if (i < numConnection - 1)
                        {
                            connections[i] = connections[numConnection - 1];
                            connections[numConnection - 1] = null;
                        }
                        numConnection--;
                        _conn.Dispose();
                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "GarnetConnectionStore.TryRemove");
            }
            finally
            {
                _lock.WriteUnlock();
            }

            return false;
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
            for (int i = 0; i < numConnection; i++)
            {
                var _conn = connections[i];
                if (_conn.nodeid == nodeId)
                {
                    conn = _conn;
                    return true;
                }
            }
            return false;
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
                if (UnsafeGetConnection(nodeId, out conn))
                    return true;
            }
            finally
            {
                _lock.ReadUnlock();
            }
            return false;
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
        /// <param name="linkStatus">Metrics info to retrieve for connection.</param>
        /// <returns></returns>
        public bool GetConnectionInfo(string nodeId, ref MetricsItem[] linkStatus)
        {
            try
            {
                _lock.ReadLock();
                if (UnsafeGetConnection(nodeId, out var conn))
                {
                    var nowTicks = DateTimeOffset.UtcNow.Ticks;
                    var last_io_seconds = conn.GossipRecv == -1 ? -1 : nowTicks - conn.GossipSend;
                    last_io_seconds = last_io_seconds < 0 ? 0 : TimeSpan.FromTicks(last_io_seconds).Seconds;
                    var connection_status = conn.IsConnected ? "up" : "down";
                    linkStatus[0] = new("master_link_status", connection_status);
                    linkStatus[1] = new("master_last_io_seconds_ago", last_io_seconds.ToString());
                    return true;
                }

            }
            finally
            {
                _lock.ReadUnlock();
            }

            return false;
        }
    }
}