// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.cluster
{
    /// <summary>
    /// NodeRole identifier
    /// </summary>
    public enum NodeRole : byte
    {
        /// <summary>
        /// PRIMARY Role identifier
        /// </summary>
        PRIMARY = 0x0,
        /// <summary>
        /// REPLICA Role identifier
        /// </summary>
        REPLICA,
        /// <summary>
        /// UNASSIGNED Role identifier
        /// </summary>
        UNASSIGNED,
    }

    /// <summary>
    /// Cluster worker definition
    /// </summary>
    public struct Worker
    {
        /// <summary>
        /// Unique node ID
        /// </summary>
        public string Nodeid;

        /// <summary>
        /// IP address
        /// </summary>
        public string Address;

        /// <summary>
        /// Port
        /// </summary>
        public int Port;

        /// <summary>
        /// Configuration epoch.
        /// </summary>
        public long ConfigEpoch;

        /// <summary>
        /// Role of node (i.e 0: primary 1: replica).
        /// </summary>
        public NodeRole Role;

        /// <summary>
        /// Node ID that this node is replicating (i.e. primary id).
        /// </summary>
        public string ReplicaOfNodeId;

        /// <summary>
        /// Replication offset (readonly value for information only)
        /// </summary>
        public long ReplicationOffset;

        /// <summary>
        /// Hostname of this instance
        /// </summary>
        public string hostname;

        /// <summary>
        /// ToString
        /// </summary>
        /// <returns></returns>
        public override string ToString() => $"{Nodeid} {Address} {Port} {ConfigEpoch} {Role} {ReplicaOfNodeId}";
    }
}