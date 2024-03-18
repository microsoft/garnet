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
        public string nodeid;

        /// <summary>
        /// IP address
        /// </summary>
        public string address;

        /// <summary>
        /// Port
        /// </summary>
        public int port;

        /// <summary>
        /// Configuration epoch.
        /// </summary>
        public long configEpoch;

        /// <summary>
        /// Current config epoch used for voting.
        /// </summary>
        public long currentConfigEpoch;

        /// <summary>
        /// Last config epoch this worker has voted for.
        /// </summary>
        public long lastVotedConfigEpoch;

        /// <summary>
        /// Role of node (i.e 0: primary 1: replica).
        /// </summary>
        public NodeRole role;

        /// <summary>
        /// Node ID that this node is replicating (i.e. primary id).
        /// </summary>
        public string replicaOfNodeId;

        /// <summary>
        /// Replication offset (readonly value for information only)
        /// </summary>
        public long replicationOffset;

        /// <summary>
        /// Hostname of this instance
        /// </summary>
        public string hostname;

        /// <summary>
        /// ToString
        /// </summary>
        /// <returns></returns>
        public override string ToString() => $"{nodeid} {address} {port} {configEpoch} {role} {replicaOfNodeId}";
    }
}