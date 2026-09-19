// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    /// <summary>
    /// Per-node role state for a Garnet instance running under the standalone Sentinel
    /// control plane (<see cref="GarnetServerOptions.EnableStandaloneReplication"/>).
    ///
    /// <para>This is a deliberately tiny class: it tracks only what INFO replication,
    /// ROLE, and the standalone <c>REPLICAOF</c> command need to surface to Sentinel
    /// and to clients. The actual replication stream is not driven from here; the
    /// primary side discovers attached replicas through <see cref="ReplicaRegistry"/>
    /// and the replica side opens a <c>GarnetClientSession</c> to its primary on
    /// receipt of <c>REPLICAOF host port</c>.</para>
    ///
    /// <para>Lifecycle:</para>
    /// <list type="bullet">
    ///   <item>Initial state: role is <c>"master"</c>, no primary
    ///         endpoint, masterLinkStatus is <c>"up"</c>.</item>
    ///   <item><c>REPLICAOF host port</c> sets role to <c>"slave"</c>,
    ///         records the primary endpoint, and starts the outbound handshake in a
    ///         background task. The handshake updates masterLinkStatus
    ///         as it progresses.</item>
    ///   <item><c>REPLICAOF NO ONE</c> clears the primary endpoint and returns
    ///         role to <c>"master"</c>.</item>
    /// </list>
    /// </summary>
    internal sealed class LocalReplicationState
    {
        readonly object sync = new();

        /// <summary>"master" or "slave", as reported in INFO replication and ROLE.</summary>
        string role;

        /// <summary>Primary host, when this node is a replica. Null when this node is a primary.</summary>
        string primaryHost;

        /// <summary>Primary port, when this node is a replica. 0 when this node is a primary.</summary>
        int primaryPort;

        /// <summary>"up" or "down", reported as master_link_status on the replica.</summary>
        string masterLinkStatus;

        /// <summary>
        /// True once the replica has completed PSYNC (sent + received +FULLRESYNC) and
        /// is in the steady-state where it would receive REPLCONF ACKs from the
        /// primary's perspective.
        /// </summary>
        bool syncCompleted;

        /// <summary>Wall-clock UTC time of the last successful contact with the primary.</summary>
        DateTime lastInteractionUtc;

        public LocalReplicationState()
        {
            role = "master";
            primaryHost = null;
            primaryPort = 0;
            masterLinkStatus = "up";
            syncCompleted = false;
            lastInteractionUtc = DateTime.UtcNow;
        }

        /// <summary>Snapshot the current role state for INFO replication and ROLE.</summary>
        public LocalReplicationSnapshot Snapshot()
        {
            lock (sync)
            {
                return new LocalReplicationSnapshot(
                    role,
                    primaryHost,
                    primaryPort,
                    masterLinkStatus,
                    syncCompleted,
                    lastInteractionUtc);
            }
        }

        /// <summary>
        /// Transitions this node to "slave" of <paramref name="host"/>:<paramref name="port"/>.
        /// Called from the standalone <c>REPLICAOF host port</c> handler before the
        /// outbound handshake begins. The handshake will subsequently update the link
        /// status and <see cref="MarkSyncCompleted"/> via <see cref="ReportLinkUp"/>.
        /// </summary>
        public void BecomeReplica(string host, int port)
        {
            lock (sync)
            {
                role = "slave";
                primaryHost = host;
                primaryPort = port;
                masterLinkStatus = "down";   // Optimistic: assume down until handshake proves otherwise.
                syncCompleted = false;
                lastInteractionUtc = DateTime.UtcNow;
            }
        }

        /// <summary>
        /// Transitions this node back to "master" and clears the primary endpoint.
        /// Called from <c>REPLICAOF NO ONE</c> on a standalone replica.
        /// </summary>
        public void BecomePrimary()
        {
            lock (sync)
            {
                role = "master";
                primaryHost = null;
                primaryPort = 0;
                masterLinkStatus = "up";
                syncCompleted = false;
                lastInteractionUtc = DateTime.UtcNow;
            }
        }

        /// <summary>
        /// Records a successful contact with the primary: link is up, last-interaction
        /// time refreshed. Called on every received reply from the primary's outbound
        /// session.
        /// </summary>
        public void ReportLinkUp()
        {
            lock (sync)
            {
                masterLinkStatus = "up";
                lastInteractionUtc = DateTime.UtcNow;
            }
        }

        /// <summary>
        /// Records a contact failure with the primary: link is down, last-interaction
        /// time preserved so master_last_io_seconds_ago continues to climb.
        /// </summary>
        public void ReportLinkDown()
        {
            lock (sync)
            {
                masterLinkStatus = "down";
            }
        }

        /// <summary>
        /// Marks the replica as having completed the PSYNC handshake. Required for
        /// <c>INFO replication</c> on the replica to report <c>master_sync_in_progress:0</c>
        /// and to advertise a stable <c>master_link_status:up</c>.
        /// </summary>
        public void MarkSyncCompleted()
        {
            lock (sync)
            {
                syncCompleted = true;
                lastInteractionUtc = DateTime.UtcNow;
            }
        }
    }

    /// <summary>
    /// Immutable snapshot of <see cref="LocalReplicationState"/>, taken under the
    /// state's lock. Safe to read without re-locking.
    /// </summary>
    internal readonly record struct LocalReplicationSnapshot(
        string Role,
        string PrimaryHost,
        int PrimaryPort,
        string MasterLinkStatus,
        bool SyncCompleted,
        DateTime LastInteractionUtc);
}