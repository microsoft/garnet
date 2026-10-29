// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Net;

namespace Garnet.server
{
    /// <summary>
    /// Tracks stock RESP replicas that have attached to this node as primary, outside
    /// of cluster mode.
    ///
    /// <para>This exists to satisfy a specific requirement of Redis Sentinel: an
    /// orchestrator discovers a primary's replicas <em>only</em> by reading the
    /// primary's <c>INFO replication</c> output and parsing the
    /// <c>slave&lt;N&gt;:ip=...,port=...,state=...,offset=...,lag=...</c> lines.
    /// A controlled comparison against Redis 7.4.11 confirms this: a real primary
    /// reports <c>connected_slaves:1</c> plus a <c>slave0</c> line and Sentinel sees
    /// <c>num-slaves=1</c>, whereas a Garnet primary hard-coded to report
    /// <c>connected_slaves:0</c> leaves Sentinel seeing <c>num-slaves=0</c> even while
    /// a replica is successfully attached.</para>
    ///
    /// <para>With no visible replicas, Sentinel has no candidate to promote, so
    /// failover can never proceed. Recording handshakes here is therefore the
    /// prerequisite for any Sentinel-driven failover.</para>
    ///
    /// <para>Scope: this class only <em>observes</em> and reports. It does not drive a
    /// replication stream, and offsets reported here are the last offset a replica
    /// acknowledged via <c>REPLCONF ACK</c> (0 until the replica starts acking).</para>
    /// </summary>
    internal sealed class ReplicaRegistry
    {
        /// <summary>
        /// A single attached replica.
        ///
        /// <para>Mutable and updated in place as further REPLCONF messages arrive, so
        /// the fields are deliberately not readonly.</para>
        /// </summary>
        internal sealed class ReplicaEntry
        {
            /// <summary>Replica's advertised IP, from REPLCONF ip-address.</summary>
            public string IpAddress;

            /// <summary>
            /// Replica's listening port, from REPLCONF listening-port. This is the port
            /// Sentinel will later connect to, so it must be the replica's own port and
            /// not the ephemeral port of the replication socket.
            /// </summary>
            public int ListeningPort;

            /// <summary>
            /// Ephemeral source port of the replication connection. Used as the identity
            /// key, because REPLCONF ip-address is optional and may be absent.
            /// </summary>
            public readonly int SourcePort;

            /// <summary>
            /// Last offset acknowledged by the replica via REPLCONF ACK. Redis reports 0
            /// for a replica that has not yet acked.
            /// </summary>
            public long AckOffset;

            /// <summary>
            /// Wall-clock time of the last message received from this replica, used to
            /// derive the "lag" field in seconds.
            /// </summary>
            public DateTime LastInteractionUtc;

            /// <summary>
            /// True once the replica has completed a PSYNC handshake.
            /// </summary>
            public bool SyncCompleted;

            public ReplicaEntry(int sourcePort)
            {
                SourcePort = sourcePort;
                IpAddress = null;
                ListeningPort = 0;
                AckOffset = 0;
                LastInteractionUtc = DateTime.UtcNow;
                SyncCompleted = false;
            }

            /// <summary>
            /// Renders this replica as the value of a <c>slave&lt;N&gt;</c> line in
            /// <c>INFO replication</c>, matching the format Redis emits and Sentinel
            /// parses, e.g.
            /// <c>ip=127.0.0.1,port=7931,state=online,offset=459,lag=1</c>.
            /// </summary>
            public string ToInfoString(long nowUnixSeconds)
            {
                var lag = nowUnixSeconds - new DateTimeOffset(LastInteractionUtc).ToUnixTimeSeconds();
                if (lag < 0) lag = 0;

                // A replica that has not yet finished PSYNC is reported the way Redis
                // reports one mid-handshake.
                var state = SyncCompleted ? "online" : "sync";

                // Redis uses the connection's peer address when the replica has not
                // announced one via REPLCONF ip-address. GetOrAdd populates this
                // field with the peer address on first REPLCONF, so the only time
                // it is null is a test-only entry that bypassed the registry.
                var ip = string.IsNullOrEmpty(IpAddress) ? "127.0.0.1" : IpAddress;

                return $"ip={ip},port={ListeningPort},state={state},offset={AckOffset},lag={lag}";
            }
        }

        readonly object sync = new();
        readonly Dictionary<int, ReplicaEntry> replicas = [];

        /// <summary>Number of replicas currently attached.</summary>
        public int Count
        {
            get { lock (sync) return replicas.Count; }
        }

        /// <summary>
        /// Records that a connection identified by <paramref name="sourcePort"/> has
        /// spoken REPLCONF. Creates the entry on first contact and refreshes
        /// <see cref="ReplicaEntry.LastInteractionUtc"/> thereafter.
        /// </summary>
        public ReplicaEntry GetOrAdd(int sourcePort, string remoteAddress)
        {
            lock (sync)
            {
                if (!replicas.TryGetValue(sourcePort, out var entry))
                {
                    entry = new ReplicaEntry(sourcePort)
                    {
                        IpAddress = NormalizeAddress(remoteAddress)
                    };
                    replicas[sourcePort] = entry;
                }

                entry.LastInteractionUtc = DateTime.UtcNow;
                return entry;
            }
        }

        /// <summary>Records the replica's advertised listening port.</summary>
        public void SetListeningPort(int sourcePort, int listeningPort)
        {
            lock (sync)
            {
                if (replicas.TryGetValue(sourcePort, out var entry))
                {
                    entry.ListeningPort = listeningPort;
                    entry.LastInteractionUtc = DateTime.UtcNow;
                }
            }
        }

        /// <summary>Records the replica's advertised IP address.</summary>
        public void SetIpAddress(int sourcePort, string ipAddress)
        {
            lock (sync)
            {
                if (replicas.TryGetValue(sourcePort, out var entry))
                {
                    entry.IpAddress = NormalizeAddress(ipAddress);
                    entry.LastInteractionUtc = DateTime.UtcNow;
                }
            }
        }

        /// <summary>Records the offset acknowledged by the replica.</summary>
        public void SetAckOffset(int sourcePort, long offset)
        {
            lock (sync)
            {
                if (replicas.TryGetValue(sourcePort, out var entry))
                {
                    entry.AckOffset = offset;
                    entry.LastInteractionUtc = DateTime.UtcNow;
                }
            }
        }

        /// <summary>
        /// Marks the replica as having completed its PSYNC handshake, which moves it
        /// from the "sync" state to "online" in INFO output.
        /// </summary>
        public void MarkSyncCompleted(int sourcePort)
        {
            lock (sync)
            {
                if (replicas.TryGetValue(sourcePort, out var entry))
                {
                    entry.SyncCompleted = true;
                    entry.LastInteractionUtc = DateTime.UtcNow;
                }
            }
        }

        /// <summary>
        /// Removes a replica, e.g. when its connection drops.
        /// </summary>
        public void Remove(int sourcePort)
        {
            lock (sync)
                replicas.Remove(sourcePort);
        }

        /// <summary>
        /// Snapshot of the current replicas, ordered by source port for stable output.
        /// Returns <c>slave0</c>, <c>slave1</c>, ... value strings ready to be emitted
        /// in <c>INFO replication</c>.
        /// </summary>
        public List<string> GetSlaveInfoStrings()
        {
            var nowUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            lock (sync)
            {
                var result = new List<string>(replicas.Count);
                var keys = new List<int>(replicas.Keys);
                keys.Sort();
                foreach (var key in keys)
                    result.Add(replicas[key].ToInfoString(nowUnix));

                return result;
            }
        }

        /// <summary>
        /// Normalizes an address for the INFO line. IPv6 loopback and IPv4-mapped
        /// forms are reduced to the dotted-quad that Sentinel expects.
        /// </summary>
        static string NormalizeAddress(string remoteAddress)
        {
            if (string.IsNullOrEmpty(remoteAddress))
                return null;

            if (IPAddress.TryParse(remoteAddress, out var parsed))
            {
                if (parsed.IsIPv4MappedToIPv6)
                    parsed = parsed.MapToIPv4();

                return parsed.ToString();
            }

            return remoteAddress;
        }
    }
}
