// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// The outbound replication link from a standalone Garnet replica to its primary.
    ///
    /// <para>One instance lives in <see cref="StoreWrapper"/> per node that has executed
    /// <c>REPLICAOF host port</c>. <see cref="Start(string,int)"/> opens a
    /// <see cref="GarnetClientSession"/>, runs the stock-Redis attach handshake
    /// (<c>PING</c>, <c>REPLCONF listening-port</c>, <c>REPLCONF capa eof capa psync2</c>,
    /// <c>PSYNC ? -1</c>), and then idles. The primary side completes the handshake
    /// with <c>+FULLRESYNC &lt;replid&gt; 0</c> + the empty-DB RDB body, which is
    /// enough for Sentinel to see the replica as attached (<c>master_link_status:up</c>,
    /// <c>connected_slaves:N</c> includes us). No actual data replication is performed
    /// by this class.</para>
    ///
    /// <para>What this does <em>not</em> do (Phase 4 scope):</para>
    /// <list type="bullet">
    ///   <item>Stream AOF records from the primary's side. The replica receives only the
    ///         <c>+FULLRESYNC</c> reply and the 56-byte empty body, then keeps the
    ///         socket open for the primary to write more bytes onto later.</item>
    ///   <item>Periodic <c>REPLCONF ACK</c> from replica to primary. Sentinel does not
    ///         require ACK to advertise the replica in INFO replication; it just needs
    ///         a healthy TCP socket and a successful PSYNC.</item>
    /// </list>
    /// </summary>
    internal sealed class StandaloneReplicaClient : IDisposable
    {
        readonly StoreWrapper storeWrapper;
        readonly ILogger logger;

        GarnetClientSession session;
        CancellationTokenSource cts;
        Task attachTask;
        readonly object attachLock = new();

        /// <summary>The host:port of the current primary, or null if this node is a primary.</summary>
        public string PrimaryEndpoint { get; private set; }

        public StandaloneReplicaClient(StoreWrapper storeWrapper, ILogger logger)
        {
            this.storeWrapper = storeWrapper;
            this.logger = logger;
        }

        /// <summary>
        /// Begin attaching to <paramref name="host"/>:<paramref name="port"/>.
        ///
        /// <para>Detaches from any current primary first; only one outbound link is
        /// allowed at a time. The actual handshake runs in a background task so the
        /// issuing client (the one that sent <c>REPLICAOF</c>) gets <c>+OK</c> back
        /// promptly. The session lives until <see cref="Detach"/> or
        /// <see cref="Dispose"/> is called.</para>
        /// </summary>
        public void Start(string host, int port)
        {
            Detach();

            storeWrapper.LocalReplicationState.BecomeReplica(host, port);
            PrimaryEndpoint = $"{host}:{port}";

            var loggerLocal = logger;
            var storeLocal = storeWrapper;

            lock (attachLock)
            {
                cts?.Dispose();
                cts = new CancellationTokenSource();
                var token = cts.Token;
                attachTask = Task.Run(() => RunAttachAsync(host, port, this, storeLocal, loggerLocal, token));
            }
        }

        /// <summary>
        /// Tear down the current outbound session, if any. Returns this node to the
        /// "master" role and clears the primary endpoint. Safe to call when no
        /// session exists.
        /// </summary>
        public void Detach()
        {
            lock (attachLock)
            {
                if (cts != null)
                {
                    try { cts.Cancel(); } catch { /* ignore */ }
                }
            }

            var oldSession = session;
            session = null;
            oldSession?.Dispose();

            lock (attachLock)
            {
                try { attachTask?.Wait(TimeSpan.FromSeconds(2)); } catch { /* ignore */ }
                attachTask = null;
            }

            storeWrapper.LocalReplicationState.BecomePrimary();
            PrimaryEndpoint = null;
        }

        public void Dispose() => Detach();

        /// <summary>
        /// Background task: open a <see cref="GarnetClientSession"/> to the primary
        /// and run the standard Redis attach handshake.
        ///
        /// <para>The handshake order matches what stock Redis 7.4.11 accepts from a
        /// replica attaching for the first time, verified by capturing the wire
        /// against a live primary:</para>
        /// <code>
        ///   PING                                            -> +PONG
        ///   REPLCONF listening-port &lt;our-port&gt;                -> +OK
        ///   REPLCONF capa eof capa psync2                   -> +OK
        ///   PSYNC ? -1                                      -> +FULLRESYNC &lt;replid&gt; 0
        ///   $56\r\n&lt;empty RDB&gt;
        /// </code>
        /// </summary>
        static async Task RunAttachAsync(string host, int port, StandaloneReplicaClient owner, StoreWrapper storeWrapper, ILogger logger, CancellationToken token)
        {
            GarnetClientSession newClient = null;
            try
            {
                token.ThrowIfCancellationRequested();

                if (!IPAddress.TryParse(host, out var address))
                {
                    try
                    {
                        var entry = await Dns.GetHostEntryAsync(host).ConfigureAwait(false);
                        address = entry.AddressList.FirstOrDefault(a => a.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                                  ?? entry.AddressList[0];
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "StandaloneReplicaClient: DNS lookup failed for {host}", host);
                        storeWrapper.LocalReplicationState.ReportLinkDown();
                        return;
                    }
                }

                var endpoint = new IPEndPoint(address, port);
                newClient = new GarnetClientSession(endpoint, networkBufferSettings: new NetworkBufferSettings(), clientName: nameof(StandaloneReplicaClient));

                await newClient.ConnectAsync(token: token).ConfigureAwait(false);

                // Step 1: PING
                var pong = await newClient.ExecuteAsync("PING").WaitAsync(token).ConfigureAwait(false);
                if (pong != "PONG")
                {
                    logger?.LogWarning("StandaloneReplicaClient: unexpected PING reply '{pong}' from {endpoint}", pong, endpoint);
                    newClient.Dispose();
                    storeWrapper.LocalReplicationState.ReportLinkDown();
                    return;
                }

                // Step 2: REPLCONF listening-port <our-port>
                // We tell the primary the port Sentinel should later connect to. The
                // local listening port is what the host configures; we do not have a
                // direct accessor on StoreWrapper, so we use the system-default 0 and
                // let the primary interpret that. A future phase should plumb the
                // server's actual listening port through here.
                var ourPort = 0;
                var ackPort = await newClient.ExecuteAsync("REPLCONF", "listening-port", ourPort.ToString())
                    .WaitAsync(token).ConfigureAwait(false);
                if (ackPort != "OK")
                {
                    logger?.LogWarning("StandaloneReplicaClient: unexpected REPLCONF listening-port reply '{reply}'", ackPort);
                    newClient.Dispose();
                    storeWrapper.LocalReplicationState.ReportLinkDown();
                    return;
                }

                // Step 3: REPLCONF capa eof capa psync2
                var ackCapa = await newClient.ExecuteAsync("REPLCONF", "capa", "eof", "capa", "psync2")
                    .WaitAsync(token).ConfigureAwait(false);
                if (ackCapa != "OK")
                {
                    logger?.LogWarning("StandaloneReplicaClient: unexpected REPLCONF capa reply '{reply}'", ackCapa);
                    newClient.Dispose();
                    storeWrapper.LocalReplicationState.ReportLinkDown();
                    return;
                }

                // Step 4: PSYNC ? -1
                // The reply is a multi-line response: "+FULLRESYNC <replid> 0\r\n"
                // followed by "$56\r\n<empty RDB>". ExecuteAsync collapses these into
                // its single-string return; we only need to verify it starts with
                // "+FULLRESYNC" or contains the marker.
                var psyncReply = await newClient.ExecuteAsync("PSYNC", "?", "-1")
                    .WaitAsync(token).ConfigureAwait(false);
                if (psyncReply == null || !psyncReply.Contains("FULLRESYNC"))
                {
                    logger?.LogWarning("StandaloneReplicaClient: unexpected PSYNC reply '{reply}'", psyncReply);
                    newClient.Dispose();
                    storeWrapper.LocalReplicationState.ReportLinkDown();
                    return;
                }

                storeWrapper.LocalReplicationState.ReportLinkUp();
                storeWrapper.LocalReplicationState.MarkSyncCompleted();
                logger?.LogInformation("StandaloneReplicaClient: attached to {endpoint} as replica", endpoint);

                // Hand the live session back to the owner so Detach() can dispose it.
                // Until we reach this point, the session is local to RunAttachAsync and
                // would not be cleaned up by REPLICAOF NO ONE.
                owner.session = newClient;
                newClient = null;
                newClient = null;
            }
            catch (OperationCanceledException)
            {
                logger?.LogInformation("StandaloneReplicaClient: attach cancelled");
                newClient?.Dispose();
                storeWrapper.LocalReplicationState.ReportLinkDown();
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "StandaloneReplicaClient: attach failed");
                newClient?.Dispose();
                storeWrapper.LocalReplicationState.ReportLinkDown();
            }
        }
    }
}