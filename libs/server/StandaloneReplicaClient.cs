// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// The outbound replication link from a standalone Garnet replica to its primary.
    ///
    /// <para>One instance lives in <see cref="StoreWrapper"/> per node that has executed
    /// <c>REPLICAOF host port</c>. <see cref="Start(string,int)"/> opens a
    /// TCP connection, runs the stock-Redis attach handshake
    /// (<c>PING</c>, <c>REPLCONF listening-port</c>, <c>REPLCONF capa eof capa psync2</c>,
    /// <c>PSYNC ? -1</c>), and then consumes Garnet AOF frames. The primary side completes the handshake
    /// with <c>+FULLRESYNC &lt;replid&gt; 0</c> + the empty-DB RDB body, which is
    /// enough for Sentinel to see the replica as attached and for writes made after
    /// attachment to be replayed on the replica.</para>
    /// </summary>
    internal sealed class StandaloneReplicaClient : IDisposable
    {
        readonly StoreWrapper storeWrapper;
        readonly ILogger logger;

        TcpClient client;
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

            var oldClient = client;
            client = null;
            oldClient?.Dispose();

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
        /// Background task: open a TCP connection to the primary
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
            TcpClient newClient = null;
            AofProcessor aofProcessor = null;
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
                newClient = new TcpClient(endpoint.AddressFamily);
                await newClient.ConnectAsync(endpoint, token).ConfigureAwait(false);
                var stream = newClient.GetStream();

                // Step 1: PING
                await WriteCommandAsync(stream, token, "PING").ConfigureAwait(false);
                var pong = await ReadLineAsync(stream, token).ConfigureAwait(false);
                if (pong != "+PONG")
                {
                    logger?.LogWarning("StandaloneReplicaClient: unexpected PING reply '{pong}' from {endpoint}", pong, endpoint);
                    newClient.Dispose();
                    storeWrapper.LocalReplicationState.ReportLinkDown();
                    return;
                }

                // Step 2: REPLCONF listening-port <our-port>
                // Tell the primary the port Sentinel should later connect to.
                var ourPort = GetListeningPort(storeWrapper);
                await WriteCommandAsync(stream, token, "REPLCONF", "listening-port", ourPort.ToString()).ConfigureAwait(false);
                var ackPort = await ReadLineAsync(stream, token).ConfigureAwait(false);
                if (ackPort != "+OK")
                {
                    logger?.LogWarning("StandaloneReplicaClient: unexpected REPLCONF listening-port reply '{reply}'", ackPort);
                    newClient.Dispose();
                    storeWrapper.LocalReplicationState.ReportLinkDown();
                    return;
                }

                // Step 3: REPLCONF capa eof capa psync2
                await WriteCommandAsync(stream, token, "REPLCONF", "capa", "eof", "capa", "psync2").ConfigureAwait(false);
                var ackCapa = await ReadLineAsync(stream, token).ConfigureAwait(false);
                if (ackCapa != "+OK")
                {
                    logger?.LogWarning("StandaloneReplicaClient: unexpected REPLCONF capa reply '{reply}'", ackCapa);
                    newClient.Dispose();
                    storeWrapper.LocalReplicationState.ReportLinkDown();
                    return;
                }

                // Step 4: PSYNC ? -1
                await WriteCommandAsync(stream, token, "PSYNC", "?", "-1").ConfigureAwait(false);
                var psyncReply = await ReadLineAsync(stream, token).ConfigureAwait(false);
                if (!psyncReply.StartsWith("+FULLRESYNC ", StringComparison.Ordinal))
                {
                    logger?.LogWarning("StandaloneReplicaClient: unexpected PSYNC reply '{reply}'", psyncReply);
                    newClient.Dispose();
                    storeWrapper.LocalReplicationState.ReportLinkDown();
                    return;
                }

                var rdbHeader = await ReadLineAsync(stream, token).ConfigureAwait(false);
                if (rdbHeader.Length < 2 || rdbHeader[0] != '$' || !int.TryParse(rdbHeader.AsSpan(1), out var rdbLength) || rdbLength < 0)
                    throw new InvalidOperationException($"Invalid RDB length header '{rdbHeader}'");

                var rdb = new byte[rdbLength];
                await stream.ReadExactlyAsync(rdb, token).ConfigureAwait(false);

                storeWrapper.LocalReplicationState.ReportLinkUp();
                storeWrapper.LocalReplicationState.MarkSyncCompleted();
                logger?.LogInformation("StandaloneReplicaClient: attached to {endpoint} as replica", endpoint);

                owner.client = newClient;
                aofProcessor = new AofProcessor(
                    storeWrapper,
                    recordToAof: storeWrapper.serverOptions.EnableAOF,
                    logger: logger);

                var header = new byte[StandaloneReplicationWireFormat.HeaderLength];
                while (true)
                {
                    await stream.ReadExactlyAsync(header, token).ConfigureAwait(false);
                    if (!StandaloneReplicationWireFormat.TryReadHeader(header, out var payloadLength, out var currentAddress))
                        throw new InvalidOperationException("Primary sent an invalid standalone replication frame");

                    var payload = new byte[payloadLength];
                    await stream.ReadExactlyAsync(payload, token).ConfigureAwait(false);
                    ProcessAofRecord(aofProcessor, payload, currentAddress);
                    storeWrapper.LocalReplicationState.ReportLinkUp();
                }
            }
            catch (OperationCanceledException)
            {
                logger?.LogInformation("StandaloneReplicaClient: attach cancelled");
                storeWrapper.LocalReplicationState.ReportLinkDown();
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "StandaloneReplicaClient: attach failed");
                storeWrapper.LocalReplicationState.ReportLinkDown();
            }
            finally
            {
                aofProcessor?.Dispose();
                newClient?.Dispose();
                if (ReferenceEquals(owner.client, newClient))
                    owner.client = null;
            }
        }

        static int GetListeningPort(StoreWrapper storeWrapper)
            => storeWrapper.serverOptions.EndPoints.OfType<IPEndPoint>().FirstOrDefault()?.Port ?? 0;

        static async Task WriteCommandAsync(NetworkStream stream, CancellationToken token, params string[] args)
        {
            var command = new StringBuilder();
            command.Append('*').Append(args.Length).Append("\r\n");
            foreach (var arg in args)
            {
                var byteCount = Encoding.UTF8.GetByteCount(arg);
                command.Append('$').Append(byteCount).Append("\r\n").Append(arg).Append("\r\n");
            }

            var bytes = Encoding.UTF8.GetBytes(command.ToString());
            await stream.WriteAsync(bytes, token).ConfigureAwait(false);
        }

        static async Task<string> ReadLineAsync(NetworkStream stream, CancellationToken token)
        {
            var buffer = new byte[256];
            var length = 0;
            while (true)
            {
                if (length == buffer.Length)
                {
                    if (buffer.Length >= 64 * 1024)
                        throw new InvalidOperationException("Replication handshake line exceeds 64 KiB");
                    Array.Resize(ref buffer, buffer.Length * 2);
                }

                await stream.ReadExactlyAsync(buffer.AsMemory(length, 1), token).ConfigureAwait(false);
                if (length > 0 && buffer[length - 1] == '\r' && buffer[length] == '\n')
                    return Encoding.ASCII.GetString(buffer, 0, length - 1);
                length++;
            }
        }

        static unsafe void ProcessAofRecord(AofProcessor aofProcessor, byte[] payload, long currentAddress)
        {
            fixed (byte* ptr = payload)
                aofProcessor.ProcessAofRecordInternal(0, ptr, payload.Length, asReplica: true, out _, currentAddress);
        }
    }
}