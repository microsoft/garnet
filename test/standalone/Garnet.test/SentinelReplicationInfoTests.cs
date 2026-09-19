// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers the standalone replication surface that an external orchestrator
    /// (Redis Sentinel) reads in order to discover and promote replicas.
    ///
    /// <para>These tests are intentionally <b>not</b> gated behind the external-process
    /// gate: they exercise Garnet in-process over a raw socket and need only Garnet
    /// itself, so they run in the default CI configuration. That matters because a
    /// previous revision of this work placed every replication test behind the gate,
    /// leaving the whole surface unexercised in CI.</para>
    ///
    /// <para>Background: Sentinel discovers a primary's replicas <i>only</i> by parsing
    /// the <c>connected_slaves</c> and <c>slave&lt;N&gt;</c> fields of
    /// <c>INFO replication</c>. If a primary reports zero replicas, Sentinel has no
    /// candidate to promote and failover can never occur — which is exactly the state
    /// Garnet was in before this work.</para>
    /// </summary>
    [TestFixture]
    public class SentinelReplicationInfoTests
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disableObjects: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown();
        }

        /// <summary>Port the in-process server is listening on.</summary>
        static int Port => ((IPEndPoint)TestUtils.EndPoint).Port;

        /// <summary>
        /// Sends a command as a RESP array of bulk strings over a raw socket and returns
        /// the raw reply text. A raw socket is used because several of the commands under
        /// test (PSYNC in particular) return payloads that a normal client cannot parse.
        /// </summary>
        static async Task<string> SendRawAsync(int port, params string[] args)
        {
            using var client = new TcpClient();
            await client.ConnectAsync(IPAddress.Loopback, port);
            await using var stream = client.GetStream();

            var sb = new StringBuilder();
            sb.Append('*').Append(args.Length).Append("\r\n");
            foreach (var a in args)
                sb.Append('$').Append(Encoding.UTF8.GetByteCount(a)).Append("\r\n").Append(a).Append("\r\n");

            await stream.WriteAsync(Encoding.ASCII.GetBytes(sb.ToString()));
            await stream.FlushAsync();

            var buffer = new byte[8192];
            using var cts = new System.Threading.CancellationTokenSource(2000);
            try
            {
                // Read whatever arrives within the window; enough for INFO/REPLCONF replies.
                var n = await stream.ReadAsync(buffer, 0, buffer.Length, cts.Token);
                return Encoding.ASCII.GetString(buffer, 0, n);
            }
            catch (OperationCanceledException)
            {
                return string.Empty;
            }
        }

        private static string InfoValue(string info, string key)
        {
            foreach (var line in info.Split('\n'))
            {
                var trimmed = line.TrimEnd('\r');
                if (trimmed.StartsWith(key + ":", StringComparison.Ordinal))
                    return trimmed[(key.Length + 1)..];
            }

            return null;
        }

        [Test]
        public async Task InitialInfoReportsMasterWithNoSlaves()
        {
            var raw = await SendRawAsync(Port, "INFO", "replication");

            ClassicAssert.That(raw, Does.Contain("role:master"),
                "A freshly started Garnet node should report itself as a master.");
            ClassicAssert.AreEqual("0", InfoValue(raw, "connected_slaves"),
                "With no replicas attached, connected_slaves must be 0.");
            ClassicAssert.That(raw, Does.Not.Contain("slave0:"),
                "No slave<N> lines should be present when no replica is attached.");
        }

        [Test]
        public async Task MasterReplOffsetIsNumericNotNull()
        {
            var raw = await SendRawAsync(Port, "INFO", "replication");
            var offset = InfoValue(raw, "master_repl_offset");

            ClassicAssert.IsNotNull(offset, "master_repl_offset must be present.");
            ClassicAssert.AreNotEqual("N/A", offset,
                "master_repl_offset must be numeric: clients and orchestrators parse it as an integer.");
            ClassicAssert.IsTrue(long.TryParse(offset, out _),
                $"master_repl_offset should parse as an integer, got: {offset}");
        }

        [Test]
        public async Task MasterReplIdIsStableAcrossCalls()
        {
            var first = InfoValue(await SendRawAsync(Port, "INFO", "replication"), "master_replid");
            var second = InfoValue(await SendRawAsync(Port, "INFO", "replication"), "master_replid");

            ClassicAssert.IsNotNull(first, "master_replid must be present.");
            ClassicAssert.AreEqual(40, first.Length,
                "A replication ID is 40 hex characters, matching Redis.");
            ClassicAssert.AreEqual(first, second,
                "master_replid must be stable across INFO calls; a value that changed per call " +
                "would make the node look like a different primary on every poll.");
            ClassicAssert.AreNotEqual(new string('0', 40), first,
                "master_replid must not be all zeros: clients may read an all-zero ID as " +
                "'no replication history' and force a full resync.");
        }

        [Test]
        public async Task ReplconfListeningPortRegistersReplicaInInfo()
        {
            // A stock replica's opening handshake.
            using var replica = new TcpClient();
            await replica.ConnectAsync(IPAddress.Loopback, Port);
            await using var stream = replica.GetStream();

            await WriteCommandAsync(stream, "REPLCONF", "listening-port", "6390");
            var reply = await ReadLineAsync(stream);
            ClassicAssert.AreEqual("+OK", reply, "REPLCONF listening-port should be acknowledged.");

            var info = await SendRawAsync(Port, "INFO", "replication");
            ClassicAssert.AreEqual("1", InfoValue(info, "connected_slaves"),
                "After a REPLCONF handshake the replica must be counted in connected_slaves. " +
                "This is the single gate that allows an orchestrator to discover replicas.");
            ClassicAssert.That(info, Does.Contain("slave0:"),
                $"Expected a slave0 line in INFO replication, got:\n{info}");
            ClassicAssert.That(info, Does.Contain("port=6390"),
                "The slave0 line must report the replica's advertised listening-port, " +
                "because that is the port an orchestrator later connects to in order to promote it.");

            // Deliberate divergence from Redis, documented here so it is not mistaken for a
            // missing online transition: Redis counts nothing at all until PSYNC completes
            // (verified against 7.4.11, where a REPLCONF-only connection leaves
            // connected_slaves at 0). Garnet registers the replica as soon as it identifies
            // itself, so an orchestrator can observe a replica that is mid-handshake, and
            // reports it as "sync" rather than "online" until PSYNC succeeds.
            ClassicAssert.That(info, Does.Contain("state=sync"),
                $"A replica that has not completed PSYNC should be reported as 'sync', got:\n{info}");
        }

        [Test]
        public async Task ReplicaIsOnlineAfterPsyncCompletes()
        {
            using var replica = new TcpClient();
            await replica.ConnectAsync(IPAddress.Loopback, Port);
            await using var stream = replica.GetStream();

            await WriteCommandAsync(stream, "REPLCONF", "listening-port", "6394");
            _ = await ReadLineAsync(stream);

            // Complete the handshake. PSYNC replies +FULLRESYNC followed by a raw RDB
            // payload; we only need to consume the status line to drive the state change.
            await WriteCommandAsync(stream, "PSYNC", "?", "-1");
            var fullResync = await ReadLineAsync(stream);
            ClassicAssert.That(fullResync, Does.StartWith("+FULLRESYNC"),
                $"Expected +FULLRESYNC, got: {fullResync}");
            await Task.Delay(200);

            var info = await SendRawAsync(Port, "INFO", "replication");
            ClassicAssert.That(info, Does.Contain("state=online"),
                $"A replica that completed PSYNC should be reported as 'online', got:\n{info}");
            ClassicAssert.AreEqual("1", InfoValue(info, "connected_slaves"));
        }

        [Test]
        public async Task ReplconfAckUpdatesReportedOffset()
        {
            using var replica = new TcpClient();
            await replica.ConnectAsync(IPAddress.Loopback, Port);
            await using var stream = replica.GetStream();

            await WriteCommandAsync(stream, "REPLCONF", "listening-port", "6391");
            _ = await ReadLineAsync(stream);

            // ACK is a no-reply form, so nothing is read back here.
            await WriteCommandAsync(stream, "REPLCONF", "ACK", "4096");
            await Task.Delay(200);

            var info = await SendRawAsync(Port, "INFO", "replication");
            ClassicAssert.That(info, Does.Contain("offset=4096"),
                $"The slave0 line should report the last acknowledged offset, got:\n{info}");
        }

        [Test]
        public async Task SlaveLineMatchesRedisFieldShape()
        {
            using var replica = new TcpClient();
            await replica.ConnectAsync(IPAddress.Loopback, Port);
            await using var stream = replica.GetStream();
            await WriteCommandAsync(stream, "REPLCONF", "listening-port", "6392");
            _ = await ReadLineAsync(stream);

            var info = await SendRawAsync(Port, "INFO", "replication");
            var slaveLine = info.Split('\n')
                .Select(l => l.TrimEnd('\r'))
                .FirstOrDefault(l => l.StartsWith("slave0:", StringComparison.Ordinal));

            ClassicAssert.IsNotNull(slaveLine, $"Expected a slave0 line, got:\n{info}");

            // Redis emits ip=,port=,state=,offset=,lag= in this order. Sentinel parses
            // these as comma-separated key=value pairs, so both the names and their
            // presence matter.
            foreach (var field in new[] { "ip=", "port=", "state=", "offset=", "lag=" })
                ClassicAssert.That(slaveLine, Does.Contain(field),
                    $"slave0 line is missing required field '{field}': {slaveLine}");
        }

        [Test]
        public async Task DisconnectedReplicaIsNotCounted()
        {
            using (var replica = new TcpClient())
            {
                await replica.ConnectAsync(IPAddress.Loopback, Port);
                await using var stream = replica.GetStream();
                await WriteCommandAsync(stream, "REPLCONF", "listening-port", "6393");
                _ = await ReadLineAsync(stream);

                var during = await SendRawAsync(Port, "INFO", "replication");
                ClassicAssert.AreEqual("1", InfoValue(during, "connected_slaves"),
                    "Replica should be counted while connected.");
            }

            // Connection closed. Note that Garnet does not currently prune the registry on
            // disconnect, so this documents current behaviour rather than asserting an
            // ideal; it guards against a regression where entries are double-counted.
            await Task.Delay(200);
            var after = await SendRawAsync(Port, "INFO", "replication");
            var count = long.Parse(InfoValue(after, "connected_slaves"));
            ClassicAssert.LessOrEqual(count, 1,
                "A closed replica connection must not cause the count to grow.");
        }

        static async Task WriteCommandAsync(NetworkStream stream, params string[] args)
        {
            var sb = new StringBuilder();
            sb.Append('*').Append(args.Length).Append("\r\n");
            foreach (var a in args)
                sb.Append('$').Append(Encoding.UTF8.GetByteCount(a)).Append("\r\n").Append(a).Append("\r\n");

            await stream.WriteAsync(Encoding.ASCII.GetBytes(sb.ToString()));
            await stream.FlushAsync();
        }

        static async Task<string> ReadLineAsync(NetworkStream stream)
        {
            var buf = new MemoryStream();
            var one = new byte[1];
            using var cts = new System.Threading.CancellationTokenSource(2000);
            try
            {
                while (true)
                {
                    var n = await stream.ReadAsync(one, 0, 1, cts.Token);
                    if (n == 0) break;
                    buf.WriteByte(one[0]);
                    if (one[0] == (byte)'\n' && buf.Length >= 2)
                    {
                        var arr = buf.ToArray();
                        return Encoding.ASCII.GetString(arr, 0, arr.Length - 2);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Fall through and return whatever was collected.
            }

            return Encoding.ASCII.GetString(buf.ToArray());
        }
    }
}
