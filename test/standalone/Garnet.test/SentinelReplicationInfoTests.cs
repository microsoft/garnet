// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Garnet.server;
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
            ClassicAssert.AreEqual(server.Provider.StoreWrapper.RunId, first,
                "Standalone INFO and checkpoint history must use the same replication ID.");
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

    /// <summary>
    /// Covers the replica-side attach flow that lets a standalone Garnet node become a
    /// replica of another Garnet node under the Sentinel control plane. The fix is
    /// gated behind <c>EnableStandaloneReplication</c> (CLI <c>--sentinel-replication</c>),
    /// because shipping it on by default would silently change the meaning of
    /// <c>REPLICAOF</c> for existing deployments.
    ///
    /// <para>These tests are in-process (no Sentinel binary required) and ungated so they
    /// run in default CI. The Sentinel binary itself is exercised separately by the
    /// gated suite under <c>test/standalone/Garnet.test.sentinel/</c>.</para>
    /// </summary>
    [TestFixture]
    public class SentinelStandaloneReplicationTests
    {
        GarnetServer primary;
        GarnetServer replica;

        int primaryPort;
        int replicaPort;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            // Pick two free ports.
            primaryPort = FindFreePort();
            replicaPort = FindFreePort();

            primary = TestUtils.CreateGarnetServer(
                Path.Combine(TestUtils.MethodTestDir, "primary"),
                port: primaryPort,
                disableObjects: true,
                enableAOF: true,
                enableStandaloneReplication: true);
            primary.Start();

            // EnableStandaloneReplication is the opt-in for the standalone REPLICAOF
            // handler. Without it, REPLICAOF on a standalone node still errors out
            // with the stock "cluster disabled" message.
            replica = TestUtils.CreateGarnetServer(
                Path.Combine(TestUtils.MethodTestDir, "replica"),
                port: replicaPort,
                disableObjects: true,
                enableAOF: true,
                enableStandaloneReplication: true);
            replica.Start();
        }

        [TearDown]
        public void TearDown()
        {
            replica?.Dispose();
            primary?.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        public async Task ReplicaofHostPortConnectsAndReportsAsSlave()
        {
            // Issue REPLICAOF on the replica. The reply should be +OK and the replica
            // should then attempt the attach handshake in the background.
            using var client = new TcpClient();
            await client.ConnectAsync(IPAddress.Loopback, replicaPort);
            await using var stream = client.GetStream();

            await WriteCommandAsync(stream, "REPLICAOF", "127.0.0.1", primaryPort.ToString());
            var reply = await ReadLineAsync(stream);
            ClassicAssert.AreEqual("+OK", reply, "REPLICAOF host port should reply +OK.");

            // Wait for the background handshake to complete (PING / REPLCONF / PSYNC).
            await WaitForReplicaCountAsync(primaryPort, expected: 1, timeoutMs: 5000);

            // The primary should now report the replica as attached.
            var primaryInfo = await TestUtils.SendRawAsync(primaryPort, "INFO", "replication");
            ClassicAssert.AreEqual("1", TestUtils.InfoValue(primaryInfo, "connected_slaves"),
                "After the replica handshake completes, the primary must report 1 connected_slaves.");
            ClassicAssert.That(primaryInfo, Does.Contain("slave0:"),
                $"Expected a slave0 line on the primary after attach, got:\n{primaryInfo}");
            ClassicAssert.That(primaryInfo, Does.Contain("state=online"),
                $"A replica that completed PSYNC should be reported as 'online', got:\n{primaryInfo}");

            // And the replica itself should now report itself as role:slave.
            var replicaInfo = await TestUtils.SendRawAsync(replicaPort, "INFO", "replication");
            ClassicAssert.AreEqual("slave", TestUtils.InfoValue(replicaInfo, "role"),
                $"After REPLICAOF, the replica should report role:slave, got:\n{replicaInfo}");
            ClassicAssert.AreEqual("127.0.0.1", TestUtils.InfoValue(replicaInfo, "master_host"));
            ClassicAssert.AreEqual(primaryPort.ToString(), TestUtils.InfoValue(replicaInfo, "master_port"));
            ClassicAssert.AreEqual("up", TestUtils.InfoValue(replicaInfo, "master_link_status"));
            ClassicAssert.AreEqual("0", TestUtils.InfoValue(replicaInfo, "master_sync_in_progress"),
                $"After PSYNC completes, master_sync_in_progress must be 0, got:\n{replicaInfo}");
        }

        [Test]
        public async Task WritesAfterAttachReplicateToReplica()
        {
            using var client = new TcpClient();
            await client.ConnectAsync(IPAddress.Loopback, replicaPort);
            await using var stream = client.GetStream();

            await WriteCommandAsync(stream, "REPLICAOF", "127.0.0.1", primaryPort.ToString());
            ClassicAssert.AreEqual("+OK", await ReadLineAsync(stream));
            await WaitForReplicaCountAsync(primaryPort, expected: 1, timeoutMs: 5000);

            ClassicAssert.AreEqual("+OK\r\n", await TestUtils.SendRawAsync(primaryPort, "SET", "replicated-key", "replicated-value"));
            await WaitForValueAsync(replicaPort, "replicated-key", "replicated-value", timeoutMs: 5000);
        }

        [Test]
        public async Task CheckpointAndCatchupReplicateOnAttach()
        {
            ClassicAssert.AreEqual("+OK\r\n", await TestUtils.SendRawAsync(primaryPort, "SET", "snapshot-key", "snapshot-value"));
            ClassicAssert.That(await primary.Provider.StoreWrapper.TakeCheckpointAsync(background: false), Is.EqualTo(CheckpointStatus.Success));
            ClassicAssert.AreEqual("+OK\r\n", await TestUtils.SendRawAsync(primaryPort, "SET", "catchup-key", "catchup-value"));

            ClassicAssert.AreEqual(
                "+OK\r\n",
                await TestUtils.SendRawAsync(replicaPort, "REPLICAOF", "127.0.0.1", primaryPort.ToString()));

            await WaitForValueAsync(replicaPort, "snapshot-key", "snapshot-value", timeoutMs: 10000);
            await WaitForValueAsync(replicaPort, "catchup-key", "catchup-value", timeoutMs: 10000);
        }

        [Test]
        public async Task PrimaryCheckpointDoesNotInterruptLiveReplication()
        {
            using var client = new TcpClient();
            await client.ConnectAsync(IPAddress.Loopback, replicaPort);
            await using var stream = client.GetStream();

            await WriteCommandAsync(stream, "REPLICAOF", "127.0.0.1", primaryPort.ToString());
            ClassicAssert.AreEqual("+OK", await ReadLineAsync(stream));
            await WaitForReplicaCountAsync(primaryPort, expected: 1, timeoutMs: 5000);

            ClassicAssert.AreEqual("+OK\r\n", await TestUtils.SendRawAsync(primaryPort, "SET", "before-checkpoint", "value-1"));
            await WaitForValueAsync(replicaPort, "before-checkpoint", "value-1", timeoutMs: 5000);

            ClassicAssert.That(await primary.Provider.StoreWrapper.TakeCheckpointAsync(background: false), Is.EqualTo(CheckpointStatus.Success));

            ClassicAssert.AreEqual("+OK\r\n", await TestUtils.SendRawAsync(primaryPort, "SET", "after-checkpoint", "value-2"));
            await WaitForValueAsync(replicaPort, "after-checkpoint", "value-2", timeoutMs: 5000);
        }

        [Test]
        public async Task CheckpointLeasePinsFilesUntilReleased()
        {
            ClassicAssert.AreEqual("+OK\r\n", await TestUtils.SendRawAsync(primaryPort, "SET", "checkpoint-key", "value-1"));
            ClassicAssert.That(await primary.Provider.StoreWrapper.TakeCheckpointAsync(background: false), Is.EqualTo(CheckpointStatus.Success));

            var checkpointStore = primary.Provider.StoreWrapper.DefaultDatabase.StandaloneCheckpointStore;
            ClassicAssert.IsNotNull(checkpointStore);
            ClassicAssert.IsTrue(checkpointStore.TryAcquireLatest(out var checkpointLease));
            var leasedLogToken = checkpointLease.Value.storeHlogToken;

            ClassicAssert.AreEqual("+OK\r\n", await TestUtils.SendRawAsync(primaryPort, "SET", "checkpoint-key", "value-2"));
            ClassicAssert.That(await primary.Provider.StoreWrapper.TakeCheckpointAsync(background: false), Is.EqualTo(CheckpointStatus.Success));

            var checkpointManager = primary.Provider.StoreWrapper.StoreCheckpointManager;
            CollectionAssert.Contains(checkpointManager.GetLogCheckpointTokens().ToArray(), leasedLogToken);

            checkpointLease.Dispose();
            CollectionAssert.DoesNotContain(checkpointManager.GetLogCheckpointTokens().ToArray(), leasedLogToken);
        }

        [Test]
        public async Task RecoveredCheckpointIsAvailableForLeasing()
        {
            ClassicAssert.AreEqual("+OK\r\n", await TestUtils.SendRawAsync(primaryPort, "SET", "recovered-key", "recovered-value"));
            var checkpointHistoryId = primary.Provider.StoreWrapper.RunId;
            ClassicAssert.That(await primary.Provider.StoreWrapper.TakeCheckpointAsync(background: false), Is.EqualTo(CheckpointStatus.Success));

            primary.Dispose(false);
            primary = TestUtils.CreateGarnetServer(
                Path.Combine(TestUtils.MethodTestDir, "primary"),
                port: primaryPort,
                disableObjects: true,
                enableAOF: true,
                tryRecover: true,
                enableStandaloneReplication: true);
            primary.Start();

            var checkpointStore = primary.Provider.StoreWrapper.DefaultDatabase.StandaloneCheckpointStore;
            ClassicAssert.IsNotNull(checkpointStore);
            ClassicAssert.IsTrue(checkpointStore.TryAcquireLatest(out var checkpointLease));
            using (checkpointLease)
            {
                ClassicAssert.AreNotEqual(default, checkpointLease.Value.storeHlogToken);
                ClassicAssert.AreNotEqual(default, checkpointLease.Value.storeIndexToken);
                ClassicAssert.AreEqual(checkpointHistoryId, checkpointLease.Value.storePrimaryReplId);
                ClassicAssert.That(checkpointLease.Value.storeCheckpointCoveredAofAddress[0], Is.GreaterThan(0));
            }
        }

        [Test]
        public async Task RoleCommandReflectsReplicaState()
        {
            using var client = new TcpClient();
            await client.ConnectAsync(IPAddress.Loopback, replicaPort);
            await using var stream = client.GetStream();

            // Before REPLICAOF, ROLE on a standalone node is ["master", 0, []].
            var beforeRole = await ReadBulkArrayAsync(stream, "ROLE");
            ClassicAssert.AreEqual(3, beforeRole.Length, "ROLE on a fresh standalone node returns 3 elements.");
            ClassicAssert.AreEqual("master", beforeRole[0]);

            await WriteCommandAsync(stream, "REPLICAOF", "127.0.0.1", primaryPort.ToString());
            _ = await ReadLineAsync(stream);

            // Wait for the attach to settle so the role is stable.
            await WaitForReplicaCountAsync(primaryPort, expected: 1, timeoutMs: 5000);

            var afterRole = await ReadBulkArrayAsync(stream, "ROLE");
            ClassicAssert.AreEqual(5, afterRole.Length,
                $"ROLE on a standalone replica returns 5 elements (slave/host/port/state/offset), got {afterRole.Length}: {string.Join(",", afterRole)}");
            ClassicAssert.AreEqual("slave", afterRole[0]);
            ClassicAssert.AreEqual("127.0.0.1", afterRole[1]);
            ClassicAssert.AreEqual(primaryPort.ToString(), afterRole[2]);
            ClassicAssert.That(afterRole[3], Is.AnyOf("connecting", "connected"),
                $"After attach, state must be connecting or connected, got: {afterRole[3]}");
        }

        [Test]
        public async Task MultipleReplicasAttachConcurrently()
        {
            // Three replicas, three Sentinels is the canonical shape. We can stand up
            // additional replicas in-process and verify the primary reports each.
            var extraPorts = new[] { FindFreePort(), FindFreePort() };
            var extraServers = new System.Collections.Generic.List<GarnetServer>();

            try
            {
                foreach (var p in extraPorts)
                {
                    var s = TestUtils.CreateGarnetServer(
                        Path.Combine(TestUtils.MethodTestDir, $"replica-{p}"),
                        port: p,
                        disableObjects: true,
                        enableAOF: true,
                        enableStandaloneReplication: true);
                    s.Start();
                    extraServers.Add(s);
                }

                foreach (var p in new[] { replicaPort }.Concat(extraPorts))
                {
                    using var c = new TcpClient();
                    await c.ConnectAsync(IPAddress.Loopback, p);
                    await using var s = c.GetStream();
                    await WriteCommandAsync(s, "REPLICAOF", "127.0.0.1", primaryPort.ToString());
                    _ = await ReadLineAsync(s);
                }

                await WaitForReplicaCountAsync(primaryPort, expected: 3, timeoutMs: 10_000);

                var info = await TestUtils.SendRawAsync(primaryPort, "INFO", "replication");
                ClassicAssert.AreEqual("3", TestUtils.InfoValue(info, "connected_slaves"),
                    $"Expected 3 connected_slaves after three replicas attached, got:\n{info}");
                ClassicAssert.That(info, Does.Contain("slave0:"));
                ClassicAssert.That(info, Does.Contain("slave1:"));
                ClassicAssert.That(info, Does.Contain("slave2:"));
            }
            finally
            {
                foreach (var s in extraServers) s.Dispose();
            }
        }

        [Test]
        public async Task ReplicaofNoOnePromotesAndClearsState()
        {
            // First attach.
            using (var c = new TcpClient())
            {
                await c.ConnectAsync(IPAddress.Loopback, replicaPort);
                await using var s = c.GetStream();
                await WriteCommandAsync(s, "REPLICAOF", "127.0.0.1", primaryPort.ToString());
                _ = await ReadLineAsync(s);
            }

            await WaitForReplicaCountAsync(primaryPort, expected: 1, timeoutMs: 5000);

            // Promote.
            using (var c = new TcpClient())
            {
                await c.ConnectAsync(IPAddress.Loopback, replicaPort);
                await using var s = c.GetStream();
                await WriteCommandAsync(s, "REPLICAOF", "NO", "ONE");
                _ = await ReadLineAsync(s);
            }

            // The replica should now report role:master and the primary should drop it.
            await WaitForReplicaCountAsync(primaryPort, expected: 0, timeoutMs: 5000);

            var replicaInfo = await TestUtils.SendRawAsync(replicaPort, "INFO", "replication");
            ClassicAssert.AreEqual("master", TestUtils.InfoValue(replicaInfo, "role"),
                $"After REPLICAOF NO ONE, the replica should be a master, got:\n{replicaInfo}");

            var primaryInfo = await TestUtils.SendRawAsync(primaryPort, "INFO", "replication");
            ClassicAssert.AreEqual("0", TestUtils.InfoValue(primaryInfo, "connected_slaves"),
                $"After the replica detached, primary should report 0 slaves, got:\n{primaryInfo}");
        }

        [Test]
        public async Task ReplicaofWithoutFlagErrorsOut()
        {
            // Spin up a third node that does NOT have the flag set, to confirm the
            // option is the actual gate (not just the existence of a handler).
            var ungatedPort = FindFreePort();
            GarnetServer ungated = null;
            try
            {
                ungated = TestUtils.CreateGarnetServer(
                    TestUtils.MethodTestDir,
                    port: ungatedPort,
                    disableObjects: true);
                ungated.Start();

                using var c = new TcpClient();
                await c.ConnectAsync(IPAddress.Loopback, ungatedPort);
                await using var s = c.GetStream();
                await WriteCommandAsync(s, "REPLICAOF", "127.0.0.1", primaryPort.ToString());
                var reply = await ReadLineAsync(s);
                ClassicAssert.That(reply, Does.StartWith("-ERR"),
                    $"Without --sentinel-replication, REPLICAOF should error, got: {reply}");
            }
            finally
            {
                ungated?.Dispose();
            }
        }

        [Test]
        public async Task ReplicaofWithoutAofErrorsOut()
        {
            var noAofPort = FindFreePort();
            using var noAofServer = TestUtils.CreateGarnetServer(
                Path.Combine(TestUtils.MethodTestDir, "no-aof"),
                port: noAofPort,
                disableObjects: true,
                enableStandaloneReplication: true);
            noAofServer.Start();

            using var client = new TcpClient();
            await client.ConnectAsync(IPAddress.Loopback, noAofPort);
            await using var stream = client.GetStream();
            await WriteCommandAsync(stream, "REPLICAOF", "127.0.0.1", primaryPort.ToString());

            var reply = await ReadLineAsync(stream);
            ClassicAssert.AreEqual("-ERR standalone replication requires AOF", reply);
        }

        [Test]
        public async Task ReplicaAnnounceIpReachesPrimaryInfoReplication()
        {
            // The replica, configured with --replica-announce-ip, must surface that
            // address in the primary's INFO replication output rather than the
            // connection's loopback peer address.
            var announcePort = FindFreePort();
            var announceIp = "10.20.30.40";
            using var announceReplica = TestUtils.CreateGarnetServer(
                Path.Combine(TestUtils.MethodTestDir, "announce-ip-replica"),
                port: announcePort,
                disableObjects: true,
                enableAOF: true,
                enableStandaloneReplication: true,
                replicaAnnounceIp: announceIp);
            announceReplica.Start();

            using var client = new TcpClient();
            await client.ConnectAsync(IPAddress.Loopback, announcePort);
            await using var stream = client.GetStream();
            await WriteCommandAsync(stream, "REPLICAOF", "127.0.0.1", primaryPort.ToString());
            ClassicAssert.AreEqual("+OK", await ReadLineAsync(stream));
            await WaitForReplicaCountAsync(primaryPort, expected: 1, timeoutMs: 5000);

            var primaryInfo = await TestUtils.SendRawAsync(primaryPort, "INFO", "replication");
            // The replica announced a non-loopback IP, so the primary must report it.
            ClassicAssert.That(primaryInfo, Does.Contain($"ip={announceIp}"),
                $"Expected primary INFO replication to contain 'ip={announceIp}', got:\n{primaryInfo}");
        }

        [Test]
        public async Task ReplicaAnnouncePortOverrideReachesPrimaryInfoReplication()
        {
            // The replica, configured with --replica-announce-port, must surface that
            // port in the primary's INFO replication output rather than the bound
            // listening port.
            var boundPort = FindFreePort();
            var announcedPort = boundPort + 1000;
            using var announceReplica = TestUtils.CreateGarnetServer(
                Path.Combine(TestUtils.MethodTestDir, "announce-port-replica"),
                port: boundPort,
                disableObjects: true,
                enableAOF: true,
                enableStandaloneReplication: true,
                replicaAnnouncePort: announcedPort);
            announceReplica.Start();

            using var client = new TcpClient();
            await client.ConnectAsync(IPAddress.Loopback, boundPort);
            await using var stream = client.GetStream();
            await WriteCommandAsync(stream, "REPLICAOF", "127.0.0.1", primaryPort.ToString());
            ClassicAssert.AreEqual("+OK", await ReadLineAsync(stream));
            await WaitForReplicaCountAsync(primaryPort, expected: 1, timeoutMs: 5000);

            var primaryInfo = await TestUtils.SendRawAsync(primaryPort, "INFO", "replication");
            ClassicAssert.That(primaryInfo, Does.Contain($"port={announcedPort}"),
                $"Expected primary INFO replication to contain 'port={announcedPort}', got:\n{primaryInfo}");
            ClassicAssert.That(primaryInfo, Does.Not.Contain($"port={boundPort},"),
                $"When --replica-announce-port is set, the bound port should NOT appear, got:\n{primaryInfo}");
        }

        // ---- helpers -----------------------------------------------------------

        static int FindFreePort()
        {
            var l = new TcpListener(IPAddress.Loopback, 0);
            l.Start();
            try { return ((IPEndPoint)l.LocalEndpoint).Port; }
            finally { l.Stop(); }
        }

        static async Task WaitForReplicaCountAsync(int port, int expected, int timeoutMs)
        {
            var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            while (DateTime.UtcNow < deadline)
            {
                var info = await TestUtils.SendRawAsync(port, "INFO", "replication");
                var raw = TestUtils.InfoValue(info, "connected_slaves");
                if (long.TryParse(raw, out var n) && n == expected) return;
                await Task.Delay(100);
            }
            throw new TimeoutException($"Timed out waiting for connected_slaves == {expected} on port {port}.");
        }

        static async Task WaitForValueAsync(int port, string key, string expected, int timeoutMs)
        {
            var expectedReply = $"${Encoding.UTF8.GetByteCount(expected)}\r\n{expected}\r\n";
            var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            while (DateTime.UtcNow < deadline)
            {
                if (await TestUtils.SendRawAsync(port, "GET", key) == expectedReply)
                    return;
                await Task.Delay(50);
            }

            throw new TimeoutException($"Timed out waiting for key '{key}' to replicate to port {port}.");
        }

        static async Task<string[]> ReadBulkArrayAsync(NetworkStream stream, params string[] args)
        {
            await WriteCommandAsync(stream, args);
            // Read the array header line "*<n>\r\n", then n elements. An element may be
            // a bulk string ($), an integer (:), or a nested array (*); this helper
            // flattens all of them into their string representation so callers can
            // assert on the wire shape.
            var firstLine = await ReadLineAsync(stream);
            if (!firstLine.StartsWith('*')) throw new InvalidOperationException($"Expected array reply, got: {firstLine}");
            var n = int.Parse(firstLine.AsSpan(1));
            var result = new string[n];
            for (var i = 0; i < n; i++)
            {
                var header = await ReadLineAsync(stream);
                switch (header[0])
                {
                    case '$':
                        {
                            var len = int.Parse(header.AsSpan(1));
                            var buf = new byte[len];
                            var got = 0;
                            while (got < len)
                            {
                                var read = await stream.ReadAsync(buf, got, len - got).ConfigureAwait(false);
                                if (read == 0) throw new EndOfStreamException();
                                got += read;
                            }
                            // Consume trailing \r\n
                            await stream.ReadAsync(new byte[2]).ConfigureAwait(false);
                            result[i] = Encoding.UTF8.GetString(buf);
                            break;
                        }
                    case ':':
                        // Integer reply: payload is the number itself, no length, no \r\n.
                        result[i] = header[1..];
                        break;
                    case '*':
                        // Nested array: recurse. Empty array "*0\r\n" -> empty string[0].
                        // Push back the header by re-reading is not supported; for our
                        // purposes the only nested reply we expect is the empty array
                        // for ROLE's third element, so handle it explicitly.
                        if (header == "*0") { result[i] = ""; }
                        else throw new NotSupportedException($"Nested arrays other than *0 are not supported in this test helper, got: {header}");
                        break;
                    default:
                        throw new InvalidOperationException($"Expected element header, got: {header}");
                }
            }
            return result;
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
            catch (OperationCanceledException) { }
            return Encoding.ASCII.GetString(buf.ToArray());
        }
    }
}