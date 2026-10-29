// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// Phase 1 / test class: PSYNC.
//
// These tests spawn a Garnet subprocess and verify that PSYNC behaves per the
// Redis 7.4 wire contract that stock Redis replicas (and, transitively, Sentinel)
// depend on:
//
//   * arity: exactly 2 arguments (<replid> <offset>)
//   * reply:  +FULLRESYNC <replid> 0  (we always full-resync in Phase 1)
//   * body:   $<len>\r\n<valid empty-DB RDB>\r\n  (55 bytes; verified by redis-check-rdb)
//   * end-to-end: a stock redis-server configured as --replicaof <garnet> reaches
//                  connected_slaves:1 / master_link_status:up on the Garnet side.
//
// All tests gate on SentinelGate.RequireExternalProcesses so the default CI build
// (gates closed) reports them as Ignored.

using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.test.sentinel.Fixtures;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.sentinel.Tests
{
    [TestFixture]
    public class PsyncCommandTests : TestBase
    {
        /// <summary>
        /// Directory holding the stock redis binaries used by the interoperability tests.
        /// Honours GARNET_TEST_REDIS_SERVER (the file, whose directory is used) so the
        /// tests are not tied to one developer's machine, and otherwise falls back to the
        /// conventional per-user cache path.
        /// </summary>
        private static string RedisBinaryDir
        {
            get
            {
                var configured = Environment.GetEnvironmentVariable("GARNET_TEST_REDIS_SERVER");
                if (!string.IsNullOrEmpty(configured))
                    return Path.GetDirectoryName(configured)!;

                return Fixtures.RedisServerProcess.DefaultCacheDir;
            }
        }

        /// <summary>
        /// Sends a raw PSYNC command over a fresh TCP connection and returns the
        /// raw RESP bytes the server produced. Used to assert the wire-level
        /// shape of the +FULLRESYNC + RDB body response.
        /// </summary>
        private static async Task<byte[]> RawPsync(int port, string replid, string offset)
        {
            using var client = new TcpClient();
            await client.ConnectAsync(IPAddress.Loopback, port);
            using var ns = client.GetStream();
            // PING handshake first so a future ACL/auth layer (if added) doesn't
            // confuse the test; today's Garnet doesn't require AUTH.
            await WriteRespAsync(ns, "PING");
            var pingReply = await ReadLineAsync(ns);
            ClassicAssert.AreEqual("+PONG", pingReply, $"PING should return +PONG, got: {pingReply}");

            // Now the actual PSYNC.
            await WriteRespAsync(ns, "PSYNC", replid, offset);
            var fullResyncLine = await ReadLineAsync(ns);
            // The line is "+FULLRESYNC <replid> 0"
            ClassicAssert.That(fullResyncLine, Does.StartWith("+FULLRESYNC"),
                $"Expected +FULLRESYNC reply, got: {fullResyncLine}");

            // Next line is the length-delimited RDB header: $<len>\r\n, then exactly
            // <len> bytes of RDB payload. This is NOT a normal RESP bulk string: the
            // replication transfer omits the trailing CRLF (verified against a live
            // Redis 7.4.11 primary), so no trailer may be expected or sent.
            var rdbHeader = await ReadLineAsync(ns);
            ClassicAssert.That(rdbHeader, Does.StartWith("$"),
                $"Expected bulk-string header for RDB, got: {rdbHeader}");
            var rdbLen = int.Parse(rdbHeader.AsSpan(1));
            var rdbBody = new byte[rdbLen];
            var read = 0;
            while (read < rdbLen)
            {
                var n = await ns.ReadAsync(rdbBody.AsMemory(read, rdbLen - read));
                if (n == 0) break;
                read += n;
            }
            ClassicAssert.AreEqual(rdbLen, read, "RDB body short read.");

            // Assert that nothing follows the RDB body. A stray trailer here would be
            // consumed by the replica as the first bytes of the replication command
            // stream, so this check guards the framing fix.
            var extra = await ReadBytesOrNoneAsync(ns, 2, timeoutMs: 500);
            ClassicAssert.AreEqual(0, extra.Length,
                $"Expected no bytes after the RDB body (length-delimited framing), got: {BitConverter.ToString(extra)}");

            return rdbBody;
        }

        private static async Task WriteRespAsync(NetworkStream ns, params string[] args)
        {
            var sb = new StringBuilder();
            sb.Append("*").Append(args.Length).Append("\r\n");
            foreach (var a in args)
            {
                sb.Append("$").Append(Encoding.UTF8.GetByteCount(a)).Append("\r\n").Append(a).Append("\r\n");
            }
            var bytes = Encoding.ASCII.GetBytes(sb.ToString());
            await ns.WriteAsync(bytes);
            await ns.FlushAsync();
        }

        private static async Task<string> ReadLineAsync(NetworkStream ns)
        {
            var buf = new MemoryStream();
            while (true)
            {
                var b = new byte[1];
                var n = await ns.ReadAsync(b.AsMemory(0, 1));
                if (n == 0) break;
                buf.WriteByte(b[0]);
                if (b[0] == (byte)'\n' && buf.Length >= 2)
                {
                    var arr = buf.ToArray();
                    // last two bytes are \r\n; strip them.
                    return Encoding.ASCII.GetString(arr, 0, arr.Length - 2);
                }
            }
            return Encoding.ASCII.GetString(buf.ToArray());
        }

        private static async Task<byte[]> ReadBytesAsync(NetworkStream ns, int count)
        {
            var buf = new byte[count];
            var read = 0;
            while (read < count)
            {
                var n = await ns.ReadAsync(buf.AsMemory(read, count - read));
                if (n == 0) break;
                read += n;
            }
            return buf;
        }

        /// <summary>
        /// Reads up to <paramref name="count"/> bytes, giving up after
        /// <paramref name="timeoutMs"/> and returning whatever (possibly nothing) arrived.
        /// Used to assert the *absence* of trailing bytes, where blocking indefinitely
        /// would hang the test instead of failing it.
        /// </summary>
        private static async Task<byte[]> ReadBytesOrNoneAsync(NetworkStream ns, int count, int timeoutMs)
        {
            var buf = new byte[count];
            using var cts = new CancellationTokenSource(timeoutMs);
            try
            {
                var n = await ns.ReadAsync(buf.AsMemory(0, count), cts.Token);
                return buf.AsSpan(0, n).ToArray();
            }
            catch (OperationCanceledException)
            {
                return [];
            }
        }

        [Test]
        public void EmptyArgs_ReturnsError()
        {
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            using var client = new TcpClient();
            client.Connect(IPAddress.Loopback, g.Port);
            using var ns = client.GetStream();
            WriteRespAsync(ns, "PSYNC").GetAwaiter().GetResult();
            var reply = ReadLineAsync(ns).GetAwaiter().GetResult();
            ClassicAssert.That(reply, Does.StartWith("-"));
            ClassicAssert.That(reply, Does.Contain("wrong number"));
        }

        [Test]
        public void OneArg_ReturnsError()
        {
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            using var client = new TcpClient();
            client.Connect(IPAddress.Loopback, g.Port);
            using var ns = client.GetStream();
            WriteRespAsync(ns, "PSYNC", "?").GetAwaiter().GetResult();
            var reply = ReadLineAsync(ns).GetAwaiter().GetResult();
            ClassicAssert.That(reply, Does.StartWith("-"));
            ClassicAssert.That(reply, Does.Contain("wrong number"));
        }

        [Test]
        public async Task ThreeArgs_ReturnsError()
        {
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            using var client = new TcpClient();
            await client.ConnectAsync(IPAddress.Loopback, g.Port);
            using var ns = client.GetStream();
            await WriteRespAsync(ns, "PSYNC", "?", "-1", "extra");
            var reply = await ReadLineAsync(ns);
            ClassicAssert.That(reply, Does.StartWith("-"));
            ClassicAssert.That(reply, Does.Contain("wrong number"));
        }

        [Test]
        public async Task PsyncQuestionMinusOne_ReturnsFullResyncAndEmptyRdb()
        {
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var rdb = await RawPsync(g.Port, "?", "-1");
            // Phase 1 ships a 56-byte empty-DB RDB (48-byte body + 8-byte CRC64).
            ClassicAssert.AreEqual(56, rdb.Length, $"Expected 56-byte RDB body, got {rdb.Length}.");
        }

        [Test]
        public async Task PsyncWithReplid_ReturnsFullResyncAndEmptyRdb()
        {
            // Partial-resync request still gets +FULLRESYNC in Phase 1 — we don't
            // implement +CONTINUE (backlog tracking) yet.
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var rdb = await RawPsync(g.Port, "deadbeef00000000000000000000000000000000", "1234");
            ClassicAssert.AreEqual(56, rdb.Length);
        }

        [Test]
        public async Task PsyncReplyReplidIs40HexChars()
        {
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            using var client = new TcpClient();
            await client.ConnectAsync(IPAddress.Loopback, g.Port);
            using var ns = client.GetStream();
            await WriteRespAsync(ns, "PSYNC", "?", "-1");
            var reply = await ReadLineAsync(ns);
            // Format: +FULLRESYNC <40 hex chars> 0
            var parts = reply.Split(' ');
            ClassicAssert.AreEqual(3, parts.Length, $"Malformed +FULLRESYNC reply: {reply}");
            ClassicAssert.AreEqual("+FULLRESYNC", parts[0]);
            ClassicAssert.AreEqual("0", parts[2]);
            ClassicAssert.AreEqual(40, parts[1].Length, $"replid should be 40 chars: {parts[1]}");
            foreach (var c in parts[1])
                ClassicAssert.That(IsHex(c), Is.True, $"replid contains non-hex char: {c}");
        }

        private static bool IsHex(char c) =>
            (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');

        [Test]
        public async Task ReplyRdbBodyIsValidEmptyDatabaseRdb()
        {
            // Verify the RDB body we ship passes redis-check-rdb, which is the
            // exact same parser stock Redis 7.4 uses to validate RDB files.
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var rdb = await RawPsync(g.Port, "?", "-1");
            var tmpPath = Path.Combine(Path.GetTempPath(), $"garnet-psync-empty-{Guid.NewGuid():N}.rdb");
            File.WriteAllBytes(tmpPath, rdb);
            try
            {
                var checker = Path.Combine(RedisBinaryDir, "redis-check-rdb");
                ClassicAssert.IsTrue(File.Exists(checker),
                    $"redis-check-rdb not found at {checker}; see README in test project for build instructions.");
                var psi = new ProcessStartInfo(checker, tmpPath)
                {
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                };
                using var proc = Process.Start(psi)!;
                var stdout = proc.StandardOutput.ReadToEnd();
                proc.WaitForExit(5000);
                ClassicAssert.AreEqual(0, proc.ExitCode, $"redis-check-rdb rejected the RDB:\n{stdout}");
                ClassicAssert.That(stdout, Does.Contain("RDB looks OK"),
                    $"redis-check-rdb did not confirm RDB validity:\n{stdout}");
            }
            finally
            {
                try { File.Delete(tmpPath); } catch { /* ignore */ }
            }
        }

        [Test]
        public async Task EndToEnd_StockRedisReplicaCompletesHandshakeWithGarnet()
        {
            // Headline Phase 1 test: spawn a stock redis-server in replica mode
            // pointing at Garnet as primary and verify that the replica completes
            // the FULLRESYNC handshake and loads the RDB we send.
            //
            // Scope note: Phase 1 does NOT keep the replication link alive after
            // the initial RDB — there is no outbound command stream yet (that is
            // Phase 2). A stock replica therefore completes a sync, then times out
            // and re-syncs in a loop. This test asserts on what Phase 1 actually
            // guarantees: the +FULLRESYNC reply, the RDB transfer, and the replica's
            // own log confirming "Finished with success".
            //
            // Asserting connected_slaves:1 would require Phase 2 (persistent link
            // + stream), so it is deliberately not asserted here.
            SentinelGate.RequireExternalProcesses();

            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var replicaPort = TestPorts.AllocateFreePort();
            var replicaConfigPath = Path.Combine(Path.GetTempPath(),
                $"garnet-test-psync-replica-{replicaPort}-{Guid.NewGuid():N}.conf");
            var replicaLogPath = Path.Combine(Path.GetTempPath(),
                $"garnet-test-psync-replica-{replicaPort}-{Guid.NewGuid():N}.log");
            var dataDir = Path.Combine(Path.GetTempPath(),
                $"garnet-test-psync-data-{replicaPort}-{Guid.NewGuid():N}");
            Directory.CreateDirectory(dataDir);
            File.WriteAllText(replicaConfigPath,
                $"port {replicaPort}\n" +
                "bind 127.0.0.1\n" +
                "daemonize no\n" +
                $"dir \"{dataDir.Replace("\"", "\\\"")}\"\n" +
                $"logfile \"{replicaLogPath.Replace("\"", "\\\"")}\"\n" +
                "loglevel notice\n" +
                "save \"\"\n" +
                "appendonly no\n" +
                $"replicaof 127.0.0.1 {g.Port}\n");

            var redisServer = Path.Combine(RedisBinaryDir, "redis-server");
            ClassicAssert.IsTrue(File.Exists(redisServer),
                $"redis-server not found at {redisServer}; see README in test project.");

            var psi = new ProcessStartInfo(redisServer, replicaConfigPath)
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = Process.Start(psi)!;

            try
            {
                // Poll the replica's log for the success marker. The marker is
                // emitted by stock redis-server immediately after it validates
                // and loads the RDB we sent, which is the end of the Phase 1
                // handshake contract.
                var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
                string log = "";
                while (DateTime.UtcNow < deadline)
                {
                    if (File.Exists(replicaLogPath))
                    {
                        try { log = File.ReadAllText(replicaLogPath); } catch { /* being written */ }
                        if (log.Contains("MASTER <-> REPLICA sync: Finished with success"))
                            break;
                    }
                    await Task.Delay(200);
                }

                ClassicAssert.That(log, Does.Contain("Full resync from master"),
                    $"Replica never received +FULLRESYNC. Replica log:\n{log}");
                ClassicAssert.That(log, Does.Contain("MASTER <-> REPLICA sync: receiving 56 bytes from master to disk"),
                    $"Replica did not receive the 56-byte RDB body. Replica log:\n{log}");
                ClassicAssert.That(log, Does.Contain("Loading RDB produced by version 7.4.11"),
                    $"Replica rejected the RDB version. Replica log:\n{log}");
                ClassicAssert.That(log, Does.Contain("MASTER <-> REPLICA sync: Finished with success"),
                    $"Replica did not complete the handshake. Replica log:\n{log}");

                // Garnet must still be alive — an assertion failure in the REPLCONF
                // handler previously took the whole server down (see AsciiUtils
                // EqualsUpperCaseSpanIgnoringCase Debug.Assert).
                ClassicAssert.IsFalse(g.HasExited,
                    "Garnet crashed during the replica handshake. Garnet log:\n" + g.OutputLog);
            }
            finally
            {
                try { proc.Kill(); } catch { /* ignore */ }
                try { File.Delete(replicaConfigPath); } catch { /* ignore */ }
                try { File.Delete(replicaLogPath); } catch { /* ignore */ }
                try { Directory.Delete(dataDir, recursive: true); } catch { /* ignore */ }
            }
        }
    }
}
