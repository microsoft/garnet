// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// Phase 1 / test class: REPLCONF.
//
// These tests spawn a Garnet subprocess and verify that REPLCONF behaves per the
// Redis 7.4 wire contract that Redis Sentinel depends on. Every expectation here
// was established by probing a live Redis 7.4.11 server and recording its reply:
//   * REPLCONF            -> +OK            (arity is -1; bare is legal)
//   * REPLCONF k v        -> +OK
//   * REPLCONF k          -> -ERR syntax error        (odd pairing)
//   * REPLCONF ACK n      -> (no reply at all)
//   * REPLCONF GETACK *   -> (no reply at all)
//   * REPLCONF <unknown>  -> -ERR Unrecognized REPLCONF option: <key>
//
// All tests gate on SentinelGate.RequireExternalProcesses so the default CI build
// (gates closed) reports them as Ignored.

using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Garnet.test.sentinel.Fixtures;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.sentinel.Tests
{
    [TestFixture]
    public class ReplConfCommandTests : TestBase
    {
        /// <summary>
        /// Opens a raw multiplexed connection to a Garnet subprocess and issues
        /// REPLCONF directly, returning the result as a RedisResult so tests can
        /// assert on the wire-level reply (e.g. "+OK" vs error).
        /// </summary>
        private static async Task<RedisResult> ReplConf(GarnetServerProcess g, params string[] args)
        {
            var config = new ConfigurationOptions
            {
                EndPoints = { { "127.0.0.1", g.Port } },
                ConnectTimeout = 5000,
                SyncTimeout = 5000,
                AllowAdmin = true,
            };
            using var redis = await ConnectionMultiplexer.ConnectAsync(config);
            // StackExchange.Redis exposes ExecuteAsync(command, params object[] args).
            // Variable-length REPLCONF arguments go through the params overload.
            // Server error replies surface as RedisServerException; the message is the
            // raw "-ERR ..." string returned by the server, which is what the negative
            // tests assert on. We re-throw as a regular exception so the assert path
            // can extract the original reply text without unwrapping a Redis type.
            var db = redis.GetDatabase();
            try
            {
                return await db.ExecuteAsync("REPLCONF", args);
            }
            catch (RedisServerException ex)
            {
                // Convert into a synthetic RedisResult-style payload so callers can
                // use a single uniform assertion path.
                throw new System.InvalidOperationException(
                    "REPLCONF returned a server error: " + ex.Message,
                    ex);
            }
        }

        /// <summary>
        /// Sends REPLCONF over a bare TCP socket and returns the raw reply bytes as a
        /// string, without waiting for a reply that may never come.
        ///
        /// <para>Needed for the no-reply forms (ACK / GETACK): StackExchange.Redis would
        /// block until its sync timeout and surface a failure, whereas a raw socket lets
        /// us assert that the server genuinely sent nothing.</para>
        /// </summary>
        private static async Task<string> RawReplConf(GarnetServerProcess g, params string[] args)
        {
            using var client = new TcpClient();
            await client.ConnectAsync("127.0.0.1", g.Port);
            await using var stream = client.GetStream();

            // Encode as a RESP array of bulk strings. Note the array header counts
            // REPLCONF itself in addition to the supplied options, and the bulk length is
            // derived from the token rather than hardcoded.
            const string Command = "REPLCONF";
            var sb = new StringBuilder();
            sb.Append('*').Append(args.Length + 1).Append("\r\n");
            sb.Append('$').Append(Encoding.ASCII.GetByteCount(Command)).Append("\r\n").Append(Command).Append("\r\n");
            foreach (var a in args)
            {
                sb.Append('$').Append(Encoding.UTF8.GetByteCount(a)).Append("\r\n").Append(a).Append("\r\n");
            }

            var request = Encoding.ASCII.GetBytes(sb.ToString());
            await stream.WriteAsync(request);
            await stream.FlushAsync();

            // Give the server a generous window to respond. For a no-reply command we
            // expect to read zero bytes; for a bug that returns +OK we would read 5.
            var buffer = new byte[512];
            var readTask = stream.ReadAsync(buffer, 0, buffer.Length);
            var completed = await Task.WhenAny(readTask, Task.Delay(1000)).ConfigureAwait(false);
            if (completed != readTask)
                return string.Empty;

            var n = await readTask.ConfigureAwait(false);
            return n == 0 ? string.Empty : Encoding.ASCII.GetString(buffer, 0, n);
        }

        [Test]
        public async Task EmptyArgs_ReturnsOK()
        {
            // Redis accepts a bare REPLCONF: arity is -1, and the implementation only
            // rejects an odd number of option arguments. Verified against 7.4.11.
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var result = await ReplConf(g);
            ClassicAssert.AreEqual("OK", (string)result!);
        }

        [Test]
        public void OddArgs_ReturnsSyntaxError()
        {
            // A lone option name is an odd pairing. Redis rejects this with
            // "-ERR syntax error" (not an arity error, because arity is -1).
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var ex = ClassicAssert.ThrowsAsync<System.InvalidOperationException>(
                async () => await ReplConf(g, "listening-port"));
            ClassicAssert.That(ex!.Message, Does.Contain("syntax error"),
                $"Expected 'syntax error', got: {ex.Message}");
        }

        [Test]
        public async Task SingleListeningPort_ReturnsOK()
        {
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var result = await ReplConf(g, "listening-port", "6379");
            var s = (string)result!;
            ClassicAssert.That(s, Does.Not.StartWith("-"), $"REPLCONF listening-port should return +OK, got error: {s}");
            ClassicAssert.AreEqual("OK", s);
        }

        [Test]
        public async Task SingleIpAddress_ReturnsOK()
        {
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var result = await ReplConf(g, "ip-address", "10.0.0.5");
            ClassicAssert.AreEqual("OK", (string)result!);
        }

        [Test]
        public async Task CapaEof_ReturnsOK()
        {
            // Sentinel sends REPLCONF capa eof during handshake.
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var result = await ReplConf(g, "capa", "eof");
            ClassicAssert.AreEqual("OK", (string)result!);
        }

        [Test]
        public async Task CapaPsync2_ReturnsOK()
        {
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var result = await ReplConf(g, "capa", "psync2");
            ClassicAssert.AreEqual("OK", (string)result!);
        }

        [Test]
        public async Task MultiplePairs_ReturnsOK()
        {
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            // Sentinel's typical opening handshake, abbreviated.
            var result = await ReplConf(
                g,
                "listening-port", "6379",
                "ip-address", "127.0.0.1",
                "capa", "eof",
                "capa", "psync2");
            ClassicAssert.AreEqual("OK", (string)result!);
        }

        [Test]
        public async Task AckIsNoReply()
        {
            // REPLCONF ACK is a no-reply form: Redis sends nothing back, because a reply
            // would inject unsolicited bytes into the replication command stream.
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var raw = await RawReplConf(g, "ack", "0");
            ClassicAssert.AreEqual(string.Empty, raw,
                $"REPLCONF ACK must produce no reply, got: {raw}");
        }

        [Test]
        public async Task GetackIsNoReply()
        {
            // REPLCONF GETACK is likewise a no-reply form.
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var raw = await RawReplConf(g, "getack", "*");
            ClassicAssert.AreEqual(string.Empty, raw,
                $"REPLCONF GETACK must produce no reply, got: {raw}");
        }

        [Test]
        public void UnknownKey_ReturnsUnrecognizedError()
        {
            // Redis does NOT accept unknown REPLCONF options for forward compatibility;
            // it reports "-ERR Unrecognized REPLCONF option: <key>". Verified on 7.4.11.
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var ex = ClassicAssert.ThrowsAsync<System.InvalidOperationException>(
                async () => await ReplConf(g, "future-key-from-redis-99", "future-value"));
            ClassicAssert.That(ex!.Message, Does.Contain("Unrecognized REPLCONF option"),
                $"Expected 'Unrecognized REPLCONF option', got: {ex.Message}");
        }

        [Test]
        public void UnknownKeyUnknownValue_ReturnsUnrecognizedError()
        {
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var ex = ClassicAssert.ThrowsAsync<System.InvalidOperationException>(
                async () => await ReplConf(g, "weird key with spaces", "weird value with spaces"));
            ClassicAssert.That(ex!.Message, Does.Contain("Unrecognized REPLCONF option"),
                $"Expected 'Unrecognized REPLCONF option', got: {ex.Message}");
        }

        [Test]
        public async Task RdbOnly_ReturnsOK()
        {
            // rdb-only is a recognised REPLCONF option in Redis 7.4 with an integer value.
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var result = await ReplConf(g, "rdb-only", "0");
            ClassicAssert.AreEqual("OK", (string)result!);
        }

        [Test]
        public void NoOneConnectsIsNotARedisOption()
        {
            // Guard against a plausible-looking but fictional option: Redis 7.4 rejects
            // "no-one-connects" as unrecognized. An earlier revision of this handler
            // wrongly whitelisted it.
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var ex = ClassicAssert.ThrowsAsync<System.InvalidOperationException>(
                async () => await ReplConf(g, "no-one-connects", "1"));
            ClassicAssert.That(ex!.Message, Does.Contain("Unrecognized REPLCONF option"),
                $"Expected 'no-one-connects' to be rejected, got: {ex.Message}");
        }
    }
}
