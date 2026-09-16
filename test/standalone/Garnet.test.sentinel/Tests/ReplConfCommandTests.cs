// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// Phase 1 / test class: REPLCONF.
//
// These tests spawn a Garnet subprocess and verify that REPLCONF behaves per the
// Redis 7.4 wire contract that Redis Sentinel depends on:
//   * accepts an even-length list of key/value pairs (arity >= 2)
//   * always replies +OK, even for unknown keys (forward-compatibility for Sentinel)
//   * rejects zero or odd-arity argument lists with a wrong-number-of-arguments error
//
// All tests gate on SentinelGate.RequireExternalProcesses so the default CI build
// (gates closed) reports them as Ignored.

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

        [Test]
        public void EmptyArgs_ReturnsError()
        {
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var ex = ClassicAssert.ThrowsAsync<System.InvalidOperationException>(
                async () => await ReplConf(g));
            ClassicAssert.That(ex!.Message, Does.Contain("wrong number"),
                $"Expected 'wrong number of arguments' error, got: {ex.Message}");
        }

        [Test]
        public void OddArgs_ReturnsError()
        {
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var ex = ClassicAssert.ThrowsAsync<System.InvalidOperationException>(
                async () => await ReplConf(g, "listening-port"));
            ClassicAssert.That(ex!.Message, Does.Contain("wrong number"),
                $"Expected 'wrong number of arguments' error, got: {ex.Message}");
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
        public async Task AckWithIntOffset_ReturnsOK()
        {
            // Replica -> primary acknowledgement of replication offset.
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var result = await ReplConf(g, "ack", "0");
            ClassicAssert.AreEqual("OK", (string)result!);
        }

        [Test]
        public async Task Getack_ReturnsOK()
        {
            // Primary -> replica request for current offset.
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var result = await ReplConf(g, "getack", "*");
            ClassicAssert.AreEqual("OK", (string)result!);
        }

        [Test]
        public async Task NoOneConnects_ReturnsOK()
        {
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var result = await ReplConf(g, "no-one-connects", "1");
            ClassicAssert.AreEqual("OK", (string)result!);
        }

        [Test]
        public async Task UnknownKey_ReturnsOK()
        {
            // Forward-compatibility: unknown REPLCONF keys must be accepted with +OK
            // so Sentinel can extend its handshake in future versions without breaking
            // older clients.
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var result = await ReplConf(g, "future-key-from-redis-99", "future-value");
            ClassicAssert.AreEqual("OK", (string)result!);
        }

        [Test]
        public async Task UnknownKeyUnknownValue_ReturnsOK()
        {
            SentinelGate.RequireExternalProcesses();
            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var result = await ReplConf(g, "weird key with spaces", "weird value with spaces");
            ClassicAssert.AreEqual("OK", (string)result!);
        }
    }
}
