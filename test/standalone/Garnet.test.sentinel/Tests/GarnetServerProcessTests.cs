// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// Smoke tests for the ProcessWrapper harness itself. These tests do not
// exercise Phase 1 server-side changes; they verify that we can spawn
// GarnetServer, redis-server, and redis-sentinel as subprocesses and talk
// to them over RESP. Each test must call SentinelGate.RequireExternalProcesses
// so that a default build (gates closed) reports them as Ignored rather than
// launching external processes.

using System.Threading.Tasks;
using Garnet.test.sentinel.Fixtures;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.sentinel.Tests
{
    [TestFixture]
    public class GarnetServerProcessTests : TestBase
    {
        [Test]
        public async Task GarnetServer_PingPong()
        {
            SentinelGate.RequireExternalProcesses();

            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var config = new ConfigurationOptions
            {
                EndPoints = { { "127.0.0.1", g.Port } },
                ConnectTimeout = 5000,
                SyncTimeout = 5000,
            };
            using var redis = await ConnectionMultiplexer.ConnectAsync(config);
            var pong = await redis.GetDatabase().PingAsync();
            ClassicAssert.That(pong.TotalMilliseconds, Is.GreaterThanOrEqualTo(0),
                "Ping returned a negative duration; server isn't responding.");
        }

        [Test]
        public async Task RedisServer_PingPong()
        {
            SentinelGate.RequireExternalProcesses();

            using var r = new RedisServerProcess(TestPorts.AllocateFreePort());
            r.Start();

            var config = new ConfigurationOptions
            {
                EndPoints = { { "127.0.0.1", r.Port } },
                ConnectTimeout = 5000,
                SyncTimeout = 5000,
            };
            using var redis = await ConnectionMultiplexer.ConnectAsync(config);
            var pong = await redis.GetDatabase().PingAsync();
            ClassicAssert.That(pong.TotalMilliseconds, Is.GreaterThanOrEqualTo(0));
        }

        [Test]
        public async Task GarnetServer_GetSet_RoundTrip()
        {
            SentinelGate.RequireExternalProcesses();

            using var g = new GarnetServerProcess(TestPorts.AllocateFreePort());
            g.Start();

            var config = new ConfigurationOptions
            {
                EndPoints = { { "127.0.0.1", g.Port } },
                ConnectTimeout = 5000,
                SyncTimeout = 5000,
            };
            using var redis = await ConnectionMultiplexer.ConnectAsync(config);
            var db = redis.GetDatabase();
            const string key = "smoke:k1", value = "hello-from-garnet";

            ClassicAssert.IsTrue(await db.StringSetAsync(key, value));
            var got = await db.StringGetAsync(key);
            ClassicAssert.AreEqual(value, got.ToString());
        }
    }
}
