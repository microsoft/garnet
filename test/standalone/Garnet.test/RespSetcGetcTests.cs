// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Tests for the Garnet-specific SETC / GETC consistency commands.
    /// SETC sets a key and returns the AOF logical address (freshness token) of the write.
    /// GETC reads a key, optionally gating the read on that token (single-log replica only).
    /// On a standalone primary there is no replication to wait for, so GETC reads immediately.
    /// </summary>
    [TestFixture]
    public class RespSetcGetcTests
    {
        GarnetServer server;

        private void StartServer(bool enableAOF)
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: enableAOF);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            TestUtils.OnTearDown();
        }

        [Test]
        public void SetcReturnsMinusOneWhenAofDisabled()
        {
            StartServer(enableAOF: false);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var address = (long)db.Execute("SETC", "foo", "bar");
            ClassicAssert.AreEqual(-1, address, "SETC should report -1 when the write is not logged to the AOF.");

            var value = (string)db.Execute("GET", "foo");
            ClassicAssert.AreEqual("bar", value);
        }

        [Test]
        public void SetcReturnsMonotonicAddressWhenAofEnabled()
        {
            StartServer(enableAOF: true);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var first = (long)db.Execute("SETC", "foo", "bar1");
            ClassicAssert.GreaterOrEqual(first, 0, "SETC should return a valid AOF address when the write is logged.");

            var second = (long)db.Execute("SETC", "foo", "bar2");
            ClassicAssert.Greater(second, first, "Subsequent single-log writes should advance the AOF address.");
        }

        [Test]
        public void GetcReadsValueOnStandalonePrimary([Values] bool enableAOF)
        {
            StartServer(enableAOF);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var address = (long)db.Execute("SETC", "foo", "bar");

            // On a standalone primary there is no replica to wait for, so GETC reads immediately
            // regardless of the supplied token/timeout.
            var value = (string)db.Execute("GETC", "foo", address.ToString(), "1000");
            ClassicAssert.AreEqual("bar", value);
        }

        [Test]
        public void GetcReturnsNullForMissingKey()
        {
            StartServer(enableAOF: false);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var value = db.Execute("GETC", "nosuchkey", "0", "1000");
            ClassicAssert.IsTrue(value.IsNull);
        }

        [Test]
        public void GetcZeroTimeoutMeansNoWaitOnStandalone()
        {
            StartServer(enableAOF: true);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var address = (long)db.Execute("SETC", "foo", "bar");

            // timeout <= 0 means "wait indefinitely", but with no replica the gate is a no-op.
            var value = (string)db.Execute("GETC", "foo", address.ToString(), "0");
            ClassicAssert.AreEqual("bar", value);
        }

        [Test]
        public void SetcWrongNumberOfArgumentsReturnsError()
        {
            StartServer(enableAOF: false);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var ex = Assert.Throws<RedisServerException>(() => db.Execute("SETC", "foo"));
            ClassicAssert.IsTrue(ex.Message.Contains("wrong number of arguments"));
        }

        [Test]
        public void GetcWrongNumberOfArgumentsReturnsError()
        {
            StartServer(enableAOF: false);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var ex = Assert.Throws<RedisServerException>(() => db.Execute("GETC", "foo", "0"));
            ClassicAssert.IsTrue(ex.Message.Contains("wrong number of arguments"));
        }

        [Test]
        public void GetcNonIntegerAddressReturnsError()
        {
            StartServer(enableAOF: false);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var ex = Assert.Throws<RedisServerException>(() => db.Execute("GETC", "foo", "notanumber", "1000"));
            ClassicAssert.IsTrue(ex.Message.Contains("not an integer"));
        }
    }
}