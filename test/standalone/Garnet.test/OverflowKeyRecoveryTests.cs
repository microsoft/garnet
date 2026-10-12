// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Issue 2164: a non-inline (overflow) key could be checkpointed but not recovered. Recovery Pass 1 builds the
    /// index from an address-only LogRecord that has no ObjectIdMap, so hashing an overflow key dereferenced a null
    /// map and threw NullReferenceException out of RecoverFromPage.
    /// </summary>
    [TestFixture]
    public class OverflowKeyRecoveryTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.OnTearDown();
        }

        [Test]
        public void RecoverCheckpointWithOverflowKey([Values(1024, 65536)] int keyLength)
        {
            var overflowKey = new string('k', keyLength);

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                _ = db.StringSet("seed", "value");
                _ = db.Execute("SAVE");

                _ = db.HashSet(overflowKey, "field", "value");
                _ = db.StringSet(overflowKey + ":str", "strvalue");
                _ = db.Execute("SAVE");
            }

            // Restart and recover: this is where RecoverFromPage threw.
            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, failOnRecoveryError: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                Assert.That((string)db.StringGet("seed"), Is.EqualTo("value"));
                Assert.That((string)db.HashGet(overflowKey, "field"), Is.EqualTo("value"));
                Assert.That((string)db.StringGet(overflowKey + ":str"), Is.EqualTo("strvalue"));
            }
        }
    }
}