// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Scanning Tsavorite log during a transaction isn't safe, generally, because of locking concerns.
    /// 
    /// Tests here validate that the commands that _do_ scan are implemented in a way that works around those issues.
    /// </summary>
    [TestFixture]
    public class ScanInTransactionTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: true, latencyMonitor: true, metricsSamplingFreq: 1, lowMemory: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        public async Task DBSizeAsync()
        {
#if !DEBUG
            ClassicAssert.Ignore("Deadlocks are lethal in RELEASE builds");
#endif

            var dbSize = (int)await RunInTransactionAsync("a", static db => db.ExecuteAsync("DBSIZE")).ConfigureAwait(false);
            ClassicAssert.AreEqual(1, dbSize);
        }

        [Test]
        public async Task KeysAsync()
        {
#if !DEBUG
            ClassicAssert.Ignore("Deadlocks are lethal in RELEASE builds");
#endif

            var keysRes = (string[])await RunInTransactionAsync("a", static db => db.ExecuteAsync("KEYS", ["*"])).ConfigureAwait(false);
            ClassicAssert.AreEqual(1, keysRes.Length);
            ClassicAssert.AreEqual("a", keysRes[0]);
        }

        [Test]
        public async Task ScanAsync()
        {
#if !DEBUG
            ClassicAssert.Ignore("Deadlocks are lethal in RELEASE builds");
#endif

            var scanRes = (RedisResult[])await RunInTransactionAsync("a", static db => db.ExecuteAsync("SCAN", ["0"])).ConfigureAwait(false);
            ClassicAssert.AreEqual(2, scanRes.Length);
            ClassicAssert.AreEqual(0, (long)scanRes[0]);
            ClassicAssert.AreEqual("a", (string)scanRes[1]);
        }

        [Test]
        public async Task InfoKeyspaceAsync()
        {
#if !DEBUG
            ClassicAssert.Ignore("Deadlocks are lethal in RELEASE builds");
#endif

            var infoRes = (string)await RunInTransactionAsync("a", static db => db.ExecuteAsync("INFO", ["KEYSPACE"])).ConfigureAwait(false);
            ClassicAssert.IsNotNull(infoRes);
        }

        /// <summary>
        /// Helper which sets up transactions, forces a dangerous mutation, runs the provided command, and then reports deadlocks.
        /// 
        /// On no deadlock, returns the result of the queued command.
        /// </summary>
        private static async Task<RedisResult> RunInTransactionAsync(string mutateKey, Func<IDatabase, Task<RedisResult>> queueCommand)
        {
            using var redis = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig(allowAdmin: true)).ConfigureAwait(false);
            try
            {
                var db = redis.GetDatabase(0);

                var startTran = (string)await db.ExecuteAsync("MULTI").ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", startTran);
                var setTran = (string)await db.ExecuteAsync("SET", [mutateKey, "x"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("QUEUED", setTran);

                var commandTran = (string)await queueCommand(db).ConfigureAwait(false);
                ClassicAssert.AreEqual("QUEUED", commandTran);

                var execTask = db.ExecuteAsync("EXEC");
                var timeoutTask = Task.Delay(TimeSpan.FromSeconds(1));

                _ = await Task.WhenAny(execTask, timeoutTask).ConfigureAwait(false);
                ClassicAssert.IsTrue(execTask.IsCompletedSuccessfully, "Transaction deadlocked, EXEC did not complete in time");

                var execTran = (RedisResult[])await execTask.ConfigureAwait(false);
                ClassicAssert.AreEqual(2, execTran.Length);
                ClassicAssert.AreEqual("OK", (string)execTran[0]);

                return execTran[1];
            }
            finally
            {
                redis.Close(allowCommandsToComplete: false);
            }
        }
    }
}
