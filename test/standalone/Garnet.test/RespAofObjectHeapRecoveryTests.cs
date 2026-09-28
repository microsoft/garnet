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
    /// AOF recovery of object (collection) data whose heap far exceeds the configured memory budget. Replay is ordinary
    /// store traffic, but the size tracker's background resizer used to be started only after recovery finished, so nothing
    /// could relieve the heap-driven allocation backpressure: replay spun on RETRY_NOW forever and the server never began
    /// accepting connections. Regression for issue #2174.
    /// </summary>
    [TestFixture]
    public class RespAofObjectHeapRecoveryTests : TestBase
    {
        /// <summary>Combined page + object-heap budget. The hashes written below exceed it many times over.</summary>
        const string MemorySize = "64k";
        const string PageSize = "4k";

        const int NumHashes = 500;
        const int ValueSize = 2048;

        /// <summary>Generous relative to the couple of seconds recovery takes when replay is not livelocked.</summary>
        static readonly TimeSpan RecoveryTimeout = TimeSpan.FromMinutes(2);

        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.OnTearDown();
        }

        [Test]
        public void RecoverObjectsExceedingHeapBudget()
        {
            var value = new string('x', ValueSize);

            server = CreateServer(tryRecover: false);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                for (var i = 0; i < NumHashes; i++)
                    db.HashSet($"k:{i}", "data", value);
            }

            // Shut down without deleting the data, leaving the whole AOF to be replayed (no checkpoint was taken).
            server.Dispose(false);
            server = null;

            // Recover on a worker so a regression fails the test instead of wedging the test host. Leave `server` null
            // until Start() returns: disposing a server whose replay is stuck would block on the same allocation.
            var recovered = CreateServer(tryRecover: true);
            var startTask = Task.Run(recovered.Start);
            if (!startTask.Wait(RecoveryTimeout))
                Assert.Fail($"AOF recovery did not complete within {RecoveryTimeout}; replay is waiting on an eviction that cannot happen while the size tracker's resizer is not running.");
            startTask.GetAwaiter().GetResult();     // surface any exception
            server = recovered;

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                for (var i = 0; i < NumHashes; i++)
                    ClassicAssert.AreEqual(value, db.HashGet($"k:{i}", "data").ToString(), $"k:{i} must be recovered from the AOF");
            }
        }

        static GarnetServer CreateServer(bool tryRecover)
            => TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, commitWait: true, tryRecover: tryRecover,
                    memorySize: MemorySize, pageSize: PageSize, failOnRecoveryError: true);
    }
}