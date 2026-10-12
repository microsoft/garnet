// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// A ZADD against an expired sorted set that has been evicted to disk completes the RMW through
    /// ContinuePendingRMW. CopyUpdater declines the expired source with ExpireAndResume before populating the
    /// newly allocated record, so that record reaches ReinitializeExpiredRecord's disposal with its objectId slot
    /// still unset.
    /// </summary>
    [TestFixture]
    public class ExpiredDiskRecordRmwTests : TestBase
    {
        GarnetServer server;
        StringWriter serverLog;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            serverLog = new StringWriter();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, logTo: serverLog);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            serverLog?.Dispose();
            serverLog = null;
            TestUtils.OnTearDown();
        }

        [Test]
        public void ZAddOnExpiredDiskEvictedRecord()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const int KeyCount = 200;
            const int MemberCount = 20;

            // Write sorted sets with a short TTL; the volume pushes them out of memory to disk.
            for (var i = 0; i < KeyCount; i++)
            {
                var entries = new SortedSetEntry[MemberCount];
                for (var m = 0; m < MemberCount; m++)
                    entries[m] = new SortedSetEntry($"member-{m}-{new string('p', 32)}", m);
                _ = db.SortedSetAdd($"zs:{i}", entries);
                _ = db.KeyExpire($"zs:{i}", TimeSpan.FromSeconds(2));
            }

            // Let the TTLs lapse; the records are on disk by now.
            Thread.Sleep(TimeSpan.FromSeconds(3));

            // ZADD against expired, disk-resident records. Previously this threw IndexOutOfRangeException out of
            // ProcessMessages, which killed the session: the client saw a reset connection rather than a reply.
            for (var i = 0; i < KeyCount; i++)
            {
                var entries = new SortedSetEntry[MemberCount];
                for (var m = 0; m < MemberCount; m++)
                    entries[m] = new SortedSetEntry($"member2-{m}-{new string('q', 32)}", m);
                _ = db.SortedSetAdd($"zs:{i}", entries);
            }

            // The connection must still be usable, and each key must have been recreated fresh rather than merged.
            Assert.That(db.Ping(), Is.GreaterThan(TimeSpan.Zero));
            Assert.That(db.SortedSetLength("zs:0"), Is.EqualTo(MemberCount));
            Assert.That(serverLog.ToString(), Does.Not.Contain("ProcessMessages threw an exception"));
        }
    }
}