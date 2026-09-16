// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Threading;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// A checkpoint that fails must never be reported to clients as a successful save. LASTSAVE must not advance,
    /// SAVE must return an error, and INFO PERSISTENCE must report <c>rdb_last_bgsave_status:err</c>. Otherwise a
    /// client following the documented "BGSAVE then poll LASTSAVE" pattern treats data as durable that was never
    /// written, and the loss only surfaces as an empty store after the next restart.
    /// </summary>
    /// <remarks>
    /// The failure is injected through <see cref="FailingCheckpointDeviceFactoryCreator"/> rather than by making the
    /// filesystem itself fail, so the tests do not depend on path length, file permissions or platform.
    /// </remarks>
    [TestFixture]
    public class CheckpointFailureTests : TestBase
    {
        const string StatusPrefix = "rdb_last_bgsave_status:";
        const string TestKey = "CheckpointFailureTestKey";
        const string TestValue = "CheckpointFailureTestValue";

        static readonly long EpochTicks = DateTimeOffset.FromUnixTimeSeconds(0).Ticks;
        static readonly TimeSpan CheckpointTimeout = TimeSpan.FromSeconds(30);

        GarnetServer server;
        GarnetServerOptions options;
        FailingCheckpointDeviceFactoryCreator deviceFactoryCreator;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = CreateServer(tryRecover: false);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown();
        }

        GarnetServer CreateServer(bool tryRecover)
        {
            // Built through GetGarnetServerOptions rather than CreateGarnetServer because only the options object
            // exposes DeviceFactoryCreator, which is how the checkpoint failure is injected.
            options = TestUtils.GetGarnetServerOptions(
                checkpointDir: TestUtils.MethodTestDir,
                logDir: TestUtils.MethodTestDir,
                endpoint: TestUtils.EndPoint,
                enableCluster: false,
                tryRecover: tryRecover);

            deviceFactoryCreator = new FailingCheckpointDeviceFactoryCreator(options.StoreCheckpointBaseDirectory);
            options.DeviceFactoryCreator = deviceFactoryCreator;

            return new GarnetServer(options);
        }

        [Test]
        public void BackgroundSaveFailureDoesNotAdvanceLastSave()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var redisServer = redis.GetServer(TestUtils.EndPoint);

            db.StringSet(TestKey, TestValue);
            ClassicAssert.AreEqual(EpochTicks, redisServer.LastSave().Ticks, "LASTSAVE should start at the epoch");

            deviceFactoryCreator.ArmCheckpointFailure = true;
            redisServer.Save(SaveType.BackgroundSave);

            // BGSAVE replies before the checkpoint runs, so wait for the outcome to be recorded rather than sleeping
            // for an arbitrary interval.
            WaitForLastSaveStatus(db, "err");

            ClassicAssert.AreEqual(EpochTicks, redisServer.LastSave().Ticks,
                "LASTSAVE advanced for a checkpoint that failed, so a client polling it would treat unwritten data as durable");
            ClassicAssert.AreEqual(0, CountCheckpointFiles(dbId: 0), "A failed checkpoint should not leave checkpoint files behind");
        }

        [Test]
        public void SaveFailureReturnsErrorToClient()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var redisServer = redis.GetServer(TestUtils.EndPoint);

            db.StringSet(TestKey, TestValue);

            deviceFactoryCreator.ArmCheckpointFailure = true;
            var ex = Assert.Throws<RedisServerException>(() => db.Execute("SAVE"));
            ClassicAssert.IsTrue(ex.Message.StartsWith("ERR checkpoint failed", StringComparison.Ordinal),
                $"SAVE should report the failure to the client, but replied: {ex.Message}");

            ClassicAssert.AreEqual(EpochTicks, redisServer.LastSave().Ticks, "LASTSAVE advanced for a failed SAVE");
            ClassicAssert.AreEqual("err", GetLastSaveStatus(db));
            ClassicAssert.AreEqual(0, CountCheckpointFiles(dbId: 0));
        }

        [Test]
        public void SaveSucceedsAfterFailureAndDataSurvivesRestart()
        {
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                var redisServer = redis.GetServer(TestUtils.EndPoint);

                db.StringSet(TestKey, TestValue);

                deviceFactoryCreator.ArmCheckpointFailure = true;
                _ = Assert.Throws<RedisServerException>(() => db.Execute("SAVE"));
                ClassicAssert.AreEqual(EpochTicks, redisServer.LastSave().Ticks);

                // A failed checkpoint must leave the server able to take the next one.
                deviceFactoryCreator.ArmCheckpointFailure = false;
                ClassicAssert.AreEqual("OK", db.Execute("SAVE").ToString());

                ClassicAssert.AreNotEqual(EpochTicks, redisServer.LastSave().Ticks, "LASTSAVE should advance for a successful save");
                ClassicAssert.AreEqual("ok", GetLastSaveStatus(db));
                ClassicAssert.Greater(CountCheckpointFiles(dbId: 0), 0);
            }

            server.Dispose(false);
            server = CreateServer(tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual(TestValue, db.StringGet(TestKey).ToString(),
                    "The successful save should have been recoverable");
            }
        }

        [Test]
        public void BackgroundSaveFailureDoesNotAdvanceLastSaveForAnyDatabase()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db0 = redis.GetDatabase(0);

            // Touching a non-zero database promotes the server to the multi-database manager, which records the
            // last save time through its own code path.
            var db1 = redis.GetDatabase(1);

            db0.StringSet(TestKey, TestValue);
            db1.StringSet(TestKey, TestValue);

            deviceFactoryCreator.ArmCheckpointFailure = true;
            ClassicAssert.AreEqual("Background saving started", db0.Execute("BGSAVE").ToString());

            WaitForLastSaveStatus(db0, "err");
            WaitForLastSaveStatus(db1, "err");

            ClassicAssert.AreEqual(0, (long)db0.Execute("LASTSAVE"), "LASTSAVE advanced for DB 0 after a failed checkpoint");
            ClassicAssert.AreEqual(0, (long)db1.Execute("LASTSAVE"), "LASTSAVE advanced for DB 1 after a failed checkpoint");
            ClassicAssert.AreEqual(0, CountCheckpointFiles(dbId: 0));
            ClassicAssert.AreEqual(0, CountCheckpointFiles(dbId: 1));
        }

        /// <summary>
        /// Number of files written under the given database's log checkpoint directory.
        /// </summary>
        int CountCheckpointFiles(int dbId)
        {
            var namingScheme = new DefaultCheckpointNamingScheme(options.GetStoreCheckpointDirectory(dbId));
            var checkpointDir = Path.Combine(namingScheme.BaseName, namingScheme.LogCheckpointBasePath);

            return Directory.Exists(checkpointDir)
                ? Directory.GetFiles(checkpointDir, "*", SearchOption.AllDirectories).Length
                : 0;
        }

        static string GetLastSaveStatus(IDatabase db)
        {
            var info = db.Execute("INFO", "PERSISTENCE").ToString();
            var line = info.Split("\r\n").FirstOrDefault(x => x.StartsWith(StatusPrefix, StringComparison.Ordinal));
            ClassicAssert.IsNotNull(line, $"INFO PERSISTENCE did not report {StatusPrefix}");

            return line[StatusPrefix.Length..];
        }

        static void WaitForLastSaveStatus(IDatabase db, string expected)
        {
            var deadline = DateTime.UtcNow + CheckpointTimeout;
            while (GetLastSaveStatus(db) != expected && DateTime.UtcNow < deadline)
                Thread.Sleep(10);

            ClassicAssert.AreEqual(expected, GetLastSaveStatus(db),
                $"{StatusPrefix} did not become '{expected}' within {CheckpointTimeout.TotalSeconds} seconds");
        }
    }
}