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
    /// Covers isolation of per-database tiered storage: each logical database must own its main-log
    /// and object-log devices so that a record evicted from memory can never be read back as another
    /// database's record.
    /// </summary>
    [TestFixture]
    public class MultiDatabaseStorageTierTests : TestBase
    {
        const int NumKeys = 300;
        const int NumDbs = 2;

        // Raw-string values must exceed the low-memory log budget in aggregate, otherwise every record
        // stays resident and the main-log device is never exercised.
        const int StringPadding = 256;

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

        static string Val(int dbId, int key) => $"db{dbId}-v{key:D5}";

        static string StrVal(int dbId, int key) => Val(dbId, key) + new string('x', StringPadding);

        static string StoreDir => Path.Combine(new DirectoryInfo(TestUtils.MethodTestDir).FullName, "Store");

        /// <summary>
        /// Returns the number of values that matched this database's own writes and the number that
        /// matched another database's writes.
        /// </summary>
        static (int Correct, int Foreign) CountValues(IDatabase db, int dbId, Func<IDatabase, int, string> read, Func<int, int, string> expected)
        {
            int correct = 0, foreign = 0;
            for (var key = 0; key < NumKeys; key++)
            {
                var got = read(db, key);
                if (got == expected(dbId, key))
                    ++correct;
                else if (Enumerable.Range(0, NumDbs).Any(other => other != dbId && got == expected(other, key)))
                    ++foreign;
            }
            return (correct, foreign);
        }

        static string ReadHash(IDatabase db, int key) => db.HashGet($"h:{key}", "f");

        static string ReadString(IDatabase db, int key) => db.StringGet($"s:{key}");

        static void WriteHashes(IDatabase db, int dbId)
        {
            for (var key = 0; key < NumKeys; key++)
                db.HashSet($"h:{key}", [new HashEntry("f", Val(dbId, key))]);
        }

        static void WriteStrings(IDatabase db, int dbId)
        {
            for (var key = 0; key < NumKeys; key++)
                db.StringSet($"s:{key}", StrVal(dbId, key));
        }

        /// <summary>
        /// Captures formatted log messages so a test can assert on what recovery reported.
        /// </summary>
        static void AssertNoCrossDatabaseReads(ConnectionMultiplexer redis, Func<IDatabase, int, string> read, Func<int, int, string> expected)
        {
            for (var dbId = 0; dbId < NumDbs; dbId++)
            {
                var (correct, foreign) = CountValues(redis.GetDatabase(dbId), dbId, read, expected);
                TestContext.Progress.WriteLine($"db{dbId}: correct={correct} foreign={foreign}");
                ClassicAssert.AreEqual(0, foreign, $"db{dbId} returned another database's values");
                ClassicAssert.AreEqual(NumKeys, correct, $"db{dbId} lost records");
            }
        }

        /// <summary>
        /// Database 0's log device names are part of the on-disk contract: disk-based cluster replication
        /// derives them independently on the primary and the replica, and stores written before
        /// per-database logs must still recover. A rename here silently breaks both.
        /// </summary>
        [Test]
        public void DefaultDatabaseLogFileNamesAreStable()
        {
            ClassicAssert.AreEqual("hlog", GarnetServerOptions.GetHybridLogFileName(0, isObj: false));
            ClassicAssert.AreEqual("hlog_objs", GarnetServerOptions.GetHybridLogFileName(0, isObj: true));
            ClassicAssert.AreEqual("rangeindex", GarnetServerOptions.GetRangeIndexDirectoryName(0));

            ClassicAssert.AreEqual("hlog_1", GarnetServerOptions.GetHybridLogFileName(1, isObj: false));
            ClassicAssert.AreEqual("hlog_objs_1", GarnetServerOptions.GetHybridLogFileName(1, isObj: true));
            ClassicAssert.AreEqual("rangeindex_1", GarnetServerOptions.GetRangeIndexDirectoryName(1));
        }

        /// <summary>
        /// Object records spill to the object log. lowMemory=true forces eviction to the log device;
        /// lowMemory=false keeps everything resident. Only the evicting case can expose a shared device.
        /// </summary>
        [Test]
        public void DatabasesMustNotShareObjectLogDevice([Values(false, true)] bool lowMemory)
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: lowMemory);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            for (var dbId = 0; dbId < NumDbs; dbId++)
                WriteHashes(redis.GetDatabase(dbId), dbId);

            AssertNoCrossDatabaseReads(redis, ReadHash, Val);
        }

        /// <summary>
        /// Same shape as the object-log case, but with raw strings so the main log is exercised.
        /// </summary>
        [Test]
        public void DatabasesMustNotShareMainLogDevice([Values(false, true)] bool lowMemory)
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: lowMemory);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            for (var dbId = 0; dbId < NumDbs; dbId++)
                WriteStrings(redis.GetDatabase(dbId), dbId);

            AssertNoCrossDatabaseReads(redis, ReadString, StrVal);
        }

        /// <summary>
        /// Each database must own its log segment files, mirroring the per-database checkpoint and AOF
        /// directories. Database 0 keeps the unsuffixed names so existing stores recover unchanged.
        /// </summary>
        [Test]
        public void PerDatabaseLogFilesAreDistinct()
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                for (var dbId = 0; dbId < NumDbs; dbId++)
                {
                    WriteStrings(redis.GetDatabase(dbId), dbId);
                    WriteHashes(redis.GetDatabase(dbId), dbId);
                }
            }

            var files = Directory.Exists(StoreDir)
                ? Directory.GetFiles(StoreDir).Select(Path.GetFileName).ToArray()
                : [];
            TestContext.Progress.WriteLine($"{StoreDir}: {string.Join(", ", files.OrderBy(f => f))}");

            foreach (var expected in new[] { "hlog.0", "hlog_objs.0", "hlog_1.0", "hlog_objs_1.0" })
                ClassicAssert.Contains(expected, files, $"Missing log segment {expected}");
        }

        /// <summary>
        /// FLUSHDB with UNSAFETRUNCATELOG removes on-disk segments below the flushed database's new
        /// begin address. With per-database devices that can only affect the flushed database.
        /// </summary>
        [Test]
        public void FlushDatabaseDoesNotDiscardOtherDatabaseTieredData()
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            for (var dbId = 0; dbId < NumDbs; dbId++)
                WriteStrings(redis.GetDatabase(dbId), dbId);

            var db0 = redis.GetDatabase(0);
            ClassicAssert.AreEqual("OK", (string)db0.Execute("FLUSHDB", "SYNC", "UNSAFETRUNCATELOG"));

            var (correct, foreign) = CountValues(redis.GetDatabase(1), 1, ReadString, StrVal);
            TestContext.Progress.WriteLine($"after FLUSHDB on db0 -> db1: correct={correct} foreign={foreign}");
            ClassicAssert.AreEqual(0, foreign, "db1 returned another database's values after db0 was flushed");
            ClassicAssert.AreEqual(NumKeys, correct, "db1 lost records when db0 was flushed");
        }

        /// <summary>
        /// A checkpoint records addresses into the log device the store actually wrote, so recovery is
        /// only correct when that device belongs to the database alone.
        /// </summary>
        [Test]
        public void MultiDatabaseTieredSaveRecoverTest()
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                for (var dbId = 0; dbId < NumDbs; dbId++)
                {
                    WriteStrings(redis.GetDatabase(dbId), dbId);
                    WriteHashes(redis.GetDatabase(dbId), dbId);
                }

                var garnetServer = redis.GetServer(TestUtils.EndPoint);
                garnetServer.Save(SaveType.BackgroundSave);
                while (garnetServer.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks)
                    Thread.Sleep(10);
            }

            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                AssertNoCrossDatabaseReads(redis, ReadString, StrVal);
                AssertNoCrossDatabaseReads(redis, ReadHash, Val);
            }
        }

        /// <summary>
        /// A swap relabels two databases without moving their files, so the mapping recorded in the
        /// checkpoint is the only thing that can restore the labels. Without it, recovery would read
        /// the directory names and hand each store back its pre-swap id.
        /// </summary>
        [Test]
        public void SwapDbSurvivesSaveAndRecover()
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                for (var dbId = 0; dbId < NumDbs; dbId++)
                    WriteStrings(redis.GetDatabase(dbId), dbId);

                ClassicAssert.AreEqual("OK", (string)redis.GetDatabase(0).Execute("SWAPDB", 0, 1));

                var garnetServer = redis.GetServer(TestUtils.EndPoint);
                garnetServer.Save(SaveType.BackgroundSave);
                while (garnetServer.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks)
                    Thread.Sleep(10);
            }

            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, tryRecover: true);
            server.Start();

            using var redis2 = ConnectionMultiplexer.Connect(TestUtils.GetConfig());

            // After the swap, database 0 holds what database 1 wrote, and vice versa.
            var (db0Correct, _) = CountValues(redis2.GetDatabase(0), 1, ReadString, StrVal);
            var (db1Correct, _) = CountValues(redis2.GetDatabase(1), 0, ReadString, StrVal);
            TestContext.Progress.WriteLine($"db0 holding db1's data: {db0Correct}/{NumKeys}; db1 holding db0's data: {db1Correct}/{NumKeys}");

            ClassicAssert.AreEqual(NumKeys, db0Correct, "Database 0 did not recover the records written to database 1 before the swap");
            ClassicAssert.AreEqual(NumKeys, db1Correct, "Database 1 did not recover the records written to database 0 before the swap");
        }

        /// <summary>
        /// The persisted mapping and the directories on disk can disagree, because a mapping names slots
        /// that may since have been removed. The mapping must still be honored for the slots that remain:
        /// falling back to slot == id would recover a surviving slot's records under the index it
        /// carried <em>before</em> the swap, silently handing one database another's data.
        /// </summary>
        [Test]
        public void MappingIsHonoredWhenASlotIsMissing()
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                for (var dbId = 0; dbId < 3; dbId++)
                    WriteStrings(redis.GetDatabase(dbId), dbId);

                // Slot 1 becomes database 2 and slot 2 becomes database 1, so the checkpoint records the
                // mapping [0, 2, 1]. Slot 0 is left alone so that it still anchors database 0.
                ClassicAssert.AreEqual("OK", (string)redis.GetDatabase(0).Execute("SWAPDB", 1, 2));

                var garnetServer = redis.GetServer(TestUtils.EndPoint);
                garnetServer.Save(SaveType.BackgroundSave);
                while (garnetServer.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks)
                    Thread.Sleep(10);
            }

            server.Dispose(false);
            server = null;

            // Drop everything belonging to storage slot 2, leaving the mapping naming a slot that
            // recovery will not discover.
            var slot2Checkpoints = Path.Combine(StoreDir, "checkpoints_2");
            ClassicAssert.IsTrue(Directory.Exists(slot2Checkpoints), "Storage slot 2 was never checkpointed, so the scenario is not set up");
            Directory.Delete(slot2Checkpoints, recursive: true);
            foreach (var path in Directory.GetFiles(StoreDir, "hlog*_2.*"))
                File.Delete(path);

            var logBuffer = new StringWriter();
            var logs = TextWriter.Synchronized(logBuffer);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, tryRecover: true, logTo: logs);
            server.Start();

            string reported;
            lock (logs)
                reported = logBuffer.ToString();

            using var redis2 = ConnectionMultiplexer.Connect(TestUtils.GetConfig());

            // Slot 0 was never remapped, so database 0 still holds its own records.
            var (db0Correct, _) = CountValues(redis2.GetDatabase(0), 0, ReadString, StrVal);
            ClassicAssert.AreEqual(NumKeys, db0Correct, "Database 0 did not recover its own records");

            // Slot 1 holds what was written to database 1, and the mapping labels it database 2.
            // Honoring the mapping is what puts it there; ignoring it would leave these records at
            // database 1, which is the misattribution this test exists to catch.
            var (db2Correct, _) = CountValues(redis2.GetDatabase(2), 1, ReadString, StrVal);
            ClassicAssert.AreEqual(NumKeys, db2Correct, "Storage slot 1 did not recover under the database ID the mapping assigned it");

            // Slot 2 is gone, so the index it was mapped to recovers nothing rather than picking up
            // another slot's records.
            var (db1Correct, db1Foreign) = CountValues(redis2.GetDatabase(1), 1, ReadString, StrVal);
            ClassicAssert.AreEqual(0, db1Correct + db1Foreign, "Database 1 recovered records although the storage slot mapped to it was removed");

            ClassicAssert.IsTrue(reported.Contains("no checkpoint directory for that slot exists", StringComparison.Ordinal),
                "Recovery did not report that the mapping names a storage slot with no checkpoint directory");
        }

        /// <summary>
        /// Two swaps can leave the slots in a true cycle rather than a pair of exchanges, so that every
        /// slot is labelled with an id another surviving slot still holds. Recovery has to detach all of
        /// them before placing any, and a pairwise test cannot tell that apart from placing each in turn.
        /// </summary>
        [Test]
        public void CyclicMappingSurvivesSaveAndRecover()
        {
            const int CycleDbs = 3;

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                for (var dbId = 0; dbId < CycleDbs; dbId++)
                    WriteStrings(redis.GetDatabase(dbId), dbId);

                // Rotate the three labels: slot 0 -> database 1, slot 1 -> database 2, slot 2 -> database 0.
                ClassicAssert.AreEqual("OK", (string)redis.GetDatabase(0).Execute("SWAPDB", 0, 1));
                ClassicAssert.AreEqual("OK", (string)redis.GetDatabase(0).Execute("SWAPDB", 0, 2));

                var garnetServer = redis.GetServer(TestUtils.EndPoint);
                garnetServer.Save(SaveType.BackgroundSave);
                while (garnetServer.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks)
                    Thread.Sleep(10);
            }

            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, tryRecover: true);
            server.Start();

            using var redis2 = ConnectionMultiplexer.Connect(TestUtils.GetConfig());

            // Each database must hold the records of the one it took its slot from, all the way around the
            // cycle: database 0 has what was written to 2, 1 has 0's, and 2 has 1's.
            foreach (var (dbId, wroteBy) in new[] { (0, 2), (1, 0), (2, 1) })
            {
                var (correct, _) = CountValues(redis2.GetDatabase(dbId), wroteBy, ReadString, StrVal);
                TestContext.Progress.WriteLine($"db{dbId} holding db{wroteBy}'s data: {correct}/{NumKeys}");
                ClassicAssert.AreEqual(NumKeys, correct,
                    $"Database {dbId} did not recover the records written to database {wroteBy} before the swaps");
            }
        }
        /// <summary>
        /// A lone storage slot can still hold a database other than 0, because a swap relabels a database
        /// without moving its files. The directory names alone show one database and would select the
        /// single-database manager, which cannot place a store under an id that differs from its slot, so
        /// the checkpoint's mapping has to be consulted before that choice is made.
        /// </summary>
        [Test]
        public void MappingIsHonoredWhenOnlySlotZeroRemains()
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                for (var dbId = 0; dbId < NumDbs; dbId++)
                    WriteStrings(redis.GetDatabase(dbId), dbId);

                ClassicAssert.AreEqual("OK", (string)redis.GetDatabase(0).Execute("SWAPDB", 0, 1));

                var garnetServer = redis.GetServer(TestUtils.EndPoint);
                garnetServer.Save(SaveType.BackgroundSave);
                while (garnetServer.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks)
                    Thread.Sleep(10);
            }

            server.Dispose(false);
            server = null;

            // Leave only slot 0, so nothing in the directory names suggests more than one database.
            Directory.Delete(Path.Combine(StoreDir, "checkpoints_1"), recursive: true);
            foreach (var path in Directory.GetFiles(StoreDir, "hlog*_1.*"))
                File.Delete(path);

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, tryRecover: true);
            server.Start();

            using var redis2 = ConnectionMultiplexer.Connect(TestUtils.GetConfig());

            // Slot 0 holds what was written to database 0 before the swap, and the mapping labels it
            // database 1. Recovering it as database 0 would hand one database another database's records.
            var (db1Correct, _) = CountValues(redis2.GetDatabase(1), 0, ReadString, StrVal);
            ClassicAssert.AreEqual(NumKeys, db1Correct, "Storage slot 0 did not recover under the database ID the mapping assigned it");

            // Slot 1 is gone, so the id it was mapped to recovers nothing rather than another slot's records.
            var (db0Correct, db0Foreign) = CountValues(redis2.GetDatabase(0), 0, ReadString, StrVal);
            ClassicAssert.AreEqual(0, db0Correct + db0Foreign, "Database 0 recovered records although the storage slot mapped to it was removed");
        }

        /// <summary>
        /// A store written before databases had their own log devices has a checkpoint for database 1 but
        /// no hlog_1 to recover it from, and database 0's log is the shared one. Both must be reported
        /// explicitly rather than surfacing as the same message a fresh start produces.
        /// </summary>
        [Test]
        public void PriorLogLayoutIsReportedOnRecovery()
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                for (var dbId = 0; dbId < NumDbs; dbId++)
                    WriteStrings(redis.GetDatabase(dbId), dbId);

                var garnetServer = redis.GetServer(TestUtils.EndPoint);
                garnetServer.Save(SaveType.BackgroundSave);
                while (garnetServer.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks)
                    Thread.Sleep(10);
            }

            server.Dispose(false);
            server = null;

            // Reproduce the on-disk shape that predates individual logs. The checkpoints above were
            // written by this build, so they carry the current version; roll them back to the downlevel
            // version, or recovery would classify them as current-layout and take the wrong branch.
            DowngradeCheckpoints(Path.Combine(StoreDir, "checkpoints"));
            DowngradeCheckpoints(Path.Combine(StoreDir, "checkpoints_1"));

            // Database 1's records lived in the single shared file, so it has no log of its own.
            var perDbLogs = Directory.GetFiles(StoreDir, "hlog*_1.*");
            ClassicAssert.IsNotEmpty(perDbLogs, "Database 1 never wrote a log segment, so the scenario is not set up");
            foreach (var path in perDbLogs)
                File.Delete(path);

            var logBuffer = new StringWriter();
            var logs = TextWriter.Synchronized(logBuffer);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, tryRecover: true, logTo: logs);
            server.Start();

            string reported;
            lock (logs)
                reported = logBuffer.ToString();

            var matched = reported.Split(Environment.NewLine).Where(l => l.Contains("2152", StringComparison.Ordinal)).ToArray();
            TestContext.Progress.WriteLine(string.Join(Environment.NewLine, matched));

            ClassicAssert.IsTrue(reported.Contains("Database 1 was checkpointed before", StringComparison.Ordinal),
                "Recovery did not report database 1's unrecoverable tiered records");
            ClassicAssert.IsTrue(reported.Contains("Database 0 was checkpointed before", StringComparison.Ordinal),
                "Recovery did not report that database 0's shared log may hold another database's records");
        }

        /// <summary>
        /// Rewrites every log checkpoint's metadata under a database's checkpoint directory to the
        /// downlevel version, which is how a checkpoint written before per-database log devices
        /// appears. Metadata is stored as an int32 payload length followed by the payload; the payload
        /// is round-tripped through the production serializer so the file keeps its original
        /// sector-aligned size.
        /// </summary>
        /// <param name="checkpointDir">A <c>Store/checkpoints[_i]</c> directory</param>
        static void DowngradeCheckpoints(string checkpointDir)
        {
            // Only the log checkpoints carry a version this test cares about; index checkpoints use a
            // different metadata format.
            var cprDir = Path.Combine(checkpointDir, "cpr-checkpoints");
            var infoFiles = Directory.GetFiles(cprDir, "info.dat.0", SearchOption.AllDirectories);
            ClassicAssert.IsNotEmpty(infoFiles, $"No log checkpoint metadata under {cprDir}");

            foreach (var infoFile in infoFiles)
            {
                var bytes = File.ReadAllBytes(infoFile);
                var payloadLength = BitConverter.ToInt32(bytes, 0);

                HybridLogRecoveryInfo recoveryInfo = new();
                using (var reader = new StreamReader(new MemoryStream(bytes, sizeof(int), payloadLength)))
                    recoveryInfo.Initialize(reader);

                ClassicAssert.AreEqual(HybridLogRecoveryInfo.CheckpointVersion, recoveryInfo.hybridLogRecoveryVersion,
                    $"Checkpoint metadata in {infoFile} was not written at the current version");

                // Serializing at the downlevel version drops the fields that version does not carry and
                // checksums the rest as that version did, so the result is what an older build wrote.
                var newPayload = recoveryInfo.ToByteArray(HybridLogRecoveryInfo.MinRecoverableCheckpointVersion);
                ClassicAssert.LessOrEqual(sizeof(int) + newPayload.Length, bytes.Length);

                BitConverter.GetBytes(newPayload.Length).CopyTo(bytes, 0);
                newPayload.CopyTo(bytes, sizeof(int));
                File.WriteAllBytes(infoFile, bytes);
            }
        }
    }
}