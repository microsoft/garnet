// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Text;
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
        /// A store written before databases had their own log devices has a checkpoint for database 1 but
        /// no hlog_1 to recover it from, and database 0's log is the shared one. Both must be reported
        /// explicitly rather than surfacing as the same message a fresh start produces.
        /// </summary>
        [Test]
        public void PreFixCheckpointIsReportedOnRecovery()
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

            // Reproduce the pre-fix on-disk shape. The checkpoints above were written by this build, so
            // they carry the current version; roll them back to the downlevel version, or recovery
            // would classify them as current-layout and take the wrong branch.
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

                // ToByteArray always stamps the current version, so rewrite the version line directly.
                // Everything after it is unchanged, and the version is outside Checksum().
                var newPayload = DowngradePayload(recoveryInfo.ToByteArray());
                ClassicAssert.LessOrEqual(sizeof(int) + newPayload.Length, bytes.Length);

                BitConverter.GetBytes(newPayload.Length).CopyTo(bytes, 0);
                newPayload.CopyTo(bytes, sizeof(int));
                File.WriteAllBytes(infoFile, bytes);
            }
        }

        /// <summary>
        /// Replaces the leading version line of a serialized <see cref="HybridLogRecoveryInfo"/> payload
        /// with the downlevel version, and drops the trailing fields that version does not have.
        /// </summary>
        static byte[] DowngradePayload(byte[] payload)
        {
            var lines = Encoding.UTF8.GetString(payload).Split(Environment.NewLine).ToList();
            ClassicAssert.AreEqual(HybridLogRecoveryInfo.CheckpointVersion.ToString(), lines[0]);
            lines[0] = HybridLogRecoveryInfo.MinRecoverableCheckpointVersion.ToString();

            // A downlevel payload ends after the cookie: drop the mapping length, its entries, and the
            // swap epoch that this build appends. With no mapping written, that is exactly two lines.
            var last = lines.Count - 1;
            while (last >= 0 && lines[last].Length == 0)
                last--;
            ClassicAssert.GreaterOrEqual(last, 2, "Payload is too short to be a checkpoint");
            lines.RemoveRange(last - 1, 2);

            return Encoding.UTF8.GetBytes(string.Concat(lines.Select(line => line + Environment.NewLine)));
        }
    }
}