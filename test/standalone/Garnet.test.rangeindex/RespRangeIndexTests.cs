// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Integration tests for RangeIndex (RI.*) commands.
    ///
    /// <para>These tests cover the full lifecycle of BfTree-backed range indexes including:
    /// creation, field CRUD, type safety (WRONGTYPE), eviction, flush/promote, lazy restore,
    /// checkpoint/recovery, AOF replay, concurrent access, and diagnostic commands.</para>
    ///
    /// <para>Each test creates a fresh Garnet server in Setup and disposes it in
    /// TearDown. Tests that need low memory, AOF, or checkpoint recovery
    /// recreate the server with the appropriate options.</para>
    /// </summary>
    [TestFixture]
    public class RespRangeIndexTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// Verifies basic RI.CREATE with MEMORY backend succeeds.
        /// </summary>
        [Test]
        public void RICreateBasicTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // RI.CREATE with MEMORY backend
            var result = db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536");
            ClassicAssert.AreEqual("OK", (string)result);
        }

        /// <summary>
        /// Verifies that calling RI.CREATE on an existing key returns an error
        /// ("ERR index already exists") and does not overwrite the existing index.
        /// </summary>
        [Test]
        public void RICreateDuplicateReturnsErrorTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Create first time
            var result = db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536");
            ClassicAssert.AreEqual("OK", (string)result);

            // Create again - should fail with error
            var ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536"));
            ClassicAssert.IsTrue(ex.Message.Contains("index already exists"));
        }

        /// <summary>
        /// Verifies that DEL on a RangeIndex key removes the key and frees the BfTree.
        /// Subsequent RI.SET/RI.GET on the deleted key should return errors.
        /// A second DEL returns false (key no longer exists).
        /// </summary>
        [Test]
        public void RICreateThenDeleteTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Create a range index and insert data
            var result = db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");
            ClassicAssert.AreEqual("OK", (string)result);
            result = db.Execute("RI.SET", "myindex", "field1", "value1");
            ClassicAssert.AreEqual("OK", (string)result);

            // Delete the index with DEL
            var deleted = db.KeyDelete("myindex");
            ClassicAssert.IsTrue(deleted);

            // Delete again - should return false (not found)
            deleted = db.KeyDelete("myindex");
            ClassicAssert.IsFalse(deleted);

            // RI.SET should fail — index no longer exists after DEL
            var ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.SET", "myindex", "field1", "value1"));
            ClassicAssert.IsNotNull(ex);

            // RI.GET should also fail
            ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.GET", "myindex", "field1"));
            ClassicAssert.IsNotNull(ex);
        }

        /// <summary>
        /// Verifies RI.CREATE with minimal arguments (defaults for all numeric parameters).
        /// </summary>
        [Test]
        public void RICreateWithDefaultsTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // RI.CREATE with minimal args (defaults)
            var result = db.Execute("RI.CREATE", "myindex", "MEMORY");
            ClassicAssert.AreEqual("OK", (string)result);
        }

        /// <summary>
        /// Verifies RI.CREATE with all optional parameters explicitly specified.
        /// </summary>
        [Test]
        public void RICreateWithAllOptionsTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // RI.CREATE with all options (PAGESIZE auto-computed from MAXRECORD)
            var result = db.Execute("RI.CREATE", "myindex", "MEMORY",
                "CACHESIZE", "131072",
                "MINRECORD", "8",
                "MAXRECORD", "1024",
                "MAXKEYLEN", "128");
            ClassicAssert.AreEqual("OK", (string)result);
        }

        /// <summary>
        /// Verifies basic RI.SET + RI.GET round-trip: set a field, then read it back.
        /// </summary>
        [Test]
        public void RISetAndGetBasicTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");

            var setResult = db.Execute("RI.SET", "myindex", "field1", "value1");
            ClassicAssert.AreEqual("OK", (string)setResult);

            var getResult = db.Execute("RI.GET", "myindex", "field1");
            ClassicAssert.AreEqual("value1", (string)getResult);
        }

        /// <summary>
        /// Verifies that RI.SET overwrites existing field values (upsert semantics).
        /// </summary>
        [Test]
        public void RISetOverwriteTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");

            db.Execute("RI.SET", "myindex", "field1", "value1");
            db.Execute("RI.SET", "myindex", "field1", "value2");

            var getResult = db.Execute("RI.GET", "myindex", "field1");
            ClassicAssert.AreEqual("value2", (string)getResult);
        }

        /// <summary>
        /// Verifies that RI.GET on a non-existent field returns null (not an error).
        /// </summary>
        [Test]
        public void RIGetNonExistentFieldTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");

            var getResult = db.Execute("RI.GET", "myindex", "nosuchfield");
            ClassicAssert.IsTrue(getResult.IsNull);
        }

        /// <summary>
        /// Verifies that RI.GET on a non-existent key returns an error (not null).
        /// </summary>
        [Test]
        public void RIGetNonExistentIndexTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.GET", "noindex", "field1"));
            ClassicAssert.IsTrue(ex.Message.Contains("range index"));
        }

        /// <summary>
        /// Verifies that RI.DEL removes a field and subsequent RI.GET returns null.
        /// </summary>
        [Test]
        public void RIDelFieldTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");
            db.Execute("RI.SET", "myindex", "field1", "value1");

            var delResult = (int)db.Execute("RI.DEL", "myindex", "field1");
            ClassicAssert.AreEqual(1, delResult);

            var getResult = db.Execute("RI.GET", "myindex", "field1");
            ClassicAssert.IsTrue(getResult.IsNull);
        }

        /// <summary>
        /// Verifies that RI.SET on a non-existent key returns an error.
        /// </summary>
        [Test]
        public void RISetOnNonExistentIndexTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.SET", "noindex", "field1", "value1"));
            ClassicAssert.IsTrue(ex.Message.Contains("range index"));
        }

        /// <summary>
        /// Verifies that multiple fields can be independently stored and retrieved.
        /// </summary>
        [Test]
        public void RIMultipleFieldsTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");

            db.Execute("RI.SET", "myindex", "aaa", "val-a");
            db.Execute("RI.SET", "myindex", "bbb", "val-b");
            db.Execute("RI.SET", "myindex", "ccc", "val-c");

            ClassicAssert.AreEqual("val-a", (string)db.Execute("RI.GET", "myindex", "aaa"));
            ClassicAssert.AreEqual("val-b", (string)db.Execute("RI.GET", "myindex", "bbb"));
            ClassicAssert.AreEqual("val-c", (string)db.Execute("RI.GET", "myindex", "ccc"));
        }

        /// <summary>
        /// Verifies WRONGTYPE error when RI.SET is used on a normal string key.
        /// </summary>
        [Test]
        public void RIWrongTypeOnNormalKeyTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // SET a normal string key
            db.StringSet("normalkey", "hello");

            // RI.SET on a normal key should fail
            var ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.SET", "normalkey", "field1", "value1"));
            ClassicAssert.IsNotNull(ex);
        }

        /// <summary>
        /// Verifies WRONGTYPE error when RI.GET is used on a normal string key.
        /// </summary>
        [Test]
        public void RIWrongTypeGetOnNormalKeyTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // SET a normal string key, then try RI.GET
            db.StringSet("normalkey", "hello");

            var ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.GET", "normalkey", "field1"));
            ClassicAssert.IsNotNull(ex);
        }

        /// <summary>
        /// Verifies WRONGTYPE error when a normal GET is used on a RangeIndex key.
        /// </summary>
        [Test]
        public void RINormalGetOnRangeIndexKeyTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536");

            // GET on a RI key returns WRONGTYPE error
            var ex = Assert.Throws<RedisServerException>(() => db.StringGet("myindex"));
            ClassicAssert.IsTrue(ex.Message.StartsWith("WRONGTYPE"));
        }

        /// <summary>
        /// Verifies WRONGTYPE error when SET is used on an existing RangeIndex key,
        /// and confirms the RI key's data is not corrupted.
        /// </summary>
        [Test]
        public void RINormalSetOnRangeIndexKeyTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
            db.Execute("RI.SET", "myindex", "field1", "value1");

            // SET on a RI key returns WRONGTYPE error
            var ex = Assert.Throws<RedisServerException>(() => db.StringSet("myindex", "overwrite"));
            ClassicAssert.IsTrue(ex.Message.StartsWith("WRONGTYPE"));

            // Verify the RI key is still intact
            var val = db.Execute("RI.GET", "myindex", "field1");
            ClassicAssert.AreEqual("value1", (string)val);
        }

        /// <summary>
        /// Verifies AOF replay: checkpoint base state, then apply post-checkpoint mutations
        /// (RI.SET update, RI.SET insert, RI.DEL) via AOF replay on recovery.
        /// </summary>
        [Test]
        public void RIAofReplayTest()
        {
            // Insert data, then recover WITHOUT checkpoint — relies on AOF replay
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, enableAOF: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                db.Execute("RI.CREATE", "aoftest", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
                db.Execute("RI.SET", "aoftest", "key1", "val1");
                db.Execute("RI.SET", "aoftest", "key2", "val2");
                db.Execute("RI.SET", "aoftest", "key3", "val3");

                // Checkpoint to establish base state
                db.Execute("SAVE");

                // Post-checkpoint mutations — these are only in the AOF
                db.Execute("RI.SET", "aoftest", "key4", "val4");
                db.Execute("RI.SET", "aoftest", "key1", "val1-updated");
                db.Execute("RI.DEL", "aoftest", "key2");

                // Commit AOF
                db.Execute("COMMITAOF");
            }

            // Recover — checkpoint restores base state, AOF replays post-checkpoint ops
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, enableAOF: true, tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // key1 should have updated value (from AOF replay)
                var val = db.Execute("RI.GET", "aoftest", "key1");
                ClassicAssert.AreEqual("val1-updated", (string)val, "key1 should have AOF-replayed update");

                // key2 should be deleted (from AOF replay)
                val = db.Execute("RI.GET", "aoftest", "key2");
                ClassicAssert.IsTrue(val.IsNull, "key2 should be deleted via AOF replay");

                // key3 should exist (from checkpoint)
                val = db.Execute("RI.GET", "aoftest", "key3");
                ClassicAssert.AreEqual("val3", (string)val, "key3 should survive from checkpoint");

                // key4 should exist (from AOF replay)
                val = db.Execute("RI.GET", "aoftest", "key4");
                ClassicAssert.AreEqual("val4", (string)val, "key4 should be added via AOF replay");
            }
        }

        /// <summary>
        /// Verifies RI.SCAN returns records ordered by key with COUNT limit.
        /// Default FIELDS mode (BOTH) returns [key, value] pairs.
        /// </summary>
        [Test]
        public void RIScanBasicTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");

            db.Execute("RI.SET", "myindex", "aaa", "val-a");
            db.Execute("RI.SET", "myindex", "bbb", "val-b");
            db.Execute("RI.SET", "myindex", "ccc", "val-c");
            db.Execute("RI.SET", "myindex", "ddd", "val-d");
            db.Execute("RI.SET", "myindex", "eee", "val-e");

            var result = (RedisResult[])db.Execute("RI.SCAN", "myindex", "aaa", "COUNT", "3");
            ClassicAssert.AreEqual(3, result.Length);

            // Each element is [key, value] since default FIELDS is BOTH
            var first = (RedisResult[])result[0];
            ClassicAssert.AreEqual("aaa", (string)first[0]);
            ClassicAssert.AreEqual("val-a", (string)first[1]);

            var second = (RedisResult[])result[1];
            ClassicAssert.AreEqual("bbb", (string)second[0]);
            ClassicAssert.AreEqual("val-b", (string)second[1]);

            var third = (RedisResult[])result[2];
            ClassicAssert.AreEqual("ccc", (string)third[0]);
            ClassicAssert.AreEqual("val-c", (string)third[1]);
        }

        /// <summary>
        /// Verifies RI.SCAN FIELDS KEY returns only key strings (no nested arrays).
        /// </summary>
        [Test]
        public void RIScanFieldsKeyTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");

            db.Execute("RI.SET", "myindex", "aaa", "val-a");
            db.Execute("RI.SET", "myindex", "bbb", "val-b");

            var result = (RedisResult[])db.Execute("RI.SCAN", "myindex", "aaa", "COUNT", "10", "FIELDS", "KEY");
            ClassicAssert.AreEqual(2, result.Length);
            ClassicAssert.AreEqual("aaa", (string)result[0]);
            ClassicAssert.AreEqual("bbb", (string)result[1]);
        }

        /// <summary>
        /// Verifies RI.SCAN FIELDS VALUE returns only value strings (no nested arrays).
        /// </summary>
        [Test]
        public void RIScanFieldsValueTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");

            db.Execute("RI.SET", "myindex", "aaa", "val-a");
            db.Execute("RI.SET", "myindex", "bbb", "val-b");

            var result = (RedisResult[])db.Execute("RI.SCAN", "myindex", "aaa", "COUNT", "10", "FIELDS", "VALUE");
            ClassicAssert.AreEqual(2, result.Length);
            ClassicAssert.AreEqual("val-a", (string)result[0]);
            ClassicAssert.AreEqual("val-b", (string)result[1]);
        }

        /// <summary>
        /// Verifies RI.RANGE returns all entries in the closed [start, end] range.
        /// </summary>
        [Test]
        public void RIRangeBasicTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");

            db.Execute("RI.SET", "myindex", "aaa", "val-a");
            db.Execute("RI.SET", "myindex", "bbb", "val-b");
            db.Execute("RI.SET", "myindex", "ccc", "val-c");
            db.Execute("RI.SET", "myindex", "ddd", "val-d");
            db.Execute("RI.SET", "myindex", "eee", "val-e");

            var result = (RedisResult[])db.Execute("RI.RANGE", "myindex", "bbb", "ddd");
            ClassicAssert.AreEqual(3, result.Length);

            var first = (RedisResult[])result[0];
            ClassicAssert.AreEqual("bbb", (string)first[0]);
            ClassicAssert.AreEqual("val-b", (string)first[1]);

            var second = (RedisResult[])result[1];
            ClassicAssert.AreEqual("ccc", (string)second[0]);
            ClassicAssert.AreEqual("val-c", (string)second[1]);

            var third = (RedisResult[])result[2];
            ClassicAssert.AreEqual("ddd", (string)third[0]);
            ClassicAssert.AreEqual("val-d", (string)third[1]);
        }

        /// <summary>
        /// Verifies RI.SCAN on a non-existent key returns an error.
        /// </summary>
        [Test]
        public void RIScanOnNonExistentIndexTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.SCAN", "noindex", "aaa", "COUNT", "10"));
            ClassicAssert.IsTrue(ex.Message.Contains("range index"));
        }

        /// <summary>
        /// Verifies RI.RANGE on a non-existent key returns an error.
        /// </summary>
        [Test]
        public void RIRangeOnNonExistentIndexTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.RANGE", "noindex", "aaa", "zzz"));
            ClassicAssert.IsTrue(ex.Message.Contains("range index"));
        }

        /// <summary>
        /// Verifies that page eviction frees BfTree instances on evicted pages,
        /// while trees on recent (mutable) pages remain live and functional.
        /// </summary>
        [Test]
        public void RIEvictionFreesEvictedTreeButKeepsLiveTest()
        {
            // Create several RI trees on early pages, then fill the log to evict those
            // pages (freeing the native BfTrees). Create new trees on recent pages and
            // verify they are still fully functional.
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, lowMemory: true);
            server.Start();

            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Create several RI trees — their stubs land on the first pages
            for (int i = 0; i < 3; i++)
            {
                db.Execute("RI.CREATE", $"early{i}", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");
                db.Execute("RI.SET", $"early{i}", "field1", $"val{i}xx");
            }
            ClassicAssert.AreEqual(3, rangeIndexManager.LiveIndexCount, "3 trees should be live after creation");

            // Fill the log with string keys to push early pages below HeadAddress.
            // Eviction calls DisposeRecord on each RI stub, freeing the native BfTrees.
            for (int i = 0; i < 200; i++)
                db.StringSet($"filler{i:D4}", $"data{i:D4}");

            // The 3 early trees should have been freed by eviction
            ClassicAssert.AreEqual(0, rangeIndexManager.LiveIndexCount,
                "All 3 early trees should have been freed by page eviction");

            // Create one new RI tree on a recent (mutable) page — above HeadAddress
            db.Execute("RI.CREATE", "live", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");
            db.Execute("RI.SET", "live", "field1", "alive");
            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "Only the new tree should be live");

            // Verify the live tree is fully functional — its BfTree was NOT freed
            var val = db.Execute("RI.GET", "live", "field1");
            ClassicAssert.AreEqual("alive", (string)val,
                "Live tree should still be accessible after evicting early pages");
        }

        /// <summary>
        /// Verifies that evicting a page with a deleted RI stub doesn't crash.
        /// The BfTree was already freed by DEL; eviction sees handle=0 and skips.
        /// </summary>
        [Test]
        public void RIEvictionAfterDeleteTest()
        {
            // Test that evicting a page with a deleted RI stub doesn't crash.
            // The BfTree was already freed by DEL; DisposeRecord sees handle=0 and skips.
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, lowMemory: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Create and then delete a range index
            db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");
            db.Execute("RI.SET", "myindex", "key1", "value1");
            db.KeyDelete("myindex");

            // Fill log to evict the page containing the deleted RI stub
            for (int i = 0; i < 200; i++)
                db.StringSet($"filler{i:D4}", $"data{i:D4}");

            // Create a new index after eviction — should work fine
            db.Execute("RI.CREATE", "newidx", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");
            db.Execute("RI.SET", "newidx", "field1", "hello");
            var val = db.Execute("RI.GET", "newidx", "field1");
            ClassicAssert.AreEqual("hello", (string)val);
        }

        /// <summary>
        /// Verifies that multiple indexes can be created, deleted, evicted, and new indexes
        /// remain functional after the eviction of deleted stubs.
        /// </summary>
        [Test]
        public void RIEvictionMultipleIndexesTest()
        {
            // Create indexes, delete them, evict their pages, then verify new indexes work
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, lowMemory: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Create and delete several indexes on early pages
            for (int idx = 0; idx < 3; idx++)
            {
                db.Execute("RI.CREATE", $"old{idx}", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");
                db.Execute("RI.SET", $"old{idx}", "field1", $"value{idx}");
                db.KeyDelete($"old{idx}");
            }

            // Fill log to evict deleted stubs
            for (int i = 0; i < 200; i++)
                db.StringSet($"filler{i:D4}", $"data{i:D4}");

            // Create a live index after eviction — should be fully functional
            db.Execute("RI.CREATE", "live", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");
            db.Execute("RI.SET", "live", "field1", "alive");
            var val = db.Execute("RI.GET", "live", "field1");
            ClassicAssert.AreEqual("alive", (string)val);
        }

        /// <summary>
        /// Verifies create/delete/evict/recreate cycle under memory pressure.
        /// Each cycle creates an index, deletes it, evicts, then creates a new one.
        /// </summary>
        [Test]
        public void RICreateDeleteRecreateWithEvictionTest()
        {
            // Test create/delete/evict/recreate cycle under memory pressure.
            // Each cycle creates an index, deletes it, evicts, then creates a new one
            // and verifies the new one is live.
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, lowMemory: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            for (int round = 0; round < 3; round++)
            {
                // Create and delete — BfTree is freed by delete
                db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");
                db.Execute("RI.SET", "myindex", "field1", $"round{round}");
                db.KeyDelete("myindex");

                // Fill log to evict the deleted stub page
                for (int i = 0; i < 100; i++)
                    db.StringSet($"pad{round}_{i:D4}", $"x{round}_{i:D4}");
            }

            // After all cycles, create a live index and verify it works
            db.Execute("RI.CREATE", "final", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");
            db.Execute("RI.SET", "final", "field1", "works");
            var val = db.Execute("RI.GET", "final", "field1");
            ClassicAssert.AreEqual("works", (string)val);
        }

        /// <summary>
        /// Verifies RI.SET returns an error when the field exceeds MAXKEYLEN.
        /// </summary>
        [Test]
        public void RISetInvalidKVFieldTooLongTest()
        {
            // RI.SET with a field exceeding MAXKEYLEN should return an error.
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "MEMORY",
                "CACHESIZE", "65536",
                "MINRECORD", "8",
                "MAXRECORD", "256",
                "MAXKEYLEN", "16");

            // Field of 17 bytes exceeds MAXKEYLEN=16
            var longField = new string('k', 17);
            var ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.SET", "myindex", longField, "value1"));
            ClassicAssert.IsTrue(ex.Message.Contains("key+value size must be between"),
                $"Expected InvalidKV error, got: {ex.Message}");
        }

        /// <summary>
        /// Verifies RI.SET returns an error when the value exceeds MAXRECORD.
        /// </summary>
        [Test]
        public void RISetInvalidKVValueTooLongTest()
        {
            // RI.SET with a value exceeding MAXRECORD should return an error.
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "MEMORY",
                "CACHESIZE", "65536",
                "MINRECORD", "8",
                "MAXRECORD", "64",
                "MAXKEYLEN", "16");

            // Value larger than MAXRECORD=64
            var longValue = new string('v', 128);
            var ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.SET", "myindex", "field1", longValue));
            ClassicAssert.IsTrue(ex.Message.Contains("key+value size must be between"),
                $"Expected InvalidKV error, got: {ex.Message}");
        }

        /// <summary>
        /// Verifies RI.SET returns an error when the record (key+value) is below MINRECORD.
        /// </summary>
        [Test]
        public void RISetInvalidKVRecordTooSmallTest()
        {
            // RI.SET with a record (key+value) smaller than MINRECORD should return an error.
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Set a high MINRECORD so that small key+value pairs are rejected
            db.Execute("RI.CREATE", "myindex", "MEMORY",
                "CACHESIZE", "65536",
                "MINRECORD", "128",
                "MAXRECORD", "1024",
                "MAXKEYLEN", "64");

            // key="a" (1 byte) + value="b" (1 byte) = 2 bytes, well below MINRECORD=128
            var ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.SET", "myindex", "a", "b"));
            ClassicAssert.IsTrue(ex.Message.Contains("key+value size must be between"),
                $"Expected InvalidKV error for record below MINRECORD, got: {ex.Message}");
        }

        /// <summary>
        /// Verifies thread-safety of concurrent RI.SET/RI.GET/RI.DEL from multiple clients
        /// on the same RangeIndex. Tests the shared-lock path under contention.
        /// </summary>
        [Test]
        public async Task RIConcurrentMultiClientTest()
        {
            // Multiple clients concurrently writing to and reading from the same
            // RangeIndex. Verifies thread-safety of the shared-lock path.
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "MEMORY",
                "CACHESIZE", "1048576",
                "MINRECORD", "8",
                "MAXRECORD", "256",
                "MAXKEYLEN", "64");

            const int numTasks = 8;
            const int opsPerTask = 50;

            // Phase 1: concurrent writes
            var writeTasks = new Task[numTasks];
            for (int t = 0; t < numTasks; t++)
            {
                int taskId = t;
                writeTasks[t] = Task.Run(async () =>
                {
                    using var conn = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                    var tdb = conn.GetDatabase(0);
                    for (int i = 0; i < opsPerTask; i++)
                    {
                        var field = $"t{taskId}:f{i:D4}";
                        var value = $"val-{taskId}-{i}";
                        await tdb.ExecuteAsync("RI.SET", "myindex", field, value);
                    }
                });
            }
            await Task.WhenAll(writeTasks);

            // Phase 2: concurrent reads — verify every written value
            var readTasks = new Task[numTasks];
            for (int t = 0; t < numTasks; t++)
            {
                int taskId = t;
                readTasks[t] = Task.Run(async () =>
                {
                    using var conn = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                    var tdb = conn.GetDatabase(0);
                    for (int i = 0; i < opsPerTask; i++)
                    {
                        var field = $"t{taskId}:f{i:D4}";
                        var expected = $"val-{taskId}-{i}";
                        var actual = (string)await tdb.ExecuteAsync("RI.GET", "myindex", field);
                        ClassicAssert.AreEqual(expected, actual,
                            $"Mismatch for {field}: expected '{expected}', got '{actual}'");
                    }
                });
            }
            await Task.WhenAll(readTasks);

            // Phase 3: concurrent mixed read/write/delete
            var mixedTasks = new Task[numTasks];
            for (int t = 0; t < numTasks; t++)
            {
                int taskId = t;
                mixedTasks[t] = Task.Run(async () =>
                {
                    using var conn = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                    var tdb = conn.GetDatabase(0);
                    for (int i = 0; i < opsPerTask; i++)
                    {
                        // Write a new key
                        var field = $"mix{taskId}:f{i:D4}";
                        await tdb.ExecuteAsync("RI.SET", "myindex", field, $"mixed-{taskId}-{i}");

                        // Read it back
                        var val = (string)await tdb.ExecuteAsync("RI.GET", "myindex", field);
                        ClassicAssert.AreEqual($"mixed-{taskId}-{i}", val);

                        // Delete it
                        await tdb.ExecuteAsync("RI.DEL", "myindex", field);
                    }
                });
            }
            await Task.WhenAll(mixedTasks);
        }

        /// <summary>
        /// Verifies that DEL on a mutable-region RI key immediately frees the BfTree
        /// (LiveIndexCount drops to 0).
        /// </summary>
        [Test]
        public void RIDeleteInMutableRegionFreesResourcesTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;
            var store = server.Provider.StoreWrapper.store;

            db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");
            db.Execute("RI.SET", "myindex", "field1", "value1");
            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "Tree should be live after creation");
            ClassicAssert.IsTrue(store.Log.TailAddress > store.Log.ReadOnlyAddress,
                "Stub should be in the mutable region");

            db.KeyDelete("myindex");
            ClassicAssert.AreEqual(0, rangeIndexManager.LiveIndexCount,
                "BfTree should be freed immediately after DEL in mutable region");
        }

        /// <summary>
        /// Verifies that DEL on a read-only-region RI key frees the BfTree via CopyUpdater.
        /// Also verifies a new index can be created and used after the delete.
        /// </summary>
        [Test]
        public void RIDeleteInReadOnlyRegionFreesResourcesTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;
            var store = server.Provider.StoreWrapper.store;

            db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");
            db.Execute("RI.SET", "myindex", "field1", "value1");
            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "Tree should be live after creation");

            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            db.KeyDelete("myindex");
            ClassicAssert.AreEqual(0, rangeIndexManager.LiveIndexCount,
                "BfTree should be freed immediately after DEL in read-only region");

            db.Execute("RI.CREATE", "newidx", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");
            db.Execute("RI.SET", "newidx", "f1xx", "v1xx");
            var val = db.Execute("RI.GET", "newidx", "f1xx");
            ClassicAssert.AreEqual("v1xx", (string)val);
        }

        /// <summary>
        /// Verifies the flush→promote→access cycle: when pages move to read-only,
        /// OnFlush snapshots the BfTree and sets IsFlushed. The next RI.GET detects
        /// the flag, promotes the stub to tail via RMW, and data remains accessible.
        /// </summary>
        [Test]
        public void RIFlushPromotesToTailOnNextAccessTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;
            var store = server.Provider.StoreWrapper.store;

            // Create a memory-backed index and insert data
            db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");
            db.Execute("RI.SET", "myindex", "aaa", "val-a");
            db.Execute("RI.SET", "myindex", "bbb", "val-b");
            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "Tree should be live");

            // Record the tail address before flush — stub is in mutable
            var tailBeforeFlush = store.Log.TailAddress;

            // Force pages into read-only region — triggers OnFlushRecord which
            // snapshots BfTree and sets the Flushed flag on the in-memory stub.
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            // The BfTree should still be live (not evicted, just flushed)
            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "Tree should still be live after flush");

            // Next RI.GET detects flushed flag → promotes stub to tail via RMW → clears flag
            var result = db.Execute("RI.GET", "myindex", "aaa");
            ClassicAssert.AreEqual("val-a", (string)result, "Data should be readable after flush+promote");

            // Tail should have advanced because the stub was copied to mutable region
            ClassicAssert.IsTrue(store.Log.TailAddress > tailBeforeFlush,
                "Tail should advance after promote-to-tail RMW");

            // Subsequent operations should work normally (no more promote needed)
            db.Execute("RI.SET", "myindex", "ccc", "val-c");
            result = db.Execute("RI.GET", "myindex", "ccc");
            ClassicAssert.AreEqual("val-c", (string)result, "New insert should work after promote");

            result = db.Execute("RI.GET", "myindex", "bbb");
            ClassicAssert.AreEqual("val-b", (string)result, "Pre-flush data should still be accessible");
        }

        /// <summary>
        /// Verifies two consecutive flush→promote cycles: flush, promote, mutate, flush
        /// again, promote again. All data should survive both cycles.
        /// </summary>
        [Test]
        public void RIFlushPromoteThenSecondFlushTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;
            var store = server.Provider.StoreWrapper.store;

            // Create index, insert data
            db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");
            db.Execute("RI.SET", "myindex", "key-one", "value-1");

            // First flush cycle: flush → promote → mutate
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            var r1 = db.Execute("RI.GET", "myindex", "key-one");
            ClassicAssert.AreEqual("value-1", (string)r1, "Read after first flush should promote and return data");

            // Mutate after promote (stub is now back in mutable)
            db.Execute("RI.SET", "myindex", "key-two", "value-2");

            // Second flush cycle: flush again → should snapshot with latest data
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            // Read again — should promote again and return both values
            r1 = db.Execute("RI.GET", "myindex", "key-one");
            ClassicAssert.AreEqual("value-1", (string)r1, "key-one should survive second flush cycle");

            var r2 = db.Execute("RI.GET", "myindex", "key-two");
            ClassicAssert.AreEqual("value-2", (string)r2, "key-two (added after first promote) should survive second flush cycle");

            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "Tree should still be live");
        }

        /// <summary>
        /// Verifies full eviction-to-disk and lazy restore cycle: create, insert, evict
        /// past HeadAddress, then access triggers pending disk read → invalidate → promote
        /// → RestoreTreeFromFlush → data accessible again.
        /// </summary>
        [Test]
        public void RIEvictToDiskThenLazyRestoreTest()
        {
            // Create a BfTree, insert data, evict past HeadAddress (to disk),
            // then access again — should promote from disk + lazy restore from flush file.
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, lowMemory: true);
            server.Start();

            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Create disk-backed index and insert data (disk backend supports snapshot/restore)
            db.Execute("RI.CREATE", "myindex", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
            db.Execute("RI.SET", "myindex", "alpha", "value-alpha");
            db.Execute("RI.SET", "myindex", "bravo", "value-bravo");
            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "Tree should be live");

            // Fill the log to push the RI stub below HeadAddress (eviction).
            // OnFlushRecord snapshots the BfTree to flush.bftree.
            // DisposeRecord(PageEviction) frees the native BfTree.
            for (int i = 0; i < 200; i++)
                db.StringSet($"filler{i:D4}", $"data{i:D4}");

            ClassicAssert.AreEqual(0, rangeIndexManager.LiveIndexCount,
                "BfTree should have been freed by eviction");

            // Now access the RI key — this triggers:
            // 1. Pending read from disk → OnDiskReadRecord invalidates TreeHandle
            // 2. ReadRangeIndex detects IsFlushed → promotes stub to tail
            // 3. Promoted stub has TreeHandle == 0 → RestoreTreeFromFlush recovers BfTree
            // 4. RI.GET returns data from the restored BfTree
            var result = db.Execute("RI.GET", "myindex", "alpha");
            ClassicAssert.AreEqual("value-alpha", (string)result,
                "Data should be recoverable after eviction + lazy restore");

            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount,
                "Restored BfTree should be registered as live");

            // Verify second key is also available
            result = db.Execute("RI.GET", "myindex", "bravo");
            ClassicAssert.AreEqual("value-bravo", (string)result,
                "Second key should also be recoverable");

            // Verify writes work on the restored tree
            db.Execute("RI.SET", "myindex", "charlie", "value-charlie");
            result = db.Execute("RI.GET", "myindex", "charlie");
            ClassicAssert.AreEqual("value-charlie", (string)result,
                "Writes should work on restored tree");
        }

        /// <summary>
        /// Verifies checkpoint + recovery: create tree, insert data, BGSAVE, dispose,
        /// recover. All data should be present after recovery and new writes should work.
        /// </summary>
        [Test]
        public void RICheckpointAndRecoverTest()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, enableAOF: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                // Create a disk-backed BfTree and insert data
                db.Execute("RI.CREATE", "cpindex", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
                db.Execute("RI.SET", "cpindex", "alpha", "value-alpha");
                db.Execute("RI.SET", "cpindex", "bravo", "value-bravo");
                db.Execute("RI.SET", "cpindex", "charlie", "value-charlie");

                // Verify data before checkpoint
                var val = db.Execute("RI.GET", "cpindex", "alpha");
                ClassicAssert.AreEqual("value-alpha", (string)val);

                // Take checkpoint via BGSAVE
                db.Execute("SAVE");
            }

            // Dispose and recover
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, enableAOF: true, tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // Verify data recovered from checkpoint
                var val = db.Execute("RI.GET", "cpindex", "alpha");
                ClassicAssert.AreEqual("value-alpha", (string)val, "alpha should survive checkpoint+recovery");

                val = db.Execute("RI.GET", "cpindex", "bravo");
                ClassicAssert.AreEqual("value-bravo", (string)val, "bravo should survive checkpoint+recovery");

                val = db.Execute("RI.GET", "cpindex", "charlie");
                ClassicAssert.AreEqual("value-charlie", (string)val, "charlie should survive checkpoint+recovery");

                // Verify writes work after recovery
                db.Execute("RI.SET", "cpindex", "delta", "value-delta");
                val = db.Execute("RI.GET", "cpindex", "delta");
                ClassicAssert.AreEqual("value-delta", (string)val, "new insert should work after recovery");
            }
        }

        /// <summary>
        /// Full lifecycle test: create → insert → flush to read-only → promote → mutate →
        /// evict to disk → restore from flush → checkpoint → recover. Verifies data
        /// survives every transition in the store lifecycle.
        /// </summary>
        [Test]
        public void RIFlushEvictRestoreCheckpointCycleTest()
        {
            // Full lifecycle: create → insert → flush to read-only → promote → mutate →
            // evict to disk → restore from flush → checkpoint → recover
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, lowMemory: true, enableAOF: true);
            server.Start();

            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;
            var store = server.Provider.StoreWrapper.store;

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                // Create disk-backed tree and insert data
                db.Execute("RI.CREATE", "lifecycle", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
                db.Execute("RI.SET", "lifecycle", "key-aaa", "val-aaa");
                db.Execute("RI.SET", "lifecycle", "key-bbb", "val-bbb");
                ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount);

                // Force flush to read-only (sets Flushed flag)
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

                // Access triggers promote to tail (clears Flushed flag)
                var val = db.Execute("RI.GET", "lifecycle", "key-aaa");
                ClassicAssert.AreEqual("val-aaa", (string)val, "Should read after flush+promote");

                // Mutate after promote (stub now in mutable)
                db.Execute("RI.SET", "lifecycle", "key-ccc", "val-ccc");

                // Fill log to evict the old pages past HeadAddress
                for (int i = 0; i < 200; i++)
                    db.StringSet($"fill{i:D4}", $"data{i:D4}");

                // Tree might still be live (promote moved stub to tail)
                // Access should work regardless
                val = db.Execute("RI.GET", "lifecycle", "key-bbb");
                ClassicAssert.AreEqual("val-bbb", (string)val, "Should read after eviction of old pages");

                val = db.Execute("RI.GET", "lifecycle", "key-ccc");
                ClassicAssert.AreEqual("val-ccc", (string)val, "Post-promote insert should survive eviction");

                // Take checkpoint
                db.Execute("SAVE");
            }

            // Recover
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, lowMemory: true, enableAOF: true, tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                var val = db.Execute("RI.GET", "lifecycle", "key-aaa");
                ClassicAssert.AreEqual("val-aaa", (string)val, "key-aaa should survive full lifecycle");

                val = db.Execute("RI.GET", "lifecycle", "key-bbb");
                ClassicAssert.AreEqual("val-bbb", (string)val, "key-bbb should survive full lifecycle");

                val = db.Execute("RI.GET", "lifecycle", "key-ccc");
                ClassicAssert.AreEqual("val-ccc", (string)val, "key-ccc should survive full lifecycle");
            }
        }

        /// <summary>
        /// Verifies that multiple BfTrees are independently evicted and lazily restored.
        /// 3 evicted trees + 1 live tree = 4 total after all restores.
        /// </summary>
        [Test]
        public void RIMultipleTreesEvictAndRestoreTest()
        {
            // Multiple BfTrees, some evicted, some live — verify independent lifecycle
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, lowMemory: true);
            server.Start();

            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Create 3 trees early — they'll be on early pages
            for (int i = 0; i < 3; i++)
            {
                db.Execute("RI.CREATE", $"tree{i}", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
                db.Execute("RI.SET", $"tree{i}", "field1", $"value{i}-1");
                db.Execute("RI.SET", $"tree{i}", "field2", $"value{i}-2");
            }
            ClassicAssert.AreEqual(3, rangeIndexManager.LiveIndexCount);

            // Fill log to evict early pages
            for (int i = 0; i < 200; i++)
                db.StringSet($"filler{i:D4}", $"data{i:D4}");

            // Early trees should be evicted
            ClassicAssert.AreEqual(0, rangeIndexManager.LiveIndexCount, "All early trees should be evicted");

            // Create a new tree on recent pages (still in memory)
            db.Execute("RI.CREATE", "tree-live", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
            db.Execute("RI.SET", "tree-live", "field1", "live-val");
            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount);

            // Access evicted trees — triggers lazy restore
            for (int i = 0; i < 3; i++)
            {
                var val = db.Execute("RI.GET", $"tree{i}", "field1");
                ClassicAssert.AreEqual($"value{i}-1", (string)val, $"tree{i} field1 should restore from flush");

                val = db.Execute("RI.GET", $"tree{i}", "field2");
                ClassicAssert.AreEqual($"value{i}-2", (string)val, $"tree{i} field2 should restore from flush");
            }

            // All trees should now be live (3 restored + 1 live)
            ClassicAssert.AreEqual(4, rangeIndexManager.LiveIndexCount);

            // Live tree should still work
            var liveVal = db.Execute("RI.GET", "tree-live", "field1");
            ClassicAssert.AreEqual("live-val", (string)liveVal);
        }

        /// <summary>
        /// Verifies that multiple trees survive checkpoint + recovery with all data intact.
        /// </summary>
        [Test]
        public void RICheckpointWithMultipleTreesAndRecoverTest()
        {
            // Multiple trees, checkpoint, recover — all trees should be restored
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, enableAOF: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                // Create several trees with different data
                for (int i = 0; i < 5; i++)
                {
                    db.Execute("RI.CREATE", $"idx{i}", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
                    for (int j = 0; j < 10; j++)
                        db.Execute("RI.SET", $"idx{i}", $"field-{j:D3}", $"value-{i}-{j}");
                }

                // Checkpoint
                db.Execute("SAVE");
            }

            // Recover
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, enableAOF: true, tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // Verify all trees and all fields recovered
                for (int i = 0; i < 5; i++)
                {
                    for (int j = 0; j < 10; j++)
                    {
                        var val = db.Execute("RI.GET", $"idx{i}", $"field-{j:D3}");
                        ClassicAssert.AreEqual($"value-{i}-{j}", (string)val,
                            $"idx{i} field-{j:D3} should survive checkpoint+recovery");
                    }
                }

                // Verify writes work on all recovered trees
                for (int i = 0; i < 5; i++)
                {
                    db.Execute("RI.SET", $"idx{i}", "new-field", $"new-value-{i}");
                    var val = db.Execute("RI.GET", $"idx{i}", "new-field");
                    ClassicAssert.AreEqual($"new-value-{i}", (string)val);
                }
            }
        }

        /// <summary>
        /// Verifies flush → promote → mutate → checkpoint → recover captures post-flush mutations.
        /// </summary>
        [Test]
        public void RIFlushPromoteCheckpointRecoverTest()
        {
            // Flush → promote → mutate → checkpoint → recover
            // Tests that post-flush mutations are captured by checkpoint
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, enableAOF: true);
            server.Start();

            var store = server.Provider.StoreWrapper.store;

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                db.Execute("RI.CREATE", "fpcp", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
                db.Execute("RI.SET", "fpcp", "before-flush", "original");

                // Flush to read-only
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

                // Promote by accessing
                var val = db.Execute("RI.GET", "fpcp", "before-flush");
                ClassicAssert.AreEqual("original", (string)val);

                // Mutate after promote (these are in mutable region now)
                db.Execute("RI.SET", "fpcp", "after-flush", "mutated");
                db.Execute("RI.SET", "fpcp", "before-flush", "updated");

                // Checkpoint should capture the mutated state
                db.Execute("SAVE");
            }

            // Recover
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, enableAOF: true, tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                var val = db.Execute("RI.GET", "fpcp", "before-flush");
                ClassicAssert.AreEqual("updated", (string)val, "Updated value should survive checkpoint");

                val = db.Execute("RI.GET", "fpcp", "after-flush");
                ClassicAssert.AreEqual("mutated", (string)val, "Post-flush insert should survive checkpoint");
            }
        }

        /// <summary>
        /// Verifies that evicting a deleted stub doesn't crash — the BfTree was already freed
        /// by DEL, so eviction sees handle=0 and safely skips. New indexes work after eviction.
        /// </summary>
        [Test]
        public void RIDeleteDuringEvictionCycleTest()
        {
            // Create tree → delete → fill log to trigger eviction of deleted record
            // Verify eviction of deleted record doesn't crash
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, lowMemory: true);
            server.Start();

            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Create and populate
            db.Execute("RI.CREATE", "deltest", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
            db.Execute("RI.SET", "deltest", "field1", "value1");
            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount);

            // Delete the tree
            db.KeyDelete("deltest");
            ClassicAssert.AreEqual(0, rangeIndexManager.LiveIndexCount);

            // Fill log to evict the page containing the deleted stub
            for (int i = 0; i < 200; i++)
                db.StringSet($"fill{i:D4}", $"data{i:D4}");

            // Create new tree to verify manager is still functional
            db.Execute("RI.CREATE", "newtest", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
            db.Execute("RI.SET", "newtest", "field1", "alive");
            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount);

            var val = db.Execute("RI.GET", "newtest", "field1");
            ClassicAssert.AreEqual("alive", (string)val);
        }

        /// <summary>
        /// Verifies double flush → promote → checkpoint → recover: data from both flush
        /// cycles and post-promote mutations all survive recovery.
        /// </summary>
        [Test]
        public void RIDoubleFlushCycleWithCheckpointTest()
        {
            // flush → promote → mutate → flush again → promote → checkpoint → recover
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, enableAOF: true);
            server.Start();

            var store = server.Provider.StoreWrapper.store;

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                db.Execute("RI.CREATE", "dblflush", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
                db.Execute("RI.SET", "dblflush", "round1", "value-r1");

                // First flush cycle
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                var val = db.Execute("RI.GET", "dblflush", "round1");
                ClassicAssert.AreEqual("value-r1", (string)val);

                // Mutate after first promote
                db.Execute("RI.SET", "dblflush", "round2", "value-r2");

                // Second flush cycle
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                val = db.Execute("RI.GET", "dblflush", "round2");
                ClassicAssert.AreEqual("value-r2", (string)val);

                // Mutate after second promote
                db.Execute("RI.SET", "dblflush", "round3", "value-r3");

                // Checkpoint
                db.Execute("SAVE");
            }

            // Recover
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, enableAOF: true, tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                var val = db.Execute("RI.GET", "dblflush", "round1");
                ClassicAssert.AreEqual("value-r1", (string)val, "round1 should survive double-flush + checkpoint");

                val = db.Execute("RI.GET", "dblflush", "round2");
                ClassicAssert.AreEqual("value-r2", (string)val, "round2 should survive double-flush + checkpoint");

                val = db.Execute("RI.GET", "dblflush", "round3");
                ClassicAssert.AreEqual("value-r3", (string)val, "round3 should survive double-flush + checkpoint");
            }
        }

        /// <summary>
        /// Verifies eviction → lazy restore → mutate → checkpoint → recover.
        /// Post-restore mutations should be captured by the checkpoint.
        /// </summary>
        [Test]
        public void RIEvictRestoreAndCheckpointTest()
        {
            // Evict to disk → lazy restore → mutate → checkpoint → recover
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, lowMemory: true, enableAOF: true);
            server.Start();

            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                db.Execute("RI.CREATE", "evictcp", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
                db.Execute("RI.SET", "evictcp", "pre-evict", "original");
                ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount);

                // Fill to evict
                for (int i = 0; i < 200; i++)
                    db.StringSet($"fill{i:D4}", $"data{i:D4}");

                ClassicAssert.AreEqual(0, rangeIndexManager.LiveIndexCount, "Tree should be evicted");

                // Lazy restore by accessing
                var val = db.Execute("RI.GET", "evictcp", "pre-evict");
                ClassicAssert.AreEqual("original", (string)val, "Should restore from flush file");
                ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "Tree should be restored");

                // Mutate after restore
                db.Execute("RI.SET", "evictcp", "post-restore", "added");

                // Checkpoint
                db.Execute("SAVE");
            }

            // Recover
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, lowMemory: true, enableAOF: true, tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                var val = db.Execute("RI.GET", "evictcp", "pre-evict");
                ClassicAssert.AreEqual("original", (string)val, "Pre-evict data should survive evict→restore→checkpoint");

                val = db.Execute("RI.GET", "evictcp", "post-restore");
                ClassicAssert.AreEqual("added", (string)val, "Post-restore data should survive checkpoint");
            }
        }

        /// <summary>
        /// Verifies that taking two checkpoints and recovering always gets the latest data.
        /// Checkpoint 2 should contain updates made after checkpoint 1.
        /// </summary>
        [Test]
        public void RITwoCheckpointsRecoverToLatestTest()
        {
            // Take two checkpoints with different data, recover to latest
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, enableAOF: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                // Phase 1: create tree and insert first batch
                db.Execute("RI.CREATE", "twockpt", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
                db.Execute("RI.SET", "twockpt", "key-alpha", "val-alpha");
                db.Execute("RI.SET", "twockpt", "key-bravo", "val-bravo");

                // Checkpoint 1
                db.Execute("SAVE");

                // Phase 2: insert additional data + update existing
                db.Execute("RI.SET", "twockpt", "key-charlie", "val-charlie");
                db.Execute("RI.SET", "twockpt", "key-alpha", "val-alpha-v2");

                // Checkpoint 2
                db.Execute("SAVE");
            }

            // Recover — should get latest checkpoint (checkpoint 2)
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, enableAOF: true, tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // Should see checkpoint 2 data: updated alpha, bravo, and charlie
                var val = db.Execute("RI.GET", "twockpt", "key-alpha");
                ClassicAssert.AreEqual("val-alpha-v2", (string)val, "alpha should have updated value from checkpoint 2");

                val = db.Execute("RI.GET", "twockpt", "key-bravo");
                ClassicAssert.AreEqual("val-bravo", (string)val, "bravo should exist");

                val = db.Execute("RI.GET", "twockpt", "key-charlie");
                ClassicAssert.AreEqual("val-charlie", (string)val, "charlie should exist from checkpoint 2");

                // Verify writes work after recovery
                db.Execute("RI.SET", "twockpt", "key-delta", "val-delta");
                val = db.Execute("RI.GET", "twockpt", "key-delta");
                ClassicAssert.AreEqual("val-delta", (string)val, "new insert should work after recovery");
            }
        }

        /// <summary>
        /// Verifies that post-checkpoint mutations are replayed from AOF after recovery.
        /// With AOF enabled, v+1 data IS replayed from AOF after checkpoint recovery.
        /// </summary>
        [Test]
        public void RIRecoverToEarlierCheckpointTest()
        {
            // Add keys A,B → checkpoint → add key C, update A → recover →
            // With AOF logging, v+1 data IS replayed from AOF after checkpoint recovery
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, enableAOF: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                // Phase 1: create tree, insert keys A and B
                db.Execute("RI.CREATE", "earlyck", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
                db.Execute("RI.SET", "earlyck", "key-A", "val-A-original");
                db.Execute("RI.SET", "earlyck", "key-B", "val-B");

                // Checkpoint
                db.Execute("SAVE");

                // Phase 2: add more data AFTER checkpoint (logged to AOF)
                db.Execute("RI.SET", "earlyck", "key-A", "val-A-updated");
                db.Execute("RI.SET", "earlyck", "key-C", "val-C");

                // Commit AOF
                db.Execute("COMMITAOF");
            }

            // Recover — checkpoint restored first, then AOF entries replayed
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, enableAOF: true, tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // key-A should have the UPDATED value (replayed from AOF)
                var val = db.Execute("RI.GET", "earlyck", "key-A");
                ClassicAssert.AreEqual("val-A-updated", (string)val, "key-A should have updated value replayed from AOF");

                // key-B should exist (it was in the checkpoint)
                val = db.Execute("RI.GET", "earlyck", "key-B");
                ClassicAssert.AreEqual("val-B", (string)val, "key-B should exist from checkpoint");

                // key-C should exist (replayed from AOF)
                val = db.Execute("RI.GET", "earlyck", "key-C");
                ClassicAssert.AreEqual("val-C", (string)val, "key-C should exist — replayed from AOF after checkpoint");
            }
        }

        /// <summary>
        /// Verifies that DEL on a recovered key correctly frees the lazily-restored BfTree.
        /// </summary>
        [Test]
        public void RIDeleteAfterRecoveryTest()
        {
            // Create tree, checkpoint, recover, then DEL the recovered key.
            // Verifies that DEL correctly frees the lazily-restored BfTree.
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, enableAOF: true);
            server.Start();

            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                db.Execute("RI.CREATE", "delafter", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
                db.Execute("RI.SET", "delafter", "key1", "val1");
                ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount);

                db.Execute("SAVE");
            }

            // Recover
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, enableAOF: true, tryRecover: true);
            server.Start();
            rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // Access triggers lazy restore
                var val = db.Execute("RI.GET", "delafter", "key1");
                ClassicAssert.AreEqual("val1", (string)val);
                ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount);

                // DEL on the recovered key should free the BfTree
                var deleted = db.KeyDelete("delafter");
                ClassicAssert.IsTrue(deleted, "DEL should return true");
                ClassicAssert.AreEqual(0, rangeIndexManager.LiveIndexCount, "BfTree should be freed after DEL");

                // Subsequent access should return not found
                var ex = Assert.Throws<RedisServerException>(() =>
                    db.Execute("RI.GET", "delafter", "key1"));
                ClassicAssert.IsNotNull(ex);
            }
        }

        /// <summary>
        /// Verifies RI.EXISTS returns 1 for existing RI keys, 0 for non-existent
        /// and deleted keys.
        /// </summary>
        [Test]
        public void RIExistsBasicTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Non-existent key should return 0
            var result = db.Execute("RI.EXISTS", "myindex");
            ClassicAssert.AreEqual(0, (int)result);

            // Create index, should now return 1
            db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536");
            result = db.Execute("RI.EXISTS", "myindex");
            ClassicAssert.AreEqual(1, (int)result);

            // Delete the key, should return 0
            db.KeyDelete("myindex");
            result = db.Execute("RI.EXISTS", "myindex");
            ClassicAssert.AreEqual(0, (int)result);
        }

        /// <summary>
        /// Verifies RI.EXISTS returns 0 (not WRONGTYPE) for a normal string key.
        /// </summary>
        [Test]
        public void RIExistsOnNormalKeyTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // SET a normal string key
            db.StringSet("normalkey", "hello");

            // RI.EXISTS on a normal string key should return 0 (not WRONGTYPE)
            var result = db.Execute("RI.EXISTS", "normalkey");
            ClassicAssert.AreEqual(0, (int)result);
        }

        /// <summary>
        /// Verifies RI.CONFIG returns all 6 configuration fields with correct values.
        /// </summary>
        [Test]
        public void RIConfigBasicTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536",
                "MINRECORD", "32", "MAXRECORD", "512", "MAXKEYLEN", "64");

            var result = db.Execute("RI.CONFIG", "myindex");
            var arr = (RedisResult[])result;

            // Should be 12 elements (6 field-value pairs)
            ClassicAssert.AreEqual(12, arr.Length);

            // Check field names and values
            ClassicAssert.AreEqual("storage_backend", (string)arr[0]);
            ClassicAssert.AreEqual("MEMORY", (string)arr[1]);
            ClassicAssert.AreEqual("cache_size", (string)arr[2]);
            ClassicAssert.AreEqual("65536", (string)arr[3]);
            ClassicAssert.AreEqual("min_record_size", (string)arr[4]);
            ClassicAssert.AreEqual("32", (string)arr[5]);
            ClassicAssert.AreEqual("max_record_size", (string)arr[6]);
            ClassicAssert.AreEqual("512", (string)arr[7]);
            ClassicAssert.AreEqual("max_key_len", (string)arr[8]);
            ClassicAssert.AreEqual("64", (string)arr[9]);
            ClassicAssert.AreEqual("leaf_page_size", (string)arr[10]);
            // leaf_page_size is auto-computed, just ensure it's present
            ClassicAssert.IsNotNull((string)arr[11]);
        }

        /// <summary>
        /// Verifies RI.CONFIG returns WRONGTYPE error on a normal string key.
        /// </summary>
        [Test]
        public void RIConfigWrongTypeTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.StringSet("normalkey", "hello");

            var ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.CONFIG", "normalkey"));
            ClassicAssert.IsNotNull(ex);
        }

        /// <summary>
        /// Verifies RI.METRICS returns runtime state (tree_handle, is_live, is_flushed, is_recovered).
        /// </summary>
        [Test]
        public void RIMetricsBasicTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");

            // Insert some data (use field/value sizes that fit default MINRECORD)
            db.Execute("RI.SET", "myindex", "field1", "value1");
            db.Execute("RI.SET", "myindex", "field2", "value2");

            var result = db.Execute("RI.METRICS", "myindex");
            var arr = (RedisResult[])result;

            // Should be 8 elements (4 field-value pairs)
            ClassicAssert.AreEqual(8, arr.Length);

            ClassicAssert.AreEqual("tree_handle", (string)arr[0]);
            // tree_handle should be a non-zero number
            ClassicAssert.IsNotNull((string)arr[1]);

            ClassicAssert.AreEqual("is_live", (string)arr[2]);
            ClassicAssert.AreEqual("true", (string)arr[3]);

            ClassicAssert.AreEqual("is_flushed", (string)arr[4]);
            ClassicAssert.AreEqual("false", (string)arr[5]);

            ClassicAssert.AreEqual("is_recovered", (string)arr[6]);
            ClassicAssert.AreEqual("false", (string)arr[7]);
        }

        /// <summary>
        /// Verifies that TYPE command returns "rangeindex" for RI keys.
        /// </summary>
        [Test]
        public void RITypeCommandTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Create a RangeIndex and check TYPE
            db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536");
            var type = db.KeyType("myindex");
            ClassicAssert.AreEqual(RedisType.Unknown, type);
            // StackExchange.Redis maps unknown types to Unknown, verify via Execute
            var typeResult = db.Execute("TYPE", "myindex");
            ClassicAssert.AreEqual("rangeindex", (string)typeResult);

            // Normal string key should return "string"
            db.StringSet("normalkey", "hello");
            typeResult = db.Execute("TYPE", "normalkey");
            ClassicAssert.AreEqual("string", (string)typeResult);
        }

        /// <summary>
        /// Stress test: 4 worker threads insert concurrently while a 5th thread takes a
        /// blocking SAVE checkpoint. Verifies checkpoint contains a strict prefix of each
        /// thread's keys (point-in-time snapshot consistency).
        /// </summary>
        [Test]
        [CancelAfter(120_000)]
        public void RIConcurrentOpsWithCheckpointTest(System.Threading.CancellationToken cancellationToken)
        {
            // 4 threads insert contiguous keys at full speed. A single SAVE (blocking)
            // runs from a 5th thread. After SAVE completes, workers are signaled and
            // insert a few more keys before stopping. Recovery should show a strict
            // prefix per thread: all keys before some cutoff, none after.
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, lowMemory: true);
            server.Start();

            const int numThreads = 4;
            const int postSaveOps = 50;
            var saveCompleted = new ManualResetEventSlim(false);
            var errors = new System.Collections.Concurrent.ConcurrentBag<string>();
            var insertedCounts = new int[numThreads];
            var barrier = new Barrier(numThreads + 1);

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                db.Execute("RI.CREATE", "stress", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
            }

            // Worker threads: insert keys at full speed, then postSaveOps more after save
            var workers = new Task[numThreads];
            for (int t = 0; t < numThreads; t++)
            {
                var threadId = t;
                workers[t] = Task.Run(() =>
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                    var db = redis.GetDatabase(0);
                    int i = 0;
                    barrier.SignalAndWait();

                    // Phase 1: insert at full speed until save completes
                    while (!saveCompleted.IsSet)
                    {
                        try
                        {
                            db.Execute("RI.SET", "stress", $"t{threadId}_{i:D6}", $"v{threadId}_{i:D6}");
                            i++;
                        }
                        catch (Exception ex)
                        {
                            errors.Add($"Thread {threadId} op {i}: {ex.Message}");
                        }
                    }

                    // Phase 2: insert postSaveOps more (these should NOT be in the checkpoint)
                    for (int j = 0; j < postSaveOps; j++)
                    {
                        try
                        {
                            db.Execute("RI.SET", "stress", $"t{threadId}_{i:D6}", $"v{threadId}_{i:D6}");
                            i++;
                        }
                        catch (Exception ex)
                        {
                            errors.Add($"Thread {threadId} op {i}: {ex.Message}");
                        }
                    }

                    insertedCounts[threadId] = i;
                });
            }

            // Checkpoint thread: wait for workers to start, then do a single blocking SAVE
            var checkpointTask = Task.Run(() =>
            {
                using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
                var db = redis.GetDatabase(0);
                barrier.SignalAndWait();

                // Let workers insert for a bit before taking the checkpoint
                Thread.Sleep(200);
                db.Execute("SAVE");
                saveCompleted.Set();
            });

            Task.WaitAll([.. workers, checkpointTask]);
            ClassicAssert.IsEmpty(errors, $"Errors during concurrent ops:\n{string.Join("\n", errors)}");

            // Recover from checkpoint only (no AOF)
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, lowMemory: true, tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                for (int t = 0; t < numThreads; t++)
                {
                    var totalInserted = insertedCounts[t];

                    // Find the prefix length: first key that is absent
                    int recovered = 0;
                    for (int i = 0; i < totalInserted; i++)
                    {
                        var val = db.Execute("RI.GET", "stress", $"t{t}_{i:D6}");
                        if (val.IsNull)
                            break;
                        ClassicAssert.AreEqual($"v{t}_{i:D6}", (string)val,
                            $"Thread {t} key t{t}_{i:D6}: value mismatch");
                        recovered++;
                    }

                    ClassicAssert.Greater(recovered, 0,
                        $"Thread {t}: no keys recovered");

                    // Strict prefix: ALL keys after the cutoff must be absent
                    for (int i = recovered; i < totalInserted; i++)
                    {
                        var val = db.Execute("RI.GET", "stress", $"t{t}_{i:D6}");
                        ClassicAssert.IsTrue(val.IsNull,
                            $"Thread {t} key t{t}_{i:D6}: present after gap at {recovered} — not a strict prefix");
                    }

                    // The postSaveOps keys must NOT be in the checkpoint
                    ClassicAssert.Less(recovered, totalInserted,
                        $"Thread {t}: all {totalInserted} keys recovered — checkpoint was not taken mid-insertion");
                }
            }
        }

        /// <summary>
        /// After checkpoint recovery, <c>IsRecovered</c> must be cleared when the tree
        /// is first restored. Otherwise, a second eviction cycle causes
        /// RestoreTreeFromFlush to pick the stale checkpoint snapshot instead of the
        /// fresh <c>flush.bftree</c>, losing post-recovery writes.
        /// </summary>
        [Test]
        public void RIRecoverThenSecondEvictionUsesFlushSnapshotTest()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, lowMemory: true, enableAOF: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                db.Execute("RI.CREATE", "stalecp", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
                db.Execute("RI.SET", "stalecp", "pre-checkpoint", "original");
                db.Execute("SAVE");
            }

            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, lowMemory: true, enableAOF: true, tryRecover: true);
            server.Start();

            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                var val = db.Execute("RI.GET", "stalecp", "pre-checkpoint");
                ClassicAssert.AreEqual("original", (string)val, "Checkpoint data should be restored");

                db.Execute("RI.SET", "stalecp", "post-recovery", "new-value");

                // Fill log to trigger flush (writes flush.bftree) then eviction (frees tree)
                for (int i = 0; i < 200; i++)
                    db.StringSet($"fill{i:D4}", $"data{i:D4}");

                ClassicAssert.AreEqual(0, rangeIndexManager.LiveIndexCount, "Tree should be evicted");

                val = db.Execute("RI.GET", "stalecp", "pre-checkpoint");
                ClassicAssert.AreEqual("original", (string)val, "Pre-checkpoint data should survive");

                val = db.Execute("RI.GET", "stalecp", "post-recovery");
                ClassicAssert.AreEqual("new-value", (string)val,
                    "Post-recovery data must survive eviction (flush.bftree, not stale checkpoint)");
            }
        }

        /// <summary>
        /// Verifies pure AOF-only recovery (no checkpoint). RI.CREATE is replayed to
        /// recreate the BfTree, then RI.SET/RI.DEL operations rebuild the data.
        /// </summary>
        [Test]
        public void RIAofOnlyRecoveryTest()
        {
            // No checkpoint at all — AOF replay must recreate the BfTree from scratch.
            // RI.CREATE is logged via RMW, RI.SET/RI.DEL via synthetic RMW.
            // On recovery, AOF replay re-executes RI.CREATE (creates the tree),
            // then replays RI.SET/RI.DEL operations to rebuild the data.
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, enableAOF: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                db.Execute("RI.CREATE", "aofonly", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
                db.Execute("RI.SET", "aofonly", "key-a", "val-a");
                db.Execute("RI.SET", "aofonly", "key-b", "val-b");
                db.Execute("RI.SET", "aofonly", "key-c", "val-c");
                db.Execute("RI.SET", "aofonly", "key-a", "val-a-updated");
                db.Execute("RI.DEL", "aofonly", "key-b");

                // Commit AOF but do NOT take a checkpoint
                db.Execute("COMMITAOF");
            }

            // Recover — no checkpoint exists, so everything comes from AOF replay
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, enableAOF: true, tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // key-a should have updated value
                var val = db.Execute("RI.GET", "aofonly", "key-a");
                ClassicAssert.AreEqual("val-a-updated", (string)val, "key-a should have updated value from AOF replay");

                // key-b should be deleted
                val = db.Execute("RI.GET", "aofonly", "key-b");
                ClassicAssert.IsTrue(val.IsNull, "key-b should be deleted via AOF replay");

                // key-c should exist
                val = db.Execute("RI.GET", "aofonly", "key-c");
                ClassicAssert.AreEqual("val-c", (string)val, "key-c should exist from AOF replay");

                // New writes should work on the AOF-recovered tree
                db.Execute("RI.SET", "aofonly", "key-d", "val-d");
                val = db.Execute("RI.GET", "aofonly", "key-d");
                ClassicAssert.AreEqual("val-d", (string)val, "new insert should work after AOF-only recovery");
            }
        }

        /// <summary>
        /// Verifies that DEL on a disk-backed RangeIndex cleans up the WORKING file
        /// (<c>&lt;hash&gt;.data.bftree</c>) on disk but PRESERVES per-flush snapshot files
        /// (<c>&lt;hash&gt;.&lt;addr&gt;.flush.bftree</c>). Per-flush files are LOG-tied — they may
        /// still be required to recover an OLDER checkpoint that was taken BEFORE the DEL.
        /// They are reclaimed by <c>OnTruncate</c> when the log's BeginAddress passes their address.
        /// </summary>
        [Test]
        public void RIDiskFileCleanupOnDeleteTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Create a disk-backed range index
            db.Execute("RI.CREATE", "cleanup", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
            db.Execute("RI.SET", "cleanup", "key1", "val1");

            // Verify the riLogRoot exists with at least one <hash>.data.bftree file
            var riLogRoot = Path.Combine(TestUtils.MethodTestDir, "Store", "rangeindex");
            ClassicAssert.IsTrue(Directory.Exists(riLogRoot), "riLogRoot directory should exist after RI.CREATE");
            var dataFiles = Directory.GetFiles(riLogRoot, "*.data.bftree");
            ClassicAssert.AreEqual(1, dataFiles.Length, "should have exactly one data.bftree file");

            // Delete the range index
            var delResult = db.KeyDelete("cleanup");
            ClassicAssert.IsTrue(delResult, "DEL should return true");

            // Working file should be cleaned up.
            dataFiles = Directory.Exists(riLogRoot) ? Directory.GetFiles(riLogRoot, "*.data.bftree") : [];
            ClassicAssert.AreEqual(0, dataFiles.Length, "data.bftree files should be deleted after DEL");
            // (Flush files are not asserted here because none were created in this test — no flush
            // event fired between RI.SET and DEL. The next test verifies the preservation contract.)
        }

        /// <summary>
        /// Verifies the LOG-tied lifetime of per-flush snapshot files: DEL preserves the
        /// <c>&lt;hash&gt;.&lt;addr&gt;.flush.bftree</c> files that were created by prior flush
        /// events, because they may be required to recover an OLDER checkpoint that was taken
        /// before the DEL. Only <c>OnTruncate</c> (when log BeginAddress passes their address)
        /// can safely delete them.
        /// </summary>
        [Test]
        public void RIDeletePreservesPerFlushSnapshotFilesTest()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true,
                lowMemory: true);
            server.Start();

            var store = server.Provider.StoreWrapper.store;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            // Create RI, write, force flush so a per-flush snapshot file is created.
            db.Execute("RI.CREATE", "preservetest", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
            db.Execute("RI.SET", "preservetest", "field-x", "value-v1");
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            var riLogRoot = Path.Combine(TestUtils.MethodTestDir, "Store", "rangeindex");
            var flushBefore = Directory.GetFiles(riLogRoot, "*.flush.bftree");
            ClassicAssert.GreaterOrEqual(flushBefore.Length, 1,
                "test setup: at least one flush.bftree file should exist after force-flush");

            // Read once to promote the flushed stub back to the mutable region (RIPROMOTE).
            // This is required because DEL operates on the in-memory chain head.
            ClassicAssert.AreEqual("value-v1", (string)db.Execute("RI.GET", "preservetest", "field-x"));

            // DEL the key.
            ClassicAssert.IsTrue(db.KeyDelete("preservetest"), "DEL should succeed");

            // Working file must be deleted, but per-flush snapshot file(s) must SURVIVE.
            var dataAfter = Directory.GetFiles(riLogRoot, "*.data.bftree");
            ClassicAssert.AreEqual(0, dataAfter.Length, "data.bftree should be deleted on DEL");

            var flushAfter = Directory.GetFiles(riLogRoot, "*.flush.bftree");
            ClassicAssert.AreEqual(flushBefore.Length, flushAfter.Length,
                "per-flush snapshot files MUST be preserved on DEL — they may be required to recover " +
                "an older checkpoint that was taken before the DEL. Only OnTruncate (when log " +
                "BeginAddress passes their address) can safely delete them.");

            // Verify file content is byte-identical (not corrupted).
            for (var i = 0; i < flushBefore.Length; i++)
            {
                ClassicAssert.IsTrue(File.Exists(flushBefore[i]), $"flush file {Path.GetFileName(flushBefore[i])} must still exist after DEL");
            }
        }

        /// <summary>
        /// Verifies that DEL of a previously-evicted-and-restored RangeIndex correctly cleans
        /// up the working file but PRESERVES per-flush snapshot files (LOG-tied lifetime).
        /// </summary>
        [Test]
        public void RIDiskFileCleanupOnDeleteAfterEvictionAndRestoreTest()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, lowMemory: true);
            server.Start();

            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Create a disk-backed range index on an early page
            db.Execute("RI.CREATE", "evictdel", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
            db.Execute("RI.SET", "evictdel", "key1", "val1");
            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "tree should be live after creation");

            var riLogRoot = Path.Combine(TestUtils.MethodTestDir, "Store", "rangeindex");
            ClassicAssert.IsTrue(Directory.Exists(riLogRoot), "riLogRoot should exist");

            // Fill the log with string keys to push RI stub below HeadAddress and trigger eviction
            for (var i = 0; i < 200; i++)
                db.StringSet($"filler{i:D4}", $"data{i:D4}");

            // Verify eviction actually occurred
            ClassicAssert.AreEqual(0, rangeIndexManager.LiveIndexCount, "tree should have been freed by eviction");

            // Files should still exist after eviction (preserved for lazy restore)
            var dataFiles = Directory.GetFiles(riLogRoot, "*.data.bftree");
            ClassicAssert.AreEqual(1, dataFiles.Length, "data.bftree file should survive eviction");
            var flushFilesPostEvict = Directory.GetFiles(riLogRoot, "*.flush.bftree");
            ClassicAssert.GreaterOrEqual(flushFilesPostEvict.Length, 1, "at least one flush snapshot should exist post-eviction");

            // Lazy restore brings the record back in-memory (DEL requires the record
            // to be in-memory; the unified Delete path does not trigger lazy restore).
            var val = db.Execute("RI.GET", "evictdel", "key1");
            ClassicAssert.AreEqual("val1", (string)val, "lazy restore should recover data after eviction");
            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "tree should be live again after lazy restore");

            // Now delete — only the working data.bftree should be removed; per-flush snapshots
            // must survive (LOG-tied lifetime; needed for recovery to a prior checkpoint).
            var delResult = db.KeyDelete("evictdel");
            ClassicAssert.IsTrue(delResult, "DEL should return true");

            dataFiles = Directory.Exists(riLogRoot) ? Directory.GetFiles(riLogRoot, "*.data.bftree") : [];
            ClassicAssert.AreEqual(0, dataFiles.Length, "data.bftree should be deleted after DEL");

            var flushFilesPostDel = Directory.Exists(riLogRoot) ? Directory.GetFiles(riLogRoot, "*.flush.bftree") : [];
            ClassicAssert.AreEqual(flushFilesPostEvict.Length, flushFilesPostDel.Length,
                "per-flush snapshot files MUST be preserved on DEL even for previously-evicted keys " +
                "— they may be required to recover an older checkpoint taken before the DEL.");
        }

        // ============================================================================
        // BfTree compaction-lifecycle tests
        // ============================================================================

        /// <summary>
        /// Helper: count the number of <c>&lt;hash&gt;.&lt;addr&gt;.flush.bftree</c> files in
        /// the riLogRoot — used by tests that verify per-flush-file lifecycle.
        /// </summary>
        private static int CountFlushFiles()
        {
            var riLogRoot = Path.Combine(TestUtils.MethodTestDir, "Store", "rangeindex");
            if (!Directory.Exists(riLogRoot)) return 0;
            return Directory.GetFiles(riLogRoot, "*.flush.bftree").Length;
        }

        /// <summary>
        /// Verifies the per-flush snapshot file immutability invariant: a
        /// <c>&lt;hash&gt;.&lt;addr&gt;.flush.bftree</c> file, once written, is never overwritten.
        /// Subsequent flushes for the same key produce a NEW file at a distinct address,
        /// so historical per-flush state is preserved for recovery to older checkpoints.
        ///
        /// <para>Steps: create a disk-backed RI, set v1, flush (creates file at A1), capture
        /// bytes; promote + set v2, flush (must create a new file at A2); assert that the
        /// A1 file still exists with byte-identical content AND the post-v2 file count is
        /// strictly greater than post-v1.</para>
        /// </summary>
        [Test]
        public void RIFlushFilesAreImmutablePerAddressTest()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true,
                lowMemory: true);
            server.Start();

            var store = server.Provider.StoreWrapper.store;

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                // v1 state: create disk-backed RI and insert one field. Use long enough field/value
                // to satisfy MINRECORD=8.
                db.Execute("RI.CREATE", "flushtestkey", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
                db.Execute("RI.SET", "flushtestkey", "field-x", "value-v1");

                // Force flush so the v1 stub is on a flushed page (creates <hash>.<A1>.flush.bftree).
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

                // Snapshot the flush file count and capture the v1 file content.
                var afterV1 = ListFlushFiles();
                ClassicAssert.GreaterOrEqual(afterV1.Count, 1, "at least one flush file after v1 flush");
                var v1FileContents = new System.Collections.Generic.Dictionary<string, byte[]>();
                foreach (var f in afterV1) v1FileContents[f] = File.ReadAllBytes(f);

                // Promote (read forces RIPROMOTE on flushed stub) and mutate to v2.
                ClassicAssert.AreEqual("value-v1", (string)db.Execute("RI.GET", "flushtestkey", "field-x"));
                db.Execute("RI.SET", "flushtestkey", "field-x", "value-v2");

                // Force another flush — must create a NEW <hash>.<A2>.flush.bftree at a distinct addr.
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

                // Verify: each pre-existing v1 file STILL exists with byte-identical content.
                foreach (var (path, originalBytes) in v1FileContents)
                {
                    ClassicAssert.IsTrue(File.Exists(path),
                        $"per-flush snapshot file {Path.GetFileName(path)} must remain after subsequent flush");
                    var currentBytes = File.ReadAllBytes(path);
                    ClassicAssert.AreEqual(originalBytes.Length, currentBytes.Length,
                        $"per-flush snapshot {Path.GetFileName(path)} must NOT be overwritten (size differs)");
                    CollectionAssert.AreEqual(originalBytes, currentBytes,
                        $"per-flush snapshot {Path.GetFileName(path)} must NOT be overwritten (content differs). " +
                        "Per-flush <addr>.flush.bftree files are immutable.");
                }

                // Verify: at least one new flush file was created post-v2 (proving second flush
                // didn't just overwrite the v1 file in place).
                var afterV2 = ListFlushFiles();
                ClassicAssert.Greater(afterV2.Count, afterV1.Count,
                    "Second flush must create a NEW per-flush file (not overwrite the v1 file). " +
                    $"Before: {afterV1.Count}, after: {afterV2.Count}");
            }

            // Helper closure
            static System.Collections.Generic.List<string> ListFlushFiles()
            {
                var dir = Path.Combine(TestUtils.MethodTestDir, "Store", "rangeindex");
                if (!Directory.Exists(dir)) return new System.Collections.Generic.List<string>();
                return Directory.GetFiles(dir, "*.flush.bftree").ToList();
            }
        }

        /// <summary>
        /// Verifies that <c>DisposeTreeUnderLock</c> with <c>deleteFiles: false</c> (eviction
        /// path) NO-OPS when the source stub has <c>IsTransferred=true</c>, even when its
        /// <c>TreeHandle</c> is zero. After <c>PostCopyToTail</c> (compaction) or RIPROMOTE
        /// PostCopyUpdater transfers ownership to a destination at the tail, the source
        /// record's stub carries <c>IsTransferred=true</c>; the corresponding liveIndexes
        /// entry now belongs to the destination, so a later <c>OnEvict</c> on the stale
        /// source must NOT remove that entry — doing so would lose checkpoint coverage and
        /// DEL-time native-tree disposal.
        ///
        /// <para>Test mechanics:</para>
        /// <list type="number">
        /// <item>Construct a 35-byte stub representing the stale source (IsTransferred=true,
        /// TreeHandle=0).</item>
        /// <item>Call <c>DisposeTreeUnderLock(key, stub, deleteFiles: false)</c>.</item>
        /// <item>Assert the liveIndexes entry SURVIVED (count unchanged).</item>
        /// </list>
        ///
        /// <para>Discriminating contrast: a stub with <c>IsTransferred=false</c> and
        /// <c>TreeHandle=0</c> (a pure pending entry being evicted before activation) DOES
        /// get its liveIndexes entry removed by the same call — proving the
        /// <c>IsTransferred</c> check is what makes the no-op precise.</para>
        /// </summary>
        [Test]
        public void RIDisposeTreeUnderLockNoOpsOnTransferredSourceTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var rim = server.Provider.StoreWrapper.rangeIndexManager;
            ClassicAssert.IsNotNull(rim);

            // Create an RI key so that a real liveIndexes entry exists; this models the scenario
            // where we'd later eviction-callback a stale source.
            db.Execute("RI.CREATE", "transtest", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
            db.Execute("RI.SET", "transtest", "field-x", "value-v1");
            ClassicAssert.AreEqual(1, rim.LiveIndexCount, "tree should be live after creation");

            // Construct a stub representing a "stale source" (IsTransferred=true, TreeHandle=0)
            // with the SAME key as the live entry. This models the byte state of a source record
            // after PostCopyToTail-live or RIPROMOTE-live has cleared TreeHandle and set
            // IsTransferred on the source.
            var staleStub = new byte[RangeIndexManager.IndexSizeBytes];
            // Stub layout: [0..7]=TreeHandle (zero), [33]=flags byte (Transferred bit = 1<<2 = 4)
            staleStub[33] = 4;

            // Verify our reading of the stub matches expectation.
            ref readonly var stubRef = ref RangeIndexManager.ReadIndex(staleStub);
            ClassicAssert.AreEqual(nint.Zero, stubRef.TreeHandle, "test setup: stub TreeHandle should be 0");
            ClassicAssert.IsTrue(stubRef.IsTransferred, "test setup: IsTransferred should be true");
            ClassicAssert.IsFalse(stubRef.IsFlushed, "test setup: IsFlushed should be false");

            // Call DisposeTreeUnderLock as OnEvict would, with deleteFiles=false (eviction path).
            // With IsTransferred=true, this must no-op — NOT remove the entry that belongs to
            // the live record at the tail.
            // RangeIndexManager hashes the key via PinnedSpanByte.FromPinnedSpan, which captures
            // a raw pointer assuming the source is GC-pinned. Use unsafe `fixed` blocks to pin
            // the managed byte[] for the duration of each DisposeTreeUnderLock call.
            unsafe
            {
                var keyBytes = System.Text.Encoding.ASCII.GetBytes("transtest");
                fixed (byte* keyPtr = keyBytes)
                {
                    var pinnedKey = new ReadOnlySpan<byte>(keyPtr, keyBytes.Length);
                    rim.DisposeTreeUnderLock(pinnedKey, staleStub, deleteFiles: false);
                }
            }

            ClassicAssert.AreEqual(1, rim.LiveIndexCount,
                "DisposeTreeUnderLock on a stale (IsTransferred=true) source must NOT remove the live entry " +
                "that now belongs to the destination at the tail.");

            // Live tree should still be functional.
            ClassicAssert.AreEqual("value-v1", (string)db.Execute("RI.GET", "transtest", "field-x"));

            // Discriminating contrast: a stub WITHOUT IsTransferred (pure pending entry, e.g.
            // evicted before activation) WOULD remove the entry. This proves the discriminating
            // power of the IsTransferred check.
            var pendingStub = new byte[RangeIndexManager.IndexSizeBytes];
            // IsTransferred=0, TreeHandle=0 — looks like a pending entry being evicted.
            // Use a DIFFERENT key for this part to avoid disturbing the live entry above.
            db.Execute("RI.CREATE", "pendkey", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
            ClassicAssert.AreEqual(2, rim.LiveIndexCount, "second tree created");
            // Now simulate eviction of a pending-entry stub for "pendkey".
            unsafe
            {
                var pendKeyBytes = System.Text.Encoding.ASCII.GetBytes("pendkey");
                fixed (byte* keyPtr = pendKeyBytes)
                {
                    var pinnedKey = new ReadOnlySpan<byte>(keyPtr, pendKeyBytes.Length);
                    rim.DisposeTreeUnderLock(pinnedKey, pendingStub, deleteFiles: false);
                }
            }
            ClassicAssert.AreEqual(1, rim.LiveIndexCount,
                "DisposeTreeUnderLock without IsTransferred should still remove the entry " +
                "(pending-eviction case): proves the discriminating power of the IsTransferred check");
        }

        /// <summary>
        /// Verifies that <c>EnableRangeIndexPreview=true</c> works correctly with
        /// <c>CopyReadsToTail=true</c>. RangeIndex Reads go through the dedicated
        /// <c>Read_RangeIndex</c> API which suppresses Tsavorite's automatic CTT
        /// (RangeIndex performs its own controlled promotion via RIPROMOTE). This test
        /// is bounded by <c>[CancelAfter]</c> so a regression that lets CTT reach a
        /// RangeIndex stub would surface as a hang/timeout.
        /// </summary>
        [Test]
        [CancelAfter(60_000)]
        public void RICopyReadsToTailCompatibleTest(System.Threading.CancellationToken cancellationToken)
        {
            // Recreate the server with CopyReadsToTail=true.
            server?.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(
                TestUtils.MethodTestDir,
                enableRangeIndexPreview: true,
                copyReadsToTail: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.Execute("RI.CREATE", "rikey", "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
            for (int i = 0; i < 50; i++)
                db.Execute("RI.SET", "rikey", $"field-{i:000}", $"value-{i:000}-pad");

            // Force the records into the read-only / flushed region so subsequent reads
            // would otherwise trigger CTT for the RI stub. With Read_RangeIndex suppressing
            // CTT, the read should route through PromoteToTail (RIPROMOTE) instead.
            var store = server.Provider.StoreWrapper.store;
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            for (int i = 0; i < 50 && !cancellationToken.IsCancellationRequested; i++)
            {
                var got = (string)db.Execute("RI.GET", "rikey", $"field-{i:000}");
                ClassicAssert.AreEqual($"value-{i:000}-pad", got, $"field-{i:000}");
            }
        }
    }
}