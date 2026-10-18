// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Tests that RangeIndex record triggers (DEL and page eviction) never wait for the RangeIndex lock, which a data
    /// operation holds while it waits for the record's bucket latch or runs an epoch drain.
    /// </summary>
    [TestFixture]
    public class RangeIndexLockingTests : TestBase
    {
        /// <summary>
        /// How long an operation that should not wait on another one is given before it is judged blocked.
        /// </summary>
        private static readonly TimeSpan NotBlockedTimeout = TimeSpan.FromSeconds(10);

        private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(30);

        private const ExceptionInjectionType Pause = ExceptionInjectionType.RangeIndex_Pause_After_Shared_Lock;
        private const ExceptionInjectionType Fail = ExceptionInjectionType.RangeIndex_Fail_After_Shared_Lock;
        private const ExceptionInjectionType StagingLock = ExceptionInjectionType.RangeIndex_Before_Staging_Lock;

        private GarnetServer server;

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
            ExceptionInjectionHelper.DisableException(Pause);
            ExceptionInjectionHelper.DisableException(Fail);
            ExceptionInjectionHelper.DisableException(StagingLock);

            server.Dispose();
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// DEL of a RangeIndex key must not wait for the RangeIndex lock while it holds the key's bucket latch. A data
        /// operation holding the lock shared reads the stub next, which needs that latch, so the two would deadlock.
        /// The tree is retired once the operation releases the lock, and recreating the key waits for that.
        /// </summary>
        [Test]
        public async Task DeleteDoesNotWaitForDataOperationHoldingLockAsync()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            const string Key = "rikey";
            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redisReader = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redisDel = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var dbReader = redisReader.GetDatabase(0);
            var dbDel = redisDel.GetDatabase(0);

            CreateIndex(db, Key);
            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount);

            ExceptionInjectionHelper.EnableException(Pause);

            // The reader parks holding the shared RangeIndex lock, before it reads the stub.
            var reader = dbReader.ExecuteAsync("RI.GET", Key, "field1");
            await ExceptionInjectionHelper.WaitOnClearAsync(Pause).WaitAsync(WaitTimeout);

            var delete = dbDel.KeyDeleteAsync(Key);
            var deleteRanAlongside = await CompletesWithinAsync(delete, NotBlockedTimeout);

            // A read of the key's bucket, which is what the parked reader does next, must get through as well.
            var exists = db.KeyExistsAsync(Key);
            var existsRanAlongside = await CompletesWithinAsync(exists, NotBlockedTimeout);

            if (!deleteRanAlongside)
            {
                // DEL holds the bucket latch while it waits for the reader's lock. Releasing the reader into its read of
                // that bucket would deadlock the two, so fail the reader instead, which releases its lock.
                ExceptionInjectionHelper.EnableException(Fail);

                _ = Assert.CatchAsync<RedisException>(() => ReleaseParkedAndAwaitAsync(reader));
                _ = await delete.WaitAsync(WaitTimeout);
                _ = await exists.WaitAsync(WaitTimeout);

                Assert.Fail("DEL waited for the RangeIndex lock held by a data operation while holding the key's bucket latch, " +
                    $"which that operation needs next (a read of the same bucket {(existsRanAlongside ? "completed" : "was blocked too")})");
            }

            ClassicAssert.IsTrue(existsRanAlongside, "A read of the deleted key was blocked");
            ClassicAssert.IsFalse(await exists, "The key exists after DEL");

            // The tree is still in use, so its retirement waits for the reader; recreating the key waits for that.
            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "The tree was retired while a data operation held the lock");
            var recreate = db.ExecuteAsync("RI.CREATE", Key, "DISK", "CACHESIZE", "65536", "MINRECORD", "8");

            var readerException = Assert.ThrowsAsync<RedisServerException>(() => ReleaseParkedAndAwaitAsync(reader));
            StringAssert.Contains("range index not found", readerException.Message);

            ClassicAssert.AreEqual("OK", (string)await recreate.WaitAsync(WaitTimeout));
            ClassicAssert.AreEqual("OK", (string)db.Execute("RI.SET", Key, "field2", "value2"));
            ClassicAssert.AreEqual("value2", (string)db.Execute("RI.GET", Key, "field2"));
            ClassicAssert.IsTrue(db.Execute("RI.GET", Key, "field1").IsNull, "The recreated index has the deleted index's data");
            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount);
        }

        /// <summary>
        /// Page eviction of a RangeIndex stub must not wait for the RangeIndex lock: eviction runs in an epoch drain, which
        /// can run on a thread that holds that lock, and holds up every other thread's epoch progress meanwhile. The tree is
        /// retired once the data operation holding the lock releases it, and the key is then restored from disk.
        /// </summary>
        [Test]
        public async Task EvictionDoesNotWaitForDataOperationHoldingLockAsync()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            const string Key = "rikey";
            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;
            var store = server.Provider.StoreWrapper.store;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redisReader = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var dbReader = redisReader.GetDatabase(0);

            CreateIndex(db, Key);
            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount);

            ExceptionInjectionHelper.EnableException(Pause);

            // The reader parks holding the shared RangeIndex lock, before it reads the stub.
            var reader = dbReader.ExecuteAsync("RI.GET", Key, "field1");
            await ExceptionInjectionHelper.WaitOnClearAsync(Pause).WaitAsync(WaitTimeout);

            var evict = Task.Run(() => store.Log.ShiftHeadAddress(store.Log.TailAddress, wait: true));
            var evictionRanAlongside = await CompletesWithinAsync(evict, NotBlockedTimeout);

            if (!evictionRanAlongside)
            {
                // Eviction is parked in the epoch drain waiting for the reader's lock; fail the reader to release it.
                ExceptionInjectionHelper.EnableException(Fail);

                _ = Assert.CatchAsync<RedisException>(() => ReleaseParkedAndAwaitAsync(reader));
                await evict.WaitAsync(WaitTimeout);

                Assert.Fail("Page eviction waited in an epoch drain for the RangeIndex lock held by a data operation");
            }

            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "The tree was retired while a data operation held the lock");

            // The reader finds the stub evicted, waits for the retirement, and restores the tree from its flush snapshot.
            ClassicAssert.AreEqual("value1", (string)await ReleaseParkedAndAwaitAsync(reader));
            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "The restored tree is not registered");
            ClassicAssert.AreEqual("OK", (string)db.Execute("RI.SET", Key, "field2", "value2"));
            ClassicAssert.AreEqual("value2", (string)db.Execute("RI.GET", Key, "field2"));
        }

        /// <summary>
        /// DEL of a RangeIndex stub that is superseded rather than elided leaves the stub sealed in the log, where page
        /// eviction visits it again. That eviction must not retire whatever tree the key has by then, such as the tree of
        /// the key recreated after the DEL.
        /// </summary>
        [Test]
        public void EvictionOfDeletedStubKeepsRecreatedTree()
        {
            const string Key = "rikey";

            // A small hash index, so that a key sharing the stub's hash chain is quick to find
            const string IndexSize = "1k";
            const long IndexBuckets = 1024 / 64;

            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, indexSize: IndexSize);
            server.Start();

            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;
            var store = server.Provider.StoreWrapper.store;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            CreateIndex(db, Key);

            // A later record in the stub's hash chain keeps DEL from eliding the stub, so DEL supersedes it instead.
            ClassicAssert.IsTrue(db.StringSet(TestUtils.FindKeyInSameHashChain(Key, IndexBuckets), "value"));
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            _ = db.KeyDelete(Key);
            ClassicAssert.IsFalse(db.KeyExists(Key), "DEL did not remove the RangeIndex key");
            ClassicAssert.AreEqual(0, rangeIndexManager.LiveIndexCount, "DEL did not retire the tree");
            ClassicAssert.AreEqual(1, CountEvictableRecords(store, Key), "DEL elided the stub rather than superseding it");

            var endOfDeletedStub = store.Log.TailAddress;
            ClassicAssert.AreEqual("OK", (string)db.Execute("RI.CREATE", Key, "DISK", "CACHESIZE", "65536", "MINRECORD", "8"));
            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount);

            // Evict the deleted stub, but not the recreated one.
            store.Log.ShiftHeadAddress(endOfDeletedStub, wait: true);

            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "Evicting the deleted stub retired the recreated key's tree");
            ClassicAssert.AreEqual("OK", (string)db.Execute("RI.SET", Key, "field2", "value2"));
            ClassicAssert.AreEqual("value2", (string)db.Execute("RI.GET", Key, "field2"));
        }

        /// <summary>
        /// Promoting a flushed stub whose tree was evicted stages the key's working file from the stub's flush snapshot,
        /// under the exclusive RangeIndex lock. A data operation holding the lock shared reads the stub next, which needs the
        /// stub's bucket latch, so the promote must wait for the lock without holding that latch, unlike a store callback.
        /// </summary>
        [Test]
        public async Task PromoteWaitsForLockWithoutHoldingBucketLatchAsync()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            const string Key = "rikey";
            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redisHolder = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redisPromoter = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var dbHolder = redisHolder.GetDatabase(0);
            var dbPromoter = redisPromoter.GetDatabase(0);

            CreateIndex(db, Key);
            EvictToDisk(Key);

            ExceptionInjectionHelper.EnableException(Pause);

            // The holder parks holding the shared RangeIndex lock, before it reads the stub.
            var holder = dbHolder.ExecuteAsync("RI.GET", Key, "field1");
            await ExceptionInjectionHelper.WaitOnClearAsync(Pause).WaitAsync(WaitTimeout);

            // The promoter reads the flushed stub and promotes it, which stages the working file under the exclusive lock.
            ExceptionInjectionHelper.EnableException(StagingLock);
            var promoter = dbPromoter.ExecuteAsync("RI.GET", Key, "field1");
            await ExceptionInjectionHelper.WaitOnClearAsync(StagingLock).WaitAsync(WaitTimeout);

            // The promoter waits for the holder's lock. A read of the stub's bucket, which the holder does next, must get through.
            var exists = db.KeyExistsAsync(Key);
            var existsRanAlongside = await CompletesWithinAsync(exists, NotBlockedTimeout);

            if (!existsRanAlongside)
            {
                // The promoter holds the bucket latch while it waits for the holder's lock. Releasing the holder into its read
                // of that bucket would deadlock the two, so fail the holder instead, which releases its lock.
                ExceptionInjectionHelper.EnableException(Fail);

                _ = Assert.CatchAsync<RedisException>(() => ReleaseParkedAndAwaitAsync(holder));
                _ = await promoter.WaitAsync(WaitTimeout);
                _ = await exists.WaitAsync(WaitTimeout);

                Assert.Fail("Promoting a flushed stub waited for the RangeIndex lock held by a data operation while holding the " +
                    "stub's bucket latch, which that operation needs next");
            }

            ClassicAssert.IsTrue(await exists, "The key does not exist");
            ClassicAssert.AreEqual("value1", (string)await ReleaseParkedAndAwaitAsync(holder));
            ClassicAssert.AreEqual("value1", (string)await promoter.WaitAsync(WaitTimeout));

            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "The restored tree is not registered");
            ClassicAssert.AreEqual("OK", (string)db.Execute("RI.SET", Key, "field2", "value2"));
            ClassicAssert.AreEqual("value2", (string)db.Execute("RI.GET", Key, "field2"));
        }

        /// <summary>
        /// Copying a flushed stub whose tree was evicted to the tail, as a read with CopyReadsToTail or compaction does, runs a
        /// store callback that holds the stub's bucket latch and the epoch. A data operation can hold the RangeIndex lock shared
        /// for as long as it runs, so the callback must not wait for that lock, which would hold up every writer to the bucket
        /// and every wait for the epoch to advance, including the data operation's own.
        /// </summary>
        [Test]
        public async Task CopyToTailDoesNotWaitForDataOperationHoldingLockAsync()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            const string Key = "rikey";

            // A small hash index, so that a key sharing the stub's bucket is quick to find
            const string IndexSize = "1k";
            const long IndexBuckets = 1024 / 64;

            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, copyReadsToTail: true, indexSize: IndexSize);
            server.Start();

            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;
            var keyInSameBucket = TestUtils.FindKeyInSameHashChain(Key, IndexBuckets);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redisHolder = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redisCopier = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var dbHolder = redisHolder.GetDatabase(0);
            var dbCopier = redisCopier.GetDatabase(0);

            CreateIndex(db, Key);
            EvictToDisk(Key);

            ExceptionInjectionHelper.EnableException(Pause);

            // The holder parks holding the shared RangeIndex lock, before it reads the stub.
            var holder = dbHolder.ExecuteAsync("RI.GET", Key, "field1");
            await ExceptionInjectionHelper.WaitOnClearAsync(Pause).WaitAsync(WaitTimeout);

            // EXISTS reads the evicted stub from disk and copies it to the tail.
            var copier = dbCopier.KeyExistsAsync(Key);
            var copierRanAlongside = await CompletesWithinAsync(copier, NotBlockedTimeout);

            if (!copierRanAlongside)
            {
                var write = db.StringSetAsync(keyInSameBucket, "value");
                var writeRanAlongside = await CompletesWithinAsync(write, TimeSpan.FromSeconds(1));

                // Fail the holder rather than run it, which releases its lock.
                ExceptionInjectionHelper.EnableException(Fail);

                _ = Assert.CatchAsync<RedisException>(() => ReleaseParkedAndAwaitAsync(holder));
                _ = await copier.WaitAsync(WaitTimeout);
                _ = await write.WaitAsync(WaitTimeout);

                Assert.Fail("Copying a flushed stub to the tail waited, in a store callback, for the RangeIndex lock held by a data " +
                    $"operation (a write to the stub's bucket {(writeRanAlongside ? "completed" : "waited for it too")})");
            }

            ClassicAssert.IsTrue(await copier, "The key does not exist");
            ClassicAssert.IsTrue(await db.StringSetAsync(keyInSameBucket, "value").WaitAsync(WaitTimeout));
            ClassicAssert.AreEqual("value1", (string)await ReleaseParkedAndAwaitAsync(holder));

            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "The restored tree is not registered");
            ClassicAssert.AreEqual("OK", (string)db.Execute("RI.SET", Key, "field2", "value2"));
            ClassicAssert.AreEqual("value2", (string)db.Execute("RI.GET", Key, "field2"));
        }

        /// <summary>
        /// A flushed stub whose tree was evicted keeps its flushed state when copied to the tail, with its own copy of the flush
        /// snapshot. It can then be flushed and evicted again before any data operation promotes it, and its tree is still
        /// restored from that snapshot.
        /// </summary>
        [Test]
        public void CopiedStubKeepsFlushSnapshotThroughEviction()
        {
            const string Key = "rikey";

            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, copyReadsToTail: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            CreateIndex(db, Key);
            EvictToDisk(Key);

            // EXISTS reads the evicted stub from disk and copies it to the tail.
            var tailBeforeCopy = server.Provider.StoreWrapper.store.Log.TailAddress;
            ClassicAssert.IsTrue(db.KeyExists(Key));
            ClassicAssert.Greater(server.Provider.StoreWrapper.store.Log.TailAddress, tailBeforeCopy, "EXISTS did not copy the stub to the tail");

            EvictToDisk(Key);

            ClassicAssert.AreEqual("value1", (string)db.Execute("RI.GET", Key, "field1"));
            ClassicAssert.AreEqual(1, server.Provider.StoreWrapper.rangeIndexManager.LiveIndexCount, "The restored tree is not registered");
            ClassicAssert.AreEqual("OK", (string)db.Execute("RI.SET", Key, "field2", "value2"));
            ClassicAssert.AreEqual("value2", (string)db.Execute("RI.GET", Key, "field2"));
        }

        /// <summary>
        /// Flushes the RangeIndex key's stub and evicts it, which retires its tree; the tree is then only in the stub's flush
        /// snapshot, and the next data operation promotes the stub and restores the tree from that snapshot.
        /// </summary>
        private void EvictToDisk(string key)
        {
            var store = server.Provider.StoreWrapper.store;
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            store.Log.ShiftHeadAddress(store.Log.TailAddress, wait: true);

            ClassicAssert.AreEqual(0, server.Provider.StoreWrapper.rangeIndexManager.LiveIndexCount, $"Eviction did not retire the tree of {key}");
        }

        /// <summary>
        /// Number of in-memory records for the RangeIndex key that page eviction will visit: neither elided nor tombstones.
        /// </summary>
        private static int CountEvictableRecords<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, string key)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            var keyBytes = Encoding.ASCII.GetBytes(key);
            var count = 0;

            using var iter = store.Log.Scan(store.Log.HeadAddress, store.Log.TailAddress, DiskScanBufferingMode.NoBuffering, includeClosedRecords: true);
            while (iter.GetNext())
            {
                if (!iter.Info.Invalid && !iter.Info.Tombstone && iter.RecordType == RangeIndexManager.RangeIndexRecordType && iter.Key.SequenceEqual(keyBytes))
                    count++;
            }

            return count;
        }

        private static void CreateIndex(IDatabase db, string key)
        {
            ClassicAssert.AreEqual("OK", (string)db.Execute("RI.CREATE", key, "DISK", "CACHESIZE", "65536", "MINRECORD", "8"));
            ClassicAssert.AreEqual("OK", (string)db.Execute("RI.SET", key, "field1", "value1"));
        }

        private static async Task<bool> CompletesWithinAsync(Task task, TimeSpan timeout)
            => await Task.WhenAny(task, Task.Delay(timeout)) == task;

        /// <summary>
        /// Releases the operation parked at the injection point without re-arming the point, and waits for that
        /// operation to finish, so it runs straight through the point if it passes it again (e.g. on a retry).
        /// </summary>
        private static async Task<RedisResult> ReleaseParkedAndAwaitAsync(Task<RedisResult> parked)
        {
            ExceptionInjectionHelper.SuspendParking();
            try
            {
                return await parked.WaitAsync(WaitTimeout);
            }
            finally
            {
                ExceptionInjectionHelper.ResumeParking();
            }
        }
    }
}