// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using Garnet.server.BfTreeInterop;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Tests that RangeIndex store callbacks (DEL, page eviction, flush, copy to the tail) never wait for the RangeIndex lock,
    /// which a data operation holds while it waits for the record's bucket latch or runs an epoch drain, and that the work they
    /// defer or hand to a data operation keeps every tree registered and recoverable.
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
        private const ExceptionInjectionType RestorePause = ExceptionInjectionType.RangeIndex_Pause_Restoring_Tree;
        private const ExceptionInjectionType PromotePause = ExceptionInjectionType.RangeIndex_Pause_Before_Promote;
        private const ExceptionInjectionType PublishPause = ExceptionInjectionType.RangeIndex_Pause_Before_Restore_Publish;
        private const ExceptionInjectionType RetirementWait = ExceptionInjectionType.RangeIndex_Waits_For_Retirement;
        private const ExceptionInjectionType CheckpointWait = ExceptionInjectionType.RangeIndex_Checkpoint_Waits_For_Flush_Snapshots;
        private const ExceptionInjectionType CheckpointWaitForTree = ExceptionInjectionType.RangeIndex_Waits_For_Checkpoint;
        private const ExceptionInjectionType CreatePause = ExceptionInjectionType.RangeIndex_Pause_Before_Create_Tree;
        private const ExceptionInjectionType TransactionPause = ExceptionInjectionType.Transaction_Pause_After_Lock_Acquisition;

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
            ExceptionInjectionHelper.DisableException(RestorePause);
            ExceptionInjectionHelper.DisableException(PromotePause);
            ExceptionInjectionHelper.DisableException(PublishPause);
            ExceptionInjectionHelper.DisableException(RetirementWait);
            ExceptionInjectionHelper.DisableException(CheckpointWait);
            ExceptionInjectionHelper.DisableException(CheckpointWaitForTree);
            ExceptionInjectionHelper.DisableException(CreatePause);
            ExceptionInjectionHelper.DisableException(TransactionPause);

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
        /// Flushing a stub whose tree is not restored, such as one just promoted, snapshots the key's working file under the
        /// RangeIndex lock, which a restore of that tree holds exclusively. The flush runs in an epoch drain, possibly on a thread
        /// holding a bucket latch the restore needs, and holds up every other epoch action while it runs, so it must not wait for
        /// the lock: the snapshot is taken once the restore releases it.
        /// </summary>
        [Test]
        public async Task FlushDoesNotWaitForRestoreHoldingLockAsync()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            const string Key = "rikey";
            var store = server.Provider.StoreWrapper.store;
            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redisRestorer = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var dbRestorer = redisRestorer.GetDatabase(0);

            CreateIndex(db, Key);
            EvictToDisk(Key);
            var flushSnapshots = CountFlushSnapshots(Key);

            ExceptionInjectionHelper.EnableException(RestorePause);

            // The restorer promotes the evicted stub to the tail, then parks restoring its tree, holding the exclusive lock.
            var restorer = dbRestorer.ExecuteAsync("RI.GET", Key, "field1");
            await ExceptionInjectionHelper.WaitOnClearAsync(RestorePause).WaitAsync(WaitTimeout);

            // Flush the promoted stub.
            var flush = Task.Run(() => store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true));
            var flushRanAlongside = await CompletesWithinAsync(flush, NotBlockedTimeout);

            ClassicAssert.AreEqual("value1", (string)await ReleaseParkedAndAwaitAsync(restorer));
            await flush.WaitAsync(WaitTimeout);

            ClassicAssert.IsTrue(flushRanAlongside, "Flushing a stub whose tree was being restored waited for the RangeIndex lock held by the restore");

            // The flushed stub's snapshot is taken once the restore releases the lock.
            ClassicAssert.IsTrue(SpinWait.SpinUntil(() => rangeIndexManager.PendingFlushSnapshotCount == 0, WaitTimeout),
                "The flush snapshot deferred while the lock was held was not taken");
            ClassicAssert.AreEqual(flushSnapshots + 1, CountFlushSnapshots(Key), "The flushed stub has no flush snapshot");

            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "The restored tree is not registered");
            ClassicAssert.AreEqual("OK", (string)db.Execute("RI.SET", Key, "field2", "value2"));
            ClassicAssert.AreEqual("value2", (string)db.Execute("RI.GET", Key, "field2"));
        }

        /// <summary>
        /// The promote of a stub whose tree was evicted stages the key's working file, then copies the stub to the tail. An
        /// eviction of the source stub in between must not leave the copy without a pending entry for the key, which is what a
        /// checkpoint taken before the copy's tree is restored captures the tree from.
        /// </summary>
        [Test]
        public async Task CheckpointCapturesPromotedTreeWhoseSourceWasEvictedAsync()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            const string Key = "rikey";

            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, copyReadsToTail: true);
            server.Start();

            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            using var redisPromoter = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var dbPromoter = redisPromoter.GetDatabase(0);

            CreateIndex(db, Key);
            EvictToDisk(Key);

            // EXISTS reads the evicted stub from disk and copies it to the tail, still flushed, so that it is in memory.
            ClassicAssert.IsTrue(db.KeyExists(Key));

            ExceptionInjectionHelper.EnableException(PromotePause);

            // The promoter stages the working file from the stub's flush snapshot, then parks before copying the stub to the tail.
            var promoter = dbPromoter.ExecuteAsync("RI.GET", Key, "field1");
            await ExceptionInjectionHelper.WaitOnClearAsync(PromotePause).WaitAsync(WaitTimeout);

            // Evict the stub being promoted.
            EvictToDisk(Key);

            // The promoter copies the stub to the tail, then parks before it reads the copy and restores its tree.
            ExceptionInjectionHelper.EnableException(Pause);
            ExceptionInjectionHelper.EnableException(PromotePause);
            await ExceptionInjectionHelper.WaitOnClearAsync(Pause).WaitAsync(WaitTimeout);
            ExceptionInjectionHelper.DisableException(PromotePause);

            var registered = rangeIndexManager.LiveIndexCount == 1;

            ClassicAssert.AreEqual("OK", (string)await db.ExecuteAsync("SAVE").WaitAsync(WaitTimeout));
            var captured = Directory.EnumerateFiles(TestUtils.MethodTestDir, RangeIndexManager.HashKeyToPrefix(Encoding.ASCII.GetBytes(Key)) + ".bftree",
                SearchOption.AllDirectories).Any();

            ClassicAssert.AreEqual("value1", (string)await ReleaseParkedAndAwaitAsync(promoter));

            ClassicAssert.IsTrue(registered, "The copy of the promoted stub, whose tree is not restored yet, has no entry for the key");
            ClassicAssert.IsTrue(captured, "A checkpoint taken before the promoted stub's tree was restored did not capture the tree");

            // A write after the checkpoint is not in it, and recovering from it restores the tree as the checkpoint captured it.
            ClassicAssert.AreEqual("OK", (string)db.Execute("RI.SET", Key, "field2", "value2"));

            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, tryRecover: true);
            server.Start();

            using var redisRecovered = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var dbRecovered = redisRecovered.GetDatabase(0);
            ClassicAssert.AreEqual("value1", (string)dbRecovered.Execute("RI.GET", Key, "field1"));
            ClassicAssert.IsTrue(dbRecovered.Execute("RI.GET", Key, "field2").IsNull, "The recovered tree has a write made after the checkpoint");
        }

        /// <summary>
        /// Registering a tree for a key whose tree is already registered keeps the registered tree and disposes the new one
        /// once no reader can be using it, which is deferred on the store epoch. The registration's callers do not hold the
        /// epoch, so the deferral must take it.
        /// </summary>
        [Test]
        public void DuplicateRegistrationDisposesNewTreeAfterReaders()
        {
            const string Key = "rikey";
            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;
            var keyBytes = Encoding.ASCII.GetBytes(Key);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            CreateIndex(db, Key);

            // Memory-backed, so that it does not open the key's working file
            var duplicate = rangeIndexManager.CreateBfTree(StorageBackendType.Memory, keyBytes, cacheSize: 65536, minRecordSize: 8,
                maxRecordSize: 1024, maxKeyLen: 128, leafPageSize: RangeIndexManager.ComputeLeafPageSize(1024));
            try
            {
                rangeIndexManager.RegisterIndex(duplicate, SpanByteComparer.StaticGetHashCode64(keyBytes), keyBytes);
            }
            catch
            {
                duplicate.Dispose();
                throw;
            }

            ClassicAssert.IsFalse(server.Provider.StoreWrapper.DefaultDatabase.Epoch.ThisInstanceProtected(), "The registration left the epoch held");
            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount);
            ClassicAssert.AreEqual("value1", (string)db.Execute("RI.GET", Key, "field1"), "The registration replaced the key's tree");
            ClassicAssert.IsTrue(SpinWait.SpinUntil(() => duplicate.NativePtr == nint.Zero, WaitTimeout), "The new tree was not disposed");
        }

        /// <summary>
        /// A restore whose stub is flushed meanwhile copies the stub to the tail with the restored tree, as a promote does. The
        /// old copy must then be marked transferred, so that its eviction leaves the key's entry, which by then holds the live
        /// tree; and the new copy, which has no flush snapshot at its address, must not be flushed, or it is promoted again.
        /// </summary>
        [Test]
        public async Task EvictionOfRestoredStubsOldCopyKeepsTreeAsync()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            const string Key = "rikey";
            var store = server.Provider.StoreWrapper.store;
            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            using var redisRestorer = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var dbRestorer = redisRestorer.GetDatabase(0);

            CreateIndex(db, Key);
            EvictToDisk(Key);

            ExceptionInjectionHelper.EnableException(RestorePause);

            // The restorer promotes the evicted stub to the tail, then parks restoring its tree.
            var restorer = dbRestorer.ExecuteAsync("RI.GET", Key, "field1");
            await ExceptionInjectionHelper.WaitOnClearAsync(RestorePause).WaitAsync(WaitTimeout);

            // Flush the promoted stub, so that the restore copies it to the tail.
            await Task.Run(() => store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true)).WaitAsync(WaitTimeout);
            var endOfOldCopy = store.Log.TailAddress;

            ClassicAssert.AreEqual("value1", (string)await ReleaseParkedAndAwaitAsync(restorer));

            var stubs = ReadStubs(store, Key);
            ClassicAssert.IsTrue(stubs.Single(s => s.Address < endOfOldCopy).Stub.IsTransferred, "The old copy of a restored stub is not marked transferred");
            ClassicAssert.AreEqual(1, stubs.Count(s => s.Address >= endOfOldCopy), "The restore's copy of a flushed stub was promoted again");

            // Evict the old copy, but not the restore's.
            store.Log.ShiftHeadAddress(endOfOldCopy, wait: true);
            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "Evicting the old copy of a restored stub removed the key's tree");

            ClassicAssert.AreEqual("OK", (string)db.Execute("SAVE"));
            ClassicAssert.IsTrue(IsCapturedByCheckpoint(Key), "A checkpoint did not capture the restored tree");

            ClassicAssert.IsTrue(db.KeyDelete(Key));
            ClassicAssert.AreEqual(0, rangeIndexManager.LiveIndexCount, "DEL did not retire the restored tree");
        }

        /// <summary>
        /// The eviction of a stub that a promote is about to copy defers its retirement while the key's lock is held, so the
        /// retirement can run after the copy has registered the key's pending entry. It must leave that entry, which belongs
        /// to the copy, and from which a checkpoint taken before the copy's tree is restored captures the tree.
        /// </summary>
        [Test]
        public async Task DeferredRetirementOfPromotedStubKeepsCopysEntryAsync()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            const string Key = "rikey";

            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, copyReadsToTail: true);
            server.Start();

            var store = server.Provider.StoreWrapper.store;
            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            using var redisPromoter = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var dbPromoter = redisPromoter.GetDatabase(0);

            CreateIndex(db, Key);
            EvictToDisk(Key);

            // EXISTS reads the evicted stub from disk and copies it to the tail, still flushed, so that it is in memory.
            ClassicAssert.IsTrue(db.KeyExists(Key));

            ExceptionInjectionHelper.EnableException(PromotePause);

            // The promoter stages the working file from the stub's flush snapshot, then parks before copying the stub to the tail.
            var promoter = dbPromoter.ExecuteAsync("RI.GET", Key, "field1");
            await ExceptionInjectionHelper.WaitOnClearAsync(PromotePause).WaitAsync(WaitTimeout);

            WithLockHeld(Key, () =>
            {
                // Evict the stub, which defers its retirement.
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                store.Log.ShiftHeadAddress(store.Log.TailAddress, wait: true);

                // The promoter copies the stub to the tail, which registers the key's pending entry, then fails before it
                // restores the tree, once it gets the lock.
                ExceptionInjectionHelper.EnableException(Fail);
                ExceptionInjectionHelper.EnableException(PromotePause);
                ClassicAssert.IsTrue(SpinWait.SpinUntil(() => rangeIndexManager.LiveIndexCount == 1, WaitTimeout), "The promote did not register the key's pending entry");
                ExceptionInjectionHelper.DisableException(PromotePause);
            });

            _ = Assert.CatchAsync<RedisException>(() => promoter.WaitAsync(WaitTimeout));
            rangeIndexManager.WaitForRetirement(Encoding.ASCII.GetBytes(Key));

            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "The deferred retirement of the promoted stub removed the copy's entry");

            ClassicAssert.AreEqual("OK", (string)db.Execute("SAVE"));
            ClassicAssert.IsTrue(IsCapturedByCheckpoint(Key), "A checkpoint taken before the promoted stub's tree was restored did not capture the tree");
            ClassicAssert.AreEqual("value1", (string)db.Execute("RI.GET", Key, "field1"));
        }

        /// <summary>
        /// A restore registers the key's tree and then, without the key's lock, publishes the tree's handle in the stub. An
        /// eviction of the stub in between sees a stub whose tree is not restored, and must leave the key's entry, which is
        /// already the restored tree's.
        /// </summary>
        [Test]
        public async Task EvictionBeforeRestorePublishesTreeKeepsTreeAsync()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            const string Key = "rikey";
            var store = server.Provider.StoreWrapper.store;
            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redisRestorer = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var dbRestorer = redisRestorer.GetDatabase(0);

            CreateIndex(db, Key);
            EvictToDisk(Key);

            ExceptionInjectionHelper.EnableException(PublishPause);

            // The restorer promotes the evicted stub to the tail, restores and registers its tree, then parks before publishing it.
            var restorer = dbRestorer.ExecuteAsync("RI.GET", Key, "field1");
            await ExceptionInjectionHelper.WaitOnClearAsync(PublishPause).WaitAsync(WaitTimeout);
            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount);

            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            store.Log.ShiftHeadAddress(store.Log.TailAddress, wait: true);
            var registered = rangeIndexManager.LiveIndexCount == 1;

            ClassicAssert.AreEqual("value1", (string)await ReleaseParkedAndAwaitAsync(restorer));
            ClassicAssert.IsTrue(registered, "Evicting a stub whose tree a restore was about to publish removed the tree's entry");

            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount);
            ClassicAssert.AreEqual("OK", (string)db.Execute("RI.SET", Key, "field2", "value2"));
            ClassicAssert.AreEqual("value2", (string)db.Execute("RI.GET", Key, "field2"));

            ClassicAssert.IsTrue(db.KeyDelete(Key));
            ClassicAssert.AreEqual(0, rangeIndexManager.LiveIndexCount, "DEL did not retire the restored tree");
        }

        /// <summary>
        /// A stub flushed while its tree is not restored and the key's lock is held has its flush snapshot deferred. Evicted and
        /// copied back to the tail before that snapshot is taken, the copy has none and relies on the key's working file, so it
        /// needs the key's pending entry, which the eviction's retirement removes, for a checkpoint to capture its tree.
        /// </summary>
        [Test]
        public async Task CopyOfStubWithPendingFlushSnapshotKeepsEntryAsync()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            const string Key = "rikey";

            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, copyReadsToTail: true);
            server.Start();

            var store = server.Provider.StoreWrapper.store;
            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            using var redisPromoter = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var dbPromoter = redisPromoter.GetDatabase(0);

            CreateIndex(db, Key);
            EvictToDisk(Key);

            // The promoter copies the evicted stub to the tail, which registers the key's pending entry, then fails before it
            // restores the tree.
            ExceptionInjectionHelper.EnableException(PromotePause);
            var promoter = dbPromoter.ExecuteAsync("RI.GET", Key, "field1");
            await ExceptionInjectionHelper.WaitOnClearAsync(PromotePause).WaitAsync(WaitTimeout);

            ExceptionInjectionHelper.EnableException(Fail);
            ExceptionInjectionHelper.EnableException(PromotePause);
            _ = Assert.CatchAsync<RedisException>(() => promoter.WaitAsync(WaitTimeout));
            ExceptionInjectionHelper.DisableException(PromotePause);
            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "The promote did not register the key's pending entry");

            WithLockHeld(Key, () =>
            {
                // Flush the promoted stub, which defers its snapshot, and evict it, which defers its retirement.
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                ClassicAssert.AreEqual(1, rangeIndexManager.PendingFlushSnapshotCount, "The flush snapshot was not deferred");
                store.Log.ShiftHeadAddress(store.Log.TailAddress, wait: true);

                // EXISTS reads the evicted stub from disk and copies it to the tail, before its flush snapshot is taken.
                var tailBeforeCopy = store.Log.TailAddress;
                ClassicAssert.IsTrue(db.KeyExists(Key));
                ClassicAssert.Greater(store.Log.TailAddress, tailBeforeCopy, "EXISTS did not copy the stub to the tail");
            });

            rangeIndexManager.WaitForRetirement(Encoding.ASCII.GetBytes(Key));
            ClassicAssert.IsTrue(SpinWait.SpinUntil(() => rangeIndexManager.PendingFlushSnapshotCount == 0, WaitTimeout),
                "The flush snapshot deferred while the lock was held was not taken");

            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "The copy of a stub evicted before its flush snapshot was taken has no entry for the key");

            ClassicAssert.AreEqual("OK", (string)db.Execute("SAVE"));
            ClassicAssert.IsTrue(IsCapturedByCheckpoint(Key), "A checkpoint taken before the copy's tree was restored did not capture the tree");
            ClassicAssert.AreEqual("value1", (string)db.Execute("RI.GET", Key, "field1"));
        }

        /// <summary>
        /// DEL that gets the RangeIndex lock at once retires the tree there and then, but disposes it and deletes the key's
        /// working file only once every thread holding the store epoch has moved on. Recreating the key must wait for that, as
        /// it does for a deferred retirement, or the new tree opens the old working file, which the cleanup then deletes.
        /// </summary>
        [Test]
        public async Task RecreateWaitsForCleanupOfDeletedTreeAsync()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            const string Key = "rikey";
            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;
            var dataPath = rangeIndexManager.LogDataPathFor(Encoding.ASCII.GetBytes(Key));

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            CreateIndex(db, Key);

            Task<RedisResult> recreate;
            bool recreateWaited;

            // A thread holding the store epoch keeps the deleted tree's cleanup from running.
            using (new EpochHolder(server.Provider.StoreWrapper.DefaultDatabase.Epoch))
            {
                ClassicAssert.IsTrue(db.KeyDelete(Key));
                ClassicAssert.AreEqual(0, rangeIndexManager.LiveIndexCount, "DEL did not retire the tree at once");

                ExceptionInjectionHelper.EnableException(RetirementWait);
                recreate = db.ExecuteAsync("RI.CREATE", Key, "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
                var waiting = ExceptionInjectionHelper.WaitOnClearAsync(RetirementWait);
                recreateWaited = await Task.WhenAny(recreate, waiting).WaitAsync(WaitTimeout) == waiting;
            }

            ClassicAssert.AreEqual("OK", (string)await recreate.WaitAsync(WaitTimeout));
            ClassicAssert.IsTrue(recreateWaited, "RI.CREATE did not wait for the cleanup of the deleted key's tree");

            ClassicAssert.IsTrue(File.Exists(dataPath), "The cleanup of the deleted tree deleted the recreated tree's working file");
            ClassicAssert.IsTrue(db.Execute("RI.GET", Key, "field1").IsNull, "The recreated index has the deleted index's data");
            ClassicAssert.AreEqual("OK", (string)db.Execute("RI.SET", Key, "field2", "value2"));
            ClassicAssert.AreEqual("value2", (string)db.Execute("RI.GET", Key, "field2"));
        }

        /// <summary>
        /// A fold-over checkpoint persists its version by flushing the log to its tail, which flushes the stubs on those pages.
        /// Recovery from the checkpoint restores a flushed stub's tree from its flush snapshot, and the snapshot of a stub whose
        /// tree is not restored is deferred while the key's RangeIndex lock is held, so the checkpoint must not complete before
        /// the snapshots its flush deferred are taken.
        /// </summary>
        [Test]
        public async Task FoldOverCheckpointWaitsForDeferredFlushSnapshotAsync()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            const string Key = "rikey";

            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, useFoldOverCheckpoints: true);
            server.Start();

            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            using var redisRestorer = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var dbRestorer = redisRestorer.GetDatabase(0);

            CreateIndex(db, Key);
            EvictToDisk(Key);
            var flushSnapshots = CountFlushSnapshots(Key);

            ExceptionInjectionHelper.EnableException(RestorePause);

            // The restorer promotes the evicted stub to the tail, then parks restoring its tree, holding the exclusive lock.
            var restorer = dbRestorer.ExecuteAsync("RI.GET", Key, "field1");
            await ExceptionInjectionHelper.WaitOnClearAsync(RestorePause).WaitAsync(WaitTimeout);

            // The checkpoint flushes the promoted stub, whose snapshot is deferred until the restorer releases the lock.
            ExceptionInjectionHelper.EnableException(CheckpointWait);
            var save = db.ExecuteAsync("SAVE");
            var checkpointWaits = ExceptionInjectionHelper.WaitOnClearAsync(CheckpointWait);
            var first = await Task.WhenAny(save, checkpointWaits).WaitAsync(WaitTimeout);
            var pendingWhenCompleted = first == save ? rangeIndexManager.PendingFlushSnapshotCount : 0;

            ClassicAssert.AreEqual("value1", (string)await ReleaseParkedAndAwaitAsync(restorer));
            ClassicAssert.AreEqual("OK", (string)await save.WaitAsync(WaitTimeout));

            ClassicAssert.IsTrue(first != save || pendingWhenCompleted == 0,
                "The fold-over checkpoint completed while the flush snapshot of a stub it flushed was still to be taken");
            ClassicAssert.AreEqual(0, rangeIndexManager.PendingFlushSnapshotCount, "A flush snapshot is still pending after the checkpoint");
            ClassicAssert.AreEqual(flushSnapshots + 1, CountFlushSnapshots(Key), "The stub the checkpoint flushed has no flush snapshot");

            // Recovering from the checkpoint restores the flushed stub's tree from that snapshot.
            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, useFoldOverCheckpoints: true, tryRecover: true);
            server.Start();

            using var redisRecovered = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            ClassicAssert.AreEqual("value1", (string)redisRecovered.GetDatabase(0).Execute("RI.GET", Key, "field1"));
        }

        /// <summary>
        /// A data operation whose key's tree a checkpoint has yet to snapshot releases the RangeIndex lock and waits until the
        /// checkpoint has taken that snapshot, which the checkpoint marks taken only on the trees still registered. If the
        /// key's tree is retired meanwhile, here by evicting its stub, the operation must stop waiting and retry, which finds
        /// the stub evicted and restores the tree from its flush snapshot.
        /// </summary>
        [Test]
        public async Task ReadWaitingForCheckpointProceedsWhenTreeIsRetiredAsync()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            const string Key = "rikey";
            const string TransactionKey = "txnkey";

            // The default index size of TestUtils.CreateGarnetServer, 1m, in buckets of 64 bytes
            const long IndexBuckets = (1L << 20) / 64;

            var store = server.Provider.StoreWrapper.store;
            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;
            var driver = server.Provider.StoreWrapper.DefaultDatabase.StateMachineDriver;
            var keyBytes = Encoding.ASCII.GetBytes(Key);

            // The transaction that holds up the checkpoint must not lock the bucket of the key the reader reads.
            ClassicAssert.AreNotEqual(SpanByteComparer.StaticGetHashCode64(keyBytes) & (IndexBuckets - 1),
                SpanByteComparer.StaticGetHashCode64(Encoding.ASCII.GetBytes(TransactionKey)) & (IndexBuckets - 1),
                "The transaction's key shares the RangeIndex key's bucket");

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            using var redisTransaction = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redisReader = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            CreateIndex(db, Key);
            ClassicAssert.IsTrue(rangeIndexManager.TryGetEntry(keyBytes, out var entry), "The key's tree is not registered");

            Task<bool> exec = null;
            Task<RedisResult> reader = null;
            bool readerFinished;

            ExceptionInjectionHelper.EnableException(TransactionPause);
            try
            {
                // The transaction parks with its version verified, so a checkpoint waits for it in IN_PROGRESS, its barrier set.
                var transaction = redisTransaction.GetDatabase(0).CreateTransaction();
                _ = transaction.StringSetAsync(TransactionKey, "value");
                exec = transaction.ExecuteAsync();
                await ExceptionInjectionHelper.WaitOnClearAsync(TransactionPause).WaitAsync(WaitTimeout);

                ClassicAssert.AreEqual("Background saving started", (string)await db.ExecuteAsync("BGSAVE").WaitAsync(WaitTimeout));
                ClassicAssert.IsTrue(SpinWait.SpinUntil(() => driver.SystemState.Phase == Phase.IN_PROGRESS, WaitTimeout),
                    "The checkpoint did not reach IN_PROGRESS");
                ClassicAssert.AreEqual(1, Volatile.Read(ref entry.SnapshotPending), "The checkpoint did not mark the key's tree for its snapshot");

                // The reader releases the lock to wait for the checkpoint's snapshot of the key's tree.
                ExceptionInjectionHelper.EnableException(CheckpointWaitForTree);
                reader = redisReader.GetDatabase(0).ExecuteAsync("RI.GET", Key, "field1");
                await ExceptionInjectionHelper.WaitOnClearAsync(CheckpointWaitForTree).WaitAsync(WaitTimeout);

                // Evicting the stub retires the key's tree before the checkpoint snapshots it.
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                store.Log.ShiftHeadAddress(store.Log.TailAddress, wait: true);
                ClassicAssert.IsFalse(rangeIndexManager.TryGetEntry(keyBytes, out var current) && ReferenceEquals(current, entry),
                    "Eviction did not retire the key's tree");

                readerFinished = await CompletesWithinAsync(reader, NotBlockedTimeout);
            }
            finally
            {
                // Release a reader still waiting for the retired tree, then the transaction, which lets the checkpoint finish.
                Volatile.Write(ref entry.SnapshotPending, 0);

                ExceptionInjectionHelper.SuspendParking();
                try
                {
                    if (exec != null)
                        _ = await exec.WaitAsync(WaitTimeout);
                }
                finally
                {
                    ExceptionInjectionHelper.ResumeParking();
                }
            }

            ClassicAssert.AreEqual("value1", (string)await reader.WaitAsync(WaitTimeout));
            ClassicAssert.IsTrue(readerFinished, "A read waiting for the checkpoint's snapshot of a tree retired meanwhile did not finish");
            ClassicAssert.IsTrue(SpinWait.SpinUntil(() => driver.SystemState.Phase == Phase.REST, WaitTimeout), "The checkpoint did not complete");

            ClassicAssert.AreEqual(1, rangeIndexManager.LiveIndexCount, "The restored tree is not registered");
            ClassicAssert.AreEqual("OK", (string)db.Execute("RI.SET", Key, "field2", "value2"));
            ClassicAssert.AreEqual("value2", (string)db.Execute("RI.GET", Key, "field2"));
        }

        /// <summary>
        /// RI.CREATE over an expired RangeIndex key whose record is in the mutable region must create a new index. Tsavorite
        /// would reinitialize the expired record in place, writing the new tree into it, and the expired tree's retirement
        /// deletes the key's working file, which the new tree must not have opened before.
        /// </summary>
        [Test]
        public void CreateOverExpiredKeyInMutableRegionCreatesNewIndex()
        {
            const string Key = "rikey";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            CreateIndex(db, Key);
            ExpireNow(db, Key);

            AssertCreateMakesNewIndex(db, Key);
        }

        /// <summary>
        /// RI.CREATE over an expired RangeIndex key whose record is in the read-only region must create a new index, rather
        /// than report that the index exists.
        /// </summary>
        [Test]
        public void CreateOverExpiredKeyInReadOnlyRegionCreatesNewIndex()
        {
            const string Key = "rikey";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            CreateIndex(db, Key);
            ExpireNow(db, Key);

            var store = server.Provider.StoreWrapper.store;
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            AssertCreateMakesNewIndex(db, Key);
        }

        /// <summary>
        /// RI.CREATE over a deleted RangeIndex key whose tombstone is revivified must register the new tree. Tsavorite reports
        /// a revivified record as updated in place, as it does an existing one.
        /// </summary>
        [Test]
        public void CreateOverRevivifiedTombstoneRegistersNewTree()
        {
            const string Key = "rikey";

            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, useReviv: true, useInChainRevivOnly: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            CreateIndex(db, Key);

            // A deleted record stays in its chain, where it can be revivified, only if an older record of the key is below it:
            // flush the stub, then promote it to the tail.
            var store = server.Provider.StoreWrapper.store;
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            ClassicAssert.AreEqual("value1", (string)db.Execute("RI.GET", Key, "field1"));
            ClassicAssert.IsTrue(db.KeyDelete(Key));

            AssertCreateMakesNewIndex(db, Key);
        }

        /// <summary>
        /// AOF replay of an RI.CREATE that created an index over an expired key must create it as well.
        /// </summary>
        [Test]
        public void ReplayOfCreateOverExpiredKeyCreatesNewIndex()
        {
            const string Key = "rikey";

            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, enableAOF: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                CreateIndex(db, Key);
                ExpireNow(db, Key);
                AssertCreateMakesNewIndex(db, Key);
                _ = db.Execute("COMMITAOF");
            }

            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true, enableAOF: true, tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                RedisResult value = null;
                Assert.DoesNotThrow(() => value = db.Execute("RI.GET", Key, "field2"), "The replayed RI.CREATE did not create the index");
                ClassicAssert.AreEqual("value2", (string)value);
                ClassicAssert.IsTrue(db.Execute("RI.GET", Key, "field1").IsNull, "The replayed index has the expired index's data");
            }
        }

        /// <summary>
        /// An RI.CREATE that found no record of the key, and then finds one that has expired when it stores its stub, must not
        /// store the stub in that record: it removes the record and creates the index once the record's tree is retired.
        /// </summary>
        [Test]
        public async Task CreateOverKeyExpiredDuringCreateCreatesNewIndexAsync()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            const string Key = "rikey";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            using var otherRedis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var otherDb = otherRedis.GetDatabase(0);

            ExceptionInjectionHelper.EnableException(CreatePause);
            var create = db.ExecuteAsync("RI.CREATE", Key, "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
            await ExceptionInjectionHelper.WaitOnClearAsync(CreatePause).WaitAsync(WaitTimeout);

            // Another client creates the key, which then expires, while the first RI.CREATE is paused.
            CreateIndex(otherDb, Key);
            ExpireNow(otherDb, Key);

            RedisResult created = null;
            RedisServerException error = null;
            try
            {
                created = await ReleaseParkedAndAwaitAsync(create);
            }
            catch (RedisServerException ex)
            {
                error = ex;
            }

            ClassicAssert.IsNull(error, $"RI.CREATE over a key that expired during the command failed: {error?.Message}");
            ClassicAssert.AreEqual("OK", (string)created);
            AssertKeyHoldsNewIndex(db, Key);
        }

        /// <summary>
        /// Publishing a migrated RangeIndex over an expired key must register the published tree, and must not replace the key's
        /// working file until the expired record's tree, which uses that file, is retired.
        /// </summary>
        [Test]
        public void PublishOfMigratedIndexOverExpiredKeyRegistersTree()
        {
            const string SourceKey = "risource";
            const string Key = "rikey";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            ClassicAssert.AreEqual("OK", (string)db.Execute("RI.CREATE", SourceKey, "DISK", "CACHESIZE", "65536", "MINRECORD", "8"));
            ClassicAssert.AreEqual("OK", (string)db.Execute("RI.SET", SourceKey, "migrated", "value"));

            CreateIndex(db, Key);
            ExpireNow(db, Key);

            var storeWrapper = server.Provider.StoreWrapper;
            var rangeIndexManager = storeWrapper.rangeIndexManager;
            using (var localSession = new LocalServerSession(storeWrapper))
            {
                var sourceKeyBytes = Encoding.ASCII.GetBytes(SourceKey);
                string snapshotPath;
                byte[] stubBytes;
                unsafe
                {
                    fixed (byte* sourceKeyPtr = sourceKeyBytes)
                    {
                        ClassicAssert.IsTrue(rangeIndexManager.SnapshotForMigration(localSession.storageSession,
                            PinnedSpanByte.FromPinnedPointer(sourceKeyPtr, sourceKeyBytes.Length), out snapshotPath, out _, out stubBytes));
                    }
                }

                ref var ctx = ref localSession.storageSession.stringBasicContext;
                ClassicAssert.AreEqual(RangeIndexManager.PublishMigratedIndexResult.Success,
                    rangeIndexManager.PublishMigratedIndex(Encoding.ASCII.GetBytes(Key), stubBytes, snapshotPath, replaceOption: false, ref ctx, appendOnlyFile: null));
            }

            ClassicAssert.AreEqual("value", (string)db.Execute("RI.GET", Key, "migrated"));
            AssertKeyHoldsNewIndex(db, Key);
        }

        /// <summary>
        /// RI.CREATE of a key that holds a live index must fail without creating a tree: a new disk-backed tree writes its root
        /// page to the key's working file, which the live tree uses.
        /// </summary>
        [Test]
        public void DuplicateCreateDoesNotWriteIntoLiveIndexFile()
        {
            const string Key = "rikey";
            var dataPath = server.Provider.StoreWrapper.rangeIndexManager.LogDataPathFor(Encoding.ASCII.GetBytes(Key));

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            CreateIndex(db, Key);
            var length = new FileInfo(dataPath).Length;

            var error = Assert.Throws<RedisServerException>(() => db.Execute("RI.CREATE", Key, "DISK", "CACHESIZE", "65536", "MINRECORD", "8"));
            ClassicAssert.AreEqual("ERR index already exists", error.Message);
            ClassicAssert.AreEqual(length, new FileInfo(dataPath).Length, "The duplicate RI.CREATE wrote into the live index's working file");
            ClassicAssert.AreEqual("value1", (string)db.Execute("RI.GET", Key, "field1"));
        }

        /// <summary>
        /// Sets the key's expiration in the past, so that the key expires at once and its record stays in the store.
        /// </summary>
        private static void ExpireNow(IDatabase db, string key)
        {
            ClassicAssert.AreEqual(1, (int)db.Execute("PEXPIREAT", key, "1"));
            ClassicAssert.IsFalse(db.KeyExists(key), "The key did not expire");
        }

        /// <summary>
        /// Runs RI.CREATE over the key, which does not hold a live index, and checks that it makes a new index.
        /// </summary>
        private void AssertCreateMakesNewIndex(IDatabase db, string key)
        {
            RedisResult created = null;
            Assert.DoesNotThrow(() => created = db.Execute("RI.CREATE", key, "DISK", "CACHESIZE", "65536", "MINRECORD", "8"),
                "RI.CREATE over a key without a live index failed");
            ClassicAssert.AreEqual("OK", (string)created);

            AssertKeyHoldsNewIndex(db, key);
        }

        /// <summary>
        /// Checks that the key holds a new index, without the data of the index created by <see cref="CreateIndex"/>: the key's
        /// record holds the registered tree, whose working file remains once every earlier tree of the key is retired.
        /// </summary>
        private void AssertKeyHoldsNewIndex(IDatabase db, string key)
        {
            var keyBytes = Encoding.ASCII.GetBytes(key);
            var rangeIndexManager = server.Provider.StoreWrapper.rangeIndexManager;

            ClassicAssert.IsTrue(rangeIndexManager.TryGetEntry(keyBytes, out var entry) && entry.Tree != null, "The new tree is not registered");
            var stubs = ReadStubs(server.Provider.StoreWrapper.store, key);
            ClassicAssert.AreEqual(entry.Tree.NativePtr, stubs[^1].Stub.TreeHandle, "The key's record does not hold the registered tree");

            rangeIndexManager.WaitForRetirement(keyBytes);
            ClassicAssert.IsTrue(File.Exists(rangeIndexManager.LogDataPathFor(keyBytes)), "Retiring an earlier tree of the key deleted the new tree's working file");

            ClassicAssert.IsTrue(db.Execute("RI.GET", key, "field1").IsNull, "The new index has the earlier index's data");
            ClassicAssert.AreEqual("OK", (string)db.Execute("RI.SET", key, "field2", "value2"));
            ClassicAssert.AreEqual("value2", (string)db.Execute("RI.GET", key, "field2"));
        }

        /// <summary>
        /// Number of flush snapshots of the RangeIndex key.
        /// </summary>
        private static int CountFlushSnapshots(string key)
            => Directory.GetFiles(TestUtils.MethodTestDir, RangeIndexManager.HashKeyToPrefix(Encoding.ASCII.GetBytes(key)) + ".*.flush.bftree",
                SearchOption.AllDirectories).Length;

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

        /// <summary>
        /// The stubs of the RangeIndex key's in-memory records that are neither elided nor tombstones, with their addresses,
        /// oldest first.
        /// </summary>
        private static List<(long Address, RangeIndexManager.RangeIndexStub Stub)> ReadStubs<TStoreFunctions, TAllocator>(
            TsavoriteKV<TStoreFunctions, TAllocator> store, string key)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            var keyBytes = Encoding.ASCII.GetBytes(key);
            var stubs = new List<(long Address, RangeIndexManager.RangeIndexStub Stub)>();

            using var iter = store.Log.Scan(store.Log.HeadAddress, store.Log.TailAddress, DiskScanBufferingMode.NoBuffering, includeClosedRecords: true);
            while (iter.GetNext())
            {
                if (!iter.Info.Invalid && !iter.Info.Tombstone && iter.RecordType == RangeIndexManager.RangeIndexRecordType && iter.Key.SequenceEqual(keyBytes))
                    stubs.Add((iter.CurrentAddress, RangeIndexManager.ReadIndex(iter.ValueSpan)));
            }

            return stubs;
        }

        /// <summary>
        /// True if a checkpoint has a snapshot of the RangeIndex key's tree.
        /// </summary>
        private static bool IsCapturedByCheckpoint(string key)
            => Directory.EnumerateFiles(TestUtils.MethodTestDir, RangeIndexManager.HashKeyToPrefix(Encoding.ASCII.GetBytes(key)) + ".bftree",
                SearchOption.AllDirectories).Any();

        /// <summary>
        /// Runs <paramref name="action"/> holding the RangeIndex key's lock exclusively, as a restore of its tree does, which
        /// defers the key's flush snapshots and retirements until the lock is released.
        /// </summary>
        private void WithLockHeld(string key, Action action)
        {
            var keyHash = SpanByteComparer.StaticGetHashCode64(Encoding.ASCII.GetBytes(key));
            using (server.Provider.StoreWrapper.rangeIndexManager.AcquireExclusiveForDelete(keyHash))
                action();
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

        /// <summary>
        /// Holds the store epoch on a thread of its own until disposed, which keeps epoch actions queued meanwhile from running.
        /// </summary>
        private sealed class EpochHolder : IDisposable
        {
            private readonly ManualResetEventSlim held = new();
            private readonly ManualResetEventSlim release = new();
            private readonly Thread thread;

            public EpochHolder(LightEpoch epoch)
            {
                thread = new Thread(() =>
                {
                    epoch.Resume();
                    try
                    {
                        held.Set();
                        release.Wait();
                    }
                    finally
                    {
                        epoch.Suspend();
                    }
                })
                { IsBackground = true, Name = nameof(EpochHolder) };
                thread.Start();

                if (!held.Wait(WaitTimeout))
                {
                    release.Set();
                    throw new TimeoutException("The epoch holder did not take the epoch");
                }
            }

            public void Dispose()
            {
                release.Set();
                thread.Join();
                held.Dispose();
                release.Dispose();
            }
        }
    }
}