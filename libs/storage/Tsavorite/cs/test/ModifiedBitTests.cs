// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using NUnit.Framework;
using Tsavorite.core;
using Tsavorite.test.LockTable;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.ModifiedBit
{
    internal class ModifiedBitTestComparer : ITsavoriteEqualityComparer<int>
    {
        public bool Equals(ref int k1, ref int k2) => k1 == k2;

        public long GetHashCode64(ref int k) => Utility.GetHashCode(k);
    }

    [TestFixture]
    class ModifiedBitTests
    {
        const int numRecords = 1000;
        const int valueMult = 1_000_000;


        ModifiedBitTestComparer comparer;

        private TsavoriteKV<int, int> store;
        private ClientSession<int, int, int, int, Empty, SimpleFunctions<int, int>> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: false);
            comparer = new ModifiedBitTestComparer();
            store = new TsavoriteKV<int, int>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 12, MemorySizeBits = 22 }, comparer: comparer, concurrencyControlMode: ConcurrencyControlMode.LockTable);
            session = store.NewSession<int, int, Empty, SimpleFunctions<int, int>>(new SimpleFunctions<int, int>());
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
        }

        void Populate()
        {
            for (int key = 0; key < numRecords; key++)
                Assert.IsFalse(session.Upsert(key, key * valueMult).IsPending);
        }

        void AssertLockandModified(LockableUnsafeContext<int, int, int, int, Empty, SimpleFunctions<int, int>> luContext, int key, bool xlock, bool slock, bool modified = false)
        {
            OverflowBucketLockTableTests.AssertLockCounts(store, ref key, xlock, slock);
            var isM = luContext.IsModified(key);
            Assert.AreEqual(modified, isM, "modified mismatch");
        }

        void AssertLockandModified(LockableContext<int, int, int, int, Empty, SimpleFunctions<int, int>> luContext, int key, bool xlock, bool slock, bool modified = false)
        {
            OverflowBucketLockTableTests.AssertLockCounts(store, ref key, xlock, slock);
            var isM = luContext.IsModified(key);
            Assert.AreEqual(modified, isM, "modified mismatch");
        }

        void AssertLockandModified(ClientSession<int, int, int, int, Empty, SimpleFunctions<int, int>> session, int key, bool xlock, bool slock, bool modified = false)
        {
            var luContext = session.LockableUnsafeContext;
            luContext.BeginUnsafe();

            OverflowBucketLockTableTests.AssertLockCounts(store, ref key, xlock, slock);
            var isM = luContext.IsModified(key);
            Assert.AreEqual(modified, isM, "Modified mismatch");

            luContext.EndUnsafe();
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void LockAndNotModify()
        {
            Populate();
            Random r = new(100);
            int key = r.Next(numRecords);
            session.ResetModified(key);

            var lContext = session.LockableContext;
            lContext.BeginLockable();
            AssertLockandModified(lContext, key, xlock: false, slock: false, modified: false);

            var keyVec = new[] { new FixedLengthLockableKeyStruct<int>(key, LockType.Exclusive, lContext) };

            lContext.Lock(keyVec);
            AssertLockandModified(lContext, key, xlock: true, slock: false, modified: false);

            lContext.Unlock(keyVec);
            AssertLockandModified(lContext, key, xlock: false, slock: false, modified: false);

            keyVec[0].LockType = LockType.Shared;

            lContext.Lock(keyVec);
            AssertLockandModified(lContext, key, xlock: false, slock: true, modified: false);

            lContext.Unlock(keyVec);
            AssertLockandModified(lContext, key, xlock: false, slock: false, modified: false);
            lContext.EndLockable();
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void ResetModifyForNonExistingKey()
        {
            Populate();
            int key = numRecords + 100;
            session.ResetModified(key);
            AssertLockandModified(session, key, xlock: false, slock: false, modified: false);
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void ModifyClientSession([Values(true, false)] bool flushToDisk, [Values] UpdateOp updateOp)
        {
            Populate();

            int key = numRecords - 500;
            int value = 14;
            session.ResetModified(key);
            AssertLockandModified(session, key, xlock: false, slock: false, modified: false);

            if (flushToDisk)
                store.Log.FlushAndEvict(wait: true);

            Status status = default;
            switch (updateOp)
            {
                case UpdateOp.Upsert:
                    status = session.Upsert(key, value);
                    break;
                case UpdateOp.RMW:
                    status = session.RMW(key, value);
                    break;
                case UpdateOp.Delete:
                    status = session.Delete(key);
                    break;
                default:
                    break;
            }
            if (flushToDisk)
            {
                switch (updateOp)
                {
                    case UpdateOp.RMW:
                        Assert.IsTrue(status.IsPending, status.ToString());
                        session.CompletePending(wait: true);
                        break;
                    default:
                        Assert.IsTrue(status.NotFound);
                        break;
                }
                (status, var _) = session.Read(key);
                Assert.IsTrue(status.Found || updateOp == UpdateOp.Delete);
            }

            if (updateOp == UpdateOp.Delete)
                AssertLockandModified(session, key, xlock: false, slock: false, modified: false);
            else
                AssertLockandModified(session, key, xlock: false, slock: false, modified: true);
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void ModifyLUC([Values(true, false)] bool flushToDisk, [Values] UpdateOp updateOp)
        {
            Populate();

            int key = numRecords - 500;
            int value = 14;
            session.ResetModified(key);
            var luContext = session.LockableUnsafeContext;
            luContext.BeginUnsafe();
            luContext.BeginLockable();
            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: false);
            luContext.EndLockable();
            luContext.EndUnsafe();

            if (flushToDisk)
                store.Log.FlushAndEvict(wait: true);

            Status status = default;

            luContext.BeginUnsafe();
            luContext.BeginLockable();

            var keyVec = new[] { new FixedLengthLockableKeyStruct<int>(key, LockType.Exclusive, luContext) };

            luContext.Lock(keyVec);

            switch (updateOp)
            {
                case UpdateOp.Upsert:
                    status = luContext.Upsert(key, value);
                    break;
                case UpdateOp.RMW:
                    status = luContext.RMW(key, value);
                    break;
                case UpdateOp.Delete:
                    status = luContext.Delete(key);
                    break;
                default:
                    break;
            }

            if (flushToDisk)
            {
                switch (updateOp)
                {
                    case UpdateOp.RMW:
                        Assert.IsTrue(status.IsPending, status.ToString());
                        luContext.CompletePending(wait: true);
                        break;
                    default:
                        Assert.IsTrue(status.NotFound);
                        break;
                }
            }

            luContext.Unlock(keyVec);

            if (flushToDisk)
            {
                keyVec[0].LockType = LockType.Shared;
                luContext.Lock(keyVec);
                (status, var _) = luContext.Read(key);
                Assert.AreEqual(updateOp != UpdateOp.Delete, status.Found, status.ToString());
                luContext.Unlock(keyVec);
            }

            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: updateOp != UpdateOp.Delete);

            luContext.EndLockable();
            luContext.EndUnsafe();
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void ModifyUC([Values(true, false)] bool flushToDisk, [Values] UpdateOp updateOp)
        {
            Populate();

            int key = numRecords - 500;
            int value = 14;
            session.ResetModified(key);
            AssertLockandModified(session, key, xlock: false, slock: false, modified: false);

            if (flushToDisk)
                store.Log.FlushAndEvict(wait: true);

            Status status = default;
            var unsafeContext = session.UnsafeContext;

            unsafeContext.BeginUnsafe();
            switch (updateOp)
            {
                case UpdateOp.Upsert:
                    status = unsafeContext.Upsert(key, value);
                    break;
                case UpdateOp.RMW:
                    status = unsafeContext.RMW(key, value);
                    break;
                case UpdateOp.Delete:
                    status = unsafeContext.Delete(key);
                    break;
                default:
                    break;
            }
            if (flushToDisk)
            {
                switch (updateOp)
                {
                    case UpdateOp.RMW:
                        Assert.IsTrue(status.IsPending, status.ToString());
                        unsafeContext.CompletePending(wait: true);
                        break;
                    default:
                        Assert.IsTrue(status.NotFound);
                        break;
                }
                (status, var _) = unsafeContext.Read(key);
                Assert.IsTrue(status.Found || updateOp == UpdateOp.Delete);
            }
            unsafeContext.EndUnsafe();

            AssertLockandModified(session, key, xlock: false, slock: false, modified: updateOp != UpdateOp.Delete);
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void ModifyLC([Values(true, false)] bool flushToDisk, [Values] UpdateOp updateOp)
        {
            Populate();

            int key = numRecords - 500;
            int value = 14;
            session.ResetModified(key);
            var lContext = session.LockableContext;
            lContext.BeginLockable();
            AssertLockandModified(lContext, key, xlock: false, slock: false, modified: false);

            var keyVec = new[] { new FixedLengthLockableKeyStruct<int>(key, LockType.Exclusive, lContext) };

            lContext.Lock(keyVec);

            if (flushToDisk)
                store.Log.FlushAndEvict(wait: true);

            Status status = default;

            switch (updateOp)
            {
                case UpdateOp.Upsert:
                    status = lContext.Upsert(key, value);
                    break;
                case UpdateOp.RMW:
                    status = lContext.RMW(key, value);
                    break;
                case UpdateOp.Delete:
                    status = lContext.Delete(key);
                    break;
                default:
                    break;
            }

            if (flushToDisk)
            {
                switch (updateOp)
                {
                    case UpdateOp.RMW:
                        Assert.IsTrue(status.IsPending, status.ToString());
                        lContext.CompletePending(wait: true);
                        break;
                    default:
                        Assert.IsTrue(status.NotFound);
                        break;
                }
            }

            lContext.Unlock(keyVec);

            if (flushToDisk)
            {
                keyVec[0].LockType = LockType.Shared;
                lContext.Lock(keyVec);
                (status, var _) = lContext.Read(key);
                Assert.AreEqual(updateOp != UpdateOp.Delete, status.Found, status.ToString());
                lContext.Unlock(keyVec);
            }

            AssertLockandModified(lContext, key, xlock: false, slock: false, modified: updateOp != UpdateOp.Delete);
            lContext.EndLockable();
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void CopyToTailTest()
        {
            Populate();
            store.Log.FlushAndEvict(wait: true);

            var luContext = session.LockableUnsafeContext;

            int input = 0, output = 0, key = 200;
            ReadOptions readOptions = new() { CopyOptions = new(ReadCopyFrom.AllImmutable, ReadCopyTo.MainLog) };

            luContext.BeginUnsafe();
            luContext.BeginLockable();
            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: true);

            var keyVec = new[] { new FixedLengthLockableKeyStruct<int>(key, LockType.Shared, luContext) };

            luContext.Lock(keyVec);
            AssertLockandModified(luContext, key, xlock: false, slock: true, modified: true);

            // Check Read Copy to Tail resets the modified
            var status = luContext.Read(ref key, ref input, ref output, ref readOptions, out _);
            Assert.IsTrue(status.IsPending, status.ToString());
            luContext.CompletePending(wait: true);

            luContext.Unlock(keyVec);
            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: true);

            // Check Read Copy to Tail resets the modified on locked key
            key += 10;
            keyVec[0] = new(key, LockType.Exclusive, luContext);
            luContext.Lock(keyVec);
            status = luContext.Read(ref key, ref input, ref output, ref readOptions, out _);
            Assert.IsTrue(status.IsPending, status.ToString());
            luContext.CompletePending(wait: true);
            AssertLockandModified(luContext, key, xlock: true, slock: false, modified: true);
            luContext.Unlock(keyVec);
            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: true);

            luContext.EndLockable();
            luContext.EndUnsafe();
        }
    }
}