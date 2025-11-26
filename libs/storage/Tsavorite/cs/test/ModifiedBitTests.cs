// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using Tsavorite.test.LockTable;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.ModifiedBit
{
    // Must be in a separate block so the "using StructStoreFunctions" is the first line in its namespace declaration.
    internal struct ModifiedBitTestComparer : IKeyComparer<int>
    {
        public readonly bool Equals(ref int k1, ref int k2) => k1 == k2;

        public readonly long GetHashCode64(ref int k) => Utility.GetHashCode(k);
    }
}

namespace Tsavorite.test.ModifiedBit
{
    using IntAllocator = BlittableAllocator<int, int, StoreFunctions<int, int, ModifiedBitTestComparer, DefaultRecordDisposer<int, int>>>;
    using IntStoreFunctions = StoreFunctions<int, int, ModifiedBitTestComparer, DefaultRecordDisposer<int, int>>;

    [TestFixture]
    class ModifiedBitTests
    {
        const int NumRecords = 1000;
        const int ValueMult = 1_000_000;

        ModifiedBitTestComparer comparer;

        private TsavoriteKV<int, int, IntStoreFunctions, IntAllocator> store;
        private ClientSession<int, int, int, int, Empty, SimpleSimpleFunctions<int, int>, IntStoreFunctions, IntAllocator> session;
        private BasicContext<int, int, int, int, Empty, SimpleSimpleFunctions<int, int>, IntStoreFunctions, IntAllocator> bContext;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: false);
            comparer = new ModifiedBitTestComparer();
            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                PageSize = 1L << 12,
                MemorySize = 1L << 22
            }, StoreFunctions<int, int>.Create(comparer)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
            session = store.NewSession<int, int, Empty, SimpleSimpleFunctions<int, int>>(new SimpleSimpleFunctions<int, int>());
            bContext = session.BasicContext;
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
            for (int key = 0; key < NumRecords; key++)
                ClassicAssert.IsFalse(bContext.Upsert(key, key * ValueMult).IsPending);
        }

        void AssertLockandModified(LockableUnsafeContext<int, int, int, int, Empty, SimpleSimpleFunctions<int, int>, IntStoreFunctions, IntAllocator> luContext, int key, bool xlock, bool slock, bool modified = false)
        {
            OverflowBucketLockTableTests.AssertLockCounts(store, ref key, xlock, slock);
            var isM = luContext.IsModified(key);
            ClassicAssert.AreEqual(modified, isM, "modified mismatch");
        }

        void AssertLockandModified(LockableContext<int, int, int, int, Empty, SimpleSimpleFunctions<int, int>, IntStoreFunctions, IntAllocator> luContext, int key, bool xlock, bool slock, bool modified = false)
        {
            OverflowBucketLockTableTests.AssertLockCounts(store, ref key, xlock, slock);
            var isM = luContext.IsModified(key);
            ClassicAssert.AreEqual(modified, isM, "modified mismatch");
        }

        void AssertLockandModified(ClientSession<int, int, int, int, Empty, SimpleSimpleFunctions<int, int>, IntStoreFunctions, IntAllocator> session, int key, bool xlock, bool slock, bool modified = false)
        {
            var luContext = session.LockableUnsafeContext;
            luContext.BeginUnsafe();

            OverflowBucketLockTableTests.AssertLockCounts(store, ref key, xlock, slock);
            var isM = luContext.IsModified(key);
            ClassicAssert.AreEqual(modified, isM, "Modified mismatch");

            luContext.EndUnsafe();
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void LockAndNotModify()
        {
            Populate();
            Random r = new(100);
            int key = r.Next(NumRecords);
            bContext.ResetModified(key);

            var lContext = session.LockableContext;
            lContext.BeginLockable();
            AssertLockandModified(lContext, key, xlock: false, slock: false, modified: false);

            var keyVec = new[] { new FixedLengthLockableKeyStruct<int>(key, LockType.Exclusive, lContext) };

            lContext.Lock<FixedLengthLockableKeyStruct<int>>(keyVec);
            AssertLockandModified(lContext, key, xlock: true, slock: false, modified: false);

            lContext.Unlock<FixedLengthLockableKeyStruct<int>>(keyVec);
            AssertLockandModified(lContext, key, xlock: false, slock: false, modified: false);

            keyVec[0].LockType = LockType.Shared;

            lContext.Lock<FixedLengthLockableKeyStruct<int>>(keyVec);
            AssertLockandModified(lContext, key, xlock: false, slock: true, modified: false);

            lContext.Unlock<FixedLengthLockableKeyStruct<int>>(keyVec);
            AssertLockandModified(lContext, key, xlock: false, slock: false, modified: false);
            lContext.EndLockable();
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void ResetModifyForNonExistingKey()
        {
            Populate();
            int key = NumRecords + 100;
            bContext.ResetModified(key);
            AssertLockandModified(session, key, xlock: false, slock: false, modified: false);
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void ModifyClientSession([Values(true, false)] bool flushToDisk, [Values] UpdateOp updateOp)
        {
            Populate();

            int key = NumRecords - 500;
            int value = 14;
            bContext.ResetModified(key);
            AssertLockandModified(session, key, xlock: false, slock: false, modified: false);

            if (flushToDisk)
                store.Log.FlushAndEvict(wait: true);

            Status status = default;
            switch (updateOp)
            {
                case UpdateOp.Upsert:
                    status = bContext.Upsert(key, value);
                    break;
                case UpdateOp.RMW:
                    status = bContext.RMW(key, value);
                    break;
                case UpdateOp.Delete:
                    status = bContext.Delete(key);
                    break;
                default:
                    break;
            }
            if (flushToDisk)
            {
                switch (updateOp)
                {
                    case UpdateOp.RMW:
                        ClassicAssert.IsTrue(status.IsPending, status.ToString());
                        _ = bContext.CompletePending(wait: true);
                        break;
                    default:
                        ClassicAssert.IsTrue(status.NotFound);
                        break;
                }
                (status, _) = bContext.Read(key);
                ClassicAssert.IsTrue(status.Found || updateOp == UpdateOp.Delete);
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

            int key = NumRecords - 500;
            int value = 14;
            bContext.ResetModified(key);
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

            luContext.Lock<FixedLengthLockableKeyStruct<int>>(keyVec);

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
                        ClassicAssert.IsTrue(status.IsPending, status.ToString());
                        _ = luContext.CompletePending(wait: true);
                        break;
                    default:
                        ClassicAssert.IsTrue(status.NotFound);
                        break;
                }
            }

            luContext.Unlock<FixedLengthLockableKeyStruct<int>>(keyVec);

            if (flushToDisk)
            {
                keyVec[0].LockType = LockType.Shared;
                luContext.Lock<FixedLengthLockableKeyStruct<int>>(keyVec);
                (status, _) = luContext.Read(key);
                ClassicAssert.AreEqual(updateOp != UpdateOp.Delete, status.Found, status.ToString());
                luContext.Unlock<FixedLengthLockableKeyStruct<int>>(keyVec);
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

            int key = NumRecords - 500;
            int value = 14;
            bContext.ResetModified(key);
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
                        ClassicAssert.IsTrue(status.IsPending, status.ToString());
                        _ = unsafeContext.CompletePending(wait: true);
                        break;
                    default:
                        ClassicAssert.IsTrue(status.NotFound);
                        break;
                }
                (status, _) = unsafeContext.Read(key);
                ClassicAssert.IsTrue(status.Found || updateOp == UpdateOp.Delete);
            }
            unsafeContext.EndUnsafe();

            AssertLockandModified(session, key, xlock: false, slock: false, modified: updateOp != UpdateOp.Delete);
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void ModifyLC([Values(true, false)] bool flushToDisk, [Values] UpdateOp updateOp)
        {
            Populate();

            int key = NumRecords - 500;
            int value = 14;
            bContext.ResetModified(key);
            var lContext = session.LockableContext;
            lContext.BeginLockable();
            AssertLockandModified(lContext, key, xlock: false, slock: false, modified: false);

            var keyVec = new[] { new FixedLengthLockableKeyStruct<int>(key, LockType.Exclusive, lContext) };

            lContext.Lock<FixedLengthLockableKeyStruct<int>>(keyVec);

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
                        ClassicAssert.IsTrue(status.IsPending, status.ToString());
                        _ = lContext.CompletePending(wait: true);
                        break;
                    default:
                        ClassicAssert.IsTrue(status.NotFound);
                        break;
                }
            }

            lContext.Unlock<FixedLengthLockableKeyStruct<int>>(keyVec);

            if (flushToDisk)
            {
                keyVec[0].LockType = LockType.Shared;
                lContext.Lock<FixedLengthLockableKeyStruct<int>>(keyVec);
                (status, _) = lContext.Read(key);
                ClassicAssert.AreEqual(updateOp != UpdateOp.Delete, status.Found, status.ToString());
                lContext.Unlock<FixedLengthLockableKeyStruct<int>>(keyVec);
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

            luContext.Lock<FixedLengthLockableKeyStruct<int>>(keyVec);
            AssertLockandModified(luContext, key, xlock: false, slock: true, modified: true);

            // Check Read Copy to Tail resets the modified
            var status = luContext.Read(ref key, ref input, ref output, ref readOptions, out _);
            ClassicAssert.IsTrue(status.IsPending, status.ToString());
            _ = luContext.CompletePending(wait: true);

            luContext.Unlock<FixedLengthLockableKeyStruct<int>>(keyVec);
            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: true);

            // Check Read Copy to Tail resets the modified on locked key
            key += 10;
            keyVec[0] = new(key, LockType.Exclusive, luContext);
            luContext.Lock<FixedLengthLockableKeyStruct<int>>(keyVec);
            status = luContext.Read(ref key, ref input, ref output, ref readOptions, out _);
            ClassicAssert.IsTrue(status.IsPending, status.ToString());
            _ = luContext.CompletePending(wait: true);
            AssertLockandModified(luContext, key, xlock: true, slock: false, modified: true);
            luContext.Unlock<FixedLengthLockableKeyStruct<int>>(keyVec);
            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: true);

            luContext.EndLockable();
            luContext.EndUnsafe();
        }
    }
}