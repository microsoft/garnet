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
    using IntAllocator = SpanByteAllocator<StoreFunctions<IntKeyComparer, SpanByteRecordDisposer>>;
    using IntStoreFunctions = StoreFunctions<IntKeyComparer, SpanByteRecordDisposer>;

    [TestFixture]
    class ModifiedBitTests
    {
        const int NumRecords = 1000;
        const int ValueMult = 1_000_000;

        IntKeyComparer comparer;

        private TsavoriteKV<IntStoreFunctions, IntAllocator> store;
        private ClientSession<int, int, Empty, SimpleIntSimpleFunctions, IntStoreFunctions, IntAllocator> session;
        private BasicContext<int, int, Empty, SimpleIntSimpleFunctions, IntStoreFunctions, IntAllocator> bContext;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: false);
            comparer = IntKeyComparer.Instance;
            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                PageSize = 1L << 12,
                MemorySize = 1L << 22
            }, StoreFunctions.Create(comparer, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
            session = store.NewSession<int, int, Empty, SimpleIntSimpleFunctions>(new SimpleIntSimpleFunctions());
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
            {
                var value = key * ValueMult;
                ClassicAssert.IsFalse(bContext.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref value)).IsPending);
            }
        }

        void AssertLockandModified(TransactionalUnsafeContext<int, int, Empty, SimpleIntSimpleFunctions, IntStoreFunctions, IntAllocator> luContext, int key, bool xlock, bool slock, bool modified = false)
        {
            OverflowBucketLockTableTests.AssertLockCounts(store, SpanByte.FromPinnedVariable(ref key), xlock, slock);
            var isM = luContext.IsModified(SpanByte.FromPinnedVariable(ref key));
            ClassicAssert.AreEqual(modified, isM, "modified mismatch");
        }

        void AssertLockandModified(TransactionalContext<int, int, Empty, SimpleIntSimpleFunctions, IntStoreFunctions, IntAllocator> luContext, int key, bool xlock, bool slock, bool modified = false)
        {
            OverflowBucketLockTableTests.AssertLockCounts(store, SpanByte.FromPinnedVariable(ref key), xlock, slock);
            var isM = luContext.IsModified(SpanByte.FromPinnedVariable(ref key));
            ClassicAssert.AreEqual(modified, isM, "modified mismatch");
        }

        void AssertLockandModified(ClientSession<int, int, Empty, SimpleIntSimpleFunctions, IntStoreFunctions, IntAllocator> session, int key, bool xlock, bool slock, bool modified = false)
        {
            var luContext = session.TransactionalUnsafeContext;
            luContext.BeginUnsafe();

            OverflowBucketLockTableTests.AssertLockCounts(store, SpanByte.FromPinnedVariable(ref key), xlock, slock);
            var isM = luContext.IsModified(SpanByte.FromPinnedVariable(ref key));
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
            bContext.ResetModified(SpanByte.FromPinnedVariable(ref key));

            var lContext = session.TransactionalContext;
            lContext.BeginTransaction();
            AssertLockandModified(lContext, key, xlock: false, slock: false, modified: false);

            var keyVec = new[] { new FixedLengthTransactionalKeyStruct(SpanByte.FromPinnedVariable(ref key), LockType.Exclusive, lContext) };

            lContext.Lock(keyVec);
            AssertLockandModified(lContext, key, xlock: true, slock: false, modified: false);

            lContext.Unlock(keyVec);
            AssertLockandModified(lContext, key, xlock: false, slock: false, modified: false);

            keyVec[0].LockType = LockType.Shared;

            lContext.Lock(keyVec);
            AssertLockandModified(lContext, key, xlock: false, slock: true, modified: false);

            lContext.Unlock(keyVec);
            AssertLockandModified(lContext, key, xlock: false, slock: false, modified: false);
            lContext.EndTransaction();
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void ResetModifyForNonExistingKey()
        {
            Populate();
            int key = NumRecords + 100;
            bContext.ResetModified(SpanByte.FromPinnedVariable(ref key));
            AssertLockandModified(session, key, xlock: false, slock: false, modified: false);
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void ModifyClientSession([Values(true, false)] bool flushToDisk, [Values] UpdateOp updateOp)
        {
            Populate();

            int key = NumRecords - 500;
            int value = 14;
            bContext.ResetModified(SpanByte.FromPinnedVariable(ref key));
            AssertLockandModified(session, key, xlock: false, slock: false, modified: false);

            if (flushToDisk)
                store.Log.FlushAndEvict(wait: true);

            Status status = default;
            switch (updateOp)
            {
                case UpdateOp.Upsert:
                    status = bContext.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref value));
                    break;
                case UpdateOp.RMW:
                    status = bContext.RMW(SpanByte.FromPinnedVariable(ref key), ref value);
                    break;
                case UpdateOp.Delete:
                    status = bContext.Delete(SpanByte.FromPinnedVariable(ref key));
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
                (status, _) = bContext.Read(SpanByte.FromPinnedVariable(ref key));
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
            bContext.ResetModified(SpanByte.FromPinnedVariable(ref key));
            var luContext = session.TransactionalUnsafeContext;
            luContext.BeginUnsafe();
            luContext.BeginTransaction();
            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: false);
            luContext.EndTransaction();
            luContext.EndUnsafe();

            if (flushToDisk)
                store.Log.FlushAndEvict(wait: true);

            Status status = default;

            luContext.BeginUnsafe();
            luContext.BeginTransaction();

            var keyVec = new[] { new FixedLengthTransactionalKeyStruct(SpanByte.FromPinnedVariable(ref key), LockType.Exclusive, luContext) };

            luContext.Lock(keyVec);

            switch (updateOp)
            {
                case UpdateOp.Upsert:
                    status = luContext.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref value));
                    break;
                case UpdateOp.RMW:
                    status = luContext.RMW(SpanByte.FromPinnedVariable(ref key), ref value);
                    break;
                case UpdateOp.Delete:
                    status = luContext.Delete(SpanByte.FromPinnedVariable(ref key));
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

            luContext.Unlock(keyVec);

            if (flushToDisk)
            {
                keyVec[0].LockType = LockType.Shared;
                luContext.Lock(keyVec);
                (status, _) = luContext.Read(SpanByte.FromPinnedVariable(ref key));
                ClassicAssert.AreEqual(updateOp != UpdateOp.Delete, status.Found, status.ToString());
                luContext.Unlock(keyVec);
            }

            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: updateOp != UpdateOp.Delete);

            luContext.EndTransaction();
            luContext.EndUnsafe();
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void ModifyUC([Values(true, false)] bool flushToDisk, [Values] UpdateOp updateOp)
        {
            Populate();

            int key = NumRecords - 500;
            int value = 14;
            bContext.ResetModified(SpanByte.FromPinnedVariable(ref key));
            AssertLockandModified(session, key, xlock: false, slock: false, modified: false);

            if (flushToDisk)
                store.Log.FlushAndEvict(wait: true);

            Status status = default;
            var unsafeContext = session.UnsafeContext;

            unsafeContext.BeginUnsafe();
            switch (updateOp)
            {
                case UpdateOp.Upsert:
                    status = unsafeContext.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref value));
                    break;
                case UpdateOp.RMW:
                    status = unsafeContext.RMW(SpanByte.FromPinnedVariable(ref key), ref value);
                    break;
                case UpdateOp.Delete:
                    status = unsafeContext.Delete(SpanByte.FromPinnedVariable(ref key));
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
                (status, _) = unsafeContext.Read(SpanByte.FromPinnedVariable(ref key));
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
            bContext.ResetModified(SpanByte.FromPinnedVariable(ref key));
            var lContext = session.TransactionalContext;
            lContext.BeginTransaction();
            AssertLockandModified(lContext, key, xlock: false, slock: false, modified: false);

            var keyVec = new[] { new FixedLengthTransactionalKeyStruct(SpanByte.FromPinnedVariable(ref key), LockType.Exclusive, lContext) };

            lContext.Lock(keyVec);

            if (flushToDisk)
                store.Log.FlushAndEvict(wait: true);

            Status status = default;

            switch (updateOp)
            {
                case UpdateOp.Upsert:
                    status = lContext.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref value));
                    break;
                case UpdateOp.RMW:
                    status = lContext.RMW(SpanByte.FromPinnedVariable(ref key), ref value);
                    break;
                case UpdateOp.Delete:
                    status = lContext.Delete(SpanByte.FromPinnedVariable(ref key));
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

            lContext.Unlock(keyVec);

            if (flushToDisk)
            {
                keyVec[0].LockType = LockType.Shared;
                lContext.Lock(keyVec);
                (status, _) = lContext.Read(SpanByte.FromPinnedVariable(ref key));
                ClassicAssert.AreEqual(updateOp != UpdateOp.Delete, status.Found, status.ToString());
                lContext.Unlock(keyVec);
            }

            AssertLockandModified(lContext, key, xlock: false, slock: false, modified: updateOp != UpdateOp.Delete);
            lContext.EndTransaction();
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void CopyToTailTest()
        {
            Populate();
            store.Log.FlushAndEvict(wait: true);

            var luContext = session.TransactionalUnsafeContext;

            int input = 0, output = 0, key = 200;
            ReadOptions readOptions = new() { CopyOptions = new(ReadCopyFrom.AllImmutable, ReadCopyTo.MainLog) };

            luContext.BeginUnsafe();
            luContext.BeginTransaction();
            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: true);

            var keyVec = new[] { new FixedLengthTransactionalKeyStruct(SpanByte.FromPinnedVariable(ref key), LockType.Shared, luContext) };

            luContext.Lock(keyVec);
            AssertLockandModified(luContext, key, xlock: false, slock: true, modified: true);

            // Check Read Copy to Tail resets the modified
            var status = luContext.Read(SpanByte.FromPinnedVariable(ref key), ref input, ref output, ref readOptions, out _);
            ClassicAssert.IsTrue(status.IsPending, status.ToString());
            _ = luContext.CompletePending(wait: true);

            luContext.Unlock(keyVec);
            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: false);

            // Check Read Copy to Tail resets the modified on locked key
            key += 10;
            keyVec[0] = new(SpanByte.FromPinnedVariable(ref key), LockType.Exclusive, luContext);
            luContext.Lock(keyVec);
            status = luContext.Read(SpanByte.FromPinnedVariable(ref key), ref input, ref output, ref readOptions, out _);
            ClassicAssert.IsTrue(status.IsPending, status.ToString());
            _ = luContext.CompletePending(wait: true);
            AssertLockandModified(luContext, key, xlock: true, slock: false, modified: false);
            luContext.Unlock(keyVec);
            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: false);

            luContext.EndTransaction();
            luContext.EndUnsafe();
        }
    }
}