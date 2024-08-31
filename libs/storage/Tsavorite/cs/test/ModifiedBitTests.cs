// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using Tsavorite.test.LockTable;
using Tsavorite.test.recovery.objects;
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
            HashBucketLockTableTests.AssertLockCounts(store, ref key, xlock, slock);
            var isM = luContext.IsModified(key);
            ClassicAssert.AreEqual(modified, isM, "modified mismatch");
        }

        void AssertLockandModified(LockableContext<int, int, int, int, Empty, SimpleSimpleFunctions<int, int>, IntStoreFunctions, IntAllocator> lContext, int key, bool xlock, bool slock, bool modified = false)
        {
            HashBucketLockTableTests.AssertLockCounts(store, ref key, xlock, slock);
            var isM = lContext.IsModified(key);
            ClassicAssert.AreEqual(modified, isM, "modified mismatch");
        }

        TestTransactionalKernelSession<int, int, int, int, Empty, SimpleSimpleFunctions<int, int>, IntStoreFunctions, IntAllocator> GetKernelSession
            (LockableContext<int, int, int, int, Empty, SimpleSimpleFunctions<int, int>, IntStoreFunctions, IntAllocator> context) => new(context.Session);

        TestTransactionalKernelSession<int, int, int, int, Empty, SimpleSimpleFunctions<int, int>, IntStoreFunctions, IntAllocator> GetKernelSession
            (LockableUnsafeContext<int, int, int, int, Empty, SimpleSimpleFunctions<int, int>, IntStoreFunctions, IntAllocator> context) => new(context.Session);

        TestTransientKernelSession<int, int, int, int, Empty, SimpleSimpleFunctions<int, int>, IntStoreFunctions, IntAllocator> GetKernelSession
            (UnsafeContext<int, int, int, int, Empty, SimpleSimpleFunctions<int, int>, IntStoreFunctions, IntAllocator> context) => new(context.Session);

        void AssertLockandModified(ClientSession<int, int, int, int, Empty, SimpleSimpleFunctions<int, int>, IntStoreFunctions, IntAllocator> session, int key, bool xlock, bool slock, bool modified = false)
        {
            var luContext = session.LockableUnsafeContext;
            var kernelSession = GetKernelSession(luContext);

            kernelSession.BeginUnsafe();

            HashBucketLockTableTests.AssertLockCounts(store, ref key, xlock, slock);
            var isM = luContext.IsModified(key);
            ClassicAssert.AreEqual(modified, isM, "Modified mismatch");

            kernelSession.EndUnsafe();
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
            var kernelSession = GetKernelSession(lContext);

            kernelSession.BeginTransaction();
            AssertLockandModified(lContext, key, xlock: false, slock: false, modified: false);

            var keyVec = new[] { new FixedLengthLockableKeyStruct<int>(key, LockType.Exclusive, lContext) };

            kernelSession.BeginUnsafe();
            store.Kernel.Lock(ref kernelSession, keyVec);
            kernelSession.EndUnsafe();
            AssertLockandModified(lContext, key, xlock: true, slock: false, modified: false);

            kernelSession.BeginUnsafe();
            store.Kernel.Unlock(ref kernelSession, keyVec);
            kernelSession.EndUnsafe();
            AssertLockandModified(lContext, key, xlock: false, slock: false, modified: false);

            keyVec[0].LockType = LockType.Shared;

            kernelSession.BeginUnsafe();
            store.Kernel.Lock(ref kernelSession, keyVec);
            kernelSession.EndUnsafe();
            AssertLockandModified(lContext, key, xlock: false, slock: true, modified: false);

            kernelSession.BeginUnsafe();
            store.Kernel.Unlock(ref kernelSession, keyVec);
            kernelSession.EndUnsafe();
            AssertLockandModified(lContext, key, xlock: false, slock: false, modified: false);
            kernelSession.EndTransaction();
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
            var kernelSession = GetKernelSession(luContext);

            kernelSession.BeginUnsafe();
            kernelSession.BeginTransaction();
            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: false);
            kernelSession.EndTransaction();
            kernelSession.EndUnsafe();

            if (flushToDisk)
                store.Log.FlushAndEvict(wait: true);

            Status status = default;

            kernelSession.BeginUnsafe();
            kernelSession.BeginTransaction();

            var keyVec = new[] { new FixedLengthLockableKeyStruct<int>(key, LockType.Exclusive, luContext) };

            store.Kernel.Lock(ref kernelSession, keyVec);

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

            store.Kernel.Unlock(ref kernelSession, keyVec);

            if (flushToDisk)
            {
                keyVec[0].LockType = LockType.Shared;
                store.Kernel.Lock(ref kernelSession, keyVec);
                (status, _) = luContext.Read(key);
                ClassicAssert.AreEqual(updateOp != UpdateOp.Delete, status.Found, status.ToString());
                store.Kernel.Unlock(ref kernelSession, keyVec);
            }

            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: updateOp != UpdateOp.Delete);

            kernelSession.EndTransaction  ();
            kernelSession.EndUnsafe();
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
            var kernelSession = GetKernelSession(unsafeContext);

            kernelSession.BeginUnsafe();
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
            kernelSession.EndUnsafe();

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
            var kernelSession = GetKernelSession(lContext);

            kernelSession.BeginTransaction();
            AssertLockandModified(lContext, key, xlock: false, slock: false, modified: false);

            var keyVec = new[] { new FixedLengthLockableKeyStruct<int>(key, LockType.Exclusive, lContext) };

            kernelSession.BeginUnsafe();
            store.Kernel.Lock(ref kernelSession, keyVec);
            kernelSession.EndUnsafe();

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

            kernelSession.BeginUnsafe();
            store.Kernel.Unlock(ref kernelSession, keyVec);
            kernelSession.EndUnsafe();

            if (flushToDisk)
            {
                kernelSession.BeginUnsafe();
                keyVec[0].LockType = LockType.Shared;
                store.Kernel.Lock(ref kernelSession, keyVec);
                kernelSession.EndUnsafe();

                (status, _) = lContext.Read(key);
                ClassicAssert.AreEqual(updateOp != UpdateOp.Delete, status.Found, status.ToString());

                kernelSession.BeginUnsafe();
                store.Kernel.Unlock(ref kernelSession, keyVec);
                kernelSession.EndUnsafe();
            }

            AssertLockandModified(lContext, key, xlock: false, slock: false, modified: updateOp != UpdateOp.Delete);
            kernelSession.EndTransaction();
        }

        [Test]
        [Category(ModifiedBitTestCategory), Category(SmokeTestCategory)]
        public void CopyToTailTest()
        {
            Populate();
            store.Log.FlushAndEvict(wait: true);

            var luContext = session.LockableUnsafeContext;
            var kernelSession = GetKernelSession(luContext);

            int input = 0, output = 0, key = 200;
            ReadOptions readOptions = new() { CopyOptions = new(ReadCopyFrom.AllImmutable, ReadCopyTo.MainLog) };

            kernelSession.BeginUnsafe();
            kernelSession.BeginTransaction();
            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: true);

            var keyVec = new[] { new FixedLengthLockableKeyStruct<int>(key, LockType.Shared, luContext) };

            store.Kernel.Lock(ref kernelSession, keyVec);
            AssertLockandModified(luContext, key, xlock: false, slock: true, modified: true);

            // Check Read Copy to Tail resets the modified
            var status = luContext.Read(ref key, ref input, ref output, ref readOptions, out _);
            ClassicAssert.IsTrue(status.IsPending, status.ToString());
            _ = luContext.CompletePending(wait: true);

            store.Kernel.Unlock(ref kernelSession, keyVec);
            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: true);

            // Check Read Copy to Tail resets the modified on locked key
            key += 10;
            keyVec[0] = new(key, LockType.Exclusive, luContext);
            store.Kernel.Lock(ref kernelSession, keyVec);
            status = luContext.Read(ref key, ref input, ref output, ref readOptions, out _);
            ClassicAssert.IsTrue(status.IsPending, status.ToString());
            _ = luContext.CompletePending(wait: true);
            AssertLockandModified(luContext, key, xlock: true, slock: false, modified: true);
            store.Kernel.Unlock(ref kernelSession, keyVec);
            AssertLockandModified(luContext, key, xlock: false, slock: false, modified: true);

            kernelSession.EndTransaction();
            kernelSession.EndUnsafe();
        }
    }
}