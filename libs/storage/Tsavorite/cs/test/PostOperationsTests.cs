// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using IntAllocator = BlittableAllocator<int, int, StoreFunctions<int, int, IntKeyComparer, DefaultRecordDisposer<int, int>>>;
    using IntStoreFunctions = StoreFunctions<int, int, IntKeyComparer, DefaultRecordDisposer<int, int>>;

    [AllureNUnit]
    [TestFixture]
    internal class PostOperationsTests : AllureTestBase
    {
        class PostFunctions : SimpleSimpleFunctions<int, int>
        {
            internal long pswAddress;
            internal long piuAddress;
            internal long pcuAddress;
            internal long psdAddress;
            internal bool returnFalseFromPCU;

            internal void Clear()
            {
                pswAddress = Constants.kInvalidAddress;
                piuAddress = Constants.kInvalidAddress;
                pcuAddress = Constants.kInvalidAddress;
                psdAddress = Constants.kInvalidAddress;
            }

            internal PostFunctions() : base() { }

            public override void PostSingleWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref UpsertInfo upsertInfo, WriteReason reason) { pswAddress = upsertInfo.Address; }

            public override bool InitialUpdater(ref int key, ref int input, ref int value, ref int output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) { value = input; return true; }
            /// <inheritdoc/>
            public override void PostInitialUpdater(ref int key, ref int input, ref int value, ref int output, ref RMWInfo rmwInfo) { piuAddress = rmwInfo.Address; }

            public override bool InPlaceUpdater(ref int key, ref int input, ref int value, ref int output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => false; // For this test, we want this to fail and lead to InitialUpdater

            /// <inheritdoc/>
            public override bool CopyUpdater(ref int key, ref int input, ref int oldValue, ref int newValue, ref int output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) { newValue = oldValue; return true; }
            /// <inheritdoc/>
            public override bool PostCopyUpdater(ref int key, ref int input, ref int oldValue, ref int newValue, ref int output, ref RMWInfo rmwInfo)
            {
                pcuAddress = rmwInfo.Address;
                if (returnFalseFromPCU)
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                return !returnFalseFromPCU;
            }

            public override void PostSingleDeleter(ref int key, ref DeleteInfo deleteInfo) { psdAddress = deleteInfo.Address; }
            public override bool ConcurrentDeleter(ref int key, ref int value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo) => false;
        }

        private TsavoriteKV<int, int, IntStoreFunctions, IntAllocator> store;
        private ClientSession<int, int, int, int, Empty, PostFunctions, IntStoreFunctions, IntAllocator> session;
        private BasicContext<int, int, int, int, Empty, PostFunctions, IntStoreFunctions, IntAllocator> bContext;
        private IDevice log;

        const int NumRecords = 100;
        const int TargetKey = 42;
        long expectedAddress;

        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            DeleteDirectory(MethodTestDir, wait: true);

            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "PostOperations.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                MemorySize = 1L << 15,
                PageSize = 1L << 10
            }, StoreFunctions<int, int>.Create(IntKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
            session = store.NewSession<int, int, Empty, PostFunctions>(new PostFunctions());
            bContext = session.BasicContext;
            Populate();
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
            DeleteDirectory(MethodTestDir);
        }

        void Populate()
        {
            for (var key = 0; key < NumRecords; ++key)
            {
                expectedAddress = store.Log.TailAddress;
                _ = bContext.Upsert(key, key * 100);
                ClassicAssert.AreEqual(expectedAddress, session.functions.pswAddress);
            }

            session.functions.Clear();
            expectedAddress = store.Log.TailAddress;
        }

        internal void CompletePendingAndVerifyInsertedAddress()
        {
            // Note: Only Read and RMW have Pending results.
            _ = bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            _ = GetSinglePendingResult(completedOutputs, out var recordMetadata);
            ClassicAssert.AreEqual(expectedAddress, recordMetadata.Address);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void PostSingleWriterTest()
        {
            // Populate has already executed the not-found test (InternalInsert) as part of its normal insert.

            // Execute the ReadOnly (InternalInsert) test
            store.Log.FlushAndEvict(wait: true);
            _ = bContext.Upsert(TargetKey, TargetKey * 1000);
            _ = bContext.CompletePending(wait: true);
            ClassicAssert.AreEqual(expectedAddress, session.functions.pswAddress);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void PostInitialUpdaterTest()
        {
            // Execute the not-found test (InternalRMW).
            _ = bContext.RMW(NumRecords + 1, (NumRecords + 1) * 1000);
            ClassicAssert.AreEqual(expectedAddress, session.functions.piuAddress);
            session.functions.Clear();

            // Now cause an attempt at InPlaceUpdater, which we've set to fail, so CopyUpdater is done (InternalInsert).
            expectedAddress = store.Log.TailAddress;
            _ = bContext.RMW(TargetKey, TargetKey * 1000);
            ClassicAssert.AreEqual(expectedAddress, session.functions.pcuAddress);

            // Execute the not-in-memory test (InternalContinuePendingRMW). First delete the record so it has a tombstone; this will go to InitialUpdater.
            _ = bContext.Delete(TargetKey);
            store.Log.FlushAndEvict(wait: true);
            expectedAddress = store.Log.TailAddress;

            _ = bContext.RMW(TargetKey, TargetKey * 1000);
            CompletePendingAndVerifyInsertedAddress();
            ClassicAssert.AreEqual(expectedAddress, session.functions.piuAddress);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void PostCopyUpdaterTest()
        {
            // First try to modify in-memory, readonly (InternalRMW).
            store.Log.ShiftReadOnlyAddress(store.Log.ReadOnlyAddress, wait: true);
            _ = bContext.RMW(TargetKey, TargetKey * 1000);
            ClassicAssert.AreEqual(expectedAddress, session.functions.pcuAddress);

            // Execute the not-in-memory test (InternalContinuePendingRMW).
            store.Log.FlushAndEvict(wait: true);
            expectedAddress = store.Log.TailAddress;
            _ = bContext.RMW(TargetKey, TargetKey * 1000);
            CompletePendingAndVerifyInsertedAddress();
            ClassicAssert.AreEqual(expectedAddress, session.functions.pcuAddress);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void PostCopyUpdaterFalseTest([Values(FlushMode.ReadOnly, FlushMode.OnDisk)] FlushMode flushMode)
        {
            // Verify the key exists
            var (status, output) = bContext.Read(TargetKey);
            ClassicAssert.IsTrue(status.Found, "Expected the record to exist");
            session.functions.returnFalseFromPCU = true;

            // Make the record read-only
            if (flushMode == FlushMode.OnDisk)
                store.Log.ShiftReadOnlyAddress(store.Log.ReadOnlyAddress, wait: true);
            else
                store.Log.FlushAndEvict(wait: true);

            // Call RMW
            _ = bContext.RMW(TargetKey, TargetKey * 1000);

            // Verify the key no longer exists.
            (status, output) = bContext.Read(TargetKey);
            ClassicAssert.IsFalse(status.Found, "Expected the record to no longer exist");
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void PostSingleDeleterTest()
        {
            // Execute the not-in-memory test (InternalDelete); ConcurrentDeleter returns false to force a new record to be added.
            _ = bContext.Delete(TargetKey);
            ClassicAssert.AreEqual(expectedAddress, session.functions.psdAddress);

            // Execute the not-in-memory test (InternalDelete).
            store.Log.FlushAndEvict(wait: true);
            expectedAddress = store.Log.TailAddress;
            _ = bContext.Delete(TargetKey + 1);
            ClassicAssert.AreEqual(expectedAddress, session.functions.psdAddress);
        }
    }
}