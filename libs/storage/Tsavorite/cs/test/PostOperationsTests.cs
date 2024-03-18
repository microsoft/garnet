// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test
{
    [TestFixture]
    internal class PostOperationsTests
    {
        class PostFunctions : SimpleFunctions<int, int>
        {
            internal long pswAddress;
            internal long piuAddress;
            internal long pcuAddress;
            internal long psdAddress;

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
            public override void PostCopyUpdater(ref int key, ref int input, ref int oldValue, ref int newValue, ref int output, ref RMWInfo rmwInfo) { pcuAddress = rmwInfo.Address; }

            public override void PostSingleDeleter(ref int key, ref DeleteInfo deleteInfo) { psdAddress = deleteInfo.Address; }
            public override bool ConcurrentDeleter(ref int key, ref int value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo) => false;
        }

        private TsavoriteKV<int, int> store;
        private ClientSession<int, int, int, int, Empty, PostFunctions> session;
        private IDevice log;

        const int numRecords = 100;
        const int targetKey = 42;
        long expectedAddress;

        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            log = Devices.CreateLogDevice($"{TestUtils.MethodTestDir}/PostOperations.log", deleteOnClose: true);
            store = new TsavoriteKV<int, int>
                       (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 10 });
            session = store.NewSession<int, int, Empty, PostFunctions>(new PostFunctions());
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
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        void Populate()
        {
            for (var key = 0; key < numRecords; ++key)
            {
                expectedAddress = store.Log.TailAddress;
                session.Upsert(key, key * 100);
                Assert.AreEqual(expectedAddress, session.functions.pswAddress);
            }

            session.functions.Clear();
            expectedAddress = store.Log.TailAddress;
        }

        internal void CompletePendingAndVerifyInsertedAddress()
        {
            // Note: Only Read and RMW have Pending results.
            session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            TestUtils.GetSinglePendingResult(completedOutputs, out var recordMetadata);
            Assert.AreEqual(expectedAddress, recordMetadata.Address);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void PostSingleWriterTest()
        {
            // Populate has already executed the not-found test (InternalInsert) as part of its normal insert.

            // Execute the ReadOnly (InternalInsert) test
            store.Log.FlushAndEvict(wait: true);
            session.Upsert(targetKey, targetKey * 1000);
            session.CompletePending(wait: true);
            Assert.AreEqual(expectedAddress, session.functions.pswAddress);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void PostInitialUpdaterTest()
        {
            // Execute the not-found test (InternalRMW).
            session.RMW(numRecords + 1, (numRecords + 1) * 1000);
            Assert.AreEqual(expectedAddress, session.functions.piuAddress);
            session.functions.Clear();

            // Now cause an attempt at InPlaceUpdater, which we've set to fail, so CopyUpdater is done (InternalInsert).
            expectedAddress = store.Log.TailAddress;
            session.RMW(targetKey, targetKey * 1000);
            Assert.AreEqual(expectedAddress, session.functions.pcuAddress);

            // Execute the not-in-memory test (InternalContinuePendingRMW). First delete the record so it has a tombstone; this will go to InitialUpdater.
            session.Delete(targetKey);
            store.Log.FlushAndEvict(wait: true);
            expectedAddress = store.Log.TailAddress;

            session.RMW(targetKey, targetKey * 1000);
            CompletePendingAndVerifyInsertedAddress();
            Assert.AreEqual(expectedAddress, session.functions.piuAddress);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void PostCopyUpdaterTest()
        {
            // First try to modify in-memory, readonly (InternalRMW).
            store.Log.ShiftReadOnlyAddress(store.Log.ReadOnlyAddress, wait: true);
            session.RMW(targetKey, targetKey * 1000);
            Assert.AreEqual(expectedAddress, session.functions.pcuAddress);

            // Execute the not-in-memory test (InternalContinuePendingRMW).
            store.Log.FlushAndEvict(wait: true);
            expectedAddress = store.Log.TailAddress;
            session.RMW(targetKey, targetKey * 1000);
            CompletePendingAndVerifyInsertedAddress();
            Assert.AreEqual(expectedAddress, session.functions.pcuAddress);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void PostSingleDeleterTest()
        {
            // Execute the not-in-memory test (InternalDelete); ConcurrentDeleter returns false to force a new record to be added.
            session.Delete(targetKey);
            Assert.AreEqual(expectedAddress, session.functions.psdAddress);

            // Execute the not-in-memory test (InternalDelete).
            store.Log.FlushAndEvict(wait: true);
            expectedAddress = store.Log.TailAddress;
            session.Delete(targetKey + 1);
            Assert.AreEqual(expectedAddress, session.functions.psdAddress);
        }
    }
}