// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using IntAllocator = SpanByteAllocator<StoreFunctions<IntKeyComparer, SpanByteRecordDisposer>>;
    using IntStoreFunctions = StoreFunctions<IntKeyComparer, SpanByteRecordDisposer>;

    [AllureNUnit]
    [TestFixture]
    internal class PostOperationsTests : AllureTestBase
    {
        class PostFunctions : SimpleIntSimpleFunctions
        {
            internal long pswAddress;
            internal long piuAddress;
            internal long pcuAddress;
            internal long psdAddress;
            internal bool returnFalseFromPCU;

            internal void Clear()
            {
                pswAddress = LogAddress.kInvalidAddress;
                piuAddress = LogAddress.kInvalidAddress;
                pcuAddress = LogAddress.kInvalidAddress;
                psdAddress = LogAddress.kInvalidAddress;
            }

            internal PostFunctions() : base() { }

            public override void PostInitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref int input, ReadOnlySpan<byte> src, ref int output, ref UpsertInfo upsertInfo)
                => pswAddress = upsertInfo.Address;

            public override bool InitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                dstLogRecord.ValueSpan.AsRef<int>() = input;
                return true;
            }

            /// <inheritdoc/>
            public override void PostInitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref int value, ref int output, ref RMWInfo rmwInfo)
                => piuAddress = rmwInfo.Address;

            public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref int input, ref int output, ref RMWInfo rmwInfo)
                => false; // For this test, we want this to fail and lead to InitialUpdater

            /// <inheritdoc/>
            public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                dstLogRecord.ValueSpan.AsRef<int>() = srcLogRecord.ValueSpan.AsRef<int>();
                return true;
            }

            /// <inheritdoc/>
            public override bool PostCopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                pcuAddress = rmwInfo.Address;
                if (returnFalseFromPCU)
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                return !returnFalseFromPCU;
            }

            public override void PostInitialDeleter(ref LogRecord dstLogRecord, ref DeleteInfo deleteInfo)
                => psdAddress = deleteInfo.Address;

            public override bool InPlaceDeleter(ref LogRecord dstLogRecord, ref DeleteInfo deleteInfo)
                => false;
        }

        private TsavoriteKV<IntStoreFunctions, IntAllocator> store;
        private ClientSession<int, int, Empty, PostFunctions, IntStoreFunctions, IntAllocator> session;
        private BasicContext<int, int, Empty, PostFunctions, IntStoreFunctions, IntAllocator> bContext;
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
            }, StoreFunctions.Create(IntKeyComparer.Instance, SpanByteRecordDisposer.Instance)
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
                if ((expectedAddress % store.hlogBase.PageSize) == 0)
                    expectedAddress += PageHeader.Size;
                var value = key * 100;
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref value));
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
        public void PostInitialWriterTest()
        {
            // Populate has already executed the not-found test (InternalInsert) as part of its normal insert.

            // Execute the ReadOnly (InternalInsert) test
            store.Log.FlushAndEvict(wait: true);
            int key = TargetKey, value = TargetKey * 100;
            _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref value));
            _ = bContext.CompletePending(wait: true);
            ClassicAssert.AreEqual(expectedAddress, session.functions.pswAddress);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void PostInitialUpdaterTest()
        {
            // Execute the not-found test (InternalRMW).
            int key = NumRecords + 1, value = (NumRecords + 1) * 1000;
            _ = bContext.RMW(SpanByte.FromPinnedVariable(ref key), ref value);
            ClassicAssert.AreEqual(expectedAddress, session.functions.piuAddress);
            session.functions.Clear();

            // Now cause an attempt at InPlaceUpdater, which we've set to fail, so CopyUpdater is done (InternalInsert).
            expectedAddress = store.Log.TailAddress;
            key = TargetKey;
            value = TargetKey * 1000;

            _ = bContext.RMW(SpanByte.FromPinnedVariable(ref key), ref value);
            ClassicAssert.AreEqual(expectedAddress, session.functions.pcuAddress);

            // Execute the not-in-memory test (InternalContinuePendingRMW). First delete the record so it has a tombstone; this will go to InitialUpdater.
            _ = bContext.Delete(SpanByte.FromPinnedVariable(ref key));
            store.Log.FlushAndEvict(wait: true);
            expectedAddress = store.Log.TailAddress;

            _ = bContext.RMW(SpanByte.FromPinnedVariable(ref key), ref value);
            CompletePendingAndVerifyInsertedAddress();
            ClassicAssert.AreEqual(expectedAddress, session.functions.piuAddress);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void PostCopyUpdaterTest()
        {
            // First try to modify in-memory, readonly (InternalRMW).
            var key = TargetKey;
            var value = TargetKey * 1000;
            store.Log.ShiftReadOnlyAddress(store.Log.ReadOnlyAddress, wait: true);
            _ = bContext.RMW(SpanByte.FromPinnedVariable(ref key), ref value);
            ClassicAssert.AreEqual(expectedAddress, session.functions.pcuAddress);

            // Execute the not-in-memory test (InternalContinuePendingRMW).
            store.Log.FlushAndEvict(wait: true);
            expectedAddress = store.Log.TailAddress;
            _ = bContext.RMW(SpanByte.FromPinnedVariable(ref key), ref value);
            CompletePendingAndVerifyInsertedAddress();
            ClassicAssert.AreEqual(expectedAddress, session.functions.pcuAddress);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void PostCopyUpdaterFalseTest([Values(FlushMode.ReadOnly, FlushMode.OnDisk)] FlushMode flushMode)
        {
            // Verify the key exists
            var key = TargetKey;
            var value = TargetKey * 1000;
            var (status, _ /*output*/) = bContext.Read(SpanByte.FromPinnedVariable(ref key));
            ClassicAssert.IsTrue(status.Found, "Expected the record to exist");
            session.functions.returnFalseFromPCU = true;

            // Make the record read-only
            if (flushMode == FlushMode.OnDisk)
                store.Log.ShiftReadOnlyAddress(store.Log.ReadOnlyAddress, wait: true);
            else
                store.Log.FlushAndEvict(wait: true);

            // Call RMW
            _ = bContext.RMW(SpanByte.FromPinnedVariable(ref key), ref value);

            // Verify the key no longer exists.
            (status, _ /*output*/) = bContext.Read(SpanByte.FromPinnedVariable(ref key));
            ClassicAssert.IsFalse(status.Found, "Expected the record to no longer exist");
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void PostInitialDeleterTest()
        {
            // Execute the not-in-memory test (InternalDelete); InPlaceDeleter returns false to force a new record to be added.
            var key = TargetKey;
            _ = bContext.Delete(SpanByte.FromPinnedVariable(ref key));
            ClassicAssert.AreEqual(expectedAddress, session.functions.psdAddress);

            // Execute the not-in-memory test (InternalDelete).
            store.Log.FlushAndEvict(wait: true);
            expectedAddress = store.Log.TailAddress;
            key++;
            _ = bContext.Delete(SpanByte.FromPinnedVariable(ref key));
            ClassicAssert.AreEqual(expectedAddress, session.functions.psdAddress);
        }
    }
}