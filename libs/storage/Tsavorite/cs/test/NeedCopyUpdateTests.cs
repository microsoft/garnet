// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.NeedCopyUpdateTests;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using LongAllocator = BlittableAllocator<long, long, StoreFunctions<long, long, LongKeyComparer, DefaultRecordDisposer<long, long>>>;
    using LongStoreFunctions = StoreFunctions<long, long, LongKeyComparer, DefaultRecordDisposer<long, long>>;

    using RMWValueAllocator = GenericAllocator<int, RMWValueObj, StoreFunctions<int, RMWValueObj, IntKeyComparer, DefaultRecordDisposer<int, RMWValueObj>>>;
    using RMWValueStoreFunctions = StoreFunctions<int, RMWValueObj, IntKeyComparer, DefaultRecordDisposer<int, RMWValueObj>>;

    [AllureNUnit]
    [TestFixture]
    internal class NeedCopyUpdateTests
    {
        private TsavoriteKV<int, RMWValueObj, RMWValueStoreFunctions, RMWValueAllocator> store;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "tests.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "tests.obj.log"), deleteOnClose: true);

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                MemorySize = 1L << 15,
                PageSize = 1L << 10
            }, StoreFunctions<int, RMWValueObj>.Create(IntKeyComparer.Instance, keySerializerCreator: null, () => new RMWValueSerializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;
            DeleteDirectory(MethodTestDir);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void TryAddTest()
        {
            TryAddTestFunctions functions = new();
            using var session = store.NewSession<RMWValueObj, RMWValueObj, Status, TryAddTestFunctions>(functions);
            var bContext = session.BasicContext;

            Status status;
            var key = 1;
            var value1 = new RMWValueObj { value = 1 };
            var value2 = new RMWValueObj { value = 2 };

            functions.noNeedInitialUpdater = true;
            status = bContext.RMW(ref key, ref value1); // needInitialUpdater false + NOTFOUND
            ClassicAssert.IsFalse(status.Found, status.ToString());
            ClassicAssert.IsFalse(value1.flag); // InitialUpdater is not called
            functions.noNeedInitialUpdater = false;

            status = bContext.RMW(ref key, ref value1); // InitialUpdater + NotFound
            ClassicAssert.IsFalse(status.Found, status.ToString());
            ClassicAssert.IsTrue(value1.flag); // InitialUpdater is called

            status = bContext.RMW(ref key, ref value2); // InPlaceUpdater + Found
            ClassicAssert.IsTrue(status.Record.InPlaceUpdated, status.ToString());

            store.Log.Flush(true);
            status = bContext.RMW(ref key, ref value2); // NeedCopyUpdate returns false, so RMW returns simply Found
            ClassicAssert.IsTrue(status.Found, status.ToString());

            store.Log.FlushAndEvict(true);
            status = bContext.RMW(ref key, ref value2, new(StatusCode.Found)); // PENDING + NeedCopyUpdate + Found
            ClassicAssert.IsTrue(status.IsPending, status.ToString());
            _ = bContext.CompletePendingWithOutputs(out var outputs, true);

            var output = new RMWValueObj();
            (status, output) = GetSinglePendingResult(outputs);
            ClassicAssert.IsTrue(status.Found, status.ToString()); // NeedCopyUpdate returns false, so RMW returns simply Found

            // Test stored value. Should be value1
            status = bContext.Read(ref key, ref value1, ref output, new(StatusCode.Found));
            ClassicAssert.IsTrue(status.IsPending, status.ToString());
            _ = bContext.CompletePending(true);

            status = bContext.Delete(ref key);
            ClassicAssert.IsTrue(!status.Found && status.Record.Created, status.ToString());
            _ = bContext.CompletePending(true);
            store.Log.FlushAndEvict(true);
            status = bContext.RMW(ref key, ref value2, new(StatusCode.NotFound | StatusCode.CreatedRecord)); // PENDING + InitialUpdater + NOTFOUND
            ClassicAssert.IsTrue(status.IsPending, status.ToString());
            _ = bContext.CompletePending(true);
        }

        internal class RMWValueObj
        {
            public int value;
            public bool flag;
        }

        internal class RMWValueSerializer : BinaryObjectSerializer<RMWValueObj>
        {
            public override void Serialize(ref RMWValueObj value)
            {
                writer.Write(value.value);
            }

            public override void Deserialize(out RMWValueObj value)
            {
                value = new RMWValueObj
                {
                    value = reader.ReadInt32()
                };
            }
        }

        internal class TryAddTestFunctions : TryAddFunctions<int, RMWValueObj, Status>
        {
            internal bool noNeedInitialUpdater;

            public override bool NeedInitialUpdate(ref int key, ref RMWValueObj input, ref RMWValueObj output, ref RMWInfo rmwInfo)
            {
                return !noNeedInitialUpdater && base.NeedInitialUpdate(ref key, ref input, ref output, ref rmwInfo);
            }

            public override bool InitialUpdater(ref int key, ref RMWValueObj input, ref RMWValueObj value, ref RMWValueObj output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                input.flag = true;
                _ = base.InitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo);
                return true;
            }

            public override bool CopyUpdater(ref int key, ref RMWValueObj input, ref RMWValueObj oldValue, ref RMWValueObj newValue, ref RMWValueObj output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                Assert.Fail("CopyUpdater");
                return false;
            }

            public override void RMWCompletionCallback(ref int key, ref RMWValueObj input, ref RMWValueObj output, Status ctx, Status status, RecordMetadata recordMetadata)
            {
                ClassicAssert.AreEqual(ctx, status);

                if (!status.Found)
                    ClassicAssert.IsTrue(input.flag); // InitialUpdater is called.
            }

            public override void ReadCompletionCallback(ref int key, ref RMWValueObj input, ref RMWValueObj output, Status ctx, Status status, RecordMetadata recordMetadata)
            {
                ClassicAssert.AreEqual(output.value, input.value);
            }
        }
    }

    [AllureNUnit]
    [TestFixture]
    internal class NeedCopyUpdateTestsSinglePage : AllureTestBase
    {
        private TsavoriteKV<long, long, LongStoreFunctions, LongAllocator> store;
        private IDevice log;

        const int PageSizeBits = 16;
        const int RecsPerPage = (1 << PageSizeBits) / 24;   // 24 bits in RecordInfo, key, value

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "test.log"), deleteOnClose: true);

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                MutableFraction = 0.1,
                MemorySize = 1L << (PageSizeBits + 1),
                PageSize = 1L << PageSizeBits
            }, StoreFunctions<long, long>.Create(LongKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            DeleteDirectory(MethodTestDir);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void CopyUpdateFromHeadReadOnlyPageTest()
        {
            RMWSinglePageFunctions functions = new();
            using var session = store.NewSession<long, long, Empty, RMWSinglePageFunctions>(functions);
            var bContext = session.BasicContext;

            // Two records is the most that can "fit" into the first Constants.kFirstValueAddress "range"; therefore when we close pages
            // after flushing, ClosedUntilAddress will be aligned with the end of the page, so we will succeed in the allocation that
            // caused the HeadAddress to be moved above logicalAddress in CreateNewRecordRMW.
            const int padding = 2;

            for (int key = 0; key < RecsPerPage - padding; key++)
            {
                var status = bContext.RMW(key, key << 32 + key);
                ClassicAssert.IsTrue(status.IsCompletedSuccessfully, status.ToString());
            }

            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            // This should trigger CopyUpdater, after flushing the oldest page (closest to HeadAddress).
            for (int key = 0; key < RecsPerPage - padding; key++)
            {
                var status = bContext.RMW(key, key << 32 + key);
                if (status.IsPending)
                    _ = bContext.CompletePending(wait: true);
            }
        }

        internal class RMWSinglePageFunctions : SimpleSimpleFunctions<long, long>
        {
        }
    }
}