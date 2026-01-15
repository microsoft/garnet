// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using LongAllocator = SpanByteAllocator<StoreFunctions<LongKeyComparer, SpanByteRecordDisposer>>;
    using LongStoreFunctions = StoreFunctions<LongKeyComparer, SpanByteRecordDisposer>;

    using RMWValueAllocator = ObjectAllocator<StoreFunctions<IntKeyComparer, DefaultRecordDisposer>>;
    using RMWValueStoreFunctions = StoreFunctions<IntKeyComparer, DefaultRecordDisposer>;

    [TestFixture]
    internal class NeedCopyUpdateTests
    {
        private TsavoriteKV<RMWValueStoreFunctions, RMWValueAllocator> store;
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
            }, StoreFunctions.Create(IntKeyComparer.Instance, () => new RMWValueSerializer())
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
            status = bContext.RMW(SpanByte.FromPinnedVariable(ref key), ref value1); // needInitialUpdater false + NOTFOUND
            ClassicAssert.IsFalse(status.Found, status.ToString());
            ClassicAssert.IsFalse(value1.flag); // InitialUpdater is not called
            functions.noNeedInitialUpdater = false;

            status = bContext.RMW(SpanByte.FromPinnedVariable(ref key), ref value1); // InitialUpdater + NotFound
            ClassicAssert.IsFalse(status.Found, status.ToString());
            ClassicAssert.IsTrue(value1.flag); // InitialUpdater is called

            status = bContext.RMW(SpanByte.FromPinnedVariable(ref key), ref value2); // InPlaceUpdater + Found
            ClassicAssert.IsTrue(status.Record.InPlaceUpdated, status.ToString());

            store.Log.Flush(true);
            status = bContext.RMW(SpanByte.FromPinnedVariable(ref key), ref value2); // NeedCopyUpdate returns false, so RMW returns simply Found
            ClassicAssert.IsTrue(status.Found, status.ToString());

            store.Log.FlushAndEvict(true);
            status = bContext.RMW(SpanByte.FromPinnedVariable(ref key), ref value2, new(StatusCode.Found)); // PENDING + NeedCopyUpdate + Found
            ClassicAssert.IsTrue(status.IsPending, status.ToString());
            _ = bContext.CompletePendingWithOutputs(out var outputs, true);

            var output = new RMWValueObj();
            (status, output) = GetSinglePendingResult(outputs);
            ClassicAssert.IsTrue(status.Found, status.ToString()); // NeedCopyUpdate returns false, so RMW returns simply Found

            // Test stored value. Should be value1
            status = bContext.Read(SpanByte.FromPinnedVariable(ref key), ref value1, ref output, new(StatusCode.Found));
            ClassicAssert.IsTrue(status.IsPending, status.ToString());
            _ = bContext.CompletePending(true);

            status = bContext.Delete(SpanByte.FromPinnedVariable(ref key));
            ClassicAssert.IsTrue(!status.Found && status.Record.Created, status.ToString());
            _ = bContext.CompletePending(true);
            store.Log.FlushAndEvict(true);
            status = bContext.RMW(SpanByte.FromPinnedVariable(ref key), ref value2, new(StatusCode.NotFound | StatusCode.CreatedRecord)); // PENDING + InitialUpdater + NOTFOUND
            ClassicAssert.IsTrue(status.IsPending, status.ToString());
            _ = bContext.CompletePending(true);
        }

        internal class RMWValueObj : HeapObjectBase
        {
            public int value;
            public bool flag;

            public override IHeapObject Clone() => throw new System.NotImplementedException();
            public override void Dispose() => throw new System.NotImplementedException();
            public override void DoSerialize(BinaryWriter writer) => throw new System.NotImplementedException();
            public override void WriteType(BinaryWriter writer, bool isNull) => throw new System.NotImplementedException();
        }

        internal class RMWValueSerializer : BinaryObjectSerializer<IHeapObject>
        {
            public override void Serialize(IHeapObject value)
            {
                writer.Write(((RMWValueObj)value).value);
            }

            public override void Deserialize(out IHeapObject value)
            {
                value = new RMWValueObj
                {
                    value = reader.ReadInt32()
                };
            }
        }

        internal class TryAddTestFunctions : SessionFunctionsBase<RMWValueObj, RMWValueObj, Status>
        {
            internal bool noNeedInitialUpdater;

            public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref RMWValueObj input, ref RMWValueObj output, ref ReadInfo readInfo)
            {
                output = (RMWValueObj)srcLogRecord.ValueObject;
                return true;
            }

            public override bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref RMWValueObj input, ref RMWValueObj output, ref RMWInfo rmwInfo)
                => !noNeedInitialUpdater;

            public override bool InitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref RMWValueObj input, ref RMWValueObj output, ref RMWInfo rmwInfo)
            {
                input.flag = true;
                Assert.That(dstLogRecord.TrySetValueObjectAndPrepareOptionals(input, in sizeInfo));
                output = input;
                return true;
            }

            public override bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref RMWValueObj input, ref RMWValueObj output, ref RMWInfo rmwInfo)
                => false;

            public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref RMWValueObj input, ref RMWValueObj output, ref RMWInfo rmwInfo)
            {
                Assert.Fail("CopyUpdater should not be called here");
                return false;
            }

            public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref RMWValueObj input)
                => new() { KeySize = srcLogRecord.Key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
            /// <inheritdoc/>
            public override RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref RMWValueObj input)
                => new() { KeySize = key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
            /// <inheritdoc/>

            public override void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref RMWValueObj input, ref RMWValueObj output, Status ctx, Status status, RecordMetadata recordMetadata)
            {
                ClassicAssert.AreEqual(ctx, status);

                if (!status.Found)
                    ClassicAssert.IsTrue(input.flag); // InitialUpdater is called.
            }

            public override void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref RMWValueObj input, ref RMWValueObj output, Status ctx, Status status, RecordMetadata recordMetadata)
            {
                ClassicAssert.AreEqual(output.value, input.value);
            }
        }
    }

    [TestFixture]
    internal class NeedCopyUpdateTestsSinglePage
    {
        private TsavoriteKV<LongStoreFunctions, LongAllocator> store;
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
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
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

            for (long key = 0; key < RecsPerPage - padding; key++)
            {
                long value = ((int)key << 32) + key;
                var status = bContext.RMW(SpanByte.FromPinnedVariable(ref key), ref value);
                ClassicAssert.IsTrue(status.IsCompletedSuccessfully, status.ToString());
            }

            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            // This should trigger CopyUpdater, after flushing the oldest page (closest to HeadAddress).
            for (long key = 0; key < RecsPerPage - padding; key++)
            {
                long value = ((int)key << 32) + key;
                var status = bContext.RMW(SpanByte.FromPinnedVariable(ref key), ref value);
                if (status.IsPending)
                    _ = bContext.CompletePending(wait: true);
            }
        }

        internal class RMWSinglePageFunctions : SimpleLongSimpleFunctions
        {
        }
    }
}