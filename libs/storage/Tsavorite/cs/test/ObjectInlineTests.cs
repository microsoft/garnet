// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.Objects
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>;

    [AllureNUnit]
    [TestFixture]
    internal class ObjectInlineTests : AllureTestBase
    {
        private TsavoriteKV<ClassStoreFunctions, ClassAllocator> store;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectTests.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectTests.obj.log"), deleteOnClose: true);

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                MemorySize = 1L << 15,
                PageSize = 1L << 10
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer(), DefaultRecordDisposer.Instance)
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

        [Test, Category(TsavoriteKVTestCategory), Category(SmokeTestCategory), Category(ObjectIdMapCategory)]
        public void ObjectAsInlineStructUpsertTest()
        {
            using var session = store.NewSession<TestObjectInput, TestObjectOutput, Empty, TestInlineObjectFunctions>(new TestInlineObjectFunctions());
            var bContext = session.BasicContext;

            TestObjectKey keyStruct = new() { key = 9999999 };
            Span<byte> key = SpanByte.FromPinnedVariable(ref keyStruct);
            TestObjectInput input = new() { value = 23 };
            TestObjectOutput output = default;

            // Overflow<->Inline conversions

            // Start with an inline value.
            input.wantValueStyle = TestValueStyle.Inline;
            _ = bContext.Upsert(key, ref input, desiredValue: (IHeapObject)null, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.None));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            input.value = 24;
            _ = bContext.Upsert(key, ref input, desiredValue: (IHeapObject)null, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.None));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            input.value = 25;
            input.wantValueStyle = TestValueStyle.Overflow;
            _ = bContext.Upsert(key, ref input, desiredValue: (IHeapObject)null, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.None));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            input.value = 26;
            _ = bContext.Upsert(key, ref input, desiredValue: (IHeapObject)null, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.None));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            // Overflow<->Object conversions

            input.value = 30;
            input.wantValueStyle = TestValueStyle.Object;   // Overflow -> Object
            _ = bContext.Upsert(key, ref input, desiredValue: (IHeapObject)null, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.None));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            input.value = 31;
            input.wantValueStyle = TestValueStyle.Overflow;   // Object -> Overflow
            _ = bContext.Upsert(key, ref input, desiredValue: (IHeapObject)null, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.None));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            input.value = 32;
            input.wantValueStyle = TestValueStyle.Object;   // Overflow -> Object again
            _ = bContext.Upsert(key, ref input, desiredValue: (IHeapObject)null, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.None));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            // Object<->Inline conversions

            input.value = 40;
            input.wantValueStyle = TestValueStyle.Inline;   // Object -> Inline
            _ = bContext.Upsert(key, ref input, desiredValue: (IHeapObject)null, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.None));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            input.value = 41;
            input.wantValueStyle = TestValueStyle.Object;   // Inline -> Object
            _ = bContext.Upsert(key, ref input, desiredValue: (IHeapObject)null, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.None));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.value.value, Is.EqualTo(input.value));
        }

        [Test, Category(TsavoriteKVTestCategory), Category(SmokeTestCategory), Category(ObjectIdMapCategory)]
        public void ObjectAsInlineStructRMWTest()
        {
            using var session = store.NewSession<TestObjectInput, TestObjectOutput, Empty, TestInlineObjectFunctions>(new TestInlineObjectFunctions());
            var bContext = session.BasicContext;

            TestObjectKey keyStruct = new() { key = 9999999 };
            Span<byte> key = SpanByte.FromPinnedVariable(ref keyStruct);
            TestObjectInput input = new() { value = 23 };
            TestObjectOutput output = default;

            // Overflow<->Inline conversions

            // Start with an inline value.
            input.wantValueStyle = TestValueStyle.Inline;
            _ = bContext.RMW(key, ref input, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.None));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.value.value, Is.EqualTo(input.value));
            var priorSum = input.value;

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            input.value = 24;
            _ = bContext.RMW(key, ref input, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.value.value, Is.EqualTo(priorSum + input.value));
            priorSum += input.value;

            input.value = 25;
            input.wantValueStyle = TestValueStyle.Overflow;
            _ = bContext.RMW(key, ref input, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.value.value, Is.EqualTo(priorSum + input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.value.value, Is.EqualTo(priorSum + input.value));
            priorSum += input.value;

            input.value = 26;
            _ = bContext.RMW(key, ref input, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.value.value, Is.EqualTo(priorSum + input.value));
            priorSum += input.value;

            // Overflow<->Object conversions

            input.value = 30;
            input.wantValueStyle = TestValueStyle.Object;   // Overflow -> Object
            _ = bContext.RMW(key, ref input, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.value.value, Is.EqualTo(priorSum + input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.value.value, Is.EqualTo(priorSum + input.value));
            priorSum += input.value;

            input.value = 31;
            input.wantValueStyle = TestValueStyle.Overflow;   // Object -> Overflow
            _ = bContext.RMW(key, ref input, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.value.value, Is.EqualTo(priorSum + input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.value.value, Is.EqualTo(priorSum + input.value));
            priorSum += input.value;

            input.value = 32;
            input.wantValueStyle = TestValueStyle.Object;   // Overflow -> Object again
            _ = bContext.RMW(key, ref input, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.value.value, Is.EqualTo(priorSum + input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.value.value, Is.EqualTo(priorSum + input.value));
            priorSum += input.value;

            // Object<->Inline conversions

            input.value = 40;
            input.wantValueStyle = TestValueStyle.Inline;   // Object -> Inline
            _ = bContext.RMW(key, ref input, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.value.value, Is.EqualTo(priorSum + input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.value.value, Is.EqualTo(priorSum + input.value));
            priorSum += input.value;

            input.value = 41;
            input.wantValueStyle = TestValueStyle.Object;   // Inline -> Object
            _ = bContext.RMW(key, ref input, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.value.value, Is.EqualTo(priorSum + input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.value.value, Is.EqualTo(priorSum + input.value));
        }

        public class TestInlineObjectFunctions : TestObjectFunctions
        {
            // Force test of overflow values
            const int OverflowValueSize = 1 << (LogSettings.kDefaultMaxInlineValueSizeBits + 1);
            byte[] pinnedValueOverflowBytes = GC.AllocateArray<byte>(OverflowValueSize, pinned: true);
            Span<byte> GetOverflowValueSpanByte() => new(pinnedValueOverflowBytes);

            public override bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            {
                Assert.That(sizeInfo.ValueIsInline, Is.EqualTo(logRecord.Info.ValueIsInline), $"Non-IPU mismatch in sizeInfo ({sizeInfo.ValueIsInline}) and dstLogRecord ({logRecord.Info.ValueIsInline}) ValueIsInline in {Utility.GetCurrentMethodName()}");
                return DoWriter(ref logRecord, in sizeInfo, ref input, srcValue: null, ref output);
            }

            public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            {
                // Use the same record for source and dest; DoUpdater does not modify dest until all source info is processed.
                return DoUpdater(in logRecord, ref logRecord, in sizeInfo, input, ref output);
            }

            public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            {
                Assert.That(sizeInfo.ValueIsInline, Is.EqualTo(dstLogRecord.Info.ValueIsInline), $"Non-IPU mismatch in sizeInfo ({sizeInfo.ValueIsInline}) and dstLogRecord ({dstLogRecord.Info.ValueIsInline}) ValueIsInline in {Utility.GetCurrentMethodName()}");
                return DoUpdater(in srcLogRecord, ref dstLogRecord, in sizeInfo, input, ref output);
            }

            private bool DoUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord logRecord, in RecordSizeInfo sizeInfo, TestObjectInput input, ref TestObjectOutput output)
                where TSourceLogRecord : ISourceLogRecord
            {
                Set(ref output.srcValueStyle, srcLogRecord.Info);
                SetAndVerify(ref input, ref output.destValueStyle, sizeInfo.ValueIsInline, sizeInfo.ValueIsOverflow);

                // If the value is inline it is a ValueStruct; if it is overflow it is a buffer with the first long set to the desired value.
                long srcValue;
                if (srcLogRecord.Info.ValueIsInline)
                    srcValue = (int)srcLogRecord.ValueSpan.AsRef<ValueStruct>().vfield1;
                else if (srcLogRecord.Info.ValueIsOverflow)
                {
                    Assert.That(srcLogRecord.ValueSpan.Length, Is.EqualTo(OverflowValueSize));
                    srcValue = (int)srcLogRecord.ValueSpan.AsRef<long>();
                }
                else
                    srcValue = ((TestObjectValue)srcLogRecord.ValueObject).value;

                output.value = srcLogRecord.Info.ValueIsObject ? (TestObjectValue)srcLogRecord.ValueObject : new TestObjectValue { value = (int)srcValue };

                var result = false;
                switch (output.destValueStyle)
                {
                    case TestValueStyle.Inline:
                        ValueStruct valueStruct = new() { vfield1 = srcValue + input.value, vfield2 = (srcValue + input.value) * 100 };
                        result = logRecord.TrySetValueSpanAndPrepareOptionals(SpanByte.FromPinnedVariable(ref valueStruct), in sizeInfo);
                        break;
                    case TestValueStyle.Overflow:
                        Span<byte> overflowValue = GetOverflowValueSpanByte();
                        overflowValue.AsRef<long>() = srcValue + input.value;
                        result = logRecord.TrySetValueSpanAndPrepareOptionals(overflowValue, in sizeInfo);
                        break;
                    case TestValueStyle.Object:
                        result = logRecord.TrySetValueObjectAndPrepareOptionals(output.value, in sizeInfo);
                        break;
                    default:
                        Assert.Fail("Unknown value style");
                        return false;
                }

                if (result)
                    output.value.value += input.value;
                return result;
            }

            public override bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, IHeapObject srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo)
            {
                Set(ref output.srcValueStyle, logRecord.Info);
                return DoWriter(ref logRecord, in sizeInfo, ref input, (TestObjectValue)srcValue, ref output);
            }

            public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, ref ReadInfo readInfo)
            {
                Set(ref output.srcValueStyle, srcLogRecord.Info);

                // If the value is inline it is a ValueStruct; if it is overflow it is a buffer with the first long set to the desired value.
                if (srcLogRecord.Info.ValueIsInline)
                    output.value = new TestObjectValue() { value = (int)srcLogRecord.ValueSpan.AsRef<ValueStruct>().vfield1 };
                else if (srcLogRecord.Info.ValueIsOverflow)
                {
                    Assert.That(srcLogRecord.ValueSpan.Length, Is.EqualTo(OverflowValueSize));
                    unsafe { output.value = new TestObjectValue() { value = (int)srcLogRecord.ValueSpan.AsRef<long>() }; }
                }
                else
                    output.value = (TestObjectValue)srcLogRecord.ValueObject;
                return true;
            }

            public override bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, IHeapObject srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo)
            {
                Assert.That(sizeInfo.ValueIsInline, Is.EqualTo(logRecord.Info.ValueIsInline), $"Non-IPU mismatch in sizeInfo ({sizeInfo.ValueIsInline}) and dstLogRecord ({logRecord.Info.ValueIsInline}) ValueIsInline in {Utility.GetCurrentMethodName()}");
                return DoWriter(ref logRecord, in sizeInfo, ref input, (TestObjectValue)srcValue, ref output);
            }

            private bool DoWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, TestObjectValue srcValue, ref TestObjectOutput output)
            {
                Assert.That(srcValue, Is.Null, "srcValue should be null for these upsert tests; use Input instead");
                output.srcValueStyle = TestValueStyle.None;
                SetAndVerify(ref input, ref output.destValueStyle, sizeInfo.ValueIsInline, sizeInfo.ValueIsOverflow);

                output.value = new TestObjectValue { value = input.value };
                switch (output.destValueStyle)
                {
                    case TestValueStyle.Inline:
                        ValueStruct valueStruct = new() { vfield1 = input.value, vfield2 = input.value * 100 };
                        return logRecord.TrySetValueSpanAndPrepareOptionals(SpanByte.FromPinnedVariable(ref valueStruct), in sizeInfo);
                    case TestValueStyle.Overflow:
                        Span<byte> overflowValue = GetOverflowValueSpanByte();
                        overflowValue.AsRef<long>() = input.value;
                        return logRecord.TrySetValueSpanAndPrepareOptionals(overflowValue, in sizeInfo);
                    case TestValueStyle.Object:
                        return logRecord.TrySetValueObjectAndPrepareOptionals(output.value, in sizeInfo);
                    default:
                        Assert.Fail("Unknown value style");
                        return false;
                }
            }

            static void Set(ref TestValueStyle style, RecordInfo recordInfo) => Set(ref style, recordInfo.ValueIsInline, recordInfo.ValueIsOverflow);

            static void Set(ref TestValueStyle style, bool isInline, bool isOverflow)
            {
                style = (isInline, isOverflow) switch
                {
                    (true, false) => TestValueStyle.Inline,
                    (false, true) => TestValueStyle.Overflow,
                    _ => TestValueStyle.Object
                };
            }
            static void SetAndVerify(ref TestObjectInput input, ref TestValueStyle style, bool isInline, bool isOverflow)
            {
                Set(ref style, isInline, isOverflow);
                Assert.That(input.wantValueStyle, Is.EqualTo(style));
            }

            static RecordFieldInfo GetFieldInfo(ReadOnlySpan<byte> key, ref TestObjectInput input)
                => new()
                {
                    KeySize = key.Length,
                    ValueSize = input.wantValueStyle switch
                    {
                        TestValueStyle.Inline => ValueStruct.AsSpanByteDataSize,
                        TestValueStyle.Overflow => OverflowValueSize,
                        TestValueStyle.Object => ObjectIdMap.ObjectIdSize,
                        _ => int.MaxValue
                    },
                    ValueIsObject = input.wantValueStyle == TestValueStyle.Object
                };

            public override unsafe RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TestObjectInput input)
                => GetFieldInfo(srcLogRecord.Key, ref input);
            public override unsafe RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref TestObjectInput input)
                => GetFieldInfo(key, ref input);
            public override unsafe RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref TestObjectInput input)
                => GetFieldInfo(key, ref input);
        }
    }
}