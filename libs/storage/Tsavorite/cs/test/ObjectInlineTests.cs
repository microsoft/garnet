// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using ClassAllocator = ObjectAllocator<TestObjectValue, StoreFunctions<TestObjectValue, TestObjectKey.Comparer, DefaultRecordDisposer<TestObjectValue>>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectValue, TestObjectKey.Comparer, DefaultRecordDisposer<TestObjectValue>>;

    [TestFixture]
    internal class ObjectInlineTests
    {
        private TsavoriteKV<TestObjectValue, ClassStoreFunctions, ClassAllocator> store;
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
            }, StoreFunctions<TestObjectValue>.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer(), DefaultRecordDisposer<TestObjectValue>.Instance)
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
            SpanByte key = SpanByteFrom(ref keyStruct);
            TestObjectInput input = new() { value = 23 };
            TestObjectOutput output = default;

            // Overflow<->Inline conversions

            // Start with an inline value.
            input.wantValueStyle = TestValueStyle.Inline;
            _ = bContext.Upsert(key, ref input, desiredValue: null, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.None));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            input.value = 24;
            _ = bContext.Upsert(key, ref input, desiredValue: null, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.None));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            input.value = 25;
            input.wantValueStyle = TestValueStyle.Overflow;
            _ = bContext.Upsert(key, ref input, desiredValue: null, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.None));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            input.value = 26;
            _ = bContext.Upsert(key, ref input, desiredValue: null, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.None));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            // Overflow<->Object conversions

            input.value = 30;
            input.wantValueStyle = TestValueStyle.Object;   // Overflow -> Object
            _ = bContext.Upsert(key, ref input, desiredValue: null, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.None));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Object));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            input.value = 31;
            input.wantValueStyle = TestValueStyle.Overflow;   // Object -> Overflow
            _ = bContext.Upsert(key, ref input, desiredValue: null, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.None));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Overflow));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            input.value = 32;
            input.wantValueStyle = TestValueStyle.Object;   // Overflow -> Object again
            _ = bContext.Upsert(key, ref input, desiredValue: null, ref output);
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
            _ = bContext.Upsert(key, ref input, desiredValue: null, ref output);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.None));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.destValueStyle, Is.EqualTo(TestValueStyle.Inline));
            Assert.That(output.value.value, Is.EqualTo(input.value));

            input.value = 41;
            input.wantValueStyle = TestValueStyle.Object;   // Inline -> Object
            _ = bContext.Upsert(key, ref input, desiredValue: null, ref output);
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
            SpanByte key = SpanByteFrom(ref keyStruct);
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
            SpanByte GetOverflowValueSpanByte() => SpanByte.FromPinnedSpan(new ReadOnlySpan<byte>(pinnedValueOverflowBytes));

            public override bool InitialUpdater(ref LogRecord<TestObjectValue> logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            {
                Assert.That(sizeInfo.ValueIsInline, Is.EqualTo(logRecord.Info.ValueIsInline), $"Non-IPU mismatch in sizeInfo ({sizeInfo.ValueIsInline}) and dstLogRecord ({logRecord.Info.ValueIsInline}) ValueIsInline in {Utility.GetCurrentMethodName()}");
                return DoWriter(ref logRecord, ref sizeInfo, ref input, srcValue: null, ref output);
            }

            public override bool InPlaceUpdater(ref LogRecord<TestObjectValue> logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            {
                // Use the same record for source and dest; DoUpdater does not modify dest until all source info is processed.
                return DoUpdater(ref logRecord, ref logRecord, ref sizeInfo, input, ref output);
            }

            public override bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<TestObjectValue> dstLogRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            {
                Assert.That(sizeInfo.ValueIsInline, Is.EqualTo(dstLogRecord.Info.ValueIsInline), $"Non-IPU mismatch in sizeInfo ({sizeInfo.ValueIsInline}) and dstLogRecord ({dstLogRecord.Info.ValueIsInline}) ValueIsInline in {Utility.GetCurrentMethodName()}");
                return DoUpdater(ref srcLogRecord, ref dstLogRecord, ref sizeInfo, input, ref output);
            }

            private bool DoUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<TestObjectValue> logRecord, ref RecordSizeInfo sizeInfo, TestObjectInput input, ref TestObjectOutput output)
                where TSourceLogRecord : ISourceLogRecord<TestObjectValue>
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
                    unsafe { srcValue = (int)*(long*)srcLogRecord.ValueSpan.ToPointer(); }
                }
                else
                    srcValue = srcLogRecord.ValueObject.value;

                output.value = srcLogRecord.Info.ValueIsObject ? srcLogRecord.ValueObject : new TestObjectValue { value = (int)srcValue };
                output.value.value += input.value;

                switch (output.destValueStyle)
                {
                    case TestValueStyle.Inline:
                        ValueStruct valueStruct = new() { vfield1 = srcValue + input.value, vfield2 = (srcValue + input.value) * 100 };
                        return logRecord.TrySetValueSpan(SpanByteFrom(ref valueStruct), ref sizeInfo);
                    case TestValueStyle.Overflow:
                        SpanByte overflowValue = GetOverflowValueSpanByte();
                        unsafe { *(long*)overflowValue.ToPointer() = srcValue + input.value; }
                        return logRecord.TrySetValueSpan(overflowValue, ref sizeInfo);
                    case TestValueStyle.Object:
                        return logRecord.TrySetValueObject(output.value, ref sizeInfo);
                    default:
                        Assert.Fail("Unknown value style");
                        return false;
                }
            }

            public override bool ConcurrentReader(ref LogRecord<TestObjectValue> logRecord, ref TestObjectInput input, ref TestObjectOutput output, ref ReadInfo readInfo)
                => SingleReader(ref logRecord, ref input, ref output, ref readInfo);

            public override bool ConcurrentWriter(ref LogRecord<TestObjectValue> logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, TestObjectValue srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo)
            {
                Set(ref output.srcValueStyle, logRecord.Info);
                return DoWriter(ref logRecord, ref sizeInfo, ref input, srcValue, ref output);
            }

            public override bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, ref ReadInfo readInfo)
            {
                Set(ref output.srcValueStyle, srcLogRecord.Info);

                // If the value is inline it is a ValueStruct; if it is overflow it is a buffer with the first long set to the desired value.
                if (srcLogRecord.Info.ValueIsInline)
                    output.value = new TestObjectValue() { value = (int)srcLogRecord.ValueSpan.AsRef<ValueStruct>().vfield1 };
                else if (srcLogRecord.Info.ValueIsOverflow)
                { 
                    Assert.That(srcLogRecord.ValueSpan.Length, Is.EqualTo(OverflowValueSize));
                    unsafe { output.value = new TestObjectValue() { value = (int)*(long*)srcLogRecord.ValueSpan.ToPointer() }; }
                }
                else
                    output.value = srcLogRecord.ValueObject;
                return true;
            }

            public override bool SingleWriter(ref LogRecord<TestObjectValue> logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, TestObjectValue srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
            {
                Assert.That(sizeInfo.ValueIsInline, Is.EqualTo(logRecord.Info.ValueIsInline), $"Non-IPU mismatch in sizeInfo ({sizeInfo.ValueIsInline}) and dstLogRecord ({logRecord.Info.ValueIsInline}) ValueIsInline in {Utility.GetCurrentMethodName()}");
                return DoWriter(ref logRecord, ref sizeInfo, ref input, srcValue, ref output);
            }

            private bool DoWriter(ref LogRecord<TestObjectValue> logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, TestObjectValue srcValue, ref TestObjectOutput output)
            {
                Assert.That(srcValue, Is.Null, "srcValue should be null for these upsert tests; use Input instead");
                output.srcValueStyle = TestValueStyle.None;
                SetAndVerify(ref input, ref output.destValueStyle, sizeInfo.ValueIsInline, sizeInfo.ValueIsOverflow);

                output.value = new TestObjectValue { value = input.value };
                switch (output.destValueStyle)
                {
                    case TestValueStyle.Inline:
                        ValueStruct valueStruct = new() { vfield1 = input.value, vfield2 = input.value * 100 };
                        return logRecord.TrySetValueSpan(SpanByteFrom(ref valueStruct), ref sizeInfo);
                    case TestValueStyle.Overflow:
                        SpanByte overflowValue = GetOverflowValueSpanByte();
                        unsafe { *(long*)(overflowValue.ToPointer()) = input.value; }
                        return logRecord.TrySetValueSpan(overflowValue, ref sizeInfo);
                    case TestValueStyle.Object:
                        return logRecord.TrySetValueObject(output.value, ref sizeInfo);
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

            static RecordFieldInfo GetFieldInfo(SpanByte key, ref TestObjectInput input)
                => new()
                {
                    KeyDataSize = key.Length,
                    ValueDataSize = input.wantValueStyle switch
                    {
                        TestValueStyle.Inline => ValueStruct.AsSpanByteDataSize,
                        TestValueStyle.Overflow => OverflowValueSize,
                        TestValueStyle.Object => ObjectIdMap.ObjectIdSize,
                        _ => int.MaxValue
                    },
                    ValueIsObject = input.wantValueStyle == TestValueStyle.Object
                };

            public override unsafe RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TestObjectInput input)
                => GetFieldInfo(srcLogRecord.Key, ref input);
            public override unsafe RecordFieldInfo GetRMWInitialFieldInfo(SpanByte key, ref TestObjectInput input)
                => GetFieldInfo(key, ref input);
            public override unsafe RecordFieldInfo GetUpsertFieldInfo(SpanByte key, TestObjectValue value, ref TestObjectInput input)
                => GetFieldInfo(key, ref input);
        }
    }
}
