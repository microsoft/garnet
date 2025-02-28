// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using NUnit.Framework;
using NUnit.Framework.Legacy;
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

            // Start with an inline value.
            input.wantInlineValue = true;
            _ = bContext.Upsert(key, ref input, desiredValue: null, ref output);
            Assert.That(output.srcWasInlineValue, Is.False);
            Assert.That(output.destIsInlineValue, Is.True);
            Assert.That(output.value.value, Is.EqualTo(input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcWasInlineValue, Is.True);
            Assert.That(output.destIsInlineValue, Is.True);
            Assert.That(output.value.value, Is.EqualTo(input.value));

            input.value = 24;
            _ = bContext.Upsert(key, ref input, desiredValue: null, ref output);
            Assert.That(output.srcWasInlineValue, Is.True);
            Assert.That(output.destIsInlineValue, Is.True);
            Assert.That(output.value.value, Is.EqualTo(input.value));

            input.value = 25;
            input.wantInlineValue = false;
            _ = bContext.Upsert(key, ref input, desiredValue: null, ref output);
            Assert.That(output.srcWasInlineValue, Is.True);
            Assert.That(output.destIsInlineValue, Is.False);
            Assert.That(output.value.value, Is.EqualTo(input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcWasInlineValue, Is.False);
            Assert.That(output.destIsInlineValue, Is.False);
            Assert.That(output.value.value, Is.EqualTo(input.value));

            input.value = 26;
            _ = bContext.Upsert(key, ref input, desiredValue: null, ref output);
            Assert.That(output.srcWasInlineValue, Is.False);
            Assert.That(output.destIsInlineValue, Is.False);
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

            // Start with an inline value.
            input.wantInlineValue = true;
            _ = bContext.RMW(key, ref input, ref output);
            Assert.That(output.srcWasInlineValue, Is.False);
            Assert.That(output.destIsInlineValue, Is.True);
            Assert.That(output.value.value, Is.EqualTo(input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcWasInlineValue, Is.True);
            Assert.That(output.destIsInlineValue, Is.True);
            Assert.That(output.value.value, Is.EqualTo(input.value));

            input.value = 24;
            _ = bContext.RMW(key, ref input, ref output);
            Assert.That(output.srcWasInlineValue, Is.True);
            Assert.That(output.destIsInlineValue, Is.True);
            Assert.That(output.value.value, Is.EqualTo(23 + input.value));

            input.value = 25;
            input.wantInlineValue = false;
            _ = bContext.RMW(key, ref input, ref output);
            Assert.That(output.srcWasInlineValue, Is.True);
            Assert.That(output.destIsInlineValue, Is.False);
            Assert.That(output.value.value, Is.EqualTo(23 + 24 + input.value));

            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(output.srcWasInlineValue, Is.False);
            Assert.That(output.destIsInlineValue, Is.False);
            Assert.That(output.value.value, Is.EqualTo(23 + 24 + input.value));

            input.value = 26;
            _ = bContext.RMW(key, ref input, ref output);
            Assert.That(output.srcWasInlineValue, Is.False);
            Assert.That(output.destIsInlineValue, Is.False);
            Assert.That(output.value.value, Is.EqualTo(23 + 24 + 25 + input.value));
        }

        public class TestInlineObjectFunctions : TestObjectFunctions
        {
            public override bool InitialUpdater(ref LogRecord<TestObjectValue> logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            {
                Assert.That(sizeInfo.ValueIsInline, Is.EqualTo(logRecord.Info.ValueIsInline), $"Non-IPU mismatch in sizeInfo ({sizeInfo.ValueIsInline}) and dstLogRecord ({logRecord.Info.ValueIsInline}) ValueIsInline in {Utility.GetCurrentMethodName()}");
                output.destIsInlineValue = sizeInfo.ValueIsInline;
                if (sizeInfo.ValueIsInline)
                {
                    ValueStruct valueStruct = new() { vfield1 = input.value, vfield2 = input.value * 100 };
                    output.value = new TestObjectValue { value = (int)valueStruct.vfield1 };
                    return logRecord.TrySetValueSpan(SpanByteFrom(ref valueStruct), ref sizeInfo);
                }
                output.value = new TestObjectValue { value = input.value };
                return logRecord.TrySetValueObject(output.value);   // We know it is not inline, so test this overload of TrySetValueObject
            }

            public override bool InPlaceUpdater(ref LogRecord<TestObjectValue> logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            {
                output.srcWasInlineValue = logRecord.Info.ValueIsInline;
                output.destIsInlineValue = sizeInfo.ValueIsInline;
                if (sizeInfo.ValueIsInline)
                {
                    var valueStruct = new ValueStruct()
                    {
                        vfield1 = logRecord.Info.ValueIsInline
                            ? logRecord.ValueSpan.AsRef<ValueStruct>().vfield1 + input.value
                            : logRecord.ValueObject.value + input.value
                    };

                    valueStruct.vfield2 = valueStruct.vfield1 * 100;
                    output.value = new TestObjectValue { value = (int)valueStruct.vfield1 };
                    return logRecord.TrySetValueSpan(SpanByteFrom(ref valueStruct), ref sizeInfo);
                }

                // New record is an object. See if we are converting to an inline.
                if (logRecord.Info.ValueIsInline)
                {
                    output.value = new TestObjectValue { value = (int)logRecord.ValueSpan.AsRef<ValueStruct>().vfield1 + input.value };
                    return logRecord.TrySetValueObject(output.value, ref sizeInfo);
                }

                // Both are object.
                output.value = logRecord.ValueObject;
                logRecord.ValueObject.value += input.value;
                return true;
            }

            public override bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<TestObjectValue> dstLogRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            {
                Assert.That(sizeInfo.ValueIsInline, Is.EqualTo(dstLogRecord.Info.ValueIsInline), $"Non-IPU mismatch in sizeInfo ({sizeInfo.ValueIsInline}) and dstLogRecord ({dstLogRecord.Info.ValueIsInline}) ValueIsInline in {Utility.GetCurrentMethodName()}");
                output.srcWasInlineValue = srcLogRecord.Info.ValueIsInline;
                output.destIsInlineValue = sizeInfo.ValueIsInline;
                if (sizeInfo.ValueIsInline)
                {
                    var valueStruct = new ValueStruct()
                    {
                        vfield1 = srcLogRecord.Info.ValueIsInline
                            ? srcLogRecord.ValueSpan.AsRef<ValueStruct>().vfield1 + input.value
                            : srcLogRecord.ValueObject.value + input.value
                    };

                    valueStruct.vfield2 = valueStruct.vfield1 * 100;
                    output.value = new TestObjectValue { value = (int)valueStruct.vfield1 };
                    return dstLogRecord.TrySetValueSpan(SpanByteFrom(ref valueStruct), ref sizeInfo);
                }

                // New record is an object. See if we are converting to an inline.
                if (srcLogRecord.Info.ValueIsInline)
                {
                    output.value = new TestObjectValue { value = (int)srcLogRecord.ValueSpan.AsRef<ValueStruct>().vfield1 + input.value };
                    return dstLogRecord.TrySetValueObject(output.value, ref sizeInfo);
                }

                // Both are object.
                output.value = new TestObjectValue { value = srcLogRecord.ValueObject.value + input.value };
                return dstLogRecord.TrySetValueObject(output.value);
            }

            public override bool ConcurrentReader(ref LogRecord<TestObjectValue> logRecord, ref TestObjectInput input, ref TestObjectOutput output, ref ReadInfo readInfo)
                => SingleReader(ref logRecord, ref input, ref output, ref readInfo);

            public override bool ConcurrentWriter(ref LogRecord<TestObjectValue> logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, TestObjectValue srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo)
            {
                output.srcWasInlineValue = logRecord.Info.ValueIsInline;
                return DoWriter(ref logRecord, ref sizeInfo, ref input, srcValue, ref output);
            }

            public override bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, ref ReadInfo readInfo)
            {
                output.srcWasInlineValue = output.srcWasInlineValue = srcLogRecord.Info.ValueIsInline;
                if (srcLogRecord.Info.ValueIsInline)
                    output.value = new TestObjectValue() { value = (int)srcLogRecord.ValueSpan.AsRef<ValueStruct>().vfield1 };
                else
                    output.value = srcLogRecord.ValueObject;
                return true;
            }

            public override bool SingleWriter(ref LogRecord<TestObjectValue> logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, TestObjectValue srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
            {
                Assert.That(sizeInfo.ValueIsInline, Is.EqualTo(logRecord.Info.ValueIsInline), $"Non-IPU mismatch in sizeInfo ({sizeInfo.ValueIsInline}) and dstLogRecord ({logRecord.Info.ValueIsInline}) ValueIsInline in {Utility.GetCurrentMethodName()}");
                return DoWriter(ref logRecord, ref sizeInfo, ref input, srcValue, ref output);
            }

            private static bool DoWriter(ref LogRecord<TestObjectValue> logRecord, ref RecordSizeInfo sizeInfo, ref TestObjectInput input, TestObjectValue srcValue, ref TestObjectOutput output)
            {
                Assert.That(srcValue, Is.Null, "srcValue should be null for these upsert tests; use Input instead");
                output.value = new TestObjectValue { value = input.value };
                output.destIsInlineValue = sizeInfo.ValueIsInline;
                if (sizeInfo.ValueIsInline)
                {
                    var valueStruct = new ValueStruct() { vfield1 = input.value, vfield2 = input.value * 100 };
                    return logRecord.TrySetValueSpan(SpanByteFrom(ref valueStruct), ref sizeInfo);
                }
                return logRecord.TrySetValueObject(output.value, ref sizeInfo);
            }

            public override unsafe RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref TestObjectInput input)
                => new() { KeyTotalSize = srcLogRecord.Key.TotalSize, ValueTotalSize = input.wantInlineValue ? ValueStruct.AsSpanByteTotalSize : RecordFieldInfo.ValueObjectIdSize };
            public override unsafe RecordFieldInfo GetRMWInitialFieldInfo(SpanByte key, ref TestObjectInput input)
                => new() { KeyTotalSize = key.TotalSize, ValueTotalSize = input.wantInlineValue ? ValueStruct.AsSpanByteTotalSize : RecordFieldInfo.ValueObjectIdSize };
            public override unsafe RecordFieldInfo GetUpsertFieldInfo(SpanByte key, TestObjectValue value, ref TestObjectInput input)
                => new() { KeyTotalSize = key.TotalSize, ValueTotalSize = input.wantInlineValue ? ValueStruct.AsSpanByteTotalSize : RecordFieldInfo.ValueObjectIdSize };
        }
    }
}
