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
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>;

    [AllureNUnit]
    [TestFixture]
    internal class ObjectLogCompactionTests : AllureTestBase
    {
        private TsavoriteKV<ClassStoreFunctions, ClassAllocator> store;
        private ClientSession<TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete, ClassStoreFunctions, ClassAllocator> session;
        private BasicContext<TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete, ClassStoreFunctions, ClassAllocator> bContext;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            DeleteDirectory(MethodTestDir, wait: true);

            var kvSettings = new KVSettings()
            {
                IndexSize = 1L << 13,
                MutableFraction = 0.1,
                MemorySize = 1L << 14,
                PageSize = 1L << 9
            };

            // For this class, compactionType is the first (and currently only) parameter. Using this to illustrate the approach; NUnit doesn't provide metadata for arguments,
            // so for multi-parameter tests it is probably better to stay with the "separate SetUp method" approach.
            Assert.That(TestContext.CurrentContext.Test.Arguments[0].GetType(), Is.EqualTo(typeof(CompactionType)));
            var compactionType = (CompactionType)TestContext.CurrentContext.Test.Arguments[0];
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, $"LogCompactBasicTest_{compactionType}.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, $"LogCompactBasicTest_{compactionType}.obj.log"), deleteOnClose: true);

            kvSettings.LogDevice = log;
            kvSettings.ObjectLogDevice = objlog;

            store = new(kvSettings
                , StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer(), DefaultRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            session = store.NewSession<TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete());
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
            objlog?.Dispose();
            objlog = null;

            DeleteDirectory(MethodTestDir);
        }

        // Basic test that where shift begin address to untilAddress after compact
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void LogCompactBasicTest([Values] CompactionType compactionType)
        {
            TestObjectInput input = new();

            const int totalRecords = 500;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == 250)
                    compactUntil = store.Log.TailAddress;

                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), value, 0);
            }

            compactUntil = session.Compact(compactUntil, compactionType);
            store.Log.Truncate();
            ClassicAssert.AreEqual(compactUntil, store.Log.BeginAddress);

            // Read all keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                TestObjectOutput output = new();

                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };

                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, 0);
                if (status.IsPending)
                {
                    _ = bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(value.value, output.value.value);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Compaction")]
        public void LogCompactTestNewEntries([Values] CompactionType compactionType)
        {
            TestObjectInput input = new();

            const int totalRecords = 2000;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == 1000)
                    compactUntil = store.Log.TailAddress;

                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), value, 0);
            }

            // Put fresh entries for 1000 records
            for (int i = 0; i < 1000; i++)
            {
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), value, 0);
            }

            store.Log.Flush(true);

            var tail = store.Log.TailAddress;
            compactUntil = session.Compact(compactUntil, compactionType);
            store.Log.Truncate();
            ClassicAssert.AreEqual(compactUntil, store.Log.BeginAddress);
            ClassicAssert.AreEqual(tail, store.Log.TailAddress);

            // Read 2000 keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                TestObjectOutput output = new();
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };

                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, 0);
                if (status.IsPending)
                {
                    _ = bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(value.value, output.value.value);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Compaction")]
        [Category("Smoke")]
        public void LogCompactAfterDeleteTest([Values] CompactionType compactionType)
        {
            TestObjectInput input = new();

            const int totalRecords = 2000;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == totalRecords / 2)
                    compactUntil = store.Log.TailAddress;

                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), value, 0);

                if (i % 8 == 0)
                {
                    int j = i / 4;
                    key1 = new TestObjectKey { key = j };
                    _ = bContext.Delete(SpanByte.FromPinnedVariable(ref key1));
                }
            }

            compactUntil = session.Compact(compactUntil, compactionType);
            store.Log.Truncate();
            ClassicAssert.AreEqual(compactUntil, store.Log.BeginAddress);

            // Read keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                TestObjectOutput output = new();
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };

                int ctx = ((i < 500) && (i % 2 == 0)) ? 1 : 0;

                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, ctx);
                if (status.IsPending)
                {
                    _ = bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }

                if (ctx == 0)
                {
                    ClassicAssert.IsTrue(status.Found);
                    ClassicAssert.AreEqual(value.value, output.value.value);
                }
                else
                    ClassicAssert.IsFalse(status.Found);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Compaction")]

        public void LogCompactBasicCustomFctnTest([Values] CompactionType compactionType)
        {
            TestObjectInput input = new();

            const int totalRecords = 2000;
            var compactUntil = 0L;

            for (var i = 0; i < totalRecords; i++)
            {
                if (i == totalRecords / 2)
                    compactUntil = store.Log.TailAddress;

                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), value, 0);
            }

            compactUntil = session.Compact(compactUntil, compactionType, default(EvenCompactionFunctions));
            store.Log.Truncate();
            ClassicAssert.AreEqual(compactUntil, store.Log.BeginAddress);

            // Read 2000 keys - all should be present
            for (var i = 0; i < totalRecords; i++)
            {
                var output = new TestObjectOutput();
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };

                var ctx = (i < (totalRecords / 2) && (i % 2 != 0)) ? 1 : 0;

                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, ctx);
                if (status.IsPending)
                {
                    _ = bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }

                if (ctx == 0)
                {
                    ClassicAssert.IsTrue(status.Found);
                    ClassicAssert.AreEqual(value.value, output.value.value);
                }
                else
                    ClassicAssert.IsFalse(status.Found);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Compaction")]

        public void LogCompactCopyInPlaceCustomFctnTest([Values] CompactionType compactionType)
        {
            // Update: irrelevant as session compaction no longer uses Copy/CopyInPlace
            // This test checks if CopyInPlace returning false triggers call to Copy

            using var session = store.NewSession<TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete());

            var key = new TestObjectKey { key = 100 };
            var value = new TestObjectValue { value = 20 };

            _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key), value, 0);

            store.Log.Flush(true);

            value = new TestObjectValue { value = 21 };
            _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key), value, 0);

            store.Log.Flush(true);

            var compactionFunctions = new Test2CompactionFunctions();
            var compactUntil = session.Compact(store.Log.TailAddress, compactionType, compactionFunctions);
            store.Log.Truncate();

            var input = default(TestObjectInput);
            var output = default(TestObjectOutput);
            var status = bContext.Read(SpanByte.FromPinnedVariable(ref key), ref input, ref output);
            if (status.IsPending)
            {
                ClassicAssert.IsTrue(bContext.CompletePendingWithOutputs(out var outputs, wait: true));
                (status, output) = GetSinglePendingResult(outputs);
            }
            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.AreEqual(value.value, output.value.value);
        }

        private class Test2CompactionFunctions : ICompactionFunctions
        {
            public bool IsDeleted<TSourceLogRecord>(in TSourceLogRecord logRecord)
                where TSourceLogRecord : ISourceLogRecord
                => false;
        }

        private struct EvenCompactionFunctions : ICompactionFunctions
        {
            public readonly bool IsDeleted<TSourceLogRecord>(in TSourceLogRecord logRecord)
                where TSourceLogRecord : ISourceLogRecord
                => ((TestObjectValue)logRecord.ValueObject).value % 2 != 0;
        }
    }
}