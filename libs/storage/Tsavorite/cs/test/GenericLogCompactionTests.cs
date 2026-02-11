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
    using ClassAllocator = GenericAllocator<MyKey, MyValue, StoreFunctions<MyKey, MyValue, MyKey.Comparer, DefaultRecordDisposer<MyKey, MyValue>>>;
    using ClassStoreFunctions = StoreFunctions<MyKey, MyValue, MyKey.Comparer, DefaultRecordDisposer<MyKey, MyValue>>;

    [AllureNUnit]
    [TestFixture]
    internal class GenericLogCompactionTests : AllureTestBase
    {
        private TsavoriteKV<MyKey, MyValue, ClassStoreFunctions, ClassAllocator> store;
        private ClientSession<MyKey, MyValue, MyInput, MyOutput, int, MyFunctionsDelete, ClassStoreFunctions, ClassAllocator> session;
        private BasicContext<MyKey, MyValue, MyInput, MyOutput, int, MyFunctionsDelete, ClassStoreFunctions, ClassAllocator> bContext;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            DeleteDirectory(MethodTestDir, wait: true);

            var kvSettings = new KVSettings<MyKey, MyValue>()
            {
                IndexSize = 1L << 13,
                MutableFraction = 0.1,
                MemorySize = 1L << 14,
                PageSize = 1L << 9
            };

            if (TestContext.CurrentContext.Test.Arguments.Length == 0)
            {
                // Default log creation
                log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "GenericLogCompactionTests.log"), deleteOnClose: true);
                objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "GenericLogCompactionTests.obj.log"), deleteOnClose: true);
            }
            else
            {
                // For this class, deviceType is the only parameter. Using this to illustrate the approach; NUnit doesn't provide metadata for arguments,
                // so for multi-parameter tests it is probably better to stay with the "separate SetUp method" approach.
                var deviceType = (TestDeviceType)TestContext.CurrentContext.Test.Arguments[0];

                log = CreateTestDevice(deviceType, Path.Join(MethodTestDir, $"LogCompactBasicTest_{deviceType}.log"));
                objlog = CreateTestDevice(deviceType, Path.Join(MethodTestDir, $"LogCompactBasicTest_{deviceType}.obj.log"));

                kvSettings.SegmentSize = 1L << 22;
            }

            kvSettings.LogDevice = log;
            kvSettings.ObjectLogDevice = objlog;

            store = new(kvSettings
                , StoreFunctions<MyKey, MyValue>.Create(new MyKey.Comparer(), () => new MyKeySerializer(), () => new MyValueSerializer(), DefaultRecordDisposer<MyKey, MyValue>.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            session = store.NewSession<MyInput, MyOutput, int, MyFunctionsDelete>(new MyFunctionsDelete());
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

            OnTearDown();
        }

        // Basic test that where shift begin address to untilAddress after compact
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void LogCompactBasicTest([Values] CompactionType compactionType)
        {
            MyInput input = new();

            const int totalRecords = 500;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == 250)
                    compactUntil = store.Log.TailAddress;

                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                _ = bContext.Upsert(ref key1, ref value, 0);
            }

            compactUntil = session.Compact(compactUntil, compactionType);
            store.Log.Truncate();
            ClassicAssert.AreEqual(compactUntil, store.Log.BeginAddress);

            // Read all keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                MyOutput output = new();

                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = bContext.Read(ref key1, ref input, ref output, 0);
                if (status.IsPending)
                {
                    _ = bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    ClassicAssert.IsTrue(completedOutputs.Next());
                    ClassicAssert.IsTrue(completedOutputs.Current.Status.Found);
                    output = completedOutputs.Current.Output;
                    ClassicAssert.IsFalse(completedOutputs.Next());
                    completedOutputs.Dispose();
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
            MyInput input = new();

            const int totalRecords = 2000;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == 1000)
                    compactUntil = store.Log.TailAddress;

                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                _ = bContext.Upsert(ref key1, ref value, 0);
            }

            // Put fresh entries for 1000 records
            for (int i = 0; i < 1000; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                _ = bContext.Upsert(ref key1, ref value, 0);
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
                MyOutput output = new();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = bContext.Read(ref key1, ref input, ref output, 0);
                if (status.IsPending)
                    _ = bContext.CompletePending(true);
                else
                {
                    ClassicAssert.IsTrue(status.Found);
                    ClassicAssert.AreEqual(value.value, output.value.value);
                }
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Compaction")]
        [Category("Smoke")]
        public void LogCompactAfterDeleteTest([Values] CompactionType compactionType)
        {
            MyInput input = new();

            const int totalRecords = 2000;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == totalRecords / 2)
                    compactUntil = store.Log.TailAddress;

                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                _ = bContext.Upsert(ref key1, ref value, 0);

                if (i % 8 == 0)
                {
                    int j = i / 4;
                    key1 = new MyKey { key = j };
                    _ = bContext.Delete(ref key1);
                }
            }

            compactUntil = session.Compact(compactUntil, compactionType);
            store.Log.Truncate();
            ClassicAssert.AreEqual(compactUntil, store.Log.BeginAddress);

            // Read keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                MyOutput output = new();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                int ctx = ((i < 500) && (i % 2 == 0)) ? 1 : 0;

                var status = bContext.Read(ref key1, ref input, ref output, ctx);
                if (status.IsPending)
                    _ = bContext.CompletePending(true);
                else
                {
                    if (ctx == 0)
                    {
                        ClassicAssert.IsTrue(status.Found);
                        ClassicAssert.AreEqual(value.value, output.value.value);
                    }
                    else
                    {
                        ClassicAssert.IsFalse(status.Found);
                    }
                }
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Compaction")]

        public void LogCompactBasicCustomFctnTest([Values] CompactionType compactionType)
        {
            MyInput input = new();

            const int totalRecords = 2000;
            var compactUntil = 0L;

            for (var i = 0; i < totalRecords; i++)
            {
                if (i == totalRecords / 2)
                    compactUntil = store.Log.TailAddress;

                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                _ = bContext.Upsert(ref key1, ref value, 0);
            }

            compactUntil = session.Compact(compactUntil, compactionType, default(EvenCompactionFunctions));
            store.Log.Truncate();
            ClassicAssert.AreEqual(compactUntil, store.Log.BeginAddress);

            // Read 2000 keys - all should be present
            for (var i = 0; i < totalRecords; i++)
            {
                var output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var ctx = (i < (totalRecords / 2) && (i % 2 != 0)) ? 1 : 0;

                var status = bContext.Read(ref key1, ref input, ref output, ctx);
                if (status.IsPending)
                {
                    _ = bContext.CompletePending(true);
                }
                else
                {
                    if (ctx == 0)
                    {
                        ClassicAssert.IsTrue(status.Found);
                        ClassicAssert.AreEqual(value.value, output.value.value);
                    }
                    else
                    {
                        ClassicAssert.IsFalse(status.Found);
                    }
                }
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Compaction")]

        public void LogCompactCopyInPlaceCustomFctnTest([Values] CompactionType compactionType)
        {
            // Update: irrelevant as session compaction no longer uses Copy/CopyInPlace
            // This test checks if CopyInPlace returning false triggers call to Copy

            using var session = store.NewSession<MyInput, MyOutput, int, MyFunctionsDelete>(new MyFunctionsDelete());

            var key = new MyKey { key = 100 };
            var value = new MyValue { value = 20 };

            _ = bContext.Upsert(ref key, ref value, 0);

            store.Log.Flush(true);

            value = new MyValue { value = 21 };
            _ = bContext.Upsert(ref key, ref value, 0);

            store.Log.Flush(true);

            var compactionFunctions = new Test2CompactionFunctions();
            var compactUntil = session.Compact(store.Log.TailAddress, compactionType, compactionFunctions);
            store.Log.Truncate();

            var input = default(MyInput);
            var output = default(MyOutput);
            var status = bContext.Read(ref key, ref input, ref output);
            if (status.IsPending)
            {
                ClassicAssert.IsTrue(bContext.CompletePendingWithOutputs(out var outputs, wait: true));
                (status, output) = GetSinglePendingResult(outputs);
            }
            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.AreEqual(value.value, output.value.value);
        }

        private class Test2CompactionFunctions : ICompactionFunctions<MyKey, MyValue>
        {
            public bool IsDeleted(ref MyKey key, ref MyValue value) => false;
        }

        private struct EvenCompactionFunctions : ICompactionFunctions<MyKey, MyValue>
        {
            public readonly bool IsDeleted(ref MyKey key, ref MyValue value) => value.value % 2 != 0;
        }
    }
}