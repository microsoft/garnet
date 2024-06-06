﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    [TestFixture]
    internal class GenericLogCompactionTests
    {
        private TsavoriteKV<MyKey, MyValue> store;
        private ClientSession<MyKey, MyValue, MyInput, MyOutput, int, MyFunctionsDelete> session;
        private BasicContext<MyKey, MyValue, MyInput, MyOutput, int, MyFunctionsDelete> bContext;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            DeleteDirectory(MethodTestDir, wait: true);

            if (TestContext.CurrentContext.Test.Arguments.Length == 0)
            {
                // Default log creation
                log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "GenericLogCompactionTests.log"), deleteOnClose: true);
                objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "GenericLogCompactionTests.obj.log"), deleteOnClose: true);

                store = new TsavoriteKV<MyKey, MyValue>
                    (128,
                    logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 14, PageSizeBits = 9 },
                    serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
                    );
            }
            else
            {
                // For this class, deviceType is the only parameter. Using this to illustrate the approach; NUnit doesn't provide metadata for arguments,
                // so for multi-parameter tests it is probably better to stay with the "separate SetUp method" approach.
                var deviceType = (DeviceType)TestContext.CurrentContext.Test.Arguments[0];

                log = CreateTestDevice(deviceType, Path.Join(MethodTestDir, $"LogCompactBasicTest_{deviceType}.log"));
                objlog = CreateTestDevice(deviceType, Path.Join(MethodTestDir, $"LogCompactBasicTest_{deviceType}.obj.log"));

                store = new TsavoriteKV<MyKey, MyValue>
                    (128,
                    logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 14, PageSizeBits = 9, SegmentSizeBits = 22 },
                    serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
                    );
            }
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

            DeleteDirectory(MethodTestDir);
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
                bContext.Upsert(ref key1, ref value, 0);
            }

            compactUntil = session.Compact(compactUntil, compactionType);
            store.Log.Truncate();
            Assert.AreEqual(compactUntil, store.Log.BeginAddress);

            // Read all keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                MyOutput output = new();

                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = bContext.Read(ref key1, ref input, ref output, 0);
                if (status.IsPending)
                {
                    bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    Assert.IsTrue(completedOutputs.Next());
                    Assert.IsTrue(completedOutputs.Current.Status.Found);
                    output = completedOutputs.Current.Output;
                    Assert.IsFalse(completedOutputs.Next());
                    completedOutputs.Dispose();
                }
                Assert.IsTrue(status.Found);
                Assert.AreEqual(value.value, output.value.value);
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
                bContext.Upsert(ref key1, ref value, 0);
            }

            // Put fresh entries for 1000 records
            for (int i = 0; i < 1000; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                bContext.Upsert(ref key1, ref value, 0);
            }

            store.Log.Flush(true);

            var tail = store.Log.TailAddress;
            compactUntil = session.Compact(compactUntil, compactionType);
            store.Log.Truncate();
            Assert.AreEqual(compactUntil, store.Log.BeginAddress);
            Assert.AreEqual(tail, store.Log.TailAddress);

            // Read 2000 keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                MyOutput output = new();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                var status = bContext.Read(ref key1, ref input, ref output, 0);
                if (status.IsPending)
                    bContext.CompletePending(true);
                else
                {
                    Assert.IsTrue(status.Found);
                    Assert.AreEqual(value.value, output.value.value);
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
                bContext.Upsert(ref key1, ref value, 0);

                if (i % 8 == 0)
                {
                    int j = i / 4;
                    key1 = new MyKey { key = j };
                    bContext.Delete(ref key1);
                }
            }

            compactUntil = session.Compact(compactUntil, compactionType);
            store.Log.Truncate();
            Assert.AreEqual(compactUntil, store.Log.BeginAddress);

            // Read keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                MyOutput output = new();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                int ctx = ((i < 500) && (i % 2 == 0)) ? 1 : 0;

                var status = bContext.Read(ref key1, ref input, ref output, ctx);
                if (status.IsPending)
                    bContext.CompletePending(true);
                else
                {
                    if (ctx == 0)
                    {
                        Assert.IsTrue(status.Found);
                        Assert.AreEqual(value.value, output.value.value);
                    }
                    else
                    {
                        Assert.IsFalse(status.Found);
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
                bContext.Upsert(ref key1, ref value, 0);
            }

            compactUntil = session.Compact(compactUntil, compactionType, default(EvenCompactionFunctions));
            store.Log.Truncate();
            Assert.AreEqual(compactUntil, store.Log.BeginAddress);

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
                    bContext.CompletePending(true);
                }
                else
                {
                    if (ctx == 0)
                    {
                        Assert.IsTrue(status.Found);
                        Assert.AreEqual(value.value, output.value.value);
                    }
                    else
                    {
                        Assert.IsFalse(status.Found);
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

            bContext.Upsert(ref key, ref value, 0);

            store.Log.Flush(true);

            value = new MyValue { value = 21 };
            bContext.Upsert(ref key, ref value, 0);

            store.Log.Flush(true);

            var compactionFunctions = new Test2CompactionFunctions();
            var compactUntil = session.Compact(store.Log.TailAddress, compactionType, compactionFunctions);
            store.Log.Truncate();

            var input = default(MyInput);
            var output = default(MyOutput);
            var status = bContext.Read(ref key, ref input, ref output);
            if (status.IsPending)
            {
                bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                (status, output) = GetSinglePendingResult(outputs);
            }
            Assert.IsTrue(status.Found);
            Assert.AreEqual(value.value, output.value.value);
        }

        private class Test2CompactionFunctions : ICompactionFunctions<MyKey, MyValue>
        {
            public bool IsDeleted(ref MyKey key, ref MyValue value) => false;
        }

        private struct EvenCompactionFunctions : ICompactionFunctions<MyKey, MyValue>
        {
            public bool IsDeleted(ref MyKey key, ref MyValue value) => value.value % 2 != 0;
        }

    }
}