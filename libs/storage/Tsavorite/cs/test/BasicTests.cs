// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using LongAllocator = SpanByteAllocator<StoreFunctions<LongKeyComparer, SpanByteRecordDisposer>>;
    using LongStoreFunctions = StoreFunctions<LongKeyComparer, SpanByteRecordDisposer>;

    using StructAllocator = SpanByteAllocator<StoreFunctions<KeyStruct.Comparer, SpanByteRecordDisposer>>;
    using StructStoreFunctions = StoreFunctions<KeyStruct.Comparer, SpanByteRecordDisposer>;

    //** NOTE - more detailed / in depth Read tests in ReadAddressTests.cs 
    //** These tests ensure the basics are fully covered

    [AllureNUnit]
    [TestFixture]
    internal class BasicTests : AllureTestBase
    {
        private TsavoriteKV<StructStoreFunctions, StructAllocator> store;
        private ClientSession<InputStruct, OutputStruct, Empty, Functions, StructStoreFunctions, StructAllocator> session;
        private BasicContext<InputStruct, OutputStruct, Empty, Functions, StructStoreFunctions, StructAllocator> bContext;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            DeleteDirectory(MethodTestDir, wait: true);
        }

        private void Setup(KVSettings kvSettings, TestDeviceType deviceType, int latencyMs = DefaultLocalMemoryDeviceLatencyMs)
        {
            kvSettings.IndexSize = 1L << 13;

            string filename = Path.Join(MethodTestDir, TestContext.CurrentContext.Test.Name + deviceType.ToString() + ".log");
            log = CreateTestDevice(deviceType, filename, latencyMs: latencyMs);
            kvSettings.LogDevice = log;

            store = new(kvSettings
                , StoreFunctions.Create(KeyStruct.Comparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            session = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());
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
            DeleteDirectory(MethodTestDir);
        }

        private void AssertCompleted(Status expected, Status actual)
        {
            if (actual.IsPending)
                (actual, _) = CompletePendingResult();
            ClassicAssert.AreEqual(expected, actual);
        }

        private (Status status, OutputStruct output) CompletePendingResult()
        {
            _ = bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            return GetSinglePendingResult(completedOutputs);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void NativeInMemWriteRead([Values] TestDeviceType deviceType)
        {
            Setup(new() { PageSize = 1L << 10, MemorySize = 1L << 12, SegmentSize = 1L << 22 }, deviceType);

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), SpanByte.FromPinnedVariable(ref value), Empty.Default);
            var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);

            AssertCompleted(new(StatusCode.Found), status);
            ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
            ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void NativeInMemWriteReadDelete([Values] TestDeviceType deviceType)
        {
            Setup(new() { PageSize = 1L << 10, MemorySize = 1L << 12, SegmentSize = 1L << 22 }, deviceType);

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), SpanByte.FromPinnedVariable(ref value), Empty.Default);
            var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);
            AssertCompleted(new(StatusCode.Found), status);

            _ = bContext.Delete(SpanByte.FromPinnedVariable(ref key1), Empty.Default);

            status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);
            AssertCompleted(new(StatusCode.NotFound), status);

            var key2 = new KeyStruct { kfield1 = 14, kfield2 = 15 };
            var value2 = new ValueStruct { vfield1 = 24, vfield2 = 25 };

            _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key2), SpanByte.FromPinnedVariable(ref value2), Empty.Default);
            status = bContext.Read(SpanByte.FromPinnedVariable(ref key2), ref input, ref output, Empty.Default);

            AssertCompleted(new(StatusCode.Found), status);
            ClassicAssert.AreEqual(value2.vfield1, output.value.vfield1);
            ClassicAssert.AreEqual(value2.vfield2, output.value.vfield2);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void NativeInMemWriteReadDelete2()
        {
            // Just set this one since Write Read Delete already does all four devices
            var deviceType = TestDeviceType.MLSD;

            const int count = 10;

            Setup(new() { MemorySize = 1L << 29 }, deviceType);

            InputStruct input = default;
            OutputStruct output = default;

            for (var i = 0; i < 10 * count; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = 14 };
                var value = new ValueStruct { vfield1 = i, vfield2 = 24 };

                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), SpanByte.FromPinnedVariable(ref value), Empty.Default);
            }

            for (var i = 0; i < 10 * count; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = 14 };
                _ = bContext.Delete(SpanByte.FromPinnedVariable(ref key1), Empty.Default);
            }

            for (var i = 0; i < 10 * count; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = 14 };
                var value = new ValueStruct { vfield1 = i, vfield2 = 24 };

                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);
                AssertCompleted(new(StatusCode.NotFound), status);

                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), SpanByte.FromPinnedVariable(ref value), Empty.Default);
            }

            for (var i = 0; i < 10 * count; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = 14 };
                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);
                AssertCompleted(new(StatusCode.Found), status);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public unsafe void NativeInMemWriteRead2()
        {
            // Just use this one instead of all four devices since InMemWriteRead covers all four devices
            var deviceType = TestDeviceType.MLSD;

            const int count = 200;

            Setup(new() { MemorySize = 1L << 29 }, deviceType);
            session = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());

            InputStruct input = default;

            Random r = new(10);
            for (var c = 0; c < count; c++)
            {
                var i = r.Next(10000);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), SpanByte.FromPinnedVariable(ref value), Empty.Default);
            }

            r = new Random(10);

            for (var c = 0; c < count; c++)
            {
                var i = r.Next(10000);
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                if (bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default).IsPending)
                {
                    _ = bContext.CompletePending(true);
                }

                ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
                ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
            }

            // Clean up and retry - should not find now
            store.Log.ShiftBeginAddress(store.Log.TailAddress, truncateLog: true);

            r = new Random(10);
            for (var c = 0; c < count; c++)
            {
                var i = r.Next(10000);
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                ClassicAssert.IsFalse(bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default).Found);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void TestShiftHeadAddress([Values] TestDeviceType deviceType, [Values] BatchMode batchMode)
        {
            InputStruct input = default;
            const int RandSeed = 10;
            const int RandRange = 1000000;
            const int NumRecs = 2000;

            Random r = new(RandSeed);
            var sw = Stopwatch.StartNew();

            var latencyMs = batchMode == BatchMode.NoBatch ? 0 : DefaultLocalMemoryDeviceLatencyMs;
            Setup(new() { MemorySize = 1L << 22, SegmentSize = 1L << 22, PageSize = 1L << 10 }, deviceType, latencyMs: latencyMs);

            for (var c = 0; c < NumRecs; c++)
            {
                var i = r.Next(RandRange);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), SpanByte.FromPinnedVariable(ref value), Empty.Default);
            }

            r = new Random(RandSeed);
            sw.Restart();

            for (var c = 0; c < NumRecs; c++)
            {
                var i = r.Next(RandRange);
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                if (bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default).IsPending)
                {
                    ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
                    ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
                }
            }
            _ = bContext.CompletePending(true);

            // Shift head and retry - should not find in main memory now
            store.Log.FlushAndEvict(true);

            r = new Random(RandSeed);
            sw.Restart();

            const int batchSize = 256;
            for (var c = 0; c < NumRecs; c++)
            {
                var i = r.Next(RandRange);
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var foundStatus = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(foundStatus.IsPending);
                if (batchMode == BatchMode.NoBatch)
                {
                    Status status;
                    _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, output) = GetSinglePendingResult(outputs);
                    ClassicAssert.IsTrue(status.Found, status.ToString());
                    ClassicAssert.AreEqual(key1.kfield1, output.value.vfield1);
                    ClassicAssert.AreEqual(key1.kfield2, output.value.vfield2);
                    outputs.Dispose();
                }
                else if (c > 0 && (c % batchSize) == 0)
                {
                    _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                    var count = 0;
                    while (outputs.Next())
                    {
                        count++;
                        ClassicAssert.AreEqual(outputs.Current.Key.AsRef<KeyStruct>().kfield1, outputs.Current.Output.value.vfield1);
                        ClassicAssert.AreEqual(outputs.Current.Key.AsRef<KeyStruct>().kfield2, outputs.Current.Output.value.vfield2);
                    }
                    outputs.Dispose();
                    ClassicAssert.AreEqual(batchSize + (c == batchSize ? 1 : 0), count);
                }
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public unsafe void NativeInMemRMWRefKeys([Values] TestDeviceType deviceType)
        {
            InputStruct input = default;
            OutputStruct output = default;

            Setup(new() { MemorySize = 1L << 22, SegmentSize = 1L << 22, PageSize = 1L << 10 }, deviceType);

            var nums = Enumerable.Range(0, 1000).ToArray();
            var rnd = new Random(11);
            for (var i = 0; i < nums.Length; ++i)
            {
                var randomIndex = rnd.Next(nums.Length);
                (nums[i], nums[randomIndex]) = (nums[randomIndex], nums[i]);
            }

            for (var j = 0; j < nums.Length; ++j)
            {
                var i = nums[j];
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                input = new InputStruct { ifield1 = i, ifield2 = i + 1 };
                _ = bContext.RMW(SpanByte.FromPinnedVariable(ref key1), ref input, Empty.Default);
            }
            for (var j = 0; j < nums.Length; ++j)
            {
                var i = nums[j];
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                input = new InputStruct { ifield1 = i, ifield2 = i + 1 };
                if (bContext.RMW(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default).IsPending)
                {
                    _ = bContext.CompletePending(true);
                }
                else
                {
                    ClassicAssert.AreEqual(2 * i, output.value.vfield1);
                    ClassicAssert.AreEqual(2 * (i + 1), output.value.vfield2);
                }
            }

            Status status;
            KeyStruct key;

            for (var j = 0; j < nums.Length; ++j)
            {
                var i = nums[j];

                key = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                ValueStruct value = new() { vfield1 = i, vfield2 = i + 1 };

                status = bContext.Read(SpanByte.FromPinnedVariable(ref key), ref input, ref output, Empty.Default);

                AssertCompleted(new(StatusCode.Found), status);
                ClassicAssert.AreEqual(2 * value.vfield1, output.value.vfield1);
                ClassicAssert.AreEqual(2 * value.vfield2, output.value.vfield2);
            }

            key = new KeyStruct { kfield1 = nums.Length, kfield2 = nums.Length + 1 };
            status = bContext.Read(SpanByte.FromPinnedVariable(ref key), ref input, ref output, Empty.Default);
            AssertCompleted(new(StatusCode.NotFound), status);
        }

        // Tests the overload where no reference params used: key,input,userContext
        [Test]
        [Category("TsavoriteKV")]
        public unsafe void NativeInMemRMWNoRefKeys([Values] TestDeviceType deviceType)
        {
            InputStruct input = default;

            Setup(new() { MemorySize = 1L << 22, SegmentSize = 1L << 22, PageSize = 1L << 10 }, deviceType);

            var nums = Enumerable.Range(0, 1000).ToArray();
            var rnd = new Random(11);
            for (var i = 0; i < nums.Length; ++i)
            {
                var randomIndex = rnd.Next(nums.Length);
                (nums[i], nums[randomIndex]) = (nums[randomIndex], nums[i]);
            }

            // InitialUpdater
            for (var j = 0; j < nums.Length; ++j)
            {
                var i = nums[j];
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                input = new InputStruct { ifield1 = i, ifield2 = i + 1 };
                _ = bContext.RMW(SpanByte.FromPinnedVariable(ref key1), ref input, Empty.Default);
            }

            // CopyUpdater
            for (var j = 0; j < nums.Length; ++j)
            {
                var i = nums[j];
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                input = new InputStruct { ifield1 = i, ifield2 = i + 1 };
                _ = bContext.RMW(SpanByte.FromPinnedVariable(ref key1), ref input);
            }

            OutputStruct output = default;
            Status status;
            KeyStruct key;

            for (var j = 0; j < nums.Length; ++j)
            {
                var i = nums[j];

                key = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                ValueStruct value = new() { vfield1 = i, vfield2 = i + 1 };

                status = bContext.Read(SpanByte.FromPinnedVariable(ref key), ref input, ref output, Empty.Default);

                AssertCompleted(new(StatusCode.Found), status);
                ClassicAssert.AreEqual(2 * value.vfield1, output.value.vfield1);
                ClassicAssert.AreEqual(2 * value.vfield2, output.value.vfield2);
            }

            key = new KeyStruct { kfield1 = nums.Length, kfield2 = nums.Length + 1 };
            status = bContext.Read(SpanByte.FromPinnedVariable(ref key), ref input, ref output, Empty.Default);
            AssertCompleted(new(StatusCode.NotFound), status);
        }

        // Tests the overload of .Read(key, input, out output, context)
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ReadNoRefKeyInputOutput([Values] TestDeviceType deviceType)
        {
            InputStruct input = default;

            Setup(new() { MemorySize = 1L << 22, SegmentSize = 1L << 22, PageSize = 1L << 10 }, deviceType);

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            OutputStruct output = default;
            _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), SpanByte.FromPinnedVariable(ref value), Empty.Default);
            var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);
            AssertCompleted(new(StatusCode.Found), status);

            // Verify the read data
            ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
            ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
            ClassicAssert.AreEqual(key1.kfield1, 13);
            ClassicAssert.AreEqual(key1.kfield2, 14);
        }

        // Test the overload call of .Read (key, out output, userContext)
        [Test]
        [Category("TsavoriteKV")]
        public void ReadNoRefKey([Values] TestDeviceType deviceType)
        {
            Setup(new() { MemorySize = 1L << 22, SegmentSize = 1L << 22, PageSize = 1L << 10 }, deviceType);

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            OutputStruct output = default;
            _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), SpanByte.FromPinnedVariable(ref value), Empty.Default);
            var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref output, Empty.Default);
            AssertCompleted(new(StatusCode.Found), status);

            // Verify the read data
            ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
            ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
            ClassicAssert.AreEqual(key1.kfield1, 13);
            ClassicAssert.AreEqual(key1.kfield2, 14);
        }


        // Test the overload call of .Read (ref key, ref output, userContext)
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ReadWithoutInput([Values] TestDeviceType deviceType)
        {
            Setup(new() { MemorySize = 1L << 22, SegmentSize = 1L << 22, PageSize = 1L << 10 }, deviceType);

            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), SpanByte.FromPinnedVariable(ref value), Empty.Default);
            var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref output, Empty.Default);
            AssertCompleted(new(StatusCode.Found), status);

            // Verify the read data
            ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
            ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
            ClassicAssert.AreEqual(key1.kfield1, 13);
            ClassicAssert.AreEqual(key1.kfield2, 14);
        }

        // Test the overload call of .Read (key)
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ReadBareMinParams([Values] TestDeviceType deviceType)
        {
            Setup(new() { MemorySize = 1L << 22, SegmentSize = 1L << 22, PageSize = 1L << 10 }, deviceType);

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), SpanByte.FromPinnedVariable(ref value), Empty.Default);

            var (status, output) = bContext.Read(SpanByte.FromPinnedVariable(ref key1));
            AssertCompleted(new(StatusCode.Found), status);

            ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
            ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
            ClassicAssert.AreEqual(key1.kfield1, 13);
            ClassicAssert.AreEqual(key1.kfield2, 14);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ReadAtAddressDefaultOptions()
        {
            // Just functional test of ReadFlag so one device is enough
            var deviceType = TestDeviceType.MLSD;

            Setup(new() { MemorySize = 1L << 29 }, deviceType);

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };
            ReadOptions readOptions = default;

            _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), SpanByte.FromPinnedVariable(ref value), Empty.Default);
            var status = bContext.ReadAtAddress(store.Log.BeginAddress, ref input, ref output, ref readOptions, out _, Empty.Default);
            AssertCompleted(new(StatusCode.Found), status);

            ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
            ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
            ClassicAssert.AreEqual(key1.kfield1, 13);
            ClassicAssert.AreEqual(key1.kfield2, 14);
        }

        class SkipReadCacheFunctions : Functions
        {
            internal long expectedReadAddress;

            public override bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, ref InputStruct input, ref OutputStruct output, ref ReadInfo readInfo)
            {
                output.value = logRecord.ValueSpan.AsRef<ValueStruct>();
                ClassicAssert.AreEqual(expectedReadAddress, readInfo.Address);
                expectedReadAddress = -1;   // show that the test executed
                return true;
            }

            public override void ReadCompletionCallback(ref DiskLogRecord logRecord, ref InputStruct input, ref OutputStruct output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                // Do no data verifications here; they're done in the test
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ReadAtAddressIgnoreReadCache()
        {
            // Another ReadFlag functional test so one device is enough
            var deviceType = TestDeviceType.MLSD;

            Setup(new() { MemorySize = 1L << 29, ReadCacheEnabled = true }, deviceType);

            SkipReadCacheFunctions functions = new();
            using var skipReadCacheSession = store.NewSession<InputStruct, OutputStruct, Empty, SkipReadCacheFunctions>(functions);
            var skipReadCachebContext = skipReadCacheSession.BasicContext;

            InputStruct input = default;
            OutputStruct output = default;
            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };
            var readAtAddress = store.Log.BeginAddress;
            Status status;

            _ = skipReadCachebContext.Upsert(SpanByte.FromPinnedVariable(ref key1), SpanByte.FromPinnedVariable(ref value), Empty.Default);

            void VerifyOutput()
            {
                ClassicAssert.AreEqual(-1, functions.expectedReadAddress);     // make sure the test executed
                ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
                ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
                ClassicAssert.AreEqual(13, key1.kfield1);
                ClassicAssert.AreEqual(14, key1.kfield2);
            }

            void VerifyResult()
            {
                if (status.IsPending)
                {
                    _ = skipReadCachebContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.Found);
                VerifyOutput();
            }

            // This will just be an ordinary read, as the record is in memory.
            functions.expectedReadAddress = readAtAddress;
            status = skipReadCachebContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output);
            ClassicAssert.IsTrue(status.Found);
            VerifyOutput();

            // ReadCache is used when the record is read from disk.
            store.Log.FlushAndEvict(wait: true);

            // Do not put it into the read cache.
            functions.expectedReadAddress = readAtAddress;
            ReadOptions readOptions = new() { CopyOptions = ReadCopyOptions.None };
            status = skipReadCachebContext.ReadAtAddress(readAtAddress, SpanByte.FromPinnedVariable(ref key1), ref input, ref output, ref readOptions, out _);
            VerifyResult();

            ClassicAssert.AreEqual(store.ReadCache.BeginAddress, store.ReadCache.TailAddress);

            // Put it into the read cache.
            functions.expectedReadAddress = readAtAddress;
            readOptions.CopyOptions = new(ReadCopyFrom.AllImmutable, ReadCopyTo.ReadCache);
            status = skipReadCachebContext.ReadAtAddress(readAtAddress, SpanByte.FromPinnedVariable(ref key1), ref input, ref output, ref readOptions, out _);
            ClassicAssert.IsTrue(status.IsPending);
            VerifyResult();

            ClassicAssert.Less(store.ReadCache.BeginAddress, store.ReadCache.TailAddress);

            // Now this will read from the read cache.
            functions.expectedReadAddress = LogAddress.kInvalidAddress;
            status = skipReadCachebContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output);
            ClassicAssert.IsFalse(status.IsPending);
            ClassicAssert.IsTrue(status.Found);
            VerifyOutput();
        }

        // Simple Upsert test where ref key and ref value but nothing else set
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void UpsertDefaultsTest([Values] TestDeviceType deviceType)
        {
            Setup(new() { MemorySize = 1L << 22, SegmentSize = 1L << 22, PageSize = 1L << 10 }, deviceType);

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            ClassicAssert.AreEqual(0, store.EntryCount);

            _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key1), SpanByte.FromPinnedVariable(ref value));
            var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, Empty.Default);
            AssertCompleted(new(StatusCode.Found), status);

            ClassicAssert.AreEqual(1, store.EntryCount);
            ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
            ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
        }

        //**** Quick End to End Sample code from help docs ***
        // Very minor changes to LogDevice call and type of Asserts to use but basically code from Sample code in docs
        // Also tests the overload call of .Read (ref key ref output) 
        [Test]
        [Category("TsavoriteKV")]
        public static void KVBasicsSampleEndToEndInDocs()
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: false);

            using var store = new TsavoriteKV<LongStoreFunctions, LongAllocator>(
                new()
                {
                    IndexSize = 1L << 26,
                    LogDevice = log,
                }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var bContext = s.BasicContext;

            long keyNum = 1, valueNum = 1, input = 10, output = 0;
            ReadOnlySpan<byte> key = SpanByte.FromPinnedVariable(ref keyNum), value = SpanByte.FromPinnedVariable(ref valueNum);
            _ = bContext.Upsert(key, value);
            _ = bContext.Read(key, ref output);
            ClassicAssert.AreEqual(valueNum, output);
            _ = bContext.RMW(key, ref input);
            _ = bContext.RMW(key, ref input);
            _ = bContext.Read(key, ref output);
            ClassicAssert.AreEqual(10, output);
        }

        [Test]
        [Category("TsavoriteKV")]
        public static void LogPathtooLong()
        {
            if (!OperatingSystem.IsWindows())
                Assert.Ignore("Skipped");

            string testDir = new('x', Native32.WIN32_MAX_PATH - 11);                       // As in LSD, -11 for ".<segment>"
            using var log = Devices.CreateLogDevice(testDir, deleteOnClose: true);     // Should succeed
            _ = Assert.Throws<TsavoriteException>(() => Devices.CreateLogDevice(testDir + "y", deleteOnClose: true));
        }

        [Test]
        [Category("TsavoriteKV")]
        public static void BasicSyncOperationsTest()
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: false);

            using var store = new TsavoriteKV<LongStoreFunctions, LongAllocator>(
                new()
                {
                    IndexSize = 1L << 26,
                    LogDevice = log,
                }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var bContext = session.BasicContext;

            const int numRecords = 500;
            const int valueMult = 1_000_000;

            var hashes = new long[numRecords];
            Status status;
            long output = 0;

            for (var keyNum = 0L; keyNum < numRecords; keyNum++)
            {
                var valueNum = keyNum + valueMult;
                ReadOnlySpan<byte> key = SpanByte.FromPinnedVariable(ref keyNum), value = SpanByte.FromPinnedVariable(ref valueNum);

                hashes[keyNum] = store.storeFunctions.GetKeyHashCode64(key);
                status = bContext.Upsert(key, value);
                ClassicAssert.IsTrue(status.Record.Created, status.ToString());
                status = bContext.Read(key, ref output);
                ClassicAssert.IsTrue(status.Found, status.ToString());
                ClassicAssert.AreEqual(valueNum, output);
            }

            void doUpdate(bool useRMW)
            {
                // Update and Read without keyHash
                for (var keyNum = 0L; keyNum < numRecords; keyNum++)
                {
                    var valueNum = keyNum + valueMult * 2;
                    ReadOnlySpan<byte> key = SpanByte.FromPinnedVariable(ref keyNum), value = SpanByte.FromPinnedVariable(ref valueNum);
                    if (useRMW)
                    {
                        status = bContext.RMW(key, ref valueNum);
                        ClassicAssert.IsTrue(status.Record.InPlaceUpdated, status.ToString());
                    }
                    else
                    {
                        status = bContext.Upsert(key, value);
                        ClassicAssert.IsTrue(status.Record.InPlaceUpdated, status.ToString());
                    }
                    status = bContext.Read(key, ref output);
                    ClassicAssert.IsTrue(status.Found, status.ToString());
                    ClassicAssert.AreEqual(valueNum, output);
                }

                // Update and Read with keyHash
                for (var keyNum = 0L; keyNum < numRecords; keyNum++)
                {
                    var valueNum = keyNum + valueMult * 3;
                    ReadOnlySpan<byte> key = SpanByte.FromPinnedVariable(ref keyNum), value = SpanByte.FromPinnedVariable(ref valueNum);
                    if (useRMW)
                    {
                        RMWOptions rmwOptions = new() { KeyHash = hashes[keyNum] };
                        status = bContext.RMW(key, ref valueNum, ref rmwOptions);
                        ClassicAssert.IsTrue(status.Record.InPlaceUpdated, status.ToString());
                    }
                    else
                    {
                        UpsertOptions upsertOptions = new() { KeyHash = hashes[keyNum] };
                        status = bContext.Upsert(key, value, ref upsertOptions);
                        ClassicAssert.IsTrue(status.Record.InPlaceUpdated, status.ToString());
                    }
                    ReadOptions readOptions = new() { KeyHash = hashes[keyNum] };
                    status = bContext.Read(key, ref output, ref readOptions);
                    ClassicAssert.IsTrue(status.Found, status.ToString());
                    ClassicAssert.AreEqual(valueNum, output);
                }
            }

            doUpdate(useRMW: false);
            doUpdate(useRMW: true);

            // Delete without keyHash
            for (var keyNum = 0L; keyNum < numRecords; keyNum++)
            {
                var key = SpanByte.FromPinnedVariable(ref keyNum);
                status = bContext.Delete(key);
                ClassicAssert.IsTrue(status.Found, status.ToString());
                status = bContext.Read(key, ref output);
                ClassicAssert.IsTrue(status.NotFound, status.ToString());
            }

            // Update and Read without keyHash
            for (var keyNum = 0L; keyNum < numRecords; keyNum++)
            {
                var key = SpanByte.FromPinnedVariable(ref keyNum);
                DeleteOptions deleteOptions = new() { KeyHash = hashes[keyNum] };
                status = bContext.Delete(key, ref deleteOptions);
                ReadOptions readOptions = new() { KeyHash = hashes[keyNum] };
                status = bContext.Read(key, ref output, ref readOptions);
                ClassicAssert.IsTrue(status.NotFound, status.ToString());
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        public static void BasicOperationsTest()
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: false);

            using var store = new TsavoriteKV<LongStoreFunctions, LongAllocator>(
                new()
                {
                    IndexSize = 1L << 26,
                    LogDevice = log,
                }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var bContext = session.BasicContext;

            const int numRecords = 500;
            const int valueMult = 1_000_000;

            var hashes = new long[numRecords];
            Status status;
            long output;

            for (var keyNum = 0L; keyNum < numRecords; keyNum++)
            {
                var valueNum = keyNum + valueMult;
                ReadOnlySpan<byte> key = SpanByte.FromPinnedVariable(ref keyNum), value = SpanByte.FromPinnedVariable(ref valueNum);

                hashes[keyNum] = store.storeFunctions.GetKeyHashCode64(key);
                status = bContext.Upsert(key, value);
                ClassicAssert.IsTrue(status.Record.Created, status.ToString());
                (status, output) = bContext.Read(key);
                ClassicAssert.IsTrue(status.Found, status.ToString());
                ClassicAssert.AreEqual(valueNum, output);
            }

            void doUpdate(bool useRMW)
            {
                // Update and Read without keyHash
                for (var keyNum = 0L; keyNum < numRecords; keyNum++)
                {
                    var valueNum = keyNum + valueMult * 2;
                    ReadOnlySpan<byte> key = SpanByte.FromPinnedVariable(ref keyNum), value = SpanByte.FromPinnedVariable(ref valueNum);

                    if (useRMW)
                    {
                        status = bContext.RMW(key, ref valueNum);
                        ClassicAssert.IsTrue(status.Record.InPlaceUpdated, status.ToString());
                    }
                    else
                    {
                        status = bContext.Upsert(key, value);
                        ClassicAssert.IsTrue(status.Record.InPlaceUpdated, status.ToString());
                    }
                    (status, output) = bContext.Read(key);
                    ClassicAssert.IsTrue(status.Found, status.ToString());
                    ClassicAssert.AreEqual(valueNum, output);
                }

                // Update and Read with keyHash
                for (var keyNum = 0L; keyNum < numRecords; keyNum++)
                {
                    var valueNum = keyNum + valueMult * 3;
                    ReadOnlySpan<byte> key = SpanByte.FromPinnedVariable(ref keyNum), value = SpanByte.FromPinnedVariable(ref valueNum);

                    if (useRMW)
                    {
                        RMWOptions rmwOptions = new() { KeyHash = hashes[keyNum] };
                        status = bContext.RMW(key, ref valueNum, ref rmwOptions);
                        ClassicAssert.IsTrue(status.Record.InPlaceUpdated, status.ToString());
                    }
                    else
                    {
                        UpsertOptions upsertOptions = new() { KeyHash = hashes[keyNum] };
                        status = bContext.Upsert(key, value, ref upsertOptions);
                        ClassicAssert.IsTrue(status.Record.InPlaceUpdated, status.ToString());
                    }
                    ReadOptions readOptions = new() { KeyHash = hashes[keyNum] };
                    (status, output) = bContext.Read(key, ref readOptions);
                    ClassicAssert.IsTrue(status.Found, status.ToString());
                    ClassicAssert.AreEqual(valueNum, output);
                }
            }

            doUpdate(useRMW: false);
            doUpdate(useRMW: true);

            // Delete without keyHash
            for (var keyNum = 0L; keyNum < numRecords; keyNum++)
            {
                var key = SpanByte.FromPinnedVariable(ref keyNum);

                status = bContext.Delete(key);
                ClassicAssert.IsTrue(status.Found, status.ToString());
                (status, _) = bContext.Read(key);
                ClassicAssert.IsTrue(status.NotFound, status.ToString());
            }

            // Update and Read without keyHash
            for (var keyNum = 0L; keyNum < numRecords; keyNum++)
            {
                var key = SpanByte.FromPinnedVariable(ref keyNum);

                DeleteOptions deleteOptions = new() { KeyHash = hashes[keyNum] };
                status = bContext.Delete(key, ref deleteOptions);
                ReadOptions readOptions = new() { KeyHash = hashes[keyNum] };
                (status, _) = bContext.Read(key, ref readOptions);
                ClassicAssert.IsTrue(status.NotFound, status.ToString());
            }
        }
    }
}