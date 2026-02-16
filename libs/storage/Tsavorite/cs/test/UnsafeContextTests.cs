// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.UnsafeContext
{
    using StructAllocator = BlittableAllocator<KeyStruct, ValueStruct, StoreFunctions<KeyStruct, ValueStruct, KeyStruct.Comparer, DefaultRecordDisposer<KeyStruct, ValueStruct>>>;
    using StructStoreFunctions = StoreFunctions<KeyStruct, ValueStruct, KeyStruct.Comparer, DefaultRecordDisposer<KeyStruct, ValueStruct>>;

    //** These tests ensure the basics are fully covered - taken from BasicTests

    [AllureNUnit]
    [TestFixture]
    internal class BasicUnsafeContextTests : AllureTestBase
    {
        private TsavoriteKV<KeyStruct, ValueStruct, StructStoreFunctions, StructAllocator> store;
        private ClientSession<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions, StructStoreFunctions, StructAllocator> fullSession;
        private UnsafeContext<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions, StructStoreFunctions, StructAllocator> uContext;
        private IDevice log;
        TestDeviceType deviceType;

        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            DeleteDirectory(MethodTestDir, wait: true);
        }

        private void Setup(KVSettings<KeyStruct, ValueStruct> kvSettings, TestDeviceType deviceType)
        {
            string filename = Path.Join(MethodTestDir, TestContext.CurrentContext.Test.Name + deviceType.ToString() + ".log");
            log = CreateTestDevice(deviceType, filename);
            kvSettings.LogDevice = log;
            kvSettings.IndexSize = 1L << 13;

            store = new(kvSettings
                , StoreFunctions<KeyStruct, ValueStruct>.Create(KeyStruct.Comparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
            fullSession = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());
            uContext = fullSession.UnsafeContext;
        }

        [TearDown]
        public void TearDown()
        {
            uContext = default;
            fullSession?.Dispose();
            fullSession = null;
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            OnTearDown();
        }

        private void AssertCompleted(Status expected, Status actual)
        {
            if (actual.IsPending)
                (actual, _) = CompletePendingResult();
            ClassicAssert.AreEqual(expected, actual);
        }

        private (Status status, OutputStruct output) CompletePendingResult()
        {
            _ = uContext.CompletePendingWithOutputs(out var completedOutputs);
            return GetSinglePendingResult(completedOutputs);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void NativeInMemWriteRead([Values] TestDeviceType deviceType)
        {
            Setup(new() { PageSize = 1L << 10, MemorySize = 1L << 12, SegmentSize = 1L << 22 }, deviceType);
            uContext.BeginUnsafe();

            try
            {
                InputStruct input = default;
                OutputStruct output = default;

                var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
                var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

                _ = uContext.Upsert(ref key1, ref value, Empty.Default);
                var status = uContext.Read(ref key1, ref input, ref output, Empty.Default);

                AssertCompleted(new(StatusCode.Found), status);
                ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
                ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
            }
            finally
            {
                uContext.EndUnsafe();
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void NativeInMemWriteReadDelete([Values] TestDeviceType deviceType)
        {
            Setup(new() { PageSize = 1L << 10, MemorySize = 1L << 12, SegmentSize = 1L << 22 }, deviceType);
            uContext.BeginUnsafe();

            try
            {
                InputStruct input = default;
                OutputStruct output = default;

                var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
                var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

                _ = uContext.Upsert(ref key1, ref value, Empty.Default);
                var status = uContext.Read(ref key1, ref input, ref output, Empty.Default);
                AssertCompleted(new(StatusCode.Found), status);

                _ = uContext.Delete(ref key1, Empty.Default);

                status = uContext.Read(ref key1, ref input, ref output, Empty.Default);
                AssertCompleted(new(StatusCode.NotFound), status);

                var key2 = new KeyStruct { kfield1 = 14, kfield2 = 15 };
                var value2 = new ValueStruct { vfield1 = 24, vfield2 = 25 };

                _ = uContext.Upsert(ref key2, ref value2, Empty.Default);
                status = uContext.Read(ref key2, ref input, ref output, Empty.Default);

                AssertCompleted(new(StatusCode.Found), status);
                ClassicAssert.AreEqual(value2.vfield1, output.value.vfield1);
                ClassicAssert.AreEqual(value2.vfield2, output.value.vfield2);
            }
            finally
            {
                uContext.EndUnsafe();
            }
        }


        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void NativeInMemWriteReadDelete2()
        {
            // Just set this one since Write Read Delete already does all four devices
            deviceType = TestDeviceType.MLSD;

            const int count = 10;

            // Setup(new () { MemorySize = 1L << 22, SegmentSize = 1L << 22, PageSize = 1L << 10 }, deviceType);
            Setup(new() { MemorySize = 1L << 29 }, deviceType);
            uContext.BeginUnsafe();

            try
            {
                InputStruct input = default;
                OutputStruct output = default;

                for (int i = 0; i < 10 * count; i++)
                {
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = 14 };
                    var value = new ValueStruct { vfield1 = i, vfield2 = 24 };

                    _ = uContext.Upsert(ref key1, ref value, Empty.Default);
                }

                for (int i = 0; i < 10 * count; i++)
                {
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = 14 };
                    _ = uContext.Delete(ref key1, Empty.Default);
                }

                for (int i = 0; i < 10 * count; i++)
                {
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = 14 };
                    var value = new ValueStruct { vfield1 = i, vfield2 = 24 };

                    var status = uContext.Read(ref key1, ref input, ref output, Empty.Default);
                    AssertCompleted(new(StatusCode.NotFound), status);

                    _ = uContext.Upsert(ref key1, ref value, Empty.Default);
                }

                for (int i = 0; i < 10 * count; i++)
                {
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = 14 };
                    var status = uContext.Read(ref key1, ref input, ref output, Empty.Default);
                    AssertCompleted(new(StatusCode.Found), status);
                }
            }
            finally
            {
                uContext.EndUnsafe();
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public unsafe void NativeInMemWriteRead2()
        {
            // Just use this one instead of all four devices since InMemWriteRead covers all four devices
            deviceType = TestDeviceType.MLSD;

            int count = 200;

            // Setup(128, new () { MemorySize = 1L << 22, SegmentSize = 1L << 22, PageSize = 1L << 10 }, deviceType);
            Setup(new() { MemorySize = 1L << 29 }, deviceType);
            uContext.BeginUnsafe();

            try
            {
                InputStruct input = default;

                Random r = new(10);
                for (int c = 0; c < count; c++)
                {
                    var i = r.Next(10000);
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                    _ = uContext.Upsert(ref key1, ref value, Empty.Default);
                }

                r = new Random(10);

                for (int c = 0; c < count; c++)
                {
                    var i = r.Next(10000);
                    OutputStruct output = default;
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                    if (uContext.Read(ref key1, ref input, ref output, Empty.Default).IsPending)
                        _ = uContext.CompletePending(true);

                    ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
                    ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
                }

                // Clean up and retry - should not find now
                store.Log.ShiftBeginAddress(store.Log.TailAddress, truncateLog: true);

                r = new Random(10);
                for (int c = 0; c < count; c++)
                {
                    var i = r.Next(10000);
                    OutputStruct output = default;
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    ClassicAssert.IsFalse(uContext.Read(ref key1, ref input, ref output, Empty.Default).Found);
                }
            }
            finally
            {
                uContext.EndUnsafe();
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public async Task TestShiftHeadAddressUC([Values] TestDeviceType deviceType, [Values] CompletionSyncMode syncMode)
        {
            InputStruct input = default;
            const int RandSeed = 10;
            const int RandRange = 10000;
            const int NumRecs = 200;

            Random r = new(RandSeed);
            var sw = Stopwatch.StartNew();

            Setup(new() { MemorySize = 1L << 22, SegmentSize = 1L << 22, PageSize = 1L << 10 }, deviceType);
            uContext.BeginUnsafe();

            try
            {
                for (int c = 0; c < NumRecs; c++)
                {
                    var i = r.Next(RandRange);
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                    _ = uContext.Upsert(ref key1, ref value, Empty.Default);
                }

                r = new Random(RandSeed);
                sw.Restart();

                for (int c = 0; c < NumRecs; c++)
                {
                    var i = r.Next(RandRange);
                    OutputStruct output = default;
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                    Status status = uContext.Read(ref key1, ref input, ref output, Empty.Default);
                    if (!status.IsPending)
                    {
                        ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
                        ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
                    }
                }
                if (syncMode == CompletionSyncMode.Sync)
                {
                    _ = uContext.CompletePending(true);
                }
                else
                {
                    uContext.EndUnsafe();
                    await uContext.CompletePendingAsync();
                    uContext.BeginUnsafe();
                }

                // Shift head and retry - should not find in main memory now
                store.Log.FlushAndEvict(true);

                r = new Random(RandSeed);
                sw.Restart();

                for (int c = 0; c < NumRecs; c++)
                {
                    var i = r.Next(RandRange);
                    OutputStruct output = default;
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    Status foundStatus = uContext.Read(ref key1, ref input, ref output, Empty.Default);
                    ClassicAssert.IsTrue(foundStatus.IsPending);
                }

                CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty> outputs;
                if (syncMode == CompletionSyncMode.Sync)
                {
                    _ = uContext.CompletePendingWithOutputs(out outputs, wait: true);
                }
                else
                {
                    uContext.EndUnsafe();
                    outputs = await uContext.CompletePendingWithOutputsAsync();
                    uContext.BeginUnsafe();
                }

                int count = 0;
                while (outputs.Next())
                {
                    count++;
                    ClassicAssert.AreEqual(outputs.Current.Key.kfield1, outputs.Current.Output.value.vfield1);
                    ClassicAssert.AreEqual(outputs.Current.Key.kfield2, outputs.Current.Output.value.vfield2);
                }
                outputs.Dispose();
                ClassicAssert.AreEqual(NumRecs, count);
            }
            finally
            {
                uContext.EndUnsafe();
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
            uContext.BeginUnsafe();

            try
            {
                var nums = Enumerable.Range(0, 1000).ToArray();
                var rnd = new Random(11);
                for (int i = 0; i < nums.Length; ++i)
                {
                    int randomIndex = rnd.Next(nums.Length);
                    int temp = nums[randomIndex];
                    nums[randomIndex] = nums[i];
                    nums[i] = temp;
                }

                for (int j = 0; j < nums.Length; ++j)
                {
                    var i = nums[j];
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    input = new InputStruct { ifield1 = i, ifield2 = i + 1 };
                    _ = uContext.RMW(ref key1, ref input, Empty.Default);
                }
                for (int j = 0; j < nums.Length; ++j)
                {
                    var i = nums[j];
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    input = new InputStruct { ifield1 = i, ifield2 = i + 1 };
                    if (uContext.RMW(ref key1, ref input, ref output, Empty.Default).IsPending)
                    {
                        _ = uContext.CompletePending(true);
                    }
                    else
                    {
                        ClassicAssert.AreEqual(2 * i, output.value.vfield1);
                        ClassicAssert.AreEqual(2 * (i + 1), output.value.vfield2);
                    }
                }

                Status status;
                KeyStruct key;

                for (int j = 0; j < nums.Length; ++j)
                {
                    var i = nums[j];

                    key = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    ValueStruct value = new() { vfield1 = i, vfield2 = i + 1 };

                    status = uContext.Read(ref key, ref input, ref output, Empty.Default);

                    AssertCompleted(new(StatusCode.Found), status);
                    ClassicAssert.AreEqual(2 * value.vfield1, output.value.vfield1);
                    ClassicAssert.AreEqual(2 * value.vfield2, output.value.vfield2);
                }

                key = new KeyStruct { kfield1 = nums.Length, kfield2 = nums.Length + 1 };
                status = uContext.Read(ref key, ref input, ref output, Empty.Default);
                AssertCompleted(new(StatusCode.NotFound), status);
            }
            finally
            {
                uContext.EndUnsafe();
            }
        }

        // Tests the overload where no reference params used: key,input,userContext
        [Test]
        [Category("TsavoriteKV")]
        public unsafe void NativeInMemRMWNoRefKeys([Values] TestDeviceType deviceType)
        {
            InputStruct input = default;

            Setup(new() { MemorySize = 1L << 22, SegmentSize = 1L << 22, PageSize = 1L << 10 }, deviceType);
            uContext.BeginUnsafe();

            try
            {
                var nums = Enumerable.Range(0, 1000).ToArray();
                var rnd = new Random(11);
                for (int i = 0; i < nums.Length; ++i)
                {
                    int randomIndex = rnd.Next(nums.Length);
                    int temp = nums[randomIndex];
                    nums[randomIndex] = nums[i];
                    nums[i] = temp;
                }

                for (int j = 0; j < nums.Length; ++j)
                {
                    var i = nums[j];
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    input = new InputStruct { ifield1 = i, ifield2 = i + 1 };
                    _ = uContext.RMW(ref key1, ref input, Empty.Default);
                }
                for (int j = 0; j < nums.Length; ++j)
                {
                    var i = nums[j];
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    input = new InputStruct { ifield1 = i, ifield2 = i + 1 };
                    _ = uContext.RMW(key1, input);  // no ref and do not set any other params
                }

                OutputStruct output = default;
                Status status;
                KeyStruct key;

                for (int j = 0; j < nums.Length; ++j)
                {
                    var i = nums[j];

                    key = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    ValueStruct value = new() { vfield1 = i, vfield2 = i + 1 };

                    status = uContext.Read(ref key, ref input, ref output, Empty.Default);

                    AssertCompleted(new(StatusCode.Found), status);
                    ClassicAssert.AreEqual(2 * value.vfield1, output.value.vfield1);
                    ClassicAssert.AreEqual(2 * value.vfield2, output.value.vfield2);
                }

                key = new KeyStruct { kfield1 = nums.Length, kfield2 = nums.Length + 1 };
                status = uContext.Read(ref key, ref input, ref output, Empty.Default);
                AssertCompleted(new(StatusCode.NotFound), status);
            }
            finally
            {
                uContext.EndUnsafe();
            }
        }

        // Tests the overload of .Read(key, input, out output,  context)
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ReadNoRefKeyInputOutput([Values] TestDeviceType deviceType)
        {
            InputStruct input = default;

            Setup(new() { MemorySize = 1L << 22, SegmentSize = 1L << 22, PageSize = 1L << 10 }, deviceType);
            uContext.BeginUnsafe();

            try
            {
                var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
                var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

                _ = uContext.Upsert(ref key1, ref value, Empty.Default);
                var status = uContext.Read(key1, input, out OutputStruct output, Empty.Default);
                AssertCompleted(new(StatusCode.Found), status);

                // Verify the read data
                ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
                ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
                ClassicAssert.AreEqual(key1.kfield1, 13);
                ClassicAssert.AreEqual(key1.kfield2, 14);
            }
            finally
            {
                uContext.EndUnsafe();
            }
        }

        // Test the overload call of .Read (key, out output, userContext)
        [Test]
        [Category("TsavoriteKV")]
        public void ReadNoRefKey([Values] TestDeviceType deviceType)
        {
            Setup(new() { MemorySize = 1L << 22, SegmentSize = 1L << 22, PageSize = 1L << 10 }, deviceType);
            uContext.BeginUnsafe();

            try
            {
                var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
                var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

                _ = uContext.Upsert(ref key1, ref value, Empty.Default);
                var status = uContext.Read(key1, out OutputStruct output, Empty.Default);
                AssertCompleted(new(StatusCode.Found), status);

                // Verify the read data
                ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
                ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
                ClassicAssert.AreEqual(key1.kfield1, 13);
                ClassicAssert.AreEqual(key1.kfield2, 14);
            }
            finally
            {
                uContext.EndUnsafe();
            }
        }


        // Test the overload call of .Read (ref key, ref output, userContext)
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ReadWithoutInput([Values] TestDeviceType deviceType)
        {
            Setup(new() { MemorySize = 1L << 22, SegmentSize = 1L << 22, PageSize = 1L << 10 }, deviceType);
            uContext.BeginUnsafe();

            try
            {
                OutputStruct output = default;

                var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
                var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

                _ = uContext.Upsert(ref key1, ref value, Empty.Default);
                var status = uContext.Read(ref key1, ref output, Empty.Default);
                AssertCompleted(new(StatusCode.Found), status);

                // Verify the read data
                ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
                ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
                ClassicAssert.AreEqual(key1.kfield1, 13);
                ClassicAssert.AreEqual(key1.kfield2, 14);
            }
            finally
            {
                uContext.EndUnsafe();
            }
        }

        // Test the overload call of .Read (key)
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ReadBareMinParams([Values] TestDeviceType deviceType)
        {
            Setup(new() { MemorySize = 1L << 22, SegmentSize = 1L << 22, PageSize = 1L << 10 }, deviceType);
            uContext.BeginUnsafe();

            try
            {
                var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
                var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

                _ = uContext.Upsert(ref key1, ref value, Empty.Default);

                var (status, output) = uContext.Read(key1);
                AssertCompleted(new(StatusCode.Found), status);

                ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
                ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
                ClassicAssert.AreEqual(key1.kfield1, 13);
                ClassicAssert.AreEqual(key1.kfield2, 14);
            }
            finally
            {
                uContext.EndUnsafe();
            }
        }
    }
}