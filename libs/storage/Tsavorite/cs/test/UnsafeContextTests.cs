// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.UnsafeContext
{
    //** These tests ensure the basics are fully covered - taken from BasicTests

    [TestFixture]
    internal class BasicUnsafeContextTests
    {
        private TsavoriteKV<KeyStruct, ValueStruct> store;
        private ClientSession<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions> fullSession;
        private UnsafeContext<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions> uContext;
        private IDevice log;
        private string path;
        DeviceType deviceType;

        [SetUp]
        public void Setup()
        {
            path = MethodTestDir + "/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            DeleteDirectory(path, wait: true);
        }

        private void Setup(long size, LogSettings logSettings, DeviceType deviceType)
        {
            string filename = path + TestContext.CurrentContext.Test.Name + deviceType.ToString() + ".log";
            log = CreateTestDevice(deviceType, filename);
            logSettings.LogDevice = log;
            store = new TsavoriteKV<KeyStruct, ValueStruct>(size, logSettings);
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
            DeleteDirectory(path);
        }

        private void AssertCompleted(Status expected, Status actual)
        {
            if (actual.IsPending)
                (actual, _) = CompletePendingResult();
            Assert.AreEqual(expected, actual);
        }

        private (Status status, OutputStruct output) CompletePendingResult()
        {
            uContext.CompletePendingWithOutputs(out var completedOutputs);
            return GetSinglePendingResult(completedOutputs);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void NativeInMemWriteRead([Values] DeviceType deviceType)
        {
            Setup(128, new LogSettings { PageSizeBits = 10, MemorySizeBits = 12, SegmentSizeBits = 22 }, deviceType);
            uContext.BeginUnsafe();

            try
            {
                InputStruct input = default;
                OutputStruct output = default;

                var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
                var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

                uContext.Upsert(ref key1, ref value, Empty.Default, 0);
                var status = uContext.Read(ref key1, ref input, ref output, Empty.Default, 0);

                AssertCompleted(new(StatusCode.Found), status);
                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
            }
            finally
            {
                uContext.EndUnsafe();
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void NativeInMemWriteReadDelete([Values] DeviceType deviceType)
        {
            Setup(128, new LogSettings { PageSizeBits = 10, MemorySizeBits = 12, SegmentSizeBits = 22 }, deviceType);
            uContext.BeginUnsafe();

            try
            {
                InputStruct input = default;
                OutputStruct output = default;

                var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
                var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

                uContext.Upsert(ref key1, ref value, Empty.Default, 0);
                var status = uContext.Read(ref key1, ref input, ref output, Empty.Default, 0);
                AssertCompleted(new(StatusCode.Found), status);

                uContext.Delete(ref key1, Empty.Default, 0);

                status = uContext.Read(ref key1, ref input, ref output, Empty.Default, 0);
                AssertCompleted(new(StatusCode.NotFound), status);

                var key2 = new KeyStruct { kfield1 = 14, kfield2 = 15 };
                var value2 = new ValueStruct { vfield1 = 24, vfield2 = 25 };

                uContext.Upsert(ref key2, ref value2, Empty.Default, 0);
                status = uContext.Read(ref key2, ref input, ref output, Empty.Default, 0);

                AssertCompleted(new(StatusCode.Found), status);
                Assert.AreEqual(value2.vfield1, output.value.vfield1);
                Assert.AreEqual(value2.vfield2, output.value.vfield2);
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
            deviceType = DeviceType.MLSD;

            const int count = 10;

            // Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);
            Setup(128, new LogSettings { MemorySizeBits = 29 }, deviceType);
            uContext.BeginUnsafe();

            try
            {
                InputStruct input = default;
                OutputStruct output = default;

                for (int i = 0; i < 10 * count; i++)
                {
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = 14 };
                    var value = new ValueStruct { vfield1 = i, vfield2 = 24 };

                    uContext.Upsert(ref key1, ref value, Empty.Default, 0);
                }

                for (int i = 0; i < 10 * count; i++)
                {
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = 14 };
                    uContext.Delete(ref key1, Empty.Default, 0);
                }

                for (int i = 0; i < 10 * count; i++)
                {
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = 14 };
                    var value = new ValueStruct { vfield1 = i, vfield2 = 24 };

                    var status = uContext.Read(ref key1, ref input, ref output, Empty.Default, 0);
                    AssertCompleted(new(StatusCode.NotFound), status);

                    uContext.Upsert(ref key1, ref value, Empty.Default, 0);
                }

                for (int i = 0; i < 10 * count; i++)
                {
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = 14 };
                    var status = uContext.Read(ref key1, ref input, ref output, Empty.Default, 0);
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
            deviceType = DeviceType.MLSD;

            int count = 200;

            // Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);
            Setup(128, new LogSettings { MemorySizeBits = 29 }, deviceType);
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
                    uContext.Upsert(ref key1, ref value, Empty.Default, 0);
                }

                r = new Random(10);

                for (int c = 0; c < count; c++)
                {
                    var i = r.Next(10000);
                    OutputStruct output = default;
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                    if (uContext.Read(ref key1, ref input, ref output, Empty.Default, 0).IsPending)
                    {
                        uContext.CompletePending(true);
                    }

                    Assert.AreEqual(value.vfield1, output.value.vfield1);
                    Assert.AreEqual(value.vfield2, output.value.vfield2);
                }

                // Clean up and retry - should not find now
                store.Log.ShiftBeginAddress(store.Log.TailAddress, truncateLog: true);

                r = new Random(10);
                for (int c = 0; c < count; c++)
                {
                    var i = r.Next(10000);
                    OutputStruct output = default;
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    Assert.IsFalse(uContext.Read(ref key1, ref input, ref output, Empty.Default, 0).Found);
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
        public async Task TestShiftHeadAddressUC([Values] DeviceType deviceType, [Values] SyncMode syncMode)
        {
            InputStruct input = default;
            const int RandSeed = 10;
            const int RandRange = 10000;
            const int NumRecs = 200;

            Random r = new(RandSeed);
            var sw = Stopwatch.StartNew();

            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);
            uContext.BeginUnsafe();

            try
            {
                for (int c = 0; c < NumRecs; c++)
                {
                    var i = r.Next(RandRange);
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                    if (syncMode == SyncMode.Sync)
                    {
                        uContext.Upsert(ref key1, ref value, Empty.Default, 0);
                    }
                    else
                    {
                        uContext.EndUnsafe();
                        var status = (await uContext.UpsertAsync(ref key1, ref value)).Complete();
                        uContext.BeginUnsafe();
                        Assert.IsFalse(status.IsPending);
                    }
                }

                r = new Random(RandSeed);
                sw.Restart();

                for (int c = 0; c < NumRecs; c++)
                {
                    var i = r.Next(RandRange);
                    OutputStruct output = default;
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                    Status status;
                    if (syncMode == SyncMode.Sync || (c % 1 == 0))  // in .Async mode, half the ops should be sync to test CompletePendingAsync
                    {
                        status = uContext.Read(ref key1, ref input, ref output, Empty.Default, 0);
                    }
                    else
                    {
                        uContext.EndUnsafe();
                        (status, output) = (await uContext.ReadAsync(ref key1, ref input)).Complete();
                        uContext.BeginUnsafe();
                    }
                    if (!status.IsPending)
                    {
                        Assert.AreEqual(value.vfield1, output.value.vfield1);
                        Assert.AreEqual(value.vfield2, output.value.vfield2);
                    }
                }
                if (syncMode == SyncMode.Sync)
                {
                    uContext.CompletePending(true);
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
                    Status foundStatus = uContext.Read(ref key1, ref input, ref output, Empty.Default, 0);
                    Assert.IsTrue(foundStatus.IsPending);
                }

                CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty> outputs;
                if (syncMode == SyncMode.Sync)
                {
                    uContext.CompletePendingWithOutputs(out outputs, wait: true);
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
                    Assert.AreEqual(outputs.Current.Key.kfield1, outputs.Current.Output.value.vfield1);
                    Assert.AreEqual(outputs.Current.Key.kfield2, outputs.Current.Output.value.vfield2);
                }
                outputs.Dispose();
                Assert.AreEqual(NumRecs, count);
            }
            finally
            {
                uContext.EndUnsafe();
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public unsafe void NativeInMemRMWRefKeys([Values] DeviceType deviceType)
        {
            InputStruct input = default;
            OutputStruct output = default;

            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);
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
                    uContext.RMW(ref key1, ref input, Empty.Default, 0);
                }
                for (int j = 0; j < nums.Length; ++j)
                {
                    var i = nums[j];
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    input = new InputStruct { ifield1 = i, ifield2 = i + 1 };
                    if (uContext.RMW(ref key1, ref input, ref output, Empty.Default, 0).IsPending)
                    {
                        uContext.CompletePending(true);
                    }
                    else
                    {
                        Assert.AreEqual(2 * i, output.value.vfield1);
                        Assert.AreEqual(2 * (i + 1), output.value.vfield2);
                    }
                }

                Status status;
                KeyStruct key;

                for (int j = 0; j < nums.Length; ++j)
                {
                    var i = nums[j];

                    key = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    ValueStruct value = new() { vfield1 = i, vfield2 = i + 1 };

                    status = uContext.Read(ref key, ref input, ref output, Empty.Default, 0);

                    AssertCompleted(new(StatusCode.Found), status);
                    Assert.AreEqual(2 * value.vfield1, output.value.vfield1);
                    Assert.AreEqual(2 * value.vfield2, output.value.vfield2);
                }

                key = new KeyStruct { kfield1 = nums.Length, kfield2 = nums.Length + 1 };
                status = uContext.Read(ref key, ref input, ref output, Empty.Default, 0);
                AssertCompleted(new(StatusCode.NotFound), status);
            }
            finally
            {
                uContext.EndUnsafe();
            }
        }

        // Tests the overload where no reference params used: key,input,userContext,serialNo
        [Test]
        [Category("TsavoriteKV")]
        public unsafe void NativeInMemRMWNoRefKeys([Values] DeviceType deviceType)
        {
            InputStruct input = default;

            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);
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
                    uContext.RMW(ref key1, ref input, Empty.Default, 0);
                }
                for (int j = 0; j < nums.Length; ++j)
                {
                    var i = nums[j];
                    var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    input = new InputStruct { ifield1 = i, ifield2 = i + 1 };
                    uContext.RMW(key1, input);  // no ref and do not set any other params
                }

                OutputStruct output = default;
                Status status;
                KeyStruct key;

                for (int j = 0; j < nums.Length; ++j)
                {
                    var i = nums[j];

                    key = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                    ValueStruct value = new() { vfield1 = i, vfield2 = i + 1 };

                    status = uContext.Read(ref key, ref input, ref output, Empty.Default, 0);

                    AssertCompleted(new(StatusCode.Found), status);
                    Assert.AreEqual(2 * value.vfield1, output.value.vfield1);
                    Assert.AreEqual(2 * value.vfield2, output.value.vfield2);
                }

                key = new KeyStruct { kfield1 = nums.Length, kfield2 = nums.Length + 1 };
                status = uContext.Read(ref key, ref input, ref output, Empty.Default, 0);
                AssertCompleted(new(StatusCode.NotFound), status);
            }
            finally
            {
                uContext.EndUnsafe();
            }
        }

        // Tests the overload of .Read(key, input, out output,  context, serialNo)
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ReadNoRefKeyInputOutput([Values] DeviceType deviceType)
        {
            InputStruct input = default;

            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);
            uContext.BeginUnsafe();

            try
            {
                var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
                var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

                uContext.Upsert(ref key1, ref value, Empty.Default, 0);
                var status = uContext.Read(key1, input, out OutputStruct output, Empty.Default, 111);
                AssertCompleted(new(StatusCode.Found), status);

                // Verify the read data
                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
                Assert.AreEqual(key1.kfield1, 13);
                Assert.AreEqual(key1.kfield2, 14);
            }
            finally
            {
                uContext.EndUnsafe();
            }
        }

        // Test the overload call of .Read (key, out output, userContext, serialNo)
        [Test]
        [Category("TsavoriteKV")]
        public void ReadNoRefKey([Values] DeviceType deviceType)
        {
            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);
            uContext.BeginUnsafe();

            try
            {
                var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
                var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

                uContext.Upsert(ref key1, ref value, Empty.Default, 0);
                var status = uContext.Read(key1, out OutputStruct output, Empty.Default, 1);
                AssertCompleted(new(StatusCode.Found), status);

                // Verify the read data
                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
                Assert.AreEqual(key1.kfield1, 13);
                Assert.AreEqual(key1.kfield2, 14);
            }
            finally
            {
                uContext.EndUnsafe();
            }
        }


        // Test the overload call of .Read (ref key, ref output, userContext, serialNo)
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ReadWithoutInput([Values] DeviceType deviceType)
        {
            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);
            uContext.BeginUnsafe();

            try
            {
                OutputStruct output = default;

                var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
                var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

                uContext.Upsert(ref key1, ref value, Empty.Default, 0);
                var status = uContext.Read(ref key1, ref output, Empty.Default, 99);
                AssertCompleted(new(StatusCode.Found), status);

                // Verify the read data
                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
                Assert.AreEqual(key1.kfield1, 13);
                Assert.AreEqual(key1.kfield2, 14);
            }
            finally
            {
                uContext.EndUnsafe();
            }
        }

        // Test the overload call of .Read (ref key, ref input, ref output, ref recordInfo, userContext: context)
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ReadWithoutSerialID()
        {
            // Just checking without Serial ID so one device type is enough
            deviceType = DeviceType.MLSD;

            Setup(128, new LogSettings { MemorySizeBits = 29 }, deviceType);
            uContext.BeginUnsafe();

            try
            {
                InputStruct input = default;
                OutputStruct output = default;

                var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
                var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

                uContext.Upsert(ref key1, ref value, Empty.Default, 0);
                var status = uContext.Read(ref key1, ref input, ref output, Empty.Default);
                AssertCompleted(new(StatusCode.Found), status);

                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
                Assert.AreEqual(key1.kfield1, 13);
                Assert.AreEqual(key1.kfield2, 14);
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
        public void ReadBareMinParams([Values] DeviceType deviceType)
        {
            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);
            uContext.BeginUnsafe();

            try
            {
                var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
                var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

                uContext.Upsert(ref key1, ref value, Empty.Default, 0);

                var (status, output) = uContext.Read(key1);
                AssertCompleted(new(StatusCode.Found), status);

                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
                Assert.AreEqual(key1.kfield1, 13);
                Assert.AreEqual(key1.kfield2, 14);
            }
            finally
            {
                uContext.EndUnsafe();
            }
        }
    }
}