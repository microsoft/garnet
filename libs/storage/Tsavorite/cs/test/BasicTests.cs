// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    //** NOTE - more detailed / in depth Read tests in ReadAddressTests.cs 
    //** These tests ensure the basics are fully covered

    [TestFixture]
    internal class BasicTests
    {
        private TsavoriteKV<KeyStruct, ValueStruct> store;
        private ClientSession<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions> session;
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

        private void Setup(long size, LogSettings logSettings, DeviceType deviceType, int latencyMs = DefaultLocalMemoryDeviceLatencyMs)
        {
            string filename = path + TestContext.CurrentContext.Test.Name + deviceType.ToString() + ".log";
            log = CreateTestDevice(deviceType, filename, latencyMs: latencyMs);
            logSettings.LogDevice = log;
            store = new TsavoriteKV<KeyStruct, ValueStruct>(size, logSettings);
            session = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());
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
            session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            return GetSinglePendingResult(completedOutputs);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void NativeInMemWriteRead([Values] DeviceType deviceType)
        {
            Setup(128, new LogSettings { PageSizeBits = 10, MemorySizeBits = 12, SegmentSizeBits = 22 }, deviceType);

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);

            AssertCompleted(new(StatusCode.Found), status);
            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void NativeInMemWriteReadDelete([Values] DeviceType deviceType)
        {
            Setup(128, new LogSettings { PageSizeBits = 10, MemorySizeBits = 12, SegmentSizeBits = 22 }, deviceType);

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
            AssertCompleted(new(StatusCode.Found), status);

            session.Delete(ref key1, Empty.Default, 0);

            status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
            AssertCompleted(new(StatusCode.NotFound), status);

            var key2 = new KeyStruct { kfield1 = 14, kfield2 = 15 };
            var value2 = new ValueStruct { vfield1 = 24, vfield2 = 25 };

            session.Upsert(ref key2, ref value2, Empty.Default, 0);
            status = session.Read(ref key2, ref input, ref output, Empty.Default, 0);

            AssertCompleted(new(StatusCode.Found), status);
            Assert.AreEqual(value2.vfield1, output.value.vfield1);
            Assert.AreEqual(value2.vfield2, output.value.vfield2);
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

            InputStruct input = default;
            OutputStruct output = default;

            for (int i = 0; i < 10 * count; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = 14 };
                var value = new ValueStruct { vfield1 = i, vfield2 = 24 };

                session.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            for (int i = 0; i < 10 * count; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = 14 };
                session.Delete(ref key1, Empty.Default, 0);
            }

            for (int i = 0; i < 10 * count; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = 14 };
                var value = new ValueStruct { vfield1 = i, vfield2 = 24 };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                AssertCompleted(new(StatusCode.NotFound), status);

                session.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            for (int i = 0; i < 10 * count; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = 14 };
                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                AssertCompleted(new(StatusCode.Found), status);
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
            session = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());

            InputStruct input = default;

            Random r = new(10);
            for (int c = 0; c < count; c++)
            {
                var i = r.Next(10000);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            r = new Random(10);

            for (int c = 0; c < count; c++)
            {
                var i = r.Next(10000);
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                if (session.Read(ref key1, ref input, ref output, Empty.Default, 0).IsPending)
                {
                    session.CompletePending(true);
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
                Assert.IsFalse(session.Read(ref key1, ref input, ref output, Empty.Default, 0).Found);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public unsafe void TestShiftHeadAddress([Values] DeviceType deviceType, [Values] BatchMode batchMode)
        {
            InputStruct input = default;
            const int RandSeed = 10;
            const int RandRange = 1000000;
            const int NumRecs = 2000;

            Random r = new(RandSeed);
            var sw = Stopwatch.StartNew();

            var latencyMs = batchMode == BatchMode.NoBatch ? 0 : DefaultLocalMemoryDeviceLatencyMs;
            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType, latencyMs: latencyMs);

            for (int c = 0; c < NumRecs; c++)
            {
                var i = r.Next(RandRange);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            r = new Random(RandSeed);
            sw.Restart();

            for (int c = 0; c < NumRecs; c++)
            {
                var i = r.Next(RandRange);
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                if (session.Read(ref key1, ref input, ref output, Empty.Default, 0).IsPending)
                {
                    Assert.AreEqual(value.vfield1, output.value.vfield1);
                    Assert.AreEqual(value.vfield2, output.value.vfield2);
                }
            }
            session.CompletePending(true);

            // Shift head and retry - should not find in main memory now
            store.Log.FlushAndEvict(true);

            r = new Random(RandSeed);
            sw.Restart();

            const int batchSize = 256;
            for (int c = 0; c < NumRecs; c++)
            {
                var i = r.Next(RandRange);
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                Status foundStatus = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(foundStatus.IsPending);
                if (batchMode == BatchMode.NoBatch)
                {
                    Status status;
                    session.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, output) = GetSinglePendingResult(outputs);
                    Assert.IsTrue(status.Found, status.ToString());
                    Assert.AreEqual(key1.kfield1, output.value.vfield1);
                    Assert.AreEqual(key1.kfield2, output.value.vfield2);
                    outputs.Dispose();
                }
                else if (c > 0 && (c % batchSize) == 0)
                {
                    session.CompletePendingWithOutputs(out var outputs, wait: true);
                    int count = 0;
                    while (outputs.Next())
                    {
                        count++;
                        Assert.AreEqual(outputs.Current.Key.kfield1, outputs.Current.Output.value.vfield1);
                        Assert.AreEqual(outputs.Current.Key.kfield2, outputs.Current.Output.value.vfield2);
                    }
                    outputs.Dispose();
                    Assert.AreEqual(batchSize + (c == batchSize ? 1 : 0), count);
                }
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
                session.RMW(ref key1, ref input, Empty.Default, 0);
            }
            for (int j = 0; j < nums.Length; ++j)
            {
                var i = nums[j];
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                input = new InputStruct { ifield1 = i, ifield2 = i + 1 };
                if (session.RMW(ref key1, ref input, ref output, Empty.Default, 0).IsPending)
                {
                    session.CompletePending(true);
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

                status = session.Read(ref key, ref input, ref output, Empty.Default, 0);

                AssertCompleted(new(StatusCode.Found), status);
                Assert.AreEqual(2 * value.vfield1, output.value.vfield1);
                Assert.AreEqual(2 * value.vfield2, output.value.vfield2);
            }

            key = new KeyStruct { kfield1 = nums.Length, kfield2 = nums.Length + 1 };
            status = session.Read(ref key, ref input, ref output, Empty.Default, 0);
            AssertCompleted(new(StatusCode.NotFound), status);
        }

        // Tests the overload where no reference params used: key,input,userContext,serialNo
        [Test]
        [Category("TsavoriteKV")]
        public unsafe void NativeInMemRMWNoRefKeys([Values] DeviceType deviceType)
        {
            InputStruct input = default;

            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);

            var nums = Enumerable.Range(0, 1000).ToArray();
            var rnd = new Random(11);
            for (int i = 0; i < nums.Length; ++i)
            {
                int randomIndex = rnd.Next(nums.Length);
                int temp = nums[randomIndex];
                nums[randomIndex] = nums[i];
                nums[i] = temp;
            }

            // InitialUpdater
            for (int j = 0; j < nums.Length; ++j)
            {
                var i = nums[j];
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                input = new InputStruct { ifield1 = i, ifield2 = i + 1 };
                session.RMW(ref key1, ref input, Empty.Default, 0);
            }

            // CopyUpdater
            for (int j = 0; j < nums.Length; ++j)
            {
                var i = nums[j];
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                input = new InputStruct { ifield1 = i, ifield2 = i + 1 };
                session.RMW(key1, input);  // no ref and do not set any other params
            }

            OutputStruct output = default;
            Status status;
            KeyStruct key;

            for (int j = 0; j < nums.Length; ++j)
            {
                var i = nums[j];

                key = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                ValueStruct value = new() { vfield1 = i, vfield2 = i + 1 };

                status = session.Read(ref key, ref input, ref output, Empty.Default, 0);

                AssertCompleted(new(StatusCode.Found), status);
                Assert.AreEqual(2 * value.vfield1, output.value.vfield1);
                Assert.AreEqual(2 * value.vfield2, output.value.vfield2);
            }

            key = new KeyStruct { kfield1 = nums.Length, kfield2 = nums.Length + 1 };
            status = session.Read(ref key, ref input, ref output, Empty.Default, 0);
            AssertCompleted(new(StatusCode.NotFound), status);
        }

        // Tests the overload of .Read(key, input, out output,  context, serialNo)
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ReadNoRefKeyInputOutput([Values] DeviceType deviceType)
        {
            InputStruct input = default;

            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = session.Read(key1, input, out OutputStruct output, Empty.Default, 111);
            AssertCompleted(new(StatusCode.Found), status);

            // Verify the read data
            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
            Assert.AreEqual(key1.kfield1, 13);
            Assert.AreEqual(key1.kfield2, 14);
        }

        // Test the overload call of .Read (key, out output, userContext, serialNo)
        [Test]
        [Category("TsavoriteKV")]
        public void ReadNoRefKey([Values] DeviceType deviceType)
        {
            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = session.Read(key1, out OutputStruct output, Empty.Default, 1);
            AssertCompleted(new(StatusCode.Found), status);

            // Verify the read data
            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
            Assert.AreEqual(key1.kfield1, 13);
            Assert.AreEqual(key1.kfield2, 14);
        }


        // Test the overload call of .Read (ref key, ref output, userContext, serialNo)
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ReadWithoutInput([Values] DeviceType deviceType)
        {
            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);

            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = session.Read(ref key1, ref output, Empty.Default, 99);
            AssertCompleted(new(StatusCode.Found), status);

            // Verify the read data
            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
            Assert.AreEqual(key1.kfield1, 13);
            Assert.AreEqual(key1.kfield2, 14);
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

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = session.Read(ref key1, ref input, ref output, Empty.Default);
            AssertCompleted(new(StatusCode.Found), status);

            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
            Assert.AreEqual(key1.kfield1, 13);
            Assert.AreEqual(key1.kfield2, 14);
        }

        // Test the overload call of .Read (key)
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ReadBareMinParams([Values] DeviceType deviceType)
        {
            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);

            var (status, output) = session.Read(key1);
            AssertCompleted(new(StatusCode.Found), status);

            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
            Assert.AreEqual(key1.kfield1, 13);
            Assert.AreEqual(key1.kfield2, 14);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ReadAtAddressDefaultOptions()
        {
            // Just functional test of ReadFlag so one device is enough
            deviceType = DeviceType.MLSD;

            Setup(128, new LogSettings { MemorySizeBits = 29 }, deviceType);

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };
            ReadOptions readOptions = default;

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = session.ReadAtAddress(store.Log.BeginAddress, ref input, ref output, ref readOptions, out _, Empty.Default, 0);
            AssertCompleted(new(StatusCode.Found), status);

            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
            Assert.AreEqual(key1.kfield1, 13);
            Assert.AreEqual(key1.kfield2, 14);
        }

        class SkipReadCacheFunctions : Functions
        {
            internal long expectedReadAddress;

            public override bool SingleReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst, ref ReadInfo readInfo)
            {
                Assign(ref value, ref dst, ref readInfo);
                return true;
            }

            public override bool ConcurrentReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
            {
                Assign(ref value, ref dst, ref readInfo);
                return true;
            }

            void Assign(ref ValueStruct value, ref OutputStruct dst, ref ReadInfo readInfo)
            {
                dst.value = value;
                Assert.AreEqual(expectedReadAddress, readInfo.Address);
                expectedReadAddress = -1;   // show that the test executed
            }
            public override void ReadCompletionCallback(ref KeyStruct key, ref InputStruct input, ref OutputStruct output, Empty ctx, Status status, RecordMetadata recordMetadata)
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
            deviceType = DeviceType.MLSD;

            Setup(128, new LogSettings { MemorySizeBits = 29, ReadCacheSettings = new ReadCacheSettings() }, deviceType);

            SkipReadCacheFunctions functions = new();
            using var skipReadCacheSession = store.NewSession<InputStruct, OutputStruct, Empty, SkipReadCacheFunctions>(functions);

            InputStruct input = default;
            OutputStruct output = default;
            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };
            var readAtAddress = store.Log.BeginAddress;
            Status status;

            skipReadCacheSession.Upsert(ref key1, ref value, Empty.Default, 0);

            void VerifyOutput()
            {
                Assert.AreEqual(-1, functions.expectedReadAddress);     // make sure the test executed
                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
                Assert.AreEqual(13, key1.kfield1);
                Assert.AreEqual(14, key1.kfield2);
            }

            void VerifyResult()
            {
                if (status.IsPending)
                {
                    skipReadCacheSession.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                Assert.IsTrue(status.Found);
                VerifyOutput();
            }

            // This will just be an ordinary read, as the record is in memory.
            functions.expectedReadAddress = readAtAddress;
            status = skipReadCacheSession.Read(ref key1, ref input, ref output);
            Assert.IsTrue(status.Found);
            VerifyOutput();

            // ReadCache is used when the record is read from disk.
            store.Log.FlushAndEvict(wait: true);

            // DisableReadCacheUpdates is primarily for indexing, so a read during index scan does not result in a readcache update.
            // Reading at a normal logical address will not use the readcache, because the "readcache" bit is not set in that logical address.
            // And we cannot get a readcache address, since reads satisfied from the readcache pass kInvalidAddress to functions.
            // Therefore, we test here simply that we do not put it in the readcache when we tell it not to.

            // Do not put it into the read cache.
            functions.expectedReadAddress = readAtAddress;
            ReadOptions readOptions = new() { CopyOptions = ReadCopyOptions.None };
            status = skipReadCacheSession.ReadAtAddress(readAtAddress, ref key1, ref input, ref output, ref readOptions, out _);
            VerifyResult();

            Assert.AreEqual(store.ReadCache.BeginAddress, store.ReadCache.TailAddress);

            // Put it into the read cache.
            functions.expectedReadAddress = readAtAddress;
            readOptions.CopyOptions = new(ReadCopyFrom.AllImmutable, ReadCopyTo.ReadCache);
            status = skipReadCacheSession.ReadAtAddress(readAtAddress, ref key1, ref input, ref output, ref readOptions, out _);
            Assert.IsTrue(status.IsPending);
            VerifyResult();

            Assert.Less(store.ReadCache.BeginAddress, store.ReadCache.TailAddress);

            // Now this will read from the read cache.
            functions.expectedReadAddress = Constants.kInvalidAddress;
            status = skipReadCacheSession.Read(ref key1, ref input, ref output);
            Assert.IsFalse(status.IsPending);
            Assert.IsTrue(status.Found);
            VerifyOutput();
        }

        // Simple Upsert test where ref key and ref value but nothing else set
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void UpsertDefaultsTest([Values] DeviceType deviceType)
        {
            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            Assert.AreEqual(0, store.EntryCount);

            session.Upsert(ref key1, ref value);
            var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
            AssertCompleted(new(StatusCode.Found), status);

            Assert.AreEqual(1, store.EntryCount);
            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
        }

        // Simple Upsert test of overload where not using Ref for key and value and setting all parameters
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void UpsertNoRefNoDefaultsTest()
        {
            // Just checking more parameter values so one device is enough
            deviceType = DeviceType.MLSD;

            Setup(128, new LogSettings { MemorySizeBits = 29 }, deviceType);

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(key1, value, Empty.Default, 0);
            var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
            AssertCompleted(new(StatusCode.Found), status);

            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
        }


        // Upsert Test using Serial Numbers ... based on the VersionedRead Sample
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void UpsertSerialNumberTest()
        {
            // Simple Upsert of Serial Number test so one device is enough
            deviceType = DeviceType.MLSD;

            Setup(128, new LogSettings { MemorySizeBits = 29 }, deviceType);

            int numKeys = 100;
            int keyMod = 10;
            int maxLap = numKeys / keyMod;
            InputStruct input = default;
            OutputStruct output = default;

            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };
            var key = new KeyStruct { kfield1 = 13, kfield2 = 14 };

            for (int i = 0; i < numKeys; i++)
            {
                // lap is used to illustrate the changing values
                var lap = i / keyMod;
                session.Upsert(ref key, ref value, serialNo: lap);
            }

            // Now verify 
            for (int j = 0; j < numKeys; j++)
            {
                var status = session.Read(ref key, ref input, ref output, serialNo: maxLap + 1);

                AssertCompleted(new(StatusCode.Found), status);
                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
            }
        }

        //**** Quick End to End Sample code from help docs ***
        // Very minor changes to LogDevice call and type of Asserts to use but basically code from Sample code in docs
        // Also tests the overload call of .Read (ref key ref output) 
        [Test]
        [Category("TsavoriteKV")]
        public static void KVBasicsSampleEndToEndInDocs()
        {
            string testDir = MethodTestDir;
            using var log = Devices.CreateLogDevice($"{testDir}/hlog.log", deleteOnClose: false);
            using var store = new TsavoriteKV<long, long>(1L << 20, new LogSettings { LogDevice = log });
            using var s = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
            long key = 1, value = 1, input = 10, output = 0;
            s.Upsert(ref key, ref value);
            s.Read(ref key, ref output);
            Assert.AreEqual(value, output);
            s.RMW(ref key, ref input);
            s.RMW(ref key, ref input);
            s.Read(ref key, ref output);
            Assert.AreEqual(10, output);
        }

#if WINDOWS
        [Test]
        [Category("TsavoriteKV")]
        public static void LogPathtooLong()
        {
            string testDir = new('x', Native32.WIN32_MAX_PATH - 11);                       // As in LSD, -11 for ".<segment>"
            using var log = Devices.CreateLogDevice($"{testDir}", deleteOnClose: true);     // Should succeed
            Assert.Throws(typeof(TsavoriteException), () => Devices.CreateLogDevice($"{testDir}y", deleteOnClose: true));
        }
#endif

        [Test]
        [Category("TsavoriteKV")]
        public static void UshortKeyByteValueTest()
        {
            using var log = Devices.CreateLogDevice($"{MethodTestDir}/hlog.log", deleteOnClose: false);
            using var store = new TsavoriteKV<ushort, byte>(1L << 20, new LogSettings { LogDevice = log });
            using var s = store.NewSession<byte, byte, Empty, SimpleFunctions<ushort, byte>>(new SimpleFunctions<ushort, byte>());
            ushort key = 1024;
            byte value = 1, input = 10, output = 0;

            // For blittable types, the records are not 8-byte aligned; RecordSize is sizeof(RecordInfo) + sizeof(ushort) + sizeof(byte)
            const int expectedRecordSize = sizeof(long) + sizeof(ushort) + sizeof(byte);
            Assert.AreEqual(11, expectedRecordSize);
            long prevTailLogicalAddress = store.hlog.GetTailAddress();
            long prevTailPhysicalAddress = store.hlog.GetPhysicalAddress(prevTailLogicalAddress);
            for (var ii = 0; ii < 5; ++ii, ++key, ++value, ++input)
            {
                output = 0;
                s.Upsert(ref key, ref value);
                s.Read(ref key, ref output);
                Assert.AreEqual(value, output);
                s.RMW(ref key, ref input);
                s.Read(ref key, ref output);
                Assert.AreEqual(input, output);

                var tailLogicalAddress = store.hlog.GetTailAddress();
                Assert.AreEqual(expectedRecordSize, tailLogicalAddress - prevTailLogicalAddress);
                long tailPhysicalAddress = store.hlog.GetPhysicalAddress(tailLogicalAddress);
                Assert.AreEqual(expectedRecordSize, tailPhysicalAddress - prevTailPhysicalAddress);

                prevTailLogicalAddress = tailLogicalAddress;
                prevTailPhysicalAddress = tailPhysicalAddress;
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        public static void BasicSyncOperationsTest()
        {
            using var log = Devices.CreateLogDevice($"{MethodTestDir}/hlog.log", deleteOnClose: false);
            using var store = new TsavoriteKV<long, long>(1L << 20, new LogSettings { LogDevice = log });
            using var session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
            const int numRecords = 500;
            const int valueMult = 1_000_000;

            var hashes = new long[numRecords];
            Status status;
            long output;

            for (long key = 0; key < numRecords; key++)
            {
                long value = key + valueMult;
                hashes[key] = store.comparer.GetHashCode64(ref key);
                status = session.Upsert(key, value);
                Assert.IsTrue(status.Record.Created, status.ToString());
                status = session.Read(key, out output);
                Assert.IsTrue(status.Found, status.ToString());
                Assert.AreEqual(value, output);
            }

            void doUpdate(bool useRMW)
            {
                // Update and Read without keyHash
                for (long key = 0; key < numRecords; key++)
                {
                    long value = key + valueMult * 2;
                    if (useRMW)
                    {
                        status = session.RMW(key, value);
                        Assert.IsTrue(status.Record.InPlaceUpdated, status.ToString());
                    }
                    else
                    {
                        status = session.Upsert(key, value);
                        Assert.IsTrue(status.Record.InPlaceUpdated, status.ToString());
                    }
                    status = session.Read(key, out output);
                    Assert.IsTrue(status.Found, status.ToString());
                    Assert.AreEqual(value, output);
                }

                // Update and Read with keyHash
                for (long key = 0; key < numRecords; key++)
                {
                    long value = key + valueMult * 3;
                    if (useRMW)
                    {
                        RMWOptions rmwOptions = new() { KeyHash = hashes[key] };
                        status = session.RMW(key, value, ref rmwOptions);
                        Assert.IsTrue(status.Record.InPlaceUpdated, status.ToString());
                    }
                    else
                    {
                        UpsertOptions upsertOptions = new() { KeyHash = hashes[key] };
                        status = session.Upsert(key, value, ref upsertOptions);
                        Assert.IsTrue(status.Record.InPlaceUpdated, status.ToString());
                    }
                    ReadOptions readOptions = new() { KeyHash = hashes[key] };
                    status = session.Read(key, out output, ref readOptions);
                    Assert.IsTrue(status.Found, status.ToString());
                    Assert.AreEqual(value, output);
                }
            }

            doUpdate(useRMW: false);
            doUpdate(useRMW: true);

            // Delete without keyHash
            for (long key = 0; key < numRecords; key++)
            {
                status = session.Delete(key);
                Assert.IsTrue(status.Found, status.ToString());
                status = session.Read(key, out _);
                Assert.IsTrue(status.NotFound, status.ToString());
            }

            // Update and Read without keyHash
            for (long key = 0; key < numRecords; key++)
            {
                DeleteOptions deleteOptions = new() { KeyHash = hashes[key] };
                status = session.Delete(key, ref deleteOptions);
                ReadOptions readOptions = new() { KeyHash = hashes[key] };
                status = session.Read(key, out _, ref readOptions);
                Assert.IsTrue(status.NotFound, status.ToString());
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        public static async Task BasicAsyncOperationsTest()
        {
            using var log = Devices.CreateLogDevice($"{MethodTestDir}/hlog.log", deleteOnClose: false);
            using var store = new TsavoriteKV<long, long>(1L << 20, new LogSettings { LogDevice = log });
            using var session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
            const int numRecords = 500;
            const int valueMult = 1_000_000;

            var hashes = new long[numRecords];
            Status status;
            long output;

            for (long key = 0; key < numRecords; key++)
            {
                long value = key + valueMult;
                hashes[key] = store.comparer.GetHashCode64(ref key);
                status = await CompleteAsync(session.UpsertAsync(key, value));
                Assert.IsTrue(status.Record.Created, status.ToString());
                (status, output) = await CompleteAsync(session.ReadAsync(key));
                Assert.IsTrue(status.Found, status.ToString());
                Assert.AreEqual(value, output);
            }

            async void doUpdate(bool useRMW)
            {
                // Update and Read without keyHash
                for (long key = 0; key < numRecords; key++)
                {
                    long value = key + valueMult * 2;
                    if (useRMW)
                    {
                        status = await CompleteAsync(session.RMWAsync(key, value));
                        Assert.IsTrue(status.Record.InPlaceUpdated, status.ToString());
                    }
                    else
                    {
                        status = await CompleteAsync(session.UpsertAsync(key, value));
                        Assert.IsTrue(status.Record.InPlaceUpdated, status.ToString());
                    }
                    (status, output) = await CompleteAsync(session.ReadAsync(key));
                    Assert.IsTrue(status.Found, status.ToString());
                    Assert.AreEqual(value, output);
                }

                // Update and Read with keyHash
                for (long key = 0; key < numRecords; key++)
                {
                    long value = key + valueMult * 3;
                    if (useRMW)
                    {
                        RMWOptions rmwOptions = new() { KeyHash = hashes[key] };
                        status = await CompleteAsync(session.RMWAsync(key, value, ref rmwOptions));
                        Assert.IsTrue(status.Record.InPlaceUpdated, status.ToString());
                    }
                    else
                    {
                        UpsertOptions upsertOptions = new() { KeyHash = hashes[key] };
                        status = await CompleteAsync(session.UpsertAsync(key, value, ref upsertOptions));
                        Assert.IsTrue(status.Record.InPlaceUpdated, status.ToString());
                    }
                    ReadOptions readOptions = new() { KeyHash = hashes[key] };
                    (status, output) = await CompleteAsync(session.ReadAsync(key, ref readOptions));
                    Assert.IsTrue(status.Found, status.ToString());
                    Assert.AreEqual(value, output);
                }
            }

            doUpdate(useRMW: false);
            doUpdate(useRMW: true);

            // Delete without keyHash
            for (long key = 0; key < numRecords; key++)
            {
                status = await CompleteAsync(session.DeleteAsync(key));
                Assert.IsTrue(status.Found, status.ToString());
                (status, _) = await CompleteAsync(session.ReadAsync(key));
                Assert.IsTrue(status.NotFound, status.ToString());
            }

            // Update and Read without keyHash
            for (long key = 0; key < numRecords; key++)
            {
                DeleteOptions deleteOptions = new() { KeyHash = hashes[key] };
                status = await CompleteAsync(session.DeleteAsync(key, ref deleteOptions));
                ReadOptions readOptions = new() { KeyHash = hashes[key] };
                (status, _) = await CompleteAsync(session.ReadAsync(key, ref readOptions));
                Assert.IsTrue(status.NotFound, status.ToString());
            }
        }
    }
}