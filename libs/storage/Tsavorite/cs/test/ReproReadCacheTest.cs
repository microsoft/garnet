// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.ReadCacheTests
{
    using SpanByteStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    [TestFixture]
    internal class RandomReadCacheTests
    {
        class Functions : SpanByteFunctions<Empty>
        {
            public override bool ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
                => SingleReader(ref key, ref input, ref value, ref dst, ref readInfo);

            public override bool SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo)
            {
                var parsedKey = long.Parse(key.AsReadOnlySpan());
                var parsedInput = long.Parse(input.AsReadOnlySpan());
                var actualValue = long.Parse(value.AsReadOnlySpan());
                ClassicAssert.AreEqual(parsedKey * 2, actualValue);
                ClassicAssert.AreEqual(parsedInput, actualValue);

                value.CopyTo(ref dst, MemoryPool<byte>.Shared);
                return true;
            }

            public override void ReadCompletionCallback(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, Empty context, Status status, RecordMetadata recordMetadata)
            {
                ClassicAssert.IsTrue(status.Found);
                var parsedKey = long.Parse(key.AsReadOnlySpan());
                var parsedInput = long.Parse(input.AsReadOnlySpan());
                var actualValue = long.Parse(output.AsReadOnlySpan());
                ClassicAssert.AreEqual(parsedKey * 2, actualValue);
                ClassicAssert.AreEqual(parsedInput, actualValue);
                ClassicAssert.IsNotNull(output.Memory, $"key {parsedKey}, in ReadCC");
            }
        }

        IDevice log = default;
        TsavoriteKV<SpanByte, SpanByte, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store = default;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);

            string filename = Path.Join(MethodTestDir, "BasicTests.log");

            var kvSettings = new KVSettings<SpanByte, SpanByte>()
            {
                IndexSize = 1L << 26,
                MemorySize = 1L << 15,
                PageSize = 1L << 12,
            };

            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is ReadCacheMode rcm)
                {
                    if (rcm == ReadCacheMode.UseReadCache)
                    {
                        kvSettings.ReadCacheMemorySize = 1L << 15;
                        kvSettings.ReadCachePageSize = 1L << 12;
                        kvSettings.ReadCacheSecondChanceFraction = 0.1;
                        kvSettings.ReadCacheEnabled = true;
                    }
                    continue;
                }
                if (arg is DeviceType deviceType)
                {
                    log = CreateTestDevice(deviceType, filename, deleteOnClose: true);
                    continue;
                }
            }

            kvSettings.LogDevice = log ??= Devices.CreateLogDevice(filename, deleteOnClose: true);

            store = new(kvSettings
                , StoreFunctions<SpanByte, SpanByte>.Create()
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = default;
            log?.Dispose();
            log = default;
            DeleteDirectory(MethodTestDir);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(StressTestCategory)]
        //[Repeat(300)]
        public void RandomReadCacheTest([Values(1, 2, 8)] int numThreads, [Values] KeyContentionMode keyContentionMode, [Values] ReadCacheMode readCacheMode)
        {
            if (numThreads == 1 && keyContentionMode == KeyContentionMode.Contention)
                Assert.Ignore("Skipped because 1 thread cannot have contention");
            if (numThreads > 2 && IsRunningAzureTests)
                Assert.Ignore("Skipped because > 2 threads when IsRunningAzureTests");
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            const int PendingMod = 16;

            void LocalRead(BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, Functions, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> sessionContext, int i, ref int numPending, bool isLast)
            {
                Span<byte> keySpan = stackalloc byte[64];
                Span<byte> inputSpan = stackalloc byte[64];

                var key = i;
                var input = i * 2;

                _ = key.TryFormat(keySpan, out var keyBytesWritten);
                _ = input.TryFormat(inputSpan, out var inputBytesWritten);

                var sbKey = SpanByte.FromPinnedSpan(keySpan.Slice(0, keyBytesWritten));
                var sbInput = SpanByte.FromPinnedSpan(inputSpan.Slice(0, inputBytesWritten));
                SpanByteAndMemory output = default;

                var status = sessionContext.Read(ref sbKey, ref sbInput, ref output);

                if (status.Found)
                {
                    ClassicAssert.AreEqual(i * 2, long.Parse(output.AsReadOnlySpan()));
                    output.Memory.Dispose();
                }
                else
                {
                    ClassicAssert.IsTrue(status.IsPending, $"was not Pending: {key}; status {status}");
                    ++numPending;
                }

                if (numPending > 0 && ((numPending % PendingMod) == 0 || isLast))
                {
                    _ = sessionContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    using (completedOutputs)
                    {
                        while (completedOutputs.Next())
                        {
                            var completedStatus = completedOutputs.Current.Status;
                            var completedOutput = completedOutputs.Current.Output;
                            // Note: do NOT overwrite 'key' here
                            long keyLong = long.Parse(completedOutputs.Current.Key.AsReadOnlySpan());

                            ClassicAssert.IsTrue(completedStatus.Found, $"key {keyLong}, {completedStatus}, wasPending {true}, pt 1");
                            ClassicAssert.IsNotNull(completedOutput.Memory, $"key {keyLong}, wasPending {true}, pt 2");
                            ClassicAssert.AreEqual(keyLong * 2, long.Parse(completedOutput.AsReadOnlySpan()), $"key {keyLong}, wasPending {true}, pt 3");
                            completedOutput.Memory.Dispose();
                        }
                    }
                }
            }

            void LocalRun(int startKey, int endKey)
            {
                var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, Functions>(new Functions());
                var sessionContext = session.BasicContext;

                int numPending = 0;

                // read through the keys in order (works)
                for (int i = startKey; i < endKey; i++)
                    LocalRead(sessionContext, i, ref numPending, i == endKey - 1);

                // pick random keys to read
                var r = new Random(2115);
                for (int i = startKey; i < endKey; i++)
                    LocalRead(sessionContext, r.Next(startKey, endKey), ref numPending, i == endKey - 1);
            }

            const int MaxKeys = 8000;

            { // Write the values first (single-threaded, all keys)
                var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, Functions>(new Functions());
                var bContext = session.BasicContext;

                Span<byte> keySpan = stackalloc byte[64];
                Span<byte> valueSpan = stackalloc byte[64];

                for (int i = 0; i < MaxKeys; i++)
                {
                    var key = i;
                    var value = i * 2;

                    _ = key.TryFormat(keySpan, out var keyBytesWritten);
                    _ = value.TryFormat(valueSpan, out var valueBytesWritten);

                    var sbKey = SpanByte.FromPinnedSpan(keySpan.Slice(0, keyBytesWritten));
                    var sbValue = SpanByte.FromPinnedSpan(valueSpan.Slice(0, valueBytesWritten));

                    var status = bContext.Upsert(sbKey, sbValue);
                    ClassicAssert.IsTrue(!status.Found && status.Record.Created, status.ToString());
                }
            }

            if (numThreads == 1)
            {
                LocalRun(0, MaxKeys);
                return;
            }

            var numKeysPerThread = MaxKeys / numThreads;

            List<Task> tasks = [];   // Task rather than Thread for propagation of exception.
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                if (keyContentionMode == KeyContentionMode.Contention)
                    tasks.Add(Task.Factory.StartNew(() => LocalRun(0, MaxKeys)));
                else
                    tasks.Add(Task.Factory.StartNew(() => LocalRun(numKeysPerThread * tid, numKeysPerThread * (tid + 1))));
            }
            Task.WaitAll([.. tasks]);
        }
    }
}