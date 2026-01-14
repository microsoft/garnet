// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.ReadCacheTests
{
    using SpanByteStoreFunctions = StoreFunctions<SpanByteComparer, SpanByteRecordDisposer>;

    [TestFixture]
    internal class RandomReadCacheTests
    {
        class Functions : SpanByteFunctions<Empty>
        {
            public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref PinnedSpanByte input, ref SpanByteAndMemory dst, ref ReadInfo readInfo)
            {
                var keyString = new string(MemoryMarshal.Cast<byte, char>(srcLogRecord.Key));
                var inputString = new string(MemoryMarshal.Cast<byte, char>(input.ReadOnlySpan));
                var valueString = new string(MemoryMarshal.Cast<byte, char>(srcLogRecord.ValueSpan));
                var actualValue = long.Parse(valueString);
                ClassicAssert.AreEqual(long.Parse(keyString) * 2, actualValue);
                ClassicAssert.AreEqual(long.Parse(inputString), actualValue);

                srcLogRecord.ValueSpan.CopyTo(ref dst, MemoryPool<byte>.Shared);
                return true;
            }

            public override void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, Empty context, Status status, RecordMetadata recordMetadata)
            {
                ClassicAssert.IsTrue(status.Found);
                var keyString = new string(MemoryMarshal.Cast<byte, char>(diskLogRecord.Key));
                var inputString = new string(MemoryMarshal.Cast<byte, char>(input.ReadOnlySpan));
                var outputString = new string(MemoryMarshal.Cast<byte, char>(output.ReadOnlySpan));
                var actualValue = long.Parse(outputString);
                ClassicAssert.AreEqual(long.Parse(keyString) * 2, actualValue);
                ClassicAssert.AreEqual(long.Parse(inputString), actualValue);
                ClassicAssert.IsNotNull(output.Memory, $"key {keyString}, in ReadCC");
            }
        }

        IDevice log = default;
        TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store = default;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);

            string filename = Path.Join(MethodTestDir, "BasicTests.log");

            var kvSettings = new KVSettings()
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
                if (arg is TestDeviceType deviceType)
                {
                    log = CreateTestDevice(deviceType, filename, deleteOnClose: true);
                    continue;
                }
            }

            kvSettings.LogDevice = log ??= Devices.CreateLogDevice(filename, deleteOnClose: true);

            store = new(kvSettings
                , StoreFunctions.Create(SpanByteComparer.Instance, SpanByteRecordDisposer.Instance)
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
        //[Repeat(1000)]
        public void RandomReadCacheTest([Values(1, 2, 8)] int numThreads, [Values] KeyContentionMode keyContentionMode, [Values] ReadCacheMode readCacheMode)
        {
            if (numThreads == 1 && keyContentionMode == KeyContentionMode.Contention)
                Assert.Ignore("Skipped because 1 thread cannot have contention");
            if (numThreads > 2 && IsRunningAzureTests)
                Assert.Ignore("Skipped because > 2 threads when IsRunningAzureTests");
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1}, name = {TestContext.CurrentContext.Test.Name} ***");

            const int PendingMod = 16;

            unsafe void LocalRead(BasicContext<PinnedSpanByte, SpanByteAndMemory, Empty, Functions, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> sessionContext, int i, ref int numPending, bool isLast)
            {
                // These are OK to be local to this LocalRead call; if it goes pending, they will be copied into IHeapContainers.
                var keyString = $"{i}";
                var key = MemoryMarshal.Cast<char, byte>(keyString.AsSpan());

                SpanByteAndMemory output = default;
                Status status;
                var inputString = $"{i * 2}";
                fixed (char* _ = inputString)
                {
                    var input = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<char, byte>(inputString.AsSpan()));
                    status = sessionContext.Read(key, ref input, ref output);
                }

                if (status.Found)
                {
                    var outputString = new string(MemoryMarshal.Cast<byte, char>(output.ReadOnlySpan));
                    ClassicAssert.AreEqual(i * 2, long.Parse(outputString));
                    output.Memory.Dispose();
                }
                else
                {
                    ClassicAssert.IsTrue(status.IsPending, $"was not Pending: {keyString}; status {status}");
                    ++numPending;
                }

                if (numPending > 0 && ((numPending % PendingMod) == 0 || isLast))
                {
                    _ = sessionContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    using (completedOutputs)
                    {
                        while (completedOutputs.Next())
                        {
                            status = completedOutputs.Current.Status;
                            output = completedOutputs.Current.Output;
                            // Note: do NOT overwrite 'key' here
                            long keyLong = long.Parse(new string(MemoryMarshal.Cast<byte, char>(completedOutputs.Current.Key)));

                            ClassicAssert.IsTrue(status.Found, $"key {keyLong}, {status}, wasPending {true}, pt 1");
                            ClassicAssert.IsNotNull(output.Memory, $"key {keyLong}, wasPending {true}, pt 2");
                            var outputString = new string(MemoryMarshal.Cast<byte, char>(output.ReadOnlySpan));
                            ClassicAssert.AreEqual(keyLong * 2, long.Parse(outputString), $"key {keyLong}, wasPending {true}, pt 3");
                            output.Memory.Dispose();
                        }
                    }
                }
            }

            void LocalRun(int startKey, int endKey)
            {
                var session = store.NewSession<PinnedSpanByte, SpanByteAndMemory, Empty, Functions>(new Functions());
                var sessionContext = session.BasicContext;

                int numPending = 0;

                // read through the keys in order (works)
                for (int keyNum = startKey; keyNum < endKey; keyNum++)
                    LocalRead(sessionContext, keyNum, ref numPending, keyNum == endKey - 1);

                // pick random keys to read
                var r = new Random(2115);
                for (int keyNum = startKey; keyNum < endKey; keyNum++)
                    LocalRead(sessionContext, r.Next(startKey, endKey), ref numPending, keyNum == endKey - 1);
            }

            const int MaxKeys = 8000;

            { // Write the values first (single-threaded, all keys)
                var session = store.NewSession<PinnedSpanByte, SpanByteAndMemory, Empty, Functions>(new Functions());
                var bContext = session.BasicContext;
                for (int i = 0; i < MaxKeys; i++)
                {
                    var keyString = $"{i}";
                    var valueString = $"{i * 2}";
                    var key = MemoryMarshal.Cast<char, byte>(keyString.AsSpan());
                    var value = MemoryMarshal.Cast<char, byte>(valueString.AsSpan());

                    var status = bContext.Upsert(key, value);
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