// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if LOGRECORD_TODO

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
    using SpanByteStoreFunctions = StoreFunctions<SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    [TestFixture]
    internal class RandomReadCacheTests
    {
        class Functions : SpanByteFunctions<Empty>
        {
            public override bool Reader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo)
            {
                var keyString = new string(MemoryMarshal.Cast<byte, char>(key.AsReadOnlySpan()));
                var inputString = new string(MemoryMarshal.Cast<byte, char>(input.AsReadOnlySpan()));
                var valueString = new string(MemoryMarshal.Cast<byte, char>(value.AsReadOnlySpan()));
                var actualValue = long.Parse(valueString);
                ClassicAssert.AreEqual(long.Parse(keyString) * 2, actualValue);
                ClassicAssert.AreEqual(long.Parse(inputString), actualValue);

                value.CopyTo(ref dst, MemoryPool<byte>.Shared);
                return true;
            }

            public override void ReadCompletionCallback(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, Empty context, Status status, RecordMetadata recordMetadata)
            {
                ClassicAssert.IsTrue(status.Found);
                var keyString = new string(MemoryMarshal.Cast<byte, char>(key.AsReadOnlySpan()));
                var inputString = new string(MemoryMarshal.Cast<byte, char>(input.AsReadOnlySpan()));
                var outputString = new string(MemoryMarshal.Cast<byte, char>(output.AsReadOnlySpan()));
                var actualValue = long.Parse(outputString);
                ClassicAssert.AreEqual(long.Parse(keyString) * 2, actualValue);
                ClassicAssert.AreEqual(long.Parse(inputString), actualValue);
                ClassicAssert.IsNotNull(output.Memory, $"key {keyString}, in ReadCC");
            }
        }

        IDevice log = default;
        TsavoriteKV<SpanByte, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store = default;

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
        public unsafe void RandomReadCacheTest([Values(1, 2, 8)] int numThreads, [Values] KeyContentionMode keyContentionMode, [Values] ReadCacheMode readCacheMode)
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
                var keyString = $"{i}";
                var inputString = $"{i * 2}";
                var key = MemoryMarshal.Cast<char, byte>(keyString.AsSpan());
                var input = MemoryMarshal.Cast<char, byte>(inputString.AsSpan());

                fixed (byte* kptr = key, iptr = input)
                {
                    var sbKey = SpanByte.FromPinnedSpan(key);
                    var sbInput = SpanByte.FromPinnedSpan(input);
                    SpanByteAndMemory output = default;

                    var status = sessionContext.Read(ref sbKey, ref sbInput, ref output);

                    if (status.Found)
                    {
                        var outputString = new string(MemoryMarshal.Cast<byte, char>(output.AsReadOnlySpan()));
                        ClassicAssert.AreEqual(i * 2, long.Parse(outputString));
                        output.Memory.Dispose();
                    }
                    else
                    {
                        ClassicAssert.IsTrue(status.IsPending, $"was not Pending: {keyString}; status {status}");
                        ++numPending;
                    }
                }

                if (numPending > 0 && ((numPending % PendingMod) == 0 || isLast))
                {
                    _ = sessionContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    using (completedOutputs)
                    {
                        while (completedOutputs.Next())
                        {
                            var status = completedOutputs.Current.Status;
                            var output = completedOutputs.Current.Output;
                            // Note: do NOT overwrite 'key' here
                            long keyLong = long.Parse(new string(MemoryMarshal.Cast<byte, char>(completedOutputs.Current.Key.AsReadOnlySpan())));

                            ClassicAssert.IsTrue(status.Found, $"key {keyLong}, {status}, wasPending {true}, pt 1");
                            ClassicAssert.IsNotNull(output.Memory, $"key {keyLong}, wasPending {true}, pt 2");
                            var outputString = new string(MemoryMarshal.Cast<byte, char>(output.AsReadOnlySpan()));
                            ClassicAssert.AreEqual(keyLong * 2, long.Parse(outputString), $"key {keyLong}, wasPending {true}, pt 3");
                            output.Memory.Dispose();
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
                for (int i = 0; i < MaxKeys; i++)
                {
                    var keyString = $"{i}";
                    var valueString = $"{i * 2}";
                    var key = MemoryMarshal.Cast<char, byte>(keyString.AsSpan());
                    var value = MemoryMarshal.Cast<char, byte>(valueString.AsSpan());
                    fixed (byte* k = key, v = value)
                    {
                        var sbKey = SpanByte.FromPinnedSpan(key);
                        var sbValue = SpanByte.FromPinnedSpan(value);
                        var status = bContext.Upsert(sbKey, sbValue);
                        ClassicAssert.IsTrue(!status.Found && status.Record.Created, status.ToString());
                    }
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

#endif // LOGRECORD_TODO