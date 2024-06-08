﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.ReadCacheTests
{
    [TestFixture]
    internal class RandomReadCacheTests
    {
        class Functions : SpanByteFunctions<Empty>
        {
            public override bool ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
                => SingleReader(ref key, ref input, ref value, ref dst, ref readInfo);

            public override bool SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo)
            {
                var keyString = new string(MemoryMarshal.Cast<byte, char>(key.AsReadOnlySpan()));
                var inputString = new string(MemoryMarshal.Cast<byte, char>(input.AsReadOnlySpan()));
                var valueString = new string(MemoryMarshal.Cast<byte, char>(value.AsReadOnlySpan()));
                var actualValue = long.Parse(valueString);
                Assert.AreEqual(long.Parse(keyString) * 2, actualValue);
                Assert.AreEqual(long.Parse(inputString), actualValue);

                value.CopyTo(ref dst, MemoryPool<byte>.Shared);
                return true;
            }

            public override void ReadCompletionCallback(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, Empty context, Status status, RecordMetadata recordMetadata)
            {
                Assert.IsTrue(status.Found);
                var keyString = new string(MemoryMarshal.Cast<byte, char>(key.AsReadOnlySpan()));
                var inputString = new string(MemoryMarshal.Cast<byte, char>(input.AsReadOnlySpan()));
                var outputString = new string(MemoryMarshal.Cast<byte, char>(output.AsReadOnlySpan()));
                var actualValue = long.Parse(outputString);
                Assert.AreEqual(long.Parse(keyString) * 2, actualValue);
                Assert.AreEqual(long.Parse(inputString), actualValue);
                Assert.IsNotNull(output.Memory, $"key {keyString}, in ReadCC");
            }
        }

        IDevice log = default;
        TsavoriteKV<SpanByte, SpanByte> store = default;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);

            ReadCacheSettings readCacheSettings = default;
            string filename = Path.Join(MethodTestDir, "BasicTests.log");

            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is ReadCacheMode rcm)
                {
                    if (rcm == ReadCacheMode.UseReadCache)
                        readCacheSettings = new()
                        {
                            MemorySizeBits = 15,
                            PageSizeBits = 12,
                            SecondChanceFraction = 0.1,
                        };
                    continue;
                }
                if (arg is DeviceType deviceType)
                {
                    log = CreateTestDevice(deviceType, filename, deleteOnClose: true);
                    continue;
                }
            }
            log ??= Devices.CreateLogDevice(filename, deleteOnClose: true);

            store = new TsavoriteKV<SpanByte, SpanByte>(
                size: 1L << 20,
                new LogSettings
                {
                    LogDevice = log,
                    MemorySizeBits = 15,
                    PageSizeBits = 12,
                    ReadCacheSettings = readCacheSettings,
                });
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

            void LocalRead(BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, Functions> sessionContext, int i, ref int numPending, bool isLast)
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
                        Assert.AreEqual(i * 2, long.Parse(outputString));
                        output.Memory.Dispose();
                    }
                    else
                    {
                        Assert.IsTrue(status.IsPending, $"was not Pending: {keyString}; status {status}");
                        ++numPending;
                    }
                }

                if (numPending > 0 && ((numPending % PendingMod) == 0 || isLast))
                {
                    sessionContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    using (completedOutputs)
                    {
                        while (completedOutputs.Next())
                        {
                            var status = completedOutputs.Current.Status;
                            var output = completedOutputs.Current.Output;
                            // Note: do NOT overwrite 'key' here
                            long keyLong = long.Parse(new string(MemoryMarshal.Cast<byte, char>(completedOutputs.Current.Key.AsReadOnlySpan())));

                            Assert.IsTrue(status.Found, $"key {keyLong}, {status}, wasPending {true}, pt 1");
                            Assert.IsNotNull(output.Memory, $"key {keyLong}, wasPending {true}, pt 2");
                            var outputString = new string(MemoryMarshal.Cast<byte, char>(output.AsReadOnlySpan()));
                            Assert.AreEqual(keyLong * 2, long.Parse(outputString), $"key {keyLong}, wasPending {true}, pt 3");
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
                        Assert.IsTrue(!status.Found && status.Record.Created, status.ToString());
                    }
                }
            }

            if (numThreads == 1)
            {
                LocalRun(0, MaxKeys);
                return;
            }

            var numKeysPerThread = MaxKeys / numThreads;

            List<Task> tasks = new();   // Task rather than Thread for propagation of exception.
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                if (keyContentionMode == KeyContentionMode.Contention)
                    tasks.Add(Task.Factory.StartNew(() => LocalRun(0, MaxKeys)));
                else
                    tasks.Add(Task.Factory.StartNew(() => LocalRun(numKeysPerThread * tid, numKeysPerThread * (tid + 1))));
            }
            Task.WaitAll(tasks.ToArray());
        }
    }
}