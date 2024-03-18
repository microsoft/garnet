// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
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
        public class Context
        {
            public Status Status { get; set; }
        }

        class Functions : SpanByteFunctions<Context>
        {
            public override bool ConcurrentReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
                => SingleReader(ref key, ref input, ref value, ref dst, ref readInfo);

            public override bool SingleReader(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst, ref ReadInfo readInfo)
            {
                var keyString = new string(MemoryMarshal.Cast<byte, char>(key.AsReadOnlySpan()));
                var inputString = new string(MemoryMarshal.Cast<byte, char>(input.AsReadOnlySpan()));
                var valueString = new string(MemoryMarshal.Cast<byte, char>(value.AsReadOnlySpan()));
                Assert.AreEqual(long.Parse(keyString) * 2, long.Parse(valueString));
                Assert.AreEqual(long.Parse(inputString), long.Parse(valueString));

                value.CopyTo(ref dst, MemoryPool<byte>.Shared);
                return true;
            }

            public override void ReadCompletionCallback(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, Context context, Status status, RecordMetadata recordMetadata)
            {
                Assert.IsTrue(status.Found);
                var keyString = new string(MemoryMarshal.Cast<byte, char>(key.AsReadOnlySpan()));
                var inputString = new string(MemoryMarshal.Cast<byte, char>(input.AsReadOnlySpan()));
                var outputString = new string(MemoryMarshal.Cast<byte, char>(output.AsReadOnlySpan()));
                Assert.AreEqual(long.Parse(keyString) * 2, long.Parse(outputString));
                Assert.AreEqual(long.Parse(inputString), long.Parse(outputString));
                context.Status = status;

                // Need to do this here because we don't get Output below
                output.Memory?.Dispose();
            }
        }

        IDevice log = default;
        TsavoriteKV<SpanByte, SpanByte> store = default;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);

            ReadCacheSettings readCacheSettings = default;
            string filename = MethodTestDir + "/BasicTests.log";

            var concurrencyControlMode = ConcurrencyControlMode.None;
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
                if (arg is ConcurrencyControlMode ccm)
                {
                    concurrencyControlMode = ccm;
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
                }, concurrencyControlMode: concurrencyControlMode);
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
        public unsafe void RandomReadCacheTest([Values(1, 2, 8)] int numThreads, [Values] KeyContentionMode keyContentionMode,
                                                [Values] ConcurrencyControlMode concurrencyControlMode, [Values] ReadCacheMode readCacheMode,
#if WINDOWS
                                                [Values(DeviceType.LSD
#else
                                                [Values(DeviceType.MLSD
#endif
                                                )] DeviceType deviceType)
        {
            if (numThreads == 1 && keyContentionMode == KeyContentionMode.Contention)
                Assert.Ignore("Skipped because 1 thread cannot have contention");
            if (numThreads > 2 && IsRunningAzureTests)
                Assert.Ignore("Skipped because > 2 threads when IsRunningAzureTests");
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            void LocalRead(BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Context, Functions> sessionContext, int i)
            {
                var keyString = $"{i}";
                var inputString = $"{i * 2}";
                var key = MemoryMarshal.Cast<char, byte>(keyString.AsSpan());
                var input = MemoryMarshal.Cast<char, byte>(inputString.AsSpan());

                fixed (byte* kptr = key, iptr = input)
                {
                    var context = new Context();
                    var sbKey = SpanByte.FromFixedSpan(key);
                    var sbInput = SpanByte.FromFixedSpan(input);
                    SpanByteAndMemory output = default;

                    var status = sessionContext.Read(ref sbKey, ref sbInput, ref output, context);

                    if (status.Found)
                    {
                        var outputString = new string(MemoryMarshal.Cast<byte, char>(output.AsReadOnlySpan()));
                        Assert.AreEqual(i * 2, long.Parse(outputString));
                        output.Memory?.Dispose();
                        return;
                    }

                    Assert.IsTrue(status.IsPending, $"was not Pending: {keyString}; status {status}");
                    sessionContext.CompletePending(wait: true);
                }
            }

            void LocalRun(int startKey, int endKey)
            {
                var session = store.NewSession<SpanByte, SpanByteAndMemory, Context, Functions>(new Functions());
                var sessionContext = session.BasicContext;

                // read through the keys in order (works)
                for (int i = startKey; i < endKey; i++)
                    LocalRead(sessionContext, i);

                // pick random keys to read
                var r = new Random(2115);
                for (int i = startKey; i < endKey; i++)
                    LocalRead(sessionContext, r.Next(startKey, endKey));
            }

            const int MaxKeys = 8000;

            { // Write the values first (single-threaded, all keys)
                var session = store.NewSession<SpanByte, SpanByteAndMemory, Context, Functions>(new Functions());
                for (int i = 0; i < MaxKeys; i++)
                {
                    var keyString = $"{i}";
                    var valueString = $"{i * 2}";
                    var key = MemoryMarshal.Cast<char, byte>(keyString.AsSpan());
                    var value = MemoryMarshal.Cast<char, byte>(valueString.AsSpan());
                    fixed (byte* k = key, v = value)
                    {
                        var sbKey = SpanByte.FromFixedSpan(key);
                        var sbValue = SpanByte.FromFixedSpan(value);
                        var status = session.Upsert(sbKey, sbValue);
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