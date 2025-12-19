// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.spanbyte
{
    using SpanByteStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    [AllureNUnit]
    [TestFixture]
    internal class SpanByteVLVectorTests : AllureTestBase
    {
        const int StackAllocMax = 12;

        static int GetRandomLength(Random r) => r.Next(StackAllocMax) + 1;    // +1 for 0 to StackAllocMax inclusive

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void VLVectorSingleKeyTest()
        {
            DeleteDirectory(MethodTestDir, wait: true);

            var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog1.log"), deleteOnClose: true);
            var store = new TsavoriteKV<SpanByte, SpanByte, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>>(
                new()
                {
                    IndexSize = 1L << 13,
                    LogDevice = log,
                    MemorySize = 1L << 17,
                    PageSize = 1L << 12
                }, StoreFunctions<SpanByte, SpanByte>.Create()
                    , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
                );
            var session = store.NewSession<SpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;

            // Single alloc outside the loop, to the max length we'll need.
            Span<int> keySpan = stackalloc int[1];
            Span<int> valueSpan = stackalloc int[StackAllocMax];

            Random rng = new(100);
            for (int i = 0; i < 5000; i++)
            {
                keySpan[0] = i;
                var keySpanByte = keySpan.AsSpanByte();

                var len = GetRandomLength(rng);
                for (int j = 0; j < len; j++)
                    valueSpan[j] = len;
                var valueSpanByte = valueSpan.Slice(0, len).AsSpanByte();

                _ = bContext.Upsert(ref keySpanByte, ref valueSpanByte, Empty.Default);
            }

            // Reset rng to get the same sequence of value lengths
            rng = new Random(100);
            for (int i = 0; i < 5000; i++)
            {
                keySpan[0] = i;
                var keySpanByte = keySpan.AsSpanByte();

                var valueLen = GetRandomLength(rng);
                int[] output = null;
                var status = bContext.Read(ref keySpanByte, ref output, Empty.Default);

                if (status.IsPending)
                {
                    _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, output) = GetSinglePendingResult(outputs);
                }

                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(valueLen, output.Length);
                for (int j = 0; j < valueLen; j++)
                    ClassicAssert.AreEqual(valueLen, output[j]);
            }
            session.Dispose();
            store.Dispose();
            log.Dispose();
            DeleteDirectory(MethodTestDir);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void VLVectorMultiKeyTest()
        {
            DeleteDirectory(MethodTestDir, wait: true);

            var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog1.log"), deleteOnClose: true);
            var store = new TsavoriteKV<SpanByte, SpanByte, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>>(
                new()
                {
                    IndexSize = 1L << 13,
                    LogDevice = log,
                    MemorySize = 1L << 17,
                    PageSize = 1L << 12
                }, StoreFunctions<SpanByte, SpanByte>.Create()
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
            var session = store.NewSession<SpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;

            // Single alloc outside the loop, to the max length we'll need.
            Span<int> keySpan = stackalloc int[StackAllocMax];
            Span<int> valueSpan = stackalloc int[StackAllocMax];

            Random rng = new(100);
            for (int i = 0; i < 5000; i++)
            {
                var keyLen = GetRandomLength(rng);
                for (int j = 0; j < keyLen; j++)
                    keySpan[j] = i;
                var keySpanByte = keySpan.AsSpanByte();

                var valueLen = GetRandomLength(rng);
                for (int j = 0; j < valueLen; j++)
                    valueSpan[j] = valueLen;
                var valueSpanByte = valueSpan.Slice(0, valueLen).AsSpanByte();

                _ = bContext.Upsert(ref keySpanByte, ref valueSpanByte, Empty.Default);
            }

            // Reset rng to get the same sequence of key and value lengths
            rng = new Random(100);
            for (int i = 0; i < 5000; i++)
            {
                var keyLen = GetRandomLength(rng);
                for (int j = 0; j < keyLen; j++)
                    keySpan[j] = i;
                var keySpanByte = keySpan.AsSpanByte();

                var valueLen = GetRandomLength(rng);
                int[] output = null;
                var status = bContext.Read(ref keySpanByte, ref output, Empty.Default);

                if (status.IsPending)
                {
                    _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, output) = GetSinglePendingResult(outputs);
                }

                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(valueLen, output.Length);
                for (int j = 0; j < valueLen; j++)
                    ClassicAssert.AreEqual(valueLen, output[j]);
            }

            session.Dispose();
            store.Dispose();
            log.Dispose();
            DeleteDirectory(MethodTestDir);
        }
    }
}