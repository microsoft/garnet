// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test.spanbyte
{
    using SpanByteStoreFunctions = StoreFunctions<SpanByteComparer, SpanByteRecordDisposer>;

    [AllureNUnit]
    [TestFixture]
    internal class SpanByteTests : AllureTestBase
    {
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void SpanByteTest1()
        {
            Span<byte> output = stackalloc byte[20];
            PinnedSpanByte input = default;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            try
            {
                using var log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "hlog1.log"), deleteOnClose: true);
                using var store = new TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>>(
                    new()
                    {
                        IndexSize = 1L << 13,
                        LogDevice = log,
                        MemorySize = 1L << 17,
                        PageSize = 1L << 12
                    }, StoreFunctions.Create(SpanByteComparer.Instance, SpanByteRecordDisposer.Instance)
                    , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
                );
                using var session = store.NewSession<PinnedSpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new SpanByteFunctions<Empty>());
                var bContext = session.BasicContext;

                var key1 = MemoryMarshal.Cast<char, byte>("key1".AsSpan());
                var value1 = MemoryMarshal.Cast<char, byte>("value1".AsSpan());
                var output1 = SpanByteAndMemory.FromPinnedSpan(output);

                _ = bContext.Upsert(key1, value1);
                _ = bContext.Read(key1, ref input, ref output1);

                ClassicAssert.IsTrue(output1.IsSpanByte);
                ClassicAssert.IsTrue(output1.SpanByte.ReadOnlySpan.SequenceEqual(value1));

                var key2 = MemoryMarshal.Cast<char, byte>("key2".AsSpan());
                var value2 = MemoryMarshal.Cast<char, byte>("value2value2value2".AsSpan());
                var output2 = SpanByteAndMemory.FromPinnedSpan(output);

                _ = bContext.Upsert(key2, value2);
                _ = bContext.Read(key2, ref input, ref output2);

                ClassicAssert.IsTrue(!output2.IsSpanByte);
                ClassicAssert.IsTrue(output2.Memory.Memory.Span.Slice(0, output2.Length).SequenceEqual(value2));
                output2.Memory.Dispose();
            }
            finally
            {
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void MultiRead_SpanByte_Test()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            try
            {
                using var log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "test.log"), deleteOnClose: true);
                using var store = new TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>>(
                    new()
                    {
                        IndexSize = 1L << 16,
                        LogDevice = log,
                        MemorySize = 1L << 15,
                        PageSize = 1L << 12
                    }, StoreFunctions.Create(SpanByteComparer.Instance, SpanByteRecordDisposer.Instance)
                    , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
                );
                using var session = store.NewSession<PinnedSpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new SpanByteFunctions<Empty>());
                var bContext = session.BasicContext;

                for (int i = 0; i < 200; i++)
                {
                    var key = MemoryMarshal.Cast<char, byte>($"{i}".AsSpan());
                    var value = MemoryMarshal.Cast<char, byte>($"{i + 1000}".AsSpan());
                    _ = bContext.Upsert(key, value);
                }

                // Read, evict all records to disk, read again
                MultiRead(evicted: false);
                store.Log.FlushAndEvict(true);
                MultiRead(evicted: true);

                void MultiRead(bool evicted)
                {
                    for (long key = 0; key < 50; key++)
                    {
                        // read each key multiple times
                        for (int i = 0; i < 10; i++)
                            ReadKey(key, key + 1000, evicted);
                    }
                }

                void ReadKey(long key, long value, bool evicted)
                {
                    Status status;
                    SpanByteAndMemory output = default;

                    var keySpan = MemoryMarshal.Cast<char, byte>($"{key}".AsSpan());
                    status = bContext.Read(keySpan, ref output);
                    ClassicAssert.AreEqual(evicted, status.IsPending, "evicted/pending mismatch");

                    if (evicted)
                        (status, output) = bContext.GetSinglePendingResult();
                    ClassicAssert.IsTrue(status.Found, $"expected to find key; status = {status}, pending = {evicted}");

                    ClassicAssert.IsFalse(output.IsSpanByte, "Output should not have a valid SpanByte");
                    var outputString = new string(MemoryMarshal.Cast<byte, char>(output.ReadOnlySpan));
                    ClassicAssert.AreEqual(value, long.Parse(outputString), $"outputString mismatch; pending = {evicted}");
                    output.Memory?.Dispose();
                }
            }
            finally
            {
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ShouldSkipEmptySpaceAtEndOfPage()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            using var log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "vl-iter.log"), deleteOnClose: true);
            using var store = new TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>>(
                new()
                {
                    IndexSize = 1L << 13,
                    LogDevice = log,
                    MemorySize = 1L << 17,
                    PageSize = 1L << 10    // 1KB page
                }, StoreFunctions.Create(SpanByteComparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
            using var session = store.NewSession<PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;

            const int PageSize = 1024;
            Span<byte> valueSpan = stackalloc byte[PageSize];

            Set(1L, valueSpan, 800, 1);                // Inserted on page#0 and leaves empty space
            Set(2L, valueSpan, 800, 2);                // Inserted on page#1 because there is not enough space in page#0, and leaves empty space

            // Add a second record on page#1 to fill it exactly. Page#1 starts at offset 0 on the page (unlike page#0, which starts at 24 or 64,
            // depending on data). Subtract the RecordInfo and key space for both the first record and the second record we're about to insert,
            // the value space for the first record, and the length header for the second record. This is the space available for the second record's value.
            var availableSpaceForRecord3 = PageSize * 2 - store.Log.TailAddress;
            var p2value2len = (int)availableSpaceForRecord3
                                - RecordInfo.Size
                                - RecordDataHeader.MinHeaderBytes
                                - sizeof(long);    // key size
            Set(3L, valueSpan, p2value2len, 3);        // Inserted on page#1
            ClassicAssert.AreEqual(PageSize * 2, store.Log.TailAddress, "TailAddress should be at the end of page#2");

            Set(4L, valueSpan, 64, 4);                 // Inserted on page#2

            var data = new List<(long, int, int)>();
            using (var iterator = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress))
            {
                while (iterator.GetNext())
                {
                    var scanKey = iterator.Key.AsRef<long>();
                    var scanValue = iterator.ValueSpan;

                    data.Add((scanKey, scanValue.Length, scanValue[0]));
                }
            }

            ClassicAssert.AreEqual(4, data.Count);

            ClassicAssert.AreEqual((1L, 800, 1), data[0]);
            ClassicAssert.AreEqual((2L, 800, 2), data[1]);
            ClassicAssert.AreEqual((3L, p2value2len, 3), data[2]);
            ClassicAssert.AreEqual((4L, 64, 4), data[3]);

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);

            void Set(long keyValue, Span<byte> valueSpan, int valueLength, byte tag)
            {
                valueSpan[0] = tag;
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref keyValue), valueSpan.Slice(0, valueLength), Empty.Default);
            }
        }
    }
}