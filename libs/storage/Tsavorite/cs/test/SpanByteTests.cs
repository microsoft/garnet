// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if LOGRECORD_TODO

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.core.Utility;

namespace Tsavorite.test.spanbyte
{
    using SpanByteStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    [TestFixture]
    internal class SpanByteTests
    {
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public unsafe void SpanByteTest1()
        {
            Span<byte> output = stackalloc byte[20];
            SpanByte input = default;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            try
            {
                using var log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "hlog1.log"), deleteOnClose: true);
                using var store = new TsavoriteKV<SpanByte, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>>(
                    new()
                    {
                        IndexSize = 1L << 13,
                        LogDevice = log,
                        MemorySize = 1L << 17,
                        PageSize = 1L << 12
                    }, StoreFunctions<SpanByte, SpanByte>.Create()
                    , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
                );
                using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new SpanByteFunctions<Empty>());
                var bContext = session.BasicContext;

                var key1 = MemoryMarshal.Cast<char, byte>("key1".AsSpan());
                var value1 = MemoryMarshal.Cast<char, byte>("value1".AsSpan());
                var output1 = SpanByteAndMemory.FromPinnedSpan(output);

                fixed (byte* key1Ptr = key1)
                fixed (byte* value1Ptr = value1)
                {
                    var key1SpanByte = SpanByte.FromPinnedPointer(key1Ptr, key1.Length);
                    var value1SpanByte = SpanByte.FromPinnedPointer(value1Ptr, value1.Length);

                    _ = bContext.Upsert(key1SpanByte, value1SpanByte);
                    _ = bContext.Read(ref key1SpanByte, ref input, ref output1);
                }

                ClassicAssert.IsTrue(output1.IsSpanByte);
                ClassicAssert.IsTrue(output1.SpanByte.AsReadOnlySpan().SequenceEqual(value1));

                var key2 = MemoryMarshal.Cast<char, byte>("key2".AsSpan());
                var value2 = MemoryMarshal.Cast<char, byte>("value2value2value2".AsSpan());
                var output2 = SpanByteAndMemory.FromPinnedSpan(output);

                fixed (byte* key2Ptr = key2)
                fixed (byte* value2Ptr = value2)
                {
                    var key2SpanByte = SpanByte.FromPinnedPointer(key2Ptr, key2.Length);
                    var value2SpanByte = SpanByte.FromPinnedPointer(value2Ptr, value2.Length);

                    _ = bContext.Upsert(key2SpanByte, value2SpanByte);
                    _ = bContext.Read(ref key2SpanByte, ref input, ref output2);
                }

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
        public unsafe void MultiRead_SpanByte_Test()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            try
            {
                using var log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "test.log"), deleteOnClose: true);
                using var store = new TsavoriteKV<SpanByte, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>>(
                    new()
                    {
                        IndexSize = 1L << 16,
                        LogDevice = log,
                        MemorySize = 1L << 15,
                        PageSize = 1L << 12
                    }, StoreFunctions<SpanByte, SpanByte>.Create()
                    , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
                );
                using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new SpanByteFunctions<Empty>());
                var bContext = session.BasicContext;

                for (int i = 0; i < 200; i++)
                {
                    var key = MemoryMarshal.Cast<char, byte>($"{i}".AsSpan());
                    var value = MemoryMarshal.Cast<char, byte>($"{i + 1000}".AsSpan());
                    fixed (byte* k = key, v = value)
                        _ = bContext.Upsert(SpanByte.FromPinnedSpan(key), SpanByte.FromPinnedSpan(value));
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

                    var keyBytes = MemoryMarshal.Cast<char, byte>($"{key}".AsSpan());
                    fixed (byte* _ = keyBytes)
                        status = bContext.Read(key: SpanByte.FromPinnedSpan(keyBytes), out output);
                    ClassicAssert.AreEqual(evicted, status.IsPending, "evicted/pending mismatch");

                    if (evicted)
                        (status, output) = bContext.GetSinglePendingResult();
                    ClassicAssert.IsTrue(status.Found, $"expected to find key; status = {status}, pending = {evicted}");

                    ClassicAssert.IsFalse(output.IsSpanByte, "Output should not have a valid SpanByte");
                    var outputString = new string(MemoryMarshal.Cast<byte, char>(output.AsReadOnlySpan()));
                    ClassicAssert.AreEqual(value, long.Parse(outputString), $"outputString mismatch; pending = {evicted}");
                    output.Memory.Dispose();
                }
            }
            finally
            {
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public unsafe void SpanByteUnitTest1()
        {
            Span<byte> payload = stackalloc byte[20];
            Span<byte> serialized = stackalloc byte[24];

            SpanByte sb = SpanByte.FromPinnedSpan(payload);
            ClassicAssert.IsFalse(sb.Serialized);
            ClassicAssert.AreEqual(20, sb.Length);
            ClassicAssert.AreEqual(24, sb.TotalSize);
            ClassicAssert.AreEqual(20, sb.AsSpan().Length);
            ClassicAssert.AreEqual(20, sb.AsReadOnlySpan().Length);

            fixed (byte* ptr = serialized)
                sb.CopyTo(ptr);
            ref SpanByte ssb = ref SpanByte.ReinterpretWithoutLength(serialized);
            ClassicAssert.IsTrue(ssb.Serialized);
            ClassicAssert.AreEqual(0, ssb.MetadataSize);
            ClassicAssert.AreEqual(20, ssb.Length);
            ClassicAssert.AreEqual(24, ssb.TotalSize);
            ClassicAssert.AreEqual(20, ssb.AsSpan().Length);
            ClassicAssert.AreEqual(20, ssb.AsReadOnlySpan().Length);

            ssb.MarkExtraMetadata();
            ClassicAssert.IsTrue(ssb.Serialized);
            ClassicAssert.AreEqual(8, ssb.MetadataSize);
            ClassicAssert.AreEqual(20, ssb.Length);
            ClassicAssert.AreEqual(24, ssb.TotalSize);
            ClassicAssert.AreEqual(20 - 8, ssb.AsSpan().Length);
            ClassicAssert.AreEqual(20 - 8, ssb.AsReadOnlySpan().Length);
            ssb.ExtraMetadata = 31337;
            ClassicAssert.AreEqual(31337, ssb.ExtraMetadata);

            sb.MarkExtraMetadata();
            ClassicAssert.AreEqual(20, sb.Length);
            ClassicAssert.AreEqual(24, sb.TotalSize);
            ClassicAssert.AreEqual(20 - 8, sb.AsSpan().Length);
            ClassicAssert.AreEqual(20 - 8, sb.AsReadOnlySpan().Length);
            sb.ExtraMetadata = 31337;
            ClassicAssert.AreEqual(31337, sb.ExtraMetadata);

            fixed (byte* ptr = serialized)
                sb.CopyTo(ptr);
            ClassicAssert.IsTrue(ssb.Serialized);
            ClassicAssert.AreEqual(8, ssb.MetadataSize);
            ClassicAssert.AreEqual(20, ssb.Length);
            ClassicAssert.AreEqual(24, ssb.TotalSize);
            ClassicAssert.AreEqual(20 - 8, ssb.AsSpan().Length);
            ClassicAssert.AreEqual(20 - 8, ssb.AsReadOnlySpan().Length);
            ClassicAssert.AreEqual(31337, ssb.ExtraMetadata);
        }

        [Test]
        [Category("TsavoriteKV")]
        public unsafe void ShouldSkipEmptySpaceAtEndOfPage()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            using var log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "vl-iter.log"), deleteOnClose: true);
            using var store = new TsavoriteKV<SpanByte, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>>(
                new()
                {
                    IndexSize = 1L << 13,
                    LogDevice = log,
                    MemorySize = 1L << 17,
                    PageSize = 1L << 10    // 1KB page
                }, StoreFunctions<SpanByte, SpanByte>.Create()
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
            using var session = store.NewSession<SpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;

            const int PageSize = 1024;
            Span<long> keySpan = stackalloc long[1];
            var key = keySpan.AsSpanByte();
            Span<byte> valueSpan = stackalloc byte[PageSize];
            var value = valueSpan.AsSpanByte();  // We'll adjust the length below

            Set(ref keySpan, 1L, ref valueSpan, 800, 1);                // Inserted on page#0 and leaves empty space
            Set(ref keySpan, 2L, ref valueSpan, 800, 2);                // Inserted on page#1 because there is not enough space in page#0, and leaves empty space

            // Add a second record on page#1 to fill it exactly. Page#1 starts at offset 0 on the page (unlike page#0, which starts at 24 or 64,
            // depending on data). Subtract the RecordInfo and key space for both the first record and the second record we're about to insert,
            // the value space for the first record, and the length header for the second record. This is the space available for the second record's value.
            var p2value2len = PageSize
                                - 2 * RecordInfo.GetLength()
                                - 2 * RoundUp(key.TotalSize, Constants.kRecordAlignment)
                                - RoundUp(value.TotalSize, Constants.kRecordAlignment)
                                - sizeof(int);
            Set(ref keySpan, 3L, ref valueSpan, p2value2len, 3);        // Inserted on page#1
            ClassicAssert.AreEqual(PageSize * 2, store.Log.TailAddress, "TailAddress should be at the end of page#2");

            Set(ref keySpan, 4L, ref valueSpan, 64, 4);                 // Inserted on page#2

            var data = new List<(long, int, int)>();
            using (var iterator = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress))
            {
                while (iterator.GetNext(out var info))
                {
                    var scanKey = iterator.GetKey().AsSpan<long>();
                    var scanValue = iterator.GetValue().AsSpan<byte>();

                    data.Add((scanKey[0], scanValue.Length, scanValue[0]));
                }
            }

            ClassicAssert.AreEqual(4, data.Count);

            ClassicAssert.AreEqual((1L, 800, 1), data[0]);
            ClassicAssert.AreEqual((2L, 800, 2), data[1]);
            ClassicAssert.AreEqual((3L, p2value2len, 3), data[2]);
            ClassicAssert.AreEqual((4L, 64, 4), data[3]);

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);

            void Set(ref Span<long> keySpan, long keyValue, ref Span<byte> valueSpan, int valueLength, byte tag)
            {
                keySpan[0] = keyValue;
                value.Length = valueLength;
                valueSpan[0] = tag;
                _ = bContext.Upsert(ref key, ref value, Empty.Default);
            }
        }
    }
}

#endif // LOGRECORD_TODO