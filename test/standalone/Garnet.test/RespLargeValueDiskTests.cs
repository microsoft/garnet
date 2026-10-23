// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Regression tests for flushing large overflow string values to the object log on disk: a large overflow value must round-trip
    /// when its record is evicted/flushed to the object log, and read back byte-for-byte from disk, including on the native
    /// (O_DIRECT) device, which enforces alignment.
    /// </summary>
    [TestFixture]
    public class RespLargeValueDiskTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        // Sizes chosen to exercise: a value well under one 4 MB object-log chunk, values just over and at a multiple of the chunk
        // size, and a non-sector-multiple value spanning three chunks.
        [Test]
        [TestCase(256 * 1024)]
        [TestCase(4 * 1024 * 1024 + 512)]
        [TestCase(8 * 1024 * 1024)]
        [TestCase(8 * 1024 * 1024 + 7)]
        public async Task LargeValueFlushAndReadBack(int valueSize)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var iServer = redis.GetServer(TestUtils.EndPoint);

            var value = new byte[valueSize];
            for (var i = 0; i < valueSize; i++)
                value[i] = (byte)(i * 31 + 7);

            RedisKey key = "large-key";
            ClassicAssert.IsTrue(db.StringSet(key, value));

            // Insert filler and wait until the large value's record (and its overflow value) is flushed to
            // the object log on disk. On the default Linux device this write uses O_DIRECT (true alignment
            // enforcement), so a misaligned write would fail, the read-only flush could not advance, the
            // low-memory resizer would spin waiting for it, and this test would hang until the suite timeout.
            var target = TestUtils.GetStoreAddressInfo(iServer).TailAddress;
            await TestUtils.FlushAndWaitForStoreAsync(db, iServer, target, timeoutMs: 30000).ConfigureAwait(false);

            // Read the value back (served from disk) and verify byte-for-byte.
            var read = (byte[])db.StringGet(key);
            ClassicAssert.IsNotNull(read);
            ClassicAssert.AreEqual(valueSize, read.Length);
            ClassicAssert.IsTrue(value.AsSpan().SequenceEqual(read), "Value mismatch after disk round-trip");
        }

        /// <summary>
        /// Writes "shim" overflow records of varying size before each large overflow value so that the large
        /// values begin at a spread of object-log offsets, then flushes everything to the object log and reads
        /// every record back byte-for-byte from disk.
        /// </summary>
        /// <remarks>
        /// The object-log writer cuts a record's bytes into chunks, each within one object-log page, and cuts a chunk
        /// at a page end when the page's remaining space is worth filling. Where those cuts fall depends on the log
        /// tail's offset when the value is written. Any string value larger than the &lt;= 4 KB inline limit becomes
        /// object-log overflow, so the "shim" records walk the tail across a range of offsets, including offsets near
        /// a sector end. The large values cover lengths staged through the writer's chunk buffer and lengths at and
        /// around the 4 MB chunk size, which a single chunk cannot hold.
        /// </remarks>
        [Test]
        public async Task LargeValueFlushWithPrecedingSectorCombinations()
        {
            const int OverflowBase = 8 * 1024;         // > the <= 4 KB inline limit, so shims are object-log overflow
            const int MidLargeBase = 128 * 1024;       // well under one object-log chunk
            const int ChunkSize = 4 * 1024 * 1024;     // IStreamBuffer.BufferSize, the object-log writer's default chunk size

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var iServer = redis.GetServer(TestUtils.EndPoint);

            // Sub-sector shim sizes sweep a full sector (residues incl. 0/1/near-511) so successive large values
            // start at varying offsets.
            int[] shims = [0, 1, 7, 31, 63, 64, 65, 127, 255, 256, 384, 511];
            // Cheap "large" values staged through the chunk buffer, with sector-multiple and off-by-one lengths.
            int[] smallLarge = [MidLargeBase + 512, MidLargeBase + 1, MidLargeBase + 511, 192 * 1024];
            // A few values at and around the chunk size (sector-multiple and non-multiple lengths).
            int[] bufferBoundaryLarge = [ChunkSize, ChunkSize + 512, ChunkSize + 511, ChunkSize - 1];

            var records = new List<(RedisKey key, int seed, int size)>();
            var seed = 0;
            for (var i = 0; i < shims.Length; i++)
            {
                records.Add(($"shim-{seed}", seed, OverflowBase + shims[i]));
                seed++;
                records.Add(($"big-{seed}", seed, smallLarge[i % smallLarge.Length]));
                seed++;
            }
            for (var i = 0; i < bufferBoundaryLarge.Length; i++)
            {
                records.Add(($"bshim-{seed}", seed, OverflowBase + shims[i % shims.Length]));
                seed++;
                records.Add(($"bbig-{seed}", seed, bufferBoundaryLarge[i]));
                seed++;
            }

            foreach (var (key, recordSeed, size) in records)
                ClassicAssert.IsTrue(db.StringSet(key, MakeValue(recordSeed, size)));

            // Flush everything (through the tail) to the object log on disk and wait for it to become durable.
            var target = TestUtils.GetStoreAddressInfo(iServer).TailAddress;
            await TestUtils.FlushAndWaitForStoreAsync(db, iServer, target, timeoutMs: 60000).ConfigureAwait(false);

            // Verify every record round-trips byte-for-byte (served from disk), so a misaligned/split write that
            // corrupted or dropped bytes for any near-sector-end combination is caught.
            foreach (var (key, recordSeed, size) in records)
            {
                var read = (byte[])db.StringGet(key);
                ClassicAssert.IsNotNull(read, $"null read for {key}");
                ClassicAssert.AreEqual(size, read.Length, $"length mismatch for {key}");
                ClassicAssert.IsTrue(MakeValue(recordSeed, size).AsSpan().SequenceEqual(read), $"content mismatch for {key}");
            }
        }

        /// <summary>Builds a deterministic value of <paramref name="size"/> bytes whose content depends on
        /// <paramref name="seed"/>, so a cross-record mixup (wrong bytes served) is detected on read-back.</summary>
        private static byte[] MakeValue(int seed, int size)
        {
            var value = new byte[size];
            for (var i = 0; i < size; i++)
                value[i] = (byte)((i * 31 + 7) ^ (seed * 131 + 17));
            return value;
        }
    }
}