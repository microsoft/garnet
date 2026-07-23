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
    /// Regression tests for flushing large overflow string values to the object log on disk.
    /// Reproduces the scenario that crashed a large SET on the native (O_DIRECT) device: a large
    /// overflow value must round-trip through the sector-aligned buffered write path when its record
    /// is evicted/flushed to the object log, and read back byte-for-byte from disk.
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

        // Sizes chosen to exercise: just over the 128 KB direct-write threshold, a multiple of the
        // 4 MB object-log flush buffer, an 8 MB value, and a non-sector-multiple large value.
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
            // enforcement), so if the buggy direct-DMA path is taken the misaligned write fails, the
            // read-only flush cannot advance, the low-memory resizer spins waiting for it, and this test
            // hangs (a regression trips the suite timeout). With the fix the flush completes and the value
            // is served from disk.
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
        /// values begin at a spread of offsets relative to the 512-byte device sector boundary, then flushes
        /// everything to the object log and reads every record back byte-for-byte from disk.
        /// </summary>
        /// <remarks>
        /// The object-log write buffer flushes on sector boundaries. Whether a large overflow value needs a
        /// leading sector-aligning fragment, a trailing fragment, and/or crosses one or more 4 MB flush buffers
        /// depends on where the buffer position sits (mod the sector size) when the value is written. Any string
        /// value larger than the &lt;= 4 KB inline limit becomes object-log overflow, so the sub-sector "shim"
        /// records walk the buffer position across a full sector, producing various near-sector-end combinations.
        /// This hardens the currently-active buffered write path and pre-stages coverage for re-enabling the
        /// direct-DMA write path (see <c>EnableDirectObjectLogWrite</c>), whose start/interior/end fragment split
        /// is driven by exactly these offsets.
        /// </remarks>
        [Test]
        public async Task LargeValueFlushWithPrecedingSectorCombinations()
        {
            const int OverflowBase = 8 * 1024;         // > the <= 4 KB inline limit, so shims are object-log overflow
            const int DirectThreshold = 128 * 1024;    // MaxCopySpanLen: values above this take the direct-DMA path when enabled
            const int FlushBuffer = 4 * 1024 * 1024;   // IStreamBuffer.BufferSize (object-log write buffer)

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var iServer = redis.GetServer(TestUtils.EndPoint);

            // Sub-sector shim sizes sweep a full sector (residues incl. 0/1/near-511) so successive large values
            // start at varying offsets relative to the sector boundary.
            int[] shims = [0, 1, 7, 31, 63, 64, 65, 127, 255, 256, 384, 511];
            // Cheap "large" values just over the direct-write threshold, with sector-multiple and off-by-one lengths.
            int[] smallLarge = [DirectThreshold + 512, DirectThreshold + 1, DirectThreshold + 511, 192 * 1024];
            // A few multi-buffer / buffer-boundary values (sector-multiple and non-multiple lengths).
            int[] bufferBoundaryLarge = [FlushBuffer, FlushBuffer + 512, FlushBuffer + 511, FlushBuffer - 1];

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