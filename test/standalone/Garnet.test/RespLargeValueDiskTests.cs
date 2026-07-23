// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
    }
}