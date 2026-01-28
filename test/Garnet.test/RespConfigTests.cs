// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Test dynamically changing server configuration using CONFIG SET command.
    /// </summary>
    [TestFixture(false)]
    [TestFixture(true)]
    public class RespConfigTests
    {
        GarnetServer server;
        private readonly string memorySizeStr = "17g";
        private readonly string indexSizeStr = "64m";
        private readonly string pageSizeStr = "32m";
        private readonly bool useReviv;

        // The HLOG will always have at least two pages allocated.
        const int MinLogAllocatedPageCount = 2;

        public RespConfigTests(bool useReviv)
        {
            this.useReviv = useReviv;
        }

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                memorySize: memorySizeStr,
                indexSize: indexSizeStr,
                pageSize: pageSizeStr,
                useReviv: useReviv);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        /// <summary>
        /// This test verifies that dynamically changing the memory size configuration using CONFIG SET memory
        /// incurs the expected changes in Garnet server metrics, as well as verifies error handling for incorrect inputs.
        /// </summary>
        /// <param name="smallerSize">Memory size smaller than the initial size</param>
        /// <param name="largerSize">Memory size larger than the initial size (within buffer bounds)</param>
        /// <param name="largerThanBufferSize">Memory size larger than the buffer size</param>
        /// <param name="malformedSize">Malformed memory size string</param>
        /// <remarks>Initial memory size for main log is 32GB</remarks>
        [Test]
        [TestCase("16g", "32g", "64g", "g4")]
        [TestCase("9gB", "28GB", "33G", "2gBB")]
        [TestCase("16m", "32m", "256GB", "3bm")]
        [TestCase("5MB", "30M", "128GB", "44d")]
        public void ConfigSetMemorySizeTest(string smallerSize, string largerSize, string largerThanBufferSize, string malformedSize)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var option = "memory";
            var metricName = "Log.AllocatedPageCount";
            var metricType = InfoMetricsType.STORE;
            var initMemorySize = memorySizeStr;

            var store = server.Provider.StoreWrapper.store;
            var currMemorySize = ServerOptions.ParseSize(initMemorySize, out _);
            var pageSize = ServerOptions.ParseSize(pageSizeStr, out _);

            var bufferSizeInBytes = ServerOptions.NextPowerOf2(currMemorySize);
            Assert.That(bufferSizeInBytes / pageSize, Is.EqualTo(store.Log.BufferSize));

            // Check initial AllocatedPageCount before any changes
            var metrics = server.Metrics.GetInfoMetrics(metricType);
            var miAPC = metrics.FirstOrDefault(mi => mi.Name == metricName);
            ClassicAssert.IsNotNull(miAPC);
            ClassicAssert.IsTrue(long.TryParse(miAPC.Value, out var allocatedPageCount));
            long expectedAPC = MinLogAllocatedPageCount;
            ClassicAssert.AreEqual(expectedAPC, allocatedPageCount);

            // Try to set memory size to the same value as current
            var result = db.Execute("CONFIG", "SET", option, initMemorySize);
            ClassicAssert.AreEqual("OK", result.ToString());

            // miAPC should remain unchanged
            metrics = server.Metrics.GetInfoMetrics(metricType);
            miAPC = metrics.FirstOrDefault(mi => mi.Name == metricName);
            ClassicAssert.IsNotNull(miAPC);
            ClassicAssert.IsTrue(long.TryParse(miAPC.Value, out allocatedPageCount));
            ClassicAssert.AreEqual(expectedAPC, allocatedPageCount);

            // Try to set memory size to a smaller value than current
            result = db.Execute("CONFIG", "SET", option, smallerSize);
            ClassicAssert.AreEqual("OK", result.ToString());

            // Check that miAPC has changed accordingly
            currMemorySize = ServerOptions.ParseSize(smallerSize, out _);
            metrics = server.Metrics.GetInfoMetrics(metricType);
            miAPC = metrics.FirstOrDefault(mi => mi.Name == metricName);
            ClassicAssert.IsNotNull(miAPC);
            ClassicAssert.IsTrue(long.TryParse(miAPC.Value, out allocatedPageCount));
            expectedAPC = currMemorySize / pageSize;
            ClassicAssert.AreEqual(expectedAPC, allocatedPageCount);

            // Try to set memory size to a larger value than current
            result = db.Execute("CONFIG", "SET", option, largerSize);
            ClassicAssert.AreEqual("OK", result.ToString());

            // Check that miAPC has changed accordingly
            currMemorySize = ServerOptions.ParseSize(largerSize, out _);
            metrics = server.Metrics.GetInfoMetrics(metricType);
            miAPC = metrics.FirstOrDefault(mi => mi.Name == metricName);
            ClassicAssert.IsNotNull(miAPC);
            ClassicAssert.IsTrue(long.TryParse(miAPC.Value, out allocatedPageCount));
            expectedAPC = currMemorySize / pageSize;
            ClassicAssert.AreEqual(expectedAPC, allocatedPageCount);

            // Try to set memory size larger than the buffer size - this should fail
            _ = Assert.Throws<RedisServerException>(() => db.Execute("CONFIG", "SET", option, largerThanBufferSize),
                string.Format(CmdStrings.GenericErrMemorySizeGreaterThanBuffer, option));

            // miAPC should remain unchanged
            metrics = server.Metrics.GetInfoMetrics(metricType);
            miAPC = metrics.FirstOrDefault(mi => mi.Name == metricName);
            ClassicAssert.IsNotNull(miAPC);
            ClassicAssert.IsTrue(long.TryParse(miAPC.Value, out allocatedPageCount));
            ClassicAssert.AreEqual(expectedAPC, allocatedPageCount);

            // Try to set memory size with a malformed size input - this should fail
            _ = Assert.Throws<RedisServerException>(() => db.Execute("CONFIG", "SET", option, malformedSize),
                string.Format(CmdStrings.GenericErrIncorrectSizeFormat, option));

            // miAPC should remain unchanged
            metrics = server.Metrics.GetInfoMetrics(metricType);
            miAPC = metrics.FirstOrDefault(mi => mi.Name == metricName);
            ClassicAssert.IsNotNull(miAPC);
            ClassicAssert.IsTrue(long.TryParse(miAPC.Value, out allocatedPageCount));
            ClassicAssert.AreEqual(expectedAPC, allocatedPageCount);
        }

        /// <summary>
        /// This test verifies that dynamically changing the index size configuration using CONFIG SET index / obj-index
        /// incurs the expected changes in Garnet server metrics, as well as verifies error handling for incorrect inputs.
        /// </summary>
        /// <param name="smallerSize">Index size smaller than the initial size</param>
        /// <param name="largerSize">Index size larger than the initial size</param>
        /// <param name="illegalSize">Illegal index size (not a power of 2)</param>
        /// <param name="malformedSize">Malformed index size string</param>
        /// <remarks>Initial index size for main log is 1MB</remarks>
        [Test]
        [TestCase("32m", "128m", "63m", "8d")]
        [TestCase("16mB", "256MB", "23m", "g8")]
        [TestCase("2m", "512m", "28m", "m9")]
        [TestCase("4Mb", "1024mB", "129MB", "0.3gb")]
        public void ConfigSetIndexSizeTest(string smallerSize, string largerSize, string illegalSize, string malformedSize)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var metricType = InfoMetricsType.STORE;
            var option = "index";
            var initIndexSize = indexSizeStr;

            // Check initial index size before any changes
            var currIndexSize = ServerOptions.ParseSize(initIndexSize, out _);
            var metrics = server.Metrics.GetInfoMetrics(metricType);
            var miIndexSize = metrics.FirstOrDefault(mi => mi.Name == "IndexMemorySize");
            ClassicAssert.IsNotNull(miIndexSize);
            ClassicAssert.IsTrue(long.TryParse(miIndexSize.Value, out var actualIndexSize));
            var expectedIndexSize = currIndexSize / 64;
            ClassicAssert.AreEqual(expectedIndexSize, actualIndexSize);

            // Try to set index size to the same value as current
            var result = db.Execute("CONFIG", "SET", option, initIndexSize);
            ClassicAssert.AreEqual("OK", result.ToString());

            // Index size should remain unchanged
            metrics = server.Metrics.GetInfoMetrics(metricType);
            miIndexSize = metrics.FirstOrDefault(mi => mi.Name == "IndexMemorySize");
            ClassicAssert.IsNotNull(miIndexSize);
            ClassicAssert.IsTrue(long.TryParse(miIndexSize.Value, out actualIndexSize));
            ClassicAssert.AreEqual(expectedIndexSize, actualIndexSize);

            // Try to set index size to a larger value than current
            result = db.Execute("CONFIG", "SET", option, largerSize);
            ClassicAssert.AreEqual("OK", result.ToString());

            // Check that index size has changed accordingly
            currIndexSize = ServerOptions.ParseSize(largerSize, out _);
            metrics = server.Metrics.GetInfoMetrics(metricType);
            miIndexSize = metrics.FirstOrDefault(mi => mi.Name == "IndexMemorySize");
            ClassicAssert.IsNotNull(miIndexSize);
            ClassicAssert.IsTrue(long.TryParse(miIndexSize.Value, out actualIndexSize));
            expectedIndexSize = currIndexSize / 64;
            ClassicAssert.AreEqual(expectedIndexSize, actualIndexSize);

            // Try to set index size to a smaller value than current - this should fail
            _ = Assert.Throws<RedisServerException>(() => db.Execute("CONFIG", "SET", option, smallerSize),
                string.Format(CmdStrings.GenericErrIndexSizeSmallerThanCurrent, option));

            // Try to set index size to a value that is not a power of two - this should fail
            _ = Assert.Throws<RedisServerException>(() => db.Execute("CONFIG", "SET", option, illegalSize),
                string.Format(CmdStrings.GenericErrIndexSizePowerOfTwo, option));

            // Try to set index size with a malformed size input - this should fail
            _ = Assert.Throws<RedisServerException>(() => db.Execute("CONFIG", "SET", option, malformedSize),
                string.Format(CmdStrings.GenericErrIncorrectSizeFormat, option));
        }
    }

    /// <summary>
    /// Test memory utilization behavior when dynamically changing the memory size configuration using CONFIG SET memory.
    /// </summary>
    [TestFixture(false)]
    [TestFixture(true)]
    public class RespConfigUtilizationTests
    {
        GarnetServer server;
        private readonly string memorySize = "3m";
        private readonly string indexSize = "1m";
        private readonly string pageSize = "1024";
        private readonly bool useReviv;

        public RespConfigUtilizationTests(bool useReviv)
        {
            this.useReviv = useReviv;
        }

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(null,
                memorySize: memorySize,
                indexSize: indexSize,
                pageSize: pageSize,
                useReviv: useReviv);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        /// <summary>
        /// This test verifies that dynamically changing the memory size configuration using CONFIG SET
        /// incurs the expected shifts in the head and tail addresses of the store.
        /// </summary>
        /// <param name="smallerSize">Memory size smaller than the initial size</param>
        /// <param name="largerSize">Memory size larger than the initial size (within buffer bounds)</param>
        [Test]
        [TestCase("1m", "4m")]
        [TestCase("1024k", "4000k")]
        [TestCase("1024", "4000")]
        [TestCase("1024", "4096")]
        public void ConfigSetInlineMemorySizeUtilizationTest(string smallerSize, string largerSize)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var option = "memory";
            var currMemorySize = TestUtils.GetEffectiveMemorySize(memorySize, pageSize, out var parsedPageSize);
            var initialMemorySize = currMemorySize;

            var store = server.Provider.StoreWrapper.store;
            var tracker = store.Log.LogSizeTracker;
            Assert.That(tracker.TargetSize, Is.EqualTo(currMemorySize));

            var garnetServer = redis.GetServer(TestUtils.EndPoint);
            var info = TestUtils.GetStoreAddressInfo(garnetServer);
            ClassicAssert.AreEqual(PageHeader.Size, info.TailAddress);

            var i = 0;
            var val = new RedisValue(new string('x', 512 - 32));

            // Insert records until head address moves
            var prevHead = info.HeadAddress;
            var prevTail = info.TailAddress;
            while (info.HeadAddress == prevHead)
            {
                var key = $"key{i++:00000}";
                _ = db.StringSet(key, val);

                prevHead = info.HeadAddress;
                prevTail = info.TailAddress;
                info = TestUtils.GetStoreAddressInfo(garnetServer);
            }

            prevHead = info.HeadAddress;
            prevTail = info.TailAddress;

            // Verify that records were inserted up to the configured memory size limit.
            // We may have overflowed by multiple pages.
            Assert.That(prevTail - prevHead, Is.LessThanOrEqualTo(tracker.TargetDeltaRange.high));

            ////////////////////////////////////////////////////////
            // Try to set memory size to a smaller value than current
            currMemorySize = TestUtils.GetEffectiveMemorySize(smallerSize, pageSize, out _);
            Assert.That(currMemorySize, Is.LessThan(initialMemorySize));
            var result = db.Execute("CONFIG", "SET", option, smallerSize);
            ClassicAssert.AreEqual("OK", result.ToString());
            Assert.That(tracker.TargetSize, Is.EqualTo(currMemorySize));

            // Insert records until head address moves
            prevHead = info.HeadAddress;
            prevTail = info.TailAddress;

            // Precondition: We have too much in memory for the smallSize and must evict.
            Assert.That(prevTail - prevHead, Is.GreaterThan(tracker.TargetDeltaRange.high));

            // Wait for the head address to move forward. This may be done in iterations because the ReadOnlyAddress
            // may block the first page-eviction loop.
#if true
            while (true)
            {
                Thread.Sleep(1000);
                if (info.HeadAddress > prevHead && (prevTail - prevHead) <= tracker.TargetDeltaRange.high)
                    break;

                prevHead = info.HeadAddress;
                prevTail = info.TailAddress;
                info = TestUtils.GetStoreAddressInfo(garnetServer);
            }
#else
            while (info.HeadAddress == prevHead)
            {
                var key = $"key{i++:00000}";
                _ = db.StringSet(key, val);

                prevHead = info.HeadAddress;
                prevTail = info.TailAddress;
                info = TestUtils.GetStoreAddressInfo(garnetServer);
            }
#endif
            prevHead = info.HeadAddress;
            prevTail = info.TailAddress;

            // Verify that records were inserted up to the configured memory size limit.
            // We may have overflowed by multiple pages.
            Assert.That(prevTail - prevHead, Is.LessThanOrEqualTo(tracker.TargetDeltaRange.high));

            ////////////////////////////////////////////////////////
            // Try to set memory size to a larger value than current
            currMemorySize = TestUtils.GetEffectiveMemorySize(largerSize, pageSize, out _);
            Assert.That(currMemorySize, Is.GreaterThan(initialMemorySize));
            result = db.Execute("CONFIG", "SET", option, largerSize);
            ClassicAssert.AreEqual("OK", result.ToString());
            Assert.That(tracker.TargetSize, Is.EqualTo(currMemorySize));

            // Continue to insert records until new memory capacity is reached
            prevHead = info.HeadAddress;
            prevTail = info.TailAddress;
            while (info.HeadAddress == prevHead)
            {
                var key = $"key{i++:00000}";
                _ = db.StringSet(key, val);

                prevHead = info.HeadAddress;
                prevTail = info.TailAddress;
                info = TestUtils.GetStoreAddressInfo(garnetServer);
            }

            // Verify that memory is fully utilized and within memory bounds
            Assert.That(prevTail - prevHead, Is.LessThanOrEqualTo(tracker.TargetDeltaRange.high));
        }

        /// <summary>
        /// This test verifies recovery behavior after dynamically changing the memory size configuration using CONFIG SET memory.
        /// The test fills the store to a larger capacity than the initial memory size, then verifies that recovering with the
        /// smaller initial memory size retains the last inserted keys in the expected initial capacity.
        /// </summary>
        /// <param name="largerSize">Memory size larger than the initial size (within buffer bounds)</param>
        [Test]
        [TestCase("4m")]
        public void ConfigSetMemorySizeRecoveryTest(string largerSize)
        {
            var option = "memory";
            var initMemorySize = memorySize;

            var currMemorySize = TestUtils.GetEffectiveMemorySize(initMemorySize, pageSize, out var parsedPageSize);

            int lastIdxSecondRound;
            int keysInsertedFirstRound;

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                var garnetServer = redis.GetServer(TestUtils.EndPoint);
                var info = TestUtils.GetStoreAddressInfo(garnetServer);
                ClassicAssert.AreEqual(PageHeader.Size, info.TailAddress);

                // Insert records until head address moves. We want to fit two records per page; pages are 1024 bytes so after subtracting
                // PageHeader.Size we have 960 / 2 = 480 bytes per record. Keys are 8 bytes, valueLength requires 2 bytes as it will be
                // more than 255, we have no optionals (ETag or Expiration), and we are inline so have no ObjectLogPosition, so:
                //   RecordInfo.Size + (MinLengthMetadataBytes + 1) + 8 + valueLength = 480, so valueLength = 480-22 = 458 bytes.
                // It's rounded up to kRecordAlignment (8) anyway.
                var val = new RedisValue(new string('x', 458));

                var i = 0;
                var prevHead = info.HeadAddress;
                var prevTail = info.TailAddress;
                while (info.HeadAddress == prevHead)
                {
                    var key = $"key{i++:00000}";
                    _ = db.StringSet(key, val);

                    prevHead = info.HeadAddress;
                    prevTail = info.TailAddress;
                    info = TestUtils.GetStoreAddressInfo(garnetServer);
                }

                var lastIdxFirstRound = i - 1;

                // Verify that records were inserted up to the configured memory size limit
                Assert.That(prevTail, Is.LessThanOrEqualTo(currMemorySize));
                Assert.That(currMemorySize - prevTail, Is.LessThanOrEqualTo(parsedPageSize));

                // Find the first key index that still exists in the server
                ClassicAssert.IsTrue(db.KeyExists($"key{lastIdxFirstRound:00000}"));
                var c = lastIdxFirstRound;
                while (c > 0 && db.KeyExists($"key{--c:00000}"))
                    continue;

                // Record the number of keys inserted in the first round
                keysInsertedFirstRound = lastIdxFirstRound + 1 - c;

                // Try to set memory size to a larger value than current
                var result = db.Execute("CONFIG", "SET", option, largerSize);
                ClassicAssert.AreEqual("OK", result.ToString());

                currMemorySize = TestUtils.GetEffectiveMemorySize(largerSize, pageSize, out _);

                // Continue to insert records until new memory capacity is reached
                prevHead = info.HeadAddress;
                prevTail = info.TailAddress;
                while (info.HeadAddress == prevHead)
                {
                    var key = $"key{i++:00000}";
                    _ = db.StringSet(key, val);

                    prevHead = info.HeadAddress;
                    prevTail = info.TailAddress;
                    info = TestUtils.GetStoreAddressInfo(garnetServer);
                }

                lastIdxSecondRound = i - 1;

                // Verify that memory is fully utilized
                Assert.That(prevTail - prevHead, Is.LessThanOrEqualTo(currMemorySize));
                Assert.That(currMemorySize - (prevTail - prevHead), Is.LessThanOrEqualTo(parsedPageSize));

                // SAVE and wait for completion
                garnetServer.Save(SaveType.BackgroundSave);
                while (garnetServer.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks)
                    Thread.Sleep(10);
            }

            // Restart server with initial memory size and recover data
            server.Dispose(deleteDir: false);
            server = TestUtils.CreateGarnetServer(null,
                memorySize: memorySize,
                indexSize: indexSize,
                pageSize: pageSize,
                useReviv: useReviv,
                tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                // Find the smallest key index that still exists in the server
                var c = lastIdxSecondRound;
                while (c > 0 && db.KeyExists($"key{--c:00000}"))
                    continue;

                // Verify that the number of existing keys matches the count of inserted keys in the first round of insertions
                ClassicAssert.AreEqual(keysInsertedFirstRound, lastIdxSecondRound + 1 - c);

                // Verify that all previous keys are not present in the database
                while (c > 0)
                    ClassicAssert.IsFalse(db.KeyExists($"key{--c:00000}"));
            }
        }
    }

    /// <summary>
    /// Test memory utilization behavior when dynamically changing the memory size configuration using CONFIG SET.
    /// </summary>
    [TestFixture(false)]
    [TestFixture(true)]
    public class RespConfigIndexUtilizationTests
    {
        GarnetServer server;
        private readonly string memorySize = "3m";
        private readonly string indexSize = "512";
        private readonly string pageSize = "1024";
        private readonly bool useReviv;

        public RespConfigIndexUtilizationTests(bool useReviv)
        {
            this.useReviv = useReviv;
        }

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(null,
                memorySize: memorySize,
                indexSize: indexSize,
                pageSize: pageSize,
                useReviv: useReviv);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        /// <summary>
        /// This test verifies that dynamically changing the index size configuration using CONFIG SET
        /// incurs the expected shifts in the overflow buckets of the store, and that no data is lost in the process.
        /// </summary>
        /// <param name="largerSize1">Larger index size than configured</param>
        /// <param name="largerSize2">Larger index size than previous</param>
        [Test]
        [TestCase("1024", "4096")]
        public void ConfigSetIndexSizeUtilizationTest(string largerSize1, string largerSize2)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var option = "index";
            var parsedIndexSize = ServerOptions.ParseSize(indexSize, out _);

            var currIndexSize = server.Provider.StoreWrapper.store.IndexSize;

            // Verify initial index size and overflow bucket allocations are zero
            ClassicAssert.AreEqual(parsedIndexSize / 64, currIndexSize);
            ClassicAssert.AreEqual(0, GetOverflowBucketAllocations());

            // Generate data with random keys (so that hashtable overflows)
            var val = new RedisValue("x");
            var keys = new string[500];
            for (var i = 0; i < keys.Length; i++)
                keys[i] = TestUtils.GetRandomString(8);

            // Insert first batch of data
            for (var i = 0; i < 250; i++)
                _ = db.StringSet(keys[i], val);

            // Verify that overflow bucket allocations are non-zero after initial insertions
            var currOverflowBucketAllocations = GetOverflowBucketAllocations();
            ClassicAssert.Greater(currOverflowBucketAllocations, 0);
            var prevOverflowBucketAllocations = currOverflowBucketAllocations;

            // Try to set index size to a larger value than current
            var result = db.Execute("CONFIG", "SET", option, largerSize1);
            ClassicAssert.AreEqual("OK", result.ToString());

            // Verify that overflow bucket allocations have decreased
            currOverflowBucketAllocations = GetOverflowBucketAllocations();
            ClassicAssert.Less(currOverflowBucketAllocations, prevOverflowBucketAllocations);

            // Insert second batch of data
            for (var i = 250; i < 500; i++)
                _ = db.StringSet(keys[i], val);

            prevOverflowBucketAllocations = GetOverflowBucketAllocations();

            // Try to set index size to a larger value than current
            result = db.Execute("CONFIG", "SET", option, largerSize2);
            ClassicAssert.AreEqual("OK", result.ToString());

            // Verify that overflow bucket allocations have decreased again
            currOverflowBucketAllocations = GetOverflowBucketAllocations();
            ClassicAssert.Less(currOverflowBucketAllocations, prevOverflowBucketAllocations);

            // Verify that all keys still exist in the database
            foreach (var key in keys)
                ClassicAssert.IsTrue(db.KeyExists(key));

            long GetOverflowBucketAllocations() =>
                server.Provider.StoreWrapper.store.OverflowBucketAllocations;
        }
    }

    /// <summary>
    /// Test memory utilization behavior when dynamically changing the memory size configuration using CONFIG SET memory.
    /// </summary>
    [TestFixture(false)]
    [TestFixture(true)]
    public class RespConfigHeapUtilizationTests
    {
        GarnetServer server;
        private readonly string memorySize = "3m";
        private readonly string indexSize = "512";
        private readonly string pageSize = "1024";
        private readonly bool useReviv;

        public RespConfigHeapUtilizationTests(bool useReviv)
        {
            this.useReviv = useReviv;
        }

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(null,
                memorySize: memorySize,
                indexSize: indexSize,
                pageSize: pageSize,
                useReviv: useReviv);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        /// <summary>
        /// This test verifies that dynamically changing the object store heap size configuration using CONFIG SET
        /// incurs a reduction in the empty page count of the object store.
        /// </summary>
        /// <param name="largerSize">Heap size larger than configured size</param>
        [Test]
        [TestCase("8192")]
        public void ConfigSetHeapMemorySizeUtilizationTest(string largerSize)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var option = "memory";

            // Verify that initial empty page count is zero
            var store = server.Provider.StoreWrapper.store;
            var initialApc = 3 * 1024;  // Based on initial config, AllocatedPageCount should be this
            ClassicAssert.AreEqual(initialApc, store.Log.AllocatedPageCount);

            // Add objects to store to fill up heap
            var values = new RedisValue[16];
            for (var i = 0; i < values.Length; i++)
                values[i] = "x";

            for (var i = 0; i < 8; i++)
            {
                var key = $"key{i++:00000}";
                _ = db.ListRightPush(key, values);
            }

            // Wait for log size tracker
            var sizeTrackerDelay = TimeSpan.FromSeconds(LogSizeTracker<StoreFunctions, StoreAllocator>.ResizeTaskDelaySeconds + 2);
            Thread.Sleep(sizeTrackerDelay);

            // Verify that allocated page count has decreased
            ClassicAssert.Less(store.Log.AllocatedPageCount, initialApc);
            var prevApc = store.Log.AllocatedPageCount;

            // TODO verify that the HeadAddress has moved

            // Try to set heap size to a larger value than current
            var result = db.Execute("CONFIG", "SET", option, largerSize);
            ClassicAssert.AreEqual("OK", result.ToString());

            // Wait for log size tracker
            Thread.Sleep(sizeTrackerDelay);

            // Verify that empty page count has decreased
            ClassicAssert.Less(store.Log.AllocatedPageCount, prevApc);
        }
    }
}