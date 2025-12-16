// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading;
using Allure.NUnit;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    /// <summary>
    /// Test dynamically changing server configuration using CONFIG SET command.
    /// </summary>
    [AllureNUnit]
    [TestFixture(false)]
    [TestFixture(true)]
    public class RespConfigTests : AllureTestBase
    {
        GarnetServer server;
        private string memorySize = "17g";
        private string indexSize = "64m";
        private string objectStoreLogMemorySize = "17m";
        private string objectStoreHeapMemorySize = "32m";
        private string objectStoreIndexSize = "8m";
        private bool useReviv;

        public RespConfigTests(bool useReviv)
        {
            this.useReviv = useReviv;
        }

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                memorySize: memorySize,
                indexSize: indexSize,
                objectStoreLogMemorySize: objectStoreLogMemorySize,
                objectStoreIndexSize: objectStoreIndexSize,
                objectStoreHeapMemorySize: objectStoreHeapMemorySize,
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
        /// This test verifies that dynamically changing the memory size configuration using CONFIG SET memory / obj-log-memory
        /// incurs the expected changes in Garnet server metrics, as well as verifies error handling for incorrect inputs.
        /// </summary>
        /// <param name="storeType">Store type (Main / Object)</param>
        /// <param name="smallerSize">Memory size smaller than the initial size</param>
        /// <param name="largerSize">Memory size larger than the initial size (within buffer bounds)</param>
        /// <param name="largerThanBufferSize">Memory size larger than the buffer size</param>
        /// <param name="malformedSize">Malformed memory size string</param>
        [Test]
        [TestCase(StoreType.Main, "16g", "32g", "64g", "g4")]
        [TestCase(StoreType.Main, "9gB", "28GB", "33G", "2gBB")]
        [TestCase(StoreType.Object, "16m", "32m", "64m", "3bm")]
        [TestCase(StoreType.Object, "5MB", "30M", "128mb", "44d")]
        public void ConfigSetMemorySizeTest(StoreType storeType, string smallerSize, string largerSize, string largerThanBufferSize, string malformedSize)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var option = storeType == StoreType.Main ? "memory" : "obj-log-memory";
            var metricType = storeType == StoreType.Main ? InfoMetricsType.STORE : InfoMetricsType.OBJECTSTORE;
            var initMemorySize = storeType == StoreType.Main ? memorySize : objectStoreLogMemorySize;

            var currMemorySize = ServerOptions.ParseSize(initMemorySize, out _);
            var bufferSize = ServerOptions.NextPowerOf2(currMemorySize);
            var pageSize = storeType == StoreType.Main ? 32L * 1024 * 1024 : 4 * 1024; // default page size

            // Check initial MinEPC before any changes
            var metrics = server.Metrics.GetInfoMetrics(metricType);
            var miMinEPC = metrics.FirstOrDefault(mi => mi.Name == "Log.MinEmptyPageCount");
            ClassicAssert.IsNotNull(miMinEPC);
            ClassicAssert.IsTrue(long.TryParse(miMinEPC.Value, out var minEmptyPageCount));
            var expectedMinEPC = (int)((bufferSize - currMemorySize) / pageSize);
            ClassicAssert.AreEqual(expectedMinEPC, minEmptyPageCount);

            // Try to set memory size to the same value as current
            var result = db.Execute("CONFIG", "SET", option, initMemorySize);
            ClassicAssert.AreEqual("OK", result.ToString());

            // MinEPC should remain unchanged
            metrics = server.Metrics.GetInfoMetrics(metricType);
            miMinEPC = metrics.FirstOrDefault(mi => mi.Name == "Log.MinEmptyPageCount");
            ClassicAssert.IsNotNull(miMinEPC);
            ClassicAssert.IsTrue(long.TryParse(miMinEPC.Value, out minEmptyPageCount));
            ClassicAssert.AreEqual(expectedMinEPC, minEmptyPageCount);

            // Try to set memory size to a smaller value than current
            result = db.Execute("CONFIG", "SET", option, smallerSize);
            ClassicAssert.AreEqual("OK", result.ToString());

            // Check that MinEPC has changed accordingly
            currMemorySize = ServerOptions.ParseSize(smallerSize, out _);
            metrics = server.Metrics.GetInfoMetrics(metricType);
            miMinEPC = metrics.FirstOrDefault(mi => mi.Name == "Log.MinEmptyPageCount");
            ClassicAssert.IsNotNull(miMinEPC);
            ClassicAssert.IsTrue(long.TryParse(miMinEPC.Value, out minEmptyPageCount));
            expectedMinEPC = (int)((bufferSize - currMemorySize) / pageSize);
            ClassicAssert.AreEqual(expectedMinEPC, minEmptyPageCount);

            // Try to set memory size to a larger value than current
            result = db.Execute("CONFIG", "SET", option, largerSize);
            ClassicAssert.AreEqual("OK", result.ToString());

            // Check that MinEPC has changed accordingly
            currMemorySize = ServerOptions.ParseSize(largerSize, out _);
            metrics = server.Metrics.GetInfoMetrics(metricType);
            miMinEPC = metrics.FirstOrDefault(mi => mi.Name == "Log.MinEmptyPageCount");
            ClassicAssert.IsNotNull(miMinEPC);
            ClassicAssert.IsTrue(long.TryParse(miMinEPC.Value, out minEmptyPageCount));
            expectedMinEPC = (int)((bufferSize - currMemorySize) / pageSize);
            ClassicAssert.AreEqual(expectedMinEPC, minEmptyPageCount);

            // Try to set memory size larger than the buffer size - this should fail
            Assert.Throws<RedisServerException>(() => db.Execute("CONFIG", "SET", option, largerThanBufferSize),
                string.Format(CmdStrings.GenericErrMemorySizeGreaterThanBuffer, option));

            // MinEPC should remain unchanged
            metrics = server.Metrics.GetInfoMetrics(metricType);
            miMinEPC = metrics.FirstOrDefault(mi => mi.Name == "Log.MinEmptyPageCount");
            ClassicAssert.IsNotNull(miMinEPC);
            ClassicAssert.IsTrue(long.TryParse(miMinEPC.Value, out minEmptyPageCount));
            ClassicAssert.AreEqual(expectedMinEPC, minEmptyPageCount);

            // Try to set memory size with a malformed size input - this should fail
            Assert.Throws<RedisServerException>(() => db.Execute("CONFIG", "SET", option, malformedSize),
                string.Format(CmdStrings.GenericErrIncorrectSizeFormat, option));
        }

        /// <summary>
        /// This test verifies that dynamically changing the index size configuration using CONFIG SET index / obj-index
        /// incurs the expected changes in Garnet server metrics, as well as verifies error handling for incorrect inputs.
        /// </summary>
        /// <param name="storeType">Store type (Main / Object)</param>
        /// <param name="smallerSize">Index size smaller than the initial size</param>
        /// <param name="largerSize">Index size larger than the initial size</param>
        /// <param name="illegalSize">Illegal index size (not a power of 2)</param>
        /// <param name="malformedSize">Malformed index size string</param>
        [Test]
        [TestCase(StoreType.Main, "32m", "128m", "63m", "8d")]
        [TestCase(StoreType.Main, "16mB", "256MB", "23m", "g8")]
        [TestCase(StoreType.Object, "2m", "32m", "28m", "m9")]
        [TestCase(StoreType.Object, "4Mb", "16mB", "129MB", "0.3gb")]
        public void ConfigSetIndexSizeTest(StoreType storeType, string smallerSize, string largerSize, string illegalSize, string malformedSize)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var metricType = storeType == StoreType.Main ? InfoMetricsType.STORE : InfoMetricsType.OBJECTSTORE;
            var option = storeType == StoreType.Main ? "index" : "obj-index";
            var initIndexSize = storeType == StoreType.Main ? indexSize : objectStoreIndexSize;

            // Check initial index size before any changes
            var currIndexSize = ServerOptions.ParseSize(initIndexSize, out _);
            var metrics = server.Metrics.GetInfoMetrics(metricType);
            var miIndexSize = metrics.FirstOrDefault(mi => mi.Name == "IndexSize");
            ClassicAssert.IsNotNull(miIndexSize);
            ClassicAssert.IsTrue(long.TryParse(miIndexSize.Value, out var actualIndexSize));
            var expectedIndexSize = currIndexSize / 64;
            ClassicAssert.AreEqual(expectedIndexSize, actualIndexSize);

            // Try to set index size to the same value as current
            var result = db.Execute("CONFIG", "SET", option, initIndexSize);
            ClassicAssert.AreEqual("OK", result.ToString());

            // Index size should remain unchanged
            metrics = server.Metrics.GetInfoMetrics(metricType);
            miIndexSize = metrics.FirstOrDefault(mi => mi.Name == "IndexSize");
            ClassicAssert.IsNotNull(miIndexSize);
            ClassicAssert.IsTrue(long.TryParse(miIndexSize.Value, out actualIndexSize));
            ClassicAssert.AreEqual(expectedIndexSize, actualIndexSize);

            // Try to set index size to a larger value than current
            result = db.Execute("CONFIG", "SET", option, largerSize);
            ClassicAssert.AreEqual("OK", result.ToString());

            // Check that index size has changed accordingly
            currIndexSize = ServerOptions.ParseSize(largerSize, out _);
            metrics = server.Metrics.GetInfoMetrics(metricType);
            miIndexSize = metrics.FirstOrDefault(mi => mi.Name == "IndexSize");
            ClassicAssert.IsNotNull(miIndexSize);
            ClassicAssert.IsTrue(long.TryParse(miIndexSize.Value, out actualIndexSize));
            expectedIndexSize = currIndexSize / 64;
            ClassicAssert.AreEqual(expectedIndexSize, actualIndexSize);

            // Try to set index size to a smaller value than current - this should fail
            Assert.Throws<RedisServerException>(() => db.Execute("CONFIG", "SET", option, smallerSize),
                string.Format(CmdStrings.GenericErrIndexSizeSmallerThanCurrent, option));

            // Try to set index size to a value that is not a power of two - this should fail
            Assert.Throws<RedisServerException>(() => db.Execute("CONFIG", "SET", option, illegalSize),
                string.Format(CmdStrings.GenericErrIndexSizePowerOfTwo, option));

            // Try to set index size with a malformed size input - this should fail
            Assert.Throws<RedisServerException>(() => db.Execute("CONFIG", "SET", option, malformedSize),
                string.Format(CmdStrings.GenericErrIncorrectSizeFormat, option));
        }

        /// <summary>
        /// This test verifies that dynamically changing the object store heap size configuration using CONFIG SET object_store_heap_memory_target_size
        /// incurs the expected changes in Garnet server metrics, as well as verifies error handling for incorrect inputs.
        /// </summary>
        /// <param name="smallerSize">Heap size smaller than the initial size</param>
        /// <param name="largerSize">Heap size larger than the initial size</param>
        /// <param name="malformedSize">Malformed heap size string</param>
        [Test]
        [TestCase("10m", "128m", "1.5mb")]
        [TestCase("16m", "65m", "g6")]
        public void ConfigObjHeapSizeTest(string smallerSize, string largerSize, string malformedSize)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var option = "obj-heap-memory";
            var currObjHeapSize = ServerOptions.ParseSize(objectStoreHeapMemorySize, out _);

            // Check initial heap size before any changes
            var metrics = server.Metrics.GetInfoMetrics(InfoMetricsType.MEMORY);
            var miObjHeapTargetSize = metrics.FirstOrDefault(mi => mi.Name == "object_store_heap_memory_target_size");
            ClassicAssert.IsNotNull(miObjHeapTargetSize);
            ClassicAssert.IsTrue(long.TryParse(miObjHeapTargetSize.Value, out var objHeapTargetSize));
            ClassicAssert.AreEqual(currObjHeapSize, objHeapTargetSize);

            // Try to set heap size to the same value as current
            var result = db.Execute("CONFIG", "SET", option, objectStoreHeapMemorySize);
            ClassicAssert.AreEqual("OK", result.ToString());

            // Heap size should remain unchanged
            metrics = server.Metrics.GetInfoMetrics(InfoMetricsType.MEMORY);
            miObjHeapTargetSize = metrics.FirstOrDefault(mi => mi.Name == "object_store_heap_memory_target_size");
            ClassicAssert.IsNotNull(miObjHeapTargetSize);
            ClassicAssert.IsTrue(long.TryParse(miObjHeapTargetSize.Value, out objHeapTargetSize));
            ClassicAssert.AreEqual(currObjHeapSize, objHeapTargetSize);

            // Try to set heap size to a smaller value than current
            result = db.Execute("CONFIG", "SET", option, smallerSize);
            ClassicAssert.AreEqual("OK", result.ToString());

            // Check that heap size has changed accordingly
            currObjHeapSize = ServerOptions.ParseSize(smallerSize, out _);
            metrics = server.Metrics.GetInfoMetrics(InfoMetricsType.MEMORY);
            miObjHeapTargetSize = metrics.FirstOrDefault(mi => mi.Name == "object_store_heap_memory_target_size");
            ClassicAssert.IsNotNull(miObjHeapTargetSize);
            ClassicAssert.IsTrue(long.TryParse(miObjHeapTargetSize.Value, out objHeapTargetSize));
            ClassicAssert.AreEqual(currObjHeapSize, objHeapTargetSize);

            // Try to set heap size to a larger value than current
            result = db.Execute("CONFIG", "SET", option, largerSize);
            ClassicAssert.AreEqual("OK", result.ToString());

            // Check that heap size has changed accordingly
            currObjHeapSize = ServerOptions.ParseSize(largerSize, out _);
            metrics = server.Metrics.GetInfoMetrics(InfoMetricsType.MEMORY);
            miObjHeapTargetSize = metrics.FirstOrDefault(mi => mi.Name == "object_store_heap_memory_target_size");
            ClassicAssert.IsNotNull(miObjHeapTargetSize);
            ClassicAssert.IsTrue(long.TryParse(miObjHeapTargetSize.Value, out objHeapTargetSize));
            ClassicAssert.AreEqual(currObjHeapSize, objHeapTargetSize);

            // Try to set heap size with a malformed size input - this should fail
            Assert.Throws<RedisServerException>(() => db.Execute("CONFIG", "SET", option, malformedSize),
                string.Format(CmdStrings.GenericErrIncorrectSizeFormat, option));
        }
    }

    /// <summary>
    /// Test memory utilization behavior when dynamically changing the memory size configuration using CONFIG SET.
    /// </summary>
    [TestFixture(false)]
    [TestFixture(true)]
    public class RespConfigUtilizationTests
    {
        GarnetServer server;
        private string memorySize = "3m";
        private string indexSize = "1m";
        private string objectStoreLogMemorySize = "2500";
        private string objectStoreHeapMemorySize = "1m";
        private string objectStoreIndexSize = "2048";
        private string pageSize = "1024";
        private bool useReviv;

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
                objectStorePageSize: pageSize,
                objectStoreLogMemorySize: objectStoreLogMemorySize,
                objectStoreIndexSize: objectStoreIndexSize,
                objectStoreHeapMemorySize: objectStoreHeapMemorySize,
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
        /// <param name="storeType">Store Type (Main / Object)</param>
        /// <param name="smallerSize">Memory size smaller than the initial size</param>
        /// <param name="largerSize">Memory size larger than the initial size (within buffer bounds)</param>
        [Test]
        [TestCase(StoreType.Main, "1m", "4m")]
        [TestCase(StoreType.Main, "1024k", "4000k")]
        [TestCase(StoreType.Object, "1024", "4000")]
        [TestCase(StoreType.Object, "1024", "4096")]
        public void ConfigSetMemorySizeUtilizationTest(StoreType storeType, string smallerSize, string largerSize)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var option = storeType == StoreType.Main ? "memory" : "obj-log-memory";
            var initMemorySize = storeType == StoreType.Main ? memorySize : objectStoreLogMemorySize;
            var currMemorySize = TestUtils.GetEffectiveMemorySize(initMemorySize, pageSize, out var parsedPageSize);

            var garnetServer = redis.GetServer(TestUtils.EndPoint);
            var info = TestUtils.GetStoreAddressInfo(garnetServer, isObjectStore: storeType == StoreType.Object);
            ClassicAssert.AreEqual(storeType == StoreType.Main ? 64 : 24, info.TailAddress);

            var i = 0;
            var val = new RedisValue(new string('x', storeType == StoreType.Main ? 512 - 32 : 1));

            // Insert records until head address moves
            var prevHead = info.HeadAddress;
            var prevTail = info.TailAddress;
            while (info.HeadAddress == prevHead)
            {
                var key = $"key{i++:00000}";
                if (storeType == StoreType.Main)
                    _ = db.StringSet(key, val);
                else
                    _ = db.ListRightPush(key, [val]);

                prevHead = info.HeadAddress;
                prevTail = info.TailAddress;
                info = TestUtils.GetStoreAddressInfo(garnetServer, isObjectStore: storeType == StoreType.Object);
            }

            // Verify that records were inserted up to the configured memory size limit
            Assert.That(prevTail, Is.LessThanOrEqualTo(currMemorySize));
            Assert.That(currMemorySize - prevTail, Is.LessThanOrEqualTo(parsedPageSize));

            // Try to set memory size to a smaller value than current
            var result = db.Execute("CONFIG", "SET", option, smallerSize);
            ClassicAssert.AreEqual("OK", result.ToString());

            // Verify that head address moved forward
            info = TestUtils.GetStoreAddressInfo(garnetServer, isObjectStore: storeType == StoreType.Object);
            Assert.That(info.HeadAddress, Is.GreaterThan(prevHead));

            currMemorySize = TestUtils.GetEffectiveMemorySize(smallerSize, pageSize, out _);

            // Insert records until head address moves
            prevHead = info.HeadAddress;
            prevTail = info.TailAddress;
            while (info.HeadAddress == prevHead)
            {
                var key = $"key{i++:00000}";
                if (storeType == StoreType.Main)
                    _ = db.StringSet(key, val);
                else
                    _ = db.ListRightPush(key, [val]);

                prevHead = info.HeadAddress;
                prevTail = info.TailAddress;
                info = TestUtils.GetStoreAddressInfo(garnetServer, isObjectStore: storeType == StoreType.Object);
            }

            // Verify that records were inserted up to the configured memory size limit
            Assert.That(prevTail - prevHead, Is.LessThanOrEqualTo(currMemorySize));
            Assert.That(currMemorySize - (prevTail - prevHead), Is.LessThanOrEqualTo(parsedPageSize));

            // Try to set memory size to a larger value than current
            result = db.Execute("CONFIG", "SET", option, largerSize);
            ClassicAssert.AreEqual("OK", result.ToString());
            currMemorySize = TestUtils.GetEffectiveMemorySize(largerSize, pageSize, out _);

            // Continue to insert records until new memory capacity is reached
            prevHead = info.HeadAddress;
            prevTail = info.TailAddress;
            while (info.HeadAddress == prevHead)
            {
                var key = $"key{i++:00000}";
                if (storeType == StoreType.Main)
                    _ = db.StringSet(key, val);
                else
                    _ = db.ListRightPush(key, [val]);

                prevHead = info.HeadAddress;
                prevTail = info.TailAddress;
                info = TestUtils.GetStoreAddressInfo(garnetServer, isObjectStore: storeType == StoreType.Object);
            }

            // Verify that memory is fully utilized
            Assert.That(prevTail - prevHead, Is.LessThanOrEqualTo(currMemorySize));
            Assert.That(currMemorySize - (prevTail - prevHead), Is.LessThanOrEqualTo(parsedPageSize));
        }

        /// <summary>
        /// This test verifies recovery behavior after dynamically changing the memory size configuration using CONFIG SET.
        /// The test fills the store to a larger capacity than the initial memory size, then verifies that recovering with the
        /// smaller initial memory size retains the last inserted keys in the expected initial capacity.
        /// </summary>
        /// <param name="storeType">Store Type (Main / Object)</param>
        /// <param name="largerSize">Memory size larger than the initial size (within buffer bounds)</param>
        [Test]
        [TestCase(StoreType.Main, "4m")]
        [TestCase(StoreType.Object, "4096")]
        public void ConfigSetMemorySizeRecoveryTest(StoreType storeType, string largerSize)
        {
            var option = storeType == StoreType.Main ? "memory" : "obj-log-memory";
            var initMemorySize = storeType == StoreType.Main ? memorySize : objectStoreLogMemorySize;

            var currMemorySize = TestUtils.GetEffectiveMemorySize(initMemorySize, pageSize, out var parsedPageSize);

            int lastIdxSecondRound;
            int keysInsertedFirstRound;

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                var garnetServer = redis.GetServer(TestUtils.EndPoint);
                var info = TestUtils.GetStoreAddressInfo(garnetServer, isObjectStore: storeType == StoreType.Object);
                ClassicAssert.AreEqual(storeType == StoreType.Main ? 64 : 24, info.TailAddress);

                var i = 0;
                var val = new RedisValue(new string('x', storeType == StoreType.Main ? 512 - 32 : 1));

                // Insert records until head address moves
                var prevHead = info.HeadAddress;
                var prevTail = info.TailAddress;
                while (info.HeadAddress == prevHead)
                {
                    var key = $"key{i++:00000}";
                    if (storeType == StoreType.Main)
                        _ = db.StringSet(key, val);
                    else
                        _ = db.ListRightPush(key, [val]);

                    prevHead = info.HeadAddress;
                    prevTail = info.TailAddress;
                    info = TestUtils.GetStoreAddressInfo(garnetServer, isObjectStore: storeType == StoreType.Object);
                }

                var lastIdxFirstRound = i - 1;

                // Verify that records were inserted up to the configured memory size limit
                Assert.That(prevTail, Is.LessThanOrEqualTo(currMemorySize));
                Assert.That(currMemorySize - prevTail, Is.LessThanOrEqualTo(parsedPageSize));

                // Find the first key index that still exists in the server
                ClassicAssert.IsTrue(db.KeyExists($"key{lastIdxFirstRound:00000}"));
                var c = lastIdxFirstRound;
                while (c > 0)
                {
                    if (!db.KeyExists($"key{--c:00000}")) break;
                }

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
                    if (storeType == StoreType.Main)
                        _ = db.StringSet(key, val);
                    else
                        _ = db.ListRightPush(key, [val]);

                    prevHead = info.HeadAddress;
                    prevTail = info.TailAddress;
                    info = TestUtils.GetStoreAddressInfo(garnetServer, isObjectStore: storeType == StoreType.Object);
                }

                lastIdxSecondRound = i - 1;

                // Verify that memory is fully utilized
                Assert.That(prevTail - prevHead, Is.LessThanOrEqualTo(currMemorySize));
                Assert.That(currMemorySize - (prevTail - prevHead), Is.LessThanOrEqualTo(parsedPageSize));

                garnetServer.Save(SaveType.BackgroundSave);
                while (garnetServer.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks) Thread.Sleep(10);
            }

            // Restart server with initial memory size and recover data
            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(null,
                memorySize: memorySize,
                indexSize: indexSize,
                pageSize: pageSize,
                objectStorePageSize: pageSize,
                objectStoreLogMemorySize: objectStoreLogMemorySize,
                objectStoreIndexSize: objectStoreIndexSize,
                objectStoreHeapMemorySize: objectStoreHeapMemorySize,
                useReviv: useReviv,
                tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                // Find the smallest key index that still exists in the server
                var c = lastIdxSecondRound;
                while (c > 0)
                {
                    if (!db.KeyExists($"key{--c:00000}"))
                        break;
                }

                // Verify that the number of existing keys matches the count of inserted keys in the first round of insertions
                ClassicAssert.AreEqual(keysInsertedFirstRound, lastIdxSecondRound + 1 - c);

                // Verify that all previous keys are not present in the database
                while (c > 0)
                {
                    ClassicAssert.IsFalse(db.KeyExists($"key{--c:00000}"));
                }
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
        private string memorySize = "3m";
        private string indexSize = "512";
        private string objectStoreLogMemorySize = "16384";
        private string objectStoreHeapMemorySize = "16384";
        private string pageSize = "1024";
        private bool useReviv;

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
                objectStorePageSize: pageSize,
                objectStoreLogMemorySize: objectStoreLogMemorySize,
                objectStoreIndexSize: indexSize,
                objectStoreHeapMemorySize: objectStoreHeapMemorySize,
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
        /// <param name="storeType">Store type (Main / Object)</param>
        /// <param name="largerSize1">Larger index size than configured</param>
        /// <param name="largerSize2">Larger index size than previous</param>
        [Test]
        [TestCase(StoreType.Main, "1024", "4096")]
        [TestCase(StoreType.Object, "1024", "4096")]
        public void ConfigSetIndexSizeUtilizationTest(StoreType storeType, string largerSize1, string largerSize2)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var option = storeType == StoreType.Main ? "index" : "obj-index";
            var parsedIndexSize = ServerOptions.ParseSize(indexSize, out _);

            var currIndexSize = storeType == StoreType.Main
                ? server.Provider.StoreWrapper.store.IndexSize
                : server.Provider.StoreWrapper.objectStore.IndexSize;

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
            {
                if (storeType == StoreType.Main)
                    _ = db.StringSet(keys[i], val);
                else
                    _ = db.ListRightPush(keys[i], [val]);
            }

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
            {
                if (storeType == StoreType.Main)
                    _ = db.StringSet(keys[i], val);
                else
                    _ = db.ListRightPush(keys[i], [val]);
            }

            prevOverflowBucketAllocations = GetOverflowBucketAllocations();

            // Try to set index size to a larger value than current
            result = db.Execute("CONFIG", "SET", option, largerSize2);
            ClassicAssert.AreEqual("OK", result.ToString());

            // Verify that overflow bucket allocations have decreased again
            currOverflowBucketAllocations = GetOverflowBucketAllocations();
            ClassicAssert.Less(currOverflowBucketAllocations, prevOverflowBucketAllocations);

            // Verify that all keys still exist in the database
            foreach (var key in keys)
            {
                ClassicAssert.IsTrue(db.KeyExists(key));
            }

            long GetOverflowBucketAllocations() =>
                storeType == StoreType.Main
                    ? server.Provider.StoreWrapper.store.OverflowBucketAllocations
                    : server.Provider.StoreWrapper.objectStore.OverflowBucketAllocations;
        }
    }

    /// <summary>
    /// Test memory utilization behavior when dynamically changing the memory size configuration using CONFIG SET.
    /// </summary>
    [AllureNUnit]
    [TestFixture(false)]
    [TestFixture(true)]
    public class RespConfigHeapUtilizationTests : AllureTestBase
    {
        GarnetServer server;
        private string memorySize = "3m";
        private string indexSize = "512";
        private string objectStoreLogMemorySize = "8192";
        private string objectStoreHeapMemorySize = "4096";
        private string pageSize = "1024";
        private bool useReviv;

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
                objectStorePageSize: pageSize,
                objectStoreLogMemorySize: objectStoreLogMemorySize,
                objectStoreIndexSize: indexSize,
                objectStoreHeapMemorySize: objectStoreHeapMemorySize,
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
        public void ConfigSetHeapSizeUtilizationTest(string largerSize)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var option = "obj-heap-memory";

            // Verify that initial empty page count is zero
            var objectStore = server.Provider.StoreWrapper.objectStore;
            ClassicAssert.AreEqual(0, objectStore.Log.EmptyPageCount);

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
            var sizeTrackerDelay =
                TimeSpan.FromSeconds(
                    LogSizeTracker<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator,
                        CacheSizeTracker.LogSizeCalculator>.ResizeTaskDelaySeconds + 2);
            Thread.Sleep(sizeTrackerDelay);

            // Verify that empty page count has increased
            ClassicAssert.Greater(objectStore.Log.EmptyPageCount, 0);
            var prevEpc = objectStore.Log.EmptyPageCount;

            // Try to set heap size to a larger value than current
            var result = db.Execute("CONFIG", "SET", option, largerSize);
            ClassicAssert.AreEqual("OK", result.ToString());

            // Wait for log size tracker
            Thread.Sleep(sizeTrackerDelay);

            // Verify that empty page count has decreased
            ClassicAssert.Less(objectStore.Log.EmptyPageCount, prevEpc);
        }
    }
}