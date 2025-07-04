// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespConfigTests
    {
        GarnetServer server;
        private string memorySize = "17g";
        private string indexSize = "64m";
        private string objectStoreLogMemorySize = "17m";
        private string objectStoreHeapMemorySize = "32m";
        private string objectStoreIndexSize = "8m";

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                memorySize: memorySize,
                indexSize: indexSize,
                objectStoreLogMemorySize: objectStoreLogMemorySize,
                objectStoreIndexSize: objectStoreIndexSize,
                objectStoreHeapMemorySize: objectStoreHeapMemorySize);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [TestCase(true, "16g", "32g", "64g", "g4")]
        [TestCase(true, "9gB", "28GB", "33G", "2gBB")]
        [TestCase(false, "16m", "32m", "64m", "3bm")]
        [TestCase(false, "5MB", "30M", "128mb", "44d")]

        public void ConfigSetMemorySizeTest(bool mainStore, string smallerSize, string largerSize, string largerThanBufferSize, string malformedSize)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var option = mainStore ? "memory" : "obj-log-memory";
            var metricType = mainStore ? InfoMetricsType.STORE : InfoMetricsType.OBJECTSTORE;
            var initMemorySize = mainStore ? memorySize : objectStoreLogMemorySize;

            var currMemorySize = ServerOptions.ParseSize(initMemorySize, out _);
            var bufferSize = ServerOptions.PreviousPowerOf2(currMemorySize);
            if (bufferSize != currMemorySize)
                bufferSize *= 2;
            var pageSize = mainStore ? 32L * 1024 * 1024 : 4 * 1024; // default page size

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

        [Test]
        [TestCase(true, "32m", "128m", "63m", "8d")]
        [TestCase(true, "16mB", "256MB", "23m", "g8")]
        [TestCase(false, "2m", "32m", "28m", "m9")]
        [TestCase(false, "4Mb", "16mB", "129MB", "0.3gb")]
        public void ConfigSetIndexSizeTest(bool mainStore, string smallerSize, string largerSize, string illegalSize, string malformedSize)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var metricType = mainStore ? InfoMetricsType.STORE : InfoMetricsType.OBJECTSTORE;
            var option = mainStore ? "index" : "obj-index";
            var initIndexSize = mainStore ? indexSize : objectStoreIndexSize;

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
}