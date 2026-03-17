// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Allure.NUnit;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class RespRangeIndexTests : AllureTestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        public void RICreateBasicTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // RI.CREATE with MEMORY backend
            var result = db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536");
            ClassicAssert.AreEqual("OK", (string)result);
        }

        [Test]
        public void RICreateDuplicateReturnsErrorTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Create first time
            var result = db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536");
            ClassicAssert.AreEqual("OK", (string)result);

            // Create again - should fail with error
            var ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536"));
            ClassicAssert.IsTrue(ex.Message.Contains("index already exists"));
        }

        [Test]
        public void RICreateThenDeleteTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Create a range index
            var result = db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536");
            ClassicAssert.AreEqual("OK", (string)result);

            // Delete it with DEL
            var deleted = db.KeyDelete("myindex");
            ClassicAssert.IsTrue(deleted);

            // Delete again - should return false (not found)
            deleted = db.KeyDelete("myindex");
            ClassicAssert.IsFalse(deleted);
        }

        [Test]
        public void RICreateWithDefaultsTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // RI.CREATE with minimal args (defaults)
            var result = db.Execute("RI.CREATE", "myindex", "MEMORY");
            ClassicAssert.AreEqual("OK", (string)result);
        }

        [Test]
        public void RICreateWithAllOptionsTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // RI.CREATE with all options
            var result = db.Execute("RI.CREATE", "myindex", "MEMORY",
                "CACHESIZE", "131072",
                "MINRECORD", "32",
                "MAXRECORD", "2048",
                "MAXKEYLEN", "256",
                "PAGESIZE", "8192");
            ClassicAssert.AreEqual("OK", (string)result);
        }
    }
}