// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
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

            // Create a range index and insert data
            var result = db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");
            ClassicAssert.AreEqual("OK", (string)result);
            result = db.Execute("RI.SET", "myindex", "field1", "value1");
            ClassicAssert.AreEqual("OK", (string)result);

            // Delete the index with DEL
            var deleted = db.KeyDelete("myindex");
            ClassicAssert.IsTrue(deleted);

            // Delete again - should return false (not found)
            deleted = db.KeyDelete("myindex");
            ClassicAssert.IsFalse(deleted);

            // RI.SET should fail — index no longer exists after DEL
            var ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.SET", "myindex", "field1", "value1"));
            ClassicAssert.IsNotNull(ex);

            // RI.GET should also fail
            ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.GET", "myindex", "field1"));
            ClassicAssert.IsNotNull(ex);
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

        [Test]
        public void RISetAndGetBasicTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");

            var setResult = db.Execute("RI.SET", "myindex", "field1", "value1");
            ClassicAssert.AreEqual("OK", (string)setResult);

            var getResult = db.Execute("RI.GET", "myindex", "field1");
            ClassicAssert.AreEqual("value1", (string)getResult);
        }

        [Test]
        public void RISetOverwriteTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");

            db.Execute("RI.SET", "myindex", "field1", "value1");
            db.Execute("RI.SET", "myindex", "field1", "value2");

            var getResult = db.Execute("RI.GET", "myindex", "field1");
            ClassicAssert.AreEqual("value2", (string)getResult);
        }

        [Test]
        public void RIGetNonExistentFieldTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");

            var getResult = db.Execute("RI.GET", "myindex", "nosuchfield");
            ClassicAssert.IsTrue(getResult.IsNull);
        }

        [Test]
        public void RIGetNonExistentIndexTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.GET", "noindex", "field1"));
            ClassicAssert.IsTrue(ex.Message.Contains("range index"));
        }

        [Test]
        public void RIDelFieldTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");
            db.Execute("RI.SET", "myindex", "field1", "value1");

            var delResult = (int)db.Execute("RI.DEL", "myindex", "field1");
            ClassicAssert.AreEqual(1, delResult);

            var getResult = db.Execute("RI.GET", "myindex", "field1");
            ClassicAssert.IsTrue(getResult.IsNull);
        }

        [Test]
        public void RISetOnNonExistentIndexTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.SET", "noindex", "field1", "value1"));
            ClassicAssert.IsTrue(ex.Message.Contains("range index"));
        }

        [Test]
        public void RIMultipleFieldsTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8");

            db.Execute("RI.SET", "myindex", "aaa", "val-a");
            db.Execute("RI.SET", "myindex", "bbb", "val-b");
            db.Execute("RI.SET", "myindex", "ccc", "val-c");

            ClassicAssert.AreEqual("val-a", (string)db.Execute("RI.GET", "myindex", "aaa"));
            ClassicAssert.AreEqual("val-b", (string)db.Execute("RI.GET", "myindex", "bbb"));
            ClassicAssert.AreEqual("val-c", (string)db.Execute("RI.GET", "myindex", "ccc"));
        }

        [Test]
        public void RIWrongTypeOnNormalKeyTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // SET a normal string key
            db.StringSet("normalkey", "hello");

            // RI.SET on a normal key should fail
            var ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.SET", "normalkey", "field1", "value1"));
            ClassicAssert.IsNotNull(ex);
        }

        [Test]
        public void RIWrongTypeGetOnNormalKeyTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // SET a normal string key, then try RI.GET
            db.StringSet("normalkey", "hello");

            var ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.GET", "normalkey", "field1"));
            ClassicAssert.IsNotNull(ex);
        }

        [Test]
        public void RINormalGetOnRangeIndexKeyTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("RI.CREATE", "myindex", "MEMORY", "CACHESIZE", "65536");

            // GET on a RI key returns nil (CancelOperation in Reader guard)
            var val = db.StringGet("myindex");
            ClassicAssert.IsTrue(val.IsNull);
        }

        [Test]
        public void RIScanBasicTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var diskPath = Path.Combine(TestUtils.MethodTestDir, "scanidx");
            db.Execute("RI.CREATE", "myindex", "DISK", diskPath, "CACHESIZE", "65536", "MINRECORD", "8");

            db.Execute("RI.SET", "myindex", "aaa", "val-a");
            db.Execute("RI.SET", "myindex", "bbb", "val-b");
            db.Execute("RI.SET", "myindex", "ccc", "val-c");
            db.Execute("RI.SET", "myindex", "ddd", "val-d");
            db.Execute("RI.SET", "myindex", "eee", "val-e");

            var result = (RedisResult[])db.Execute("RI.SCAN", "myindex", "aaa", "COUNT", "3");
            ClassicAssert.AreEqual(3, result.Length);

            // Each element is [key, value] since default FIELDS is BOTH
            var first = (RedisResult[])result[0];
            ClassicAssert.AreEqual("aaa", (string)first[0]);
            ClassicAssert.AreEqual("val-a", (string)first[1]);

            var second = (RedisResult[])result[1];
            ClassicAssert.AreEqual("bbb", (string)second[0]);
            ClassicAssert.AreEqual("val-b", (string)second[1]);

            var third = (RedisResult[])result[2];
            ClassicAssert.AreEqual("ccc", (string)third[0]);
            ClassicAssert.AreEqual("val-c", (string)third[1]);
        }

        [Test]
        public void RIScanFieldsKeyTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var diskPath = Path.Combine(TestUtils.MethodTestDir, "scanidx");
            db.Execute("RI.CREATE", "myindex", "DISK", diskPath, "CACHESIZE", "65536", "MINRECORD", "8");

            db.Execute("RI.SET", "myindex", "aaa", "val-a");
            db.Execute("RI.SET", "myindex", "bbb", "val-b");

            var result = (RedisResult[])db.Execute("RI.SCAN", "myindex", "aaa", "COUNT", "10", "FIELDS", "KEY");
            ClassicAssert.AreEqual(2, result.Length);
            ClassicAssert.AreEqual("aaa", (string)result[0]);
            ClassicAssert.AreEqual("bbb", (string)result[1]);
        }

        [Test]
        public void RIScanFieldsValueTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var diskPath = Path.Combine(TestUtils.MethodTestDir, "scanidx");
            db.Execute("RI.CREATE", "myindex", "DISK", diskPath, "CACHESIZE", "65536", "MINRECORD", "8");

            db.Execute("RI.SET", "myindex", "aaa", "val-a");
            db.Execute("RI.SET", "myindex", "bbb", "val-b");

            var result = (RedisResult[])db.Execute("RI.SCAN", "myindex", "aaa", "COUNT", "10", "FIELDS", "VALUE");
            ClassicAssert.AreEqual(2, result.Length);
            ClassicAssert.AreEqual("val-a", (string)result[0]);
            ClassicAssert.AreEqual("val-b", (string)result[1]);
        }

        [Test]
        public void RIRangeBasicTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var diskPath = Path.Combine(TestUtils.MethodTestDir, "rangeidx");
            db.Execute("RI.CREATE", "myindex", "DISK", diskPath, "CACHESIZE", "65536", "MINRECORD", "8");

            db.Execute("RI.SET", "myindex", "aaa", "val-a");
            db.Execute("RI.SET", "myindex", "bbb", "val-b");
            db.Execute("RI.SET", "myindex", "ccc", "val-c");
            db.Execute("RI.SET", "myindex", "ddd", "val-d");
            db.Execute("RI.SET", "myindex", "eee", "val-e");

            var result = (RedisResult[])db.Execute("RI.RANGE", "myindex", "bbb", "ddd");
            ClassicAssert.AreEqual(3, result.Length);

            var first = (RedisResult[])result[0];
            ClassicAssert.AreEqual("bbb", (string)first[0]);
            ClassicAssert.AreEqual("val-b", (string)first[1]);

            var second = (RedisResult[])result[1];
            ClassicAssert.AreEqual("ccc", (string)second[0]);
            ClassicAssert.AreEqual("val-c", (string)second[1]);

            var third = (RedisResult[])result[2];
            ClassicAssert.AreEqual("ddd", (string)third[0]);
            ClassicAssert.AreEqual("val-d", (string)third[1]);
        }

        [Test]
        public void RIScanOnNonExistentIndexTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.SCAN", "noindex", "aaa", "COUNT", "10"));
            ClassicAssert.IsTrue(ex.Message.Contains("range index"));
        }

        [Test]
        public void RIRangeOnNonExistentIndexTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var ex = Assert.Throws<RedisServerException>(() =>
                db.Execute("RI.RANGE", "noindex", "aaa", "zzz"));
            ClassicAssert.IsTrue(ex.Message.Contains("range index"));
        }
    }
}