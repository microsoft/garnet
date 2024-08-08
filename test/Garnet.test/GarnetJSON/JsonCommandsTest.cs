// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Reflection;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    class JsonCommandsTest
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            var binPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, extensionAllowUnsignedAssemblies: true, extensionBinPaths: [binPath]);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.Execute("MODULE", "LOADCS", Path.Combine(binPath, "GarnetJSON.dll"));
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void JsonSetGetTests()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            try
            {
                db.Execute("JSON.SET", "key", "$", "{\"a\": 1");
                Assert.Fail();
            }
            catch (RedisServerException e)
            {
                Assert.AreEqual("ERR Invalid input", e.Message);
            }

            db.Execute("JSON.SET", "k1", "$", "{\"f1\": {\"a\":1}, \"f2\":{\"a\":2}}");
            var result = db.Execute("JSON.GET", "k1", "$");
            Assert.AreEqual("[{\"f1\":{\"a\":1},\"f2\":{\"a\":2}}]", result.ToString());

            db.Execute("JSON.SET", "k1", "$..a", 3);
            result = db.Execute("JSON.GET", "k1", "$");
            Assert.AreEqual("[{\"f1\":{\"a\":3},\"f2\":{\"a\":3}}]", result.ToString());

            db.Execute("JSON.SET", "k1", "$.f3", 4);
            result = db.Execute("JSON.GET", "k1", "$");
            Assert.AreEqual("[{\"f1\":{\"a\":3},\"f2\":{\"a\":3},\"f3\":4}]", result.ToString());

            db.Execute("JSON.SET", "k1", "$.f5", "{\"c\": 5}");
            result = db.Execute("JSON.GET", "k1", "$");
            Assert.AreEqual("[{\"f1\":{\"a\":3},\"f2\":{\"a\":3},\"f3\":4,\"f5\":{\"c\":5}}]", result.ToString());
        }
    }
}