// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Reflection;
using System.Threading;
using Garnet.server;
using GarnetJSON;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    class JsonCommandsTest
    {
        GarnetServer server;
        string binPath;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            binPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, extensionAllowUnsignedAssemblies: true, extensionBinPaths: [binPath]);
            server.Start();
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
            RegisterCustomCommand();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Invalid JSON value
            try
            {
                db.Execute("JSON.SET", "key", "$", "{\"a\": 1");
                Assert.Fail();
            }
            catch (RedisServerException e)
            {
                ClassicAssert.AreEqual("ERR Invalid input", e.Message);
            }

            // Invalid JSON path
            try
            {
                db.Execute("JSON.SET", "key", "a", "{\"a\": 1}");
                Assert.Fail();
            }
            catch (RedisServerException e)
            {
                ClassicAssert.AreEqual("ERR Invalid input", e.Message);
            }

            db.Execute("JSON.SET", "k1", "$", "{\"f1\": {\"a\":1}, \"f2\":{\"a\":2}}");
            var result = db.Execute("JSON.GET", "k1");
            ClassicAssert.AreEqual("[{\"f1\":{\"a\":1},\"f2\":{\"a\":2}}]", result.ToString());
            result = db.Execute("JSON.GET", "k1", "$");
            ClassicAssert.AreEqual("[{\"f1\":{\"a\":1},\"f2\":{\"a\":2}}]", result.ToString());

            db.Execute("JSON.SET", "k1", "$..a", 3);
            result = db.Execute("JSON.GET", "k1", "$");
            ClassicAssert.AreEqual("[{\"f1\":{\"a\":3},\"f2\":{\"a\":3}}]", result.ToString());

            db.Execute("JSON.SET", "k1", "$.f3", 4);
            result = db.Execute("JSON.GET", "k1", "$");
            ClassicAssert.AreEqual("[{\"f1\":{\"a\":3},\"f2\":{\"a\":3},\"f3\":4}]", result.ToString());

            db.Execute("JSON.SET", "k1", "$.f5", "{\"c\": 5}");
            result = db.Execute("JSON.GET", "k1", "$");
            ClassicAssert.AreEqual("[{\"f1\":{\"a\":3},\"f2\":{\"a\":3},\"f3\":4,\"f5\":{\"c\":5}}]", result.ToString());
        }

        [Test]
        public void SaveRecoverTest()
        {
            string key = "key";
            RegisterCustomCommand();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                db.Execute("JSON.SET", key, "$", "{\"a\": 1}");
                var retValue = db.Execute("JSON.GET", key);
                ClassicAssert.AreEqual("[{\"a\":1}]", (string)retValue);

                // Issue and wait for DB save
                var server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");
                server.Save(SaveType.BackgroundSave);
                while (server.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks) Thread.Sleep(10);
            }

            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true);
            RegisterCustomCommand();
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                var retValue = db.Execute("JSON.GET", key);
                ClassicAssert.AreEqual("[{\"a\":1}]", (string)retValue);
            }
        }

        [Test]
        public void AofUpsertRecoverTest()
        {
            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true);
            RegisterCustomCommand();
            server.Start();

            var key = "aofkey";
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                db.Execute("JSON.SET", key, "$", "{\"a\": 1}");
                var retValue = db.Execute("JSON.GET", key);
                ClassicAssert.AreEqual("[{\"a\":1}]", (string)retValue);
            }

            server.Store.CommitAOF(true);
            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true);
            RegisterCustomCommand();
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                var retValue = db.Execute("JSON.GET", key);
                ClassicAssert.AreEqual("[{\"a\":1}]", (string)retValue);
            }
        }

        [Test]
        public void SerializationTest()
        {
            var jsonObject = new JsonObject(23);
            jsonObject.TrySet("$", "{\"a\": 1}");

            byte[] serializedData;
            using (var memoryStream = new MemoryStream())
            {
                using (var binaryWriter = new BinaryWriter(memoryStream))
                {
                    jsonObject.Serialize(binaryWriter);
                }

                serializedData = memoryStream.ToArray();
            }

            using (var memoryStream = new MemoryStream(serializedData))
            {
                using (var binaryReader = new BinaryReader(memoryStream))
                {
                    var deserializedObject = new JsonObject(binaryReader.ReadByte(), binaryReader);

                    deserializedObject.TryGet("$", out var newString);
                    ClassicAssert.AreEqual("[{\"a\":1}]", newString);
                }
            }
        }

        [Test]
        public void JsonModuleLoadTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.Execute("MODULE", "LOADCS", Path.Combine(binPath, "GarnetJSON.dll"));

            var key = "key";
            db.Execute("JSON.SET", key, "$", "{\"a\": 1}");
            var retValue = db.Execute("JSON.GET", key);
            ClassicAssert.AreEqual("[{\"a\":1}]", (string)retValue);
        }

        void RegisterCustomCommand()
        {
            var jsonFactory = new JsonObjectFactory();
            server.Register.NewCommand("JSON.SET", CommandType.ReadModifyWrite, jsonFactory, new JsonSET());
            server.Register.NewCommand("JSON.GET", CommandType.Read, jsonFactory, new JsonGET());
        }
    }
}