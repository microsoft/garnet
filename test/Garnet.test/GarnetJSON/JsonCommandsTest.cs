// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using Allure.NUnit;
using Garnet.server;
using GarnetJSON;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    class JsonCommandsTest : AllureTestBase
    {
        GarnetServer server;
        string binPath;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            binPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true,
                enableModuleCommand: Garnet.server.Auth.Settings.ConnectionProtectionOption.Yes,
                extensionAllowUnsignedAssemblies: true,
                extensionBinPaths: [binPath]);
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
            Assert.Throws<RedisServerException>(() => db.Execute("JSON.SET", "key", "$", "{\"a\": 1"));

            // Invalid JSON path
            Assert.Throws<RedisServerException>(() => db.Execute("JSON.SET", "key", "a", "{\"a\": 1}"),
                "ERR new objects must be created at the root");

            db.Execute("JSON.SET", "k1", "$", "{\"f1\": {\"a\":1}, \"f2\":{\"a\":2}}");
            var result = db.Execute("JSON.GET", "k1");
            ClassicAssert.AreEqual("{\"f1\":{\"a\":1},\"f2\":{\"a\":2}}", result.ToString());
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
            ClassicAssert.AreEqual("[{\"f1\":{\"a\":3},\"f2\":{\"a\":3},\"f3\":4,\"f5\":{\"c\":5}}]",
                result.ToString());

            result = db.Execute("JSON.GET", "k1", "f1");
            ClassicAssert.AreEqual("[{\"a\":3}]", result.ToString());

            result = db.Execute("JSON.GET", "k1", "f1", "$.f5");
            ClassicAssert.AreEqual("{\"f1\":[{\"a\":3}],\"$.f5\":[{\"c\":5}]}", result.ToString());
        }

        [TestCase("{\"a\": 1}", "{\"a\":1}", Description = "Simple object")]
        [TestCase("{\"a\": [1,2,3]}", "{\"a\":[1,2,3]}", Description = "Array value")]
        [TestCase("{\"a\": null}", "{\"a\":null}", Description = "Null value")]
        [TestCase("{\"a\": \"\"}", "{\"a\":\"\"}", Description = "Empty string")]
        [TestCase("{}", "{}", Description = "Empty object")]
        public void JsonSetGetBasicTypes(string input, string expectedOutput)
        {
            RegisterCustomCommand();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("JSON.SET", "key", "$", input);
            var result = db.Execute("JSON.GET", "key");
            ClassicAssert.AreEqual(expectedOutput, result.ToString());
        }

        [TestCase("$..a", 42, "{\"x\":{\"a\":1},\"y\":{\"a\":2}}", "{\"x\":{\"a\":42},\"y\":{\"a\":42}}",
            Description = "Update all 'a' fields")]
        [TestCase("$.x", "{\"b\":2}", "{\"x\":{\"a\":1}}", "{\"x\":{\"b\":2}}", Description = "Replace object")]
        [TestCase("$.new", 123, "{\"x\":1}", "{\"x\":1,\"new\":123}", Description = "Add new field")]
        [TestCase("$[0]", 42, "[1,2,3]", "[42,2,3]", Description = "Update array element")]
        public void JsonSetPathOperations(string path, object newValue, string initial, string expected)
        {
            RegisterCustomCommand();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("JSON.SET", "key", "$", initial);
            db.Execute("JSON.SET", "key", path, newValue.ToString());
            var result = db.Execute("JSON.GET", "key");
            ClassicAssert.AreEqual(expected, result.ToString());
        }

        [Test]
        public void JsonGetMultiplePathsTest()
        {
            RegisterCustomCommand();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var json = "{\"a\":{\"x\":1},\"b\":{\"x\":2},\"c\":{\"x\":3}}";
            db.Execute("JSON.SET", "key", "$", json);

            var result = db.Execute("JSON.GET", "key", "$.a", "$.b", "$.c");
            ClassicAssert.AreEqual("{\"$.a\":[{\"x\":1}],\"$.b\":[{\"x\":2}],\"$.c\":[{\"x\":3}]}", result.ToString());
        }

        [TestCase("{\"a\": ", Description = "Incomplete JSON")]
        [TestCase("invalid", Description = "Invalid JSON string")]
        [TestCase("{a:1}", Description = "Missing quotes on property")]
        public void JsonSetInvalidJsonTest(string invalidJson)
        {
            RegisterCustomCommand();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            Assert.Throws<RedisServerException>(() => db.Execute("JSON.SET", "key", "$", invalidJson));
        }

        [TestCase("$[", Description = "Invalid JSONPath syntax")]
        [TestCase("$..", Description = "Incomplete path")]
        [TestCase("$[a]", Description = "Non-numeric array index")]
        public void JsonSetInvalidPathTest(string invalidPath)
        {
            RegisterCustomCommand();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("JSON.SET", "key", "$", "{\"a\":1}");
            Assert.Throws<RedisServerException>(() => db.Execute("JSON.SET", "key", invalidPath, "42"));
        }

        [TestCase("nonexistent", Description = "Non-existent key")]
        [TestCase("key", "$..nonexistent", Description = "Non-existent path")]
        public void JsonGetNonExistentTests(string key, string path = "$")
        {
            RegisterCustomCommand();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var result = db.Execute("JSON.GET", key, path);
            ClassicAssert.IsTrue(result.IsNull);
        }

        [TestCase("$.store.book[*].author",
            "[\"Nigel Rees\",\"Evelyn Waugh\",\"Herman Melville\",\"J. R. R. Tolkien\"]",
            Description = "Wildcard array access")]
        [TestCase("$..author", "[\"Nigel Rees\",\"Evelyn Waugh\",\"Herman Melville\",\"J. R. R. Tolkien\"]",
            Description = "Recursive descent")]
        [TestCase("$.store.book[2]",
            "[{\"category\":\"fiction\",\"author\":\"Herman Melville\",\"title\":\"Moby Dick\",\"price\":8.99}]",
            Description = "Array index access")]
        [TestCase("$.store.book[?(@.price < 10)]",
            "[{\"category\":\"reference\",\"author\":\"Nigel Rees\",\"title\":\"Sayings of the Century\",\"price\":8.95},{\"category\":\"fiction\",\"author\":\"Herman Melville\",\"title\":\"Moby Dick\",\"price\":8.99}]",
            Description = "Filter expression")]
        [TestCase("$.store.bicycle.color", "[\"red\"]", Description = "Direct property access")]
        public void JsonGetComplexPathTests(string path, string expected)
        {
            RegisterCustomCommand();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string complexJson = @"{
                ""store"": {
                    ""book"": [
                        {
                            ""category"": ""reference"",
                            ""author"": ""Nigel Rees"",
                            ""title"": ""Sayings of the Century"",
                            ""price"": 8.95
                        },
                        {
                            ""category"": ""fiction"",
                            ""author"": ""Evelyn Waugh"",
                            ""title"": ""Sword of Honour"",
                            ""price"": 12.99
                        },
                        {
                            ""category"": ""fiction"",
                            ""author"": ""Herman Melville"",
                            ""title"": ""Moby Dick"",
                            ""price"": 8.99
                        },
                        {
                            ""category"": ""fiction"",
                            ""author"": ""J. R. R. Tolkien"",
                            ""title"": ""The Lord of the Rings"",
                            ""price"": 22.99
                        }
                    ],
                    ""bicycle"": {
                        ""color"": ""red"",
                        ""price"": 19.95
                    }
                }
            }";

            db.Execute("JSON.SET", "complex", "$", complexJson);
            var result = db.Execute("JSON.GET", "complex", path);
            ClassicAssert.AreEqual(expected, result.ToString());
        }

        [TestCase("$[0,1]", "[1,2]", "[1,2,3,4]", Description = "Multiple array indices")]
        [TestCase("$[1:3]", "[2,3]", "[1,2,3,4]", Description = "Array slice")]
        [TestCase("$[-2:]", "[3,4]", "[1,2,3,4]", Description = "Negative array indices")]
        [TestCase("$[::2]", "[1,3]", "[1,2,3,4]", Description = "Array step")]
        public void JsonGetArrayOperationsTest(string path, string expected, string initialJson)
        {
            RegisterCustomCommand();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("JSON.SET", "array", "$", initialJson);
            var result = db.Execute("JSON.GET", "array", path);
            ClassicAssert.AreEqual(expected, result.ToString());
        }

        [TestCase("$..[?(@.price > 20)]", "22.99", Description = "Deep search with filter")]
        [TestCase("$..book[?(@.author =~ /.*Tolkien/)]", "J. R. R. Tolkien", Description = "Regex filter")]
        public void JsonGetAdvancedFiltersTest(string path, string expectedValue)
        {
            RegisterCustomCommand();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string complexJson = @"{
                ""store"": {
                    ""book"": [
                        {
                            ""author"": ""Nigel Rees"",
                            ""price"": 8.95
                        },
                        {
                            ""author"": ""J. R. R. Tolkien"",
                            ""title"": ""The Lord of the Rings"",
                            ""price"": 22.99
                        }
                    ]
                }
            }";

            db.Execute("JSON.SET", "complex", "$", complexJson);
            var result = db.Execute("JSON.GET", "complex", path);
            ClassicAssert.IsTrue(result.ToString().Contains(expectedValue));
        }

        [TestCase("$.store.book[0].price", 15.99, Description = "Update specific array element")]
        [TestCase("$.store.book[3].price", 25.99, Description = "Update last element")]
        [TestCase("$.store.bicycle.price", 29.99, Description = "Update nested value")]
        public void JsonSetComplexPathOperationsTest(string path, decimal newPrice)
        {
            RegisterCustomCommand();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string complexJson = @"{
                ""store"": {
                    ""book"": [
                        { ""price"": 8.95 },
                        { ""price"": 12.99 },
                        { ""price"": 8.99 },
                        { ""price"": 22.99 }
                    ],
                    ""bicycle"": {
                        ""price"": 19.95
                    }
                }
            }";

            db.Execute("JSON.SET", "complex", "$", complexJson);
            db.Execute("JSON.SET", "complex", path, newPrice.ToString());
            var result = db.Execute("JSON.GET", "complex", path);
            ClassicAssert.AreEqual($"[{newPrice}]", result.ToString());
        }

        [TestCase("$..book[*].nonexistent", Description = "Create new field with wildcard")]
        [TestCase("$.store.book[?(@.price >= 10)].newfield", Description = "Create new field with filter")]
        [TestCase("$..[?(@.price)].discount", Description = "Create new nested field recursively")]
        public void JsonSetNonStaticPathNewFieldTest(string path)
        {
            RegisterCustomCommand();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string complexJson = @"{
                ""store"": {
                    ""book"": [
                        { ""price"": 8.95 },
                        { ""price"": 12.99 }
                    ],
                    ""bicycle"": {
                        ""price"": 19.95
                    }
                }
            }";

            db.Execute("JSON.SET", "complex", "$", complexJson);
            Assert.Throws<RedisServerException>(() => db.Execute("JSON.SET", "complex", path, "42.99"));
        }

        [TestCase("$..price", "42.99", Description = "Update existing prices recursively")]
        [TestCase("$.store.book[*].price", "19.99", Description = "Update prices with wildcard")]
        [TestCase("$.store.book[?(@.price >= 10)].price", "29.99", Description = "Update prices with filter")]
        public void JsonSetNonStaticPathExistingFieldTest(string path, string newValue)
        {
            RegisterCustomCommand();
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string complexJson = @"{
                ""store"": {
                    ""book"": [
                        { ""price"": 8.95 },
                        { ""price"": 12.99 }
                    ],
                    ""bicycle"": {
                        ""price"": 19.95
                    }
                }
            }";

            db.Execute("JSON.SET", "complex", "$", complexJson);
            db.Execute("JSON.SET", "complex", path, newValue);

            // Verify that the update worked by getting all price fields
            var result = db.Execute("JSON.GET", "complex", "$..price");
            var resultStr = result.ToString();
            Assert.That(resultStr.Contains(newValue), "Updated price value should be present in the result");
            Assert.That(!resultStr.Contains("8.95") || !resultStr.Contains("12.99") || !resultStr.Contains("19.95"));
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
                ClassicAssert.AreEqual("{\"a\":1}", (string)retValue);

                // Issue and wait for DB save
                var server = redis.GetServer(TestUtils.EndPoint);
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
                ClassicAssert.AreEqual("{\"a\":1}", (string)retValue);
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
                ClassicAssert.AreEqual("{\"a\":1}", (string)retValue);
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
                ClassicAssert.AreEqual("{\"a\":1}", (string)retValue);
            }
        }

        [Test]
        public void SerializationTest()
        {
            var jsonObject = new GarnetJsonObject(23);
            jsonObject.Set("$"u8, "{\"a\": 1}"u8, ExistOptions.None, out _);

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
                    var deserializedObject = new GarnetJsonObject(binaryReader.ReadByte(), binaryReader);

                    var output = new List<byte[]>();
                    deserializedObject.TryGet("$"u8, output, out _);
                    ClassicAssert.AreEqual("[{\"a\":1}]", Encoding.UTF8.GetString(output.SelectMany(x => x).ToArray()));
                }
            }
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void JsonModuleLoadTest(bool loadFromDll)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            if (loadFromDll)
            {
                db.Execute("MODULE", "LOADCS", Path.Combine(binPath, "GarnetJSON.dll"));
            }
            else
            {
                server.Register.NewModule(new JsonModule(), [], out _);
            }

            var key = "key";
            db.Execute("JSON.SET", key, "$", "{\"a\": 1}");
            var retValue = db.Execute("JSON.GET", key);
            ClassicAssert.AreEqual("{\"a\":1}", (string)retValue);
        }

        void RegisterCustomCommand()
        {
            var jsonFactory = new GarnetJsonObjectFactory();
            server.Register.NewCommand("JSON.SET", CommandType.ReadModifyWrite, jsonFactory, new JsonSET());
            server.Register.NewCommand("JSON.GET", CommandType.Read, jsonFactory, new JsonGET());
        }
    }
}