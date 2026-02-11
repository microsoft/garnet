// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class RespBlockingCollectionTests : AllureTestBase
    {
        GarnetServer server;
        private static readonly Random random = Random.Shared;

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
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [TestCase("BRPOP")]
        [TestCase("BLPOP")]
        public void BasicListBlockingPopTest(string blockingCmd)
        {
            var key = "mykey";
            var value = "myval";
            var key2 = "mykey2";
            var value2 = "myval2";

            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand($"LPUSH {key} {value}");
            var expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"{blockingCmd} {key} 10", 3);
            expectedResponse = $"*2\r\n${key.Length}\r\n{key}\r\n${value.Length}\r\n{value}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            var blockingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var btResponse = lcr.SendCommand($"{blockingCmd} {key2} 30", 3);
                var btExpectedResponse = $"*2\r\n${key2.Length}\r\n{key2}\r\n${value2.Length}\r\n{value2}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, btResponse);
            });

            var releasingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                Task.Delay(TimeSpan.FromSeconds(2)).Wait();
                return lcr.SendCommand($"LPUSH {key2} {value2}");
            });

            var timeout = TimeSpan.FromSeconds(5);
            try
            {
                Task.WaitAll([blockingTask, releasingTask], timeout);
            }
            catch (AggregateException)
            {
                Assert.Fail();
            }

            ClassicAssert.IsTrue(blockingTask.IsCompletedSuccessfully);
            ClassicAssert.IsTrue(releasingTask.IsCompletedSuccessfully);
        }

        [Test]
        [TestCase("BRPOP", "LPUSH", 0)]
        [TestCase("BRPOP", "LPUSH", 0.5)]
        [TestCase("BLPOP", "RPUSH", 0)]
        [TestCase("BLPOP", "RPUSH", 0.5)]
        public void MultiListBlockingPopTest(string blockingCmd, string pushCmd, double timeout)
        {
            var key = "mykey";

            var keyCount = 64;

            using var lightClientRequest = TestUtils.CreateRequest();

            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

            var blockingTask = Task.Run(async () =>
            {
                for (var i = 0; i < keyCount; i++)
                {
                    using var lcr = TestUtils.CreateRequest();
                    var btResponse = lcr.SendCommand($"{blockingCmd} {key} {timeout}", 3);
                    var value = $"value{i}";
                    var btExpectedResponse = $"*2\r\n${key.Length}\r\n{key}\r\n${value.Length}\r\n{value}\r\n";
                    TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, btResponse);
                    await Task.Delay(TimeSpan.FromMilliseconds(random.NextInt64(20, 100)), cts.Token);
                }
            }, cts.Token);

            var releasingTask = Task.Run(async () =>
            {
                for (var i = 0; i < keyCount; i++)
                {
                    using var lcr = TestUtils.CreateRequest();
                    var value = $"value{i}";
                    lcr.SendCommand($"{pushCmd} {key} {value}");
                    await Task.Delay(TimeSpan.FromMilliseconds(random.NextInt64(20, 100)), cts.Token);
                }
            }, cts.Token);

            // Wait for all tasks to finish
            if (!Task.WhenAll(blockingTask, releasingTask).Wait(TimeSpan.FromSeconds(60), cts.Token))
                Assert.Fail("Items not retrieved in allotted time.");
        }

        [Test]
        public void BasicBlockingListMoveTest()
        {
            var srcKey1 = "mykey_src";
            var dstKey1 = "mykey_dst";
            var value1 = "myval";
            var srcKey2 = "mykey2_src";
            var dstKey2 = "mykey2_dst";
            var value2 = "myval2";

            using var lightClientRequest = TestUtils.CreateRequest();

            var response = lightClientRequest.SendCommand($"LPUSH {srcKey1} {value1}");
            var expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"BLMOVE {srcKey1} {dstKey1} {OperationDirection.Right} {OperationDirection.Left} 10");
            expectedResponse = $"${value1.Length}\r\n{value1}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"LRANGE {srcKey1} 0 -1");
            expectedResponse = $"*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"LRANGE {dstKey1} 0 -1", 2);
            expectedResponse = $"*1\r\n${value1.Length}\r\n{value1}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            var blockingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var btResponse = lcr.SendCommand($"BLMOVE {srcKey2} {dstKey2} {OperationDirection.Left} {OperationDirection.Right} 0");
                var btExpectedResponse = $"${value2.Length}\r\n{value2}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, btResponse);
            });

            var releasingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                Task.Delay(TimeSpan.FromSeconds(2)).Wait();
                return lcr.SendCommand($"LPUSH {srcKey2} {value2}");
            });

            var timeout = TimeSpan.FromSeconds(5);
            try
            {
                Task.WaitAll([blockingTask, releasingTask], timeout);
            }
            catch (AggregateException)
            {
                Assert.Fail();
            }

            ClassicAssert.IsTrue(blockingTask.IsCompletedSuccessfully);
            ClassicAssert.IsTrue(releasingTask.IsCompletedSuccessfully);

            response = lightClientRequest.SendCommand($"LRANGE {srcKey2} 0 -1");
            expectedResponse = $"*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"LRANGE {dstKey2} 0 -1", 2);
            expectedResponse = $"*1\r\n${value2.Length}\r\n{value2}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [TestCase("BRPOP")]
        [TestCase("BLPOP")]
        public void ListBlockingPopOrderTest(string blockingCmd)
        {
            var keys = new[] { "key1", "key2", "key3", "key4", "key5" };
            var values = new[] { "value1", "value2", "value3", "value4", "value5" };

            byte[] response;
            string expectedResponse;

            using var lightClientRequest = TestUtils.CreateRequest();
            for (var i = 0; i < keys.Length; i++)
            {
                response = lightClientRequest.SendCommand($"LPUSH {keys[i]} {values[i]}");
                expectedResponse = ":1\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
            }

            for (var i = 0; i < keys.Length; i++)
            {
                response = lightClientRequest.SendCommand($"{blockingCmd} {string.Join(' ', keys)} 10", 3);
                expectedResponse = $"*2\r\n${keys[i].Length}\r\n{keys[i]}\r\n${values[i].Length}\r\n{values[i]}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
            }
        }

        [Test]
        [TestCase("BRPOP", GarnetObjectType.Set)]
        [TestCase("BRPOP", GarnetObjectType.SortedSet)]
        [TestCase("BLPOP", GarnetObjectType.Set)]
        [TestCase("BLPOP", GarnetObjectType.SortedSet)]
        public void BlockingListPopWrongTypeTests(string blockingCmd, GarnetObjectType wrongObjectType)
        {
            var keys = new[] { "key1", "key2", "key3" };
            var values = new[]
            {
                [new RedisValue("key1:v1")],
                [new RedisValue("key2:v1")],
                new[] { new RedisValue("key3:v1") }
            };

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // First key right type, second key wrong type - succeed
            var result = db.ListLeftPush(keys[0], values[0]);
            ClassicAssert.AreEqual(values[0].Length, result);

            result = wrongObjectType switch
            {
                GarnetObjectType.Set => db.SetAdd(keys[1], values[1]),
                GarnetObjectType.SortedSet => db.SortedSetAdd(keys[1],
                    values[1].Select(v => new SortedSetEntry(v, 1)).ToArray()),
                _ => -1,
            };
            ClassicAssert.AreEqual(values[1].Length, result);

            var popResult = db.Execute(blockingCmd, keys[0], keys[1], keys[2], 0);
            ClassicAssert.AreEqual(2, popResult.Length);
            ClassicAssert.AreEqual(keys[0], popResult[0].ToString());
            ClassicAssert.AreEqual(values[0][0], popResult[1].ToString());

            // First key empty, second key wrong type - fail immediately
            var keyExists = db.KeyExists(keys[0]);
            ClassicAssert.IsFalse(keyExists);

            var ex = Assert.Throws<RedisServerException>(() => db.Execute(blockingCmd, keys[0], keys[1], keys[2], 0));
            var expectedMessage = Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE);
            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(expectedMessage, ex.Message);

            // All keys empty, second key gets wrong type then right type - succeed
            var delResult = db.KeyDelete(keys[1]);
            ClassicAssert.IsTrue(delResult);

            var blockingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var btResponse = lcr.SendCommand($"{blockingCmd} {keys[0]} {keys[1]} {keys[2]} 0", 3);
                var btExpectedResponse = $"*2\r\n${keys[2].Length}\r\n{keys[2]}\r\n${values[2][0].ToString().Length}\r\n{values[2][0]}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, btResponse);
            });

            var releasingTask = Task.Run(() =>
            {
                Task.Delay(TimeSpan.FromSeconds(2)).Wait();
                result = wrongObjectType switch
                {
                    GarnetObjectType.Set => db.SetAdd(keys[2], values[2]),
                    GarnetObjectType.SortedSet => db.SortedSetAdd(keys[2],
                        values[2].Select(v => new SortedSetEntry(v, 1)).ToArray()),
                    _ => -1,
                };
                ClassicAssert.AreEqual(values[2].Length, result);

                var setPopCount = wrongObjectType switch
                {
                    GarnetObjectType.Set => db.SetPop(keys[2], 2).Length,
                    GarnetObjectType.SortedSet => db.SortedSetPop(keys[2], 2).Length,
                    _ => -1,
                };
                ClassicAssert.AreEqual(values[2].Length, setPopCount);

                result = db.ListLeftPush(keys[2], values[2]);
                ClassicAssert.AreEqual(values[2].Length, result);
            });

            var timeout = TimeSpan.FromSeconds(5);
            try
            {
                Task.WaitAll([blockingTask, releasingTask], timeout);
            }
            catch (AggregateException)
            {
                Assert.Fail();
            }

            ClassicAssert.IsTrue(blockingTask.IsCompletedSuccessfully);
            ClassicAssert.IsTrue(releasingTask.IsCompletedSuccessfully);
        }

        [Test]
        [TestCase("BZPOPMIN", GarnetObjectType.Set)]
        [TestCase("BZPOPMIN", GarnetObjectType.List)]
        [TestCase("BZPOPMAX", GarnetObjectType.Set)]
        [TestCase("BZPOPMAX", GarnetObjectType.List)]
        public void BlockingSortedSetPopWrongTypeTests(string blockingCmd, GarnetObjectType wrongObjectType)
        {
            var keys = new[] { "key1", "key2", "key3" };
            var values = new[]
            {
                [new SortedSetEntry("key1:v1", 1)],
                [new SortedSetEntry("key2:v1", 2)],
                new[] { new SortedSetEntry("key3:v1", 3) }
            };

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // First key right type, second key wrong type - succeed
            var result = db.SortedSetAdd(keys[0], values[0]);
            ClassicAssert.AreEqual(values[0].Length, result);

            result = wrongObjectType switch
            {
                GarnetObjectType.Set => db.SetAdd(keys[1], values[1].Select(v => v.Element).ToArray()),
                GarnetObjectType.List => db.ListLeftPush(keys[1], values[1].Select(v => v.Element).ToArray()),
                _ => -1,
            };
            ClassicAssert.AreEqual(values[1].Length, result);

            var popResult = db.Execute(blockingCmd, keys[0], keys[1], keys[2], 0);
            ClassicAssert.AreEqual(3, popResult.Length);
            ClassicAssert.AreEqual(keys[0], popResult[0].ToString());
            ClassicAssert.AreEqual(values[0][0].Element, popResult[1].ToString());
            ClassicAssert.AreEqual(values[0][0].Score.ToString(), popResult[2].ToString());

            // First key empty, second key wrong type - fail immediately
            var keyExists = db.KeyExists(keys[0]);
            ClassicAssert.IsFalse(keyExists);

            var ex = Assert.Throws<RedisServerException>(() => db.Execute(blockingCmd, keys[0], keys[1], keys[2], 0));
            var expectedMessage = Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE);
            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(expectedMessage, ex.Message);

            // All keys empty, second key gets wrong type then right type - succeed
            var delResult = db.KeyDelete(keys[1]);
            ClassicAssert.IsTrue(delResult);

            var blockingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var btResponse = lcr.SendCommand($"{blockingCmd} {keys[0]} {keys[1]} {keys[2]} 0", 4);
                var btExpectedResponse = $"*3\r\n${keys[2].Length}\r\n{keys[2]}\r\n${values[2][0].Element.ToString().Length}\r\n{values[2][0].Element}\r\n${values[2][0].Score.ToString().Length}\r\n{values[2][0].Score}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, btResponse);
            });

            var releasingTask = Task.Run(() =>
            {
                Task.Delay(TimeSpan.FromSeconds(2)).Wait();
                result = wrongObjectType switch
                {
                    GarnetObjectType.Set => db.SetAdd(keys[2], values[2].Select(v => v.Element).ToArray()),
                    GarnetObjectType.List => db.ListLeftPush(keys[2], values[2].Select(v => v.Element).ToArray()),
                    _ => -1,
                };
                ClassicAssert.AreEqual(values[2].Length, result);

                var setPopCount = wrongObjectType switch
                {
                    GarnetObjectType.Set => db.SetPop(keys[2], 2).Length,
                    GarnetObjectType.List => db.ListLeftPop(keys[2], 2).Length,
                    _ => -1,
                };
                ClassicAssert.AreEqual(values[2].Length, setPopCount);

                result = db.SortedSetAdd(keys[2], values[2]);
                ClassicAssert.AreEqual(values[2].Length, result);
            });

            var timeout = TimeSpan.FromSeconds(5);
            try
            {
                Task.WaitAll([blockingTask, releasingTask], timeout);
            }
            catch (AggregateException)
            {
                Assert.Fail();
            }

            ClassicAssert.IsTrue(blockingTask.IsCompletedSuccessfully);
            ClassicAssert.IsTrue(releasingTask.IsCompletedSuccessfully);
        }

        [Test]
        [TestCase(GarnetObjectType.Set)]
        [TestCase(GarnetObjectType.SortedSet)]
        public void BlockingListMultiPopWrongTypeTest(GarnetObjectType wrongObjectType)
        {
            var keys = new[] { "key1", "key2", "key3" };
            var values = new[]
            {
                [new RedisValue("key1:v1")],
                [new RedisValue("key2:v1"), new RedisValue("key2:v2")],
                new[] { new RedisValue("key3:v1"), new RedisValue("key3:v2") }
            };

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // First key right type, second key wrong type - succeed
            var result = db.ListLeftPush(keys[0], values[0]);
            ClassicAssert.AreEqual(values[0].Length, result);

            result = wrongObjectType switch
            {
                GarnetObjectType.Set => db.SetAdd(keys[1], values[1]),
                GarnetObjectType.SortedSet => db.SortedSetAdd(keys[1],
                    values[1].Select(v => new SortedSetEntry(v, 1)).ToArray()),
                _ => -1,
            };
            ClassicAssert.AreEqual(values[1].Length, result);

            var popResult = db.Execute("BLMPOP", 0, 3, keys[0], keys[1], keys[2], "RIGHT", "COUNT", 2);
            ClassicAssert.AreEqual(2, popResult.Length);
            ClassicAssert.AreEqual(keys[0], popResult[0].ToString());
            ClassicAssert.AreEqual(1, popResult[1].Length);
            ClassicAssert.AreEqual(values[0][0], popResult[1][0].ToString());

            // First key empty, second key wrong type - fail immediately
            var keyExists = db.KeyExists(keys[0]);
            ClassicAssert.IsFalse(keyExists);

            var ex = Assert.Throws<RedisServerException>(() => db.Execute("BLMPOP", 0, 3, keys[0], keys[1], keys[2], "RIGHT", "COUNT", 2));
            var expectedMessage = Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE);
            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(expectedMessage, ex.Message);

            // All keys empty, second key gets wrong type then right type - succeed
            var delResult = db.KeyDelete(keys[1]);
            ClassicAssert.IsTrue(delResult);

            var blockingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var btResponse = lcr.SendCommand($"BLMPOP 0 3 {keys[0]} {keys[1]} {keys[2]} RIGHT COUNT 2", 3);
                var btExpectedResponse = $"*2\r\n${keys[2].Length}\r\n{keys[2]}\r\n*2\r\n${values[2][0].ToString().Length}\r\n{values[2][0]}\r\n${values[2][1].ToString().Length}\r\n{values[2][1]}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, btResponse);
            });

            var releasingTask = Task.Run(() =>
            {
                Task.Delay(TimeSpan.FromSeconds(2)).Wait();
                result = wrongObjectType switch
                {
                    GarnetObjectType.Set => db.SetAdd(keys[2], values[2]),
                    GarnetObjectType.SortedSet => db.SortedSetAdd(keys[2],
                        values[2].Select(v => new SortedSetEntry(v, 1)).ToArray()),
                    _ => -1,
                };
                ClassicAssert.AreEqual(values[2].Length, result);

                var setPopCount = wrongObjectType switch
                {
                    GarnetObjectType.Set => db.SetPop(keys[2], 2).Length,
                    GarnetObjectType.SortedSet => db.SortedSetPop(keys[2], 2).Length,
                    _ => -1,
                };
                ClassicAssert.AreEqual(values[2].Length, setPopCount);

                result = db.ListLeftPush(keys[2], values[2]);
                ClassicAssert.AreEqual(values[2].Length, result);
            });

            var timeout = TimeSpan.FromSeconds(5);
            try
            {
                Task.WaitAll([blockingTask, releasingTask], timeout);
            }
            catch (AggregateException)
            {
                Assert.Fail();
            }

            ClassicAssert.IsTrue(blockingTask.IsCompletedSuccessfully);
            ClassicAssert.IsTrue(releasingTask.IsCompletedSuccessfully);
        }

        [Test]
        [TestCase(GarnetObjectType.Set)]
        [TestCase(GarnetObjectType.List)]
        public void BlockingSortedSetMultiPopWrongTypeTest(GarnetObjectType wrongObjectType)
        {
            var keys = new[] { "key1", "key2", "key3" };
            var values = new[]
            {
                [new SortedSetEntry("key1:v1", 1)],
                [new SortedSetEntry("key2:v1", 2)],
                new[] { new SortedSetEntry("key3:v1", 3) }
            };

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // First key right type, second key wrong type - succeed
            var result = db.SortedSetAdd(keys[0], values[0]);
            ClassicAssert.AreEqual(values[0].Length, result);

            result = wrongObjectType switch
            {
                GarnetObjectType.Set => db.SetAdd(keys[1], values[1].Select(v => v.Element).ToArray()),
                GarnetObjectType.List => db.ListLeftPush(keys[1], values[1].Select(v => v.Element).ToArray()),
                _ => -1,
            };
            ClassicAssert.AreEqual(values[1].Length, result);

            var popResult = db.Execute("BZMPOP", 0, 3, keys[0], keys[1], keys[2], "MAX", "COUNT", 2);
            ClassicAssert.AreEqual(2, popResult.Length);
            ClassicAssert.AreEqual(keys[0], popResult[0].ToString());
            ClassicAssert.AreEqual(1, popResult[1].Length);
            ClassicAssert.AreEqual(2, popResult[1][0].Length);
            ClassicAssert.AreEqual(values[0][0].Element, popResult[1][0][0].ToString());
            ClassicAssert.AreEqual(values[0][0].Score.ToString(), popResult[1][0][1].ToString());

            // First key empty, second key wrong type - fail immediately
            var keyExists = db.KeyExists(keys[0]);
            ClassicAssert.IsFalse(keyExists);

            var ex = Assert.Throws<RedisServerException>(() => db.Execute("BZMPOP", 0, 3, keys[0], keys[1], keys[2], "MAX", "COUNT", 2));
            var expectedMessage = Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE);
            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(expectedMessage, ex.Message);

            // All keys empty, second key gets wrong type then right type - succeed
            var delResult = db.KeyDelete(keys[1]);
            ClassicAssert.IsTrue(delResult);

            var blockingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var btResponse = lcr.SendCommand($"BZMPOP 0 3 {keys[0]} {keys[1]} {keys[2]} MAX COUNT 2", 2);
                var btExpectedResponse = $"*2\r\n${keys[2].Length}\r\n{keys[2]}\r\n*1\r\n*2\r\n${values[2][0].Element.ToString().Length}\r\n{values[2][0].Element}\r\n${values[2][0].Score.ToString().Length}\r\n{values[2][0].Score}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, btResponse);
            });

            var releasingTask = Task.Run(() =>
            {
                Task.Delay(TimeSpan.FromSeconds(2)).Wait();
                result = wrongObjectType switch
                {
                    GarnetObjectType.Set => db.SetAdd(keys[2], values[2].Select(v => v.Element).ToArray()),
                    GarnetObjectType.List => db.ListLeftPush(keys[2], values[2].Select(v => v.Element).ToArray()),
                    _ => -1,
                };
                ClassicAssert.AreEqual(values[2].Length, result);

                var setPopCount = wrongObjectType switch
                {
                    GarnetObjectType.Set => db.SetPop(keys[2], 2).Length,
                    GarnetObjectType.List => db.ListLeftPop(keys[2], 2).Length,
                    _ => -1,
                };
                ClassicAssert.AreEqual(values[2].Length, setPopCount);

                result = db.SortedSetAdd(keys[2], values[2]);
                ClassicAssert.AreEqual(values[2].Length, result);
            });

            var timeout = TimeSpan.FromSeconds(5);
            try
            {
                Task.WaitAll([blockingTask, releasingTask], timeout);
            }
            catch (AggregateException)
            {
                Assert.Fail();
            }

            ClassicAssert.IsTrue(blockingTask.IsCompletedSuccessfully);
            ClassicAssert.IsTrue(releasingTask.IsCompletedSuccessfully);
        }

        [Test]
        [TestCase(GarnetObjectType.Set)]
        [TestCase(GarnetObjectType.SortedSet)]
        public void BlockingListMoveWrongTypeTest(GarnetObjectType wrongObjectType)
        {
            var srcKey = "srcKey";
            var dstKey = "dstKey";
            var srcValues = new[] { new RedisValue("srcKey:v1"), new RedisValue("srcKey:v2") };
            var dstValues = new[] { new RedisValue("dstKey:v1") };

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Source key right type, dest key wrong type - fail immediately
            var result = db.ListLeftPush(srcKey, srcValues);
            ClassicAssert.AreEqual(srcValues.Length, result);

            result = wrongObjectType switch
            {
                GarnetObjectType.Set => db.SetAdd(dstKey, dstValues),
                GarnetObjectType.SortedSet => db.SortedSetAdd(dstKey,
                    dstValues.Select(v => new SortedSetEntry(v, 1)).ToArray()),
                _ => -1,
            };
            ClassicAssert.AreEqual(dstValues.Length, result);

            var ex = Assert.Throws<RedisServerException>(() => db.Execute("BLMOVE", srcKey, dstKey, "RIGHT", "LEFT", 0));
            var expectedMessage = Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE);
            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(expectedMessage, ex.Message);

            // Source key empty, dest key empty - dest gets wrong type, fail once source gets an item
            var delResult = db.KeyDelete(srcKey);
            ClassicAssert.IsTrue(delResult);
            delResult = db.KeyDelete(dstKey);
            ClassicAssert.IsTrue(delResult);

            var blockingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var btResponse = lcr.SendCommand($"BLMOVE {srcKey} {dstKey} RIGHT LEFT 0");
                var btExpectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE)}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, btResponse);
            });

            var releasingTask = Task.Run(() =>
            {
                Task.Delay(TimeSpan.FromSeconds(2)).Wait();
                result = wrongObjectType switch
                {
                    GarnetObjectType.Set => db.SetAdd(dstKey, dstValues),
                    GarnetObjectType.SortedSet => db.SortedSetAdd(dstKey,
                        dstValues.Select(v => new SortedSetEntry(v, 1)).ToArray()),
                    _ => -1,
                };
                ClassicAssert.AreEqual(dstValues.Length, result);

                result = db.ListLeftPush(srcKey, srcValues);
                ClassicAssert.AreEqual(srcValues.Length, result);
            });

            var timeout = TimeSpan.FromSeconds(5);
            try
            {
                Task.WaitAll([blockingTask, releasingTask], timeout);
            }
            catch (AggregateException)
            {
                Assert.Fail();
            }

            ClassicAssert.IsTrue(blockingTask.IsCompletedSuccessfully);
            ClassicAssert.IsTrue(releasingTask.IsCompletedSuccessfully);

            // Source key empty, dst key right type, src key gets wrong type then right type - succeed
            delResult = db.KeyDelete(srcKey);
            ClassicAssert.IsTrue(delResult);
            delResult = db.KeyDelete(dstKey);
            ClassicAssert.IsTrue(delResult);

            result = db.ListLeftPush(dstKey, dstValues);
            ClassicAssert.AreEqual(dstValues.Length, result);

            blockingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var btResponse = lcr.SendCommand($"BLMOVE {srcKey} {dstKey} RIGHT LEFT 0");
                var btExpectedResponse = $"${srcValues[0].ToString().Length}\r\n{srcValues[0]}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, btResponse);
            });

            releasingTask = Task.Run(() =>
            {
                Task.Delay(TimeSpan.FromSeconds(2)).Wait();
                result = wrongObjectType switch
                {
                    GarnetObjectType.Set => db.SetAdd(srcKey, srcValues),
                    GarnetObjectType.SortedSet => db.SortedSetAdd(srcKey,
                        srcValues.Select(v => new SortedSetEntry(v, 1)).ToArray()),
                    _ => -1,
                };
                ClassicAssert.AreEqual(srcValues.Length, result);

                var setPopCount = wrongObjectType switch
                {
                    GarnetObjectType.Set => db.SetPop(srcKey, 2).Length,
                    GarnetObjectType.SortedSet => db.SortedSetPop(srcKey, 2).Length,
                    _ => -1,
                };
                ClassicAssert.AreEqual(srcValues.Length, setPopCount);

                result = db.ListLeftPush(srcKey, srcValues);
                ClassicAssert.AreEqual(srcValues.Length, result);
            });

            try
            {
                Task.WaitAll([blockingTask, releasingTask], timeout);
            }
            catch (AggregateException)
            {
                Assert.Fail();
            }

            ClassicAssert.IsTrue(blockingTask.IsCompletedSuccessfully);
            ClassicAssert.IsTrue(releasingTask.IsCompletedSuccessfully);
        }

        [Test]
        [TestCase("BRPOP")]
        [TestCase("BLPOP")]
        public void BlockingClientEventsTests(string blockingCmd)
        {
            var key = "mykey";
            var value1 = "myval";
            var value2 = "myval2";

            var blockingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var response = lcr.SendCommand($"{blockingCmd} {key} 0", 3);
                var expectedResponse = $"*2\r\n${key.Length}\r\n{key}\r\n${value2.Length}\r\n{value2}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

                response = lcr.SendCommand($"LPUSH {key} {value1}");
                expectedResponse = $":1\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

                response = lcr.SendCommand($"LLEN {key}");
                expectedResponse = ":1\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

                response = lcr.SendCommand($"LPOP {key}");
                expectedResponse = $"${value1.Length}\r\n{value1}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
            });

            var releasingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                Task.Delay(TimeSpan.FromSeconds(2)).Wait();
                var response = lcr.SendCommand($"LPUSH {key} {value2}");
                var expectedResponse = $":1\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
            });

            var timeout = TimeSpan.FromSeconds(500);
            try
            {
                Task.WaitAll([blockingTask, releasingTask], timeout);
            }
            catch (AggregateException)
            {
                Assert.Fail();
            }

            ClassicAssert.IsTrue(blockingTask.IsCompletedSuccessfully);
            ClassicAssert.IsTrue(releasingTask.IsCompletedSuccessfully);
        }

        [Test]
        public void BasicBlockingListPopPushTest()
        {
            var srcKey1 = "mykey_src";
            var dstKey1 = "mykey_dst";
            var value1 = "myval";
            var srcKey2 = "mykey2_src";
            var dstKey2 = "mykey2_dst";
            var value2 = "myval2";

            using var lightClientRequest = TestUtils.CreateRequest();

            var response = lightClientRequest.SendCommand($"LPUSH {srcKey1} {value1}");
            var expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"BRPOPLPUSH {srcKey1} {dstKey1} 10");
            expectedResponse = $"${value1.Length}\r\n{value1}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"LRANGE {srcKey1} 0 -1");
            expectedResponse = $"*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"LRANGE {dstKey1} 0 -1", 2);
            expectedResponse = $"*1\r\n${value1.Length}\r\n{value1}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            var blockingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var btResponse = lcr.SendCommand($"BRPOPLPUSH {srcKey2} {dstKey2} 0");
                var btExpectedResponse = $"${value2.Length}\r\n{value2}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, btResponse);
            });

            var releasingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                Task.Delay(TimeSpan.FromSeconds(2)).Wait();
                return lcr.SendCommand($"LPUSH {srcKey2} {value2}");
            });

            var timeout = TimeSpan.FromSeconds(5);
            try
            {
                Task.WaitAll([blockingTask, releasingTask], timeout);
            }
            catch (AggregateException)
            {
                Assert.Fail();
            }

            ClassicAssert.IsTrue(blockingTask.IsCompletedSuccessfully);
            ClassicAssert.IsTrue(releasingTask.IsCompletedSuccessfully);

            response = lightClientRequest.SendCommand($"LRANGE {srcKey2} 0 -1");
            expectedResponse = $"*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"LRANGE {dstKey2} 0 -1", 2);
            expectedResponse = $"*1\r\n${value2.Length}\r\n{value2}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [TestCase(OperationDirection.Left, "value1", Description = "Pop from left")]
        [TestCase(OperationDirection.Right, "value3", Description = "Pop from right")]
        public void BasicBlmpopTest(OperationDirection direction, string expectedValue)
        {
            var key = "mykey";
            using var lightClientRequest = TestUtils.CreateRequest();

            lightClientRequest.SendCommand($"RPUSH {key} value1 value2 value3");
            var response = lightClientRequest.SendCommand($"BLMPOP 1 1 {key} {direction}");
            var expectedResponse = $"*2\r\n${key.Length}\r\n{key}\r\n*1\r\n${expectedValue.Length}\r\n{expectedValue}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [TestCase(1, "key1", "value1", Description = "First key has value")]
        [TestCase(2, "key2", "value2", Description = "Second key has value")]
        public void BlmpopMultipleKeysTest(int valueKeyIndex, string expectedKey, string expectedValue)
        {
            var keys = new[] { "key1", "key2", "key3" };
            using var lightClientRequest = TestUtils.CreateRequest();

            lightClientRequest.SendCommand($"RPUSH {keys[valueKeyIndex - 1]} {expectedValue}");
            var response = lightClientRequest.SendCommand($"BLMPOP 1 {keys.Length} {string.Join(" ", keys)} LEFT");
            var expectedResponse = $"*2\r\n${expectedKey.Length}\r\n{expectedKey}\r\n*1\r\n${expectedValue.Length}\r\n{expectedValue}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void BlmpopTimeoutTest()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("BLMPOP 1 1 nonexistentkey LEFT");
            var expectedResponse = "$-1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void BlmpopBlockingBehaviorTest()
        {
            var key = "blockingkey";
            var value = "testvalue";

            var blockingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var response = lcr.SendCommand($"BLMPOP 30 1 {key} LEFT");
                var expectedResponse = $"*2\r\n${key.Length}\r\n{key}\r\n*1\r\n${value.Length}\r\n{value}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
            });

            var pushingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                Task.Delay(TimeSpan.FromSeconds(2)).Wait();
                return lcr.SendCommand($"LPUSH {key} {value}");
            });

            Task.WaitAll([blockingTask, pushingTask], TimeSpan.FromSeconds(5));
            ClassicAssert.IsTrue(blockingTask.IsCompletedSuccessfully);
            ClassicAssert.IsTrue(pushingTask.IsCompletedSuccessfully);
        }

        [Test]
        public void BlmpopBlockingWithCountTest()
        {
            var key = "countkey";
            var values = new[] { "value1", "value2", "value3", "value4" };

            var blockingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var response = lcr.SendCommand($"BLMPOP 30 1 {key} LEFT COUNT 3");
                var expectedResponse = $"*2\r\n${key.Length}\r\n{key}\r\n*3\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n$6\r\nvalue3\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
            });

            var pushingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                Task.Delay(TimeSpan.FromSeconds(2)).Wait();
                return lcr.SendCommand($"RPUSH {key} {string.Join(" ", values)}");
            });

            Task.WaitAll([blockingTask, pushingTask], TimeSpan.FromSeconds(10));
            ClassicAssert.IsTrue(blockingTask.IsCompletedSuccessfully);
            ClassicAssert.IsTrue(pushingTask.IsCompletedSuccessfully);

            // Pop the remaining item and verify that key does not exist after
            using var lcr = TestUtils.CreateRequest();
            var response = lcr.SendCommand($"BLMPOP 30 1 {key} LEFT COUNT 1");
            var expectedResponse = $"*2\r\n${key.Length}\r\n{key}\r\n*1\r\n$6\r\nvalue4\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            using var lightClientRequest = TestUtils.CreateRequest();
            response = lightClientRequest.SendCommand($"EXISTS {key}");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [TestCase("MIN", "value1", 1.5, Description = "Pop minimum score")]
        [TestCase("MAX", "value3", 3.5, Description = "Pop maximum score")]
        public void BasicBzmpopTest(string mode, string expectedValue, double expectedScore)
        {
            var key = "mykey";
            using var lightClientRequest = TestUtils.CreateRequest();

            lightClientRequest.SendCommand($"ZADD {key} 1.5 value1 2.5 value2 3.5 value3");
            var response = lightClientRequest.SendCommand($"BZMPOP 1 1 {key} {mode}");
            var expectedResponse = $"*2\r\n${key.Length}\r\n{key}\r\n*1\r\n*2\r\n${expectedValue.Length}\r\n{expectedValue}\r\n${expectedScore.ToString().Length}\r\n{expectedScore}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [TestCase("MIN", Description = "Pop minimum score with expired items")]
        [TestCase("MAX", Description = "Pop maximum score with expired items")]
        public async Task BasicBzmpopWithExpireItemsTest(string mode)
        {
            var key = "mykey";
            using var lightClientRequest = TestUtils.CreateRequest();

            lightClientRequest.SendCommand($"ZADD {key} 1.5 value1 2.5 value2 3.5 value3");
            lightClientRequest.SendCommand($"ZPEXPIRE {key} 200 MEMBERS 3 value1 value2 value3");
            await Task.Delay(300);
            using var lcr = TestUtils.CreateRequest();
            var response = lcr.SendCommand($"BZMPOP 1 1 {key} {mode}");
            var expectedResponse = "$-1\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void BzmpopReturnTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var res = db.SortedSetAdd("a", [new SortedSetEntry("one", 1)]);
            ClassicAssert.AreEqual(1, res);
            res = db.SortedSetAdd("a", [new SortedSetEntry("two", 2)]);
            ClassicAssert.AreEqual(1, res);
            res = db.SortedSetAdd("b", [new SortedSetEntry("three", 3)]);
            ClassicAssert.AreEqual(1, res);
            var pop = db.Execute("BZMPOP", "10", "2", "a", "b", "MIN", "COUNT", "2");
            ClassicAssert.AreEqual(2, pop.Length);
            ClassicAssert.AreEqual("a", pop[0].ToString());
            ClassicAssert.AreEqual(2, pop[1].Length);
            ClassicAssert.AreEqual(2, pop[1][0].Length);
            ClassicAssert.AreEqual("one", pop[1][0][0].ToString());
            ClassicAssert.AreEqual(1, (int)(RedisValue)pop[1][0][1]);
            ClassicAssert.AreEqual(2, pop[1][1].Length);
            ClassicAssert.AreEqual("two", pop[1][1][0].ToString());
            ClassicAssert.AreEqual(2, (int)(RedisValue)pop[1][1][1]);
        }

        [Test]
        [TestCase(1, "key1", "value1", 1.5, Description = "First key has minimum value")]
        [TestCase(2, "key2", "value2", 2.5, Description = "Second key has minimum value")]
        public void BzmpopMultipleKeysTest(int valueKeyIndex, string expectedKey, string expectedValue, double expectedScore)
        {
            var keys = new[] { "key1", "key2", "key3" };
            using var lightClientRequest = TestUtils.CreateRequest();

            lightClientRequest.SendCommand($"ZADD {keys[valueKeyIndex - 1]} {expectedScore} {expectedValue}");
            var response = lightClientRequest.SendCommand($"BZMPOP 1 {keys.Length} {string.Join(" ", keys)} MIN");
            var expectedResponse = $"*2\r\n${expectedKey.Length}\r\n{expectedKey}\r\n*1\r\n*2\r\n${expectedValue.Length}\r\n{expectedValue}\r\n${expectedScore.ToString().Length}\r\n{expectedScore}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void BzmpopTimeoutTest()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand("BZMPOP 1 1 nonexistentkey MIN");
            var expectedResponse = "$-1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void BzmpopBlockingBehaviorTest()
        {
            var key = "blockingzset";
            var values = new[] { "value1", "value2", "value3" };
            var scores = new[] { "0.5", "1.5", "2.5" };

            var blockingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var response = lcr.SendCommand($"BZMPOP 30 1 {key} MIN COUNT 2", 2);
                var expectedResponse = $"*2\r\n${key.Length}\r\n{key}\r\n*2\r\n{string.Join("\r\n", values.Take(2).Zip(scores.Take(2), (v, s) => $"*2\r\n${v.Length}\r\n{v}\r\n${s.Length}\r\n{s}"))}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
            });

            var pushingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                Task.Delay(TimeSpan.FromSeconds(2)).Wait();
                var result = lcr.SendCommand($"ZADD {key} {string.Join(' ', scores.Zip(values, (s, v) => $"{s} {v}"))}");
                return result;
            });

            Task.WaitAll([blockingTask, pushingTask], TimeSpan.FromSeconds(10));
            ClassicAssert.IsTrue(blockingTask.IsCompletedSuccessfully);
            ClassicAssert.IsTrue(pushingTask.IsCompletedSuccessfully);

            // Pop the remaining item and verify that key does not exist after
            using var lcr = TestUtils.CreateRequest();
            var response = lcr.SendCommand($"BZMPOP 30 1 {key} MIN COUNT 2");
            var expectedResponse = $"*2\r\n${key.Length}\r\n{key}\r\n*1\r\n*2\r\n${values[2].Length}\r\n{values[2]}\r\n${scores[2].Length}\r\n{scores[2]}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            using var lightClientRequest = TestUtils.CreateRequest();
            response = lightClientRequest.SendCommand($"EXISTS {key}");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [TestCase("BZPOPMIN", "value1", 1.5, Description = "Pop minimum score")]
        [TestCase("BZPOPMAX", "value3", 3.5, Description = "Pop maximum score")]
        public void BasicBzpopMinMaxTest(string command, string expectedValue, double expectedScore)
        {
            var key = "zsettestkey";
            using var lightClientRequest = TestUtils.CreateRequest();

            lightClientRequest.SendCommand($"ZADD {key} 1.5 value1 2.5 value2 3.5 value3");
            var response = lightClientRequest.SendCommand($"{command} {key} 1");
            var expectedResponse = $"*3\r\n${key.Length}\r\n{key}\r\n${expectedValue.Length}\r\n{expectedValue}\r\n${expectedScore.ToString().Length}\r\n{expectedScore}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [TestCase("BZPOPMIN", Description = "Pop minimum score with expired items")]
        [TestCase("BZPOPMAX", Description = "Pop maximum score with expired items")]
        public async Task BasicBzpopMinMaxWithExpireItemsTest(string command)
        {
            var key = "zsettestkey";
            using var lightClientRequest = TestUtils.CreateRequest();

            lightClientRequest.SendCommand($"ZADD {key} 1.5 value1 2.5 value2 3.5 value3");
            lightClientRequest.SendCommand($"ZPEXPIRE {key} 200 MEMBERS 3 value1 value2 value3");
            await Task.Delay(300);
            using var lcr = TestUtils.CreateRequest();
            var response = lcr.SendCommand($"{command} {key} 1");
            var expectedResponse = "$-1\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        [TestCase("BZPOPMIN", 1, "key1", "value1", 1.5, Description = "First key has minimum")]
        [TestCase("BZPOPMAX", 2, "key2", "value2", 3.5, Description = "Second key has maximum")]
        public void BzpopMinMaxMultipleKeysTest(string command, int valueKeyIndex, string expectedKey, string expectedValue, double expectedScore)
        {
            var keys = new[] { "key1", "key2", "key3" };
            using var lightClientRequest = TestUtils.CreateRequest();

            lightClientRequest.SendCommand($"ZADD {keys[valueKeyIndex - 1]} {expectedScore} {expectedValue}");
            var response = lightClientRequest.SendCommand($"{command} {string.Join(" ", keys)} 1");
            var expectedResponse = $"*3\r\n${expectedKey.Length}\r\n{expectedKey}\r\n${expectedValue.Length}\r\n{expectedValue}\r\n${expectedScore.ToString().Length}\r\n{expectedScore}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [TestCase("BZPOPMIN", Description = "Blocking pop minimum")]
        [TestCase("BZPOPMAX", Description = "Blocking pop maximum")]
        public void BzpopMinMaxBlockingTest(string command)
        {
            var key = "blockingzset2";
            var value = "testvalue";
            var score = 2.5;

            var blockingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var response = lcr.SendCommand($"{command} {key} 30");
                var expectedResponse = $"*3\r\n${key.Length}\r\n{key}\r\n${value.Length}\r\n{value}\r\n${score.ToString().Length}\r\n{score}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
            });

            var pushingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                Task.Delay(TimeSpan.FromSeconds(2)).Wait();
                return lcr.SendCommand($"ZADD {key} {score} {value}");
            });

            Task.WaitAll([blockingTask, pushingTask], TimeSpan.FromSeconds(5));
            ClassicAssert.IsTrue(blockingTask.IsCompletedSuccessfully);
            ClassicAssert.IsTrue(pushingTask.IsCompletedSuccessfully);
        }

        [Test]
        [TestCase("BZPOPMIN", Description = "Timeout on minimum")]
        [TestCase("BZPOPMAX", Description = "Timeout on maximum")]
        public void BzpopMinMaxTimeoutTest(string command)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand($"{command} nonexistentkey 1");
            var expectedResponse = "$-1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }
    }
}