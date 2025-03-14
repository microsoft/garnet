// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    public class RespBlockingCollectionTests
    {
        GarnetServer server;
        private TaskFactory taskFactory = new();

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

            var blockingTask = taskFactory.StartNew(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var btResponse = lcr.SendCommand($"{blockingCmd} {key2} 30", 3);
                var btExpectedResponse = $"*2\r\n${key2.Length}\r\n{key2}\r\n${value2.Length}\r\n{value2}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, btResponse);
            });

            var releasingTask = taskFactory.StartNew(() =>
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

            var blockingTask = taskFactory.StartNew(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var btResponse = lcr.SendCommand($"BLMOVE {srcKey2} {dstKey2} {OperationDirection.Left} {OperationDirection.Right} 0");
                var btExpectedResponse = $"${value2.Length}\r\n{value2}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, btResponse);
            });

            var releasingTask = taskFactory.StartNew(() =>
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
        [TestCase("BRPOP")]
        [TestCase("BLPOP")]
        public void BlockingClientEventsTests(string blockingCmd)
        {
            var key = "mykey";
            var value1 = "myval";
            var value2 = "myval2";

            var blockingTask = taskFactory.StartNew(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var response = lcr.SendCommands($"{blockingCmd} {key} 30", $"LPUSH {key} {value1}", 3, 1);
                var expectedResponse = $"*2\r\n${key.Length}\r\n{key}\r\n${value2.Length}\r\n{value2}\r\n:1\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

                response = lcr.SendCommand($"LLEN {key}");
                expectedResponse = ":1\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

                response = lcr.SendCommand($"LPOP {key}");
                expectedResponse = $"${value1.Length}\r\n{value1}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
            });

            var releasingTask = taskFactory.StartNew(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                Task.Delay(TimeSpan.FromSeconds(2)).Wait();
                return lcr.SendCommand($"LPUSH {key} {value2}");
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

            var blockingTask = taskFactory.StartNew(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var btResponse = lcr.SendCommand($"BRPOPLPUSH {srcKey2} {dstKey2} 0");
                var btExpectedResponse = $"${value2.Length}\r\n{value2}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, btResponse);
            });

            var releasingTask = taskFactory.StartNew(() =>
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

            var blockingTask = taskFactory.StartNew(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var response = lcr.SendCommand($"BLMPOP 30 1 {key} LEFT");
                var expectedResponse = $"*2\r\n${key.Length}\r\n{key}\r\n*1\r\n${value.Length}\r\n{value}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
            });

            var pushingTask = taskFactory.StartNew(() =>
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

            var blockingTask = taskFactory.StartNew(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var response = lcr.SendCommand($"BLMPOP 30 1 {key} LEFT COUNT 3");
                var expectedResponse = $"*2\r\n${key.Length}\r\n{key}\r\n*3\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n$6\r\nvalue3\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
            });

            var pushingTask = taskFactory.StartNew(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                Task.Delay(TimeSpan.FromSeconds(2)).Wait();
                return lcr.SendCommand($"RPUSH {key} {string.Join(" ", values)}");
            });

            Task.WaitAll([blockingTask, pushingTask], TimeSpan.FromSeconds(10));
            ClassicAssert.IsTrue(blockingTask.IsCompletedSuccessfully);
            ClassicAssert.IsTrue(pushingTask.IsCompletedSuccessfully);
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
            var value = "testvalue";
            var score = 1.5;

            var blockingTask = taskFactory.StartNew(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var response = lcr.SendCommand($"BZMPOP 30 1 {key} MIN COUNT 2");
                var expectedResponse = $"*2\r\n${key.Length}\r\n{key}\r\n*1\r\n*2\r\n${value.Length}\r\n{value}\r\n${score.ToString().Length}\r\n{score}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
            });

            var pushingTask = taskFactory.StartNew(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                Task.Delay(TimeSpan.FromSeconds(2)).Wait();
                var result = lcr.SendCommand($"ZADD {key} {score} {value}");
                return result;
            });

            Task.WaitAll([blockingTask, pushingTask], TimeSpan.FromSeconds(10));
            ClassicAssert.IsTrue(blockingTask.IsCompletedSuccessfully);
            ClassicAssert.IsTrue(pushingTask.IsCompletedSuccessfully);
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

            var blockingTask = taskFactory.StartNew(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var response = lcr.SendCommand($"{command} {key} 30");
                var expectedResponse = $"*3\r\n${key.Length}\r\n{key}\r\n${value.Length}\r\n{value}\r\n${score.ToString().Length}\r\n{score}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
            });

            var pushingTask = taskFactory.StartNew(() =>
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