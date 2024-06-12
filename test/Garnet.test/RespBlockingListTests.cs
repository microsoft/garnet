// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;

namespace Garnet.test
{
    public class RespBlockingListTests
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
            var key3 = "mykey3";

            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand($"LPUSH {key} {value}");
            var expectedResponse = ":1\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand($"{blockingCmd} {key} 10", 3);
            expectedResponse = $"*2\r\n${key.Length}\r\n{key}\r\n${value.Length}\r\n{value}\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            var blockingTask = taskFactory.StartNew(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var btResponse = lcr.SendCommand($"{blockingCmd} {key2} 30", 3);
                var btExpectedResponse = $"*2\r\n${key2.Length}\r\n{key2}\r\n${value2.Length}\r\n{value2}\r\n";
                var btActualValue = Encoding.ASCII.GetString(btResponse).Substring(0, btExpectedResponse.Length);
                Assert.AreEqual(btExpectedResponse, btActualValue);
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
                Task.WaitAll(new[] { blockingTask, releasingTask }, timeout);
            }
            catch (AggregateException)
            {
                Assert.Fail();
            }

            Assert.IsTrue(blockingTask.IsCompletedSuccessfully);
            Assert.IsTrue(releasingTask.IsCompletedSuccessfully);

            var valRgx = new Regex(@$"^\*2\r\n\${key3.Length}\r\n{key3}\r\n\$\d+\r\n(\d+)\r\n");
            var batchSize = 2;
            var batchCount = 0;
            var blockingTaskCount = 100;
            var tasks = new Task[blockingTaskCount];
            var retrieved = new bool[blockingTaskCount];

            for (var i = 0; i < blockingTaskCount; i++)
            {
                tasks[i] = taskFactory.StartNew(() =>
                {
                    using var lcr = TestUtils.CreateRequest();
                    var tResponse = lcr.SendCommand($"{blockingCmd} {key3} 10", 3);
                    var match = valRgx.Match(Encoding.ASCII.GetString(tResponse));
                    Assert.IsTrue(match.Success && match.Groups.Count > 1);
                    Assert.IsTrue(int.TryParse(match.Groups[1].Value, out var val));
                    Assert.GreaterOrEqual(val, 0);
                    Assert.Less(val, blockingTaskCount);
                    Assert.IsFalse(retrieved[val]);
                    retrieved[val] = true;
                });

                if ((i > 0 && i % batchSize == 0) || i == blockingTaskCount - 1)
                {
                    Debug.WriteLine($"{batchCount * batchSize},{Math.Min((batchCount + 1) * batchSize, blockingTaskCount)}");

                    for (var j = batchCount * batchSize; j < Math.Min((batchCount + 1) * batchSize, blockingTaskCount); j++)
                    {
                        using var lcr = TestUtils.CreateRequest();
                        lcr.SendCommand($"LPUSH {key3} {j}");
                    }

                    batchCount++;
                }
            }

            try
            {
                Task.WaitAll(tasks, timeout);
            }
            catch (AggregateException)
            {
                Assert.Fail();
            }

            Assert.IsTrue(tasks.All(t => t.IsCompletedSuccessfully));
            Assert.IsTrue(retrieved.All(r => r));
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
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand($"BLMOVE {srcKey1} {dstKey1} {OperationDirection.Right} {OperationDirection.Left} 10");
            expectedResponse = $"${value1.Length}\r\n{value1}\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand($"LRANGE {srcKey1} 0 -1");
            expectedResponse = $"*0\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand($"LRANGE {dstKey1} 0 -1", 2);
            expectedResponse = $"*1\r\n${value1.Length}\r\n{value1}\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            var blockingTask = taskFactory.StartNew(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var btResponse = lcr.SendCommand($"BLMOVE {srcKey2} {dstKey2} {OperationDirection.Left} {OperationDirection.Right} 0");
                var btExpectedResponse = $"${value2.Length}\r\n{value2}\r\n";
                var btActualValue = Encoding.ASCII.GetString(btResponse).Substring(0, btExpectedResponse.Length);
                Assert.AreEqual(btExpectedResponse, btActualValue);
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
                Task.WaitAll(new[] { blockingTask, releasingTask }, timeout);
            }
            catch (AggregateException)
            {
                Assert.Fail();
            }

            Assert.IsTrue(blockingTask.IsCompletedSuccessfully);
            Assert.IsTrue(releasingTask.IsCompletedSuccessfully);

            response = lightClientRequest.SendCommand($"LRANGE {srcKey2} 0 -1");
            expectedResponse = $"*0\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand($"LRANGE {dstKey2} 0 -1", 2);
            expectedResponse = $"*1\r\n${value2.Length}\r\n{value2}\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
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
            string actualValue;

            using var lightClientRequest = TestUtils.CreateRequest();
            for (var i = 0; i < keys.Length; i++)
            {
                response = lightClientRequest.SendCommand($"LPUSH {keys[i]} {values[i]}");
                expectedResponse = ":1\r\n";
                actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
                Assert.AreEqual(expectedResponse, actualValue);
            }

            for (var i = 0; i < keys.Length; i++)
            {
                response = lightClientRequest.SendCommand($"{blockingCmd} {string.Join(' ', keys)} 10", 3);
                expectedResponse = $"*2\r\n${keys[i].Length}\r\n{keys[i]}\r\n${values[i].Length}\r\n{values[i]}\r\n";
                actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
                Assert.AreEqual(expectedResponse, actualValue);
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
                var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
                Assert.AreEqual(expectedResponse, actualValue);

                response = lcr.SendCommand($"LLEN {key}");
                expectedResponse = ":1\r\n";
                actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
                Assert.AreEqual(expectedResponse, actualValue);

                response = lcr.SendCommand($"LPOP {key}");
                expectedResponse = $"${value1.Length}\r\n{value1}\r\n";
                actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
                Assert.AreEqual(expectedResponse, actualValue);
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
                Task.WaitAll(new[] { blockingTask, releasingTask }, timeout);
            }
            catch (AggregateException)
            {
                Assert.Fail();
            }

            Assert.IsTrue(blockingTask.IsCompletedSuccessfully);
            Assert.IsTrue(releasingTask.IsCompletedSuccessfully);
        }
    }
}