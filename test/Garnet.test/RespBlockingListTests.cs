// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using NUnit.Framework;
using StackExchange.Redis;

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
        public void BasicListBlockingPopTest()
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

            response = lightClientRequest.SendCommand($"BRPOP {key} 10");
            expectedResponse = $"${value.Length}\r\n{value}\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            var blockingTask = taskFactory.StartNew(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var response = lcr.SendCommand($"BRPOP {key2} 30");
                var expectedResponse = $"${value2.Length}\r\n{value2}\r\n";
                var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
                Assert.AreEqual(expectedResponse, actualValue);
            });

            var releasingTask = taskFactory.StartNew(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                Task.Delay(TimeSpan.FromSeconds(2)).Wait();
                return lcr.SendCommand($"LPUSH {key2} {value2}");
            });

            Task.WaitAll(new[] { blockingTask, releasingTask });

            var valRgx = new Regex(@"^\$\d+\r\n(\d+)\r\n");
            var batchSize = Environment.ProcessorCount / 2;
            var batchCount = 0;
            var blockingTaskCount = 100;
            var tasks = new Task[blockingTaskCount];
            var retrieved = new bool[blockingTaskCount];

            for (var i = 0; i < blockingTaskCount; i++)
            {
                tasks[i] = taskFactory.StartNew(() =>
                {
                    using var lcr = TestUtils.CreateRequest();
                    var response = lcr.SendCommand($"BRPOP {key3} 10");
                    var match = valRgx.Match(Encoding.ASCII.GetString(response));
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

            Task.WaitAll(tasks);

            Assert.IsTrue(retrieved.All(r => r));
        }
    }
}
