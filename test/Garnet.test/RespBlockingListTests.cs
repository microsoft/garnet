// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
            var key = "blocking_list_test";
            var value = "myval";

            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommand($"LPUSH {key} {value}");
            var expectedResponse = ":1\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand($"BRPOP {key} 3");
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);

            var blockingTask = taskFactory.StartNew(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                return lcr.SendCommand($"BRPOP {key} 3");
            });

            var releasingTask = taskFactory.StartNew(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                Task.Delay(TimeSpan.FromSeconds(1)).Wait();
                return lcr.SendCommand($"LPUSH {key} {value}");
            });

            var result = blockingTask.Result;
        }
    }
}
