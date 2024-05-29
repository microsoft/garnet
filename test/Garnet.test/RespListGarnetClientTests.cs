// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespListGarnetClientTests
    {
        private GarnetServer server;

        [OneTimeSetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();
        }

        private static object[] LeftPushTestCases =
        [
            new object[] { "list1", new[] { "foo" }, new[] { "foo" } },
            new object[] { "list2", new[] { "foo", "baz" }, new[] { "baz", "foo" } },
            new object[] { "list1", new[] { "bar", "baz" }, new[] { "baz", "bar", "foo" } },
            new object[] { "list3", new[] { "foo", "bar", "baz" }, new[] { "baz", "bar", "foo" } },
            new object[] { "list2", new[] { "foo", "bar", "baz" }, new[] { "baz", "bar", "foo", "baz", "foo" } }
        ];

        private static object[] RightPushTestCases =
        [
            new object[] { "list1", new[] { "foo" }, new[] { "foo" } },
            new object[] { "list2", new[] { "foo", "baz" }, new[] { "foo", "baz" } },
            new object[] { "list1", new[] { "bar", "baz" }, new[] { "foo", "bar", "baz" } },
            new object[] { "list3", new[] { "foo", "bar", "baz" }, new[] { "foo", "bar", "baz" } },
            new object[] { "list2", new[] { "foo", "bar", "baz" }, new[] { "foo", "baz", "foo", "bar", "baz" } }
        ];

        private static string GetTestKey(string key)
        {
            var testName = TestContext.CurrentContext.Test.MethodName;
            return $"{testName}_{key}";
        }

        [Test]
        [TestCaseSource(nameof(LeftPushTestCases))]
        public void AddElementsToTheListHeadInBulk_WithCallback(string key, string[] elements, string[] expectedList)
        {
            // Arrange
            using var db = new GarnetClient(TestUtils.Address, TestUtils.Port);
            db.Connect();

            using ManualResetEventSlim e = new();

            var isResultSet = false;
            var actualListLength = 0L;

            // Act & Assert
            var testKey = GetTestKey(key);

            db.ListLeftPush(testKey, elements, (_, returnValue, _) =>
            {
                actualListLength = returnValue;
                isResultSet = true;
                e.Set();
            });

            e.Wait();
            Assert.IsTrue(isResultSet);
            Assert.AreEqual(expectedList.Length, actualListLength);

            ValidateListContent(testKey, expectedList);
        }

        [Test]
        [TestCaseSource(nameof(LeftPushTestCases))]
        public void AddElementsToTheListHead_WithCallback(string key, string[] elements, string[] expectedList)
        {
            // Arrange
            using var db = new GarnetClient(TestUtils.Address, TestUtils.Port);
            db.Connect();

            using ManualResetEventSlim e = new();

            var isResultSet = false;
            var actualListLength = 0L;

            // Act & Assert
            var testKey = GetTestKey(key);

            foreach (var element in elements)
            {
                db.ListLeftPush(testKey, element, (_, returnValue, _) =>
                {
                    actualListLength = returnValue;
                    isResultSet = true;
                    e.Set();
                });

                e.Wait();
                e.Reset();
            }

            Assert.IsTrue(isResultSet);
            Assert.AreEqual(expectedList.Length, actualListLength);

            ValidateListContent(testKey, expectedList);
        }

        [Test]
        [TestCaseSource(nameof(LeftPushTestCases))]
        public async Task AddElementsToTheListHead_WithAsync(string key, string[] elements, string[] expectedList)
        {
            // Arrange
            using var db = new GarnetClient(TestUtils.Address, TestUtils.Port);
            await db.ConnectAsync();

            // Act & Assert
            var testKey = GetTestKey(key);

            var actualListLength = await db.ListLeftPushAsync(testKey, elements);
            Assert.AreEqual(expectedList.Length, actualListLength);

            ValidateListContent(testKey, expectedList);
        }

        [Test]
        [TestCaseSource(nameof(RightPushTestCases))]
        public void AddElementsToListTailInBulk_WithCallback(string key, string[] elements, string[] expectedList)
        {
            // Arrange
            using var db = new GarnetClient(TestUtils.Address, TestUtils.Port);
            db.Connect();

            using ManualResetEventSlim e = new();

            var isResultSet = false;
            var actualListLength = 0L;

            // Act & Assert
            var testKey = GetTestKey(key);

            db.ListRightPush(testKey, elements, (_, returnValue, _) =>
            {
                actualListLength = returnValue;
                isResultSet = true;
                e.Set();
            });

            e.Wait();
            Assert.IsTrue(isResultSet);
            Assert.AreEqual(expectedList.Length, actualListLength);

            ValidateListContent(testKey, expectedList);
        }

        [Test]
        [TestCaseSource(nameof(RightPushTestCases))]
        public void AddElementsToListTail_WithCallback(string key, string[] elements, string[] expectedList)
        {
            // Arrange
            using var db = new GarnetClient(TestUtils.Address, TestUtils.Port);
            db.Connect();

            using ManualResetEventSlim e = new();

            var isResultSet = false;
            var actualListLength = 0L;

            // Act & Assert
            var testKey = GetTestKey(key);

            foreach (var element in elements)
            {
                db.ListRightPush(testKey, element, (_, returnValue, _) =>
                {
                    actualListLength = returnValue;
                    isResultSet = true;
                    e.Set();
                });

                e.Wait();
                e.Reset();
            }

            Assert.IsTrue(isResultSet);
            Assert.AreEqual(expectedList.Length, actualListLength);

            ValidateListContent(testKey, expectedList);
        }

        [Test]
        [TestCaseSource(nameof(RightPushTestCases))]
        public async Task AddElementsToTheListTail_WithAsync(string key, string[] elements, string[] expectedList)
        {
            // Arrange
            using var db = new GarnetClient(TestUtils.Address, TestUtils.Port);
            await db.ConnectAsync();

            // Act & Assert
            var testKey = GetTestKey(key);

            var actualListLength = await db.ListRightPushAsync(testKey, elements.ToArray());
            Assert.AreEqual(expectedList.Length, actualListLength);

            ValidateListContent(testKey, expectedList);
        }

        private void ValidateListContent(string key, string[] expectedList)
        {
            // Using SE.Redis client to validate list content since Garnet client doesn't yet support LRANGE
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var actualElements = db.ListRange(key);
            Assert.AreEqual(expectedList.Length, actualElements.Length);
            for (var i = 0; i < actualElements.Length; i++)
            {
                Assert.AreEqual(expectedList[i], actualElements[i].ToString());
            }
        }

        [OneTimeTearDown]
        public void TearDown()
        {
            server.Dispose();

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }
    }
}