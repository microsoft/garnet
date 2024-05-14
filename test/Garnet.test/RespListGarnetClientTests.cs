// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using NUnit.Framework;

namespace Garnet.test
{
    [TestFixture]
    public class RespListGarnetClientTests
    {
        private GarnetServer _server;

        [OneTimeSetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            _server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            _server.Start();
        }

        [Test]
        public void AddElementsToTheListHead_WithCallback()
        {
            // Arrange
            using var db = new GarnetClient(TestUtils.Address, TestUtils.Port);
            db.Connect();

            ManualResetEventSlim e = new();

            // Act & Assert
            db.ListLeftPush("list1", "foo", (_, returnValue, _) =>
            {
                Assert.IsTrue(1 == returnValue);
                e.Set();
            });

            e.Wait();
            e.Reset();

            db.ListLeftPush("list1", ["bar", "baz"], (_, returnValue, _) =>
            {
                Assert.IsTrue(3 == returnValue);
                e.Set();
            });

            e.Wait();
            e.Reset();

            db.ListLeftPush("list2", ["foo", "baz"], (_, returnValue, _) =>
            {
                Assert.IsTrue(2 == returnValue);
                e.Set();
            });

            e.Wait();
            e.Reset();
            e.Dispose();
        }

        [Test]
        [TestCase("list3", new string[] { "foo", "bar" }, 2)]
        [TestCase("list4", new string[] { "foo", "bar", "baz" }, 3)]
        [TestCase("list3", new string[] { "baz" }, 3)]
        [TestCase("list5", new string[] { "foo" }, 1)]
        public async Task AddElementsToTheListHead_ReturnsListItemsCount(string key, string[] elements, int result)
        {
            // Arrange
            using var db = new GarnetClient(TestUtils.Address, TestUtils.Port);
            db.Connect();

            // Act & Assert
            Assert.AreEqual(result, await db.ListLeftPushAsync(key, elements));
        }

        [Test]
        public void AddElementsToListTail_WithCallback()
        {
            // Arrange
            using var db = new GarnetClient(TestUtils.Address, TestUtils.Port);
            db.Connect();

            ManualResetEventSlim e = new();

            // Act & Assert
            db.ListRightPush("list6", "foo", (_, returnValue, _) =>
            {
                Assert.IsTrue(1 == returnValue);
                e.Set();
            });

            e.Wait();
            e.Reset();

            db.ListRightPush("list6", ["bar", "baz"], (_, returnValue, _) =>
            {
                Assert.IsTrue(3 == returnValue);
                e.Set();
            });

            e.Wait();
            e.Reset();

            db.ListRightPush("list7", ["foo", "baz"], (_, returnValue, _) =>
            {
                Assert.IsTrue(2 == returnValue);
                e.Set();
            });

            e.Wait();
            e.Reset();
            e.Dispose();
        }

        [Test]
        [TestCase("list8", new string[] { "foo", "bar" }, 2)]
        [TestCase("list9", new string[] { "foo", "bar", "baz" }, 3)]
        [TestCase("list8", new string[] { "baz" }, 3)]
        [TestCase("list10", new string[] { "foo" }, 1)]
        public async Task AddElementsToTheListTail_ReturnsListItemsCount(string key, string[] elements, int result)
        {
            // Arrange
            using var db = new GarnetClient(TestUtils.Address, TestUtils.Port);
            db.Connect();

            // Act & Assert
            Assert.AreEqual(result, await db.ListRightPushAsync(key, elements));
        }

        [OneTimeTearDown]
        public void TearDown()
        {
            _server.Dispose();

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }
    }
}
