// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.client;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class RespListGarnetClientTests : AllureTestBase
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

        private static object[] ListRangeTestCases =
        [
            new object[] { 0, -1, new string[] { "foo", "bar", "baz" } },
            new object[] { 0, 0, new string[] { "foo" } },
            new object[] { 1, 2, new string[] { "bar", "baz" } },
            new object[] { -3, 1, new string[] { "foo", "bar" } },
            new object[] { -3, 2, new string[] { "foo", "bar", "baz" } },
            new object[] { -100, 100, new string[] { "foo", "bar", "baz" } }
        ];

        [Test]
        [TestCaseSource(nameof(LeftPushTestCases))]
        public async Task AddElementsToTheListHeadInBulk_WithCallback(string key, string[] elements, string[] expectedList)
        {
            // Arrange
            using var db = new GarnetClient(TestUtils.EndPoint);
            await db.ConnectAsync();

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
            ClassicAssert.IsTrue(isResultSet);
            ClassicAssert.AreEqual(expectedList.Length, actualListLength);

            await ValidateListContentAsync(db, testKey, expectedList);
        }

        [Test]
        [TestCaseSource(nameof(LeftPushTestCases))]
        public async Task AddElementsToTheListHead_WithCallback(string key, string[] elements, string[] expectedList)
        {
            // Arrange
            using var db = new GarnetClient(TestUtils.EndPoint);
            await db.ConnectAsync();

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

            ClassicAssert.IsTrue(isResultSet);
            ClassicAssert.AreEqual(expectedList.Length, actualListLength);

            await ValidateListContentAsync(db, testKey, expectedList);
        }

        [Test]
        [TestCaseSource(nameof(LeftPushTestCases))]
        public async Task AddElementsToTheListHead_WithAsync(string key, string[] elements, string[] expectedList)
        {
            // Arrange
            using var db = new GarnetClient(TestUtils.EndPoint);
            await db.ConnectAsync();

            // Act & Assert
            var testKey = GetTestKey(key);

            var actualListLength = await db.ListLeftPushAsync(testKey, elements);
            ClassicAssert.AreEqual(expectedList.Length, actualListLength);

            await ValidateListContentAsync(db, testKey, expectedList);
        }

        [Test]
        [TestCaseSource(nameof(RightPushTestCases))]
        public async Task AddElementsToListTailInBulk_WithCallback(string key, string[] elements, string[] expectedList)
        {
            // Arrange
            using var db = new GarnetClient(TestUtils.EndPoint);
            await db.ConnectAsync();

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
            ClassicAssert.IsTrue(isResultSet);
            ClassicAssert.AreEqual(expectedList.Length, actualListLength);

            await ValidateListContentAsync(db, testKey, expectedList);
        }

        [Test]
        [TestCaseSource(nameof(RightPushTestCases))]
        public async Task AddElementsToListTail_WithCallback(string key, string[] elements, string[] expectedList)
        {
            // Arrange
            using var db = new GarnetClient(TestUtils.EndPoint);
            await db.ConnectAsync();

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

            ClassicAssert.IsTrue(isResultSet);
            ClassicAssert.AreEqual(expectedList.Length, actualListLength);

            await ValidateListContentAsync(db, testKey, expectedList);
        }

        [Test]
        [TestCaseSource(nameof(RightPushTestCases))]
        public async Task AddElementsToTheListTail_WithAsync(string key, string[] elements, string[] expectedList)
        {
            // Arrange
            using var db = new GarnetClient(TestUtils.EndPoint);
            await db.ConnectAsync();

            // Act & Assert
            var testKey = GetTestKey(key);

            var actualListLength = await db.ListRightPushAsync(testKey, [.. elements]);
            ClassicAssert.AreEqual(expectedList.Length, actualListLength);

            await ValidateListContentAsync(db, testKey, expectedList);
        }

        [Test]
        [TestCaseSource(nameof(ListRangeTestCases))]
        public async Task GetListElements(int start, int stop, string[] expectedValues)
        {
            // Arrange
            var testKey = GetTestKey("list1");
            using var db = new GarnetClient(TestUtils.EndPoint);
            await db.ConnectAsync();

            await db.KeyDeleteAsync([testKey]);
            await db.ExecuteForStringResultAsync("RPUSH", [testKey, "foo", "bar", "baz"]);

            // Act
            var values = await db.ListRangeAsync(testKey, start, stop);

            // Assert
            ClassicAssert.False(expectedValues.Length == 0);
            ClassicAssert.AreEqual(expectedValues, values);
        }

        [Test]
        public async Task GetListLength()
        {
            // Arrange
            var testKey = GetTestKey("list1");
            using var db = new GarnetClient(TestUtils.EndPoint);
            await db.ConnectAsync();

            await db.KeyDeleteAsync([testKey]);
            await db.ExecuteForStringResultAsync("RPUSH", [testKey, "foo", "bar", "baz"]);

            // Act
            var length = await db.ListLengthAsync(testKey);

            // Assert
            ClassicAssert.AreEqual(3, length);
        }

        private static string GetTestKey(string key)
        {
            var testName = TestContext.CurrentContext.Test.MethodName;
            return $"{testName}_{key}";
        }

        private static async Task ValidateListContentAsync(GarnetClient db, string key, string[] expectedList)
        {
            var actualElements = await db.ListRangeAsync(key, 0, -1);

            ClassicAssert.AreEqual(expectedList.Length, actualElements.Length);

            for (var i = 0; i < actualElements.Length; i++)
            {
                ClassicAssert.AreEqual(expectedList[i], actualElements[i].ToString());
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