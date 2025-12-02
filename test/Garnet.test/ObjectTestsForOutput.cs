// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System.Text;
using System.Threading.Tasks;
using Allure.NUnit;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{

    [AllureNUnit]
    [TestFixture]
    public class ObjectTestsForOutput : AllureTestBase
    {
        protected GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: false);
            server.Start();
        }


        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        #region sortedset

        [Test]
        [TestCase(100)]
        [TestCase(131042)]
        [TestCase(131049)]
        [TestCase(131056)]
        [TestCase(131061)]
        public async Task CanUseZRangeWithLeftOverBuffer(int size)
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            string key = "mykey";
            var valueStr = await CreateKeyOfSize(size, key);

            string zaddResult = await c.ExecuteAsync("ZADD", "cities", "100000", "Delhi", "850000", "Mumbai", "700000", "Hyderabad", "800000", "Kolkata");
            ClassicAssert.AreEqual("4", zaddResult);

            var t1 = c.ExecuteAsyncBatch("GET", key); // feed partial batch without flushing

            var batch2 = c.ExecuteForArrayAsync("ZRANGE", "cities", "0", "-1");

            var batch3Result = await c.ExecuteForArrayAsync("ZRANGE", "cities", "0", "-1");

            var batch1Result = await t1;
            ClassicAssert.AreEqual(valueStr, batch1Result);

            var batch2Result = await batch2;
            ClassicAssert.AreEqual(4, batch2Result.Length);
            ClassicAssert.AreEqual("Delhi", batch2Result[0]);
            ClassicAssert.AreEqual("Hyderabad", batch2Result[1]);
            ClassicAssert.AreEqual("Kolkata", batch2Result[2]);
            ClassicAssert.AreEqual("Mumbai", batch2Result[3]);

            ClassicAssert.AreEqual(4, batch3Result.Length);
            ClassicAssert.AreEqual("Delhi", batch3Result[0]);
            ClassicAssert.AreEqual("Hyderabad", batch3Result[1]);
            ClassicAssert.AreEqual("Kolkata", batch3Result[2]);
            ClassicAssert.AreEqual("Mumbai", batch3Result[3]);

            var batch4Result = await c.ExecuteForArrayAsync("ZRANGE", "cities", "0", "-1");
            ClassicAssert.AreEqual(4, batch4Result.Length);
            ClassicAssert.AreEqual("Delhi", batch4Result[0]);
            ClassicAssert.AreEqual("Hyderabad", batch4Result[1]);
            ClassicAssert.AreEqual("Kolkata", batch4Result[2]);
            ClassicAssert.AreEqual("Mumbai", batch4Result[3]);
        }


        [Test]
        [TestCase(100)]
        [TestCase(131042)]
        [TestCase(131049)]
        [TestCase(131056)]
        [TestCase(131061)]
        public async Task CanUseZPopMaxWithLeftOverBuffer(int size)
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            string key = "mykey";
            var valueStr = await CreateKeyOfSize(size, key);

            string zaddResult = await c.ExecuteAsync("ZADD", "cities", "100000", "Delhi", "850000", "Mumbai", "700000", "Hyderabad", "800000", "Kolkata");
            ClassicAssert.AreEqual("4", zaddResult);

            var t1 = c.ExecuteAsyncBatch("GET", key); // feed partial batch without flushing

            var batch2 = c.ExecuteForArrayAsync("ZPOPMAX", "cities");

            var batch1Result = await t1;
            ClassicAssert.AreEqual(valueStr, batch1Result);

            var batch3Result = await c.ExecuteForArrayAsync("ZPOPMAX", "cities");

            var batch2Result = await batch2;
            ClassicAssert.AreEqual(2, batch2Result.Length);
            ClassicAssert.AreEqual("Mumbai", batch2Result[0]);
            ClassicAssert.AreEqual("850000", batch2Result[1]);

            ClassicAssert.AreEqual(2, batch3Result.Length);
            ClassicAssert.AreEqual("Kolkata", batch3Result[0]);
            ClassicAssert.AreEqual("800000", batch3Result[1]);

            var batch4Result = await c.ExecuteForArrayAsync("ZPOPMAX", "cities");
            ClassicAssert.AreEqual(2, batch4Result.Length);
            ClassicAssert.AreEqual("Hyderabad", batch4Result[0]);
            ClassicAssert.AreEqual("700000", batch4Result[1]);
        }

        #endregion

        #region hashmaps
        [Test]
        [TestCase(100)]
        [TestCase(131042)]
        [TestCase(131049)]
        [TestCase(131056)]
        [TestCase(131061)]
        public async Task CanUseHGETWithLeftOverBuffer(int size)
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            var valueStr = await CreateKeyOfSize(size, "mykey");

            string zaddResult = await c.ExecuteAsync("HSET", "myhash", "field1", "field1value", "field2", "field2value", "field3", "field3value", "field4", "field4value");
            ClassicAssert.AreEqual("4", zaddResult);

            var t1 = c.ExecuteAsyncBatch("GET", "mykey"); // feed partial batch without flushing

            var t2 = c.ExecuteForArrayAsyncBatch("HMGET", "myhash", "field1", "field2");

            var t3Result = await c.ExecuteForArrayAsync("HMGET", "myhash", "field1", "field2");

            var t1Result = await t1;
            ClassicAssert.AreEqual(valueStr, t1Result);

            var t2Result = await t2;
            ClassicAssert.AreEqual(2, t2Result.Length);
            ClassicAssert.AreEqual("field1value", t2Result[0]);
            ClassicAssert.AreEqual("field2value", t2Result[1]);

            ClassicAssert.AreEqual(2, t3Result.Length);
            ClassicAssert.AreEqual("field1value", t3Result[0]);
            ClassicAssert.AreEqual("field2value", t3Result[1]);

            var t4Result = await c.ExecuteForArrayAsync("HMGET", "myhash", "field1", "field2");
            ClassicAssert.AreEqual(2, t4Result.Length);
            ClassicAssert.AreEqual("field1value", t4Result[0]);
            ClassicAssert.AreEqual("field2value", t4Result[1]);
        }


        // This is the Local test one
        [Test]
        [TestCase(100)]
        [TestCase(131042)]
        [TestCase(131049)]
        [TestCase(131056)]
        [TestCase(131061)]
        public async Task CanUseHKEYSWithLeftOverBuffer(int size)
        {

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            var valueStr = await CreateKeyOfSize(size, "mykey");

            string zaddResult = await c.ExecuteAsync("HSET", "myhash", "field1", "field1value", "field2", "field2value", "field3", "field3value", "field4", "field4value");
            ClassicAssert.AreEqual("4", zaddResult);

            var t1 = c.ExecuteAsyncBatch("GET", "mykey"); // feed partial batch without flushing

            var t2 = c.ExecuteForArrayAsyncBatch("HKEYS", "myhash");

            var t3Result = await c.ExecuteForArrayAsync("HKEYS", "myhash");

            var t1Result = await t1;
            ClassicAssert.AreEqual(valueStr, t1Result);

            var t2Result = await t2;
            ClassicAssert.AreEqual(4, t2Result.Length);
            ClassicAssert.AreEqual("field1", t2Result[0]);
            ClassicAssert.AreEqual("field2", t2Result[1]);
            ClassicAssert.AreEqual("field3", t2Result[2]);
            ClassicAssert.AreEqual("field4", t2Result[3]);

            ClassicAssert.AreEqual(4, t3Result.Length);
            ClassicAssert.AreEqual("field1", t3Result[0]);
            ClassicAssert.AreEqual("field2", t3Result[1]);
            ClassicAssert.AreEqual("field3", t3Result[2]);
            ClassicAssert.AreEqual("field4", t3Result[3]);


            var t4Result = await c.ExecuteForArrayAsync("HKEYS", "myhash");
            ClassicAssert.AreEqual(4, t4Result.Length);
            ClassicAssert.AreEqual("field1", t4Result[0]);
            ClassicAssert.AreEqual("field2", t4Result[1]);
            ClassicAssert.AreEqual("field3", t4Result[2]);
            ClassicAssert.AreEqual("field4", t4Result[3]);
        }

        #endregion

        #region lists

        [Test]
        [TestCase(100)]
        [TestCase(131042)]
        [TestCase(131049)]
        [TestCase(131056)]
        [TestCase(131061)]
        public async Task CanUseLPOPWithLeftOverBuffer(int size)
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            int length = size;
            var value = new byte[length];

            for (int i = 0; i < length; i++)
                value[i] = (byte)((byte)'a' + ((byte)i % 26));

            var valueStr = Encoding.ASCII.GetString(value);

            string setResult = await c.ExecuteAsync("SET", "mykey", valueStr);
            ClassicAssert.AreEqual("OK", setResult);

            string zaddResult = await c.ExecuteAsync("LPUSH", "cities", "Delhi", "Mumbai", "Hyderabad", "Kolkata");
            ClassicAssert.AreEqual("4", zaddResult);

            var t1 = c.ExecuteAsyncBatch("GET", "mykey"); // feed partial batch without flushing

            var t2 = await c.ExecuteAsync("LPOP", "cities");

            var getResult = await t1;
            ClassicAssert.AreEqual(valueStr, getResult);
            ClassicAssert.AreEqual("Kolkata", t2);

            var t3 = await c.ExecuteAsync("LPOP", "cities");
            ClassicAssert.AreEqual("Hyderabad", t3);
        }

        [Test]
        [TestCase(100)]
        [TestCase(131042)]
        [TestCase(131049)]
        [TestCase(131056)]
        [TestCase(131061)]
        public async Task CanUseLRangeWithLeftOverBuffer(int size)
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            string key = "mykey";
            var valueStr = await CreateKeyOfSize(size, key);

            var expectedResponse = "5";
            var actualValue = await c.ExecuteAsync("RPUSH", "mylist", "heads", "obverse", "tails", "reverse", "edge");
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            var batch1 = c.ExecuteAsyncBatch("GET", key);

            var batch2 = c.ExecuteForArrayAsync("LRANGE", "mylist", "0", "-1");

            var batch3Result = await c.ExecuteForArrayAsync("LRANGE", "mylist", "0", "-1");

            var batch1Result = await batch1;
            ClassicAssert.AreEqual(valueStr, batch1Result);

            var batch2Result = await batch2;
            ClassicAssert.AreEqual(5, batch2Result.Length);
            ClassicAssert.AreEqual("heads", batch2Result[0]);
            ClassicAssert.AreEqual("obverse", batch2Result[1]);
            ClassicAssert.AreEqual("tails", batch2Result[2]);
            ClassicAssert.AreEqual("reverse", batch2Result[3]);
            ClassicAssert.AreEqual("edge", batch2Result[4]);

            ClassicAssert.AreEqual(5, batch3Result.Length);
            ClassicAssert.AreEqual("heads", batch3Result[0]);
            ClassicAssert.AreEqual("obverse", batch3Result[1]);
            ClassicAssert.AreEqual("tails", batch3Result[2]);
            ClassicAssert.AreEqual("reverse", batch3Result[3]);
            ClassicAssert.AreEqual("edge", batch3Result[4]);

            var batch4Result = await c.ExecuteForArrayAsync("LRANGE", "mylist", "0", "-1");
            ClassicAssert.AreEqual(5, batch4Result.Length);
            ClassicAssert.AreEqual("heads", batch4Result[0]);
            ClassicAssert.AreEqual("obverse", batch4Result[1]);
            ClassicAssert.AreEqual("tails", batch4Result[2]);
            ClassicAssert.AreEqual("reverse", batch4Result[3]);
            ClassicAssert.AreEqual("edge", batch4Result[4]);
        }

        #endregion

        #region sets

        [Test]
        [TestCase(100)]
        [TestCase(131042)]
        [TestCase(131049)]
        [TestCase(131056)]
        [TestCase(131061)]
        public async Task CanUseSMembersWithLeftOverBuffer(int size)
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();
            string key = "mykey";
            var valueStr = await CreateKeyOfSize(size, key);

            var expectedResponse = "5";
            var actualValue = await c.ExecuteAsync("SADD", "myset", "heads", "obverse", "tails", "reverse", "edge");
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            var batch1 = c.ExecuteAsyncBatch("GET", key);

            var batch2 = c.ExecuteForArrayAsyncBatch("SMEMBERS", "myset");

            var batch3Result = await c.ExecuteForArrayAsync("SMEMBERS", "myset");

            var batch1Result = await batch1;
            ClassicAssert.AreEqual(valueStr, batch1Result);

            var batch2Result = await batch2;
            ClassicAssert.AreEqual(5, batch2Result.Length);
            ClassicAssert.AreEqual("heads", batch2Result[0]);
            ClassicAssert.AreEqual("obverse", batch2Result[1]);
            ClassicAssert.AreEqual("tails", batch2Result[2]);
            ClassicAssert.AreEqual("reverse", batch2Result[3]);
            ClassicAssert.AreEqual("edge", batch2Result[4]);

            ClassicAssert.AreEqual(5, batch3Result.Length);
            ClassicAssert.AreEqual("heads", batch3Result[0]);
            ClassicAssert.AreEqual("obverse", batch3Result[1]);
            ClassicAssert.AreEqual("tails", batch3Result[2]);
            ClassicAssert.AreEqual("reverse", batch3Result[3]);
            ClassicAssert.AreEqual("edge", batch3Result[4]);

            var batch4Result = await c.ExecuteForArrayAsync("SMEMBERS", "myset");
            ClassicAssert.AreEqual(5, batch4Result.Length);
            ClassicAssert.AreEqual("heads", batch4Result[0]);
            ClassicAssert.AreEqual("obverse", batch4Result[1]);
            ClassicAssert.AreEqual("tails", batch4Result[2]);
            ClassicAssert.AreEqual("reverse", batch4Result[3]);
            ClassicAssert.AreEqual("edge", batch4Result[4]);
        }

        #endregion


        [Test]
        [TestCase(100)]
        [TestCase(131042)]
        [TestCase(131049)]
        [TestCase(131056)]
        [TestCase(131061)]
        public async Task CanUseGETWithLeftOverBuffer(int size)
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();
            string key = "mykey";
            var valueStr = await CreateKeyOfSize(size, key);

            var t1 = c.ExecuteAsyncBatch("GET", "mykey"); // feed partial batch without flushing
            var t2 = c.ExecuteAsync("GET", "mykey");

            var v1 = await t1;
            var v2 = await t2;

            ClassicAssert.AreEqual(valueStr, v1);
            ClassicAssert.AreEqual(valueStr, v2);
        }


        private static async Task<string> CreateKeyOfSize(int size, string name)
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            int length = size;
            var value = new byte[length];

            for (int i = 0; i < length; i++)
                value[i] = (byte)((byte)'a' + ((byte)i % 26));

            var valueStr = Encoding.ASCII.GetString(value);

            string setResult = await c.ExecuteAsync("SET", name, valueStr);
            ClassicAssert.AreEqual("OK", setResult);

            return valueStr;
        }
    }
}