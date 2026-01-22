// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespScanCommandsTests
    {
        GarnetServer server;
        private IReadOnlyDictionary<string, RespCommandsInfo> respCustomCommandsInfo;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            ClassicAssert.IsTrue(TestUtils.TryGetCustomCommandsInfo(out respCustomCommandsInfo));
            ClassicAssert.IsNotNull(respCustomCommandsInfo);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void SeDbsizeTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.StringSet(new RedisKey("keyOne"), new RedisValue("valueone"));
            db.StringSet(new RedisKey("keyTwo"), new RedisValue("valuetwo"));
            db.SortedSetAdd("keyThree", new RedisValue("valuethree"), 1, CommandFlags.None);
            var actualResponse = db.Execute("DBSIZE");
            ClassicAssert.AreEqual(3, ((ulong)actualResponse));
        }

        [Test]
        public void SeKeysTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // add keys in main store
            db.StringSet(new RedisKey("keyOne"), new RedisValue("valueone"));
            db.StringSet(new RedisKey("keyTwo"), new RedisValue("valuetwo"));
            db.StringSet(new RedisKey("Another"), new RedisValue("valueanother"));

            // one key object at object store
            db.SortedSetAdd("keyThree", new RedisValue("OneKey"), 1, CommandFlags.None);

            var actualResponse = db.Execute("KEYS", ["key*"]);
            ClassicAssert.AreEqual(3, ((RedisResult[])actualResponse).Length);
            var listKeys = new List<string>((string[])actualResponse);
            ClassicAssert.IsTrue(listKeys.Contains("keyOne"));
            ClassicAssert.IsTrue(listKeys.Contains("keyTwo"));
            ClassicAssert.IsTrue(listKeys.Contains("keyThree"));

            actualResponse = db.Execute("KEYS", ["*other*"]);
            ClassicAssert.AreEqual(1, ((RedisResult[])actualResponse).Length);

            listKeys = new List<string>((string[])actualResponse);
            ClassicAssert.IsTrue(listKeys.Contains("Another"));

            actualResponse = db.Execute("KEYS", ["*simple*"]);
            ClassicAssert.AreEqual(0, ((RedisResult[])actualResponse).Length);

            actualResponse = db.Execute("KEYS", ["key??"]);
            ClassicAssert.AreEqual(0, ((RedisResult[])actualResponse).Length);

            actualResponse = db.Execute("KEYS", ["*"]);
            ClassicAssert.AreEqual(4, ((RedisResult[])actualResponse).Length);
        }

        [Test]
        public void SeKeysCursorTest()
        {
            // Test a large number of keys
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // set 10_000 strings
            const int KeyCount = 10_000;
            for (int i = 0; i < KeyCount; i++)
                db.StringSet($"try:{i}", i);

            // get and count keys using SE Redis, using the default pageSizeStr of 250
            var server = redis.GetServers()[db.Database];
            var keyCount1 = server.Keys().ToArray().Length;
            ClassicAssert.AreEqual(KeyCount, keyCount1, "IServer.Keys()");

            // get and count keys using KEYS
            var res = db.Execute("KEYS", "*");
            var keyCount2 = ((RedisValue[])res!).Length;
            ClassicAssert.AreEqual(KeyCount, keyCount2, "KEYS *");
        }

        [Test]
        public void CanDoMemoryUsage()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // add keys in both stores
            db.StringSet(new RedisKey("keyOne"), new RedisValue("valueone"));
            db.SortedSetAdd(new RedisKey("myss"), [new SortedSetEntry("a", 1)]);
            db.ListLeftPush(new RedisKey("mylist"), [new RedisValue("1")]);
            db.SetAdd(new RedisKey("myset"), new RedisValue("elementone"));
            db.HashSet(new RedisKey("myhash"), [new HashEntry("a", "1")]);

            string[] data = ["a", "b", "c", "d", "e", "f"];
            string key = "hllKey";
            for (int i = 0; i < data.Length; i++)
            {
                db.HyperLogLogAdd(key, data[i]);
            }

            var r = db.Execute("MEMORY", ["USAGE", "keyOne"]);
            ClassicAssert.AreEqual("32", r.ToString());

            r = db.Execute("MEMORY", ["USAGE", "myss"]);
            ClassicAssert.AreEqual("376", r.ToString());

            r = db.Execute("MEMORY", ["USAGE", "mylist"]);
            ClassicAssert.AreEqual("208", r.ToString());

            r = db.Execute("MEMORY", ["USAGE", "myset"]);
            ClassicAssert.AreEqual("232", r.ToString());

            r = db.Execute("MEMORY", ["USAGE", "myhash"]);
            ClassicAssert.AreEqual("296", r.ToString());

            r = db.Execute("MEMORY", ["USAGE", "foo"]);
            ClassicAssert.IsTrue(r.IsNull);

            r = db.Execute("MEMORY", ["USAGE", "hllKey"]);
            ClassicAssert.AreEqual("296", r.ToString());
        }


        [Test]
        public void CanGetKeyType()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // add keys in both stores
            db.StringSet(new RedisKey("keyOne"), new RedisValue("valueone"));
            db.SortedSetAdd(new RedisKey("myss"), [new SortedSetEntry("a", 1)]);
            db.ListLeftPush(new RedisKey("mylist"), [new RedisValue("1")]);
            db.SetAdd(new RedisKey("myset"), new RedisValue("elementone"));
            db.HashSet(new RedisKey("myhash"), [new HashEntry("a", "1")]);

            var r = db.Execute("TYPE", ["keyOne"]);
            ClassicAssert.IsTrue(r.ToString() == "string");

            r = db.Execute("TYPE", ["myss"]);
            ClassicAssert.IsTrue(r.ToString() == "zset");

            r = db.Execute("TYPE", ["mylist"]);
            ClassicAssert.IsTrue(r.ToString() == "list");

            r = db.Execute("TYPE", ["myset"]);
            ClassicAssert.IsTrue(r.ToString() == "set");

            r = db.Execute("TYPE", ["myhash"]);
            ClassicAssert.IsTrue(r.ToString() == "hash");
        }

        [Test]
        public void CanUsePatternsInKeysTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            //h?llo matches hello, hallo and hxllo
            //h*llo matches hllo and heeeello
            //h[ae]llo matches hello and hallo, but not hillo
            //h[^e]llo matches hallo, hbllo, ... but not hello
            //h[a-b]llo matches hallo and hbllo
            db.StringSet(new RedisKey("hello"), new RedisValue("uno"));
            db.StringSet(new RedisKey("hallo"), new RedisValue("dos"));
            db.StringSet(new RedisKey("hxllo"), new RedisValue("tres"));
            db.StringSet(new RedisKey("hllo"), new RedisValue("four"));


            var actualResponse = db.Execute("KEYS", ["h?llo"]);
            ClassicAssert.AreEqual(3, ((RedisResult[])actualResponse).Length);
            var listKeys = new List<string>((string[])actualResponse);
            ClassicAssert.IsTrue(listKeys.Contains("hello"));
            ClassicAssert.IsTrue(listKeys.Contains("hallo"));
            ClassicAssert.IsTrue(listKeys.Contains("hxllo"));


            actualResponse = db.Execute("KEYS", ["h*llo"]);
            ClassicAssert.AreEqual(4, ((RedisResult[])actualResponse).Length);
            listKeys = new List<string>((string[])actualResponse);
            ClassicAssert.IsTrue(listKeys.Contains("hllo"));
            ClassicAssert.IsTrue(listKeys.Contains("hallo"));
            ClassicAssert.IsTrue(listKeys.Contains("hxllo"));
            ClassicAssert.IsTrue(listKeys.Contains("hello"));

            actualResponse = db.Execute("KEYS", ["h[ae]llo"]);
            ClassicAssert.AreEqual(2, ((RedisResult[])actualResponse).Length);
            listKeys = new List<string>((string[])actualResponse);
            ClassicAssert.IsTrue(listKeys.Contains("hallo"));
            ClassicAssert.IsTrue(listKeys.Contains("hello"));

            actualResponse = db.Execute("KEYS", ["h[^e]llo"]);
            ClassicAssert.AreEqual(2, ((RedisResult[])actualResponse).Length);
            listKeys = new List<string>((string[])actualResponse);
            ClassicAssert.IsTrue(listKeys.Contains("hallo"));
            ClassicAssert.IsTrue(listKeys.Contains("hxllo"));

            actualResponse = db.Execute("KEYS", ["h[a-b]llo"]);
            ClassicAssert.AreEqual(1, ((RedisResult[])actualResponse).Length);
            listKeys = new List<string>((string[])actualResponse);
            ClassicAssert.IsTrue(listKeys.Contains("hallo"));

        }

        [Test]
        public void SeKeysPatternTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.StringSet(new RedisKey("keyone"), new RedisValue("valueone"));
            db.StringSet(new RedisKey("keytwo"), new RedisValue("valuetwo"));
            db.StringSet(new RedisKey("keythree"), new RedisValue("valuethree"));
            var actualResponse = db.Execute("KEYS", ["*"]);
            ClassicAssert.AreEqual(3, ((RedisResult[])actualResponse).Length);
        }


        [Test]
        public void SeKeysPatternMatchingTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.StringSet(new RedisKey("he**"), new RedisValue("keyvalueone"));
            db.StringSet(new RedisKey(@"he\*\*"), new RedisValue("keyvaluetwo"));
            db.StringSet(new RedisKey(@"he\*\*foo"), new RedisValue("keyvaluethree"));
            db.StringSet(new RedisKey(@"he**foo"), new RedisValue("keyvaluefour"));

            var actualResponse = db.Execute("KEYS", [@"he\*\*"]);
            ClassicAssert.AreEqual(1, ((RedisResult[])actualResponse).Length);
            ClassicAssert.IsTrue(String.Equals(@"he**", (((RedisResult[])actualResponse)[0]).ToString()));

            actualResponse = db.Execute("KEYS", [@"he\\*\\*"]);
            ClassicAssert.AreEqual(2, ((RedisResult[])actualResponse).Length);
            ClassicAssert.IsTrue(String.Equals(@"he\*\*", (((RedisResult[])actualResponse)[0]).ToString()));
            ClassicAssert.IsTrue(String.Equals(@"he\*\*foo", (((RedisResult[])actualResponse)[1]).ToString()));

            actualResponse = db.Execute("KEYS", [@"he**"]);
            ClassicAssert.AreEqual(4, ((RedisResult[])actualResponse).Length);
        }

        [Test]
        public void SeKeysPatternMatchingTestVerbatim()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.StringSet(new RedisKey("he**"), new RedisValue("keyvalueone"));
            db.StringSet(new RedisKey(@"he**foo"), new RedisValue("keyvaluetwo"));

            var actualResponse = db.Execute("KEYS", [@"he\*\*"]);
            ClassicAssert.AreEqual(1, ((RedisResult[])actualResponse).Length);
            ClassicAssert.IsTrue(String.Equals(@"he**", (((RedisResult[])actualResponse)[0]).ToString()));

            actualResponse = db.Execute("KEYS", [@"he\\*\\*"]);
            ClassicAssert.AreEqual(0, ((RedisResult[])actualResponse).Length);

            db.StringSet(new RedisKey(@"\\bar"), new RedisValue("secondvalue"));
            actualResponse = db.Execute("KEYS", [@"\\bar"]);
            ClassicAssert.AreEqual(0, ((RedisResult[])actualResponse).Length);

            actualResponse = db.Execute("KEYS", [@"\\\bar"]);
            ClassicAssert.AreEqual(0, ((RedisResult[])actualResponse).Length);

            actualResponse = db.Execute("KEYS", [@"\\\\bar"]);
            ClassicAssert.AreEqual(1, ((RedisResult[])actualResponse).Length);
            ClassicAssert.IsTrue(String.Equals(@"\\bar", (((RedisResult[])actualResponse)[0]).ToString()));
        }


        [Test]
        public void CanUseScanWithEmptyStore()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var result = db.Execute("SCAN", "0");
            ClassicAssert.AreEqual(ResultType.BulkString, result[0].Resp2Type);
            _ = int.TryParse((string)((RedisValue[])((RedisResult[])result!)[0])[0], out var cursor);
            RedisValue[] keysMatch = ((RedisValue[])((RedisResult[])result!)[1]);
            ClassicAssert.IsTrue(cursor == 0);
            ClassicAssert.IsTrue(keysMatch.Length == 0);
        }


        [Test]
        public void CanUseScanWithMatch()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.StringSet(new RedisKey("hello"), new RedisValue("keyvalueone"));
            db.StringSet(new RedisKey("foo"), new RedisValue("keyvaluetwo"));
            var result = db.Execute("SCAN", "0", "MATCH", "*o*", "COUNT", "1000");
            ClassicAssert.AreEqual(ResultType.BulkString, result[0].Resp2Type);
            RedisValue[] keysMatch = (RedisValue[])((RedisResult[])result!)[1];
            ClassicAssert.True(keysMatch.Contains("foo") && keysMatch.Contains("hello"));
        }

        [Test]
        public void CanUseScanAllKeys()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var nKeys = 200;
            for (int i = 0; i < nKeys; i++)
            {
                db.StringSet(new RedisKey($"key:{i}"), new RedisValue($"keyvalue-{i}"));
            }

            int cursor = 0;
            var recordsReturned = 0;

            do
            {
                var result = db.Execute("SCAN", cursor.ToString());
                ClassicAssert.AreEqual(ResultType.BulkString, result[0].Resp2Type);
                _ = int.TryParse((string)((RedisValue[])((RedisResult[])result!)[0])[0], out cursor);
                RedisValue[] keysMatch = ((RedisValue[])((RedisResult[])result!)[1]);
                recordsReturned += keysMatch.Length;
            } while (cursor != 0);

            ClassicAssert.AreEqual(nKeys, recordsReturned);
        }

        [Test]
        public void CanUseScanKeysWithCount()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var nKeys = 200;
            var rnd = new Random();

            for (int i = 0; i < nKeys; i++)
            {
                db.StringSet(new RedisKey($"key:{i}"), new RedisValue($"keyvalue-{i}"));
            }

            int cursor = 0;
            var recordsReturned = 0;
            var count = rnd.Next(1, 20);

            do
            {
                var result = db.Execute("SCAN", cursor.ToString(), "COUNT", count);
                _ = int.TryParse((string)((RedisValue[])((RedisResult[])result!)[0])[0], out cursor);
                RedisValue[] keysMatch = ((RedisValue[])((RedisResult[])result!)[1]);
                recordsReturned += keysMatch.Length;
                count = rnd.Next(1, 20);
            } while (recordsReturned < nKeys);

            ClassicAssert.IsTrue(recordsReturned == nKeys);
        }

        [Test]
        public void CanUseScanKeysWithMatchAndCount()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var nKeys = 200;

            for (int i = 0; i < nKeys; i++)
            {
                db.StringSet(new RedisKey($"key:{i}"), new RedisValue($"keyvalue-{i}"));
            }

            int cursor = 0;
            var result = db.Execute("SCAN", cursor.ToString(), "MATCH", "*11*", "COUNT", 1000);
            _ = int.TryParse((string)((RedisValue[])((RedisResult[])result!)[0])[0], out cursor);
            RedisValue[] keysMatch = ((RedisValue[])((RedisResult[])result!)[1]);
            ClassicAssert.IsTrue(cursor == 0);
            ClassicAssert.IsTrue(keysMatch.Length == 11);
        }

        [Test]
        public void CanUseScanKeysCountAndStringType()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var nKeys = 100;

            for (int i = 0; i < nKeys; i++)
            {
                db.StringSet(new RedisKey($"key:{i}"), new RedisValue($"keyvalue-{i}"));
            }

            int cursor = 0;
            var result = db.Execute("SCAN", cursor.ToString(), "TYPE", "string", "COUNT", "100");
            _ = int.TryParse((string)((RedisValue[])((RedisResult[])result!)[0])[0], out cursor);
            RedisValue[] keysMatch = ((RedisValue[])((RedisResult[])result!)[1]);
            ClassicAssert.IsTrue(cursor == 0);
            ClassicAssert.IsTrue(keysMatch.Length == 100);
        }

        [Test]
        public void CanUseScanKeysCountAndTypeWithObjects()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var nKeys = 100;

            for (int i = 0; i < nKeys; i++)
            {
                db.StringSet(new RedisKey($"key:{i}"), new RedisValue($"keyvalue-{i}"));
            }

            for (int i = 0; i < 10; i++)
            {
                db.HashSet(new RedisKey($"hskey:{i}"), [new HashEntry("field1", "1")]);
            }

            for (int i = 0; i < 10; i++)
            {
                db.SortedSetAdd(new RedisKey($"sskey:{i}"), "a", i);
            }

            for (int i = 0; i < 10; i++)
            {
                db.ListLeftPush(new RedisKey($"lkey:{i}"), new RedisValue("lvalue"));
            }

            int cursor = 0;
            var result = db.Execute("SCAN", cursor.ToString(), "TYPE", "string", "COUNT", "100");
            _ = int.TryParse((string)((RedisValue[])((RedisResult[])result!)[0])[0], out cursor);
            RedisValue[] keysMatch = ((RedisValue[])((RedisResult[])result!)[1]);
            ClassicAssert.IsTrue(cursor == 0);
            ClassicAssert.IsTrue(keysMatch.Length == 100);

            cursor = 0;
            result = db.Execute("SCAN", cursor.ToString(), "TYPE", "zset");
            _ = int.TryParse((string)((RedisValue[])((RedisResult[])result!)[0])[0], out cursor);
            keysMatch = ((RedisValue[])((RedisResult[])result!)[1]);
            ClassicAssert.IsTrue(cursor == 0);
            ClassicAssert.IsTrue(keysMatch.Length == 10);
            ClassicAssert.IsTrue(keysMatch[0].ToString().Equals("sskey:0", StringComparison.OrdinalIgnoreCase));

            cursor = 0;
            result = db.Execute("SCAN", cursor.ToString(), "TYPE", "LIST");
            _ = int.TryParse((string)((RedisValue[])((RedisResult[])result!)[0])[0], out cursor);
            keysMatch = ((RedisValue[])((RedisResult[])result!)[1]);
            ClassicAssert.IsTrue(cursor == 0);
            ClassicAssert.IsTrue(keysMatch.Length == 10);
            ClassicAssert.IsTrue(keysMatch[0].ToString().Equals("lkey:0", StringComparison.OrdinalIgnoreCase));
        }


        [Test]
        public void CanUseScanKeysAndObjects()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var nKeys = 100;

            for (int i = 0; i < nKeys; i++)
            {
                _ = db.StringSet(new RedisKey($"key:{i}"), new RedisValue($"keyvalue-{i}"));
            }

            for (int i = 0; i < nKeys; i++)
            {
                db.HashSet(new RedisKey($"hskey:{i}"), [new HashEntry("field1", "1")]);
            }

            for (int i = 0; i < nKeys; i++)
            {
                _ = db.SortedSetAdd(new RedisKey($"sskey:{i}"), [new SortedSetEntry("a", 1)]);
            }

            long cursor = 0;
            int recordsReturned = 0;

            do
            {
                var result = db.Execute("SCAN", cursor.ToString());
                _ = long.TryParse((string)((RedisValue[])((RedisResult[])result!)[0])[0], out cursor);
                RedisValue[] keysMatch = ((RedisValue[])((RedisResult[])result!)[1]);
                recordsReturned += keysMatch.Length;
            } while (cursor != 0);

            ClassicAssert.AreEqual(nKeys * 3, recordsReturned, "records returned");
        }


        [Test]
        public void CanUseScanKeysTypeAndMatch()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var nKeys = 100;

            for (int i = 0; i < nKeys; i++)
            {
                _ = db.StringSet(new RedisKey($"key:{i}"), new RedisValue($"keyvalue-{i}"));
            }

            for (int i = 0; i < nKeys; i++)
            {
                db.HashSet(new RedisKey($"hskey:{i}"), [new HashEntry("field1", "1")]);
            }

            int cursor = 0;
            int recordsReturned = 0;

            do
            {
                var result = db.Execute("SCAN", cursor.ToString(), "TYPE", "HASH", "MATCH", "hs*");
                _ = int.TryParse((string)((RedisValue[])((RedisResult[])result!)[0])[0], out cursor);
                RedisValue[] keysMatch = ((RedisValue[])((RedisResult[])result!)[1]);
                recordsReturned += keysMatch.Length;
            } while (cursor != 0);

            ClassicAssert.IsTrue(recordsReturned == nKeys);

            // Lower casing
            recordsReturned = 0;
            do
            {
                var result = db.Execute("SCAN", cursor.ToString(), "type", "hash", "match", "hs*");
                _ = int.TryParse((string)((RedisValue[])((RedisResult[])result!)[0])[0], out cursor);
                RedisValue[] keysMatch = ((RedisValue[])((RedisResult[])result!)[1]);
                recordsReturned += keysMatch.Length;
            } while (cursor != 0);

            ClassicAssert.IsTrue(recordsReturned == nKeys);

        }

        [Test]
        public void CustomObjectScanCommandTest()
        {
            // create a custom object
            var factory = new MyDictFactory();

            server.Register.NewCommand("MYDICTSET", CommandType.ReadModifyWrite, factory, new MyDictSet(), respCustomCommandsInfo["MYDICTSET"]);
            server.Register.NewCommand("MYDICTGET", CommandType.Read, factory, new MyDictGet(), respCustomCommandsInfo["MYDICTGET"]);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string keyName = "ccKey";

            for (int i = 0; i < 10; i++)
            {
                db.Execute("MYDICTSET", keyName, $"fookey-{i}", $"foovalue-{i}");
            }

            var items = db.Execute("CUSTOMOBJECTSCAN", keyName, 0, "COUNT", 5);
            ClassicAssert.IsTrue(((RedisResult[])items).Length == 2);
            // Assert Cursor value
            ClassicAssert.IsTrue(((RedisResult[])items)[0].ToString() == "5");

            // Usage with pattern and count
            items = db.Execute("CUSTOMOBJECTSCAN", keyName, 0, "MATCH", "foo*", "COUNT", 10);
            ClassicAssert.IsTrue(((RedisResult[])items).Length == 2);
            var resultAsDictionary = items.ToDictionary();

            // First element in the resulting dictionary is the cursor value
            resultAsDictionary.TryGetValue("0", out var elements);

            // Review the elements are correct based on the pattern
            for (int i = 0; i < ((RedisResult[])elements).Length / 2; i++)
            {
                ClassicAssert.IsTrue(((RedisResult[])elements)[i * 2].ToString() == $"fookey-{i}");
            }
        }

        #region LigthClientTests

        [Test]
        public void CanUsePatternsInKeysTestLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            lightClientRequest.SendCommand("SET keyone valueone");
            lightClientRequest.SendCommand("SET keytwo valuetwo");
            lightClientRequest.SendCommand("SET keythree valuethree");

            var expectedResponse = "*3\r\n$6\r\nkeyone\r\n$6\r\nkeytwo\r\n$8\r\nkeythree\r\n+PONG\r\n";
            var response = lightClientRequest.SendCommands("KEYS *", "PING", 4);
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        #endregion

    }
}