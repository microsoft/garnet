using System.Linq;
using System;
using System.Text;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class MultiDatabaseTests
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true);
            server.Start();
        }

        [Test]
        public void MultiDatabaseBasicSelectTestSE()
        {
            var db1Key1 = "db1:key1";
            var db1Key2 = "db1:key2";
            var db2Key1 = "db2:key1";
            var db2Key2 = "db2:key1";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db1 = redis.GetDatabase(0);

            db1.StringSet(db1Key1, "db1:value1");
            db1.ListLeftPush(db1Key2, [new RedisValue("db1:val1"), new RedisValue("db1:val2")]);

            var db2 = redis.GetDatabase(1);
            ClassicAssert.IsFalse(db2.KeyExists(db1Key1));
            ClassicAssert.IsFalse(db2.KeyExists(db1Key2));

            db2.StringSet(db2Key2, "db2:value2");
            db2.SetAdd(db2Key2, [new RedisValue("db2:val2"), new RedisValue("db2:val2")]);

            ClassicAssert.IsFalse(db1.KeyExists(db2Key1));
            ClassicAssert.IsFalse(db1.KeyExists(db2Key2));
        }

        [Test]
        public void MultiDatabaseSameKeyTestSE()
        {
            var key1 = "key1";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db1 = redis.GetDatabase(0);
            db1.StringSet(key1, "db1:value1");

            var db2 = redis.GetDatabase(1);
            db2.SetAdd(key1, [new RedisValue("db2:val2"), new RedisValue("db2:val2")]);

            var db1val = db1.StringGet(key1);
            ClassicAssert.AreEqual("db1:value1", db1val.ToString());

            var db2val = db2.SetPop(key1);
            ClassicAssert.AreEqual("db2:val2", db2val.ToString());
        }

        [Test]
        public void MultiDatabaseBasicSelectTestLC()
        {
            var db1Key1 = "db1:key1";
            var db1Key2 = "db1:key2";
            var db2Key1 = "db2:key1";
            var db2Key2 = "db2:key1";

            using var lightClientRequest = TestUtils.CreateRequest();

            var response = lightClientRequest.SendCommand($"SET {db1Key1} db1:value1");
            var expectedResponse = "+OK\r\n";
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand($"LPUSH {db1Key2} db1:val1 db1:val2");
            expectedResponse = ":2\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            lightClientRequest.SendCommand($"SELECT 1");
            expectedResponse = "+OK\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand($"EXISTS {db1Key1}");
            expectedResponse = ":0\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand($"EXISTS {db1Key2}");
            expectedResponse = ":0\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand($"SET {db2Key1} db2:value1");
            expectedResponse = "+OK\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand($"SADD {db2Key2} db2:val1 db2:val2");
            expectedResponse = ":2\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            lightClientRequest.SendCommand($"SELECT 0");
            expectedResponse = "+OK\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand($"GET {db1Key1}", 2);
            expectedResponse = "$10\r\ndb1:value1\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand($"LPOP {db1Key2}", 2);
            expectedResponse = "$8\r\ndb1:val2\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            lightClientRequest.SendCommand($"SELECT 1");
            expectedResponse = "+OK\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand($"GET {db2Key1}", 2);
            expectedResponse = "$10\r\ndb2:value1\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            response = lightClientRequest.SendCommand($"SPOP {db2Key2}", 2);
            expectedResponse = "$8\r\ndb2:val2\r\n";
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void MultiDatabaseSaveRecoverObjectTest()
        {
            var db1Key = "db1:key1";
            var db2Key = "db1:key1";
            var db1data = new RedisValue[] { "db1:a", "db1:b", "db1:c", "db1:d" };
            var db2data = new RedisValue[] { "db2:a", "db2:b", "db2:c", "db2:d" };
            RedisValue[] db1DataBeforeRecovery;
            RedisValue[] db2DataBeforeRecovery;

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db1 = redis.GetDatabase(0);
                db1.ListLeftPush(db1Key, db1data);
                db1data = db1data.Select(x => x).Reverse().ToArray();
                db1DataBeforeRecovery = db1.ListRange(db1Key);
                ClassicAssert.AreEqual(db1data, db1DataBeforeRecovery);

                var db2 = redis.GetDatabase(1);
                db2.SetAdd(db2Key, db2data);
                db2DataBeforeRecovery = db2.SetMembers(db2Key);
                ClassicAssert.AreEqual(db2data, db2DataBeforeRecovery);

                // Issue and wait for DB save
                var garnetServer = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");
                garnetServer.Save(SaveType.BackgroundSave);
                while (garnetServer.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks) Thread.Sleep(10);
            }

            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db1 = redis.GetDatabase(0);
                var db1ReturnedData = db1.ListRange(db1Key);
                ClassicAssert.AreEqual(db1DataBeforeRecovery, db1ReturnedData);
                ClassicAssert.AreEqual(db1data.Length, db1ReturnedData.Length);
                ClassicAssert.AreEqual(db1data, db1ReturnedData);

                var db2 = redis.GetDatabase(1);
                var db2ReturnedData = db2.SetMembers(db2Key);
                ClassicAssert.AreEqual(db2DataBeforeRecovery, db2ReturnedData);
                ClassicAssert.AreEqual(db2data.Length, db2ReturnedData.Length);
                ClassicAssert.AreEqual(db2data, db2ReturnedData);
            }
        }

        [Test]
        public void MultiDatabaseSaveRecoverRawStringTest()
        {
            var db1Key = "db1:key1";
            var db2Key = "db1:key1";
            var db1data = new RedisValue("db1:a");
            var db2data = new RedisValue("db2:a");
            RedisValue db1DataBeforeRecovery;
            RedisValue db2DataBeforeRecovery;

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db1 = redis.GetDatabase(0);
                db1.StringSet(db1Key, db1data);
                db1DataBeforeRecovery = db1.StringGet(db1Key);
                ClassicAssert.AreEqual(db1data, db1DataBeforeRecovery);

                var db2 = redis.GetDatabase(1);
                db2.StringSet(db2Key, db2data);
                db2DataBeforeRecovery = db2.StringGet(db2Key);
                ClassicAssert.AreEqual(db2data, db2DataBeforeRecovery);

                // Issue and wait for DB save
                var garnetServer = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");
                garnetServer.Save(SaveType.BackgroundSave);
                while (garnetServer.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks) Thread.Sleep(10);
            }

            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db1 = redis.GetDatabase(0);
                var db1ReturnedData = db1.StringGet(db1Key);
                ClassicAssert.AreEqual(db1DataBeforeRecovery, db1ReturnedData);
                ClassicAssert.AreEqual(db1data, db1ReturnedData);

                var db2 = redis.GetDatabase(1);
                var db2ReturnedData = db2.StringGet(db2Key);
                ClassicAssert.AreEqual(db2DataBeforeRecovery, db2ReturnedData);
                ClassicAssert.AreEqual(db2data, db2ReturnedData);
            }
        }

        [Test]
        public void MultiDatabaseAofRecoverObjectTest()
        {
            var db1Key = "db1:key1";
            var db2Key = "db2:key1";
            var db1data = new SortedSetEntry[]{ new("db1:a", 1), new("db1:b", 2), new("db1:c", 3) };
            var db2data = new SortedSetEntry[]{ new("db2:a", -1), new("db2:b", -2), new("db2:c", -3) };

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db1 = redis.GetDatabase(0);
                var added = db1.SortedSetAdd(db1Key, db1data);
                ClassicAssert.AreEqual(3, added);

                var score = db1.SortedSetScore(db1Key, "db1:a");
                ClassicAssert.True(score.HasValue);
                ClassicAssert.AreEqual(1, score.Value);

                var db2 = redis.GetDatabase(1);
                added = db2.SortedSetAdd(db2Key, db2data);
                ClassicAssert.AreEqual(3, added);

                score = db2.SortedSetScore(db2Key, "db2:a");
                ClassicAssert.True(score.HasValue);
                ClassicAssert.AreEqual(-1, score.Value);
            }

            server.Store.CommitAOF(true);
            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db1 = redis.GetDatabase(0);

                var score = db1.SortedSetScore(db1Key, "db1:a");
                ClassicAssert.True(score.HasValue);
                ClassicAssert.AreEqual(1, score.Value);

                var db2 = redis.GetDatabase(1);

                score = db2.SortedSetScore(db2Key, "db2:a");
                ClassicAssert.True(score.HasValue);
                ClassicAssert.AreEqual(-1, score.Value);
            }
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }
    }
}
