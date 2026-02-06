using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class MultiDatabaseTests : AllureTestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, lowMemory: true, commitFrequencyMs: 1000, enableLua: true);
            server.Start();
        }

        [Test]
        public void MultiDatabaseBasicSelectTestSE()
        {
            var db1Key1 = "db1:key1";
            var db1Key2 = "db1:key2";
            var db2Key1 = "db2:key1";
            var db2Key2 = "db2:key2";
            var db12Key1 = "db12:key1";
            var db12Key2 = "db12:key1";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db1 = redis.GetDatabase(0);

            db1.StringSet(db1Key1, "db1:value1");
            db1.ListLeftPush(db1Key2, [new RedisValue("db1:val1"), new RedisValue("db1:val2")]);

            var db2 = redis.GetDatabase(1);
            ClassicAssert.IsFalse(db2.KeyExists(db1Key1));
            ClassicAssert.IsFalse(db2.KeyExists(db1Key2));

            db2.StringSet(db2Key1, "db2:value2");
            db2.SetAdd(db2Key2, [new RedisValue("db2:val2"), new RedisValue("db2:val2")]);

            ClassicAssert.IsTrue(db2.KeyExists(db2Key1));
            ClassicAssert.IsTrue(db2.KeyExists(db2Key2));

            ClassicAssert.IsFalse(db1.KeyExists(db2Key1));
            ClassicAssert.IsFalse(db1.KeyExists(db2Key2));

            var db12 = redis.GetDatabase(11);
            ClassicAssert.IsFalse(db12.KeyExists(db1Key1));
            ClassicAssert.IsFalse(db12.KeyExists(db1Key2));

            db2.StringSet(db12Key2, "db12:value2");
            db2.SetAdd(db12Key2, [new RedisValue("db12:val2"), new RedisValue("db12:val2")]);

            ClassicAssert.IsFalse(db12.KeyExists(db12Key1));
            ClassicAssert.IsFalse(db12.KeyExists(db12Key2));
        }

        [Test]
        public void MultiDatabaseBasicSelectFromScriptTestSE()
        {
            var db1Key1 = "db1:key1";
            var db1Key2 = "db1:key2";
            var db2Key1 = "db2:key1";
            var db2Key2 = "db2:key2";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db1 = redis.GetDatabase(0);

            db1.StringSet(db1Key1, "db1:value1");
            db1.ListLeftPush(db1Key2, [new RedisValue("db1:val1"), new RedisValue("db1:val2")]);

            var db2 = redis.GetDatabase(1);
            db2.StringSet(db2Key1, "db2:value2");
            db2.SetAdd(db2Key2, [new RedisValue("db2:val2"), new RedisValue("db2:val2")]);

            var response = db1.ScriptEvaluate("redis.call('SELECT', 1); return redis.call('GET', KEYS[1])", [db2Key1]);
            ClassicAssert.AreEqual("db2:value2", response.ToString());
        }

        [Test]
        public void MultiDatabaseSwapDatabaseFromScriptTestLC()
        {
            var db1Key1 = "db1:key1";
            var db2Key1 = "db2:key1";

            using var lightClientRequest = TestUtils.CreateRequest();

            // Add data to DB 0
            var response = lightClientRequest.SendCommand($"SET {db1Key1} db1:value1");
            var expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Add data to DB 1
            response = lightClientRequest.SendCommand($"SELECT 1");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SET {db2Key1} db2:value1");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Select DB 0 
            response = lightClientRequest.SendCommand($"SELECT 0");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            var evalCmd = $"*4\r\n$4\r\nEVAL\r\n$61\r\nredis.call('SWAPDB', 0, 1); return redis.call('GET', KEYS[1])\r\n$1\r\n1\r\n${db2Key1.Length}\r\n{db2Key1}\r\n";
            response = lightClientRequest.SendCommand(Encoding.ASCII.GetBytes(evalCmd));
            expectedResponse = "$10\r\ndb2:value1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void MultiDatabaseDbSizeTestSE()
        {
            var dbIds = new[] { 0, 1, 13 };

            var strKvps = new (string, string)[][]
            {
                [("db1:key1", "db1:val1")],
                [("db2:key1", "db2:val1"), ("db2:key2", "db2:val2")],
                [("db12:key1", "db12:val1"), ("db12:key2", "db12:val2"), ("db12:key3", "db12:val3")]
            };

            var objKvps = new (string, string[])[][]
            {
                [("db1:key2", ["db1:val11", "db1:val12"])],
                [("db2:key3", ["db2:val31", "db2:val32"]), ("db2:key4", ["db2:val41", "db2:val42"])],
                [("db12:key4", ["db12:val41", "db12:val42"]), ("db12:key5", ["db12:val51", "db12:val52"]), ("db12:key6", ["db12:val61", "db12:val62"])]
            };

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());

            // Insert data
            for (var i = 0; i < dbIds.Length; i++)
            {
                var db = redis.GetDatabase(dbIds[i]);

                foreach (var kvp in strKvps[i])
                    db.StringSet(kvp.Item1, kvp.Item2);

                foreach (var kvp in objKvps[i])
                    db.ListLeftPush(kvp.Item1, kvp.Item2.Select(v => new RedisValue(v)).ToArray());
            }

            // Validate DB sizes
            for (var i = 0; i < dbIds.Length; i++)
            {
                var db = redis.GetDatabase(dbIds[i]);
                var actualDbSize = db.Execute("DBSIZE");
                var expectedDbSize = strKvps[i].Length + objKvps[i].Length;
                ClassicAssert.AreEqual(expectedDbSize, (ulong)actualDbSize);
            }
        }

        [Test]
        public void MultiDatabaseSimpleTransactionTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db1 = redis.GetDatabase(1);

            var tran = db1.CreateTransaction();

            tran.StringSetAsync("db2:key1", "db2:val1");
            tran.StringSetAsync("db2:key2", "db2:val2");

            var committed = tran.Execute();
            ClassicAssert.IsTrue(committed);

            string actualValue = db1.StringGet("db2:key1");
            ClassicAssert.AreEqual(actualValue, "db2:val1");

            actualValue = db1.StringGet("db2:key2");
            ClassicAssert.AreEqual(actualValue, "db2:val2");

            var db0 = redis.GetDatabase(0);
            ClassicAssert.IsFalse(db0.KeyExists("db2:key1"));
            ClassicAssert.IsFalse(db0.KeyExists("db2:key2"));
        }

        [Test]
        public void MultiDatabaseCustomCommandRegistrationTest()
        {
            server.Register.NewProcedure("SUM", () => new Sum());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());

            // Execute custom procedure in DB 0
            var db1 = redis.GetDatabase(0);

            db1.StringSet("db1:key1", "10");
            db1.StringSet("db1:key2", "20");
            db1.StringSet("db1:key3", "30");

            var retValue = db1.Execute("SUM", "db1:key1", "db1:key2", "db1:key3");
            ClassicAssert.AreEqual("60", retValue.ToString());

            // Execute custom procedure in DB 1
            var db2 = redis.GetDatabase(1);

            db2.StringSet("db2:key1", "10");
            db2.StringSet("db2:key2", "100");
            db2.StringSet("db2:key3", "20");
            db2.StringSet("db2:key4", "-20");

            // Include non-existent and string keys as well
            retValue = db2.Execute("SUM", "db2:key1", "db2:key2", "db2:key3", "db2:key4");
            ClassicAssert.AreEqual("110", retValue.ToString());
        }

        [Test]
        public void MultiDatabaseSelectErrorConditionsTestSE()
        {
            var db1Key1 = "db1:key1";
            var db1Key2 = "db1:key2";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db1 = redis.GetDatabase(0);

            db1.StringSet(db1Key1, "db1:value1");
            db1.ListLeftPush(db1Key2, [new RedisValue("db1:val1"), new RedisValue("db1:val2")]);

            var db17 = redis.GetDatabase(17);
            Assert.Throws<RedisCommandException>(() => db17.StringSet(db1Key1, "db1:value1"), "The database does not exist on the server: 17");
        }

        [Test]
        public void MultiDatabaseSwapDbErrorConditionsTestLC()
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            // SWAPDB non-numeric DBID
            var response = lightClientRequest.SendCommand($"SWAPDB a 0");
            var expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_INVALID_FIRST_DB_INDEX)}";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SWAPDB 1 b");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_INVALID_SECOND_DB_INDEX)}";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // SWAPDB DBID out-of-range
            response = lightClientRequest.SendCommand($"SWAPDB -1 0");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_DB_INDEX_OUT_OF_RANGE)}";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SWAPDB 17 1");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_DB_INDEX_OUT_OF_RANGE)}";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SWAPDB 0 -2");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_DB_INDEX_OUT_OF_RANGE)}";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SWAPDB 1 18");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_DB_INDEX_OUT_OF_RANGE)}";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // SWAPDB in cluster mode
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableCluster: true);
            server.Start();

            using var lightClientRequest2 = TestUtils.CreateRequest();

            // SWAPDB with non-numeric value
            response = lightClientRequest2.SendCommand($"SWAPDB a 0");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_SWAPDB_CLUSTER_MODE)}";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // SWAPDB with out-of-range value
            response = lightClientRequest2.SendCommand($"SWAPDB 0 -1");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_SWAPDB_CLUSTER_MODE)}";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // SWAPDB 0 0
            response = lightClientRequest2.SendCommand($"SWAPDB 0 0");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_SWAPDB_CLUSTER_MODE)}";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // SWAPDB 0 1
            response = lightClientRequest2.SendCommand($"SWAPDB 0 1");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_SWAPDB_CLUSTER_MODE)}";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [TestCase("SELECT")]
        [TestCase("SAVE")]
        [TestCase("BGSAVE")]
        [TestCase("LASTSAVE")]
        [TestCase("COMMITAOF")]
        public void MultiDatabaseSingleDbIdCommandErrorsTestLC(string cmd)
        {
            // Test that running a command with a single DB ID as input returns the correct error messages.

            using var lightClientRequest = TestUtils.CreateRequest();

            // CMD with non-numeric DBID
            var response = lightClientRequest.SendCommand($"{cmd} a");
            var expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER)}";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // CMD with DBID out-of-range
            response = lightClientRequest.SendCommand($"{cmd} -1");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_DB_INDEX_OUT_OF_RANGE)}";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"{cmd} 17");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_DB_INDEX_OUT_OF_RANGE)}";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Test that running a command with a single DB ID as input returns the correct error messages in cluster mode.

            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableCluster: true);
            server.Start();

            using var lightClientRequest2 = TestUtils.CreateRequest();

            // CMD 0 (should return successfully, as all commands support DBID 0 in cluster mode)
            response = lightClientRequest2.SendCommand($"{cmd} 0");
            expectedResponse = cmd switch
            {
                "SELECT" or "SAVE" => "+OK\r\n",
                "BGSAVE" => "+Background saving started\r\n",
                "LASTSAVE" => ":0\r\n",
                "COMMITAOF" => "+AOF file committed\r\n",
                _ => null
            };

            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // CMD 1 (returns error)
            response = lightClientRequest2.SendCommand($"{cmd} 1");
            expectedResponse = cmd switch
            {
                "SELECT" => $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_SELECT_CLUSTER_MODE)}",
                _ => $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_DB_ID_CLUSTER_MODE)}",
            };
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void MultiDatabaseSameKeyTestSE()
        {
            var key1 = "key1";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db1 = redis.GetDatabase(0);
            db1.StringSet(key1, "db1:val1");

            var db2 = redis.GetDatabase(1);
            db2.SetAdd(key1, [new RedisValue("db2:val1"), new RedisValue("db2:val2")]);

            var db12 = redis.GetDatabase(11);
            db12.ListLeftPush(key1, [new RedisValue("db12:val1"), new RedisValue("db12:val2")]);

            var db1val = db1.StringGet(key1);
            ClassicAssert.AreEqual("db1:val1", db1val.ToString());

            var db2val = db2.SetMembers(key1);
            CollectionAssert.AreEquivalent(db2val, new[] { new RedisValue("db2:val1"), new RedisValue("db2:val2") });

            var db12val = db12.ListLeftPop(key1);
            ClassicAssert.AreEqual("db12:val2", db12val.ToString());
        }

        [Test]
        public void MultiDatabaseFlushDatabasesTestSE()
        {
            var db1Key1 = "db1:key1";
            var db1Key2 = "db1:key2";
            var db2Key1 = "db2:key1";
            var db2Key2 = "db2:key2";
            var db12Key1 = "db12:key1";
            var db12Key2 = "db12:key2";
            var db1data = new RedisValue[] { "db1:a", "db1:b", "db1:c", "db1:d" };
            var db2data = new RedisValue[] { "db2:a", "db2:b", "db2:c", "db2:d" };
            var db12data = new RedisValue[] { "db12:a", "db12:b", "db12:c", "db12:d" };

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());

            var db1 = redis.GetDatabase(0);
            var result = db1.StringSet(db1Key1, "db1:val1");
            ClassicAssert.IsTrue(result);

            var count = db1.ListLeftPush(db1Key2, db1data);
            ClassicAssert.AreEqual(db1data.Length, count);

            var db2 = redis.GetDatabase(1);
            result = db2.StringSet(db2Key1, "db2:val1");
            ClassicAssert.IsTrue(result);

            count = db2.ListLeftPush(db2Key2, db2data);
            ClassicAssert.AreEqual(db2data.Length, count);

            var db12 = redis.GetDatabase(11);
            result = db12.StringSet(db12Key1, "db12:val1");
            ClassicAssert.IsTrue(result);

            count = db12.ListLeftPush(db12Key2, db12data);
            ClassicAssert.AreEqual(db12data.Length, count);

            var opResult = db1.Execute("FLUSHDB");
            ClassicAssert.AreEqual("OK", opResult.ToString());

            ClassicAssert.IsFalse(db1.KeyExists(db1Key1));
            ClassicAssert.IsFalse(db1.KeyExists(db1Key2));

            ClassicAssert.IsTrue(db2.KeyExists(db2Key1));
            ClassicAssert.IsTrue(db2.KeyExists(db2Key2));

            ClassicAssert.IsTrue(db12.KeyExists(db12Key1));
            ClassicAssert.IsTrue(db12.KeyExists(db12Key2));

            opResult = db1.Execute("FLUSHALL");
            ClassicAssert.AreEqual("OK", opResult.ToString());

            ClassicAssert.IsFalse(db2.KeyExists(db2Key1));
            ClassicAssert.IsFalse(db2.KeyExists(db2Key2));

            ClassicAssert.IsFalse(db12.KeyExists(db12Key1));
            ClassicAssert.IsFalse(db12.KeyExists(db12Key2));
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
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"LPUSH {db1Key2} db1:val1 db1:val2");
            expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            lightClientRequest.SendCommand($"SELECT 1");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"EXISTS {db1Key1}");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"EXISTS {db1Key2}");
            expectedResponse = ":0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SET {db2Key1} db2:value1");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SADD {db2Key2} db2:val1 db2:val2");
            expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            lightClientRequest.SendCommand($"SELECT 0");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"GET {db1Key1}", 2);
            expectedResponse = "$10\r\ndb1:value1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"LPOP {db1Key2}", 2);
            expectedResponse = "$8\r\ndb1:val2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            lightClientRequest.SendCommand($"SELECT 1");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"GET {db2Key1}", 2);
            expectedResponse = "$10\r\ndb2:value1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SISMEMBER {db2Key2} db2:val2");
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        // todo: remove this test once transactions across databases are supported
        [Test]
        public void MultiDatabaseSelectDatabaseInTransactionDisabledTestLC()
        {
            var db1Key1 = "db1:key1";
            var db2Key1 = "db2:key1";

            using var lightClientRequest = TestUtils.CreateRequest();

            var response = lightClientRequest.SendCommand($"MULTI");
            var expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SET {db1Key1} db1:value1");
            var queuedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(queuedResponse, response);

            response = lightClientRequest.SendCommand($"SELECT 0");
            TestUtils.AssertEqualUpToExpectedLength(queuedResponse, response);

            response = lightClientRequest.SendCommand($"SELECT 1");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_SELECT_IN_TXN_UNSUPPORTED)}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SET {db2Key1} db2:value1");
            queuedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(queuedResponse, response);

            response = lightClientRequest.SendCommand($"EXEC");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_EXEC_ABORT)}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"GET {db1Key1}");
            expectedResponse = "$-1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"GET {db2Key1}");
            expectedResponse = "$-1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SELECT 1");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"GET {db1Key1}");
            expectedResponse = "$-1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"GET {db2Key1}");
            expectedResponse = "$-1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [Ignore("Currently not supporting SELECT in a transaction")]
        public void MultiDatabaseSelectInTransactionTestLC()
        {
            var db1Key1 = "db1:key1";
            var db1Key2 = "db1:key2";
            var db2Key1 = "db2:key1";
            var db2Key2 = "db2:key1";

            using var lightClientRequest = TestUtils.CreateRequest();

            var response = lightClientRequest.SendCommand($"MULTI");
            var expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SET {db1Key1} db1:value1");
            var queuedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(queuedResponse, response);

            response = lightClientRequest.SendCommand($"LPUSH {db1Key2} db1:val1 db1:val2");
            TestUtils.AssertEqualUpToExpectedLength(queuedResponse, response);

            lightClientRequest.SendCommand($"SELECT 1");
            TestUtils.AssertEqualUpToExpectedLength(queuedResponse, response);

            response = lightClientRequest.SendCommand($"SET {db2Key1} db2:value1");
            TestUtils.AssertEqualUpToExpectedLength(queuedResponse, response);

            response = lightClientRequest.SendCommand($"SADD {db2Key2} db2:val1 db2:val2");
            TestUtils.AssertEqualUpToExpectedLength(queuedResponse, response);

            response = lightClientRequest.SendCommand($"EXEC", 6);
            expectedResponse = "*5\r\n+OK\r\n:2\r\n+OK\r\n+OK\r\n:2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SELECT 0");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"GET {db1Key1}", 2);
            expectedResponse = "$10\r\ndb1:value1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"LPOP {db1Key2}", 2);
            expectedResponse = "$8\r\ndb1:val2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            lightClientRequest.SendCommand($"SELECT 1");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"GET {db2Key1}", 2);
            expectedResponse = "$10\r\ndb2:value1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SISMEMBER {db2Key2} db2:val2");
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void MultiDatabaseSwapDatabasesTestLC()
        {
            var db1Key1 = "db1:key1";
            var db1Key2 = "db1:key2";
            var db2Key1 = "db2:key1";
            var db2Key2 = "db2:key2";
            var db12Key1 = "db12:key1";
            var db12Key2 = "db12:key2";

            using var lightClientRequest = TestUtils.CreateRequest();

            // Add data to DB 0
            var response = lightClientRequest.SendCommand($"SET {db1Key1} db1:value1");
            var expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"LPUSH {db1Key2} db1:val1 db1:val2");
            expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Add data to DB 1
            response = lightClientRequest.SendCommand($"SELECT 1");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SET {db2Key1} db2:value1");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SADD {db2Key2} db2:val1 db2:val2");
            expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Add data to DB 11
            response = lightClientRequest.SendCommand($"SELECT 11");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SET {db12Key1} db12:value1");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SADD {db12Key2} db12:val1 db12:val2");
            expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Swap DB 1 AND DB 11 (from DB 11 context)
            response = lightClientRequest.SendCommand($"SWAPDB 1 11");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Verify data in DB 11 is previous data from DB 1
            response = lightClientRequest.SendCommand($"GET {db2Key1}", 2);
            expectedResponse = "$10\r\ndb2:value1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SISMEMBER {db2Key2} db2:val2");
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Verify data in DB 1 is previous data from DB 11
            response = lightClientRequest.SendCommand($"SELECT 1");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"GET {db12Key1}", 2);
            expectedResponse = "$11\r\ndb12:value1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SISMEMBER {db12Key2} db12:val2");
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Swap DB 11 AND DB 0 (from DB 1 context)
            response = lightClientRequest.SendCommand($"SELECT 1");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SWAPDB 11 0");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Verify data in DB 0 is previous data from DB 11
            response = lightClientRequest.SendCommand($"SELECT 0");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"GET {db2Key1}", 2);
            expectedResponse = "$10\r\ndb2:value1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SISMEMBER {db2Key2} db2:val2");
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Verify data in DB 11 is previous data from DB 0
            response = lightClientRequest.SendCommand($"SELECT 11");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"GET {db1Key1}", 2);
            expectedResponse = "$10\r\ndb1:value1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"LPOP {db1Key2}", 2);
            expectedResponse = "$8\r\ndb1:val2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        // todo: remove this test once transactions across databases are supported
        [Test]
        public void MultiDatabaseSwapDatabasesInTransactionDisabledTestLC()
        {
            var db1Key1 = "db1:key1";
            var db2Key1 = "db2:key1";

            using var lightClientRequest = TestUtils.CreateRequest();

            var response = lightClientRequest.SendCommand($"MULTI");
            var expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SET {db1Key1} db1:value1");
            var queuedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(queuedResponse, response);

            response = lightClientRequest.SendCommand($"SWAPDB 0 1");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_SWAPDB_IN_TXN_UNSUPPORTED)}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SET {db2Key1} db2:value1");
            queuedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(queuedResponse, response);

            response = lightClientRequest.SendCommand($"EXEC");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_EXEC_ABORT)}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"GET {db1Key1}");
            expectedResponse = "$-1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"GET {db2Key1}");
            expectedResponse = "$-1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SELECT 1");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"GET {db1Key1}");
            expectedResponse = "$-1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"GET {db2Key1}");
            expectedResponse = "$-1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [Ignore("Currently not supporting SWAPDB in a transaction")]
        public void MultiDatabaseSwapDatabasesInTransactionTestLC()
        {
            var db2Key1 = "db2:key1";
            var db2Key2 = "db2:key2";
            var db12Key1 = "db12:key1";
            var db12Key2 = "db12:key2";

            using var lightClientRequest = TestUtils.CreateRequest();

            var response = lightClientRequest.SendCommand($"SELECT 1");
            var expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"MULTI");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Add data to DB 1
            response = lightClientRequest.SendCommand($"SET {db2Key1} db2:value1");
            var queuedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(queuedResponse, response);

            response = lightClientRequest.SendCommand($"SADD {db2Key2} db2:val1 db2:val2");
            TestUtils.AssertEqualUpToExpectedLength(queuedResponse, response);

            // Swap DB 1 AND DB 11 (from DB 1 context)
            response = lightClientRequest.SendCommand($"SWAPDB 1 11");
            TestUtils.AssertEqualUpToExpectedLength(queuedResponse, response);

            // Add data to DB 1 (previous DB 11)
            response = lightClientRequest.SendCommand($"SET {db12Key1} db12:value1");
            TestUtils.AssertEqualUpToExpectedLength(queuedResponse, response);

            response = lightClientRequest.SendCommand($"SADD {db12Key2} db12:val1 db12:val2");
            TestUtils.AssertEqualUpToExpectedLength(queuedResponse, response);

            response = lightClientRequest.SendCommand($"EXEC", 6);
            expectedResponse = "*5\r\n+OK\r\n:2\r\n+OK\r\n+OK\r\n:2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Verify data in DB 1 is previous data from DB 11
            response = lightClientRequest.SendCommand($"GET {db12Key1}", 2);
            expectedResponse = "$11\r\ndb12:value1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SISMEMBER {db12Key2} db12:val2");
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Verify data in DB 11 is previous data from DB 1
            response = lightClientRequest.SendCommand($"SELECT 11");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"GET {db2Key1}", 2);
            expectedResponse = "$10\r\ndb2:value1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand($"SISMEMBER {db2Key2} db2:val2");
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void MultiDatabaseMultiSessionSwapDatabasesErrorTestLC()
        {
            // Ensure that SWAPDB returns an error when multiple clients are connected.
            var db1Key1 = "db1:key1";
            var db2Key1 = "db2:key1";

            using var lightClientRequest1 = TestUtils.CreateRequest(); // Session for DB 0 context
            using var lightClientRequest2 = TestUtils.CreateRequest(); // Session for DB 1 context

            // Add data to DB 0
            var response = lightClientRequest1.SendCommand($"SET {db1Key1} db1:value1");
            var expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Add data to DB 1
            response = lightClientRequest2.SendCommand($"SELECT 1");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest2.SendCommand($"SET {db2Key1} db2:value1");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Swap DB 0 AND DB 1 (from DB 0 context)
            response = lightClientRequest1.SendCommand($"SWAPDB 0 1");
            expectedResponse = $"-{Encoding.ASCII.GetString(CmdStrings.RESP_ERR_SWAPDB_UNSUPPORTED)}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [Ignore("SWAPDB is currently disallowed for more than one client session. This test should be enabled once that changes.")]
        public void MultiDatabaseMultiSessionSwapDatabasesTestLC()
        {
            var db1Key1 = "db1:key1";
            var db1Key2 = "db1:key2";
            var db2Key1 = "db2:key1";
            var db2Key2 = "db2:key2";

            using var lightClientRequest1 = TestUtils.CreateRequest(); // Session for DB 0 context
            using var lightClientRequest2 = TestUtils.CreateRequest(); // Session for DB 1 context

            // Add data to DB 0
            var response = lightClientRequest1.SendCommand($"SET {db1Key1} db1:value1");
            var expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest1.SendCommand($"LPUSH {db1Key2} db1:val1 db1:val2");
            expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Add data to DB 1
            response = lightClientRequest2.SendCommand($"SELECT 1");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest2.SendCommand($"SET {db2Key1} db2:value1");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest2.SendCommand($"SADD {db2Key2} db2:val1 db2:val2");
            expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest2.SendCommand($"GET {db2Key1}", 2);
            expectedResponse = "$10\r\ndb2:value1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Swap DB 0 AND DB 1 (from DB 0 context)
            response = lightClientRequest1.SendCommand($"SWAPDB 0 1");
            expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Verify data in DB 0 is previous data from DB 1
            response = lightClientRequest1.SendCommand($"GET {db2Key1}", 2);
            expectedResponse = "$10\r\ndb2:value1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest1.SendCommand($"SISMEMBER {db2Key2} db2:val2");
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Verify data in DB 1 is previous data from DB 0
            response = lightClientRequest2.SendCommand($"GET {db1Key1}", 2);
            expectedResponse = "$10\r\ndb1:value1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest2.SendCommand($"LPOP {db1Key2}", 2);
            expectedResponse = "$8\r\ndb1:val2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [Ignore("")]
        public void MultiDatabaseSelectMultithreadedTestLC()
        {
            var cts = new CancellationTokenSource();

            // Create a set of tuples (db-id, key, value)
            var dbCount = 16;
            var keyCount = 8;
            var tuples = GenerateDataset(dbCount, keyCount);

            // Create multiple LC request objects to be used
            var lcRequests = new LightClientRequest[16];
            for (var i = 0; i < lcRequests.Length; i++)
                lcRequests[i] = TestUtils.CreateRequest(countResponseType: CountResponseType.Bytes);

            // In parallel, add each (key, value) pair to a database of id db-id
            var tasks = new Task[lcRequests.Length];
            var results = new bool[tuples.Length];
            var tupIdx = -1;
            for (var i = 0; i < tasks.Length; i++)
            {
                var lcRequest = lcRequests[i];
                tasks[i] = Task.Run(() =>
                {
                    while (true)
                    {
                        var currTupIdx = Interlocked.Increment(ref tupIdx);
                        if (currTupIdx >= tuples.Length) break;

                        var tup = tuples[currTupIdx];

                        var expectedResponse = "+OK\r\n+OK\r\n";
                        var response = lcRequest.Execute($"SELECT {tup.Item1}", $"SET {tup.Item2} {tup.Item3}", expectedResponse.Length);

                        results[currTupIdx] = response != null && expectedResponse == response;
                    }
                }, cts.Token);
            }

            // Wait for all tasks to finish
            if (!Task.WhenAll(tasks).Wait(TimeSpan.FromSeconds(60)))
            {
                cts.Cancel();
                Assert.Fail("Items not inserted in allotted time.");
            }

            // Check that all tasks successfully entered the data to the respective database
            Assert.That(results, Is.All.True);

            cts = new CancellationTokenSource();

            // In parallel, retrieve the actual value for each db-id and key
            for (var i = 0; i < tasks.Length; i++)
            {
                var lcRequest = lcRequests[i];
                tasks[i] = Task.Run(() =>
                {
                    while (true)
                    {
                        var currTupIdx = Interlocked.Increment(ref tupIdx);
                        if (currTupIdx >= tuples.Length) break;

                        var tup = tuples[currTupIdx];

                        var expectedResponse = $"+OK\r\n${tup.Item3.Length}\r\n{tup.Item3}\r\n";
                        var response = lcRequest.Execute($"SELECT {tup.Item1}", $"GET {tup.Item2}",
                            expectedResponse.Length);

                        results[currTupIdx] = response != null && expectedResponse == response;
                    }

                    lcRequest.Dispose();
                }, cts.Token);
            }

            // Wait for all tasks to finish
            if (!Task.WhenAll(tasks).Wait(TimeSpan.FromSeconds(60)))
            {
                cts.Cancel();
                Assert.Fail("Items not retrieved in allotted time.");
            }

            // Check that all the tasks retrieved the correct value successfully
            Assert.That(results, Is.All.True);
        }

        [Test]
        public void MultiDatabaseSelectMultithreadedTestSE()
        {
            // Create a set of tuples (db-id, key, value)
            var dbCount = 16;
            var keyCount = 8;
            var tuples = GenerateDataset(dbCount, keyCount);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var dbConnections = new IDatabase[dbCount];
            for (var i = 0; i < dbCount; i++)
            {
                dbConnections[i] = redis.GetDatabase(i);
            }

            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

            // In parallel, add each (key, value) pair to a database of id db-id
            var tasks = new Task[tuples.Length];
            for (var i = 0; i < tasks.Length; i++)
            {
                var tup = tuples[i];
                tasks[i] = Task.Run(async () =>
                {
                    var db = dbConnections[tup.Item1];
                    return await db.StringSetAsync(tup.Item3, tup.Item4).ConfigureAwait(false);
                }, cts.Token);
            }

            // Wait for all tasks to finish
            if (!Task.WhenAll(tasks).Wait(TimeSpan.FromSeconds(60), cts.Token))
                Assert.Fail("Items not inserted in allotted time.");

            // Check that all tasks successfully entered the data to the respective database
            Assert.That(tasks, Has.All.Matches<Task<bool>>(t => t.IsCompletedSuccessfully && t.Result));

            cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

            // In parallel, retrieve the actual value for each db-id and key
            for (var i = 0; i < tasks.Length; i++)
            {
                var tup = tuples[i];
                tasks[i] = Task.Run(async () =>
                {
                    var db = dbConnections[tup.Item1];
                    var actualValue = await db.StringGetAsync(tup.Item3).ConfigureAwait(false);
                    return actualValue.ToString() == tup.Item4;
                }, cts.Token);
            }

            // Wait for all tasks to finish
            if (!Task.WhenAll(tasks).Wait(TimeSpan.FromSeconds(60), cts.Token))
                Assert.Fail("Items not retrieved in allotted time.");

            // Check that (db-id, key, actual-value) tuples match original (db-id, key, value) tuples
            Assert.That(tasks, Has.All.Matches<Task<bool>>(t => t.IsCompletedSuccessfully && t.Result));
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
                var garnetServer = redis.GetServer(TestUtils.EndPoint);
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

                var db3 = redis.GetDatabase(2);
                ClassicAssert.IsFalse(db3.KeyExists(db1Key));
                ClassicAssert.IsFalse(db3.KeyExists(db2Key));
            }
        }

        [Test]
        public void MultiDatabaseSaveRecoverRawStringTest()
        {
            var db1Key = "db1:key1";
            var db2Key = "db2:key1";
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
                var garnetServer = redis.GetServer(TestUtils.EndPoint);
                db1.Execute("SAVE");
                //garnetServer.Save(SaveType.BackgroundSave);
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

                var db3 = redis.GetDatabase(2);
                ClassicAssert.IsFalse(db3.KeyExists(db1Key));
                ClassicAssert.IsFalse(db3.KeyExists(db2Key));
            }
        }

        [Test]
        public void MultiDatabaseAofRecoverRawStringTest()
        {
            var db1Key = "db1:key1";
            var db2Key = "db2:key1";
            var db1data = new RedisValue("db1:a");
            var db2data = new RedisValue("db2:a");

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db1 = redis.GetDatabase(0);
                var result = db1.StringSet(db1Key, db1data);
                ClassicAssert.IsTrue(result);

                var value = db1.StringGet(db1Key);
                ClassicAssert.IsTrue(value.HasValue);
                ClassicAssert.AreEqual(db1data, value.ToString());

                var db2 = redis.GetDatabase(1);
                result = db2.StringSet(db2Key, db2data);
                ClassicAssert.IsTrue(result);

                value = db2.StringGet(db2Key);
                ClassicAssert.IsTrue(value.HasValue);
                ClassicAssert.AreEqual(db2data, value.ToString());
            }

            server.Store.CommitAOF(true);
            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db1 = redis.GetDatabase(0);

                var value = db1.StringGet(db1Key);
                ClassicAssert.IsTrue(value.HasValue);
                ClassicAssert.AreEqual(db1data, value.ToString());

                var db2 = redis.GetDatabase(1);

                value = db2.StringGet(db2Key);
                ClassicAssert.IsTrue(value.HasValue);
                ClassicAssert.AreEqual(db2data, value.ToString());

                var db3 = redis.GetDatabase(2);
                ClassicAssert.IsFalse(db3.KeyExists(db1Key));
                ClassicAssert.IsFalse(db3.KeyExists(db2Key));
            }
        }

        [Test]
        public void MultiDatabaseAofRecoverObjectTest()
        {
            var db1Key = "db1:key1";
            var db2Key = "db2:key1";
            var db1data = new SortedSetEntry[] { new("db1:a", 1), new("db1:b", 2), new("db1:c", 3) };
            var db2data = new SortedSetEntry[] { new("db2:a", -1), new("db2:b", -2), new("db2:c", -3) };

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db1 = redis.GetDatabase(0);
                var added = db1.SortedSetAdd(db1Key, db1data);
                ClassicAssert.AreEqual(3, added);

                var score = db1.SortedSetScore(db1Key, "db1:a");
                ClassicAssert.IsTrue(score.HasValue);
                ClassicAssert.AreEqual(1, score.Value);

                var db2 = redis.GetDatabase(1);
                added = db2.SortedSetAdd(db2Key, db2data);
                ClassicAssert.AreEqual(3, added);

                score = db2.SortedSetScore(db2Key, "db2:a");
                ClassicAssert.IsTrue(score.HasValue);
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
                ClassicAssert.IsTrue(score.HasValue);
                ClassicAssert.AreEqual(1, score.Value);

                var db2 = redis.GetDatabase(1);

                score = db2.SortedSetScore(db2Key, "db2:a");
                ClassicAssert.IsTrue(score.HasValue);
                ClassicAssert.AreEqual(-1, score.Value);

                var db3 = redis.GetDatabase(2);
                ClassicAssert.IsFalse(db3.KeyExists(db1Key));
                ClassicAssert.IsFalse(db3.KeyExists(db2Key));
            }
        }

        [Test]
        public void MultiDatabaseSaveInProgressTest()
        {
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db1 = redis.GetDatabase(0);
                var db2 = redis.GetDatabase(1);

                // Check no saves present
                var lastsave = (int)db1.Execute("LASTSAVE");
                var lastsave1 = (int)db1.Execute("LASTSAVE", "0");
                var lastsave2 = (int)db2.Execute("LASTSAVE", "1");
                ClassicAssert.AreEqual(0, lastsave);
                ClassicAssert.AreEqual(0, lastsave1);
                ClassicAssert.AreEqual(0, lastsave2);

                // Issue background save to DB 0
                var res1 = db1.Execute("BGSAVE", "0");
                ClassicAssert.AreEqual("Background saving started", res1.ToString());

                // Issue background save to DB 1 while DB 0 save is in progress - legal
                var res2 = db1.Execute("BGSAVE", "1");
                ClassicAssert.AreEqual("Background saving started", res2.ToString());

                // Issue general background save while DB 0 save is in progress - legal
                //var res = db1.Execute("BGSAVE");
                //ClassicAssert.AreEqual("Background saving started", res.ToString());

                // Wait for saves to complete
                do
                {
                    Thread.Sleep(10);
                    //lastsave = (int)db1.Execute("LASTSAVE");
                    lastsave1 = (int)db1.Execute("LASTSAVE", "0");
                    lastsave2 = (int)db2.Execute("LASTSAVE", "1");
                }
                while (lastsave1 == 0 || lastsave2 == 0);

                // Add some data
                for (var i = 0; i < 1024; i++)
                {
                    db1.StringSet($"k{i}", new string('x', 256));
                    db2.StringSet($"k{i}", new string('x', 256));
                    db1.ListLeftPush($"k{i}o", new string('x', 256));
                    db2.ListLeftPush($"k{i}o", new string('x', 256));
                }

                // Issue general background save
                //res = db1.Execute("BGSAVE");
                //ClassicAssert.AreEqual("Background saving started", res.ToString());

                // Issue background save to DB 0 while general save is in progress - illegal
                //Assert.Throws<RedisServerException>(() => db1.Execute("BGSAVE", "0"),
                //    Encoding.ASCII.GetString(CmdStrings.RESP_ERR_CHECKPOINT_ALREADY_IN_PROGRESS));

                // Wait for save to complete
                //do
                //{
                //    Thread.Sleep(10);
                //    lastsave = (int)db1.Execute("LASTSAVE");
                //}
                //while (lastsave == 0);
            }
        }

        [Test]
        [TestCase(false)]
        [TestCase(true)]
        public void MultiDatabaseSaveRecoverByDbIdTest(bool backgroundSave)
        {
            var db1Key1 = "db1:key1";
            var db1Key2 = "db1:key2";
            var db2Key1 = "db2:key1";
            var db2Key2 = "db2:key2";
            var db2Key3 = "db2:key3";
            var db2Key4 = "db2:key4";
            var db1val = new RedisValue("db1:a");
            var db2val1 = new RedisValue("db2:a");
            var db2val2 = new RedisValue("db2:b");
            var db1data = new SortedSetEntry[] { new("db1:a", 1), new("db1:b", 2), new("db1:c", 3) };
            var db2data1 = new SortedSetEntry[] { new("db2:a", -1), new("db2:b", -2), new("db2:c", -3) };
            var db2data2 = new SortedSetEntry[] { new("db2:d", 4), new("db2:e", 5), new("db2:f", 6) };
            long expectedLastSave;
            long actualLastSave;

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                // Add object & raw string data to DB 0
                var db1 = redis.GetDatabase(0);
                var added = db1.SortedSetAdd(db1Key1, db1data);
                ClassicAssert.AreEqual(3, added);

                var set = db1.StringSet(db1Key2, db1val);
                ClassicAssert.IsTrue(set);

                // Add object & raw string data to DB 1
                var db2 = redis.GetDatabase(1);
                added = db2.SortedSetAdd(db2Key1, db2data1);
                ClassicAssert.AreEqual(3, added);

                set = db2.StringSet(db2Key2, db2val1);
                ClassicAssert.IsTrue(set);

                // Issue DB SAVE for DB 1
                var res = db1.Execute(backgroundSave ? "BGSAVE" : "SAVE", "1");
                ClassicAssert.AreEqual(backgroundSave ? "Background saving started" : "OK", res.ToString());

                var lastSave = 0L;
                string lastSaveStr;
                bool parsed;
                var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                while (!cts.IsCancellationRequested)
                {
                    // Verify DB 1 was saved by checking LASTSAVE
                    lastSaveStr = db1.Execute("LASTSAVE", "1").ToString();
                    parsed = long.TryParse(lastSaveStr, out lastSave);
                    ClassicAssert.IsTrue(parsed);
                    if (lastSave != 0)
                        break;
                    Task.Delay(TimeSpan.FromMilliseconds(100), cts.Token);
                }

                expectedLastSave = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                Assert.That(lastSave, Is.InRange(expectedLastSave - 2, expectedLastSave));
                actualLastSave = lastSave;

                // Verify DB 0 was not saved
                lastSaveStr = db1.Execute("LASTSAVE").ToString();
                parsed = long.TryParse(lastSaveStr, out lastSave);
                ClassicAssert.IsTrue(parsed);
                ClassicAssert.AreEqual(0, lastSave);
            }

            // Restart server
            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var lastSave = 0L;
                string lastSaveStr;
                bool parsed;

                // Verify that data was not recovered for DB 0
                var db1 = redis.GetDatabase(0);

                ClassicAssert.IsFalse(db1.KeyExists(db1Key1));
                ClassicAssert.IsFalse(db1.KeyExists(db1Key2));

                // Verify that data was recovered for DB 1
                var db2 = redis.GetDatabase(1);

                var score = db2.SortedSetScore(db2Key1, "db2:a");
                ClassicAssert.IsTrue(score.HasValue);
                ClassicAssert.AreEqual(-1, score.Value);

                var value = db2.StringGet(db2Key2);
                ClassicAssert.IsTrue(value.HasValue);
                ClassicAssert.AreEqual(db2val1, value.ToString());

                // Re-add object & raw string data to DB 0
                var added = db1.SortedSetAdd(db1Key1, db1data);
                ClassicAssert.AreEqual(3, added);

                var set = db1.StringSet(db1Key2, db1val);
                ClassicAssert.IsTrue(set);

                // Add new object & raw string data to DB 1
                added = db2.SortedSetAdd(db2Key3, db2data2);
                ClassicAssert.AreEqual(3, added);

                set = db2.StringSet(db2Key4, db2val2);
                ClassicAssert.IsTrue(set);

                // Issue DB SAVE for DB 0
                var res = db1.Execute(backgroundSave ? "BGSAVE" : "SAVE", "0");
                ClassicAssert.AreEqual(backgroundSave ? "Background saving started" : "OK", res.ToString());

                // Verify DB 0 was saved by checking LASTSAVE
                var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                while (!cts.IsCancellationRequested)
                {
                    lastSaveStr = db1.Execute("LASTSAVE").ToString();
                    parsed = long.TryParse(lastSaveStr, out lastSave);
                    ClassicAssert.IsTrue(parsed);
                    if (lastSave != 0)
                        break;
                    Task.Delay(TimeSpan.FromMilliseconds(100), cts.Token);
                }

                expectedLastSave = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                Assert.That(lastSave, Is.InRange(expectedLastSave - 2, expectedLastSave));

                // Verify DB 1 was not saved
                Thread.Sleep(TimeSpan.FromSeconds(2));
                lastSaveStr = db1.Execute("LASTSAVE", "1").ToString();
                parsed = long.TryParse(lastSaveStr, out lastSave);
                ClassicAssert.IsTrue(parsed);
                //ClassicAssert.AreEqual(actualLastSave, lastSave);
            }

            // Restart server
            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                // Verify that data was recovered for DB 0
                var db1 = redis.GetDatabase(0);

                var score = db1.SortedSetScore(db1Key1, "db1:a");
                ClassicAssert.IsTrue(score.HasValue);
                ClassicAssert.AreEqual(1, score.Value);

                var value = db1.StringGet(db1Key2);
                ClassicAssert.IsTrue(value.HasValue);
                ClassicAssert.AreEqual(db1val, value.ToString());

                // Verify that previous data was recovered for DB 1
                var db2 = redis.GetDatabase(1);

                score = db2.SortedSetScore(db2Key1, "db2:a");
                ClassicAssert.IsTrue(score.HasValue);
                ClassicAssert.AreEqual(-1, score.Value);

                value = db2.StringGet(db2Key2);
                ClassicAssert.IsTrue(value.HasValue);
                ClassicAssert.AreEqual(db2val1, value.ToString());

                // Verify that new data was not recovered for DB 1
                ClassicAssert.IsFalse(db1.KeyExists(db2Key3));
                ClassicAssert.IsFalse(db1.KeyExists(db2Key4));
            }
        }

        [Test]
        public void MultiDatabaseAofRecoverByDbIdTest()
        {
            var db1Key1 = "db1:key1";
            var db1Key2 = "db1:key2";
            var db2Key1 = "db2:key1";
            var db2Key2 = "db2:key2";
            var db2Key3 = "db2:key3";
            var db2Key4 = "db2:key4";
            var db1val = new RedisValue("db1:a");
            var db2val1 = new RedisValue("db2:a");
            var db2val2 = new RedisValue("db2:b");
            var db1data = new SortedSetEntry[] { new("db1:a", 1), new("db1:b", 2), new("db1:c", 3) };
            var db2data1 = new SortedSetEntry[] { new("db2:a", -1), new("db2:b", -2), new("db2:c", -3) };
            var db2data2 = new SortedSetEntry[] { new("db2:d", 4), new("db2:e", 5), new("db2:f", 6) };

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                // Add object & raw string data to DB 0
                var db1 = redis.GetDatabase(0);
                var added = db1.SortedSetAdd(db1Key1, db1data);
                ClassicAssert.AreEqual(3, added);

                var set = db1.StringSet(db1Key2, db1val);
                ClassicAssert.IsTrue(set);

                // Add object & raw string data to DB 1
                var db2 = redis.GetDatabase(1);
                added = db2.SortedSetAdd(db2Key1, db2data1);
                ClassicAssert.AreEqual(3, added);

                set = db2.StringSet(db2Key2, db2val1);
                ClassicAssert.IsTrue(set);

                // Issue COMMITAOF for DB 1
                var res = db1.Execute("COMMITAOF", "1");
                ClassicAssert.AreEqual("AOF file committed", res.ToString());
            }

            // Restart server
            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                // Verify that data was not recovered for DB 0
                var db1 = redis.GetDatabase(0);

                ClassicAssert.IsFalse(db1.KeyExists(db1Key1));
                ClassicAssert.IsFalse(db1.KeyExists(db1Key2));

                // Verify that data was recovered for DB 1
                var db2 = redis.GetDatabase(1);

                var score = db2.SortedSetScore(db2Key1, "db2:a");
                ClassicAssert.IsTrue(score.HasValue);
                ClassicAssert.AreEqual(-1, score.Value);

                var value = db2.StringGet(db2Key2);
                ClassicAssert.IsTrue(value.HasValue);
                ClassicAssert.AreEqual(db2val1, value.ToString());

                // Re-add object & raw string data to DB 0
                var added = db1.SortedSetAdd(db1Key1, db1data);
                ClassicAssert.AreEqual(3, added);

                var set = db1.StringSet(db1Key2, db1val);
                ClassicAssert.IsTrue(set);

                // Add new object & raw string data to DB 1
                added = db2.SortedSetAdd(db2Key3, db2data2);
                ClassicAssert.AreEqual(3, added);

                set = db2.StringSet(db2Key4, db2val2);
                ClassicAssert.IsTrue(set);

                // Issue COMMITAOF for DB 0
                var res = db1.Execute("COMMITAOF", "0");
                ClassicAssert.AreEqual("AOF file committed", res.ToString());
            }

            // Restart server
            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                // Verify that data was recovered for DB 0
                var db1 = redis.GetDatabase(0);

                var score = db1.SortedSetScore(db1Key1, "db1:a");
                ClassicAssert.IsTrue(score.HasValue);
                ClassicAssert.AreEqual(1, score.Value);

                var value = db1.StringGet(db1Key2);
                ClassicAssert.IsTrue(value.HasValue);
                ClassicAssert.AreEqual(db1val, value.ToString());

                // Verify that previous data was recovered for DB 1
                var db2 = redis.GetDatabase(1);

                score = db2.SortedSetScore(db2Key1, "db2:a");
                ClassicAssert.IsTrue(score.HasValue);
                ClassicAssert.AreEqual(-1, score.Value);

                value = db2.StringGet(db2Key2);
                ClassicAssert.IsTrue(value.HasValue);
                ClassicAssert.AreEqual(db2val1, value.ToString());

                // Verify that new data was not recovered for DB 1
                ClassicAssert.IsFalse(db1.KeyExists(db2Key3));
                ClassicAssert.IsFalse(db1.KeyExists(db2Key4));
            }
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        private (int, int, string, string)[] GenerateDataset(int dbCount, int keyCount)
        {
            var data = new (int, int, string, string)[dbCount * keyCount];

            for (var dbId = 0; dbId < dbCount; dbId++)
            {
                for (var keyId = 0; keyId < keyCount; keyId++)
                {
                    data[(keyCount * dbId) + keyId] = (dbId, keyId, $"key{keyId}", $"db{dbId}:val{keyId}");
                }
            }

            return data;
        }
    }
}