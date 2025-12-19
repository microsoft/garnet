// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class TransactionTests
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        public void SetUpWithLowMemory()
        {
            TearDown();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();
        }

        [Test]
        public void TxnSetTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var tran = db.CreateTransaction();
            string value1 = "abcdefg1";
            string value2 = "abcdefg2";

            tran.StringSetAsync("mykey1", value1);
            tran.StringSetAsync("mykey2", value2);
            bool committed = tran.Execute();

            string string1 = db.StringGet("mykey1");
            string string2 = db.StringGet("mykey2");

            ClassicAssert.AreEqual(string1, value1);
            ClassicAssert.AreEqual(string2, value2);
        }

        [Test]
        public void TxnExecuteTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var tran = db.CreateTransaction();
            string value1 = "abcdefg1";
            string value2 = "abcdefg2";

            tran.ExecuteAsync("SET", ["mykey1", value1]);
            tran.ExecuteAsync("SET", ["mykey2", value2]);
            bool committed = tran.Execute();

            string string1 = db.StringGet("mykey1");
            string string2 = db.StringGet("mykey2");

            ClassicAssert.AreEqual(string1, value1);
            ClassicAssert.AreEqual(string2, value2);
        }

        [Test]
        public void TxnGetTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string value1 = "abcdefg1";
            string value2 = "abcdefg2";
            db.StringSet("mykey1", value1);
            db.StringSet("mykey2", value2);

            var tran = db.CreateTransaction();
            var t1 = tran.StringGetAsync("mykey1");
            var t2 = tran.StringGetAsync("mykey2");
            bool committed = tran.Execute();


            ClassicAssert.AreEqual(t1.Result, value1);
            ClassicAssert.AreEqual(t2.Result, value2);
        }

        [Test]
        public void TxnGetSetTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string value1 = "abcdefg1";
            string value2 = "abcdefg2";
            db.StringSet("mykey1", value1);

            var tran = db.CreateTransaction();
            var t1 = tran.StringGetAsync("mykey1");
            var t2 = tran.StringSetAsync("mykey2", value2);
            bool committed = tran.Execute();

            string string2 = db.StringGet("mykey2");
            ClassicAssert.AreEqual(t1.Result, value1);
            ClassicAssert.AreEqual(string2, value2);
        }

        [Test]
        public void TxnHExpireTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "test";
            HashEntry[] he = [new HashEntry("a", "1"), new HashEntry("b", "2"), new HashEntry("c", "3")];

            var tran = db.CreateTransaction();
            var t0 = tran.HashSetAsync(key, he);
            var t1 = tran.HashFieldExpireAsync(key, [he[0].Name], System.DateTime.Now.AddHours(1));
            var t2 = tran.ExecuteAsync("HEXPIRE", [key, "1000", "FIELDS", "1", he[1].Name]);
            var committed = tran.Execute();

            var entries = db.HashGetAll(key);
            ClassicAssert.AreEqual(he, entries);

            var expire = db.HashFieldGetExpireDateTime(key, he.Select(x => x.Name).ToArray());
            ClassicAssert.Greater(expire[0], 0);
            ClassicAssert.Greater(expire[1], 0);
            ClassicAssert.AreEqual(expire[2], -1);
        }

        [Test]
        public void LargeTxn([Values(512, 2048, 8192)] int size)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string value = "abcdefg";
            string key = "mykey";
            for (int i = 0; i < size; i++)
                db.StringSet(key + i, value + i);

            Task<RedisValue>[] results = new Task<RedisValue>[size];
            var tran = db.CreateTransaction();
            for (int i = 0; i < size * 2; i++)
            {
                if (i % 2 == 0)
                    results[i / 2] = tran.StringGetAsync(key + i / 2);
                else
                {
                    int counter = (i - 1) / 2 + size;
                    tran.StringSetAsync(key + counter, value + counter);
                }
            }
            bool committed = tran.Execute();
            for (int i = 0; i < size * 2; i++)
            {
                if (i % 2 == 0)
                    ClassicAssert.AreEqual(results[i / 2].Result, value + i / 2);
                else
                {
                    int counter = (i - 1) / 2 + size;
                    string res = db.StringGet(key + counter);
                    ClassicAssert.AreEqual(res, value + counter);
                }
            }
        }

        [Test]
        public void LargeTxnWatch([Values(512, 2048, 4096)] int size)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string value = "abcdefg";
            string key = "mykey";
            for (int i = 0; i < size; i++)
                db.StringSet(key + i, value + i);

            Task<RedisValue>[] results = new Task<RedisValue>[size];
            var tran = db.CreateTransaction();
            for (int i = 0; i < size * 2; i++)
            {
                if (i % 2 == 0)
                {
                    tran.AddCondition(Condition.StringEqual(key + i / 2, value + i / 2));
                    results[i / 2] = tran.StringGetAsync(key + i / 2);
                }
                else
                {
                    int counter = (i - 1) / 2 + size;
                    tran.StringSetAsync(key + counter, value + counter);
                }
            }
            bool committed = tran.Execute();
            for (int i = 0; i < size * 2; i++)
            {
                if (i % 2 == 0)
                    ClassicAssert.AreEqual(results[i / 2].Result, value + i / 2);
                else
                {
                    int counter = (i - 1) / 2 + size;
                    string res = db.StringGet(key + counter);
                    ClassicAssert.AreEqual(res, value + counter);
                }
            }
        }

        [Test]
        public async Task TxnCommandCoverage()
        {
            RespCommand[] excludeList =
                [
                    // These are not too interesting (or better to not implement) in Transaction context,
                    // and a bit difficult to integrate into this test.
                    RespCommand.ACL,
                    RespCommand.AUTH,
                    RespCommand.BGSAVE,
                    RespCommand.DEBUG,
                    RespCommand.FAILOVER,
                    RespCommand.LASTSAVE,
                    RespCommand.MODULE,
                    RespCommand.MONITOR,
                    RespCommand.PUBSUB,
                    RespCommand.SAVE,
                    RespCommand.SCRIPT,

                    // Todo: implement
                    RespCommand.EXECWITHETAG,
                    RespCommand.EXECIFMATCH,
                    RespCommand.EXECIFNOTMATCH,
                    RespCommand.EXECIFGREATER,

                    // Not implemented
                    RespCommand.DUMP,
                    RespCommand.MIGRATE,
                    RespCommand.RESTORE,

                    // Garnet
                    RespCommand.COSCAN,
                    RespCommand.HCOLLECT,
                    RespCommand.ZCOLLECT,

                    // Decided to not support,
                    RespCommand.CONFIG,
                    RespCommand.COMMAND,
                    RespCommand.CLIENT,
                    RespCommand.DBSIZE,
                    RespCommand.FLUSHALL,
                    RespCommand.FLUSHDB,
                    RespCommand.HELLO,
                    RespCommand.INFO,
                    RespCommand.KEYS,
                    RespCommand.LATENCY,
                    RespCommand.MEMORY,
                    RespCommand.REPLICAOF,
                    RespCommand.ROLE,
                    RespCommand.SCAN,
                    RespCommand.SECONDARYOF,
                    RespCommand.SLOWLOG,

                    // SELECT / SWAPDB currently not allowed during TXN
                    RespCommand.SELECT,
                    RespCommand.SWAPDB,

                    // Implemented, client going down will crash the test
                    RespCommand.QUIT
                ];

            RespAclCategories[] excludeCat =
                [
                    // These require using a nonblocking context and we don't do that yet
                    RespAclCategories.Blocking,
                    // Not too relevant during a transaction
                    RespAclCategories.PubSub,
                    RespAclCategories.Scripting,
                    // Either disallowed or will interfere with test
                    RespAclCategories.Transaction
                ];

            ClassicAssert.IsTrue(RespCommandsInfo.TryGetRespCommandsInfo(out var respCommandsInfo));
            ClassicAssert.IsTrue(RespCommandDocs.TryGetRespCommandsDocs(out var respCommandsDocs));

            foreach (var respCommand in respCommandsInfo)
            {
                var commandInfo = respCommand.Value;
                // Exclude commmands explicitly disallowed in MULTI context
                if (commandInfo.Flags != default && commandInfo.Flags.HasFlag(RespCommandFlags.NoMulti))
                    continue;

                var commandDoc = respCommandsDocs.Where(x => x.Key == commandInfo.Name).FirstOrDefault().Value;

                // Exclude cluster commands
                if (commandDoc == default || commandDoc.Group == RespCommandGroup.Cluster)
                    continue;

                if (excludeList.Any(w => w == commandInfo.Command))
                    continue;

                if (excludeCat.Any(flag => commandInfo.AclCategories.HasFlag(flag)))
                    continue;

                // Server command equivalent to REPLICAOF
                if (commandInfo.Name == "SLAVEOF")
                {
                    continue;
                }

                var arity = Math.Abs(commandInfo.Arity);

                System.Collections.Generic.List<string> command = [commandInfo.Name];
                var startIdx = 1; // starting index including Name

                if (commandInfo.SubCommands?.Length > 0)
                {
                    commandInfo = commandInfo.SubCommands[0];
                    commandDoc = commandDoc.SubCommands.Where(x => x.Command == commandInfo.Command).First();

                    command.Add(commandInfo.Name.Split('|').Last());
                    startIdx++;
                }

                // We need this due to subcommand structure
                if (commandInfo.Name == "BITOP")
                {
                    arity++;
                }

                if (commandDoc.Arguments != null)
                    foreach (var arg in commandDoc.Arguments)
                    {
                        if (arg.ArgumentFlags.HasFlag(RespCommandArgumentFlags.Optional))
                            continue;

                        switch (arg.Type)
                        {
                            case RespCommandArgumentType.Key:
                                if (string.Compare(arg.Name, "DESTINATION", StringComparison.InvariantCultureIgnoreCase) == 0)
                                {
                                    command.Add("DEST");
                                }
                                else
                                {
                                    command.Add("KEY");
                                }
                                startIdx++;
                                break;
                            case RespCommandArgumentType.OneOf:
                                if (!string.IsNullOrEmpty(arg.Token))
                                {
                                    command.Add(arg.Token);
                                    startIdx++;
                                }
                                else if (arg is RespCommandContainerArgument con)
                                {
                                    command.Add(con.Arguments[0].Token);
                                    startIdx++;
                                }
                                break;
                            case RespCommandArgumentType.Integer:
                                if (string.Compare(arg.Name, "NUMKEYS", StringComparison.InvariantCultureIgnoreCase) == 0)
                                {
                                    command.Add("1");
                                    startIdx++;
                                }
                                break;
                        }
                    }

                for (var i = startIdx; i < arity; ++i)
                {
                    command.Add("0");
                }

                try
                {
                    using var client = TestUtils.GetGarnetClientSession();
                    client.Connect();
                    client.Execute("MULTI");
                    var result = await client.ExecuteAsync([.. command]);
                    ClassicAssert.AreEqual("QUEUED", result, commandInfo.Name + " failed transaction coverage");
                    client.Execute("DISCARD");
                }
                catch
                {
                    Assert.Fail($"{commandInfo.Name} failed transaction coverage");
                }
            }
        }

        [Test]
        public async Task SimpleWatchTest()
        {
            var lightClientRequest = TestUtils.CreateRequest();

            var response = lightClientRequest.SendCommand("SET key1 value1");
            var expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("WATCH key1");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("MULTI");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GET key1");
            expectedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
            response = lightClientRequest.SendCommand("SET key2 value2");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            await Task.Run(() => updateKey("key1", "value1_updated"));

            response = lightClientRequest.SendCommand("EXEC");
            expectedResponse = "*-1";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // This one should Commit
            lightClientRequest.SendCommand("MULTI");
            lightClientRequest.SendCommand("GET key1");
            lightClientRequest.SendCommand("SET key2 value2");
            response = lightClientRequest.SendCommand("EXEC");

            expectedResponse = "*2\r\n$14\r\nvalue1_updated\r\n+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public async Task WatchTestWithSetWithEtag()
        {
            var lightClientRequest = TestUtils.CreateRequest();

            var expectedResponse = ":1\r\n";
            var response = lightClientRequest.SendCommand("EXECWITHETAG SET key1 value1");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            expectedResponse = "+OK\r\n";
            response = lightClientRequest.SendCommand("WATCH key1");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("MULTI");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GET key1");
            expectedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SET key2 value2");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            await Task.Run(() =>
            {
                using var lightClientRequestCopy = TestUtils.CreateRequest();
                string command = "EXECWITHETAG SET key1 value1_updated";
                lightClientRequestCopy.SendCommand(command);
            });

            response = lightClientRequest.SendCommand("EXEC");
            expectedResponse = "*-1";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // This one should Commit
            lightClientRequest.SendCommand("MULTI");

            lightClientRequest.SendCommand("GET key1");
            lightClientRequest.SendCommand("SET key2 value2");
            // check that all the etag commands can be called inside a transaction
            lightClientRequest.SendCommand("EXECWITHETAG SET key3 value2");
            lightClientRequest.SendCommand("EXECWITHETAG GET key3");
            lightClientRequest.SendCommand("EXECIFNOTMATCH 1 GET key3");
            lightClientRequest.SendCommand("EXECIFMATCH 1 SET key3 anotherVal");
            lightClientRequest.SendCommand("EXECWITHETAG SET key3 arandomval");

            response = lightClientRequest.SendCommand("EXEC");

            expectedResponse = "*7\r\n$14\r\nvalue1_updated\r\n+OK\r\n:1\r\n*2\r\n:1\r\n$6\r\nvalue2\r\n*2\r\n:1\r\n$-1\r\n*2\r\n:2\r\n$-1\r\n:3\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // check if we still have the appropriate etag on the key we had set
            var otherLighClientRequest = TestUtils.CreateRequest();
            response = otherLighClientRequest.SendCommand("EXECWITHETAG GET key1");
            expectedResponse = "*2\r\n:2\r\n$14\r\nvalue1_updated\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public async Task WatchNonExistentKey()
        {
            var lightClientRequest = TestUtils.CreateRequest();

            var expectedResponse = "+OK\r\n";
            var response = lightClientRequest.SendCommand("SET key2 value2");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("WATCH key1");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("MULTI");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GET key2");
            expectedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SET key3 value3");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            await Task.Run(() => updateKey("key1", "value1"));

            response = lightClientRequest.SendCommand("EXEC");
            expectedResponse = "*-1";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // This one should Commit
            lightClientRequest.SendCommand("MULTI");
            lightClientRequest.SendCommand("GET key1");
            lightClientRequest.SendCommand("SET key2 value2");
            response = lightClientRequest.SendCommand("EXEC");

            expectedResponse = "*2\r\n$6\r\nvalue1\r\n+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public async Task WatchKeyFromDisk()
        {
            SetUpWithLowMemory();
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                for (int i = 0; i < 1000; i++)
                    db.StringSet("key" + i, "value" + i);
            }

            var lightClientRequest = TestUtils.CreateRequest();

            // this key should be in the disk
            var response = lightClientRequest.SendCommand("WATCH key1");
            var expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("MULTI");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GET key900");
            expectedResponse = "+QUEUED\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("SET key901 value901_updated");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            await Task.Run(() => updateKey("key1", "value1_updated"));

            response = lightClientRequest.SendCommand("EXEC");
            expectedResponse = "*-1";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // This one should Commit
            lightClientRequest.SendCommand("MULTI");
            lightClientRequest.SendCommand("GET key900");
            lightClientRequest.SendCommand("SET key901 value901_updated");
            response = lightClientRequest.SendCommand("EXEC");

            expectedResponse = "*2\r\n$8\r\nvalue900\r\n+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        private static void updateKey(string key, string value)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var command = $"SET {key} {value}";
            var response = lightClientRequest.SendCommand(command);

            var expectedResponse = "+OK\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }
    }
}