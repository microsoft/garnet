﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using NotImplementedException = System.NotImplementedException;

namespace Garnet.test
{
    [TestFixture]
    public class RespAdminCommandsTests
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

        #region LightclientTests

        [Test]
        public void PingTest()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var expectedResponse = "+PONG\r\n";
            var response = lightClientRequest.SendCommand("PING");
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void PingMessageTest()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var expectedResponse = "$5\r\nHELLO\r\n";
            var response = lightClientRequest.SendCommand("PING HELLO");
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void PingErrorMessageTest()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var expectedResponse = $"-{string.Format(CmdStrings.GenericErrWrongNumArgs, $"{nameof(RespCommand.PING)}")}\r\n";
            var response = lightClientRequest.SendCommand("PING HELLO WORLD", 1);
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void EchoWithNoMessageReturnErrorTest()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var expectedResponse = $"-{string.Format(CmdStrings.GenericErrWrongNumArgs, $"{nameof(RespCommand.ECHO)}")}\r\n";
            var response = lightClientRequest.SendCommand("ECHO", 1);
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        public void EchoWithMessagesReturnErrorTest()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var expectedResponse = $"-{string.Format(CmdStrings.GenericErrWrongNumArgs, $"{nameof(RespCommand.ECHO)}")}\r\n";
            var response = lightClientRequest.SendCommand("ECHO HELLO WORLD", 1);
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
            response = lightClientRequest.SendCommand("ECHO HELLO WORLD WORLD2", 1);
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void EchoWithMessageTest()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var expectedResponse = "$5\r\nHELLO\r\n";
            var response = lightClientRequest.SendCommand("ECHO HELLO", 1);
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void EchoTwoCommandsTest()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var wrongNumMessage = string.Format(CmdStrings.GenericErrWrongNumArgs, $"{nameof(RespCommand.ECHO)}");
            var expectedResponse = $"-{wrongNumMessage}\r\n$5\r\nHELLO\r\n";
            var response = lightClientRequest.SendCommands("ECHO HELLO WORLD WORLD2", "ECHO HELLO", 1, 1);
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        public void TimeCommandTest()
        {
            // this is an example, we just compare the length of the response with the expected one.
            using var lightClientRequest = TestUtils.CreateRequest();
            var expectedResponse = "*2\r\n$10\r\n1626282789\r\n$6\r\n621362\r\n";
            var response = lightClientRequest.SendCommand("TIME", 3);
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse.Length, actualValue.Length);
        }


        [Test]
        public void TimeWithReturnErrorTest()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var expectedResponse = $"-{string.Format(CmdStrings.GenericErrWrongNumArgs, nameof(RespCommand.TIME))}\r\n";
            var response = lightClientRequest.SendCommand("TIME HELLO");
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        #endregion

        #region SeClientTests

        [Test]
        public void SeSaveTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            IServer server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");

            var lastSave = server.LastSave();

            // Check no saves present
            ClassicAssert.AreEqual(DateTimeOffset.FromUnixTimeSeconds(0).Ticks, lastSave.Ticks);

            // Issue background save
            server.Save(SaveType.BackgroundSave);

            // Wait for save to complete
            while (server.LastSave() == lastSave) Thread.Sleep(10);
        }

        [Test]
        public void SeSaveRecoverTest([Values] bool disableObj, [Values] bool useAzure)
        {
            if (useAzure)
                TestUtils.IgnoreIfNotRunningAzureTests();
            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, DisableObjects: disableObj, UseAzureStorage: useAzure);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                db.StringSet("SeSaveRecoverTestKey", "SeSaveRecoverTestValue");

                // Issue and wait for DB save
                var server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");
                server.Save(SaveType.BackgroundSave);
                while (server.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks) Thread.Sleep(10);
            }

            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, UseAzureStorage: useAzure);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                var recoveredValue = db.StringGet("SeSaveRecoverTestKey");
                ClassicAssert.AreEqual("SeSaveRecoverTestValue", recoveredValue.ToString());
            }
        }

        [Test]
        public void SeSaveRecoverObjectTest()
        {
            var key = "SeSaveRecoverTestObjectKey";
            var ldata = new RedisValue[] { "a", "b", "c", "d" };
            RedisValue[] returned_data_before_recovery = default;
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                db.ListLeftPush(key, ldata);
                ldata = ldata.Select(x => x).Reverse().ToArray();
                returned_data_before_recovery = db.ListRange(key);
                ClassicAssert.AreEqual(ldata, returned_data_before_recovery);

                // Issue and wait for DB save
                var server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");
                server.Save(SaveType.BackgroundSave);
                while (server.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks) Thread.Sleep(10);
            }

            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                var returnedData = db.ListRange(key);
                ClassicAssert.AreEqual(returned_data_before_recovery, returnedData);
                ClassicAssert.AreEqual(ldata.Length, returnedData.Length);
                ClassicAssert.AreEqual(ldata, returnedData);
            }
        }

        [Test]
        public void SeSaveRecoverCustomObjectTest()
        {
            string key = "key";
            string field = "field1";
            string value = "foovalue1";

            // Register sample custom command on object
            var factory = new MyDictFactory();
            server.Register.NewCommand("MYDICTSET", CommandType.ReadModifyWrite, factory, new MyDictSet(), new RespCommandsInfo { Arity = 4 });
            server.Register.NewCommand("MYDICTGET", CommandType.Read, factory, new MyDictGet(), new RespCommandsInfo { Arity = 3 });

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                db.Execute("MYDICTSET", key, field, value);
                var retValue = db.Execute("MYDICTGET", key, field);
                ClassicAssert.AreEqual(value, (string)retValue);

                // Issue and wait for DB save
                var server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");
                server.Save(SaveType.BackgroundSave);
                while (server.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks) Thread.Sleep(10);
            }

            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true);
            server.Register.NewCommand("MYDICTSET", CommandType.ReadModifyWrite, factory, new MyDictSet(), new RespCommandsInfo { Arity = 4 });
            server.Register.NewCommand("MYDICTGET", CommandType.Read, factory, new MyDictGet(), new RespCommandsInfo { Arity = 3 });
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                var retValue = db.Execute("MYDICTGET", key, field);
                ClassicAssert.AreEqual(value, (string)retValue);
            }
        }

        [Test]
        public void SeSaveRecoverCustomScriptTest()
        {
            static void ValidateServerData(IDatabase db, string strKey, string strValue, string listKey, string listValue)
            {
                var retValue = db.StringGet(strKey);
                ClassicAssert.AreEqual(strValue, (string)retValue);
                var retList = db.ListRange(listKey);
                ClassicAssert.AreEqual(1, retList.Length);
                ClassicAssert.AreEqual(listValue, (string)retList[0]);
            }

            var strKey = "StrKey";
            var strValue = "StrValue";
            var listKey = "ListKey";
            var listValue = "ListValue";

            // Register sample custom script that updates both main store and object store keys
            server.Register.NewProcedure("SETMAINANDOBJECT", new SetStringAndList());

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                db.Execute("SETMAINANDOBJECT", strKey, strValue, listKey, listValue);
                ValidateServerData(db, strKey, strValue, listKey, listValue);

                // Issue and wait for DB save
                var server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");
                server.Save(SaveType.BackgroundSave);
                while (server.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks) Thread.Sleep(10);
            }

            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true);
            server.Register.NewProcedure("SETMAINANDOBJECT", new SetStringAndList());
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                ValidateServerData(redis.GetDatabase(0), strKey, strValue, listKey, listValue);
            }
        }

        [Test]
        [TestCase(63, 15, 1)]
        [TestCase(63, 1, 1)]
        [TestCase(16, 16, 1)]
        [TestCase(5, 64, 1)]
        public void SeSaveRecoverMultipleObjectsTest(int memorySize, int recoveryMemorySize, int pageSize)
        {
            string sizeToString(int size) => size + "k";

            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, MemorySize: sizeToString(memorySize), PageSize: sizeToString(pageSize));
            server.Start();

            var ldata = new RedisValue[] { "a", "b", "c", "d" };
            var ldataArr = ldata.Select(x => x).Reverse().ToArray();
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                for (int i = 0; i < 3000; i++)
                    db.ListLeftPush($"SeSaveRecoverTestKey{i:0000}", ldata);

                for (int i = 0; i < 3000; i++)
                    ClassicAssert.AreEqual(ldataArr, db.ListRange($"SeSaveRecoverTestKey{i:0000}"), $"key {i:0000}");

                // Issue and wait for DB save
                var server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");
                server.Save(SaveType.BackgroundSave);
                while (server.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks) Thread.Sleep(10);
            }

            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, lowMemory: true, MemorySize: sizeToString(recoveryMemorySize), PageSize: sizeToString(pageSize), objectStoreTotalMemorySize: "64k");
            server.Start();

            ClassicAssert.LessOrEqual(server.Provider.StoreWrapper.objectStore.MaxAllocatedPageCount, (recoveryMemorySize / pageSize) + 1);
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                for (var i = 3000; i < 3100; i++)
                    db.ListLeftPush($"SeSaveRecoverTestKey{i:0000}", ldata);

                for (var i = 0; i < 3100; i++)
                    ClassicAssert.AreEqual(ldataArr, db.ListRange($"SeSaveRecoverTestKey{i:0000}"), $"key {i:0000}");
            }
        }

        [Test]
        [TestCase("63k", "15k")]
        [TestCase("63k", "3k")]
        [TestCase("63k", "1k")]
        [TestCase("8k", "5k")]
        [TestCase("16k", "16k")]
        [TestCase("5k", "8k")]
        [TestCase("5k", "64k")]
        public void SeSaveRecoverMultipleKeysTest(string memorySize, string recoveryMemorySize)
        {
            bool disableObj = true;

            server.Dispose();
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, DisableObjects: disableObj, lowMemory: true, MemorySize: memorySize, PageSize: "1k", enableAOF: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                for (int i = 0; i < 1000; i++)
                {
                    db.StringSet($"SeSaveRecoverTestKey{i:0000}", $"SeSaveRecoverTestValue");
                }

                for (int i = 0; i < 1000; i++)
                {
                    var recoveredValue = db.StringGet($"SeSaveRecoverTestKey{i:0000}");
                    ClassicAssert.AreEqual("SeSaveRecoverTestValue", recoveredValue.ToString());
                }

                var inforesult = db.Execute("INFO");

                // Issue and wait for DB save
                var server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");
                server.Save(SaveType.BackgroundSave);
                while (server.LastSave().Ticks == DateTimeOffset.FromUnixTimeSeconds(0).Ticks) Thread.Sleep(10);

                for (int i = 1000; i < 2000; i++)
                {
                    db.StringSet($"SeSaveRecoverTestKey{i:0000}", $"SeSaveRecoverTestValue");
                }

                for (int i = 1000; i < 2000; i++)
                {
                    var recoveredValue = db.StringGet($"SeSaveRecoverTestKey{i:0000}");
                    ClassicAssert.AreEqual("SeSaveRecoverTestValue", recoveredValue.ToString());
                }

                db.Execute("COMMITAOF");
            }

            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, DisableObjects: disableObj, tryRecover: true, lowMemory: true, MemorySize: recoveryMemorySize, PageSize: "1k", enableAOF: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                for (int i = 0; i < 2000; i++)
                {
                    var recoveredValue = db.StringGet($"SeSaveRecoverTestKey{i:0000}");
                    ClassicAssert.AreEqual("SeSaveRecoverTestValue", recoveredValue.ToString(), $"Key SeSaveRecoverTestKey{i:0000}");
                }
            }
        }

        [Test]
        public void SeAofRecoverTest()
        {
            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                db.StringSet("SeAofRecoverTestKey", "SeAofRecoverTestValue");

                db.Execute("COMMITAOF");
            }

            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                var recoveredValue = db.StringGet("SeAofRecoverTestKey");
                ClassicAssert.AreEqual("SeAofRecoverTestValue", recoveredValue.ToString());
            }
        }

        [Test]
        public void SeFlushDbAndFlushAllTest([Values(RespCommand.FLUSHALL, RespCommand.FLUSHDB)] RespCommand cmd)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            IServer server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");

            var db = redis.GetDatabase(0);

            string origValue = "abcdefghij";
            db.StringSet("mykey", origValue);

            string retValue = db.StringGet("mykey");
            ClassicAssert.AreEqual(origValue, retValue);

            switch (cmd)
            {
                case RespCommand.FLUSHDB:
                    server.FlushDatabase();
                    break;
                case RespCommand.FLUSHALL:
                    server.FlushAllDatabases();
                    break;
                default:
                    throw new NotImplementedException();
            }

            retValue = db.StringGet("mykey");
            ClassicAssert.AreEqual(null, retValue);
        }

        [Test]
        public void SePingTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var expectedResponse = "PONG";
            var actualValue = db.Execute("PING").ToString();
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        public void SePingMessageTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var expectedResponse = "HELLO";
            var actualValue = db.Execute("PING", "HELLO").ToString();
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        public void SePingErrorMessageTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            Assert.Throws<RedisServerException>(() => db.Execute("PING", "HELLO", "WORLD"));
        }



        [Test]
        public void SeEchoWithNoMessageReturnErrorTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            Assert.Throws<RedisServerException>(() => db.Execute("ECHO"));
        }

        [Test]
        public void SeEchoWithMessageTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var expectedResponse = "HELLO";
            var actualValue = db.Execute("ECHO", "HELLO").ToString();
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        public void SeTimeCommandTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var actualValue = db.Execute("TIME");
            var seconds = ((RedisValue[])actualValue)[0];
            var microsecs = ((RedisValue[])actualValue)[1];
            ClassicAssert.AreEqual(seconds.ToString().Length, 10);
            ClassicAssert.AreEqual(microsecs.ToString().Length, 6);
        }


        [Test]
        public void SeTimeWithReturnErrorTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            Assert.Throws<RedisServerException>(() => db.Execute("TIME HELLO").ToString());
        }

        [Test]
        public async Task SeFlushDbAndFlushAllTest2([Values(RespCommand.FLUSHALL, RespCommand.FLUSHDB)] RespCommand cmd,
            [Values] bool async, [Values] bool unsafetruncatelog)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var key = $"{cmd}Test";
            var value = key;

            db.StringSet(key, value);
            var _value = db.StringGet(key);
            ClassicAssert.AreEqual(value, (string)_value);
            string[] p = default;

            if (async && unsafetruncatelog)
                p = ["ASYNC", "UNSAFETRUNCATELOG"];
            else if (unsafetruncatelog)
                p = ["UNSAFETRUNCATELOG"];

            if (async)
            {
                await db.ExecuteAsync(cmd.ToString(), p).ConfigureAwait(false);
                _value = db.StringGet(key);
                while (!_value.IsNull)
                {
                    _value = db.StringGet(key);
                    Thread.Yield();
                }
            }
            else
            {
                db.Execute(cmd.ToString(), p);
                _value = db.StringGet(key);
            }

            ClassicAssert.IsTrue(_value.IsNull);
        }

        [Test]
        [TestCase("timeout", "0")]
        [TestCase("save", "")]
        [TestCase("appendonly", "no")]
        [TestCase("slave-read-only", "no")]
        [TestCase("databases", "16")]
        [TestCase("cluster-node-timeout", "60")]
        public void SimpleConfigGet(string parameter, string parameterValue)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var result = (string[])db.Execute("CONFIG", "GET", parameter);

            ClassicAssert.AreEqual(parameter, result[0]);
            ClassicAssert.AreEqual(parameterValue, result[1]);
        }

        #endregion

        #region NegativeTests

        [Test]
        public void ConfigWrongNumberOfArguments()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            try
            {
                db.Execute("CONFIG");
                Assert.Fail("Shouldn't be reachable, command is incorrect");
            }
            catch (Exception ex)
            {
                var expectedMessage = string.Format(CmdStrings.GenericErrWrongNumArgs,
                    $"{nameof(RespCommand.CONFIG)}");
                ClassicAssert.AreEqual(expectedMessage, ex.Message);
            }
        }

        [Test]
        public void ConfigGetWrongNumberOfArguments()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            try
            {
                db.Execute("CONFIG", "GET");
                Assert.Fail("Shouldn't be reachable, command is incorrect");
            }
            catch (Exception ex)
            {
                var expectedMessage = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs,
                    $"{nameof(RespCommand.CONFIG)}|{nameof(CmdStrings.GET)}"));
                ClassicAssert.AreEqual(expectedMessage, ex.Message);
            }
        }
        #endregion
    }
}