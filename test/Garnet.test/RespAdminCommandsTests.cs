// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using StackExchange.Redis;

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
            Assert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void PingMessageTest()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var expectedResponse = "$5\r\nHELLO\r\n";
            var response = lightClientRequest.SendCommand("PING HELLO");
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void PingErrorMessageTest()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var expectedResponse = "-ERR wrong number of arguments for 'ping' command\r\n";
            var response = lightClientRequest.SendCommand("PING HELLO WORLD", 1);
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void EchoWithNoMessageReturnErrorTest()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var expectedResponse = "-ERR wrong number of arguments for 'echo' command\r\n";
            var response = lightClientRequest.SendCommand("ECHO", 1);
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        public void EchoWithMessagesReturnErrorTest()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var expectedResponse = "-ERR wrong number of arguments for 'echo' command\r\n";
            var response = lightClientRequest.SendCommand("ECHO HELLO WORLD", 1);
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
            response = lightClientRequest.SendCommand("ECHO HELLO WORLD WORLD2", 1);
            actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void EchoWithMessageTest()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var expectedResponse = "$5\r\nHELLO\r\n";
            var response = lightClientRequest.SendCommand("ECHO HELLO", 1);
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void EchoTwoCommandsTest()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var expectedResponse = "-ERR wrong number of arguments for 'echo' command\r\n$5\r\nHELLO\r\n";
            var response = lightClientRequest.SendCommands("ECHO HELLO WORLD WORLD2", "ECHO HELLO", 1, 1);
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        public void TimeCommandTest()
        {
            // this is an example, we just compare the length of the response with the expected one.
            using var lightClientRequest = TestUtils.CreateRequest();
            var expectedResponse = "*2\r\n$10\r\n1626282789\r\n$6\r\n621362\r\n";
            var response = lightClientRequest.SendCommand("TIME", 3);
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse.Length, actualValue.Length);
        }


        [Test]
        public void TimeWithReturnErrorTest()
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var expectedResponse = "-ERR wrong number of arguments for 'time' command\r\n";
            var response = lightClientRequest.SendCommand("TIME HELLO");
            var actualValue = Encoding.ASCII.GetString(response).Substring(0, expectedResponse.Length);
            Assert.AreEqual(expectedResponse, actualValue);
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
            Assert.AreEqual(DateTimeOffset.FromUnixTimeSeconds(0).Ticks, lastSave.Ticks);

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
                Assert.AreEqual("SeSaveRecoverTestValue", recoveredValue.ToString());
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
                Assert.AreEqual(ldata, returned_data_before_recovery);

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
                Assert.AreEqual(returned_data_before_recovery, returnedData);
                Assert.AreEqual(ldata.Length, returnedData.Length);
                Assert.AreEqual(ldata, returnedData);
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
                Assert.AreEqual("SeAofRecoverTestValue", recoveredValue.ToString());
            }
        }

        [Test]
        public void SeFlushDatabaseTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            IServer server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");

            var db = redis.GetDatabase(0);

            string origValue = "abcdefghij";
            db.StringSet("mykey", origValue);

            string retValue = db.StringGet("mykey");
            Assert.AreEqual(origValue, retValue);

            server.FlushDatabase();

            retValue = db.StringGet("mykey");
            Assert.AreEqual(null, retValue);
        }

        [Test]
        public void SePingTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var expectedResponse = "PONG";
            var actualValue = db.Execute("PING").ToString();
            Assert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        public void SePingMessageTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var expectedResponse = "HELLO";
            var actualValue = db.Execute("PING", "HELLO").ToString();
            Assert.AreEqual(expectedResponse, actualValue);
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
            Assert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        public void SeTimeCommandTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var actualValue = db.Execute("TIME");
            var seconds = ((RedisValue[])actualValue)[0];
            var microsecs = ((RedisValue[])actualValue)[1];
            Assert.AreEqual(seconds.ToString().Length, 10);
            Assert.AreEqual(microsecs.ToString().Length, 6);
        }


        [Test]
        public void SeTimeWithReturnErrorTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            Assert.Throws<RedisServerException>(() => db.Execute("TIME HELLO").ToString());
        }

        [Test]
        public async Task SeFlushDBTest([Values] bool async, [Values] bool unsafetruncatelog)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var key = "flushdbTest";
            var value = key;

            db.StringSet(key, value);
            var _value = db.StringGet(key);
            Assert.AreEqual(value, (string)_value);
            string[] p = default;

            if (async && unsafetruncatelog)
                p = new string[] { "ASYNC", "UNSAFETRUNCATELOG" };
            else if (unsafetruncatelog)
                p = new string[] { "UNSAFETRUNCATELOG" };

            if (async)
            {
                await db.ExecuteAsync("FLUSHDB", p).ConfigureAwait(false);
                _value = db.StringGet(key);
                while (!_value.IsNull)
                {
                    _value = db.StringGet(key);
                    Thread.Yield();
                }
            }
            else
            {
                db.Execute("FLUSHDB", p);
                _value = db.StringGet(key);
            }
            Assert.IsTrue(_value.IsNull);
        }

        #endregion
    }
}