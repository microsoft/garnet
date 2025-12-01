// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using GarnetJSON;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    public class LargeGet : CustomProcedure
    {
        public override bool Execute<TGarnetApi>(TGarnetApi garnetApi, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            static bool ResetBuffer(TGarnetApi garnetApi, ref MemoryResult<byte> output, int buffOffset)
            {
                bool status = garnetApi.ResetScratchBuffer(buffOffset);
                if (!status)
                    WriteError(ref output, "ERR ResetScratchBuffer failed");

                return status;
            }

            var offset = 0;
            var key = GetNextArg(ref procInput, ref offset);

            var buffOffset = garnetApi.GetScratchBufferOffset();
            for (var i = 0; i < 120_000; i++)
            {
                garnetApi.GET(key, out PinnedSpanByte outval);
                if (i % 100 == 0)
                {
                    if (!ResetBuffer(garnetApi, ref output, buffOffset))
                        return false;
                }
            }

            buffOffset = garnetApi.GetScratchBufferOffset();
            garnetApi.GET(key, out PinnedSpanByte outval1);
            garnetApi.GET(key, out PinnedSpanByte outval2);
            if (!ResetBuffer(garnetApi, ref output, buffOffset)) return false;

            buffOffset = garnetApi.GetScratchBufferOffset();
            var hashKey = GetNextArg(ref procInput, ref offset);
            var field = GetNextArg(ref procInput, ref offset);
            garnetApi.HashGet(hashKey, field, out var value);
            if (!ResetBuffer(garnetApi, ref output, buffOffset)) return false;

            return true;
        }
    }

    public class LargeGetTxn : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            int offset = 0;
            AddKey(GetNextArg(ref procInput, ref offset), LockType.Shared, StoreType.Main);
            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi garnetApi, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            int offset = 0;
            var key = GetNextArg(ref procInput, ref offset);
            var buffOffset = garnetApi.GetScratchBufferOffset();
            for (int i = 0; i < 120_000; i++)
            {
                garnetApi.GET(key, out PinnedSpanByte outval);
                if (i % 100 == 0)
                {
                    if (!garnetApi.ResetScratchBuffer(buffOffset))
                    {
                        WriteError(ref output, "ERR ResetScratchBuffer failed");
                        return;
                    }
                }
            }
        }
    }

    public class OutOfOrderFreeBuffer : CustomProcedure
    {
        public override bool Execute<TGarnetApi>(TGarnetApi garnetApi, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var key = GetNextArg(ref procInput, ref offset);

            var buffOffset1 = garnetApi.GetScratchBufferOffset();
            garnetApi.GET(key, out PinnedSpanByte outval1);

            var buffOffset2 = garnetApi.GetScratchBufferOffset();
            garnetApi.GET(key, out PinnedSpanByte outval2);

            if (!garnetApi.ResetScratchBuffer(buffOffset1))
            {
                WriteError(ref output, "ERR ResetScratchBuffer failed");
                return false;
            }

            // Previous reset call would have shrunk the buffer. This call should fail otherwise it will expand the buffer.
            if (garnetApi.ResetScratchBuffer(buffOffset2))
            {
                WriteError(ref output, "ERR ResetScratchBuffer shouldn't expand the buffer");
                return false;
            }

            return true;
        }
    }

    public class InvalidCommandProc : CustomProcedure
    {
        public override bool Execute<TGarnetApi>(TGarnetApi garnetApi, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var key = GetNextArg(ref procInput, ref offset);

            if (ParseCustomRawStringCommand("INVALIDCMD", out var _output))
            {
                WriteError(ref output, "ERR ExecuteCustomCommand should have failed");
            }
            else
            {
                WriteSimpleString(ref output, "OK");
            }

            return true;
        }
    }

    public class CustomIncrProc : CustomProcedure
    {
        public override bool Execute<TGarnetApi>(TGarnetApi garnetApi, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var keyToIncrement = GetNextArg(ref procInput, ref offset);

            // should update etag invisibly
            garnetApi.Increment(keyToIncrement, out long _, 1);

            var keyToReturn = GetNextArg(ref procInput, ref offset);
            garnetApi.GET(keyToReturn, out PinnedSpanByte outval);
            WriteBulkString(ref output, outval.Span);
            return true;
        }
    }

    public class RandomSubstituteOrExpandValForKeyTxn : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            int offset = 0;
            AddKey(GetNextArg(ref procInput, ref offset), LockType.Exclusive, StoreType.Main);
            AddKey(GetNextArg(ref procInput, ref offset), LockType.Exclusive, StoreType.Main);
            return true;
        }

        public override unsafe void Main<TGarnetApi>(TGarnetApi garnetApi, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            Random rnd = new Random();

            int offset = 0;
            var key = GetNextArg(ref procInput, ref offset);

            // key will have an etag associated with it already but the transaction should not be able to see it.
            // if the transaction needs to see it, then it can send GET with cmd as GETWITHETAG
            garnetApi.GET(key, out PinnedSpanByte outval);

            List<byte> valueToMessWith = outval.ToArray().ToList();

            // random decision of either substitute, expand, or reduce value
            char randChar = (char)('a' + rnd.Next(0, 26));
            int decision = rnd.Next(0, 100);
            if (decision < 33)
            {
                // substitute
                int idx = rnd.Next(0, valueToMessWith.Count);
                valueToMessWith[idx] = (byte)randChar;
            }
            else if (decision < 66)
            {
                valueToMessWith.Add((byte)randChar);
            }
            else
            {
                valueToMessWith.RemoveAt(valueToMessWith.Count - 1);
            }

            StringInput input = new StringInput(RespCommand.SET);
            input.header.cmd = RespCommand.SET;
            // if we send a SET we must explictly ask it to retain etag, and use conditional set
            input.header.metaCmd = RespMetaCommand.ExecWithEtag;

            fixed (byte* valuePtr = valueToMessWith.ToArray())
            {
                PinnedSpanByte valForKey1 = PinnedSpanByte.FromPinnedPointer(valuePtr, valueToMessWith.Count);
                input.parseState.InitializeWithArgument(valForKey1);
                // since we are setting with retain to etag, this change should be reflected in an etag update
                garnetApi.SET_Conditional(key, ref input);
            }

            var keyToIncrment = GetNextArg(ref procInput, ref offset);

            // for a non SET command the etag should be invisible and be updated automatically
            garnetApi.Increment(keyToIncrment, out long _, 1);
        }
    }

    [TestFixture]
    public class RespCustomCommandTests
    {
        GarnetServer server;
        private string _extTestDir1;
        private string _extTestDir2;

        [SetUp]
        public void Setup()
        {
            _extTestDir1 = Path.Combine(TestUtils.MethodTestDir, "test1");
            _extTestDir2 = Path.Combine(TestUtils.MethodTestDir, "test2");

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                disablePubSub: true,
                enableModuleCommand: Garnet.server.Auth.Settings.ConnectionProtectionOption.Yes,
                extensionBinPaths: [_extTestDir1, _extTestDir2],
                extensionAllowUnsignedAssemblies: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.DeleteDirectory(Directory.GetParent(_extTestDir1)?.FullName);
        }

        [Test]
        public void CustomCommandTest1()
        {
            // Register sample custom command (SETIFPM = "set if prefix match")
            int x = server.Register.NewCommand("SETIFPM", CommandType.ReadModifyWrite, new SetIfPMCustomCommand(), new RespCommandsInfo { Arity = 4 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "mykey";
            string origValue = "foovalue0";
            db.StringSet(key, origValue);

            string newValue1 = "foovalue1";
            db.Execute("SETIFPM", key, newValue1, "foo");

            // This conditional set should pass (prefix matches)
            string retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue1, retValue);

            // This conditional set should fail (prefix does not match)
            string newValue2 = "foovalue2";
            db.Execute("SETIFPM", key, newValue2, "bar");

            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue1, retValue);

            // This conditional set should pass (prefix matches)
            // New value is smaller than existing
            string newValue3 = "fooval3";
            db.Execute("SETIFPM", key, newValue3, "foo");

            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue3, retValue);


            // This conditional set should pass (prefix matches)
            // New value is smaller than existing
            string newValue4 = "foolargervalue4";
            db.Execute("SETIFPM", key, newValue4, "foo");

            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue4, retValue);
        }

        [Test]
        public void CustomCommandTest2()
        {
            // Register custom command on raw strings (SETWPIFPGT = "set with prefix, if prefix greater than")
            server.Register.NewCommand("SETWPIFPGT", CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand(), new RespCommandsInfo { Arity = 4 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mykey";
            var origValue = "foovalue0";

            // This conditional set should fail (wrong prefix size)
            bool exception = false;
            try
            {
                db.Execute("SETWPIFPGT", key, origValue, "foo");
            }
            catch (Exception ex)
            {
                exception = true;
                ClassicAssert.AreEqual(SetWPIFPGTCustomCommand.PrefixError, ex.Message);
            }
            ClassicAssert.IsTrue(exception);
            string retValue = db.StringGet(key);
            ClassicAssert.AreEqual(null, retValue);

            // This conditional set should pass (nothing there to begin with)
            db.Execute("SETWPIFPGT", key, origValue, BitConverter.GetBytes((long)0));
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(retValue.Substring(8), origValue);

            // This conditional set should fail (wrong prefix size)
            var newValue1 = "foovalue1";
            exception = false;
            try
            {
                db.Execute("SETWPIFPGT", key, newValue1, "foobar123");
            }
            catch (Exception ex)
            {
                exception = true;
                ClassicAssert.AreEqual(SetWPIFPGTCustomCommand.PrefixError, ex.Message);
            }
            ClassicAssert.IsTrue(exception);
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(retValue.Substring(8), origValue);

            // This conditional set should pass (prefix is greater)
            db.Execute("SETWPIFPGT", key, newValue1, BitConverter.GetBytes((long)1));
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue1, retValue.Substring(8));

            // This conditional set should fail (prefix is not greater)
            var newValue2 = "foovalue2";
            db.Execute("SETWPIFPGT", key, newValue2, BitConverter.GetBytes((long)1));
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue1, retValue.Substring(8));

            // This conditional set should pass (prefix is greater)
            // New value is smaller than existing
            var newValue3 = "fooval3";
            db.Execute("SETWPIFPGT", key, newValue3, BitConverter.GetBytes((long)3));
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue3, retValue.Substring(8));

            // This conditional set should pass (prefix is greater)
            // New value is larger than existing
            var newValue4 = "foolargervalue4";
            db.Execute("SETWPIFPGT", key, newValue4, BitConverter.GetBytes((long)4));
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue4, retValue.Substring(8));

            // This conditional set should pass (prefix is greater)
            var newValue5 = "foolargervalue4";
            db.Execute("SETWPIFPGT", key, newValue4, BitConverter.GetBytes(long.MaxValue));
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue5, retValue.Substring(8));
        }

        [Test]
        public void CustomCommandTest3()
        {
            // Register custom command on raw strings (SETWPIFPGT = "set with prefix, if prefix greater than")
            server.Register.NewCommand("SETWPIFPGT", CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand(), new RespCommandsInfo { Arity = 4 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mykey";
            var origValue = "foovalue0";

            // This conditional set should pass (nothing there to begin with)
            db.Execute("SETWPIFPGT", key, origValue, BitConverter.GetBytes((long)0));
            string retValue = db.StringGet(key);
            ClassicAssert.AreEqual(retValue.Substring(8), origValue);

            db.KeyExpire(key, TimeSpan.FromSeconds(1));
            Thread.Sleep(1100);

            // Key expired, return fails
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(null, retValue);

            // This conditional set should pass (nothing there to begin with)
            db.Execute("SETWPIFPGT", key, origValue, BitConverter.GetBytes((long)1));
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(origValue, retValue.Substring(8));
        }

        [Test]
        public void CustomCommandTest4()
        {
            // Register custom command on raw strings (SETWPIFPGT = "set with prefix, if prefix greater than")
            server.Register.NewCommand("SETWPIFPGT", CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand(), new RespCommandsInfo { Arity = 4 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mykey";
            var origValue = "foovalue0";

            // This conditional set should pass (nothing there to begin with)
            db.Execute("SETWPIFPGT", key, origValue, BitConverter.GetBytes((long)0));
            string retValue = db.StringGet(key);
            ClassicAssert.AreEqual(retValue.Substring(8), origValue);

            db.KeyExpire(key, TimeSpan.FromSeconds(100));

            // Key not expired
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(retValue.Substring(8), origValue);

            // This conditional set should pass
            var newValue2 = "foovalue2";
            db.Execute("SETWPIFPGT", key, newValue2, BitConverter.GetBytes((long)1));
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue2, retValue.Substring(8));
        }

        [Test]
        public void CustomCommandTest5()
        {
            // Register custom command on raw strings (SETWPIFPGT = "set with prefix, if prefix greater than")
            server.Register.NewCommand("SETWPIFPGT", CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand(), new RespCommandsInfo { Arity = 4 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var key = "mykey";
            var origValue = "foovalue0";

            // This conditional set should pass (nothing there to begin with)
            db.Execute("SETWPIFPGT", key, origValue, BitConverter.GetBytes((long)0));
            string retValue = db.StringGet(key);
            ClassicAssert.AreEqual(retValue.Substring(8), origValue);

            db.KeyExpire(key, TimeSpan.FromSeconds(100));

            // This conditional set should pass
            db.Execute("SETWPIFPGT", key, origValue, BitConverter.GetBytes((long)1));
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(origValue, retValue.Substring(8));

            // Expiration should survive operation
            var ttl = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(ttl > TimeSpan.FromSeconds(10));

            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(retValue.Substring(8), origValue);

            // This conditional set should pass
            var newValue2 = "foovalue2";
            db.Execute("SETWPIFPGT", key, newValue2, BitConverter.GetBytes((long)2));

            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue2, retValue.Substring(8));
        }

        [Test]
        public void CustomCommandTest6()
        {
            // Register sample custom command (SETIFPM = "set if prefix match")
            server.Register.NewCommand("DELIFM", CommandType.ReadModifyWrite, new DeleteIfMatchCustomCommand(), new RespCommandsInfo { Arity = 3 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "mykey";
            string origValue = "foovalue0";
            db.StringSet(key, origValue);

            // DELIFM with different value
            string newValue1 = "foovalue1";
            db.Execute("DELIFM", key, newValue1);

            // Delete should not have happened, as value does not match
            string retValue = db.StringGet(key);
            ClassicAssert.AreEqual(origValue, retValue);

            // DELIFM with same value
            db.Execute("DELIFM", key, origValue);

            // Delete should have happened, as value matches
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(null, retValue);
        }

        [Test]
        public async Task CustomCommandCaseInsensitiveTest()
        {
            server.Register.NewCommand("A.SETIFPM", CommandType.ReadModifyWrite, new SetIfPMCustomCommand(), new RespCommandsInfo { Arity = 4 });

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            var key = "mykey";
            var origValue = "foovalue0";
            c.Execute("SET", key, origValue);

            var newValue1 = "foovalue1";
            var response = await c.ExecuteAsync("a.setifpm", key, newValue1, "foo");
            // Test the command was recognized.
            ClassicAssert.AreEqual("OK", response);

            // Test the command did something.
            var retValue = await c.ExecuteAsync("GET", key);
            ClassicAssert.AreEqual(newValue1, retValue);
        }

        [Test]
        public void CustomObjectCommandTest1()
        {
            // Register sample custom command on object
            var factory = new MyDictFactory();
            server.Register.NewCommand("MYDICTSET", CommandType.ReadModifyWrite, factory, new MyDictSet(), new RespCommandsInfo { Arity = 4 });
            server.Register.NewCommand("MYDICTGET", CommandType.Read, factory, new MyDictGet(), new RespCommandsInfo { Arity = 3 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string mainkey = "key";

            string key1 = "mykey1";
            string value1 = "foovalue1";
            db.Execute("MYDICTSET", mainkey, key1, value1);

            var retValue = db.Execute("MYDICTGET", mainkey, key1);
            ClassicAssert.AreEqual(value1, (string)retValue);

            var result = db.Execute("MEMORY", "USAGE", mainkey);
            var actualValue = ResultType.Integer == result.Resp2Type ? Int32.Parse(result.ToString()) : -1;
            var expectedResponse = 304;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            string key2 = "mykey2";
            string value2 = "foovalue2";
            db.Execute("MYDICTSET", mainkey, key2, value2);

            retValue = db.Execute("MYDICTGET", mainkey, key2);
            ClassicAssert.AreEqual(value2, (string)retValue);

            result = db.Execute("MEMORY", "USAGE", mainkey);
            actualValue = ResultType.Integer == result.Resp2Type ? Int32.Parse(result.ToString()) : -1;
            expectedResponse = 440;
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CustomCommandSetWhileKeyHasTtlTest()
        {
            // Register sample custom command (SETWPIFPGT = "set if prefix greater than")
            server.Register.NewCommand("SETWPIFPGT", CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand(), new RespCommandsInfo { Arity = 4 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int expire = 5;
            string key = "mykey";
            string origValue = "foovalue0";
            long prefix = 0;
            db.Execute("SETWPIFPGT", key, origValue, BitConverter.GetBytes(prefix));
            string retValue = db.StringGet(key);
            ClassicAssert.AreEqual(origValue, retValue.Substring(8));

            db.KeyExpire(key, TimeSpan.FromSeconds(expire));
            var time = db.KeyTimeToLive(key);
            ClassicAssert.IsTrue(time.Value.TotalSeconds > 0);

            // This conditional set should pass (new prefix is greater)
            string newValue1 = "foovalue1";
            prefix = 1;
            db.Execute("SETWPIFPGT", key, newValue1, BitConverter.GetBytes(prefix));
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue1, retValue.Substring(8));

            Thread.Sleep((expire + 1) * 1000);
            string value = db.StringGet(key);
            ClassicAssert.AreEqual(null, value);
        }

        [Test]
        public void CustomCommandSetAfterKeyDeletedWithTtlTest()
        {
            // Register sample custom command (SETWPIFPGT = "set if prefix greater than")
            server.Register.NewCommand("SETWPIFPGT", CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand(), new RespCommandsInfo { Arity = 4 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int expire = 1;
            string key = "mykey";
            string origValue = "foovalue0";
            long prefix = 0;
            db.Execute("SETWPIFPGT", key, origValue, BitConverter.GetBytes(prefix));

            db.KeyExpire(key, TimeSpan.FromSeconds(expire));

            // Key should expire
            Thread.Sleep((expire + 1) * 1000);
            string value = db.StringGet(key);
            ClassicAssert.AreEqual(null, value);

            // Setting on they key that was deleted with a custom command should succeed
            string newValue1 = "foovalue10";
            prefix = 1;
            db.Execute("SETWPIFPGT", key, newValue1, BitConverter.GetBytes(prefix));
            string retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue1, retValue.Substring(8));
        }

        [Test]
        public void CustomObjectCommandTest2()
        {
            // Register sample custom command on object
            var factory = new MyDictFactory();
            server.Register.NewCommand("MYDICTSET", CommandType.ReadModifyWrite, factory, new MyDictSet(), new RespCommandsInfo { Arity = 4 });
            server.Register.NewCommand("MYDICTGET", CommandType.Read, factory, new MyDictGet(), new RespCommandsInfo { Arity = 3 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string mainkey = "key";

            string key1 = "mykey1";
            string value1 = "foovalue1";
            db.Execute("MYDICTSET", mainkey, key1, value1);

            var retValue = db.Execute("MYDICTGET", mainkey, key1);
            ClassicAssert.AreEqual(value1, (string)retValue);

            db.KeyExpire(mainkey, TimeSpan.FromSeconds(1));
            Thread.Sleep(1100);

            retValue = db.Execute("MYDICTGET", mainkey, key1);
            ClassicAssert.AreEqual(null, (string)retValue);

            string key2 = "mykey2";
            string value2 = "foovalue2";
            db.Execute("MYDICTSET", mainkey, key2, value2);

            retValue = db.Execute("MYDICTGET", mainkey, key1);
            ClassicAssert.AreEqual(null, (string)retValue);

            retValue = db.Execute("MYDICTGET", mainkey, key2);
            ClassicAssert.AreEqual(value2, (string)retValue);
        }

        [Test]
        public void CustomObjectCommandTest3()
        {
            // Register sample custom command on object
            var factory = new MyDictFactory();
            server.Register.NewCommand("MYDICTSET", CommandType.ReadModifyWrite, factory, new MyDictSet(), new RespCommandsInfo { Arity = 4 });
            server.Register.NewCommand("MYDICTGET", CommandType.Read, factory, new MyDictGet(), new RespCommandsInfo { Arity = 3 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var mainkey = "key";

            var key1 = "mykey1";
            var value1 = "foovalue1";
            db.ListLeftPush(mainkey, value1);

            var ex = Assert.Throws<RedisServerException>(() => db.Execute("MYDICTGET", mainkey, key1));
            var expectedError = Encoding.ASCII.GetString(CmdStrings.RESP_ERR_WRONG_TYPE);
            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(expectedError, ex.Message);

            var deleted = db.KeyDelete(mainkey);
            ClassicAssert.IsTrue(deleted);
            db.Execute("MYDICTSET", mainkey, key1, value1);

            ex = Assert.Throws<RedisServerException>(() => db.ListLeftPush(mainkey, value1));
            ClassicAssert.IsNotNull(ex);
            ClassicAssert.AreEqual(expectedError, ex.Message);
        }

        [Test]
        public void CustomObjectCommandTest4()
        {
            // Register sample custom command on object 1
            var factory = new MyDictFactory();
            server.Register.NewCommand("MYDICTSET", CommandType.ReadModifyWrite, factory, new MyDictSet(), new RespCommandsInfo { Arity = 4 });
            server.Register.NewCommand("MYDICTGET", CommandType.Read, factory, new MyDictGet(), new RespCommandsInfo { Arity = 3 });

            // Register sample custom command on object 2
            var jsonFactory = new GarnetJsonObjectFactory();
            server.Register.NewCommand("JSON.SET", CommandType.ReadModifyWrite, jsonFactory, new JsonSET());
            server.Register.NewCommand("JSON.GET", CommandType.Read, jsonFactory, new JsonGET());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Test custom commands of object 1 
            var mainkey = "key";

            var key1 = "mykey1";
            var value1 = "foovalue1";
            db.Execute("MYDICTSET", mainkey, key1, value1);

            var retValue = db.Execute("MYDICTGET", mainkey, key1);
            ClassicAssert.AreEqual(value1, (string)retValue);

            var key2 = "mykey2";
            var value2 = "foovalue2";
            db.Execute("MYDICTSET", mainkey, key2, value2);

            retValue = db.Execute("MYDICTGET", mainkey, key2);
            ClassicAssert.AreEqual(value2, (string)retValue);

            // Test custom commands of object 2
            db.Execute("JSON.SET", "k1", "$", "{\"f1\": {\"a\":1}, \"f2\":{\"a\":2}}");
            var result = db.Execute("JSON.GET", "k1");
            ClassicAssert.AreEqual("{\"f1\":{\"a\":1},\"f2\":{\"a\":2}}", result.ToString());
        }

        [Test]
        public async Task CustomCommandSetFollowedByTtlTestAsync()
        {
            // Register sample custom command (SETWPIFPGT = "set if prefix greater than")
            server.Register.NewCommand("SETWPIFPGT", CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand(), new RespCommandsInfo { Arity = 4 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            int expire = 180;
            string key = "mykeyKeyExpire";
            string origValue = "foovalue0";
            long prefix = 0;

            await db.ExecuteAsync("SETWPIFPGT", key, origValue, BitConverter.GetBytes(prefix));
            await db.KeyExpireAsync(key, TimeSpan.FromMinutes(expire));

            string retValue = db.StringGet(key);
            ClassicAssert.AreEqual(origValue, retValue.Substring(8));

            string newValue1 = "foovalue10";
            prefix = 1;
            await db.ExecuteAsync("SETWPIFPGT", key, newValue1, BitConverter.GetBytes(prefix));
            await db.KeyExpireAsync(key, TimeSpan.FromMinutes(expire));

            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue1, retValue.Substring(8));
        }

        [Test]
        public async Task CustomCommandSetWithCustomExpirationTestAsync()
        {
            // Register sample custom command (SETWPIFPGT = "set if prefix greater than")
            server.Register.NewCommand("SETWPIFPGT", CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand(), new RespCommandsInfo { Arity = 4 },
                expirationTicks: TimeSpan.FromSeconds(4).Ticks); // provide default expiration at registration time

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string key = "mykeyCommandExpire";
            string origValue = "foovalue0";
            long prefix = 0;

            await db.ExecuteAsync("SETWPIFPGT", key, origValue, BitConverter.GetBytes(prefix));

            string retValue = db.StringGet(key);
            ClassicAssert.AreEqual(origValue, retValue.Substring(8));

            string newValue1 = "foovalue10";
            prefix = 1;
            await db.ExecuteAsync("SETWPIFPGT", key, newValue1, BitConverter.GetBytes(prefix));

            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue1, retValue.Substring(8));

            Thread.Sleep(5000);

            // should be expired now
            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(null, retValue);
        }

        [Test]
        public void CustomCommandRegistrationTest()
        {
            server.Register.NewProcedure("SUM", () => new Sum());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.StringSet("key1", "10");
            db.StringSet("key2", "str10");
            db.StringSet("key3", "20");

            // Include non-existent and string keys as well
            var retValue = db.Execute("SUM", "key1", "key2", "key3", "key4");
            ClassicAssert.AreEqual("30", retValue.ToString());
        }

        [Test]
        public void CustomProcedureFreeBufferTest()
        {
            server.Register.NewProcedure("LARGEGET", () => new LargeGet());
            var key = "key";
            var hashKey = "hashKey";
            var hashField = "field";
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            byte[] value = new byte[10_000];
            db.StringSet(key, value);
            db.HashSet(hashKey, [new HashEntry(hashField, value)]);

            try
            {
                var result = db.Execute("LARGEGET", key, hashKey, hashField);
                ClassicAssert.AreEqual("OK", result.ToString());
            }
            catch (RedisServerException rse)
            {
                ClassicAssert.Fail(rse.Message);
            }
        }

        [Test]
        public void CustomTxnFreeBufferTest()
        {
            server.Register.NewTransactionProc("LARGEGETTXN", () => new LargeGetTxn());
            var key = "key";
            var hashKey = "hashKey";
            var hashField = "field";
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            byte[] value = new byte[10_000];
            db.StringSet(key, value);
            db.HashSet(hashKey, [new HashEntry(hashField, value)]);

            try
            {
                var result = db.Execute("LARGEGETTXN", key);
                ClassicAssert.AreEqual("OK", result.ToString());
            }
            catch (RedisServerException rse)
            {
                ClassicAssert.Fail(rse.Message);
            }
        }

        [Test]
        public void CustomProcedureOutOfOrderFreeBufferTest()
        {
            server.Register.NewProcedure("OUTOFORDERFREE", () => new OutOfOrderFreeBuffer());
            var key = "key";
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            byte[] value = new byte[10_000];
            db.StringSet(key, value);

            var result = db.Execute("OUTOFORDERFREE", key);
            ClassicAssert.AreEqual("OK", result.ToString());
        }

        private string[] CreateTestLibraries()
        {
            var runtimePath = RuntimeEnvironment.GetRuntimeDirectory();
            var binPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            ClassicAssert.IsNotNull(binPath);

            var namespaces = new[]
            {
                "Tsavorite.core",
                "Garnet.common",
                "Garnet.server",
                "System",
                "System.Buffers",
                "System.Collections.Generic",
                "System.Diagnostics",
                "System.IO",
                "System.Text",
            };

            var referenceFiles = new[]
            {
                Path.Combine(runtimePath, "System.dll"),
                Path.Combine(runtimePath, "System.Collections.dll"),
                Path.Combine(runtimePath, "System.Core.dll"),
                Path.Combine(runtimePath, "System.Private.CoreLib.dll"),
                Path.Combine(runtimePath, "System.Runtime.dll"),
                Path.Combine(binPath, "Tsavorite.core.dll"),
                Path.Combine(binPath, "Garnet.common.dll"),
                Path.Combine(binPath, "Garnet.server.dll"),
            };

            var dir1 = Path.Combine(this._extTestDir1, Path.GetFileName(TestUtils.MethodTestDir));
            var dir2 = Path.Combine(this._extTestDir2, Path.GetFileName(TestUtils.MethodTestDir));

            Directory.CreateDirectory(this._extTestDir1);
            Directory.CreateDirectory(dir1);
            Directory.CreateDirectory(this._extTestDir2);
            Directory.CreateDirectory(dir2);

            var testFilePath = Path.Combine(TestUtils.MethodTestDir, "test.cs");
            using (var testFile = File.CreateText(testFilePath))
            {
                testFile.WriteLine("namespace Garnet { public class TestClass { } }");
            }

            var libPathToFiles = new Dictionary<string, string[]>
            {
                { Path.Combine(dir1, "testLib1.dll"),
                    new []
                    {
                        Path.GetFullPath(@"../main/GarnetServer/Extensions/MyDictObject.cs", TestUtils.RootTestsProjectPath),
                        Path.GetFullPath(@"../main/GarnetServer/Extensions/MyDictSet.cs", TestUtils.RootTestsProjectPath),
                        Path.GetFullPath(@"../main/GarnetServer/Extensions/MyDictGet.cs", TestUtils.RootTestsProjectPath)
                    }
                },
                { Path.Combine(dir2, "testLib2.dll"), new [] { Path.GetFullPath(@"../main/GarnetServer/Extensions/SetIfPM.cs", TestUtils.RootTestsProjectPath) }},
                { Path.Combine(dir2, "testLib3.dll"), new []
                {
                    Path.GetFullPath(@"../main/GarnetServer/Extensions/ReadWriteTxn.cs", TestUtils.RootTestsProjectPath),
                    testFilePath,
                }}
            };

            foreach (var ltf in libPathToFiles)
            {
                TestUtils.CreateTestLibrary(namespaces, referenceFiles, ltf.Value, ltf.Key);
            }

            var notAllowedPath = Path.Combine(TestUtils.MethodTestDir, "testLib4.dll");
            if (!File.Exists(notAllowedPath))
            {
                File.Copy(Path.Combine(dir1, "testLib1.dll"), notAllowedPath);
            }

            return [Path.Combine(dir1, "testLib1.dll"), dir2];
        }

        [Test]
        public void RegisterCustomCommandTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var libraryPaths = this.CreateTestLibraries();

            var args = new List<object>
            {
                "TXN", "READWRITETX", 3, "ReadWriteTxn",
                "RMW", "SETIFPM", 2, "SetIfPMCustomCommand", TimeSpan.FromSeconds(10).Ticks,
                "RMW", "MYDICTSET", 2, "MyDictFactory", "MyDictSet",
                "READ", "MYDICTGET", 1, "MyDictFactory", "MyDictGet",
                "SRC",
            };
            args.AddRange(libraryPaths);

            // Register select custom commands and transactions
            var resp = (string)db.Execute($"REGISTERCS",
                [.. args]);

            // Test READWRITETX
            string key1 = "readkey1";
            string key2 = "readkey2";
            string value = "foovalue0";
            db.StringSet(key1, value);

            string writekey1 = "writekey1";
            string writekey2 = "writekey2";

            var result = db.Execute("READWRITETX", key1, writekey1, writekey2);
            ClassicAssert.AreEqual("SUCCESS", (string)result);

            // Read keys to verify transaction succeeded
            string retValue = db.StringGet(writekey1);
            ClassicAssert.IsNotNull(retValue);
            ClassicAssert.AreEqual(value, retValue);

            retValue = db.StringGet(writekey2);
            ClassicAssert.AreEqual(value, retValue);

            // Test SETIFPM
            string newValue1 = "foovalue1";
            string newValue2 = "foovalue2";

            // This conditional set should pass (prefix matches)
            result = db.Execute("SETIFPM", key1, newValue1, "foo");
            ClassicAssert.AreEqual("OK", (string)result);

            retValue = db.StringGet(key1);
            ClassicAssert.AreEqual(newValue1, retValue);

            // This conditional set should fail (prefix does not match)
            result = db.Execute("SETIFPM", key1, newValue2, "bar");
            ClassicAssert.AreEqual("OK", (string)result);

            retValue = db.StringGet(key1);
            ClassicAssert.AreEqual(newValue1, retValue);

            // Test MYDICTSET
            string newKey1 = "newkey1";
            string newKey2 = "newkey2";

            db.Execute("MYDICTSET", key2, newKey1, newValue1);

            var dictVal = db.Execute("MYDICTGET", key2, newKey1);
            ClassicAssert.AreEqual(newValue1, (string)dictVal);

            db.Execute("MYDICTSET", key2, newKey2, newValue2);

            // Test MYDICTGET
            dictVal = db.Execute("MYDICTGET", key2, newKey2);
            ClassicAssert.AreEqual(newValue2, (string)dictVal);
        }

        [Test]
        public void RegisterCustomCommandErrorConditionsTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var libraryPaths = this.CreateTestLibraries();

            // Malformed request #1 - no arguments
            string resp = null;
            try
            {
                resp = (string)db.Execute($"REGISTERCS");
            }
            catch (RedisServerException rse)
            {
                ClassicAssert.AreEqual(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs, "REGISTERCS")), rse.Message);
            }
            ClassicAssert.IsNull(resp);

            // Malformed request #2 - binary paths before sub-command
            var args = new List<object>() { "SRC" };
            args.AddRange(libraryPaths);
            args.AddRange(["TXN", "READWRITETX", 3, "ReadWriteTxn"]);

            try
            {
                resp = (string)db.Execute($"REGISTERCS", [.. args]);
            }
            catch (RedisServerException rse)
            {
                ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_MALFORMED_REGISTERCS_COMMAND), rse.Message);
            }
            ClassicAssert.IsNull(resp);

            // Binary file not contained in allowed paths
            args =
            [
                "RMW",
                "MYDICTSET",
                2,
                "MyDictFactory",
                "SRC",
                Path.Combine(TestUtils.MethodTestDir, "testLib4.dll")
            ];

            try
            {
                resp = (string)db.Execute($"REGISTERCS", [.. args]);
            }
            catch (RedisServerException rse)
            {
                ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_BINARY_FILES_NOT_IN_ALLOWED_PATHS), rse.Message);
            }
            ClassicAssert.IsNull(resp);

            // Class not in supplied dlls
            args =
            [
                "RMW",
                "MYDICTSET",
                2,
                "MyDictFactory",
                "SRC",
                .. libraryPaths.Skip(1),
            ];

            try
            {
                resp = (string)db.Execute($"REGISTERCS", [.. args]);
            }
            catch (RedisServerException rse)
            {
                ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_INSTANTIATING_CLASS), rse.Message);
            }
            ClassicAssert.IsNull(resp);

            // Class not in supported
            args =
            [
                "RMW",
                "MYDICTSET",
                2,
                "TestClass",
                "SRC",
                .. libraryPaths,
            ];

            try
            {
                resp = (string)db.Execute($"REGISTERCS", [.. args]);
            }
            catch (RedisServerException rse)
            {
                ClassicAssert.AreEqual(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_REGISTERCS_UNSUPPORTED_CLASS), rse.Message);
            }
            ClassicAssert.IsNull(resp);
        }

        [Test]
        public void RateLimiterTest()
        {
            server.Register.NewTransactionProc("RATELIMIT", () => new RateLimiterTxn(), new RespCommandsInfo { Arity = 4 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Basic allowed entries within limit
            var result = db.Execute("RATELIMIT", "key1", 1000, 5);
            ClassicAssert.AreEqual("ALLOWED", result.ToString());

            result = db.Execute("RATELIMIT", "key1", 1000, 5);
            ClassicAssert.AreEqual("ALLOWED", result.ToString());

            // Throttled test
            for (var i = 0; i < 5; i++)
            {
                result = db.Execute("RATELIMIT", "key2", 10000, 5);
                ClassicAssert.AreEqual("ALLOWED", result.ToString());
            }

            result = db.Execute("RATELIMIT", "key2", 1000, 5);
            ClassicAssert.AreEqual("THROTTLED", result.ToString());

            // Test sliding window expiration
            for (var i = 0; i < 5; i++)
            {
                result = db.Execute("RATELIMIT", "key3", 1000, 5);
                ClassicAssert.AreEqual("ALLOWED", result.ToString());
            }
            Thread.Sleep(TimeSpan.FromSeconds(1));
            result = db.Execute("RATELIMIT", "key3", 1000, 5);
            ClassicAssert.AreEqual("ALLOWED", result.ToString());
        }

        [Test]
        public void SortedSetCountTxn()
        {
            server.Register.NewTransactionProc("SORTEDSETCOUNT", () => new SortedSetCountTxn(), new RespCommandsInfo { Arity = 4 });

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Add entries to sorted set
            for (var i = 1; i < 10; i++)
            {
                db.SortedSetAdd("key1", $"field{i}", i);
            }

            // Run transaction to get count of elements in given range of sorted set
            var result = db.Execute("SORTEDSETCOUNT", "key1", 5, 10);
            ClassicAssert.AreEqual(5, (int)result);
        }

        [Test]
        public void CustomProcInvokingCustomCmdTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            server.Register.NewCommand("SETIFPM", CommandType.ReadModifyWrite, new SetIfPMCustomCommand(), new RespCommandsInfo { Arity = 4 });
            server.Register.NewProcedure("PROCCMD", () => new ProcCustomCmd());

            var key = "mainKey";
            var value = "foovalue0";
            db.StringSet(key, value);

            var newValue1 = "foovalue1";
            var newValue2 = "foovalue2";

            // This conditional set should pass (prefix matches)
            var result = db.Execute("PROCCMD", key, newValue1, "foo");
            ClassicAssert.AreEqual("OK", (string)result);

            var retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue1, retValue.ToString());

            // This conditional set should fail (prefix does not match)
            result = db.Execute("PROCCMD", key, newValue2, "bar");
            ClassicAssert.AreEqual("OK", (string)result);

            retValue = db.StringGet(key);
            ClassicAssert.AreEqual(newValue1, retValue.ToString());
        }

        [Test]
        public void CustomTransactionInvokingCustomCmdTest()
        {
            var factory = new MyDictFactory();
            server.Register.NewCommand("MYDICTSET", CommandType.ReadModifyWrite, factory, new MyDictSet(), new RespCommandsInfo { Arity = 4 });
            server.Register.NewCommand("MYDICTGET", CommandType.Read, factory, new MyDictGet(), new RespCommandsInfo { Arity = 3 });
            server.Register.NewTransactionProc("TXNCMD", () => new TxnCustomCmd());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var mainKey = "mainKey";
            var mainValue = "foovalue0";
            var dictKey = "dictKey";
            var dictField = "dictField";
            var dictValue = "dictValue";

            var result = db.Execute("TXNCMD", mainKey, mainValue, dictKey, dictField, dictValue);
            ClassicAssert.AreEqual("OK", (string)result);

            result = db.Execute("MYDICTGET", dictKey, dictField);
            ClassicAssert.AreEqual(dictValue, (string)result);

            var retValue = db.StringGet(mainKey);
            ClassicAssert.AreEqual(mainValue, (string)retValue);
        }

        [Test]
        public void CustomProcedureInvokingInvalidCommandTest()
        {
            server.Register.NewProcedure("PROCINVALIDCMD", () => new InvalidCommandProc());

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var result = db.Execute("PROCINVALIDCMD", "key");
            ClassicAssert.AreEqual("OK", (string)result);
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void MultiRegisterCommandTest(bool sync)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var regCount = 24;
            var regCmdTasks = new Task[regCount];
            for (var i = 0; i < regCount; i++)
            {
                var idx = i;
                regCmdTasks[i] = new Task(() => server.Register.NewCommand($"SETIFPM{idx + 1}", CommandType.ReadModifyWrite, new SetIfPMCustomCommand(),
                    new RespCommandsInfo { Arity = 4 }));
            }

            for (var i = 0; i < regCount; i++)
            {
                if (sync)
                {
                    regCmdTasks[i].RunSynchronously();
                }
                else
                {
                    regCmdTasks[i].Start();
                }
            }

            if (!sync) Task.WaitAll(regCmdTasks);

            for (var i = 0; i < regCount; i++)
            {
                var key = $"mykey{i + 1}";
                var origValue = "foovalue0";
                db.StringSet(key, origValue);

                var newValue1 = "foovalue1";
                db.Execute($"SETIFPM{i + 1}", key, newValue1, "foo");

                // This conditional set should pass (prefix matches)
                string retValue = db.StringGet(key);
                ClassicAssert.AreEqual(newValue1, retValue);
            }
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void MultiRegisterSubCommandTest(bool sync)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var factory = new MyDictFactory();
            server.Register.NewCommand("MYDICTGET", CommandType.Read, factory, new MyDictGet(), new RespCommandsInfo { Arity = 3 });

            // Only able to register 31 sub-commands, try to register 32
            var regCount = 32;
            var failedTaskIdAndMessage = new ConcurrentBag<(int, string)>();
            var regCmdTasks = new Task[regCount];
            for (var i = 0; i < regCount; i++)
            {
                var idx = i;
                regCmdTasks[i] = new Task(() =>
                {
                    try
                    {
                        server.Register.NewCommand($"MYDICTSET{idx + 1}",
                            CommandType.ReadModifyWrite, factory, new MyDictSet(), new RespCommandsInfo { Arity = 4 });
                    }
                    catch (Exception e)
                    {
                        failedTaskIdAndMessage.Add((idx, e.Message));
                    }
                });
            }

            for (var i = 0; i < regCount; i++)
            {
                if (sync)
                {
                    regCmdTasks[i].RunSynchronously();
                }
                else
                {
                    regCmdTasks[i].Start();
                }
            }

            if (!sync) Task.WaitAll(regCmdTasks);

            // Exactly one registration should fail
            ClassicAssert.AreEqual(1, failedTaskIdAndMessage.Count);
            failedTaskIdAndMessage.TryTake(out var failedTaskResult);

            var failedTaskId = failedTaskResult.Item1;
            var failedTaskMessage = failedTaskResult.Item2;
            ClassicAssert.AreEqual("Out of registration space", failedTaskMessage);

            var mainkey = "key";

            // Check that all registrations worked except the failed one
            for (var i = 0; i < regCount; i++)
            {
                if (i == failedTaskId) continue;
                var key1 = $"mykey{i + 1}";
                var value1 = $"foovalue{i + 1}";
                db.Execute($"MYDICTSET{i + 1}", mainkey, key1, value1);

                var retValue = db.Execute("MYDICTGET", mainkey, key1);
                ClassicAssert.AreEqual(value1, (string)retValue);
            }
        }

        [Test]
        public void MultiRegisterTxnTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var regCount = byte.MaxValue + 1;
            for (var i = 0; i < regCount; i++)
            {
                server.Register.NewTransactionProc($"GETTWOKEYSNOTXN{i + 1}", () => new GetTwoKeysNoTxn(), new RespCommandsInfo { Arity = 3 });
            }

            // This register should fail as there could only be byte.MaxValue + 1 transactions registered
            var e = Assert.Throws<Exception>(() => server.Register.NewTransactionProc($"GETTWOKEYSNOTXN{byte.MaxValue + 3}", () => new GetTwoKeysNoTxn(), new RespCommandsInfo { Arity = 3 }));

            ClassicAssert.AreEqual("Out of registration space", e.Message);

            for (var i = 0; i < regCount; i++)
            {
                var readkey1 = $"readkey{i + 1}.1";
                var value1 = $"foovalue{i + 1}.1";
                db.StringSet(readkey1, value1);

                var readkey2 = $"readkey{i + 1}.2";
                var value2 = $"foovalue{i + 1}.2";
                db.StringSet(readkey2, value2);

                var result = db.Execute($"GETTWOKEYSNOTXN{i + 1}", readkey1, readkey2);

                ClassicAssert.AreEqual(value1, ((string[])result)?[0]);
                ClassicAssert.AreEqual(value2, ((string[])result)?[1]);
            }
        }

        [Test]
        public void MultiRegisterProcTest()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var regCount = byte.MaxValue + 1;
            for (var i = 0; i < regCount; i++)
            {
                server.Register.NewProcedure($"SUM{i + 1}", () => new Sum());
            }

            // This register should fail as there could only be byte.MaxValue + 1 procedures registered
            var e = Assert.Throws<Exception>(() => server.Register.NewProcedure($"SUM{byte.MaxValue + 3}", () => new Sum()));
            ClassicAssert.AreEqual("Out of registration space", e.Message);

            db.StringSet("key1", "10");
            db.StringSet("key2", "35");
            db.StringSet("key3", "20");

            for (var i = 0; i < regCount; i++)
            {
                // Include non-existent and string keys as well
                var retValue = db.Execute($"SUM{i + 1}", "key1", "key2", "key3", "key4");
                ClassicAssert.AreEqual("65", retValue.ToString());
            }
        }

        [Test]
        public void CustomTxnEtagInteractionTest()
        {
            server.Register.NewTransactionProc("RANDOPS", () => new RandomSubstituteOrExpandValForKeyTxn());

            var key1 = "key1";
            var value1 = "thisisstarting";

            var key2 = "key2";
            var value2 = "17";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            try
            {
                db.Execute("SET", key1, value1, "WITHETAG");
                db.Execute("SET", key2, value2, "WITHETAG");

                RedisResult result = db.Execute("RANDOPS", key1, key2);

                ClassicAssert.AreEqual("OK", result.ToString());

                // check GETWITHETAG shows updated etag and expected values for both
                RedisResult[] res = (RedisResult[])db.Execute("EXECWITHETAG", "GET", key1);
                ClassicAssert.AreEqual("2", res[0].ToString());
                ClassicAssert.IsTrue(res[1].ToString().All(c => c - 'a' >= 0 && c - 'a' < 26));

                res = (RedisResult[])db.Execute("EXECWITHETAG", "GET", key2);
                ClassicAssert.AreEqual("2", res[0].ToString());
                ClassicAssert.AreEqual("18", res[1].ToString());
            }
            catch (RedisServerException rse)
            {
                ClassicAssert.Fail(rse.Message);
            }
        }

        [Test]
        public void CustomProcEtagInteractionTest()
        {
            server.Register.NewProcedure("INCRGET", () => new CustomIncrProc());

            var key1 = "key1";
            var value1 = "thisisstarting";

            var key2 = "key2";
            var value2 = "256";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            try
            {
                db.Execute("SET", key1, value1, "WITHETAG");
                db.Execute("SET", key2, value2, "WITHETAG");

                // incr key2, and just get key1
                RedisResult result = db.Execute("INCRGET", key2, key1);

                ClassicAssert.AreEqual(value1, result.ToString());

                // check GETWITHETAG shows updated etag and expected values for both
                RedisResult[] res = (RedisResult[])db.Execute("EXECWITHETAG", "GET", key1);
                // etag not updated for this
                ClassicAssert.AreEqual("1", res[0].ToString());
                ClassicAssert.AreEqual(value1, res[1].ToString());

                res = (RedisResult[])db.Execute("EXECWITHETAG", "GET", key2);
                // etag updated for this
                ClassicAssert.AreEqual("2", res[0].ToString());
                ClassicAssert.AreEqual("257", res[1].ToString());
            }
            catch (RedisServerException rse)
            {
                ClassicAssert.Fail(rse.Message);
            }
        }
    }
}