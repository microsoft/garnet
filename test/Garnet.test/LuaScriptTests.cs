// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    // Limits chosen here to allow completion - if you have to bump them up, consider that you might have introduced a regression
    [TestFixture(LuaMemoryManagementMode.Native, "", "")]
    [TestFixture(LuaMemoryManagementMode.Native, "", "00:00:02")]
    [TestFixture(LuaMemoryManagementMode.Tracked, "", "")]
    [TestFixture(LuaMemoryManagementMode.Tracked, "13m", "")]
    [TestFixture(LuaMemoryManagementMode.Managed, "", "")]
    [TestFixture(LuaMemoryManagementMode.Managed, "17m", "")]
    public class LuaScriptTests
    {
        /// <summary>
        /// Writes it's parameter directly into the response stream, followed by a \r\n.
        /// 
        /// Used for testing RESP3 mapping.
        /// </summary>
        private sealed class RawEcho : CustomProcedure
        {
            /// <inheritdoc/>
            public override bool Execute<TGarnetApi>(TGarnetApi garnetApi, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
            {
                ref var arg = ref procInput.parseState.GetArgSliceByRef(0);

                var mem = MemoryPool.Rent(arg.Length + 2);
                arg.ReadOnlySpan.CopyTo(mem.Memory.Span);
                mem.Memory.Span[arg.Length] = (byte)'\r';
                mem.Memory.Span[arg.Length + 1] = (byte)'\n';

                output = new(mem, arg.Length + 2);
                return true;
            }
        }

        /// <summary>
        /// For logging, but limited to a maximum length of 4K chars.
        /// </summary>
        private sealed class LimitStringWriter : TextWriter
        {
            private const int LimitChars = 4 * 1024;

            private readonly StringBuilder sb = new();

            /// <inheritdoc/>
            public override Encoding Encoding
            => new UnicodeEncoding(false, false);

            /// <inheritdoc/>
            public override void Write(string value)
            {
                lock (sb)
                {
                    var remainingSpace = LimitChars - (sb.Length + value.Length);
                    if (remainingSpace < 0)
                    {
                        _ = sb.Remove(0, -remainingSpace);
                    }

                    _ = sb.Append(value);
                }
            }

            /// <inheritdoc/>
            public override string ToString()
            {
                lock (sb)
                {
                    return sb.ToString();
                }
            }
        }

        private readonly LuaMemoryManagementMode allocMode;
        private readonly string limitBytes;
        private readonly string limitTimeout;

        private LimitStringWriter loggerOutput;
        private string aclFile;
        private GarnetServer server;

        public LuaScriptTests(LuaMemoryManagementMode allocMode, string limitBytes, string limitTimeout)
        {
            this.allocMode = allocMode;
            this.limitBytes = limitBytes;
            this.limitTimeout = limitTimeout;
        }

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            TimeSpan? timeout = string.IsNullOrEmpty(limitTimeout) ? null : TimeSpan.ParseExact(limitTimeout, "c", CultureInfo.InvariantCulture);

            aclFile = Path.GetTempFileName();
            File.WriteAllLines(
                aclFile,
                [
                    "user default on nopass +@all",
                    "user deny on nopass +@all -get -acl"
                ]
            );

            loggerOutput = new();
            server =
                TestUtils.CreateGarnetServer(
                    TestUtils.MethodTestDir,
                    enableLua: true,
                    luaMemoryMode: allocMode,
                    luaMemoryLimit: limitBytes,
                    luaTimeout: timeout,
                    useAcl: true,
                    aclFile: aclFile,
                    logTo: loggerOutput
                );

            _ = server.Register.NewProcedure(
                "RECHO",
                static () => new RawEcho(),
                commandInfo: new() { Arity = 2, FirstKey = 0, LastKey = 0 }
            );

            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            try
            {
                if (aclFile != null)
                {
                    File.Delete(aclFile);
                }
            }
            catch
            {
                // Best effort
            }
        }

        [Test]
        public void KeysArgvResize()
        {
            const string KeysScript = "return KEYS[1] .. KEYS[2]";
            const string ArgvScript = "return ARGV[1] .. ARGV[2]";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var twoKeys = (string)db.ScriptEvaluate(KeysScript, ["hello", "world"]);
            ClassicAssert.AreEqual("helloworld", twoKeys);

            var threeKeys = (string)db.ScriptEvaluate(KeysScript, ["fizz", "buzz", "foo"]);
            ClassicAssert.AreEqual("fizzbuzz", threeKeys);

            var twoArgv = (string)db.ScriptEvaluate(ArgvScript, [], ["a", "b"]);
            ClassicAssert.AreEqual("ab", twoArgv);

            var threeArgv = (string)db.ScriptEvaluate(ArgvScript, [], ["c", "d", "e"]);
            ClassicAssert.AreEqual("cd", threeArgv);
        }

        [Test]
        public void GlobalsForbidden()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var globalFuncExc =
                ClassicAssert.Throws<RedisServerException>(
                    () =>
                    {
                        _ = db.ScriptEvaluate(
                            @"function globalFunc()
                                return 1
                              end"
                        );
                    }
                );
            ClassicAssert.IsTrue(globalFuncExc.Message.Contains("Attempt to modify a readonly table"));

            var globalVar = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("global_var = 'hello'"));
            ClassicAssert.IsTrue(globalVar.Message.Contains("Attempt to modify a readonly table"));

            var metatableUpdateOnGlobals = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("setmetatable(_G, nil)"));
            ClassicAssert.IsTrue(metatableUpdateOnGlobals.Message.Contains("Attempt to modify a readonly table"));

            var rawSetG = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("rawset(_G, 'hello', 'world')"));
            ClassicAssert.IsTrue(globalVar.Message.Contains("Attempt to modify a readonly table"));
        }

        [Test]
        public void ReadOnlyGlobalTables()
        {
            // This is a bit tricky, but basically Redis forbids modifying any global tables (things like cmsgpack.*) EXCEPT
            // for KEYS and ARGV

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            List<string> illegalToModify = ["_G", "bit", "cjson", "cmsgpack", "math", "os", "redis", "string", "struct", "table"];
            List<string> legalToModify = ["KEYS", "ARGV"];

            foreach (var illegal in illegalToModify)
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate($"table.insert({illegal}, 'foo')"));
                ClassicAssert.IsTrue(exc.Message.Contains("Attempt to modify a readonly table"));
            }

            foreach (var legal in legalToModify)
            {
                var res = (string[])db.ScriptEvaluate($"table.insert({legal}, 'fizz'); return {legal};");
                ClassicAssert.IsTrue(res.Contains("fizz"));
            }
        }

        [Test]
        public void CanDoEvalUsingGarnetCallSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var keys = new RedisKey[] { new("KeyOne"), new("KeyTwo") };
            var values = new RedisValue[] { new("KeyOneValue"), new("KeyTwoValue") };
            var response = db.ScriptEvaluate("redis.call('set',KEYS[1], ARGV[1]); redis.call('set',KEYS[2],ARGV[2]); return redis.call('get',KEYS[1]);", keys, values);
            string retValue = db.StringGet("KeyOne");
            ClassicAssert.AreEqual("KeyOneValue", retValue);
            ClassicAssert.AreEqual("KeyOneValue", response.ToString());
        }

        [Test]
        public void CanDoSimpleEvalHelloSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var values = new RedisValue[] { new("hello") };
            var response = db.ScriptEvaluate("return ARGV[1]", null, values);
            ClassicAssert.AreEqual((string)response, "hello");
        }

        [Test]
        public void CanDoScriptWithArgvOperationsSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var keys = new RedisKey[] { new("KeyOne") };
            var values = new RedisValue[] { new("KeyOneValue-") };
            // SE lib sends a SCRIPT LOAD command followed by a EVAL script KEYS ARGS command
            var response = db.ScriptEvaluate("local v = ARGV[1]..\"abc\"; redis.call('set',KEYS[1], v); return redis.call('get',KEYS[1]);", keys, values);
            ClassicAssert.IsTrue(((RedisValue)response).ToString().Contains("KeyOneValue-abc", StringComparison.InvariantCultureIgnoreCase));
        }

        [Test]
        public void CanDoEvalSetGet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var response = db.Execute("EVAL", "return redis.call('SeT', KEYS[1], ARGV[1])", 1, "mykey", "myvalue");
            ClassicAssert.AreEqual("OK", (string)response);
            response = db.Execute("EVAL", "return redis.call('get', KEYS[1])", 1, "mykey");
            ClassicAssert.AreEqual("myvalue", (string)response);

            // Get numbers
            response = db.Execute("EVAL", "return redis.call('SeT', KEYS[1], ARGV[1])", 1, "mykey", 100);
            ClassicAssert.AreEqual("OK", (string)response);
            response = db.Execute("EVAL", "return redis.call('get', KEYS[1])", 1, "mykey");
            ClassicAssert.AreEqual(100, Convert.ToInt64(response));
        }

        [Test]
        public void CanDoEvalShaWithIntegerValueKeySE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var script = "return redis.call('set', KEYS[1], ARGV[1]);";
            var result = db.ScriptEvaluate(script, [(RedisKey)"mykey"], [(RedisValue)1]);
            script = "local i = redis.call('get',KEYS[1]); i = i + 1; redis.call('set', KEYS[1], i); return redis.call('get', KEYS[1]);";
            result = db.ScriptEvaluate(script, [(RedisKey)"mykey"]);
            ClassicAssert.AreEqual(2, Convert.ToInt64(result));
        }

        [Test]
        public void CanDoEvalShaWithStringValueKeySE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var script = "return redis.call('set', KEYS[1], ARGV[1]);";
            var result = db.ScriptEvaluate(script, [(RedisKey)"mykey"], [(RedisValue)"NAME"]);
            script = "local i = redis.call('get', KEYS[1]); i = \"FULL \"..i; redis.call('set', KEYS[1], i); return redis.call('get', KEYS[1]);";
            result = db.ScriptEvaluate(script, [(RedisKey)"mykey"]);
            ClassicAssert.IsTrue(((RedisValue)result).ToString() == "FULL NAME");
        }


        [Test]
        public void CanDoEvalShaWithStringValueMultipleKeysSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var script = "local i = 0; " +
                            "if redis.call('set', KEYS[1], ARGV[1]) == \"OK\" then i = i + 1; end;" +
                            "if (redis.call('set', KEYS[2], ARGV[2]) == \"OK\") then i = i + 1; end;" +
                            "return i;";
            var result = db.ScriptEvaluate(script, [(RedisKey)"mykey", (RedisKey)"myseckey"], [(RedisValue)"NAME", (RedisValue)"mysecvalue"]);
            ClassicAssert.IsTrue(((RedisValue)result) == 2);
        }


        [Test]
        public void CanDoEvalShaWithZAddOnePairSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var script = "return redis.call('zadd', KEYS[1], ARGV[1], ARGV[2])";
            var result = db.ScriptEvaluate(script, [(RedisKey)"mysskey"], [(RedisValue)1, (RedisValue)"a"]);
            ClassicAssert.IsTrue(result.ToString() == "1");
            var l = db.SortedSetLength("mysskey");
            ClassicAssert.IsTrue(l == 1);
        }


        [Test]
        public void CanDoEvalShaWithZAddMultiPairSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Create a sorted set
            var script = "return redis.call('zadd', KEYS[1], 100, \"value1\", 200, \"value2\")";
            var result = db.ScriptEvaluate(script, [(RedisKey)"mysskey"]);
            ClassicAssert.IsTrue(result.ToString() == "2");

            // Check the length
            var l = db.SortedSetLength("mysskey");
            ClassicAssert.IsTrue(l == 2);

            // Execute same script again
            result = db.ScriptEvaluate(script, [(RedisKey)"mysskey"]);
            ClassicAssert.IsTrue(result.ToString() == "0");

            // Add more pairs
            script = "return redis.call('zadd', KEYS[1], 300, \"value3\", 400, \"value4\")";
            result = db.ScriptEvaluate(script, [(RedisKey)"mysskey"]);
            ClassicAssert.IsTrue(result.ToString() == "2");

            // Review
            var r = db.SortedSetRangeByScoreWithScores("mysskey");
            ClassicAssert.IsTrue(r[0].Score == 100 && r[0].Element == "value1");
            l = db.SortedSetLength("mysskey");

            ClassicAssert.IsTrue(l == 4);
        }

        [Test]
        public void CanDoEvalShaSEMultipleThreads()
        {
            var numThreads = 1;
            var tasks = new Task[numThreads];
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var initialValue = 0;
            var rnd = new Random();
            // create the key
            var script = "redis.call('set',KEYS[1], ARGV[1]); return redis.call('get', KEYS[1]);";
            var result = db.ScriptEvaluate(script, [(RedisKey)"mykey"], [(RedisValue)initialValue]);
            ClassicAssert.IsTrue(((RedisValue)result).ToString() == "0");
            script = "local i = redis.call('get', KEYS[1]); i = i + 1; return redis.call('set', KEYS[1], i)";
            var numIterations = 10;
            //updates
            for (var i = 0; i < numThreads; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    for (var ii = 0; ii < numIterations; ii++)
                    {
                        _ = db.ScriptEvaluate(script, [(RedisKey)"mykey"]);
                        await Task.Delay(millisecondsDelay: rnd.Next(10, 50));
                    }
                });
            }
            Task.WaitAll(tasks);
            script = "return redis.call('get', KEYS[1]);";
            result = db.ScriptEvaluate(script, [(RedisKey)"mykey"]);
            ClassicAssert.IsTrue(int.Parse(((RedisValue)result).ToString()) == numThreads * numIterations);
            script = "local i = redis.call('get', KEYS[1]); i = i - 1; return redis.call('set', KEYS[1], i)";
            for (var i = 0; i < numThreads; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    for (var ii = 0; ii < numIterations; ii++)
                    {
                        _ = db.ScriptEvaluate(script, [(RedisKey)"mykey"]);
                        await Task.Delay(millisecondsDelay: rnd.Next(10, 50));
                    }
                });
            }
            Task.WaitAll(tasks);
            script = "return redis.call('get', KEYS[1]);";
            result = db.ScriptEvaluate(script, [(RedisKey)"mykey"]);
            ClassicAssert.IsTrue(int.Parse(((RedisValue)result).ToString()) == 0);
        }

        [Test]
        public void CanDoZAddGT()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const string ZADD_GT = """
                    local oldScore = tonumber(redis.call('ZSCORE', KEYS[1], ARGV[2]))
                    if oldScore == nil or oldScore < tonumber(ARGV[1]) then
                        redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
                    end
                    """;

            var tag = "tag";
            long expiryTimestamp = 100;
            var result = db.ScriptEvaluate(ZADD_GT, [(RedisKey)"zaddgtkey"], [expiryTimestamp, tag]);
            ClassicAssert.IsTrue(result.IsNull);

            // get key and verify timestamp is 100
            var r = db.SortedSetRangeByScoreWithScores("zaddgtkey");
            ClassicAssert.IsTrue(r[0].Score == 100 && r[0].Element == "tag");

            expiryTimestamp = 200;
            result = db.ScriptEvaluate(ZADD_GT, [(RedisKey)"zaddgtkey"], [expiryTimestamp, tag]);
            ClassicAssert.IsTrue(result.IsNull);

            // get key and verify timestamp is 200
            r = db.SortedSetRangeByScoreWithScores("zaddgtkey");
            ClassicAssert.IsTrue(r[0].Score == 200 && r[0].Element == "tag");

            expiryTimestamp = 150;
            result = db.ScriptEvaluate(ZADD_GT, [(RedisKey)"zaddgtkey"], [expiryTimestamp, tag]);
            ClassicAssert.IsTrue(result.IsNull);

            // get key and verify timestamp is 200
            r = db.SortedSetRangeByScoreWithScores("zaddgtkey");
            ClassicAssert.IsTrue(r[0].Score == 200 && r[0].Element == "tag");
        }

        [Test]
        public void CanDoScriptFlush()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var server = redis.GetServer(TestUtils.EndPoint);

            // Load a script in the memory
            var script = "return 1;";
            var scriptId = server.ScriptLoad(script);
            var result = db.ScriptEvaluate(scriptId);
            ClassicAssert.IsTrue(((RedisValue)result) == "1");

            // Flush script memory
            server.ScriptFlush();

            // Assert the script is not found
            _ = Assert.Throws<RedisServerException>(() => db.ScriptEvaluate(scriptId));
        }

        [Test]
        public void CannotScriptUseExtendedEncoding()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var extendedCharsScript = "return '僕'";

            var server = redis.GetServer(TestUtils.EndPoint);
            var scriptSha = server.ScriptLoad(extendedCharsScript);
            var result = redis.GetDatabase(0).ScriptEvaluate(scriptSha);
            ClassicAssert.AreNotEqual("僕", result);
        }

        [Test]
        public void UseEvalShaLightClient()
        {
            var rnd = new Random(0);
            var nameKey = "strKey-";
            var valueKey = "valueKey-";
            using var lightClientRequest = TestUtils.CreateRequest();
            var stringCmd = "*3\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n$40\r\nreturn redis.call('set',KEYS[1],ARGV[1])\r\n";
            var response = lightClientRequest.SendCommand(Encoding.ASCII.GetBytes(stringCmd), 1);
            var sha1SetScript = Encoding.ASCII.GetString(response, 5, 40);

            ClassicAssert.AreEqual("c686f316aaf1eb01d5a4de1b0b63cd233010e63d", sha1SetScript);
            for (var i = 0; i < 5000; i++)
            {
                var randPostFix = rnd.Next(1, 1000);
                valueKey = $"{valueKey}{randPostFix}";

                response = lightClientRequest.SendCommand($"EVALSHA {sha1SetScript} 1 {nameKey}{randPostFix} {valueKey}", 1);
                // Check for error reply
                ClassicAssert.IsTrue(response[0] != '-');

                response = lightClientRequest.SendCommand($"get {nameKey}{randPostFix}", 1);

                var fstEndOfLine = 0;
                foreach (var chr in response)
                {
                    ++fstEndOfLine;
                    if (chr == '\n')
                        break;
                }
                var strKeyValue = Encoding.ASCII.GetString(response, fstEndOfLine, valueKey.Length);
                ClassicAssert.AreEqual(valueKey, strKeyValue);
            }
        }

        [Test]
        public void SuccessfulStatusReturn()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var statusReplyScript = "return redis.status_reply('Success')";
            var result = db.ScriptEvaluate(statusReplyScript);
            ClassicAssert.AreEqual((RedisValue)result, "Success");
            var directReplyScript = "return { ok = 'Success' }";
            result = db.ScriptEvaluate(directReplyScript);
            ClassicAssert.AreEqual((RedisValue)result, "Success");
        }

        [Test]
        public void FailureStatusReturn()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var statusReplyScript = "return redis.error_reply('GET')";

            var excReply = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate(statusReplyScript));
            ClassicAssert.AreEqual("ERR GET", excReply.Message);

            var directReplyScript = "return { err = 'Failure' }";
            var excDirect = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate(directReplyScript));
            ClassicAssert.AreEqual("Failure", excDirect.Message);
        }

        [Test]
        public void MiscMath()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // ATan2
            {
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.atan2()"));
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.atan2(1)"));
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.atan2(1, 2, 3)"));
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.atan2('a', 1)"));
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.atan2(1, 'b')"));

                var val = (string)db.ScriptEvaluate("return tostring(math.atan2(0.1, 0.2))");
                ClassicAssert.AreEqual(Math.Round(Math.Atan2(0.1, 0.2), 14).ToString(), val);
            }

            // Cosh
            {
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.cosh()"));
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.cosh('a')"));
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.cosh(1, 2)"));

                var val = (string)db.ScriptEvaluate("return tostring(math.cosh(0.1))");
                ClassicAssert.AreEqual(Math.Round(Math.Cosh(0.1), 14).ToString(), val);
            }

            // Log10
            {
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.log10()"));
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.log10('a')"));
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.log10(1, 2)"));

                var val = (string)db.ScriptEvaluate("return tostring(math.log10(0.1))");
                ClassicAssert.AreEqual("-1.0", val);
            }

            // Pow
            {
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.pow()"));
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.pow(1)"));
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.pow(1, 2, 3)"));
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.pow('a', 1)"));
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.pow(1, 'b')"));

                var val = (string)db.ScriptEvaluate("return tostring(math.pow(0.1, 0.2))");
                ClassicAssert.AreEqual(Math.Round(Math.Pow(0.1, 0.2), 14).ToString(), val);
            }

            // Sinh
            {
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.sinh()"));
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.sinh('a')"));
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.sinh(1, 2)"));

                var val = (string)db.ScriptEvaluate("return tostring(math.sinh(0.1))");
                ClassicAssert.AreEqual(Math.Round(Math.Sinh(0.1), 14).ToString(), val);
            }

            // Tanh
            {
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.tanh()"));
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.tanh('a')"));
                _ = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("math.tanh(1, 2)"));

                var val = (string)db.ScriptEvaluate("return tostring(math.tanh(0.2))");
                ClassicAssert.AreEqual(Math.Round(Math.Tanh(0.2), 14).ToString(), val);
            }
        }

        [Test]
        public void RedisPCall()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var resErr = (string)db.ScriptEvaluate("local x = redis.pcall('Fizz'); return x.err");
            ClassicAssert.AreEqual("ERR Unknown Redis command called from script", resErr);

            _ = db.StringSet("foo", "bar");

            var resSuccess = (string)db.ScriptEvaluate("return redis.pcall('GET', 'foo')");
            ClassicAssert.AreEqual("bar", resSuccess);
        }

        [Test]
        public void RedisSha1Hex()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var resEmpty = (string)db.ScriptEvaluate("return redis.sha1hex('')");
            ClassicAssert.AreEqual("da39a3ee5e6b4b0d3255bfef95601890afd80709", resEmpty);

            var resStr = (string)db.ScriptEvaluate("return redis.sha1hex('123')");
            ClassicAssert.AreEqual("40bd001563085fc35165329ea1ff5c5ecbdbbeef", resStr);

            // Redis stringifies numbers before hashing, so same bytes are expected
            var resInt = (string)db.ScriptEvaluate("return redis.sha1hex(123)");
            ClassicAssert.AreEqual("40bd001563085fc35165329ea1ff5c5ecbdbbeef", resInt);

            // Redis still succeeds here, but effectively treats non-string, non-number values as empty strings
            var resTable = (string)db.ScriptEvaluate("return redis.sha1hex({ 1234 })");
            ClassicAssert.AreEqual("da39a3ee5e6b4b0d3255bfef95601890afd80709", resTable);

            var excEmpty = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return redis.sha1hex()"));
            ClassicAssert.IsTrue(excEmpty.Message.StartsWith("ERR wrong number of arguments"));

            var excTwo = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return redis.sha1hex('a', 'b')"));
            ClassicAssert.IsTrue(excTwo.Message.StartsWith("ERR wrong number of arguments"));
        }

        [Test]
        public void RedisLog()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var excZero = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return redis.log()"));
            ClassicAssert.IsTrue(excZero.Message.StartsWith("ERR redis.log() requires two arguments or more."));

            var excOne = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return redis.log(redis.LOG_DEBUG)"));
            ClassicAssert.IsTrue(excOne.Message.StartsWith("ERR redis.log() requires two arguments or more."));

            var excBadLevelType = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return redis.log('hello', 'world')"));
            ClassicAssert.IsTrue(excBadLevelType.Message.StartsWith("ERR First argument must be a number (log level)."));

            var excBadLevelValue = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return redis.log(-1, 'world')"));
            ClassicAssert.IsTrue(excBadLevelValue.Message.StartsWith("ERR Invalid debug level."));

            // Test logs at each level
            var debugStr = $"Should be {Guid.NewGuid()} debug";
            var verboseStr = $"Should be {Guid.NewGuid()} verbose";
            var noticeStr = $"Should be {Guid.NewGuid()} notice";
            var warningStr = $"Should be {Guid.NewGuid()} warning";
            _ = db.ScriptEvaluate($"redis.log(redis.LOG_DEBUG, '{debugStr}')");
            _ = db.ScriptEvaluate($"redis.log(redis.LOG_VERBOSE, '{verboseStr}')");
            _ = db.ScriptEvaluate($"redis.log(redis.LOG_NOTICE, '{noticeStr}')");
            _ = db.ScriptEvaluate($"redis.log(redis.LOG_WARNING, '{warningStr}')");

            var logLines = loggerOutput.ToString();

            ClassicAssert.IsTrue(Regex.IsMatch(logLines, $@"\(dbug\)] \|.*\| <.*> .* \^.*{Regex.Escape(debugStr)}\^"));
            ClassicAssert.IsTrue(Regex.IsMatch(logLines, $@"\(info\)] \|.*\| <.*> .* \^.*{Regex.Escape(verboseStr)}\^"));
            ClassicAssert.IsTrue(Regex.IsMatch(logLines, $@"\(warn\)] \|.*\| <.*> .* \^.*{Regex.Escape(noticeStr)}\^"));
            ClassicAssert.IsTrue(Regex.IsMatch(logLines, $@"\(errr\)] \|.*\| <.*> .* \^.*{Regex.Escape(warningStr)}\^"));

            // Single number log arg is legal
            _ = db.ScriptEvaluate("return redis.log(redis.LOG_DEBUG, 123)");

            // More than 1 log arg, and non-string values are legal
            _ = db.ScriptEvaluate("return redis.log(redis.LOG_DEBUG, 123, 456, 789)");

            var constantsDefined = (int[])db.ScriptEvaluate("return {redis.LOG_DEBUG, redis.LOG_VERBOSE, redis.LOG_NOTICE, redis.LOG_WARNING}");
            ClassicAssert.IsTrue(constantsDefined.AsSpan().SequenceEqual([0, 1, 2, 3]));
        }

        [Test]
        public void RedisSetRepl()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var excNotSupported = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return redis.set_repl(redis.REPL_ALL)"));
            ClassicAssert.IsTrue(excNotSupported.Message.StartsWith("ERR redis.set_repl is not supported in Garnet"));

            var constantsDefined = (int[])db.ScriptEvaluate("return {redis.REPL_ALL, redis.REPL_AOF, redis.REPL_REPLICA, redis.REPL_SLAVE, redis.REPL_NONE}");
            ClassicAssert.IsTrue(constantsDefined.AsSpan().SequenceEqual([3, 1, 2, 2, 0]));
        }

        [Test]
        public void RedisReplicateCommands()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // This is deprecated in Redis, and always returns true if called
            var res = (bool)db.ScriptEvaluate("return redis.replicate_commands()");
            ClassicAssert.IsTrue(res);
        }

        [Test]
        public void RedisDebugAndBreakpoint()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var excBreakpoint = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("redis.breakpoint()"));
            ClassicAssert.IsTrue(excBreakpoint.Message.StartsWith("ERR redis.breakpoint is not supported in Garnet"));

            var excDebug = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("redis.debug('hello')"));
            ClassicAssert.IsTrue(excDebug.Message.StartsWith("ERR redis.debug is not supported in Garnet"));
        }

        [Test]
        public void RedisAclCheckCmd()
        {
            // Note this path is more heavily exercised in ACL tests

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            using var denyRedis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(authUsername: "deny"));
            var denyDB = denyRedis.GetDatabase(0);

            var noArgs = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return redis.acl_check_cmd()"));
            ClassicAssert.IsTrue(noArgs.Message.StartsWith("ERR Please specify at least one argument for this redis lib call"));

            var invalidCmdArgType = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return redis.acl_check_cmd({123})"));
            ClassicAssert.IsTrue(invalidCmdArgType.Message.StartsWith("ERR Lua redis lib command arguments must be strings or integers"));

            var invalidCmd = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return redis.acl_check_cmd('nope')"));
            ClassicAssert.IsTrue(invalidCmd.Message.StartsWith("ERR Invalid command passed to redis.acl_check_cmd()"));

            var invalidArgType = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return redis.acl_check_cmd('GET', {123})"));
            ClassicAssert.IsTrue(invalidArgType.Message.StartsWith("ERR Lua redis lib command arguments must be strings or integers"));

            var canRun = (bool)db.ScriptEvaluate("return redis.acl_check_cmd('GET')");
            ClassicAssert.IsTrue(canRun);

            var canRunWithArg = (bool)db.ScriptEvaluate("return redis.acl_check_cmd('GET', 'foo')");
            ClassicAssert.IsTrue(canRunWithArg);

            var canRunWithTooManyArgs = (bool)db.ScriptEvaluate("return redis.acl_check_cmd('GET', 'foo', 'bar', 'fizz', 'buzz')");
            ClassicAssert.IsTrue(canRunWithTooManyArgs);

            var cantRunNoArg = (bool)denyDB.ScriptEvaluate("return redis.acl_check_cmd('GET')");
            ClassicAssert.IsFalse(cantRunNoArg);

            var cantRunWithArg = (bool)denyDB.ScriptEvaluate("return redis.acl_check_cmd('GET', 'foo')");
            ClassicAssert.IsFalse(cantRunWithArg);

            var cantRunWithTooManyArgs = (bool)denyDB.ScriptEvaluate("return redis.acl_check_cmd('GET', 'foo', 'bar')");
            ClassicAssert.IsFalse(cantRunWithTooManyArgs);

            var canRunParentCommand = (bool)db.ScriptEvaluate("return redis.acl_check_cmd('ACL')");
            ClassicAssert.True(canRunParentCommand);

            var canRunSubCommand = (bool)db.ScriptEvaluate("return redis.acl_check_cmd('ACL', 'WHOAMI')");
            ClassicAssert.True(canRunSubCommand);

            var cantRunParentCommand = (bool)denyDB.ScriptEvaluate("return redis.acl_check_cmd('ACL')");
            ClassicAssert.False(cantRunParentCommand);

            var cantRunSubCommand = (bool)denyDB.ScriptEvaluate("return redis.acl_check_cmd('ACL', 'WHOAMI')");
            ClassicAssert.False(cantRunSubCommand);
        }

        [Test]
        public void RedisSetResp()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var noArgs = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("redis.setresp()"));
            ClassicAssert.IsTrue(noArgs.Message.StartsWith("ERR redis.setresp() requires one argument."));

            var tooManyArgs = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("redis.setresp(1, 2)"));
            ClassicAssert.IsTrue(tooManyArgs.Message.StartsWith("ERR redis.setresp() requires one argument."));

            var badArg = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("redis.setresp({123})"));
            ClassicAssert.IsTrue(badArg.Message.StartsWith("ERR RESP version must be 2 or 3."));

            var resp2 = db.ScriptEvaluate("redis.setresp(2)");
            ClassicAssert.IsTrue(resp2.IsNull);

            var resp3 = db.ScriptEvaluate("redis.setresp(3)");
            ClassicAssert.IsTrue(resp3.IsNull);

            var badRespVersion = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("redis.setresp(1)"));
            ClassicAssert.IsTrue(badRespVersion.Message.StartsWith("ERR RESP version must be 2 or 3."));
        }

        [Test]
        public void RedisVersion()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var expectedVersion = Version.Parse(GarnetServer.RedisProtocolVersion);

            var asStr = (string)db.ScriptEvaluate("return redis.REDIS_VERSION");
            var asNum = (int)db.ScriptEvaluate("return redis.REDIS_VERSION_NUM");

            ClassicAssert.AreEqual(GarnetServer.RedisProtocolVersion, asStr);

            var expectedNum =
                ((byte)expectedVersion.Major << 16) |
                ((byte)expectedVersion.Minor << 8) |
                ((byte)expectedVersion.Build << 0);

            ClassicAssert.AreEqual(expectedNum, asNum);
        }

        [Test]
        public void ComplexLuaTest1()
        {
            var script = """
local setArgs = {}
for _, key in ipairs(KEYS) do
    table.insert(setArgs, key)
end
unpack(KEYS)
return redis.status_reply(table.concat(setArgs))
""";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var response = db.ScriptEvaluate(script, ["key1", "key2"], ["value", 1, 60_000]);
            ClassicAssert.AreEqual("key1key2", (string)response);
            response = db.ScriptEvaluate(script, ["key1", "key2"], ["value"]);
            ClassicAssert.AreEqual("key1key2", (string)response);
            response = db.ScriptEvaluate(script, ["key1"], ["value", 1, 60_000]);
            ClassicAssert.AreEqual("key1", (string)response);
            response = db.ScriptEvaluate(script, ["key1", "key2"]);
            ClassicAssert.AreEqual("key1key2", (string)response);
            response = db.ScriptEvaluate(script, ["key1", "key2", "key3", "key4"], ["value", 1]);
            ClassicAssert.AreEqual("key1key2key3key4", (string)response);
            response = db.ScriptEvaluate(script, [], ["value", 1, 60_000]);
            ClassicAssert.AreEqual("", (string)response);
        }

        [Test]
        public void ComplexLuaTest2()
        {
            var script1 = """
local function callpexpire(ttl)
	for _, key in ipairs(KEYS) do
		redis.call("pexpire", key, ttl)
	end
end

local function callgetrange() 
	local offset = tonumber(ARGV[2])

	for _, key in ipairs(KEYS) do
		if redis.call("getrange", key, 0, offset-1) ~= string.sub(ARGV[1], 1, offset) then
			return false
		end
	end
	return true
end

local setArgs = {}
for _, key in ipairs(KEYS) do
	table.insert(setArgs, key)
	table.insert(setArgs, ARGV[1])
end

if redis.call("msetnx", unpack(setArgs)) ~= 1 then
	if callgetrange() == false then
		return false
	end
	redis.call("mset", unpack(setArgs))
end

callpexpire(ARGV[3])
return redis.status_reply("OK")
""";

            var script2 = """
local values = redis.call("mget", unpack(KEYS))
for i, _ in ipairs(KEYS) do
	if values[i] ~= ARGV[1] then
		return false
	end
end

redis.call("del", unpack(KEYS))

return redis.status_reply("OK")
""";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var response1 = db.ScriptEvaluate(script1, ["key1", "key2"], ["foo", 3, 60_000]);
            ClassicAssert.AreEqual("OK", (string)response1);

            var response2 = db.ScriptEvaluate(script2, ["key3"], ["foo"]);
            ClassicAssert.AreEqual(false, (bool)response2);

            var response3 = db.ScriptEvaluate(script2, ["key1", "key2"], ["foo"]);
            ClassicAssert.AreEqual("OK", (string)response3);
        }

        [Test]
        public void ComplexLuaTest3()
        {
            var script1 = """
            return redis.call("mget", unpack(KEYS))
            """;

            //var script1 = "return KEYS";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            for (var i = 0; i < 10; i++)
            {
                var response1 = (string[])db.ScriptEvaluate(script1, ["key1", "key2"]);
                ClassicAssert.AreEqual(2, response1.Length);
                foreach (var item in response1)
                {
                    ClassicAssert.Null(item);
                }
            }
        }

        [Test]
        public void ScriptExistsErrors()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("SCRIPT", "EXISTS"));
            ClassicAssert.AreEqual("ERR wrong number of arguments for 'script|exists' command", exc.Message);
        }

        [Test]
        public void ScriptFlushErrors()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // > 1 args
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("SCRIPT", "FLUSH", "ASYNC", "BAR"));
                ClassicAssert.AreEqual("ERR SCRIPT FLUSH only support SYNC|ASYNC option", exc.Message);
            }

            // 1 arg, but not ASYNC or SYNC
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("SCRIPT", "FLUSH", "NOW"));
                ClassicAssert.AreEqual("ERR SCRIPT FLUSH only support SYNC|ASYNC option", exc.Message);
            }
        }

        [Test]
        public void MultiSessionScriptFlush()
        {
            // If we flush scripts from one session, they should (eventually) be removed for all sessions

            using var redis1 = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redis2 = ConnectionMultiplexer.Connect(TestUtils.GetConfig());

            var db1 = redis1.GetDatabase(0);
            var db2 = redis2.GetDatabase(0);

            _ = db1.Execute("SCRIPT", "FLUSH", "SYNC");

            var hash = (string)db1.Execute("SCRIPT", "LOAD", "return 2;");

            var check1 = (int)db1.Execute("EVALSHA", hash, "0");
            var check2 = (int)db2.Execute("EVALSHA", hash, "0");

            ClassicAssert.AreEqual(2, check1);
            ClassicAssert.AreEqual(2, check2);

            _ = db1.Execute("SCRIPT", "FLUSH", "SYNC");

            var exc1 = ClassicAssert.Throws<RedisServerException>(() => db1.Execute("EVALSHA", hash, "0"));
            var exc2 = ClassicAssert.Throws<RedisServerException>(() => db2.Execute("EVALSHA", hash, "0"));

            ClassicAssert.True(exc1.Message.StartsWith("NOSCRIPT "));
            ClassicAssert.True(exc2.Message.StartsWith("NOSCRIPT "));
        }

        [Test]
        public void CrossSessionEvalScriptCaching()
        {
            // Somewhat oddly, if we EVAL a script in one session it should be runnable by hash in all sessions
            //
            // This is to match Redis behavior

            using var redis1 = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redis2 = ConnectionMultiplexer.Connect(TestUtils.GetConfig());

            var db1 = redis1.GetDatabase(0);
            var db2 = redis2.GetDatabase(0);

            _ = db1.Execute("SCRIPT", "FLUSH", "SYNC");

            var script = "return 2;";
            var hashBytes = SHA1.HashData(Encoding.ASCII.GetBytes(script));
            var hash = string.Join("", hashBytes.Select(static x => $"{x:x2}"));

            var res1 = (int)db1.Execute("EVAL", script, "0");
            ClassicAssert.AreEqual(2, res1);

            // Should be cached in _different_ session too
            var res2 = (int)db2.Execute("EVALSHA", hash, "0");
            ClassicAssert.AreEqual(2, res2);

            var res3 = (int)db1.Execute("EVALSHA", hash, "0");
            ClassicAssert.AreEqual(2, res3);
        }

        [Test]
        public void ScriptLoadErrors()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // 0 args
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("SCRIPT", "LOAD"));
                ClassicAssert.AreEqual("ERR wrong number of arguments for 'script|load' command", exc.Message);
            }

            // > 1 args
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("SCRIPT", "LOAD", "return 'foo'", "return 'bar'"));
                ClassicAssert.AreEqual("ERR wrong number of arguments for 'script|load' command", exc.Message);
            }
        }

        [Test]
        public void ScriptExistsMultiple()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var server = redis.GetServers().First();

            var hashBytes = server.ScriptLoad("return 'foo'");

            // upper hash
            {
                var hash = string.Join("", hashBytes.Select(static x => x.ToString("X2")));

                var exists = (RedisValue[])server.Execute("SCRIPT", "EXISTS", hash, "foo", "bar");

                ClassicAssert.AreEqual(3, exists.Length);
                ClassicAssert.AreEqual(1, (long)exists[0]);
                ClassicAssert.AreEqual(0, (long)exists[1]);
                ClassicAssert.AreEqual(0, (long)exists[2]);
            }

            // lower hash
            {
                var hash = string.Join("", hashBytes.Select(static x => x.ToString("x2")));

                var exists = (RedisValue[])server.Execute("SCRIPT", "EXISTS", hash, "foo", "bar");

                ClassicAssert.AreEqual(3, exists.Length);
                ClassicAssert.AreEqual(1, (long)exists[0]);
                ClassicAssert.AreEqual(0, (long)exists[1]);
                ClassicAssert.AreEqual(0, (long)exists[2]);
            }
        }

        private static void DoErroneousRedisCall(IDatabase db, string[] args, string expectedError)
        {
            var exc = Assert.Throws<RedisServerException>(() => db.ScriptEvaluate($"return redis.call({string.Join(',', args)})"));
            ClassicAssert.IsNotNull(exc);
            StringAssert.StartsWith(expectedError, exc!.Message);
        }

        [Test]
        public void RedisCallErrors()
        {
            // Testing that our error replies for redis.call match Redis behavior
            //
            // TODO: exact matching of the hash and line number would also be nice, but that is trickier
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            // No args
            DoErroneousRedisCall(db, [],
                Encoding.ASCII.GetString(
                    CmdStrings.LUA_ERR_Please_specify_at_least_one_argument_for_this_redis_lib_call));

            // Unknown command
            DoErroneousRedisCall(db, ["'123'"],
                Encoding.ASCII.GetString(CmdStrings.LUA_ERR_Unknown_Redis_command_called_from_script));

            var badArgumentsMessage =
                Encoding.ASCII.GetString(CmdStrings
                    .LUA_ERR_Lua_redis_lib_command_arguments_must_be_strings_or_integers);

            // Bad command type
            DoErroneousRedisCall(db, ["{ foo = 'bar'}"], badArgumentsMessage);

            // GET bad arg type
            DoErroneousRedisCall(db, ["'GET'", "{ foo = 'bar'}"], badArgumentsMessage);

            // SET bad arg types
            DoErroneousRedisCall(db, ["'SET'", "'hello'", "{ foo = 'bar'}"], badArgumentsMessage);
            DoErroneousRedisCall(db, ["'SET'", "{ foo = 'bar'}", "'world'"], badArgumentsMessage);

            // Other bad arg types
            DoErroneousRedisCall(db, ["'DEL'", "{ foo = 'bar'}", "'world'"], badArgumentsMessage);
        }

        [Test]
        public void BinaryValuesInScripts()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var trickyKey = new byte[] { 0, 1, 2, 3, 4 };
            var trickyValue = new byte[] { 5, 6, 7, 8, 9, 0 };
            var trickyValue2 = new byte[] { 0, 1, 0, 1, 0, 1, 255 };

            ClassicAssert.IsTrue(db.StringSet(trickyKey, trickyValue));

            var luaEscapeKeyString = $"{string.Join("", trickyKey.Select(x => $"\\{x:X2}"))}";

            var readDirectKeyRaw = db.ScriptEvaluate($"return redis.call('GET', '{luaEscapeKeyString}')", [(RedisKey)trickyKey]);
            var readDirectKeyBytes = (byte[])readDirectKeyRaw;
            ClassicAssert.IsTrue(trickyValue.AsSpan().SequenceEqual(readDirectKeyBytes));

            var setKey = db.ScriptEvaluate("return redis.call('SET', KEYS[1], ARGV[1])", [(RedisKey)trickyKey], [(RedisValue)trickyValue2]);
            ClassicAssert.AreEqual("OK", (string)setKey);

            var readTrickyValue2Raw = db.StringGet(trickyKey);
            var readTrickyValue2 = (byte[])readTrickyValue2Raw;
            ClassicAssert.IsTrue(trickyValue2.AsSpan().SequenceEqual(readTrickyValue2));
        }

        [Test]
        public void NumberArgumentCoercion()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            _ = db.StringSet("2", "hello");
            _ = db.StringSet("2.1", "world");

            var res = (string)db.ScriptEvaluate("return redis.call('GET', 2.1)");
            ClassicAssert.AreEqual("world", res);
        }

        [Test]
        public void ComplexLuaReturns()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            // Relatively complicated
            {
                var res1 = (RedisResult[])db.ScriptEvaluate("return { 1, 'hello', { true, false }, { fizz = 'buzz', hello = 'world' }, 5, 6 }");
                ClassicAssert.AreEqual(6, res1.Length);
                ClassicAssert.AreEqual(1, (long)res1[0]);
                ClassicAssert.AreEqual("hello", (string)res1[1]);
                var res1Sub1 = (RedisResult[])res1[2];
                ClassicAssert.AreEqual(2, res1Sub1.Length);
                ClassicAssert.AreEqual(1, (long)res1Sub1[0]);
                ClassicAssert.IsTrue(res1Sub1[1].IsNull);
                var res1Sub2 = (RedisResult[])res1[2];
                ClassicAssert.AreEqual(2, res1Sub2.Length);
                ClassicAssert.IsTrue((bool)res1Sub2[0]);
                ClassicAssert.IsTrue(res1Sub2[1].IsNull);
                var res1Sub3 = (RedisResult[])res1[3];
                ClassicAssert.AreEqual(0, res1Sub3.Length);
                ClassicAssert.AreEqual(5, (long)res1[4]);
                ClassicAssert.AreEqual(6, (long)res1[5]);
            }

            // Only indexable will be included
            {
                var res2 = (RedisResult[])db.ScriptEvaluate("return { 1, 2, fizz='buzz' }");
                ClassicAssert.AreEqual(2, res2.Length);
                ClassicAssert.AreEqual(1, (long)res2[0]);
                ClassicAssert.AreEqual(2, (long)res2[1]);
            }

            // Non-string, non-number, are nullish
            {
                var res3 = (RedisResult[])db.ScriptEvaluate("return { 1, function() end, 3 }");
                ClassicAssert.AreEqual(3, res3.Length);
                ClassicAssert.AreEqual(1, (long)res3[0]);
                ClassicAssert.IsTrue(res3[1].IsNull);
                ClassicAssert.AreEqual(3, (long)res3[2]);
            }

            // Nil stops return of subsequent values
            {
                var res4 = (RedisResult[])db.ScriptEvaluate("return { 1, nil, 3 }");
                ClassicAssert.AreEqual(1, res4.Length);
                ClassicAssert.AreEqual(1, (long)res4[0]);
            }

            // Incredibly deeply nested return
            {
                const int Depth = 100;

                var tableDepth = new StringBuilder();
                for (var i = 1; i <= Depth; i++)
                {
                    if (i != 1)
                    {
                        _ = tableDepth.Append(", ");
                    }
                    _ = tableDepth.Append("{ ");
                    _ = tableDepth.Append(i);
                }

                for (var i = 1; i <= Depth; i++)
                {
                    _ = tableDepth.Append(" }");
                }

                var script = "return " + tableDepth.ToString();

                var res5 = db.ScriptEvaluate(script);

                var cur = res5;
                for (var i = 1; i < Depth; i++)
                {
                    var top = (RedisResult[])cur;
                    ClassicAssert.AreEqual(2, top.Length);
                    ClassicAssert.AreEqual(i, (long)top[0]);

                    cur = top[1];
                }

                // Remainder should have a single element
                var remainder = (RedisResult[])cur;
                ClassicAssert.AreEqual(1, remainder.Length);
                ClassicAssert.AreEqual(Depth, (long)remainder[0]);
            }

            // Incredibly wide
            {
                const int Width = 100_000;

                var tableDepth = new StringBuilder();
                for (var i = 1; i <= Width; i++)
                {
                    if (i != 1)
                    {
                        _ = tableDepth.Append(", ");
                    }
                    _ = tableDepth.Append("{ " + i + " }");
                }

                var script = "return { " + tableDepth.ToString() + " }";

                var res5 = (RedisResult[])db.ScriptEvaluate(script);
                for (var i = 0; i < Width; i++)
                {
                    var elem = (RedisResult[])res5[i];
                    ClassicAssert.AreEqual(1, elem.Length);
                    ClassicAssert.AreEqual(i + 1, (long)elem[0]);
                }
            }
        }

        [Test]
        public void MetatableReturn()
        {
            const string Script = @"
                local table = { abc = 'def', ghi = 'jkl' }
                local ret = setmetatable(
                    table,
                    {
                        __len = function (self)
                            return 4
                        end,
                        __index = function(self, key)
                            if key == 1 then
                                return KEYS[1]
                            end

                            if key == 2 then
                                return ARGV[1]
                            end

                            if key == 3 then
                                return self.ghi
                            end

                            if key == 4 then
                                return self.abc
                            end

                            return nil
                        end
                    }
                )

                -- prove that metatables WORK but also that we don't deconstruct them for returns
                return { ret, ret[1], ret[2], ret[3], ret[4] }";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var ret = (RedisResult[])db.ScriptEvaluate(Script, [(RedisKey)"foo"], [(RedisValue)"bar"]);
            ClassicAssert.AreEqual(5, ret.Length);
            var firstEmpty = (RedisResult[])ret[0];
            ClassicAssert.AreEqual(0, firstEmpty.Length);
            ClassicAssert.AreEqual("foo", (string)ret[1]);
            ClassicAssert.AreEqual("bar", (string)ret[2]);
            ClassicAssert.AreEqual("jkl", (string)ret[3]);
            ClassicAssert.AreEqual("def", (string)ret[4]);
        }

        [Test]
        public void IntentionalOOM()
        {
            if (string.IsNullOrEmpty(limitBytes))
            {
                ClassicAssert.Ignore("No memory limit enabled");
                return;
            }

            const string ScriptOOMText = @"
local foo = 'abcdefghijklmnopqrstuvwxyz'
if @Ctrl == 'OOM' then
    for i = 1, 10000000 do
        foo = foo .. foo
    end
end

return foo";
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var scriptOOM = LuaScript.Prepare(ScriptOOMText);
            var loadedScriptOOM = scriptOOM.Load(redis.GetServers()[0]);

            // OOM actually happens and is reported
            var exc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate(loadedScriptOOM, new { Ctrl = "OOM" }));
            ClassicAssert.AreEqual("ERR Lua encountered an error: not enough memory", exc.Message);

            // We can still run the script without issue (with non-crashing args) afterwards
            var res = db.ScriptEvaluate(loadedScriptOOM, new { Ctrl = "Safe" });
            ClassicAssert.AreEqual("abcdefghijklmnopqrstuvwxyz", (string)res);
        }

        [Test]
        public void Issue939()
        {

            // See: https://github.com/microsoft/garnet/issues/939
            const string Script = @"
local retArray = {}
local lockValue = ''
local writeLockValue = redis.call('GET',@LockName)
if writeLockValue ~= false then
    lockValue = writeLockValue
end
retArray[1] = lockValue
if lockValue == '' then 
    retArray[2] = redis.call('GET',@CollectionName) 
else 
    retArray[2] = ''
end

local SessionTimeout = redis.call('GET', @TimeoutName)
if SessionTimeout ~= false then
    retArray[3] = SessionTimeout
    redis.call('EXPIRE',@CollectionName, SessionTimeout)
    redis.call('EXPIRE',@TimeoutName, SessionTimeout)
else
    retArray[3] = '-1'
end

return retArray";

            const string LockName = "{/hello}-lockname";
            const string CollectionName = "{/hello}-collectionName";
            const string TimeoutName = "{/hello}-sessionTimeout";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var preparedScript = LuaScript.Prepare(Script);

            // 8 combos, keys presents or not for all 8

            Span<bool> present = [true, false];

            foreach (var lockSet in present)
            {
                foreach (var colSet in present)
                {
                    foreach (var sessionSet in present)
                    {
                        if (lockSet)
                        {
                            ClassicAssert.True(db.StringSet(LockName, "present"));
                        }
                        else
                        {
                            _ = db.KeyDelete(LockName);
                        }

                        if (colSet)
                        {
                            ClassicAssert.True(db.StringSet(CollectionName, "present"));
                        }
                        else
                        {
                            _ = db.KeyDelete(CollectionName);
                        }

                        if (sessionSet)
                        {
                            ClassicAssert.True(db.StringSet(TimeoutName, "123456"));
                        }
                        else
                        {
                            _ = db.KeyDelete(TimeoutName);
                        }

                        var res = (RedisResult[])db.ScriptEvaluate(preparedScript, new { LockName, CollectionName, TimeoutName });
                        ClassicAssert.AreEqual(3, res.Length);
                    }
                }
            }

            // Finally, check that nil is an illegal argument
            var exc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return redis.call('GET', nil)"));
            ClassicAssert.True(exc.Message.StartsWith("ERR Lua redis lib command arguments must be strings or integers"));
        }

        [Test]
        public void Issue1079()
        {
            // Repeated submission of invalid Lua scripts shouldn't be cached, and thus should produce the same compilation error each time
            //
            // They also shouldn't break the session for future executions

            const string BrokenScript = "return \"hello lua";
            const string FixedScript = "return \"hello lua\"";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var brokenExc1 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("EVAL", BrokenScript, 0));
            ClassicAssert.True(brokenExc1.Message.StartsWith("Compilation error: "));

            var brokenExc2 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("EVAL", BrokenScript, 0));
            ClassicAssert.AreEqual(brokenExc1.Message, brokenExc2.Message);

            var success = (string)db.Execute("EVAL", FixedScript, 0);
            ClassicAssert.AreEqual("hello lua", success);
        }

        [TestCase(2)]
        [TestCase(3)]
        public void LuaToResp2Conversions(int redisSetRespVersion)
        {
            // Per: https://redis.io/docs/latest/develop/interact/programmability/lua-api/#lua-to-resp2-type-conversion
            //
            // Number -> Integer
            // String -> Bulk String
            // Table -> Array
            // False -> Null Bulk
            // True -> Integer 1
            // Float -> Integer
            // Table with nil key -> Array (truncated at first nil)
            // Table with string keys -> Array (string keys excluded)
            //
            // Nil is unspecified, but experimentally is mapped to Null Bulk

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: RedisProtocol.Resp2));
            var db = redis.GetDatabase();

            var numberRes = db.ScriptEvaluate($"redis.setresp({redisSetRespVersion}); return 1");
            ClassicAssert.AreEqual(ResultType.Integer, numberRes.Resp2Type);

            var stringRes = db.ScriptEvaluate($"redis.setresp({redisSetRespVersion});return 'hello'");
            ClassicAssert.AreEqual(ResultType.BulkString, stringRes.Resp2Type);

            var tableRes = db.ScriptEvaluate($"redis.setresp({redisSetRespVersion});return {{ 0, 1, 2 }}");
            ClassicAssert.AreEqual(ResultType.Array, tableRes.Resp2Type);
            ClassicAssert.AreEqual(3, ((int[])tableRes).Length);

            // False is _weird_ and Redis does actually change this, even if it seems like it shouldn't
            var falseRes = db.ScriptEvaluate($"redis.setresp({redisSetRespVersion}); return false");
            if (redisSetRespVersion == 3)
            {
                ClassicAssert.AreEqual(ResultType.Integer, falseRes.Resp2Type);
                ClassicAssert.AreEqual(0, (int)falseRes);
            }
            else
            {
                ClassicAssert.AreEqual(ResultType.BulkString, falseRes.Resp2Type);
                ClassicAssert.Null((string)falseRes);
            }

            var trueRes = db.ScriptEvaluate($"redis.setresp({redisSetRespVersion}); return true");
            ClassicAssert.AreEqual(ResultType.Integer, trueRes.Resp2Type);
            ClassicAssert.AreEqual(1, (int)trueRes);

            var floatRes = db.ScriptEvaluate($"redis.setresp({redisSetRespVersion}); return 3.7");
            ClassicAssert.AreEqual(ResultType.Integer, floatRes.Resp2Type);
            ClassicAssert.AreEqual(3, (int)floatRes);

            var tableNilRes = db.ScriptEvaluate($"redis.setresp({redisSetRespVersion}); return {{ 0, 1, nil, 2 }}");
            ClassicAssert.AreEqual(ResultType.Array, tableNilRes.Resp2Type);
            ClassicAssert.AreEqual(2, ((int[])tableNilRes).Length);

            var tableStringRes = db.ScriptEvaluate($"redis.setresp({redisSetRespVersion}); local x = {{ 0, 1 }}; x['y'] = 'hello'; return x");
            ClassicAssert.AreEqual(ResultType.Array, tableStringRes.Resp2Type);
            ClassicAssert.AreEqual(2, ((int[])tableStringRes).Length);

            var nilRes = db.ScriptEvaluate($"redis.setresp({redisSetRespVersion}); return nil");
            ClassicAssert.AreEqual(ResultType.BulkString, nilRes.Resp2Type);
            ClassicAssert.True(nilRes.IsNull);
        }

        [Test]
        public void LuaToResp3Conversions()
        {
            // Per: https://redis.io/docs/latest/develop/interact/programmability/lua-api/#lua-to-resp3-type-conversion
            //
            // Number -> Integer
            // String -> Bulk String
            // Table -> Array
            // False -> Boolean
            // True -> Boolean
            // Float -> Integer
            // Table with nil key -> Array (truncated at first nil)
            // Table with string keys -> Array (string keys excluded)
            // Nil -> Null
            // { map = { ... } } -> Map
            // { set = { ... } } -> Set
            // { double = "..." } -> Double

            // In theory, connection protocol and script protocol are independent
            // This is what the docs imply
            // In practice, Redis does actually change behavior due to combinations of those
            //
            // So these tests are verifying that we match Redis's actual behavior
            using var redis3 = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: RedisProtocol.Resp3));
            var db3 = redis3.GetDatabase();
            using var redis2 = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: RedisProtocol.Resp2));
            var db2 = redis2.GetDatabase();

            // Connection RESP3, Script RESP3
            {
                var numberRes = db3.ScriptEvaluate($"redis.setresp(3); return 1");
                ClassicAssert.AreEqual(ResultType.Integer, numberRes.Resp3Type);

                var stringRes = db3.ScriptEvaluate($"redis.setresp(3); return 'hello'");
                ClassicAssert.AreEqual(ResultType.BulkString, stringRes.Resp3Type);

                var tableRes = db3.ScriptEvaluate($"redis.setresp(3); return {{ 0, 1, 2 }}");
                ClassicAssert.AreEqual(ResultType.Array, tableRes.Resp3Type);
                ClassicAssert.AreEqual(3, ((int[])tableRes).Length);

                var falseRes = db3.ScriptEvaluate($"redis.setresp(3); return false");
                ClassicAssert.AreEqual(ResultType.Boolean, falseRes.Resp3Type);
                ClassicAssert.False((bool)falseRes);

                var trueRes = db3.ScriptEvaluate($"redis.setresp(3); return true");
                ClassicAssert.AreEqual(ResultType.Boolean, trueRes.Resp3Type);
                ClassicAssert.True((bool)trueRes);

                var floatRes = db3.ScriptEvaluate($"redis.setresp(3); return 3.7");
                ClassicAssert.AreEqual(ResultType.Integer, floatRes.Resp3Type);
                ClassicAssert.AreEqual(3, (int)floatRes);

                var tableNilRes = db3.ScriptEvaluate($"redis.setresp(3); return {{ 0, 1, nil, 2 }}");
                ClassicAssert.AreEqual(ResultType.Array, tableNilRes.Resp3Type);
                ClassicAssert.AreEqual(2, ((int[])tableNilRes).Length);

                var tableStringRes = db3.ScriptEvaluate($"redis.setresp(3); local x = {{ 0, 1 }}; x['y'] = 'hello'; return x");
                ClassicAssert.AreEqual(ResultType.Array, tableStringRes.Resp3Type);
                ClassicAssert.AreEqual(2, ((int[])tableStringRes).Length);

                var nilRes = db3.ScriptEvaluate($"redis.setresp(3); return nil");
                ClassicAssert.AreEqual(ResultType.Null, nilRes.Resp3Type);
                ClassicAssert.True(nilRes.IsNull);

                var mapRes = db3.ScriptEvaluate($"redis.setresp(3); return {{ map = {{ hello = 'world' }} }}");
                ClassicAssert.AreEqual(ResultType.Map, mapRes.Resp3Type);
                ClassicAssert.AreEqual("world", (string)mapRes.ToDictionary()["hello"]);

                var setRes = db3.ScriptEvaluate($"redis.setresp(3); return {{ set = {{ hello = true }} }}");
                ClassicAssert.AreEqual(ResultType.Set, setRes.Resp3Type);
                ClassicAssert.AreEqual(1, ((string[])setRes).Length);
                ClassicAssert.True(((string[])setRes).Contains("hello"));

                var doubleRes = db3.ScriptEvaluate($"redis.setresp(3); return {{ double = 1.23 }}");
                ClassicAssert.AreEqual(ResultType.Double, doubleRes.Resp3Type);
                ClassicAssert.AreEqual(1.23, (double)doubleRes);
            }

            // Connection RESP2, Script RESP3
            {
                var numberRes = db2.ScriptEvaluate($"redis.setresp(3); return 1");
                ClassicAssert.AreEqual(ResultType.Integer, numberRes.Resp3Type);

                var stringRes = db2.ScriptEvaluate($"redis.setresp(3);return 'hello'");
                ClassicAssert.AreEqual(ResultType.BulkString, stringRes.Resp3Type);

                var tableRes = db2.ScriptEvaluate($"redis.setresp(3);return {{ 0, 1, 2 }}");
                ClassicAssert.AreEqual(ResultType.Array, tableRes.Resp3Type);
                ClassicAssert.AreEqual(3, ((int[])tableRes).Length);

                // SPEC BREAK!
                var falseRes = db2.ScriptEvaluate($"redis.setresp(3); return false");
                ClassicAssert.AreEqual(ResultType.Integer, falseRes.Resp3Type);
                ClassicAssert.False((bool)falseRes);

                // SPEC BREAK!
                var trueRes = db2.ScriptEvaluate($"redis.setresp(3); return true");
                ClassicAssert.AreEqual(ResultType.Integer, trueRes.Resp3Type);
                ClassicAssert.True((bool)trueRes);

                var floatRes = db2.ScriptEvaluate($"redis.setresp(3); return 3.7");
                ClassicAssert.AreEqual(ResultType.Integer, floatRes.Resp3Type);
                ClassicAssert.AreEqual(3, (int)floatRes);

                var tableNilRes = db2.ScriptEvaluate($"redis.setresp(3); return {{ 0, 1, nil, 2 }}");
                ClassicAssert.AreEqual(ResultType.Array, tableNilRes.Resp3Type);
                ClassicAssert.AreEqual(2, ((int[])tableNilRes).Length);

                var tableStringRes = db2.ScriptEvaluate($"redis.setresp(3); local x = {{ 0, 1 }}; x['y'] = 'hello'; return x");
                ClassicAssert.AreEqual(ResultType.Array, tableStringRes.Resp3Type);
                ClassicAssert.AreEqual(2, ((int[])tableStringRes).Length);

                var nilRes = db2.ScriptEvaluate($"redis.setresp(3); return nil");
                ClassicAssert.AreEqual(ResultType.Null, nilRes.Resp3Type);
                ClassicAssert.True(nilRes.IsNull);

                var mapRes = db2.ScriptEvaluate($"redis.setresp(3); return {{ map = {{ hello = 'world' }} }}");
                ClassicAssert.AreEqual(ResultType.Array, mapRes.Resp3Type);
                ClassicAssert.True(((string[])mapRes).SequenceEqual(["hello", "world"]));

                var setRes = db2.ScriptEvaluate($"redis.setresp(3); return {{ set = {{ hello = true }} }}");
                ClassicAssert.AreEqual(ResultType.Array, setRes.Resp3Type);
                ClassicAssert.AreEqual(1, ((string[])setRes).Length);
                ClassicAssert.True(((string[])setRes).Contains("hello"));

                // SPEC BREAK!
                var doubleRes = db2.ScriptEvaluate($"redis.setresp(3); return {{ double = 1.23 }}");
                ClassicAssert.AreEqual(ResultType.BulkString, doubleRes.Resp3Type);
                ClassicAssert.AreEqual("1.23", (string)doubleRes);
            }

            // Connection RESP3, Script RESP2
            {
                var numberRes = db3.ScriptEvaluate($"redis.setresp(2); return 1");
                ClassicAssert.AreEqual(ResultType.Integer, numberRes.Resp3Type);

                var stringRes = db3.ScriptEvaluate($"redis.setresp(2);return 'hello'");
                ClassicAssert.AreEqual(ResultType.BulkString, stringRes.Resp3Type);

                var tableRes = db3.ScriptEvaluate($"redis.setresp(2);return {{ 0, 1, 2 }}");
                ClassicAssert.AreEqual(ResultType.Array, tableRes.Resp3Type);
                ClassicAssert.AreEqual(3, ((int[])tableRes).Length);

                // SPEC BREAK!
                var falseRes = db3.ScriptEvaluate($"redis.setresp(2); return false");
                ClassicAssert.AreEqual(ResultType.Null, falseRes.Resp3Type);
                ClassicAssert.False((bool)falseRes);

                // SPEC BREAK!
                var trueRes = db3.ScriptEvaluate($"redis.setresp(2); return true");
                ClassicAssert.AreEqual(ResultType.Integer, trueRes.Resp3Type);
                ClassicAssert.True((bool)trueRes);

                var floatRes = db3.ScriptEvaluate($"redis.setresp(2); return 3.7");
                ClassicAssert.AreEqual(ResultType.Integer, floatRes.Resp3Type);
                ClassicAssert.AreEqual(3, (int)floatRes);

                var tableNilRes = db3.ScriptEvaluate($"redis.setresp(2); return {{ 0, 1, nil, 2 }}");
                ClassicAssert.AreEqual(ResultType.Array, tableNilRes.Resp3Type);
                ClassicAssert.AreEqual(2, ((int[])tableNilRes).Length);

                var tableStringRes = db3.ScriptEvaluate($"redis.setresp(2); local x = {{ 0, 1 }}; x['y'] = 'hello'; return x");
                ClassicAssert.AreEqual(ResultType.Array, tableStringRes.Resp3Type);
                ClassicAssert.AreEqual(2, ((int[])tableStringRes).Length);

                var nilRes = db3.ScriptEvaluate($"redis.setresp(2); return nil");
                ClassicAssert.AreEqual(ResultType.Null, nilRes.Resp3Type);
                ClassicAssert.True(nilRes.IsNull);

                var mapRes = db3.ScriptEvaluate($"redis.setresp(2); return {{ map = {{ hello = 'world' }} }}");
                ClassicAssert.AreEqual(ResultType.Map, mapRes.Resp3Type);
                ClassicAssert.AreEqual("world", (string)mapRes.ToDictionary()["hello"]);

                var setRes = db2.ScriptEvaluate($"redis.setresp(2); return {{ set = {{ hello = true }} }}");
                ClassicAssert.AreEqual(ResultType.Array, setRes.Resp3Type);
                ClassicAssert.AreEqual(1, ((string[])setRes).Length);
                ClassicAssert.True(((string[])setRes).Contains("hello"));

                // SPEC BREAK!
                var doubleRes = db2.ScriptEvaluate($"redis.setresp(2); return {{ double = 1.23 }}");
                ClassicAssert.AreEqual(ResultType.BulkString, doubleRes.Resp3Type);
                ClassicAssert.AreEqual("1.23", (string)doubleRes);
            }
        }

        // TODO: Every single command that's response changes between RESP2 and RESP3 needs to be covered

        [Test]
        public void Resp2ToLuaConversions()
        {
            // Per: https://redis.io/docs/latest/develop/interact/programmability/lua-api/#resp2-to-lua-type-conversion
            //
            // Integer -> Number
            // Bulk String -> String
            // Array -> Table
            // Simple String -> Table with ok field
            // Error reply -> Table with err field
            // Null bulk -> false
            // Null multibulk -> false

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: RedisProtocol.Resp2));
            var db = redis.GetDatabase();

            var integerRes = db.ScriptEvaluate("local res = redis.call('INCR', KEYS[1]); return type(res);", [(RedisKey)"foo"]);
            ClassicAssert.AreEqual("number", (string)integerRes);

            _ = db.StringSet("hello", "world");
            var bulkStringRes = db.ScriptEvaluate("local res = redis.call('GET', KEYS[1]); return type(res);", [(RedisKey)"hello"]);
            ClassicAssert.AreEqual("string", (string)bulkStringRes);

            _ = db.ListLeftPush("fizz", "buzz");
            var arrayRes = db.ScriptEvaluate("local res = redis.call('LRANGE', KEYS[1], '0', '1'); return type(res);", [(RedisKey)"fizz"]);
            ClassicAssert.AreEqual("table", (string)arrayRes);

            var simpleStringRes = (string[])db.ScriptEvaluate("local res = redis.call('PING'); return { type(res), res.ok };");
            ClassicAssert.AreEqual(2, simpleStringRes.Length);
            ClassicAssert.AreEqual("table", simpleStringRes[0]);
            ClassicAssert.AreEqual("PONG", simpleStringRes[1]);

            var errExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return { err = 'ERR mapped to ERR response' }"));
            ClassicAssert.AreEqual("ERR mapped to ERR response", errExc.Message);

            var nullBulkRes = (string[])db.ScriptEvaluate("local res = redis.call('GET', KEYS[1]); return { type(res), tostring(res) };", [(RedisKey)"not-set-ever"]);
            ClassicAssert.AreEqual(2, nullBulkRes.Length);
            ClassicAssert.AreEqual("boolean", nullBulkRes[0]);
            ClassicAssert.AreEqual("false", nullBulkRes[1]);

            // Since GET is special cased, use a different nil responding command to test that path
            var nullBulk2Res = (string[])db.ScriptEvaluate("local res = redis.call('HGET', KEYS[1], ARGV[1]); return { type(res), tostring(res) };", [(RedisKey)"not-set-ever"], [(RedisValue)"never-ever"]);
            ClassicAssert.AreEqual(2, nullBulk2Res.Length);
            ClassicAssert.AreEqual("boolean", nullBulk2Res[0]);
            ClassicAssert.AreEqual("false", nullBulk2Res[1]);

            var nullMultiBulk = (string[])db.ScriptEvaluate("local res = redis.call('BLPOP', KEYS[1], '1'); return { type(res), tostring(res) };", [(RedisKey)"not-set-ever"]);
            ClassicAssert.AreEqual(2, nullMultiBulk.Length);
            ClassicAssert.AreEqual("boolean", nullMultiBulk[0]);
            ClassicAssert.AreEqual("false", nullMultiBulk[1]);
        }

        [Test]
        public void NoScriptCommandsForbidden()
        {
            ClassicAssert.True(RespCommandsInfo.TryGetRespCommandsInfo(out var allCommands, externalOnly: true));

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            foreach (var (cmd, _) in allCommands.Where(static kv => kv.Value.Flags.HasFlag(RespCommandFlags.NoScript)))
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate($"redis.call('{cmd}')"));
                ClassicAssert.True(exc.Message.StartsWith("ERR This Redis command is not allowed from script"), $"Allowed NoScript command: {cmd}");
            }
        }

        [Test]
        public void IntentionalTimeout()
        {
            const string TimeoutScript = @"
local count = 0

if @Ctrl == 'Timeout' then
    while true do
        count = count + 1
    end
end

return count";

            if (string.IsNullOrEmpty(limitTimeout))
            {
                ClassicAssert.Ignore("No timeout enabled");
                return;
            }

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: RedisProtocol.Resp2));
            var db = redis.GetDatabase();

            var scriptTimeout = LuaScript.Prepare(TimeoutScript);
            var loadedScriptTimeout = scriptTimeout.Load(redis.GetServers()[0]);

            // Timeout actually happens and is reported
            var exc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate(loadedScriptTimeout, new { Ctrl = "Timeout" }));
            ClassicAssert.AreEqual("ERR Lua script exceeded configured timeout", exc.Message);

            // We can still run the script without issue (with non-crashing args) afterwards
            var res = db.ScriptEvaluate(loadedScriptTimeout, new { Ctrl = "Safe" });
            ClassicAssert.AreEqual(0, (int)res);
        }

        [TestCase(RedisProtocol.Resp2)]
        [TestCase(RedisProtocol.Resp3)]
        public void Resp3ToLuaConversions(RedisProtocol connectionProtocol)
        {
            // Using redis.setresp(2|3) controls how results from the script are
            // converted to RESP and how results from redis.call(...) are converted
            // to Lua types.

            // Connection level protocol SHOULD NOT impact how the session behaves before calls to redis.setresp(...)
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: connectionProtocol));
            var db = redis.GetDatabase();

            // Resp3 MAP -> Lua
            {
                _ = db.KeyDelete("hgetall_testkey");
                _ = db.HashSet("hgetall_testkey", "foo", "bar");

                var resp2SetToLua = (string)db.ScriptEvaluate($"return type(redis.call('HGETALL', 'hgetall_testkey').map)");
                var resp3SetToLua = (string)db.ScriptEvaluate($"redis.setresp(3) return type(redis.call('HGETALL', 'hgetall_testkey').map)");
                ClassicAssert.AreEqual("nil", resp2SetToLua);
                ClassicAssert.AreEqual("table", resp3SetToLua);
            }

            // Resp3 SET -> Lua
            {
                _ = db.KeyDelete("sdiff_testkey1");
                _ = db.KeyDelete("sdiff_testkey2");
                _ = db.SetAdd("sdiff_testkey1", "foo");
                _ = db.SetAdd("sdiff_testkey2", "bar");

                // Resp2 sets are arrays
                var resp2SetToLua = (string)db.ScriptEvaluate($"return type(redis.call('SDIFF', 'sdiff_testkey1', 'sdiff_testkey2').set)");
                var resp3SetToLua = (string)db.ScriptEvaluate($"redis.setresp(3) return type(redis.call('SDIFF', 'sdiff_testkey1', 'sdiff_testkey2').set)");
                ClassicAssert.AreEqual("nil", resp2SetToLua);
                ClassicAssert.AreEqual("table", resp3SetToLua);
            }

            // Resp3 NULL -> Lua
            {
                _ = db.KeyDelete("hget_testkey");

                // Resp2 nulls are the Nil bulk string
                var resp2NilToLua = (string)db.ScriptEvaluate($"return type(redis.call('HGET', 'hget_testkey', 'foo'))");
                var resp3NilToLua = (string)db.ScriptEvaluate($"redis.setresp(3); return type(redis.call('HGET', 'hget_testkey', 'foo'))");
                ClassicAssert.AreEqual("boolean", resp2NilToLua);
                ClassicAssert.AreEqual("nil", resp3NilToLua);
            }

            // Resp3 false -> Lua
            {
                // Resp2 bools are ints
                var resp2FalseToLua = (string)db.ScriptEvaluate($"return type(redis.call('RECHO', ':0'))");
                var resp3FalseToLua = (string)db.ScriptEvaluate($"redis.setresp(3); return type(redis.call('RECHO', '#f'))");
                ClassicAssert.AreEqual("number", resp2FalseToLua);
                ClassicAssert.AreEqual("boolean", resp3FalseToLua);
            }

            // Resp3 true -> Lua
            {
                // Resp2 bools are ints
                var resp2TrueToLua = (string)db.ScriptEvaluate($"return type(redis.call('RECHO', ':1'))");
                var resp3TrueToLua = (string)db.ScriptEvaluate($"redis.setresp(3); return type(redis.call('RECHO', '#t'))");
                ClassicAssert.AreEqual("number", resp2TrueToLua);
                ClassicAssert.AreEqual("boolean", resp3TrueToLua);
            }

            // Resp3 double -> Lua
            {
                _ = db.KeyDelete("zscore_testkey");
                _ = db.SortedSetAdd("zscore_testkey", "foo", 1.23);

                // Resp2 doubles are just strings, so .double should be nil
                var resp2DoubleToLua = (string)db.ScriptEvaluate($"return type(redis.call('ZSCORE', 'zscore_testkey', 'foo').double)");
                var resp3DoubleToLua = (string)db.ScriptEvaluate($"redis.setresp(3) return type(redis.call('ZSCORE', 'zscore_testkey', 'foo').double)");
                ClassicAssert.AreEqual("nil", resp2DoubleToLua);
                ClassicAssert.AreEqual("number", resp3DoubleToLua);
            }

            // Resp3 big number -> Lua
            {
                // No Resp2 equivalent
                var resp3BigNumToLua = (string)db.ScriptEvaluate($"redis.setresp(3); return type(redis.call('RECHO', '(123').big_number)");
                ClassicAssert.AreEqual("string", resp3BigNumToLua);
            }

            // Resp3 Verbatim string -> Lua
            {
                // No Resp2 equivalent
                var resp3VerbatimStrToLua1 = (string)db.ScriptEvaluate($"redis.setresp(3); return type(redis.call('RECHO', '=12\\r\\nfoo:fizzbuzz').format)");
                var resp3VerbatimStrToLua2 = (string)db.ScriptEvaluate($"redis.setresp(3); return type(redis.call('RECHO', '=12\\r\\nfoo:fizzbuzz').string)");
                ClassicAssert.AreEqual("string", resp3VerbatimStrToLua1);
                ClassicAssert.AreEqual("string", resp3VerbatimStrToLua2);
            }
        }

        [Test]
        public void Bit()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            // tobit
            {
                var noArgExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return bit.tobit()"));
                ClassicAssert.True(noArgExc.Message.Contains("bad argument") && noArgExc.Message.Contains("tobit"));

                // Extra arguments are legal, but ignored

                var badTypeExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return bit.tobit({})"));
                ClassicAssert.True(badTypeExc.Message.Contains("bad argument") && badTypeExc.Message.Contains("tobit"));

                // Rules are suprisingly subtle, so test a bunch of tricky values
                (string Value, string Expected)[] expectedValues = [
                    ("0", "0"),
                    ("1", "1"),
                    ("1.1", "1"),
                    ("1.5", "2"),
                    ("1.9", "2"),
                    ("0.1", "0"),
                    ("0.5", "0"),
                    ("0.9", "1"),
                    ("-1.1", "-1"),
                    ("-1.5", "-2"),
                    ("-1.9", "-2"),
                    ("-0.1", "0"),
                    ("-0.5", "0"),
                    ("-0.9", "-1"),
                    (int.MinValue.ToString(), int.MinValue.ToString()),
                    (int.MaxValue.ToString(), int.MaxValue.ToString()),
                    ((1L + int.MaxValue).ToString(), int.MinValue.ToString()),
                    ((-1L + int.MinValue).ToString(), int.MaxValue.ToString()),
                    (double.MaxValue.ToString(), "-1"),
                    (double.MinValue.ToString(), "-1"),
                    (float.MaxValue.ToString(), "-447893512"),
                    (float.MinValue.ToString(), "-447893512"),
                ];
                foreach (var (value, expected) in expectedValues)
                {
                    var actual = (string)db.ScriptEvaluate($"return bit.tobit({value})");
                    ClassicAssert.AreEqual(expected, actual, $"bit.tobit conversion for {value} was incorrect");
                }
            }

            // tohex
            {
                var noArgExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return bit.tohex()"));
                ClassicAssert.True(noArgExc.Message.Contains("bad argument") && noArgExc.Message.Contains("tohex"));

                // Extra arguments are legal, but ignored

                var badType1Exc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return bit.tohex({})"));
                ClassicAssert.True(badType1Exc.Message.Contains("bad argument") && badType1Exc.Message.Contains("tohex"));

                var badType2Exc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return bit.tohex(1, {})"));
                ClassicAssert.True(badType2Exc.Message.Contains("bad argument") && badType2Exc.Message.Contains("tohex"));

                // Make sure casing is handled correctly
                for (var h = 0; h < 16; h++)
                {
                    var lower = (string)db.ScriptEvaluate($"return bit.tohex({h}, 1)");
                    var upper = (string)db.ScriptEvaluate($"return bit.tohex({h}, -1)");

                    ClassicAssert.AreEqual(h.ToString("x1"), lower);
                    ClassicAssert.AreEqual(h.ToString("X1"), upper);
                }

                // Run through some weird values
                (string Value, int? N, string Expected)[] expectedValues = [
                    ("0", null, "00000000"),
                    ("0", 16, "00000000"),
                    ("0", -8, "00000000"),
                    ("123456", null, "0001e240"),
                    ("123456", 5, "1e240"),
                    ("123456", -5, "1E240"),
                    (int.MinValue.ToString(), null, "80000000"),
                    (int.MaxValue.ToString(), null, "7fffffff"),
                    ((1L + int.MaxValue).ToString(), null, "80000000"),
                    ((-1L + int.MinValue).ToString(), null, "7fffffff"),
                    (double.MaxValue.ToString(), 1, "f"),
                    (double.MinValue.ToString(), -1, "F"),
                    (float.MaxValue.ToString(), null, "e54daff8"),
                    (float.MinValue.ToString(), null, "e54daff8"),
                ];
                foreach (var (value, length, expected) in expectedValues)
                {
                    var actual = length != null ?
                        (string)db.ScriptEvaluate($"return bit.tohex({value},{length})") :
                        (string)db.ScriptEvaluate($"return bit.tohex({value})");

                    ClassicAssert.AreEqual(expected, actual, $"bit.tohex result for ({value},{length}) was incorrect");
                }
            }

            // bswap
            {
                var noArgExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return bit.bswap()"));
                ClassicAssert.True(noArgExc.Message.Contains("bad argument") && noArgExc.Message.Contains("bswap"));

                // Extra arguments are legal, but ignored

                var badTypeExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return bit.bswap({})"));
                ClassicAssert.True(badTypeExc.Message.Contains("bad argument") && badTypeExc.Message.Contains("bswap"));

                // Just brute force a bunch of trial values
                foreach (var a in new[] { 0, 1, 2, 4 })
                {
                    foreach (var b in new[] { 8, 16, 32, 128 })
                    {
                        foreach (var c in new[] { 0, 2, 8, 32, })
                        {
                            foreach (var d in new[] { 1, 4, 16, 64 })
                            {
                                var input = a | (b << 8) | (c << 16) | (d << 32);
                                var expected = BinaryPrimitives.ReverseEndianness(input);

                                var actual = (int)db.ScriptEvaluate($"return bit.bswap({input})");
                                ClassicAssert.AreEqual(expected, actual);
                            }
                        }
                    }
                }
            }

            // bnot
            {
                var noArgExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return bit.bnot()"));
                ClassicAssert.True(noArgExc.Message.Contains("bad argument") && noArgExc.Message.Contains("bnot"));

                // Extra arguments are legal, but ignored

                var badTypeExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return bit.bnot({})"));
                ClassicAssert.True(badTypeExc.Message.Contains("bad argument") && badTypeExc.Message.Contains("bnot"));

                foreach (var input in new int[] { 0, 1, 2, 4, 8, 32, 64, 128, 256, 0x70F0_F0F0, 0x6BCD_EF01, int.MinValue, int.MaxValue, -1 })
                {
                    var expected = ~input;

                    var actual = (int)db.ScriptEvaluate($"return bit.bnot({input})");
                    ClassicAssert.AreEqual(expected, actual);
                }
            }

            // band, bor, bxor
            {
                (int Base, string Name, Func<int, int, int> Op)[] ops = [
                    (0, "bor", static (a, b) => a | b),
                    (-1, "band", static (a, b) => a & b),
                    (0, "bxor", static (a, b) => a ^ b),
                ];

                foreach (var op in ops)
                {
                    var noArgExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate($"return bit.{op.Name}()"));
                    ClassicAssert.True(noArgExc.Message.Contains("bad argument") && noArgExc.Message.Contains(op.Name));

                    var badType1Exc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate($"return bit.{op.Name}({{}})"));
                    ClassicAssert.True(badType1Exc.Message.Contains("bad argument") && badType1Exc.Message.Contains(op.Name));

                    var badType2Exc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate($"return bit.{op.Name}(1, {{}})"));
                    ClassicAssert.True(badType2Exc.Message.Contains("bad argument") && badType2Exc.Message.Contains(op.Name));

                    var badType3Exc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate($"return bit.{op.Name}(1, 2, {{}})"));
                    ClassicAssert.True(badType3Exc.Message.Contains("bad argument") && badType3Exc.Message.Contains(op.Name));

                    // Gin up some unusual values and test them in different combinations
                    var nextArg = 0x0102_0304;
                    for (var numArgs = 1; numArgs <= 4; numArgs++)
                    {
                        var args = new List<int>();
                        while (args.Count < numArgs)
                        {
                            args.Add(nextArg);
                            nextArg *= 2;
                            nextArg += args.Count;
                        }

                        var expected = op.Base;
                        foreach (var arg in args)
                        {
                            expected = op.Op(expected, arg);
                        }

                        var actual = (int)db.ScriptEvaluate($"return bit.{op.Name}({string.Join(", ", args)})");
                        ClassicAssert.AreEqual(expected, actual);
                    }
                }
            }

            // lshift, rshift, arshift, rol, ror
            {
                (string Name, Func<int, int, int> Op)[] ops = [
                    ("lshift", static (x, n) => x << n),
                    ("rshift", static (x, n) => (int)((uint)x >> n)),
                    ("arshift", static (x, n) => x >> n),
                    ("rol", static (x, n) => (int)BitOperations.RotateLeft((uint)x, n)),
                    ("ror", static (x, n) => (int)BitOperations.RotateRight((uint)x, n)),
                ];

                foreach (var op in ops)
                {
                    var noArgExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate($"return bit.{op.Name}()"));
                    ClassicAssert.True(noArgExc.Message.Contains("bad argument") && noArgExc.Message.Contains(op.Name));

                    var badType1Exc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate($"return bit.{op.Name}({{}})"));
                    ClassicAssert.True(badType1Exc.Message.Contains("bad argument") && badType1Exc.Message.Contains(op.Name));

                    var badType2Exc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate($"return bit.{op.Name}(1, {{}})"));
                    ClassicAssert.True(badType2Exc.Message.Contains("bad argument") && badType2Exc.Message.Contains(op.Name));

                    // Extra args are allowed, but ignored

                    for (var shift = 0; shift < 16; shift++)
                    {
                        const int Value = 0x1234_5678;

                        var expected = op.Op(Value, shift);
                        var actual = (int)db.ScriptEvaluate($"return bit.{op.Name}({Value}, {shift})");

                        ClassicAssert.AreEqual(expected, actual, $"Incorrect value for bit.{op.Name}({Value}, {shift})");
                    }
                }
            }
        }

        [Test]
        public void CJson()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            // Encoding
            {
                var noArgExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return cjson.encode()"));
                ClassicAssert.True(noArgExc.Message.Contains("bad argument") && noArgExc.Message.Contains("encode"));

                var twoArgExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return cjson.encode(1, 2)"));
                ClassicAssert.True(twoArgExc.Message.Contains("bad argument") && twoArgExc.Message.Contains("encode"));

                var badTypeExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return cjson.encode((function() end))"));
                ClassicAssert.True(badTypeExc.Message.Contains("Cannot serialise"));

                var nilResp = (string)db.ScriptEvaluate("return cjson.encode(nil)");
                ClassicAssert.AreEqual("null", nilResp);

                var boolResp = (string)db.ScriptEvaluate("return cjson.encode(true)");
                ClassicAssert.AreEqual("true", boolResp);

                var doubleResp = (string)db.ScriptEvaluate("return cjson.encode(1.23)");
                ClassicAssert.AreEqual("1.23", doubleResp);

                var simpleStrResp = (string)db.ScriptEvaluate("return cjson.encode('hello')");
                ClassicAssert.AreEqual("\"hello\"", simpleStrResp);

                var encodedStrResp = (string)db.ScriptEvaluate("return cjson.encode('\"foo\" \\\\bar\\\\')");
                ClassicAssert.AreEqual("\"\\\"foo\\\" \\\\bar\\\\\"", encodedStrResp);

                var emptyTableResp = (string)db.ScriptEvaluate("return cjson.encode({})");
                ClassicAssert.AreEqual("{}", emptyTableResp);

                var keyedTableResp = (string)db.ScriptEvaluate("return cjson.encode({key=123})");
                ClassicAssert.AreEqual("{\"key\":123}", keyedTableResp);

                var indexedTableResp = (string)db.ScriptEvaluate("return cjson.encode({123, 'foo'})");
                ClassicAssert.AreEqual("[123,\"foo\"]", indexedTableResp);

                var mixedTableResp = (string)db.ScriptEvaluate("local ret = {123}; ret.bar = 'foo'; return cjson.encode(ret)");
                ClassicAssert.AreEqual("{\"1\":123,\"bar\":\"foo\"}", mixedTableResp);

                // Ordering here is undefined, just doing the brute force approach for ease of implementation
                var nestedTableResp = (string)db.ScriptEvaluate("return cjson.encode({num=1,str='hello',arr={1,2,3,4},obj={foo='bar'}})");
                string[] nestedTableRespParts = [
                    "\"arr\":[1,2,3,4]",
                    "\"num\":1",
                    "\"str\":\"hello\"",
                    "\"obj\":{\"foo\":\"bar\"}",
                ];
                var possibleNestedTableResps = new List<string>();
                for (var a = 0; a < nestedTableRespParts.Length; a++)
                {
                    for (var b = 0; b < nestedTableRespParts.Length; b++)
                    {
                        for (var c = 0; c < nestedTableRespParts.Length; c++)
                        {
                            for (var d = 0; d < nestedTableRespParts.Length; d++)
                            {
                                if (a == b || a == c || a == d || b == c || b == d || c == d)
                                {
                                    continue;
                                }

                                possibleNestedTableResps.Add($"{{{nestedTableRespParts[a]},{nestedTableRespParts[b]},{nestedTableRespParts[c]},{nestedTableRespParts[d]}}}");
                            }
                        }
                    }
                }
                ClassicAssert.True(possibleNestedTableResps.Contains(nestedTableResp));

                var nestArrayResp = (string)db.ScriptEvaluate("return cjson.encode({1,'hello',{1,2,3,4},{foo='bar'}})");
                ClassicAssert.AreEqual("[1,\"hello\",[1,2,3,4],{\"foo\":\"bar\"}]", nestArrayResp);

                var deeplyNestedButLegal =
                    (string)db.ScriptEvaluate(
@"local nested = 1
for x = 1, 1000 do
    local newNested = {}
    newNested[1] = nested;
    nested = newNested
end

return cjson.encode(nested)");
                ClassicAssert.AreEqual(new string('[', 1000) + 1 + new string(']', 1000), deeplyNestedButLegal);

                var deeplyNestedExc =
                ClassicAssert.Throws<RedisServerException>(
                    () => db.ScriptEvaluate(
@"local nested = 1
    for x = 1, 1001 do
        local newNested = {}
        newNested[1] = nested;
        nested = newNested
    end

    return cjson.encode(nested)"));
                ClassicAssert.True(deeplyNestedExc.Message.Contains("Cannot serialise, excessive nesting (1001)"));
            }

            // Decoding
            {
                var noArgExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return cjson.decode()"));
                ClassicAssert.True(noArgExc.Message.Contains("bad argument") && noArgExc.Message.Contains("decode"));

                var twoArgExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return cjson.decode(1, 2)"));
                ClassicAssert.True(twoArgExc.Message.Contains("bad argument") && twoArgExc.Message.Contains("decode"));

                var badTypeExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return cjson.decode({})"));
                ClassicAssert.True(badTypeExc.Message.Contains("bad argument") && badTypeExc.Message.Contains("decode"));

                var badFormatExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return cjson.decode('hello world')"));
                ClassicAssert.True(badFormatExc.Message.Contains("Expected value but found invalid token"));

                var nullDecode = (string)db.ScriptEvaluate("return type(cjson.decode('null'))");
                ClassicAssert.AreEqual("nil", nullDecode);

                var numberDecode = (string)db.ScriptEvaluate("return cjson.decode(123)");
                ClassicAssert.AreEqual("123", numberDecode);

                var bool1Decode = (string)db.ScriptEvaluate("return type(cjson.decode('true'))");
                ClassicAssert.AreEqual("boolean", bool1Decode);

                var bool2Decode = (string)db.ScriptEvaluate("return type(cjson.decode('false'))");
                ClassicAssert.AreEqual("boolean", bool2Decode);

                var stringDecode = (string)db.ScriptEvaluate("return cjson.decode('\"hello world\"')");
                ClassicAssert.AreEqual("hello world", stringDecode);

                var mapDecode = (string)db.ScriptEvaluate("return cjson.decode('{\"hello\":\"world\"}').hello");
                ClassicAssert.AreEqual("world", mapDecode);

                var arrayDecode = (string)db.ScriptEvaluate("return cjson.decode('[123]')[1]");
                ClassicAssert.AreEqual("123", arrayDecode);

                var complexMapDecodeArr = (string[])db.ScriptEvaluate("return cjson.decode('{\"arr\":[1,2,3,4],\"num\":1,\"str\":\"hello\",\"obj\":{\"foo\":\"bar\"}}').arr");
                ClassicAssert.True(complexMapDecodeArr.SequenceEqual(["1", "2", "3", "4"]));
                var complexMapDecodeNum = (string)db.ScriptEvaluate("return cjson.decode('{\"arr\":[1,2,3,4],\"num\":1,\"str\":\"hello\",\"obj\":{\"foo\":\"bar\"}}').num");
                ClassicAssert.AreEqual("1", complexMapDecodeNum);
                var complexMapDecodeStr = (string)db.ScriptEvaluate("return cjson.decode('{\"arr\":[1,2,3,4],\"num\":1,\"str\":\"hello\",\"obj\":{\"foo\":\"bar\"}}').str");
                ClassicAssert.AreEqual("hello", complexMapDecodeStr);
                var complexMapDecodeObj = (string)db.ScriptEvaluate("return cjson.decode('{\"arr\":[1,2,3,4],\"num\":1,\"str\":\"hello\",\"obj\":{\"foo\":\"bar\"}}').obj.foo");
                ClassicAssert.AreEqual("bar", complexMapDecodeObj);

                var complexArrDecodeNum = (string)db.ScriptEvaluate("return cjson.decode('[1,\"hello\",[1,2,3,4],{\"foo\":\"bar\"}]')[1]");
                ClassicAssert.AreEqual("1", complexArrDecodeNum);
                var complexArrDecodeStr = (string)db.ScriptEvaluate("return cjson.decode('[1,\"hello\",[1,2,3,4],{\"foo\":\"bar\"}]')[2]");
                ClassicAssert.AreEqual("hello", complexArrDecodeStr);
                var complexArrDecodeArr = (string[])db.ScriptEvaluate("return cjson.decode('[1,\"hello\",[1,2,3,4],{\"foo\":\"bar\"}]')[3]");
                ClassicAssert.True(complexArrDecodeArr.SequenceEqual(["1", "2", "3", "4"]));
                var complexArrDecodeObj = (string)db.ScriptEvaluate("return cjson.decode('[1,\"hello\",[1,2,3,4],{\"foo\":\"bar\"}]')[4].foo");
                ClassicAssert.AreEqual("bar", complexArrDecodeObj);

                // Redis cuts us off at 1000 levels of recursion, so check that we're matching that
                var deeplyNestedButLegal = (RedisResult[])db.ScriptEvaluate($"return cjson.decode('{new string('[', 1000)}{new string(']', 1000)}')");
                var deeplyNestedButLegalCur = deeplyNestedButLegal;
                for (var i = 1; i < 1000; i++)
                {
                    ClassicAssert.AreEqual(1, deeplyNestedButLegalCur.Length);
                    deeplyNestedButLegalCur = (RedisResult[])deeplyNestedButLegalCur[0];
                }
                ClassicAssert.AreEqual(0, deeplyNestedButLegalCur.Length);

                var deeplyNestedExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate($"return cjson.decode('{new string('[', 1001)}{new string(']', 1001)}')"));
                ClassicAssert.True(deeplyNestedExc.Message.Contains("Found too many nested data structures"));
            }
        }

        [Test]
        public void CMsgPackPack()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var noArgExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return cmsgpack.pack()"));
            ClassicAssert.True(noArgExc.Message.Contains("bad argument") && noArgExc.Message.Contains("pack"));

            // Multiple args are legal, and concat

            var nullResp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(nil)");
            ClassicAssert.True(nullResp.SequenceEqual(new byte[] { 0xC0 }));

            var trueResp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(true)");
            ClassicAssert.True(trueResp.SequenceEqual(new byte[] { 0xC3 }));

            var falseResp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(false)");
            ClassicAssert.True(falseResp.SequenceEqual(new byte[] { 0xC2 }));

            var tinyUInt1Resp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(0)");
            ClassicAssert.True(tinyUInt1Resp.SequenceEqual(new byte[] { 0x00 }));

            var tinyUInt2Resp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(127)");
            ClassicAssert.True(tinyUInt2Resp.SequenceEqual(new byte[] { 0x7F }));

            var tinyInt1Resp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(-1)");
            ClassicAssert.True(tinyInt1Resp.SequenceEqual(new byte[] { 0xFF }));

            var tinyInt2Resp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(-32)");
            ClassicAssert.True(tinyInt2Resp.SequenceEqual(new byte[] { 0xE0 }));

            var smallUInt1Resp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(128)");
            ClassicAssert.True(smallUInt1Resp.SequenceEqual(new byte[] { 0xCC, 0x80 }));

            var smallUInt2Resp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(255)");
            ClassicAssert.True(smallUInt2Resp.SequenceEqual(new byte[] { 0xCC, 0xFF }));

            var smallInt1Resp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(-33)");
            ClassicAssert.True(smallInt1Resp.SequenceEqual(new byte[] { 0xD0, 0xDF }));

            var smallInt2Resp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(-128)");
            ClassicAssert.True(smallInt2Resp.SequenceEqual(new byte[] { 0xD0, 0x80 }));

            var midUInt1Resp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(32768)");
            ClassicAssert.True(midUInt1Resp.SequenceEqual(new byte[] { 0xCD, 0x80, 0x00 }));

            var midUInt2Resp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(65535)");
            ClassicAssert.True(midUInt2Resp.SequenceEqual(new byte[] { 0xCD, 0xFF, 0xFF }));

            var midInt1Resp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(-129)");
            ClassicAssert.True(midInt1Resp.SequenceEqual(new byte[] { 0xD1, 0xFF, 0x7F }));

            var midInt2Resp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(-32768)");
            ClassicAssert.True(midInt2Resp.SequenceEqual(new byte[] { 0xD1, 0x80, 0x00 }));

            var uint1Resp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(2147483648)");
            ClassicAssert.True(uint1Resp.SequenceEqual(new byte[] { 0xCE, 0x80, 0x00, 0x00, 0x00 }));

            var uint2Resp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(4294967295)");
            ClassicAssert.True(uint2Resp.SequenceEqual(new byte[] { 0xCE, 0xFF, 0xFF, 0xFF, 0xFF }));

            var int1Resp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(-32769)");
            ClassicAssert.True(int1Resp.SequenceEqual(new byte[] { 0xD2, 0xFF, 0xFF, 0x7F, 0xFF }));

            var int2Resp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(-2147483648)");
            ClassicAssert.True(int2Resp.SequenceEqual(new byte[] { 0xD2, 0x80, 0x00, 0x00, 0x00 }));

            var bigUIntResp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(4294967296)");
            ClassicAssert.True(bigUIntResp.SequenceEqual(new byte[] { 0xCF, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00 }));

            var bigIntResp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(-2147483649)");
            ClassicAssert.True(bigIntResp.SequenceEqual(new byte[] { 0xD3, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F, 0xFF, 0xFF, 0xFF }));

            var floatResp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(0.1)");
            ClassicAssert.True(floatResp.SequenceEqual(new byte[] { 0xCB, 0x3F, 0xB9, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9A }));

            var tinyString1Resp = (byte[])db.ScriptEvaluate($"return cmsgpack.pack('')");
            ClassicAssert.True(tinyString1Resp.SequenceEqual(new byte[] { 0xA0, }));

            var tinyString2Resp = (byte[])db.ScriptEvaluate($"return cmsgpack.pack('{new string('0', 31)}')");
            ClassicAssert.True(tinyString2Resp.SequenceEqual(new byte[] { 0xBF }.Concat(Enumerable.Repeat((byte)'0', 31))));

            var shortString1Resp = (byte[])db.ScriptEvaluate($"return cmsgpack.pack('{new string('a', 32)}')");
            ClassicAssert.True(shortString1Resp.SequenceEqual(new byte[] { 0xD9, 0x20 }.Concat(Enumerable.Repeat((byte)'a', 32))));

            var shortString2Resp = (byte[])db.ScriptEvaluate($"return cmsgpack.pack('{new string('a', 255)}')");
            ClassicAssert.True(shortString2Resp.SequenceEqual(new byte[] { 0xD9, 0xFF }.Concat(Enumerable.Repeat((byte)'a', 255))));

            var midString1Resp = (byte[])db.ScriptEvaluate($"return cmsgpack.pack('{new string('b', 256)}')");
            ClassicAssert.True(midString1Resp.SequenceEqual(new byte[] { 0xDA, 0x01, 0x00 }.Concat(Enumerable.Repeat((byte)'b', 256))));

            var midString2Resp = (byte[])db.ScriptEvaluate($"return cmsgpack.pack('{new string('b', 65535)}')");
            ClassicAssert.True(midString2Resp.SequenceEqual(new byte[] { 0xDA, 0xFF, 0xFF }.Concat(Enumerable.Repeat((byte)'b', 65535))));

            var longStringResp = (byte[])db.ScriptEvaluate($"return cmsgpack.pack('{new string('c', 65536)}')");
            ClassicAssert.True(longStringResp.SequenceEqual(new byte[] { 0xDB, 0x00, 0x01, 0x00, 0x00 }.Concat(Enumerable.Repeat((byte)'c', 65536))));

            var emptyTableResp = (byte[])db.ScriptEvaluate("return cmsgpack.pack({})");
            ClassicAssert.True(emptyTableResp.SequenceEqual(new byte[] { 0x90 }));

            var smallArray1Resp = (byte[])db.ScriptEvaluate("return cmsgpack.pack({1})");
            ClassicAssert.True(smallArray1Resp.SequenceEqual(new byte[] { 0x91, 0x01 }));

            var smallArray2Resp = (byte[])db.ScriptEvaluate("return cmsgpack.pack({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})");
            ClassicAssert.True(smallArray2Resp.SequenceEqual(new byte[] { 0x9F, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F }));

            var midArray1Resp = (byte[])db.ScriptEvaluate($"return cmsgpack.pack({{{string.Join(", ", Enumerable.Repeat(1, 16))}}})");
            ClassicAssert.True(midArray1Resp.SequenceEqual(new byte[] { 0xDC, 0x00, 0x10 }.Concat(Enumerable.Repeat((byte)0x01, 16))));

            var midArray2Resp = (byte[])db.ScriptEvaluate($"return cmsgpack.pack({{{string.Join(", ", Enumerable.Repeat(2, ushort.MaxValue))}}})");
            ClassicAssert.True(midArray2Resp.SequenceEqual(new byte[] { 0xDC, 0xFF, 0xFF }.Concat(Enumerable.Repeat((byte)0x02, ushort.MaxValue))));

            var bigArrayResp = (byte[])db.ScriptEvaluate($"return cmsgpack.pack({{{string.Join(", ", Enumerable.Repeat(3, ushort.MaxValue + 1))}}})");
            ClassicAssert.True(bigArrayResp.SequenceEqual(new byte[] { 0xDD, 0x00, 0x01, 0x00, 0x00 }.Concat(Enumerable.Repeat((byte)0x03, ushort.MaxValue + 1))));

            var smallMap1Resp = (byte[])db.ScriptEvaluate("return cmsgpack.pack({a=1})");
            ClassicAssert.True(smallMap1Resp.SequenceEqual(new byte[] { 0x81, 0xA1, 0x61, 0x01 }));

            var smallMap2Resp = (byte[])db.ScriptEvaluate("return cmsgpack.pack({a=1,b=2,c=3,d=4,e=5,f=6,g=7,h=8,i=9,j=10,k=11,l=12,m=13,n=14,o=15})");
            ClassicAssert.AreEqual(46, smallMap2Resp.Length);
            ClassicAssert.AreEqual(0x8F, smallMap2Resp[0]);
            ClassicAssert.AreNotEqual(-1, smallMap2Resp.AsSpan().IndexOf(new byte[] { 0xA1, 0x61, 0x01 }));
            ClassicAssert.AreNotEqual(-1, smallMap2Resp.AsSpan().IndexOf(new byte[] { 0xA1, 0x62, 0x02 }));
            ClassicAssert.AreNotEqual(-1, smallMap2Resp.AsSpan().IndexOf(new byte[] { 0xA1, 0x63, 0x03 }));
            ClassicAssert.AreNotEqual(-1, smallMap2Resp.AsSpan().IndexOf(new byte[] { 0xA1, 0x64, 0x04 }));
            ClassicAssert.AreNotEqual(-1, smallMap2Resp.AsSpan().IndexOf(new byte[] { 0xA1, 0x65, 0x05 }));
            ClassicAssert.AreNotEqual(-1, smallMap2Resp.AsSpan().IndexOf(new byte[] { 0xA1, 0x66, 0x06 }));
            ClassicAssert.AreNotEqual(-1, smallMap2Resp.AsSpan().IndexOf(new byte[] { 0xA1, 0x67, 0x07 }));
            ClassicAssert.AreNotEqual(-1, smallMap2Resp.AsSpan().IndexOf(new byte[] { 0xA1, 0x68, 0x08 }));
            ClassicAssert.AreNotEqual(-1, smallMap2Resp.AsSpan().IndexOf(new byte[] { 0xA1, 0x69, 0x09 }));
            ClassicAssert.AreNotEqual(-1, smallMap2Resp.AsSpan().IndexOf(new byte[] { 0xA1, 0x6A, 0x0A }));
            ClassicAssert.AreNotEqual(-1, smallMap2Resp.AsSpan().IndexOf(new byte[] { 0xA1, 0x6B, 0x0B }));
            ClassicAssert.AreNotEqual(-1, smallMap2Resp.AsSpan().IndexOf(new byte[] { 0xA1, 0x6C, 0x0C }));
            ClassicAssert.AreNotEqual(-1, smallMap2Resp.AsSpan().IndexOf(new byte[] { 0xA1, 0x6D, 0x0D }));
            ClassicAssert.AreNotEqual(-1, smallMap2Resp.AsSpan().IndexOf(new byte[] { 0xA1, 0x6E, 0x0E }));
            ClassicAssert.AreNotEqual(-1, smallMap2Resp.AsSpan().IndexOf(new byte[] { 0xA1, 0x6F, 0x0F }));

            var midKeys = string.Join(", ", Enumerable.Range(0, 16).Select(static x => $"m_{(char)('A' + x)}={x}"));
            var midMapResp = (byte[])db.ScriptEvaluate($"return cmsgpack.pack({{ {midKeys} }})");
            ClassicAssert.AreEqual(83, midMapResp.Length);
            ClassicAssert.AreEqual(0xDE, midMapResp[0]);
            ClassicAssert.AreEqual(0, midMapResp[1]);
            ClassicAssert.AreEqual(16, midMapResp[2]);
            for (var val = 0; val < 16; val++)
            {
                var keyPart = (byte)('A' + val);
                var expected = new byte[] { 0xA3, (byte)'m', (byte)'_', keyPart, (byte)val };
                ClassicAssert.AreNotEqual(-1, midMapResp.AsSpan().IndexOf(expected));
            }

            var bigKeys = string.Join(", ", Enumerable.Range(0, ushort.MaxValue + 1).Select(static x => $"f_{x:X4}=4"));
            var bigMapResp = (byte[])db.ScriptEvaluate($"return cmsgpack.pack({{ {bigKeys} }})");
            ClassicAssert.AreEqual(524_293, bigMapResp.Length);
            ClassicAssert.AreEqual(0xDF, bigMapResp[0]);
            ClassicAssert.AreEqual(0, bigMapResp[1]);
            ClassicAssert.AreEqual(1, bigMapResp[2]);
            ClassicAssert.AreEqual(0, bigMapResp[3]);
            ClassicAssert.AreEqual(0, bigMapResp[4]);
            for (var val = 0; val <= ushort.MaxValue; val++)
            {
                var keyStr = val.ToString("X4");

                var expected = new byte[] { 0xA6, (byte)'f', (byte)'_', (byte)keyStr[0], (byte)keyStr[1], (byte)keyStr[2], (byte)keyStr[3], 4 };
                ClassicAssert.AreNotEqual(-1, bigMapResp.AsSpan().IndexOf(expected));
            }

            var complexResp = (byte[])db.ScriptEvaluate("return cmsgpack.pack({4, { key='value', arr={5, 6} }, true, 1.23})");
            ClassicAssert.AreEqual(30, complexResp.Length);
            ClassicAssert.AreEqual(0x94, complexResp[0]);
            ClassicAssert.AreEqual(0x04, complexResp[1]);
            var complexNestedMap = complexResp.AsSpan().Slice(2, 18);
            ClassicAssert.AreEqual(0x82, complexNestedMap[0]);
            complexNestedMap = complexNestedMap[1..];
            ClassicAssert.AreNotEqual(-1, complexNestedMap.IndexOf(new byte[] { 0b1010_0011, (byte)'k', (byte)'e', (byte)'y', 0b1010_0101, (byte)'v', (byte)'a', (byte)'l', (byte)'u', (byte)'e' }));
            ClassicAssert.AreNotEqual(-1, complexNestedMap.IndexOf(new byte[] { 0b1010_0011, (byte)'a', (byte)'r', (byte)'r', 0b1001_0010, 0x05, 0x06 }));
            ClassicAssert.AreEqual(0xC3, complexResp[20]);
            ClassicAssert.AreEqual(0xCB, complexResp[21]);
            ClassicAssert.AreEqual(1.23, BinaryPrimitives.ReadDoubleBigEndian(complexResp.AsSpan()[22..]));

            var concatedResp = (byte[])db.ScriptEvaluate("return cmsgpack.pack(1, 2, 3, 4, {5})");
            ClassicAssert.True(concatedResp.SequenceEqual(new byte[] { 0x01, 0x02, 0x03, 0x04, 0b1001_0001, 0x05 }));

            // Rather than an error, Redis converts a too deeply nested object into a null (very strange)
            //
            // We match that behavior

            var infiniteNestMap = (byte[])db.ScriptEvaluate("local a = {}; a.ref = a; return cmsgpack.pack(a)");
            ClassicAssert.True(infiniteNestMap.SequenceEqual(new byte[] { 0x81, 0xA3, 0x72, 0x65, 0x66, 0x81, 0xA3, 0x72, 0x65, 0x66, 0x81, 0xA3, 0x72, 0x65, 0x66, 0x81, 0xA3, 0x72, 0x65, 0x66, 0x81, 0xA3, 0x72, 0x65, 0x66, 0x81, 0xA3, 0x72, 0x65, 0x66, 0x81, 0xA3, 0x72, 0x65, 0x66, 0x81, 0xA3, 0x72, 0x65, 0x66, 0x81, 0xA3, 0x72, 0x65, 0x66, 0x81, 0xA3, 0x72, 0x65, 0x66, 0x81, 0xA3, 0x72, 0x65, 0x66, 0x81, 0xA3, 0x72, 0x65, 0x66, 0x81, 0xA3, 0x72, 0x65, 0x66, 0x81, 0xA3, 0x72, 0x65, 0x66, 0x81, 0xA3, 0x72, 0x65, 0x66, 0x81, 0xA3, 0x72, 0x65, 0x66, 0xC0 }));

            var infiniteNestArr = (byte[])db.ScriptEvaluate("local a = {}; a[1] = a; return cmsgpack.pack(a)");
            ClassicAssert.True(infiniteNestArr.SequenceEqual(new byte[] { 0x91, 0x91, 0x91, 0x91, 0x91, 0x91, 0x91, 0x91, 0x91, 0x91, 0x91, 0x91, 0x91, 0x91, 0x91, 0x91, 0xC0 }));
        }

        [Test]
        public void CMsgPackUnpack()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var noArgExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return cmsgpack.unpack()"));
            ClassicAssert.True(noArgExc.Message.Contains("bad argument") && noArgExc.Message.Contains("unpack"));

            // Multiple arguments are allowed, but ignored

            // Table ends before it should
            var badDataExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return cmsgpack.unpack('\\220\\0\\96')"));
            ClassicAssert.True(badDataExc.Message.Contains("Missing bytes in input"));

            var nullResp = (string)db.ScriptEvaluate($"return type(cmsgpack.unpack({ToLuaString(0xC0)}))");
            ClassicAssert.AreEqual("nil", nullResp);

            var trueResp = (string)db.ScriptEvaluate($"return type(cmsgpack.unpack({ToLuaString(0xC3)}))");
            ClassicAssert.AreEqual("boolean", trueResp);

            var falseResp = (string)db.ScriptEvaluate($"return type(cmsgpack.unpack({ToLuaString(0xC2)}))");
            ClassicAssert.AreEqual("boolean", falseResp);

            var tinyUInt1Resp = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0x00)})");
            ClassicAssert.AreEqual(0, tinyUInt1Resp);

            var tinyUInt2Resp = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0x7F)})");
            ClassicAssert.AreEqual(127, tinyUInt2Resp);

            var tinyInt1Resp = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0xFF)})");
            ClassicAssert.AreEqual(-1, tinyInt1Resp);

            var tinyInt2Resp = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0xE0)})");
            ClassicAssert.AreEqual(-32, tinyInt2Resp);

            var smallUInt1Resp = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0xCC, 0x80)})");
            ClassicAssert.AreEqual(128, smallUInt1Resp);

            var smallUInt2Resp = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0xCC, 0xFF)})");
            ClassicAssert.AreEqual(255, smallUInt2Resp);

            var smallInt1Resp = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0xD0, 0xDF)})");
            ClassicAssert.AreEqual(-33, smallInt1Resp);

            var smallInt2Resp = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0xD0, 0x80)})");
            ClassicAssert.AreEqual(-128, smallInt2Resp);

            var midUInt1Resp = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0xCD, 0x80, 0x00)})");
            ClassicAssert.AreEqual(32768, midUInt1Resp);

            var midUInt2Resp = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0xCD, 0xFF, 0xFF)})");
            ClassicAssert.AreEqual(65535, midUInt2Resp);

            var midInt1Resp = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0xD1, 0xFF, 0x7F)})");
            ClassicAssert.AreEqual(-129, midInt1Resp);

            var midInt2Resp = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0xD1, 0x80, 0x00)})");
            ClassicAssert.AreEqual(-32768, midInt2Resp);

            var uint1Resp = (long)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0xCE, 0x80, 0x00, 0x00, 0x00)})");
            ClassicAssert.AreEqual(2147483648, uint1Resp);

            var uint2Resp = (long)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0xCE, 0xFF, 0xFF, 0xFF, 0xFF)})");
            ClassicAssert.AreEqual(4294967295L, uint2Resp);

            var int1Resp = (long)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0xD2, 0xFF, 0xFF, 0x7F, 0xFF)})");
            ClassicAssert.AreEqual(-32769, int1Resp);

            var int2Resp = (long)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0xD2, 0x80, 0x00, 0x00, 0x00)})");
            ClassicAssert.AreEqual(-2147483648, int2Resp);

            var bigUIntResp = (long)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0xCF, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00)})");
            ClassicAssert.AreEqual(4294967296L, bigUIntResp);

            var bigIntResp = (long)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0xD3, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F, 0xFF, 0xFF, 0xFF)})");
            ClassicAssert.AreEqual(-2147483649L, bigIntResp);

            var floatResp = (string)db.ScriptEvaluate($"return tostring(cmsgpack.unpack({ToLuaString(0xCB, 0x3F, 0xB9, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9A)}))");
            ClassicAssert.AreEqual("0.1", floatResp);

            var tinyString1Resp = (string)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0xA0)})");
            ClassicAssert.AreEqual("", tinyString1Resp);

            var tinyString2Resp = (string)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(new byte[] { 0xBF }.Concat(Enumerable.Repeat((byte)'0', 31)).ToArray())})");
            ClassicAssert.AreEqual(new string('0', 31), tinyString2Resp);

            var shortString1Resp = (string)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(new byte[] { 0xD9, 0x20 }.Concat(Enumerable.Repeat((byte)'a', 32)).ToArray())})");
            ClassicAssert.AreEqual(new string('a', 32), shortString1Resp);

            var shortString2Resp = (string)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(new byte[] { 0xD9, 0xFF }.Concat(Enumerable.Repeat((byte)'a', 255)).ToArray())})");
            ClassicAssert.AreEqual(new string('a', 255), shortString2Resp);

            var midString1Resp = (string)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(new byte[] { 0xDA, 0x01, 0x00 }.Concat(Enumerable.Repeat((byte)'b', 256)).ToArray())})");
            ClassicAssert.AreEqual(new string('b', 256), midString1Resp);

            var midString2Resp = (string)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(new byte[] { 0xDA, 0xFF, 0xFF }.Concat(Enumerable.Repeat((byte)'b', 65535)).ToArray())})");
            ClassicAssert.AreEqual(new string('b', 65535), midString2Resp);

            var longStringResp = (string)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(new byte[] { 0xDB, 0x00, 0x01, 0x00, 0x00 }.Concat(Enumerable.Repeat((byte)'c', 65536)).ToArray())})");
            ClassicAssert.AreEqual(new string('c', 65536), longStringResp);

            var emptyTableResp = (int)db.ScriptEvaluate($"return #cmsgpack.unpack({ToLuaString(0x90)})");
            ClassicAssert.AreEqual(0, emptyTableResp);

            var smallArray1Resp = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0x91, 0x01)})[1]");
            ClassicAssert.AreEqual(1, smallArray1Resp);

            var smallArray2Resp = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0x9F, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F)})[14]");
            ClassicAssert.AreEqual(14, smallArray2Resp);

            var midArray1Resp = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(new byte[] { 0xDC, 0x00, 0x10 }.Concat(Enumerable.Repeat((byte)0x01, 16)).ToArray())})[16]");
            ClassicAssert.AreEqual(1, midArray1Resp);

            var midArray2Resp = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(new byte[] { 0xDC, 0xFF, 0xFF }.Concat(Enumerable.Repeat((byte)0x02, ushort.MaxValue)).ToArray())})[1000]");
            ClassicAssert.AreEqual(2, midArray2Resp);

            var bigArrayResp = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(new byte[] { 0xDD, 0x00, 0x01, 0x00, 0x00 }.Concat(Enumerable.Repeat((byte)0x03, ushort.MaxValue + 1)).ToArray())})[20000]");
            ClassicAssert.AreEqual(3, bigArrayResp);

            var smallMap1Resp = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0x81, 0xA1, 0x61, 0x01)}).a");
            ClassicAssert.AreEqual(1, smallMap1Resp);

            var smallMap2Resp = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0x8F, 0xA1, 0x61, 0x01, 0xA1, 0x62, 0x02, 0xA1, 0x63, 0x03, 0xA1, 0x64, 0x04, 0xA1, 0x65, 0x05, 0xA1, 0x66, 0x06, 0xA1, 0x67, 0x07, 0xA1, 0x68, 0x08, 0xA1, 0x69, 0x09, 0xA1, 0x6A, 0x0A, 0xA1, 0x6B, 0x0B, 0xA1, 0x6C, 0x0C, 0xA1, 0x6D, 0x0D, 0xA1, 0x6E, 0x0E, 0xA1, 0x6F, 0x0F)}).o");
            ClassicAssert.AreEqual(15, smallMap2Resp);

            var midMapBytes = new List<byte> { 0xDE, 0x00, 0x10 };
            for (var val = 0; val < 16; val++)
            {
                var keyPart = (byte)('A' + val);
                midMapBytes.AddRange([0xA3, (byte)'m', (byte)'_', keyPart, (byte)val]);
            }
            var midMapResp = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(midMapBytes.ToArray())}).m_F");
            ClassicAssert.AreEqual(5, midMapResp);

            var bigMapBytes = new List<byte> { 0xDF, 0x00, 0x01, 0x00, 0x00 };
            for (var val = 0; val <= ushort.MaxValue; val++)
            {
                var keyStr = val.ToString("X4");

                bigMapBytes.AddRange([0xA6, (byte)'f', (byte)'_', (byte)keyStr[0], (byte)keyStr[1], (byte)keyStr[2], (byte)keyStr[3], 4]);
            }
            var bigMapRes = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(bigMapBytes.ToArray())}).f_0123");
            ClassicAssert.AreEqual(4, bigMapRes);

            var nestedResp = (int)db.ScriptEvaluate($"return cmsgpack.unpack({ToLuaString(0x94, 0x04, 0x82, 0b1010_0011, (byte)'k', (byte)'e', (byte)'y', 0b1010_0101, (byte)'v', (byte)'a', (byte)'l', (byte)'u', (byte)'e', 0b1010_0011, (byte)'a', (byte)'r', (byte)'r', 0b1001_0010, 0x05, 0x06, 0xC3, 0xCB, 0x3F, 0xF3, 0xAE, 0x14, 0x7A, 0xE1, 0x47, 0xAE)})[2].arr[2]");
            ClassicAssert.AreEqual(6, nestedResp);

            var multiResp = (int[])db.ScriptEvaluate($"local a, b, c, d, e = cmsgpack.unpack({ToLuaString(0x01, 0x02, 0x03, 0x04, 0b1001_0001, 0x05)}); return {{e[1], d, c, b, a}}");
            ClassicAssert.True(multiResp.SequenceEqual([5, 4, 3, 2, 1]));

            // Helper for encoding a byte array into something that can be passed to Lua
            static string ToLuaString(params byte[] data)
            {
                var ret = new StringBuilder();
                _ = ret.Append('\'');

                foreach (var b in data)
                {
                    _ = ret.Append($"\\{b}");
                }

                _ = ret.Append('\'');

                return ret.ToString();
            }
        }

        [Test]
        public void Struct()
        {
            // Redis struct.pack/unpack/size is a subset of Lua's string.pack/unpack/packsize; so just testing for basic functionality
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var packRes = (byte[])db.ScriptEvaluate("return struct.pack('HH', 1, 2)");
            ClassicAssert.True(packRes.SequenceEqual(new byte[] { 0x01, 0x00, 0x02, 0x00 }));

            var unpackRes = (int[])db.ScriptEvaluate("return { struct.unpack('HH', '\\01\\00\\02\\00') }");
            ClassicAssert.True(unpackRes.SequenceEqual([1, 2, 5]));

            var sizeRes = (int)db.ScriptEvaluate("return struct.size('HH')");
            ClassicAssert.AreEqual(4, sizeRes);
        }

        [Test]
        public void StructPack()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var packEmptyStringRes = db.ScriptEvaluate("return struct.pack('!8')");
            ClassicAssert.AreEqual(string.Empty, (string)packEmptyStringRes);

            var packStringRes = (byte[])db.ScriptEvaluate("return struct.pack('!8c5', 'hello')");
            ClassicAssert.True(packStringRes.SequenceEqual(new byte[] { 0x68, 0x65, 0x6C, 0x6C, 0x6F }));

            var packBigEndianRes = (byte[])db.ScriptEvaluate("return struct.pack('>hB', 0x1234, 0x56)");
            ClassicAssert.True(packBigEndianRes.SequenceEqual(new byte[] { 0x12, 0x34, 0x56 }));

            var packLittleEndianRes = (byte[])db.ScriptEvaluate("return struct.pack('<hB', 0x1234, 0x56)");
            ClassicAssert.True(packLittleEndianRes.SequenceEqual(new byte[] { 0x34, 0x12, 0x56 }));

            var packAlignmentRes = (byte[])db.ScriptEvaluate("return struct.pack('!4c1i', 'Z', 0x11223344)");
            ClassicAssert.True(packAlignmentRes.SequenceEqual(new byte[] { 0x5A, 0x00, 0x00, 0x00, 0x44, 0x33, 0x22, 0x11 }));

            var packPaddingRes = (byte[])db.ScriptEvaluate("return struct.pack('BxB', 0x12, 0x34)");
            ClassicAssert.True(packPaddingRes.SequenceEqual(new byte[] { 0x12, 0x00, 0x34 }));

            var packSignedByteRes = (byte[])db.ScriptEvaluate("return struct.pack('b', -2)");
            ClassicAssert.True(packSignedByteRes.SequenceEqual(new byte[] { 0xFE }));

            var packUnsignedByteRes = (byte[])db.ScriptEvaluate("return struct.pack('B', 225)");
            ClassicAssert.True(packUnsignedByteRes.SequenceEqual(new byte[] { 0xE1 }));

            var packSignedShortRes = (byte[])db.ScriptEvaluate("return struct.pack('h', -12345)");
            ClassicAssert.True(packSignedShortRes.SequenceEqual(new byte[] { 0xC7, 0xCF }));

            var packUnsignedShortRes = (byte[])db.ScriptEvaluate("return struct.pack('H', 54321)");
            ClassicAssert.True(packUnsignedShortRes.SequenceEqual(new byte[] { 0x31, 0xD4 }));

            var packSignedLongRes = (byte[])db.ScriptEvaluate("return struct.pack('l', -100000)");
            ClassicAssert.True(packSignedLongRes.SequenceEqual(new byte[] { 0x60, 0x79, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF }));

            var packUnsignedLongRes = (byte[])db.ScriptEvaluate("return struct.pack('L', 100000)");
            ClassicAssert.True(packUnsignedLongRes.SequenceEqual(new byte[] { 0xA0, 0x86, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00 }));

            var packSizeTRes = (byte[])db.ScriptEvaluate("return struct.pack('T', 1234)");
            ClassicAssert.True(packSizeTRes.SequenceEqual(new byte[] { 0xD2, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }));

            var packSignedIntegerRes = (byte[])db.ScriptEvaluate("return struct.pack('i4', -99)");
            ClassicAssert.True(packSignedIntegerRes.SequenceEqual(new byte[] { 0x9D, 0xFF, 0xFF, 0xFF }));

            var packUnsignedIntegerRes = (byte[])db.ScriptEvaluate("return struct.pack('I2', 512)");
            ClassicAssert.True(packUnsignedIntegerRes.SequenceEqual(new byte[] { 0x00, 0x02 }));

            var packNCharsRes = (byte[])db.ScriptEvaluate("return struct.pack('c5', 'hello')");
            ClassicAssert.True(packNCharsRes.SequenceEqual(new byte[] { 0x68, 0x65, 0x6C, 0x6C, 0x6F }));

            var packMoreThanSizeCharsRes = (byte[])db.ScriptEvaluate("return struct.pack('c5', 'helloWorld')");
            ClassicAssert.True(packMoreThanSizeCharsRes.SequenceEqual(new byte[] { 0x68, 0x65, 0x6C, 0x6C, 0x6F }));

            var packWholeStringRes = (byte[])db.ScriptEvaluate("return struct.pack('c0', 'dynamic')");
            ClassicAssert.True(packWholeStringRes.SequenceEqual(new byte[] { 0x64, 0x79, 0x6E, 0x61, 0x6D, 0x69, 0x63 }));

            var packZeroTerminatedStringByteRes = (byte[])db.ScriptEvaluate("return struct.pack('s', 'hi')");
            ClassicAssert.True(packZeroTerminatedStringByteRes.SequenceEqual(new byte[] { 0x68, 0x69, 0x00 }));

            var packFloatRes = (byte[])db.ScriptEvaluate("return struct.pack('f', 3.14)");
            ClassicAssert.True(packFloatRes.SequenceEqual(new byte[] { 0xC3, 0xF5, 0x48, 0x40 }));

            var packBigEndianFloatRes = (byte[])db.ScriptEvaluate("return struct.pack('>f', 3.14)");
            ClassicAssert.True(packBigEndianFloatRes.SequenceEqual(new byte[] { 0x40, 0x48, 0xF5, 0xC3 }));

            var packLittleEndianFloatRes = (byte[])db.ScriptEvaluate("return struct.pack('<f', 3.14)");
            ClassicAssert.True(packLittleEndianFloatRes.SequenceEqual(new byte[] { 0xC3, 0xF5, 0x48, 0x40 }));

            var packDoubleRes = (byte[])db.ScriptEvaluate("return struct.pack('d', 3.14)");
            ClassicAssert.True(packDoubleRes.SequenceEqual(new byte[] { 0x1F, 0x85, 0xEB, 0x51, 0xB8, 0x1E, 0x09, 0x40 }));

            var packBigEndianDoubleRes = (byte[])db.ScriptEvaluate("return struct.pack('>d', 3.14)");
            ClassicAssert.True(packBigEndianDoubleRes.SequenceEqual(new byte[] { 0x40, 0x09, 0x1E, 0xB8, 0x51, 0xEB, 0x85, 0x1F }));

            var packLittleEndianDoubleRes = (byte[])db.ScriptEvaluate("return struct.pack('<d', 3.14)");
            ClassicAssert.True(packLittleEndianDoubleRes.SequenceEqual(new byte[] { 0x1F, 0x85, 0xEB, 0x51, 0xB8, 0x1E, 0x09, 0x40 }));

            var packSpaceAsIgnoredRes = (byte[])db.ScriptEvaluate("return struct.pack('B B', 1, 2)");
            ClassicAssert.True(packSpaceAsIgnoredRes.SequenceEqual(new byte[] { 0x01, 0x02 }));

            // Complex cases
            var packABIRes = (byte[])db.ScriptEvaluate("return struct.pack('dLc0', 0, 6, 'value1')");
            ClassicAssert.True(packABIRes.SequenceEqual(new byte[] {
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // double 0.0
                0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ulong 6
                0x76, 0x61, 0x6C, 0x75, 0x65, 0x31              // "value" + null-terminated
             }));

            var packIBCRes = (byte[])db.ScriptEvaluate("return struct.pack('i4sBc0', 42, 'test', 0x01, 'done')");
            ClassicAssert.True(packIBCRes.SequenceEqual(new byte[] {
                0x2A, 0x00, 0x00, 0x00,
                0x74, 0x65, 0x73, 0x74, 0x00,
                0x01,
                0x64, 0x6F, 0x6E, 0x65
            }));

            var packDLHRes = (byte[])db.ScriptEvaluate("return struct.pack('dLh', 1.5, 0x12345678ABCDEF00, 256)");
            ClassicAssert.True(packDLHRes.SequenceEqual(new byte[] {
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF8, 0x3F, // double 1.5 (little-endian)
                0x00, 0xEF, 0xCD, 0xAB, 0x78, 0x56, 0x34, 0x12, // ulong 0x12345678ABCDEF00
                0x00, 0x01                                     // int16: 256
            }));

            var packCC0Res = (byte[])db.ScriptEvaluate("return struct.pack('scc0', 'hello', 'A', 'B')");
            ClassicAssert.True(packCC0Res.SequenceEqual(new byte[] {
                0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x00,
                0x41,
                0x42
            }));

            var packIHRes = (byte[])db.ScriptEvaluate("return struct.pack('>ih', 0x01020304, 0x0506)");
            ClassicAssert.True(packIHRes.SequenceEqual(new byte[] {
                0x01, 0x02, 0x03, 0x04, // int32 big-endian
                0x05, 0x06              // int16 big-endian
            }));

            var packFEDC0Res = (byte[])db.ScriptEvaluate("return struct.pack('fdc0', 3.14, 2.71828, 'Z')");
            ClassicAssert.True(packFEDC0Res.SequenceEqual(new byte[] {
                0xC3, 0xF5, 0x48, 0x40,             // float 3.14
                0x90, 0xF7, 0xAA, 0x95, 0x09, 0xBF, 0x05, 0x40, // double 2.71828
                0x5A                            // "Z"
            }));
        }

        [Test]
        public void StructPackOnErrors()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            // Not power of two
            var excNotPowerOfTwoArgs = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.pack('!9')"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to format", excNotPowerOfTwoArgs.Message);

            // Invalid format
            var excFormat = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.pack(123, 123)"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to pack", excFormat.Message);

            // Format opt size exceed max int size 32
            var excOptSize = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.pack('I64', 512)"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to format", excOptSize.Message);

            // Format opt size integeral size overflow
            var excOptSizeOverflow = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.pack('I2147483648', 512)"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to format", excOptSizeOverflow.Message);

            // String too short, expected at least {size} bytes but got {l}
            var excArgPack = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.pack('c6', 'hello')"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to pack", excArgPack.Message);

            // Invalid Control Options
            var excControlOptions = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.pack('@I', 'hello')"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to format", excControlOptions.Message);

            // Invalid alignment
            var excBadAlignment = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.pack('!9c1', 'Z')"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to format", excBadAlignment.Message);
        }

        [Test]
        public void StructUnpack()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var unpackDC0Res = db.ScriptEvaluate("return { struct.unpack('dc0', '\\031\\133\\235\\081\\184\\030\\009\\064dynamic') }");
            ClassicAssert.AreEqual("dyn", (string)unpackDC0Res[0]);
            ClassicAssert.AreEqual(12, (long)unpackDC0Res[1]);

            var unpackHRes = (int[])db.ScriptEvaluate("return { struct.unpack('HH', '\\001\\000\\002\\000') }");
            ClassicAssert.True(unpackHRes.SequenceEqual([1, 2, 5]));

            var unpackCharactersRes = db.ScriptEvaluate("return { struct.unpack('c5', 'hello') }");
            ClassicAssert.AreEqual("hello", (string)unpackCharactersRes[0]);

            var unpackBigEndianRes = (int[])db.ScriptEvaluate("return { struct.unpack('>hB', '\\052\\018\\086') }");
            ClassicAssert.True(unpackBigEndianRes.SequenceEqual([0x3412, 0x56, 4]));

            var unpackLittleEndianRes = (int[])db.ScriptEvaluate("return { struct.unpack('<hB', '\\052\\018\\086') }");
            ClassicAssert.True(unpackLittleEndianRes.SequenceEqual([0x1234, 0x56, 4]));

            var unpackAlignmentRes = db.ScriptEvaluate("return { struct.unpack('!4c1i', '\\090\\000\\000\\000\\068\\051\\034\\017') }");
            ClassicAssert.AreEqual("Z", (string)unpackAlignmentRes[0]);
            ClassicAssert.AreEqual(0x11223344, (long)unpackAlignmentRes[1]);
            ClassicAssert.AreEqual(9, (long)unpackAlignmentRes[2]);

            var unpackPaddingRes = (int[])db.ScriptEvaluate("return { struct.unpack('BxB', '\\018\\000\\052') }");
            ClassicAssert.True(unpackPaddingRes.SequenceEqual([0x12, 0x34, 4]));

            var unpackSignedByteRes = (int[])db.ScriptEvaluate("return { struct.unpack('b', '\\254') }");
            ClassicAssert.True(unpackSignedByteRes.SequenceEqual([-2, 2]));

            var unpackUnsignedByteRes = (int[])db.ScriptEvaluate("return { struct.unpack('B', '\\225') }");
            ClassicAssert.True(unpackUnsignedByteRes.SequenceEqual([225, 2]));

            var unpackSignedShortRes = (int[])db.ScriptEvaluate("return { struct.unpack('h', '\\199\\207') }");
            ClassicAssert.True(unpackSignedShortRes.SequenceEqual([-12345, 3]));

            var unpackUnsignedShortRes = (int[])db.ScriptEvaluate("return { struct.unpack('H', '\\049\\212') }");
            ClassicAssert.True(unpackUnsignedShortRes.SequenceEqual([54321, 3]));

            var unpackSignedLongRes = (int[])db.ScriptEvaluate("return { struct.unpack('l', '\\096\\121\\254\\255\\255\\255\\255\\255') }");
            ClassicAssert.True(unpackSignedLongRes.SequenceEqual([-100000, 9]));

            var unpackUnsignedLongRes = (int[])db.ScriptEvaluate("return { struct.unpack('L', '\\160\\134\\001\\000\\000\\000\\000\\000') }");
            ClassicAssert.True(unpackUnsignedLongRes.SequenceEqual([100000, 9]));

            var unpackSizeTRes = (int[])db.ScriptEvaluate("return { struct.unpack('T', '\\210\\004\\000\\000\\000\\000\\000\\000') }");
            ClassicAssert.True(unpackSizeTRes.SequenceEqual([1234, 9]));

            var unpackSignedIntRes = (int[])db.ScriptEvaluate("return { struct.unpack('i4', '\\157\\255\\255\\255') }");
            ClassicAssert.True(unpackSignedIntRes.SequenceEqual([-99, 5]));

            var unpackUnsignedIntRes = (int[])db.ScriptEvaluate("return { struct.unpack('I2', '\\000\\002') }");
            ClassicAssert.True(unpackUnsignedIntRes.SequenceEqual([512, 3]));

            var unpackRes = (int[])db.ScriptEvaluate("return { struct.unpack('>hB', '\\018\\052\\086') }");
            ClassicAssert.True(unpackRes.SequenceEqual([0x1234, 0x56, 4]));

            var unpackBC0StringRes = db.ScriptEvaluate("return { struct.unpack('Bc0', '\\007dynamic') }");
            ClassicAssert.AreEqual("dynamic", (string)unpackBC0StringRes[0]);

            var unpackNullTerminatedStringRes = db.ScriptEvaluate("return { struct.unpack('s', 'hi\\000') }");
            ClassicAssert.AreEqual("hi", (string)unpackNullTerminatedStringRes[0]);

            var unpackFloatRes = db.ScriptEvaluate("return tostring(struct.unpack('f', '\\195\\245\\072\\064'))");
            ClassicAssert.AreEqual(3.14f, float.Parse((string)unpackFloatRes));

            var unpackBigEndianFloatRes = db.ScriptEvaluate("return tostring(struct.unpack('>f', '\\064\\072\\245\\195'))");
            ClassicAssert.AreEqual(3.14f, float.Parse((string)unpackBigEndianFloatRes));

            var unpackLittleEndianFloatRes = db.ScriptEvaluate("return tostring(struct.unpack('<f', '\\195\\245\\072\\064'))");
            ClassicAssert.AreEqual(3.14f, float.Parse((string)unpackLittleEndianFloatRes));

            var unpackDoubleRes = db.ScriptEvaluate("return tostring(struct.unpack('d', '\\031\\133\\235\\081\\184\\030\\009\\064'))");
            ClassicAssert.AreEqual(3.14d, double.Parse((string)unpackDoubleRes));

            var unpackBigEndianDoubleRes = db.ScriptEvaluate("return tostring(struct.unpack('>d', '\\064\\009\\030\\184\\081\\235\\133\\031'))");
            ClassicAssert.AreEqual(3.14d, double.Parse((string)unpackBigEndianDoubleRes));

            var unpackLittleEndianDoubleRes = db.ScriptEvaluate("return tostring(struct.unpack('<d', '\\031\\133\\235\\081\\184\\030\\009\\064'))");
            ClassicAssert.AreEqual(3.14d, double.Parse((string)unpackLittleEndianDoubleRes));

            var packDoubleRes = (byte[])db.ScriptEvaluate("return struct.pack('d', 3.14)");
            ClassicAssert.True(packDoubleRes.SequenceEqual(new byte[] { 0x1F, 0x85, 0xEB, 0x51, 0xB8, 0x1E, 0x09, 0x40 }));

            var packBigEndianDoubleRes = (byte[])db.ScriptEvaluate("return struct.pack('>d', 3.14)");
            ClassicAssert.True(packBigEndianDoubleRes.SequenceEqual(new byte[] { 0x40, 0x09, 0x1E, 0xB8, 0x51, 0xEB, 0x85, 0x1F }));

            var packLittleEndianDoubleRes = (byte[])db.ScriptEvaluate("return struct.pack('<d', 3.14)");
            ClassicAssert.True(packLittleEndianDoubleRes.SequenceEqual(new byte[] { 0x1F, 0x85, 0xEB, 0x51, 0xB8, 0x1E, 0x09, 0x40 }));

            var unpackSpaceRes = (int[])db.ScriptEvaluate("return { struct.unpack('B B', '\\001\\002') }");
            ClassicAssert.True(unpackSpaceRes.SequenceEqual([1, 2, 3]));

            // Complex cases
            var unpackABIRes = db.ScriptEvaluate("return { struct.unpack('dLc0', '\\000\\000\\000\\000\\000\\000\\000\\000\\006\\000\\000\\000\\000\\000\\000\\000\\118\\097\\108\\117\\101\\049') }");
            ClassicAssert.AreEqual(0.0d, (double)unpackABIRes[0]);
            ClassicAssert.AreEqual("value1", (string)unpackABIRes[1]);
            ClassicAssert.AreEqual(23, (int)unpackABIRes[2]);

            // test pos
            var unpackPosRes = (int[])db.ScriptEvaluate("return { struct.unpack('B B', '\\001\\002\\003\\004', 3) }");
            ClassicAssert.True(unpackPosRes.SequenceEqual([3, 4, 5]));

            var unpackSCRes = db.ScriptEvaluate("return { struct.unpack('sc4', 'size\\000dynamic') }");
            ClassicAssert.AreEqual("size", (string)unpackSCRes[0]);
            ClassicAssert.AreEqual("dyna", (string)unpackSCRes[1]);
            ClassicAssert.AreEqual(10, (long)unpackSCRes[2]);

            var unpackZHRes = db.ScriptEvaluate("return { struct.unpack('sH', 'hello\\000\\052\\018') }");
            ClassicAssert.AreEqual("hello", (string)unpackZHRes[0]);
            ClassicAssert.AreEqual(0x1234, (long)unpackZHRes[1]);
            ClassicAssert.AreEqual(9, (long)unpackZHRes[2]);

            var unpackHBRes = (RedisResult[])db.ScriptEvaluate("return { struct.unpack('<hB', '\\254\\255\\127') }");
            ClassicAssert.AreEqual(-2, (long)unpackHBRes[0]);
            ClassicAssert.AreEqual(127, (long)unpackHBRes[1]);
            ClassicAssert.AreEqual(4, (long)unpackHBRes[2]);
        }

        [Test]
        public void StructUnpackOnErrors()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            // Invalid format
            var excFormat = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.unpack(123, 123)"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to unpack", excFormat.Message);

            // Format opt size exceed max int size 32
            var excOptSize = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.unpack('I64', '\\000\\000\\000\\000')"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to format", excOptSize.Message);

            // Format opt size integeral size overflow
            var excOptSizeOverflow = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.unpack('I2147483648', '\\000\\002')"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to format", excOptSizeOverflow.Message);

            // String too short, expected at least {size} bytes but got {l}
            var excArgPack = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.unpack('c6', 'hello')"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to unpack", excArgPack.Message);

            // Invalid Control Options
            var excControlOptions = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.unpack('@I', 'hello')"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to format", excControlOptions.Message);

            // Missing argument
            var excMissingArgs = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.unpack('!8')"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to unpack", excMissingArgs.Message);

            // Invalid number of arguments
            var excBadAlignment = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.unpack('!5', '\\157\\255\\255\\255')"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to format", excBadAlignment.Message);

            // Invalid Third argument
            var excBadThirdArg = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.unpack('@I', 'hello', 'test')"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to unpack", excBadThirdArg.Message);

            // Invalid Third argument
            var excArgTooShort = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.unpack('c4', '\\065\\066')"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to unpack", excArgTooShort.Message);

            // Missing character size
            var excMissingCharacterSize = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.unpack('c0', 'dynamic')"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to unpack", excMissingCharacterSize.Message);

            // Invalid character size
            var excInvalidCharacterZeroSize = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.unpack('sc0', 'size\\000dynamic')"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to unpack", excInvalidCharacterZeroSize.Message);

            // Invalid String without terminator
            var excBadString = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.unpack('s', 'hello')"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to unpack", excBadString.Message);

            // Third pos has to be greater than 0
            var unpackBadPosRes = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.unpack('s', 'hello', 0)"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to unpack", unpackBadPosRes.Message);
        }

        [Test]
        public void StructSize()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var sizeSignedIntegerRes = (int)db.ScriptEvaluate("return struct.size('i')");
            ClassicAssert.AreEqual(4, sizeSignedIntegerRes);

            var sizeUnsignedIntegerRes = (int)db.ScriptEvaluate("return struct.size('I')");
            ClassicAssert.AreEqual(4, sizeUnsignedIntegerRes);

            var sizeSingedLongRes = (int)db.ScriptEvaluate("return struct.size('l')");
            ClassicAssert.AreEqual(8, sizeSingedLongRes);

            var sizeDoubleRes = (int)db.ScriptEvaluate("return struct.size('d')");
            ClassicAssert.AreEqual(8, sizeDoubleRes);

            var sizeFloatRes = (int)db.ScriptEvaluate("return struct.size('f')");
            ClassicAssert.AreEqual(4, sizeFloatRes);

            var sizeBytesRes = (int)db.ScriptEvaluate("return struct.size('c10')");
            ClassicAssert.AreEqual(10, sizeBytesRes);

            var sizeSizeTRes = (int)db.ScriptEvaluate("return struct.size('T')");
            ClassicAssert.AreEqual(8, sizeSizeTRes);

            var sizeRes = (int)db.ScriptEvaluate("return struct.size('HH')");
            ClassicAssert.AreEqual(4, sizeRes);

            var sizeComplexRes = (int)db.ScriptEvaluate("return struct.size('iIc4d')");
            ClassicAssert.AreEqual(20, sizeComplexRes);
        }

        [Test]
        public void StructSizeOnErrors()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var excFormatString = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.size('s')"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to format", excFormatString.Message);

            var excFormatCharacter = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.size('c0')"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to format", excFormatCharacter.Message);

            var excBadControlOptions = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.size('@I')"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to format", excBadControlOptions.Message);

            var excBadFormat = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return struct.size(123)"));
            ClassicAssert.AreEqual("ERR Lua encountered an error: bad argument to format", excBadFormat.Message);
        }

        [Test]
        public void MathFunctions()
        {
            // There are a number of "weird" math functions Redis supports that don't have direct .NET equivalents
            //
            // Doing some basic testing on these implementations
            //
            // We don't actually guarantee bit-for-bit or char-for-char equivalence, but "close" is worth attempting
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            // Frexp
            {
                var a = (string)db.ScriptEvaluate("local a,b = math.frexp(0); return tostring(a)..'|'..tostring(b)");
                ClassicAssert.AreEqual("0|0", a);

                var b = (string)db.ScriptEvaluate("local a,b = math.frexp(1); return tostring(a)..'|'..tostring(b)");
                ClassicAssert.AreEqual("0.5|1", b);

                var c = (string)db.ScriptEvaluate($"local a,b = math.frexp({double.MaxValue}); return tostring(a)..'|'..tostring(b)");
                ClassicAssert.AreEqual("1|1024", c);

                var d = (string)db.ScriptEvaluate($"local a,b = math.frexp({double.MinValue}); return tostring(a)..'|'..tostring(b)");
                ClassicAssert.AreEqual("-1|1024", d);

                var e = (string)db.ScriptEvaluate("local a,b = math.frexp(1234.56); return tostring(a)..'|'..tostring(b)");
                ClassicAssert.AreEqual("0.6028125|11", e);

                var f = (string)db.ScriptEvaluate("local a,b = math.frexp(-7890.12); return tostring(a)..'|'..tostring(b)");
                ClassicAssert.AreEqual("-0.9631494140625|13", f);
            }

            // Ldexp
            {
                var a = (string)db.ScriptEvaluate("return tostring(math.ldexp(0, 0))");
                ClassicAssert.AreEqual("0", a);

                var b = (string)db.ScriptEvaluate("return tostring(math.ldexp(1, 1))");
                ClassicAssert.AreEqual("2", b);

                var c = (string)db.ScriptEvaluate("return tostring(math.ldexp(0, 1))");
                ClassicAssert.AreEqual("0", c);

                var d = (string)db.ScriptEvaluate("return tostring(math.ldexp(1, 0))");
                ClassicAssert.AreEqual("1", d);

                var e = (string)db.ScriptEvaluate($"return tostring(math.ldexp({double.MaxValue}, 0))");
                ClassicAssert.AreEqual("1.7976931348623e+308", e);

                var f = (string)db.ScriptEvaluate($"return tostring(math.ldexp({double.MaxValue}, 1))");
                ClassicAssert.AreEqual("inf", f);

                var g = (string)db.ScriptEvaluate($"return tostring(math.ldexp({double.MinValue}, 0))");
                ClassicAssert.AreEqual("-1.7976931348623e+308", g);

                var h = (string)db.ScriptEvaluate($"return tostring(math.ldexp({double.MinValue}, 1))");
                ClassicAssert.AreEqual("-inf", h);

                var i = (string)db.ScriptEvaluate($"return tostring(math.ldexp(1.234, 1.234))");
                ClassicAssert.AreEqual("2.468", i);

                var j = (string)db.ScriptEvaluate($"return tostring(math.ldexp(-5.6798, 9.0123))");
                ClassicAssert.AreEqual("-2908.0576", j);
            }
        }

        [Test]
        public void Maxn()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var empty = (int)db.ScriptEvaluate("return table.maxn({})");
            ClassicAssert.AreEqual(0, empty);

            var single = (int)db.ScriptEvaluate("return table.maxn({4})");
            ClassicAssert.AreEqual(1, single);

            var multiple = (int)db.ScriptEvaluate("return table.maxn({-1, 1, 2, 5})");
            ClassicAssert.AreEqual(4, multiple);

            var keyed = (int)db.ScriptEvaluate("return table.maxn({foo='bar',fizz='buzz',hello='world'})");
            ClassicAssert.AreEqual(0, keyed);

            var mixed = (int)db.ScriptEvaluate("return table.maxn({-1, 1, foo='bar', 3})");
            ClassicAssert.AreEqual(3, mixed);

            var allNegative = (int)db.ScriptEvaluate("local x = {}; x[-1] = 1; x[-2]=2; return table.maxn(x)");
            ClassicAssert.AreEqual(0, allNegative);
        }

        [Test]
        public void LoadString()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var basic = (int)db.ScriptEvaluate("local x = loadstring('return 123'); return x()");
            ClassicAssert.AreEqual(123, basic);

            var rejectNullExc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("local x = loadstring('return \"\\0\"'); return x()"));
            ClassicAssert.True(rejectNullExc.Message.Contains("bad argument to loadstring, interior null byte"));
        }

        [Test]
        public void Issue1235()
        {
            // See: https://github.com/microsoft/garnet/issues/1235

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            // Underlying issue is that KEYS/ARGV resizing left the stack misaligned
            //
            // In order to trigger the issue, you need enough KEYS to force a re-allocation
            // but without enough ARGVs to do the same.

            var res =
                (string)db.ScriptEvaluate(
                    "return ARGV[3]",
                    ["a", "b", "c", "d", "e", "f",],
                    ["0", "1", "2"]
                );
            ClassicAssert.AreEqual("2", res);
        }

        [Test]
        [Ignore("Long running, disabled by default")]
        public void StressTimeouts()
        {
            // Pseudo-repeatably random
            const int SEED = 2025_01_30_00;

            const int DurationMS = 60 * 60 * 1_000;
            const int ThreadCount = 16;
            const string TimeoutScript = @"
local count = 0

if @Ctrl == 'Timeout' then
    while true do
        count = count + 1
    end
end

return count";

            if (string.IsNullOrEmpty(limitTimeout))
            {
                ClassicAssert.Ignore("No timeout enabled");
                return;
            }

            using var startStress = new SemaphoreSlim(0, ThreadCount);

            var threads = new Thread[ThreadCount];
            for (var i = 0; i < threads.Length; i++)
            {
                using var threadStarted = new SemaphoreSlim(0, 1);

                var rand = new Random(SEED + i);
                threads[i] =
                    new Thread(
                        () =>
                        {
                            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                            var db = redis.GetDatabase();

                            var scriptTimeout = LuaScript.Prepare(TimeoutScript);
                            var loadedScriptTimeout = scriptTimeout.Load(redis.GetServers()[0]);

                            var timeout = new { Ctrl = "Timeout" };
                            var safe = new { Ctrl = "Safe" };

                            _ = threadStarted.Release();

                            startStress.Wait();

                            var sw = Stopwatch.StartNew();
                            while (sw.ElapsedMilliseconds < DurationMS)
                            {
                                var roll = rand.Next(10);
                                if (roll == 0)
                                {
                                    var subRoll = rand.Next(5);
                                    if (subRoll == 0)
                                    {
                                        // Even more rarely, flush the script cache
                                        var res = db.Execute("SCRIPT", "FLUSH");
                                        ClassicAssert.AreEqual("OK", (string)res);
                                    }
                                    else
                                    {
                                        // Periodically cause a timeout
                                        var exc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate(loadedScriptTimeout, timeout));
                                        ClassicAssert.AreEqual("ERR Lua script exceeded configured timeout", exc.Message);
                                    }
                                }
                                else
                                {
                                    // ... but most of time, succeed
                                    var res = db.ScriptEvaluate(loadedScriptTimeout, safe);
                                    ClassicAssert.AreEqual(0, (int)res);
                                }
                            }
                        }
                    )
                    {
                        Name = $"{nameof(StressTimeouts)} #{i}",
                        IsBackground = true,
                    };

                threads[i].Start();
                threadStarted.Wait();
            }

            // Start threads, and wait from them to complete
            _ = startStress.Release(ThreadCount);
            var sw = Stopwatch.StartNew();

            const int FinalTimeout = DurationMS + 60 * 1_000;

            foreach (var thread in threads)
            {
                var timeout = thread.Join((int)(FinalTimeout - sw.ElapsedMilliseconds));
                ClassicAssert.True(timeout, "Thread did not exit in expected time");
            }
        }
    }
}