// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
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
    [TestFixture(LuaMemoryManagementMode.Managed, "16m", "")]
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
                    for (var ii = 0; ii < numIterations; ++ii)
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
                    for (var ii = 0; ii < numIterations; ++ii)
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
            var sha1SetScript = Encoding.ASCII.GetString(lightClientRequest.SendCommand(Encoding.ASCII.GetBytes(stringCmd), 1)).Substring(5, 40);

            ClassicAssert.AreEqual("c686f316aaf1eb01d5a4de1b0b63cd233010e63d", sha1SetScript);
            for (var i = 0; i < 5000; i++)
            {
                var randPostFix = rnd.Next(1, 1000);
                valueKey = $"{valueKey}{randPostFix}";

                var r = lightClientRequest.SendCommand($"EVALSHA {sha1SetScript} 1 {nameKey}{randPostFix} {valueKey}", 1);
                // Check for error reply
                ClassicAssert.IsTrue(r[0] != '-');

                var g = Encoding.ASCII.GetString(lightClientRequest.SendCommand($"get {nameKey}{randPostFix}", 1));
                var fstEndOfLine = g.IndexOf('\n', StringComparison.OrdinalIgnoreCase) + 1;
                var strKeyValue = g.Substring(fstEndOfLine, valueKey.Length);
                ClassicAssert.IsTrue(strKeyValue == valueKey);
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
        public void RedisPCall()
        {
            // This is a temporary fix to address a regression in .NET9, an open issue can be found here - https://github.com/dotnet/runtime/issues/111242
            // Once the issue is resolved the #if can be removed permanently.
#if NET9_0_OR_GREATER
            Assert.Ignore($"Ignoring test when running in .NET9.");
#endif

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
            // This is a temporary fix to address a regression in .NET9, an open issue can be found here - https://github.com/dotnet/runtime/issues/111242
            // Once the issue is resolved the #if can be removed permanently.
#if NET9_0_OR_GREATER
            Assert.Ignore($"Ignoring test when running in .NET9.");
#endif

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
            // This is a temporary fix to address a regression in .NET9, an open issue can be found here - https://github.com/dotnet/runtime/issues/111242
            // Once the issue is resolved the #if can be removed permanently.
#if NET9_0_OR_GREATER
            Assert.Ignore($"Ignoring test when running in .NET9.");
#endif

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

            // More than 1 log line, and non-string values are legal
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
            // This is a temporary fix to address a regression in .NET9, an open issue can be found here - https://github.com/dotnet/runtime/issues/111242
            // Once the issue is resolved the #if can be removed permanently.
#if NET9_0_OR_GREATER
            Assert.Ignore($"Ignoring test when running in .NET9.");
#endif

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
            // This is a temporary fix to address a regression in .NET9, an open issue can be found here - https://github.com/dotnet/runtime/issues/111242
            // Once the issue is resolved the #if can be removed permanently.
#if NET9_0_OR_GREATER
            Assert.Ignore($"Ignoring test when running in .NET9.");
#endif

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

        private void DoErroneousRedisCall(IDatabase db, string[] args, string expectedError)
        {
            var exc = Assert.Throws<RedisServerException>(() => db.ScriptEvaluate($"return redis.call({string.Join(',', args)})"));
            ClassicAssert.IsNotNull(exc);
            StringAssert.StartsWith(expectedError, exc!.Message);
        }

        [Test]
        public void RedisCallErrors()
        {
            // This is a temporary fix to address a regression in .NET9, an open issue can be found here - https://github.com/dotnet/runtime/issues/111242
            // Once the issue is resolved the #if can be removed permanently.
#if NET9_0_OR_GREATER
            Assert.Ignore($"Ignoring test when running in .NET9.");
#endif

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
            // This is a temporary fix to address a regression in .NET9, an open issue can be found here - https://github.com/dotnet/runtime/issues/111242
            // Once the issue is resolved the #if can be removed permanently.
#if NET9_0_OR_GREATER
            Assert.Ignore($"Ignoring test when running in .NET9.");
#endif

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
            // This is a temporary fix to address a regression in .NET9, an open issue can be found here - https://github.com/dotnet/runtime/issues/111242
            // Once the issue is resolved the #if can be removed permanently.
#if NET9_0_OR_GREATER
            Assert.Ignore($"Ignoring test when running in .NET9.");
#endif

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
            // This is a temporary fix to address a regression in .NET9, an open issue can be found here - https://github.com/dotnet/runtime/issues/111242
            // Once the issue is resolved the #if can be removed permanently.
#if NET9_0_OR_GREATER
            Assert.Ignore($"Ignoring test when running in .NET9.");
#endif

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
        [Ignore("Long running, disabled by default")]
        public void StressTimeouts()
        {
            // Psuedo-repeatably random
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