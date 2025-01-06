// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    // Limits chosen here to allow completion - if you have to bump them up, consider that you might have introduced a regression
    [TestFixture(LuaMemoryManagementMode.Native, "")]
    [TestFixture(LuaMemoryManagementMode.Tracked, "")]
    [TestFixture(LuaMemoryManagementMode.Tracked, "13m")]
    [TestFixture(LuaMemoryManagementMode.Managed, "")]
    [TestFixture(LuaMemoryManagementMode.Managed, "15m")]
    public class LuaScriptTests
    {
        private readonly LuaMemoryManagementMode allocMode;
        private readonly string limitBytes;

        protected GarnetServer server;

        public LuaScriptTests(LuaMemoryManagementMode allocMode, string limitBytes)
        {
            this.allocMode = allocMode;
            this.limitBytes = limitBytes;
        }

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableLua: true, luaMemoryMode: allocMode, luaMemoryLimit: limitBytes);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
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
            script = "i = redis.call('get', KEYS[1]); i = i + 1; return redis.call('set', KEYS[1], i)";
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
            script = "i = redis.call('get', KEYS[1]); i = i - 1; return redis.call('set', KEYS[1], i)";
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
            var server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");

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

            var server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");
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
            var statusReplyScript = "return redis.error_reply('Failure')";

            var excReply = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate(statusReplyScript));
            ClassicAssert.AreEqual("ERR Failure", excReply.Message);

            var directReplyScript = "return { err = 'Failure' }";
            var excDirect = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate(directReplyScript));
            ClassicAssert.AreEqual("Failure", excDirect.Message);
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
                    ClassicAssert.AreEqual(null, item);
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

        [Test]
        public void RedisCallErrors()
        {
            // Testing that our error replies for redis.call match Redis behavior
            //
            // TODO: exact matching of the hash and line number would also be nice, but that is trickier
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            // No args
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return redis.call()"));
                ClassicAssert.IsTrue(exc.Message.StartsWith("ERR Please specify at least one argument for this redis lib call"));
            }

            // Unknown command
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return redis.call('123')"));
                ClassicAssert.IsTrue(exc.Message.StartsWith("ERR Unknown Redis command called from script"));
            }

            // Bad command type
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return redis.call({ foo = 'bar'})"));
                ClassicAssert.IsTrue(exc.Message.StartsWith("ERR Lua redis lib command arguments must be strings or integers"));
            }

            // GET bad arg type
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return redis.call('GET', { foo = 'bar' })"));
                ClassicAssert.IsTrue(exc.Message.StartsWith("ERR Lua redis lib command arguments must be strings or integers"));
            }

            // SET bad arg types
            {
                var exc1 = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return redis.call('SET', 'hello', { foo = 'bar' })"));
                ClassicAssert.IsTrue(exc1.Message.StartsWith("ERR Lua redis lib command arguments must be strings or integers"));

                var exc2 = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return redis.call('SET', { foo = 'bar' }, 'world')"));
                ClassicAssert.IsTrue(exc2.Message.StartsWith("ERR Lua redis lib command arguments must be strings or integers"));
            }

            // Other bad arg types
            {
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.ScriptEvaluate("return redis.call('DEL', { foo = 'bar' })"));
                ClassicAssert.IsTrue(exc.Message.StartsWith("ERR Lua redis lib command arguments must be strings or integers"));
            }
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
                        tableDepth.Append(", ");
                    }
                    tableDepth.Append("{ " + i + " }");
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
    }
}