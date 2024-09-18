// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class LuaScriptTests
    {
        protected GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableLua: true);
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
            var script = "local ptable = {100, \"value1\", 200, \"value2\"}; return redis.call('zadd', KEYS[1], ptable)";
            var result = db.ScriptEvaluate(script, [(RedisKey)"mysskey"]);
            ClassicAssert.IsTrue(result.ToString() == "2");

            // Check the length
            var l = db.SortedSetLength("mysskey");
            ClassicAssert.IsTrue(l == 2);

            // Execute same script again
            result = db.ScriptEvaluate(script, [(RedisKey)"mysskey"]);
            ClassicAssert.IsTrue(result.ToString() == "0");

            // Add more pairs
            script = "local ptable = {300, \"value3\", 400, \"value4\"}; return redis.call('zadd', KEYS[1], ptable)";
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
            try
            {
                _ = db.ScriptEvaluate(statusReplyScript);
            }
            catch (RedisServerException ex)
            {
                ClassicAssert.AreEqual(ex.Message, "Failure");
            }
            var directReplyScript = "return { err = 'Failure' }";
            try
            {
                _ = db.ScriptEvaluate(directReplyScript);
            }
            catch (RedisServerException ex)
            {
                ClassicAssert.AreEqual(ex.Message, "Failure");
            }
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

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var response1 = (string[])db.ScriptEvaluate(script1, ["key1", "key2"]);
            ClassicAssert.AreEqual(2, response1.Length);
            foreach (var item in response1)
            {
                ClassicAssert.AreEqual(null, item);
            }
        }
    }
}