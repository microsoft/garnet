﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
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
            Assert.AreEqual("KeyOneValue", retValue);
            Assert.AreEqual("KeyOneValue", response.ToString());
        }

        [Test]
        public void CanDoSimpleEvalHelloSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var values = new RedisValue[] { new("hello") };
            RedisResult response = db.ScriptEvaluate("return ARGV[1]", null, values);
            Assert.AreEqual((string)response, "hello");
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
            Assert.IsTrue(((RedisValue)response).ToString().Contains("KeyOneValue-abc", StringComparison.InvariantCultureIgnoreCase));
        }

        [Test]
        public void CanDoEvalSetGet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var response = db.Execute("EVAL", "return redis.call('SeT', KEYS[1], ARGV[1])", 1, "mykey", "myvalue");
            Assert.AreEqual("OK", (string)response);
            response = db.Execute("EVAL", "return redis.call('get', KEYS[1])", 1, "mykey");
            Assert.AreEqual("myvalue", (string)response);

            // Get numbers
            response = db.Execute("EVAL", "return redis.call('SeT', KEYS[1], ARGV[1])", 1, "mykey", 100);
            Assert.AreEqual("OK", (string)response);
            response = db.Execute("EVAL", "return redis.call('get', KEYS[1])", 1, "mykey");
            Assert.AreEqual(100, Convert.ToInt64(response));
        }

        [Test]
        public void CanDoEvalShaWithIntegerValueKeySE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            string script = "return redis.call('set', KEYS[1], ARGV[1]);";
            var result = db.ScriptEvaluate(script, [(RedisKey)"mykey"], [(RedisValue)1]);
            script = "local i = redis.call('get',KEYS[1]); i = i + 1; redis.call('set', KEYS[1], i); return redis.call('get', KEYS[1]);";
            result = db.ScriptEvaluate(script, [(RedisKey)"mykey"]);
            Assert.AreEqual(2, Convert.ToInt64(result));
        }

        [Test]
        public void CanDoEvalShaWithStringValueKeySE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            string script = "return redis.call('set', KEYS[1], ARGV[1]);";
            var result = db.ScriptEvaluate(script, [(RedisKey)"mykey"], [(RedisValue)"NAME"]);
            script = "local i = redis.call('get', KEYS[1]); i = \"FULL \"..i; redis.call('set', KEYS[1], i); return redis.call('get', KEYS[1]);";
            result = db.ScriptEvaluate(script, [(RedisKey)"mykey"]);
            Assert.IsTrue(((RedisValue)result).ToString() == "FULL NAME");
        }


        [Test]
        public void CanDoEvalShaWithStringValueMultipleKeysSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            string script = "local i = 0; " +
                            "if redis.call('set', KEYS[1], ARGV[1]) == \"OK\" then i = i + 1; end;" +
                            "if (redis.call('set', KEYS[2], ARGV[2]) == \"OK\") then i = i + 1; end;" +
                            "return i;";
            var result = db.ScriptEvaluate(script, [(RedisKey)"mykey", (RedisKey)"myseckey"], [(RedisValue)"NAME", (RedisValue)"mysecvalue"]);
            Assert.IsTrue(((RedisValue)result) == 2);
        }


        [Test]
        public void CanDoEvalShaWithZAddOnePairSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            string script = "return redis.call('zadd', KEYS[1], ARGV[1], ARGV[2])";
            RedisResult result = db.ScriptEvaluate(script, [(RedisKey)"mysskey"], [(RedisValue)1, (RedisValue)"a"]);
            Assert.IsTrue(result.ToString() == "1");
            var l = db.SortedSetLength("mysskey");
            Assert.IsTrue(l == 1);
        }


        [Test]
        public void CanDoEvalShaWithZAddMultiPairSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Create a sorted set
            string script = "local ptable = {100, \"value1\", 200, \"value2\"}; return redis.call('zadd', KEYS[1], ptable)";
            RedisResult result = db.ScriptEvaluate(script, [(RedisKey)"mysskey"]);
            Assert.IsTrue(result.ToString() == "2");

            // Check the length
            var l = db.SortedSetLength("mysskey");
            Assert.IsTrue(l == 2);

            // Execute same script again
            result = db.ScriptEvaluate(script, [(RedisKey)"mysskey"]);
            Assert.IsTrue(result.ToString() == "0");

            // Add more pairs
            script = "local ptable = {300, \"value3\", 400, \"value4\"}; return redis.call('zadd', KEYS[1], ptable)";
            result = db.ScriptEvaluate(script, [(RedisKey)"mysskey"]);
            Assert.IsTrue(result.ToString() == "2");

            // Review
            var r = db.SortedSetRangeByScoreWithScores("mysskey");
            Assert.IsTrue(r[0].Score == 100 && r[0].Element == "value1");
            l = db.SortedSetLength("mysskey");

            Assert.IsTrue(l == 4);
        }

        [Test]
        public void CanDoEvalShaSEMultipleThreads()
        {
            var numThreads = 1;
            Task[] tasks = new Task[numThreads];
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var initialValue = 0;
            var rnd = new Random();
            // create the key
            string script = "redis.call('set',KEYS[1], ARGV[1]); return redis.call('get', KEYS[1]);";
            var result = db.ScriptEvaluate(script, [(RedisKey)"mykey"], [(RedisValue)initialValue]);
            Assert.IsTrue(((RedisValue)result).ToString() == "0");
            script = "i = redis.call('get', KEYS[1]); i = i + 1; return redis.call('set', KEYS[1], i)";
            var numIterations = 10;
            //updates
            for (int i = 0; i < numThreads; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    for (var ii = 0; ii < numIterations; ++ii)
                    {
                        db.ScriptEvaluate(script, [(RedisKey)"mykey"]);
                        await Task.Delay(millisecondsDelay: rnd.Next(10, 50));
                    }
                });
            }
            Task.WaitAll(tasks);
            script = "return redis.call('get', KEYS[1]);";
            result = db.ScriptEvaluate(script, [(RedisKey)"mykey"]);
            Assert.IsTrue(Int32.Parse(((RedisValue)result).ToString()) == numThreads * numIterations);
            script = "i = redis.call('get', KEYS[1]); i = i - 1; return redis.call('set', KEYS[1], i)";
            for (int i = 0; i < numThreads; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    for (var ii = 0; ii < numIterations; ++ii)
                    {
                        db.ScriptEvaluate(script, [(RedisKey)"mykey"]);
                        await Task.Delay(millisecondsDelay: rnd.Next(10, 50));
                    }
                });
            }
            Task.WaitAll(tasks);
            script = "return redis.call('get', KEYS[1]);";
            result = db.ScriptEvaluate(script, [(RedisKey)"mykey"]);
            Assert.IsTrue(Int32.Parse(((RedisValue)result).ToString()) == 0);
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

            string tag = "tag";
            long expiryTimestamp = 100;
            RedisResult result = db.ScriptEvaluate(ZADD_GT, [(RedisKey)"zaddgtkey"], [expiryTimestamp, tag]);
            Assert.IsTrue(result.IsNull);

            // get key and verify timestamp is 100
            var r = db.SortedSetRangeByScoreWithScores("zaddgtkey");
            Assert.IsTrue(r[0].Score == 100 && r[0].Element == "tag");

            expiryTimestamp = 200;
            result = db.ScriptEvaluate(ZADD_GT, [(RedisKey)"zaddgtkey"], [expiryTimestamp, tag]);
            Assert.IsTrue(result.IsNull);

            // get key and verify timestamp is 200
            r = db.SortedSetRangeByScoreWithScores("zaddgtkey");
            Assert.IsTrue(r[0].Score == 200 && r[0].Element == "tag");

            expiryTimestamp = 150;
            result = db.ScriptEvaluate(ZADD_GT, [(RedisKey)"zaddgtkey"], [expiryTimestamp, tag]);
            Assert.IsTrue(result.IsNull);

            // get key and verify timestamp is 200
            r = db.SortedSetRangeByScoreWithScores("zaddgtkey");
            Assert.IsTrue(r[0].Score == 200 && r[0].Element == "tag");
        }

        [Test]
        public void CanDoScriptFlush()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");

            //load a script in the memory
            string script = "return 1;";
            var scriptId = server.ScriptLoad(script);
            var result = db.ScriptEvaluate(scriptId);
            Assert.IsTrue(((RedisValue)result) == "1");

            //flush script memory
            server.ScriptFlush();

            //Assert the script is not found
            Assert.Throws<RedisServerException>(() => db.ScriptEvaluate(scriptId));
        }

        [Test]
        public void CannotScriptUseExtendedEncoding()
        {
            // Important: find a way to throw an exception to detect if there is a different encoding inside the script
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            string extendedCharsScript = "return '僕'";

            var server = redis.GetServer($"{TestUtils.Address}:{TestUtils.Port}");
            var scriptSha = server.ScriptLoad(extendedCharsScript);
            var result = redis.GetDatabase(0).ScriptEvaluate(scriptSha);
            Assert.AreNotEqual("僕", result);
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

            Assert.AreEqual("c686f316aaf1eb01d5a4de1b0b63cd233010e63d", sha1SetScript);
            for (int i = 0; i < 5000; i++)
            {
                var randPostFix = rnd.Next(1, 1000);
                valueKey = $"{valueKey}{randPostFix}";
                var r = lightClientRequest.SendCommand($"EVALSHA {sha1SetScript} 1 {nameKey}{randPostFix} {valueKey}", 1);
                var g = Encoding.ASCII.GetString(lightClientRequest.SendCommand($"get {nameKey}{randPostFix}", 1));
                var fstEndOfLine = g.IndexOf('\n', StringComparison.OrdinalIgnoreCase) + 1;
                var strKeyValue = g.Substring(fstEndOfLine, valueKey.Length);
                Assert.IsTrue(strKeyValue == valueKey);
            }
        }
    }
}