// Copyright (c) Microsoft Corporation.
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
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
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


        [Test]//start here
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
            var rnd = new Random();
            var nameKey = "strKey-";
            var valueKey = "valueKey-";
            using var lightClientRequest = TestUtils.CreateRequest();
            var stringCmd = "*3\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n$40\r\nreturn redis.call('set',KEYS[1],ARGV[1])\r\n";
            var sha1SetScript = Encoding.ASCII.GetString(lightClientRequest.SendCommand(Encoding.ASCII.GetBytes(stringCmd), 1)).Substring(5, 40);

            for (int i = 0; i < 5000; i++)
            {
                var randPostFix = rnd.Next(1, 1000);
                valueKey = $"{valueKey}{randPostFix}";
                var r = lightClientRequest.SendCommand($"EVALSHA {sha1SetScript} 1 {nameKey}{randPostFix} {valueKey}", 1);
                var g = Encoding.ASCII.GetString(lightClientRequest.SendCommand($"get {nameKey}{randPostFix}", 1));
                var fstEndOfLine = g.IndexOf("\n", StringComparison.OrdinalIgnoreCase) + 1;
                var strKeyValue = g.Substring(fstEndOfLine, valueKey.Length);
                Assert.IsTrue(strKeyValue == valueKey);
            }
        }



        [Test]
        public void CanParseKeysAttributes()
        {
            byte[][] keys = new byte[12][];
            keys[0] = Encoding.ASCII.GetBytes("foo:bar:raw");
            keys[1] = Encoding.ASCII.GetBytes("foo:bar:obj:s");
            keys[2] = Encoding.ASCII.GetBytes("foo:bar::s"); // no type, so use raw
            keys[3] = Encoding.ASCII.GetBytes("foo:bar:raw:s");
            keys[4] = Encoding.ASCII.GetBytes("foo:bar:obj:s");
            keys[5] = Encoding.ASCII.GetBytes("foo:bar:obj"); //obj type, exclusive when not specified
            keys[6] = Encoding.ASCII.GetBytes("foo:bar::x"); // no type, so use raw
            keys[7] = Encoding.ASCII.GetBytes("foo:bar::s"); // no type so use raw
            keys[8] = Encoding.ASCII.GetBytes("foo:bar:obj:x");
            keys[9] = Encoding.ASCII.GetBytes("foo"); //<empty> without any :: matched, lock both stores exclusive
            keys[10] = Encoding.ASCII.GetBytes("foo:obj:x");
            keys[11] = Encoding.ASCII.GetBytes("foo::x"); // no type so use raw


            (int nameEnd, bool? raw, bool ex)[] vals = new (int, bool?, bool)[keys.GetLength(0)];
            vals[0] = (6, true, true);
            vals[1] = (6, false, false);
            vals[2] = (6, true, false);
            vals[3] = (6, true, false);
            vals[4] = (6, false, false);
            vals[5] = (6, false, true);
            vals[6] = (6, true, true);
            vals[7] = (6, true, false);
            vals[8] = (6, false, true);
            vals[9] = (2, null, true);
            vals[10] = (2, false, true);
            vals[11] = (2, true, true);


            for (int i = 0; i < keys.GetLength(0); i++)
            {
                var result = ParseAttributes(keys[i]);
                Assert.IsTrue(result.nameEnd == vals[i].nameEnd);
                Assert.IsTrue(result.raw == vals[i].raw);
                Assert.IsTrue(result.ex == vals[i].ex);
            }
        }

        private static (int nameEnd, bool? raw, bool ex) ParseAttributes(byte[] keyName)
        {
            int[] bookmarks = new int[10];
            int totalBookmarks = 0;
            bool? raw = null;
            bool ex = true;
            int nameEnd = 0;

            for (int j = keyName.Length - 1; j >= 0; j--)
            {
                if ((char)keyName[j] == ':')
                {
                    bookmarks[totalBookmarks] = j;
                    totalBookmarks++;
                }
            }

            if (totalBookmarks == 0)
                return (keyName.Length - 1, raw, ex);

            for (int k = 0; k < totalBookmarks; k++)
            {
                var i = bookmarks[k];

                //try read locktype 's' or 'x'
                //:x </EOF>
                if (i + 1 == keyName.Length - 1)
                {
                    if ((char)keyName[i + 1] == 'x' || (char)keyName[i + 1] == 'X')
                        ex = true;
                    else
                        ex = false;
                }

                //try read keytype
                //::
                if (i + 3 < keyName.Length)
                {
                    //make sure is raw
                    if (((char)keyName[i + 1] == 'r' || (char)keyName[i + 1] == 'R')
                        && ((char)keyName[i + 2] == 'a' || (char)keyName[i + 2] == 'A')
                        && ((char)keyName[i + 3] == 'w' || (char)keyName[i + 3] == 'W'))
                    {
                        raw = true;
                        nameEnd = i - 1;
                        break;
                    }
                    //make sure is obj
                    if (((char)keyName[i + 1] == 'o' || (char)keyName[i + 1] == 'O')
                        && ((char)keyName[i + 2] == 'b' || (char)keyName[i + 2] == 'B')
                        && ((char)keyName[i + 3] == 'j' || (char)keyName[i + 3] == 'J'))
                    {
                        raw = false;
                        nameEnd = i - 1;
                        break;
                    }
                }

                //keytype is not explicit but there's a placeholder ('::*')
                if (i + 1 < keyName.Length - 1 && (char)keyName[i + 1] == ':')
                {
                    raw = true;
                    nameEnd = i - 1;
                    break;
                }
            }

            return (nameEnd, raw, ex);
        }
    }
}
