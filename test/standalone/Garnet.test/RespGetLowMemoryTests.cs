// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespGetLowMemoryTests : TestBase
    {
        GarnetServer server;
        Random r;

        [SetUp]
        public void Setup()
        {
            r = new Random(335);
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, getSG: true, disablePubSub: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        public void ScatterGatherGet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const int length = 30;
            KeyValuePair<RedisKey, RedisValue>[] input = new KeyValuePair<RedisKey, RedisValue>[length];
            for (int i = 0; i < length; i++)
                input[i] = new KeyValuePair<RedisKey, RedisValue>(i.ToString(), i.ToString());

            // MSET
            var result = db.StringSet(input);
            ClassicAssert.IsTrue(result);

            for (int iter = 1; iter < 5; iter++)
            {
                int numGets = 200 * iter;
                (RedisValue, Task<RedisValue>)[] tasks = new (RedisValue, Task<RedisValue>)[numGets];
                for (int i = 0; i < numGets; i++)
                {
                    int offset = r.Next(0, length);
                    tasks[i] = (input[offset].Value, db.StringGetAsync(input[offset].Key));
                }

                Task.WaitAll([.. tasks.Select(r => r.Item2)]);
                for (int i = 0; i < numGets; i++)
                    ClassicAssert.AreEqual(tasks[i].Item1, tasks[i].Item2.Result);
            }
        }

        // Mixed GET/SET pipeline: every GET is immediately followed by a SET, so the scatter-gather
        // look-ahead must correctly decline to batch across the SET and the interleaved results must be
        // exactly as issued. Exercises the cheap GET-prefix peek that rejects a non-GET next command.
        [Test]
        public void ScatterGatherMixedGetSetPipeline()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const int n = 100;
            for (var i = 0; i < n; i++)
                db.StringSet($"r{i}", $"val{i}");

            var batch = db.CreateBatch();
            var getTasks = new Task<RedisValue>[n];
            var setTasks = new Task<bool>[n];
            for (var i = 0; i < n; i++)
            {
                getTasks[i] = batch.StringGetAsync($"r{i}");
                setTasks[i] = batch.StringSetAsync($"w{i}", $"x{i}");
            }
            batch.Execute();
            Task.WaitAll([.. getTasks.Cast<Task>().Concat(setTasks)]);

            for (var i = 0; i < n; i++)
            {
                ClassicAssert.AreEqual($"val{i}", (string)getTasks[i].Result);
                ClassicAssert.IsTrue(setTasks[i].Result);
                ClassicAssert.AreEqual($"x{i}", (string)db.StringGet($"w{i}"));
            }
        }

        [Test]
        [TestCase(30)]      // Probably completes sync
        [TestCase(300)]     // May be a mix
        [TestCase(3_000)]   // Definitely completes async
        public void ScatterGatherMGet(int length)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var input = new KeyValuePair<RedisKey, RedisValue>[length];
            for (var i = 0; i < input.Length; i++)
                input[i] = new KeyValuePair<RedisKey, RedisValue>(i.ToString(), i.ToString());

            // MSET
            var result = db.StringSet(input);
            ClassicAssert.IsTrue(result);

            // All hits
            {
                // Single gets
                var single = input.Select(t => (t.Key, Expected: t.Value, Actual: db.StringGet(t.Key))).ToArray();

                // MGET
                var results = db.StringGet([.. input.Select(r => (RedisKey)r.Key)]);
                for (var i = 0; i < input.Length; i++)
                {
                    var expected = input[i].Value;
                    var singleActual = single[i].Actual;
                    var multiActual = results[i];

                    ClassicAssert.AreEqual(expected, singleActual);
                    ClassicAssert.AreEqual(expected, multiActual);
                }
            }

            // Some misses
            {
                var inputsWithMisses = input.Concat(Enumerable.Range(2 * input.Length, input.Length).Select(static i => new KeyValuePair<RedisKey, RedisValue>(i.ToString(), RedisValue.Null))).ToArray();

                new Random(2025_11_12_00).Shuffle(inputsWithMisses);

                // Single gets
                var single = inputsWithMisses.Select(t => (t.Key, Expected: t.Value, Actual: db.StringGet(t.Key))).ToArray();

                // MGET
                var results = db.StringGet([.. inputsWithMisses.Select(r => (RedisKey)r.Key)]);
                for (var i = 0; i < inputsWithMisses.Length; i++)
                {
                    var expected = inputsWithMisses[i].Value;
                    var singleActual = single[i].Actual;
                    var multiActual = results[i];

                    ClassicAssert.AreEqual(expected, singleActual);
                    ClassicAssert.AreEqual(expected, multiActual);
                }
            }
        }
    }
}