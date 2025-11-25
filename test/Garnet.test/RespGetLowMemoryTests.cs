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
    public class RespGetLowMemoryTests
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
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
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