// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.client.GarnetClientAPI;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespSortedSetGarnetClientTests
    {
        protected GarnetServer server;
        ManualResetEventSlim waiter;
        const int maxIterations = 3;

        static readonly SortedSetEntry[] leaderBoard =
             [
                new SortedSetEntry("Dave", 340),
                 new SortedSetEntry("Kendra", 400),
                 new SortedSetEntry("Tom", 560),
                 new SortedSetEntry("Barbara", 650),
                 new SortedSetEntry("Jennifer", 690),
                 new SortedSetEntry("Peter", 690),
                 new SortedSetEntry("Frank", 740),
                 new SortedSetEntry("Lester", 790),
                 new SortedSetEntry("Alice", 850),
                 new SortedSetEntry("Mary", 980)
             ];


        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, enableAOF: true);
            server.Start();
            waiter = new ManualResetEventSlim();
        }


        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }


        #region GarnetClientExecuteAPI

        [Test]
        public async Task CanDoZAddGarnetAsync()
        {
            using var db = new GarnetClient(TestUtils.EndPoint);
            db.Connect();

            List<string> parameters =
            [
                "leaderboard"
            ];

            foreach (SortedSetEntry item in leaderBoard)
            {
                parameters.Add(item.Score.ToString(CultureInfo.InvariantCulture));
                parameters.Add(item.Element);
            }

            await db.ExecuteForMemoryResultAsync("ZADD", parameters);

            Memory<byte> ZCARD = Encoding.ASCII.GetBytes("$5\r\nZCARD\r\n");

            // 10 entries are added
            var result = await db.ExecuteForMemoryResultAsync(ZCARD, "leaderboard");

            ClassicAssert.AreEqual("10", Encoding.ASCII.GetString(result.Span));

            // disposing MR
            result.Dispose();
        }

        [Test]
        public void CanDoZAddGarnetCallback()
        {
            using var db = new GarnetClient(TestUtils.EndPoint);
            db.Connect();
            List<string> parameters = ["myzset1", "1", "KEY1", "2", "KEY2"];
            db.ExecuteForMemoryResult(SimpleMemoryResultCallback, 1, "ZADD", parameters);
            waiter.Wait();
            waiter.Reset();
        }

        [Test]
        public void CanDoExecuteForStringResultCallback()
        {
            using var db = new GarnetClient(TestUtils.EndPoint);
            db.Connect();
            var expectedResult = "2";
            List<string> parameters = ["myzset1", "1", "KEY1", "2", "KEY2"];
            db.ExecuteForStringResult((c, s) =>
            {
                ClassicAssert.IsTrue(s == expectedResult); waiter.Set();
            }, 1, "ZADD", parameters);
            waiter.Wait();
            waiter.Reset();

            Memory<byte> ZCARD = Encoding.ASCII.GetBytes("$5\r\nZCARD\r\n");

            db.ExecuteForStringResult((c, s) =>
            {
                ClassicAssert.IsTrue(s == expectedResult); waiter.Set();
            }, 1, ZCARD, "myzset1");

            waiter.Wait();
            waiter.Reset();
        }


        [Test]
        public async Task CanDoZCardGarnetUsingMemoryResultAsync()
        {
            using var db = new GarnetClient(TestUtils.EndPoint);
            db.Connect();
            List<string> parameters = ["myzset1", "1", "KEY1", "2", "KEY2"];
            db.ExecuteForMemoryResult(SimpleMemoryResultCallback, 1, "ZADD", parameters);
            waiter.Wait();
            waiter.Reset();

            Memory<byte> ZCARD = Encoding.ASCII.GetBytes("$5\r\nZCARD\r\n");
            var result = await db.ExecuteForMemoryResultAsync(ZCARD, "myzset1");

            ClassicAssert.AreEqual("2", Encoding.ASCII.GetString(result.Span));

            //Dispose
            result.Dispose();
        }

        private void SimpleMemoryResultCallback(long context, MemoryResult<byte> result)
        {
            ClassicAssert.IsTrue(result.Span.SequenceEqual(Encoding.ASCII.GetBytes("2")));
            result.Dispose();
            waiter.Set();
        }

        [Test]
        public async Task CanDoZAddGarnetMultithread()
        {
            using var db = new GarnetClient(TestUtils.EndPoint);
            db.Connect();
            var numThreads = 2;
            Task[] tasks = new Task[numThreads];
            var rnd = new Random();
            var numIterations = 3; // how many dictionaries to be added
            ConcurrentDictionary<string, int> ss = new();

            string name = string.Empty;
            for (int i = 0; i < numThreads; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    for (var ii = 0; ii < numIterations; ii++)
                    {
                        string name = GetUniqueName(ss, ii);
                        if (name == String.Empty)
                        {
                            Assert.Fail("Concurrency issue, review test: CanDoZaddGarnetMultithread");
                        }

                        List<string> parameters =
                        [
                            name
                        ];
                        foreach (SortedSetEntry item in leaderBoard)
                        {
                            parameters.Add(item.Score.ToString(CultureInfo.InvariantCulture));
                            parameters.Add(item.Element);
                        }

                        await db.ExecuteForMemoryResultAsync("ZADD", parameters);
                        await Task.Delay(millisecondsDelay: rnd.Next(10, 50));

                        parameters = [name, "-inf", "+inf"];

                        var result = await db.ExecuteForMemoryResultArrayAsync("ZRANGEBYSCORE", parameters);

                        // assert the elements
                        foreach (var item in leaderBoard)
                        {
                            var found = result.First(t => t.Span.SequenceEqual(Encoding.ASCII.GetBytes(item.Element.ToString())));
                            ClassicAssert.IsTrue(found.Span.Length > 0);
                        }

                        Memory<byte> ZREM = Encoding.ASCII.GetBytes("$4\r\nZREM\r\n");
                        foreach (SortedSetEntry item in leaderBoard)
                        {
                            await db.ExecuteForMemoryResultAsync(ZREM, name, item.Element);
                        }

                        foreach (var item in result) item.Dispose();

                        await Task.Delay(millisecondsDelay: 3000);
                    }
                });
            }
            Task.WaitAll(tasks);

            //checkpoint
            await db.ExecuteForMemoryResultAsync("SAVE");

            Memory<byte> ZCARD = Encoding.ASCII.GetBytes("$5\r\nZCARD\r\n");
            foreach (var i in ss)
            {
                var n = await db.ExecuteForMemoryResultAsync(ZCARD, i.Key);
                ClassicAssert.AreEqual("0", Encoding.ASCII.GetString(n.Span));
                n.Dispose();
            }

            ClassicAssert.AreEqual(ss.Count, numThreads * numIterations);
        }

        [Test]
        public async Task CanUseExecuteForStringArrayResult()
        {
            using var db = new GarnetClient(TestUtils.EndPoint);
            db.Connect();

            List<string> parameters =
            [
                "leaderboard"
            ];

            foreach (SortedSetEntry item in leaderBoard)
            {
                parameters.Add(item.Score.ToString(CultureInfo.InvariantCulture));
                parameters.Add(item.Element);
            }

            await db.ExecuteForMemoryResultAsync("ZADD", parameters);

            parameters = ["leaderboard", "-inf", "+inf"];
            var result = await db.ExecuteForStringArrayResultAsync("ZRANGEBYSCORE", parameters);

            // assert the elements
            foreach (var item in leaderBoard)
            {
                var found = result.First(t => t.Equals(item.Element));
                ClassicAssert.IsTrue(found.Length > 0);
            }
        }

        [Test]
        public async Task CanUseExecuteForStringResultAsync()
        {
            using var db = new GarnetClient(TestUtils.EndPoint);
            db.Connect();
            List<string> parameters = [];
            parameters = ["myzset1", "1", "KEY1", "2", "KEY2"];

            var result = await db.ExecuteForStringResultAsync("ZADD", parameters);
            ClassicAssert.AreEqual("2", result);

            Memory<byte> ZCARD = Encoding.ASCII.GetBytes("$5\r\nZCARD\r\n");
            result = await db.ExecuteForStringResultAsync(ZCARD, "myzset1");
            ClassicAssert.AreEqual("2", result);


            Memory<byte> key = Encoding.ASCII.GetBytes("myzset1");
            result = await db.ExecuteForStringResultAsync(ZCARD, key);
            ClassicAssert.AreEqual("2", result);
        }

        [Test]
        public async Task CanUseExecuteForMemoryResultArrayWithCancellationTokenAsync()
        {
            using var db = new GarnetClient(TestUtils.EndPoint);
            db.Connect();

            List<string> parameters =
            [
                "leaderboard"
            ];

            foreach (SortedSetEntry item in leaderBoard)
            {
                parameters.Add(item.Score.ToString(CultureInfo.InvariantCulture));
                parameters.Add(item.Element);
            }

            await db.ExecuteForMemoryResultAsync("ZADD", parameters);

            var tokenSource = new CancellationTokenSource();
            var token = tokenSource.Token;

            parameters = ["leaderboard", "-inf", "+inf"];
            var t = db.ExecuteForMemoryResultArrayWithCancellationAsync("ZRANGEBYSCORE", parameters, token);
            var result = t.GetAwaiter().GetResult();
            ClassicAssert.IsTrue(t.IsCompletedSuccessfully);
            ClassicAssert.IsTrue(result.Length == 10);

            //execution with cancel token
            tokenSource.Cancel();
            t = db.ExecuteForMemoryResultArrayWithCancellationAsync("ZRANGEBYSCORE", parameters, token);
            Assert.Throws<OperationCanceledException>(() => t.Wait(tokenSource.Token));

            foreach (var i in result)
            {
                i.Dispose();
            }
            tokenSource.Dispose();
        }

        [Test]
        public async Task CanUseExecuteForMemoryResultWithCancellationTokenAsync()
        {
            using var db = new GarnetClient(TestUtils.EndPoint);
            db.Connect();

            List<string> parameters =
            [
                "leaderboard"
            ];

            foreach (SortedSetEntry item in leaderBoard)
            {
                parameters.Add(item.Score.ToString(CultureInfo.InvariantCulture));
                parameters.Add(item.Element);
            }

            await db.ExecuteForMemoryResultAsync("ZADD", parameters);

            var tokenSource = new CancellationTokenSource();
            var token = tokenSource.Token;

            // Execute without cancellation
            var t = DoZRangeAsync(1, token);
            t.GetAwaiter().GetResult();
            ClassicAssert.IsTrue(t.IsCompletedSuccessfully);

            // First cancel the token to be sure the GarnetClient call sees the canceled token
            // Else, there will be a race in the testcase
            tokenSource.Cancel();
            t = DoZRangeAsync(1, token);
            bool canceled = false;
            try
            {
                t.GetAwaiter().GetResult();
            }
            catch (OperationCanceledException)
            {
                canceled = true;
            }
            ClassicAssert.IsTrue(canceled);

            tokenSource.Dispose();
        }

        private static async Task DoZRangeAsync(int taskId, CancellationToken ct)
        {
            var rnd = new Random();
            using var db = new GarnetClient(TestUtils.EndPoint);
            db.Connect();

            var parameters = new List<string>();
            parameters.AddRange(["leaderboard", "-inf", "+inf"]);

            for (int i = 0; i <= maxIterations; i++)
            {
                var result = await db.ExecuteForStringArrayResultWithCancellationAsync("ZRANGEBYSCORE", parameters, ct);
                foreach (var item in leaderBoard)
                {
                    var found = result.FirstOrDefault(t => t.Equals(item.Element));
                    ClassicAssert.IsTrue(found != null);
                }
            }
        }

        private static string GetUniqueName(ConcurrentDictionary<string, int> names, int ii)
        {
            var rnd = new Random();
            string name;
            var i = 0;
            do
            {
                var rndMillis = rnd.Next(1, 1000);
                name = $"leaderboard-{ii}-{rndMillis}";
                if (names.TryAdd(name, ii))
                {
                    break;
                }
                i++;
            } while (i < 100);

            return name;
        }

        #endregion


        #region GarnetClientSortedSetAPI

        [Test]
        public async Task CanGarnetClientUseZaddZrem()
        {
            using var db = new GarnetClient(TestUtils.EndPoint);
            db.Connect();

            // add a new Sorted Set
            var pairs = new SortedSetPairCollection();

            foreach (SortedSetEntry item in leaderBoard)
            {
                pairs.AddSortedSetEntry(Encoding.ASCII.GetBytes(item.Element.ToString()), item.Score);
            }

            var added = await db.SortedSetAddAsync("leaderboard", pairs);
            ClassicAssert.IsTrue(added == leaderBoard.Length);

            //just update
            added = await db.SortedSetAddAsync("leaderboard", pairs);
            ClassicAssert.IsTrue(added == 0);

            //remove
            var removed = await db.SortedSetRemoveAsync("leaderboard", pairs);
            ClassicAssert.IsTrue(removed == 10);

            //length should be 0
            var len = await db.SortedSetLengthAsync("leaderboard");
            ClassicAssert.IsTrue(len == 0);
        }

        [Test]
        public void CanGarnetClientUseZaddZremWithCallback()
        {
            using var db = new GarnetClient(TestUtils.EndPoint);
            db.Connect();

            ManualResetEventSlim e = new();

            // add a new Sorted Set
            db.SortedSetAdd("myss", "sspairOne", 1, (ctx, retValue, msg) =>
            {
                ClassicAssert.IsTrue(1 == retValue);
                e.Set();
            });

            e.Wait(); e.Reset();

            //just update
            db.SortedSetAdd("myss", "sspairOne", 2, (ctx, retValue, msg) =>
            {
                //no adds were done
                ClassicAssert.IsTrue(0 == retValue);
                e.Set();
            });

            e.Wait(); e.Reset();

            //remove
            db.SortedSetRemove("myss", "sspairOne", (ctx, retValue, msg) =>
            {
                ClassicAssert.IsTrue(1 == retValue);
                e.Set();
            });

            e.Wait(); e.Reset();

            //length should be 0
            db.SortedSetLength("myss", (ctx, retValue, msg) =>
            {
                ClassicAssert.IsTrue(0 == retValue);
                e.Set();
            });

            e.Wait(); e.Reset();

            e.Dispose();
        }

        [Test]
        public async Task CanGarnetClientUseZCard()
        {
            using var db = new GarnetClient(TestUtils.EndPoint);
            db.Connect();
            var expectedValue = 0;

            ManualResetEventSlim e = new();

            //ZCARD with Callback
            //non existing key
            db.SortedSetLength("nokey", (context, retValue, msgResult) =>
            {
                ClassicAssert.AreEqual(expectedValue, retValue);
                e.Set();
            });

            e.Wait(); e.Reset();

            //ZCARD async non existing key
            var len = await db.SortedSetLengthAsync("nokey");
            ClassicAssert.AreEqual(expectedValue, len);

            // add a new Sorted Set
            List<string> parameters =
            [
                "leaderboard"
            ];

            foreach (SortedSetEntry item in leaderBoard)
            {
                parameters.Add(item.Score.ToString(CultureInfo.InvariantCulture));
                parameters.Add(item.Element);
            }

            var added = await db.ExecuteForMemoryResultAsync("ZADD", parameters);
            ClassicAssert.AreEqual("10", Encoding.ASCII.GetString(added.Span));

            //ZCARD async
            len = await db.SortedSetLengthAsync("leaderboard");
            ClassicAssert.AreEqual(10, len);

            expectedValue = 10;

            //ZCARD with Callback
            db.SortedSetLength("leaderboard", (context, retValue, msgResult) =>
            {
                ClassicAssert.AreEqual(expectedValue, retValue);
                e.Set();
            });

            e.Wait(); e.Reset();

            //freeing memory
            e.Dispose();
            added.Dispose();
        }

        #endregion

    }
}