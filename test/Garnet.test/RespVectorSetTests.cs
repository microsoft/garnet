// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespVectorSetTests
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);

            Program.RegisterHackyBenchmarkCommands(server);

            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void VADD()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "4", "4.0", "3.0", "2.0", "1.0", new byte[] { 1, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res2);

            // TODO: exact duplicates - what does Redis do?
        }

        [Test]
        public void VADDErrors()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var vectorSetKey = $"{nameof(VADDErrors)}_{Guid.NewGuid()}";

            // Bad arity
            var exc1 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD"));
            ClassicAssert.AreEqual("ERR wrong number of arguments for 'VADD' command", exc1.Message);
            var exc2 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey]));
            ClassicAssert.AreEqual("ERR wrong number of arguments for 'VADD' command", exc2.Message);
            var exc3 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "FP32"]));
            ClassicAssert.AreEqual("ERR wrong number of arguments for 'VADD' command", exc3.Message);
            var exc4 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES"]));
            ClassicAssert.AreEqual("ERR wrong number of arguments for 'VADD' command", exc4.Message);
            var exc5 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "1"]));
            ClassicAssert.AreEqual("ERR wrong number of arguments for 'VADD' command", exc5.Message);
            var exc6 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "1", "1.0"]));
            ClassicAssert.AreEqual("ERR wrong number of arguments for 'VADD' command", exc6.Message);

            // Reduce after vector
            var exc7 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "2", "1.0", "2.0", "bar", "REDUCE", "1"]));
            ClassicAssert.AreEqual("ERR invalid option after element", exc7.Message);

            // Duplicate flags
            // TODO: Redis doesn't error on these which seems... wrong, confirm with them
            //var exc8 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "1", "2.0", "bar", "CAS", "CAS"]));
            //var exc9 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "1", "2.0", "bar", "NOQUANT", "Q8"]));
            //var exc10 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "1", "2.0", "bar", "EF", "1", "EF", "1"]));
            //var exc11 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "1", "2.0", "bar", "SETATTR", "abc", "SETATTR", "abc"]));
            //var exc12 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "1", "2.0", "bar", "M", "5", "M", "5"]));

            // M out of range (Redis imposes M >= 4 and m <= 4096
            var exc13 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "1", "2.0", "bar", "M", "1"]));
            ClassicAssert.AreEqual("ERR invalid M", exc13.Message);
            var exc14 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "1", "2.0", "bar", "M", "10000"]));
            ClassicAssert.AreEqual("ERR invalid M", exc14.Message);

            // Missing/bad option value
            var exc20 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "1", "2.0", "bar", "EF"]));
            ClassicAssert.AreEqual("ERR invalid option after element", exc20.Message);
            var exc21 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "1", "2.0", "bar", "EF", "0"]));
            ClassicAssert.AreEqual("ERR invalid EF", exc21.Message);
            var exc22 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "1", "2.0", "bar", "SETATTR"]));
            ClassicAssert.AreEqual("ERR invalid option after element", exc22.Message);
            var exc23 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "1", "2.0", "bar", "M"]));
            ClassicAssert.AreEqual("ERR invalid option after element", exc23.Message);
            var exc24 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "2", "2.0", "bar"]));
            ClassicAssert.AreEqual("ERR invalid vector specification", exc24.Message);
            var exc25 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "0", "bar"]));
            ClassicAssert.AreEqual("ERR invalid vector specification", exc25.Message);
            var exc26 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "1", "fizz", "bar"]));
            ClassicAssert.AreEqual("ERR invalid vector specification", exc26.Message);

            // Unknown option
            var exc27 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "1", "2.0", "bar", "FOO"]));
            ClassicAssert.AreEqual("ERR invalid option after element", exc27.Message);

            // Malformed FP32
            var binary = new float[] { 1, 2, 3 };
            var blob = MemoryMarshal.Cast<float, byte>(binary)[..^1].ToArray();
            var exc15 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "FP32", blob, "bar"]));
            ClassicAssert.AreEqual("ERR invalid vector specification", exc15.Message);

            // Mismatch after creating a vector set
            _ = db.KeyDelete(vectorSetKey);

            _ = db.Execute("VADD", [vectorSetKey, "VALUES", "1", "1.0", new byte[] { 0, 0, 1, 0 }, "NOQUANT", "EF", "6", "M", "10"]);

            // TODO: Redis returns the same error for all these mismatches which also seems... wrong, confirm with them
            var exc16 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "2", "1.0", "2.0", "fizz"]));
            ClassicAssert.AreEqual("ERR asked quantization mismatch with existing vector set", exc16.Message);
            var exc17 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "1", "2.0", "fizz", "Q8"]));
            ClassicAssert.AreEqual("ERR asked quantization mismatch with existing vector set", exc17.Message);
            var exc18 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "1", "2.0", "fizz", "EF", "12"]));
            ClassicAssert.AreEqual("ERR asked quantization mismatch with existing vector set", exc18.Message);
            var exc19 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "1", "2.0", "fizz", "M", "20"]));
            ClassicAssert.AreEqual("ERR asked quantization mismatch with existing vector set", exc19.Message);
        }

        [Test]
        public void VEMB()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = (string[])db.Execute("VEMB", ["foo", new byte[] { 0, 0, 0, 0 }]);
            ClassicAssert.AreEqual(4, res2.Length);
            ClassicAssert.AreEqual(float.Parse("1.0"), float.Parse(res2[0]));
            ClassicAssert.AreEqual(float.Parse("2.0"), float.Parse(res2[1]));
            ClassicAssert.AreEqual(float.Parse("3.0"), float.Parse(res2[2]));
            ClassicAssert.AreEqual(float.Parse("4.0"), float.Parse(res2[3]));

            var res3 = (string[])db.Execute("VEMB", ["foo", new byte[] { 0, 0, 0, 0 }]);
            ClassicAssert.AreEqual(0, res3.Length);
        }

        [Test]
        public void VectorSetOpacity()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = ClassicAssert.Throws<RedisServerException>(() => db.StringGet("foo"));
            ClassicAssert.True(res2.Message.Contains("WRONGTYPE"));
        }

        [Test]
        public void VectorElementOpacity()
        {
            // Check that we can't touch an element with GET despite it also being in the main store

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = (string)db.StringGet(new byte[] { 0, 0, 0, 0 });
            ClassicAssert.IsNull(res2);

            var res3 = db.KeyDelete(new byte[] { 0, 0, 0, 0 });
            ClassicAssert.IsFalse(res3);

            var res4 = db.StringSet(new byte[] { 0, 0, 0, 0 }, "def", when: When.NotExists);
            ClassicAssert.IsTrue(res4);

            Span<byte> buffer = stackalloc byte[128];

            // Attempt read and writes against the "true" element key names
            var manager = GetVectorManager(server);
            var ctx = manager.HighestContext();
            for (var i = 0UL; i <= ctx; i++)
            {
                VectorManager.DistinguishVectorElementKey(i, [0, 0, 0, 0], ref buffer, out var rented);

                try
                {
                    var mangled = buffer.ToArray();

                    var res5 = (string)db.StringGet(mangled);
                    ClassicAssert.IsNull(res5);

                    var res6 = db.StringSet(mangled, "!!!!", when: When.NotExists);
                    ClassicAssert.IsTrue(res6);
                }
                finally
                {
                    if (rented != null)
                    {
                        ArrayPool<byte>.Shared.Return(rented);
                    }
                }
            }

            // Check we haven't messed up the element
            var res7 = (string[])db.Execute("VEMB", ["foo", new byte[] { 0, 0, 0, 0 }]);
            ClassicAssert.AreEqual(4, res7.Length);
            ClassicAssert.AreEqual(float.Parse("1.0"), float.Parse(res7[0]));
            ClassicAssert.AreEqual(float.Parse("2.0"), float.Parse(res7[1]));
            ClassicAssert.AreEqual(float.Parse("3.0"), float.Parse(res7[2]));
            ClassicAssert.AreEqual(float.Parse("4.0"), float.Parse(res7[3]));
        }

        [Test]
        public void VSIM()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "4", "4.0", "3.0", "2.0", "1.0", new byte[] { 0, 0, 0, 1 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res2);

            var res3 = (byte[][])db.Execute("VSIM", ["foo", "VALUES", "4", "2.1", "2.2", "2.3", "2.4", "COUNT", "5", "EPSILON", "1.0", "EF", "40"]);
            ClassicAssert.AreEqual(2, res3.Length);
            ClassicAssert.IsTrue(res3.Any(static x => x.SequenceEqual(new byte[] { 0, 0, 0, 0 })));
            ClassicAssert.IsTrue(res3.Any(static x => x.SequenceEqual(new byte[] { 0, 0, 0, 1 })));

            var res4 = (byte[][])db.Execute("VSIM", ["foo", "ELE", new byte[] { 0, 0, 0, 0 }, "COUNT", "5", "EPSILON", "1.0", "EF", "40"]);
            ClassicAssert.AreEqual(2, res4.Length);
            ClassicAssert.IsTrue(res4.Any(static x => x.SequenceEqual(new byte[] { 0, 0, 0, 0 })));
            ClassicAssert.IsTrue(res4.Any(static x => x.SequenceEqual(new byte[] { 0, 0, 0, 1 })));

            // TODO: WITHSCORES
            // TODO: WITHATTRIBS
        }

        [Test]
        public void VDIM()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "3", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VDIM", "foo");
            ClassicAssert.AreEqual(3, (int)res2);

            var res3 = db.Execute("VADD", ["bar", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res3);

            var res4 = db.Execute("VDIM", "bar");
            ClassicAssert.AreEqual(4, (int)res4);

            var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VDIM", "fizz"));
            ClassicAssert.IsTrue(exc.Message.Contains("Key not found"));
        }

        [Test]
        public void DeleteVectorSet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "3", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.KeyDelete("foo");
            ClassicAssert.IsTrue(res2);

            var res3 = db.Execute("VADD", ["fizz", "REDUCE", "3", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res3);

            var res4 = db.StringSet("buzz", "abc");
            ClassicAssert.IsTrue(res4);

            var res5 = db.KeyDelete(["fizz", "buzz"]);
            ClassicAssert.AreEqual(2, res5);
        }

        // HACK - this had better not land in main
        [Test]
        public async Task JankBenchmarkCommandsAsync()
        {
            const string PathToPreload = @"C:\Users\kmontrose\Desktop\QUASR\Test Data\Youtube\Processed\youtube-8m-part-{0}.base.fbin";
            const string PathToQuery = @"C:\Users\kmontrose\Desktop\QUASR\Test Data\Youtube\Processed\youtube-8m.query-10k.fbin";
            const string PathToWrite = @"C:\Users\kmontrose\Desktop\QUASR\Test Data\Youtube\Processed\youtube-8m-holdout-{0}.base.fbin";
            const int BenchmarkDurationSeconds = 5;
            const int ParallelBenchmarks = 1;

            var key = $"{nameof(JankBenchmarkCommandsAsync)}_{Guid.NewGuid()}";

            // Preload vector set
            (TimeSpan Duration, long Inserts)[] preloadRes;
            {
                var tasks = new Task<(TimeSpan Duration, long Inserts)>[ParallelBenchmarks];

                for (var i = 0; i < tasks.Length; i++)
                {
                    var pathToPreload = string.Format(PathToPreload, i);

                    tasks[i] =
                        Task.Run(
                            () =>
                            {
                                using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
                                {
                                    var db = redis.GetDatabase();

                                    var fillRes = (string)db.Execute("FILLBENCH", [pathToPreload, key]);
                                    var fillParts = fillRes.Split(' ');
                                    ClassicAssert.AreEqual(2, fillParts.Length);
                                    var fillTime = TimeSpan.FromMilliseconds(double.Parse(fillParts[0]));
                                    var fillInserts = long.Parse(fillParts[1]);
                                    ClassicAssert.IsTrue(fillTime.Ticks > 0);
                                    ClassicAssert.IsTrue(fillInserts > 0);

                                    return (fillTime, fillInserts);
                                }
                            }
                        );
                }

                preloadRes = await Task.WhenAll(tasks);
            }

            // Spin up some number of tasks which will do arbitrary reads and (optionally) some writes
            var benchmarkMultis = new ConnectionMultiplexer[ParallelBenchmarks];
            (TimeSpan Duration, long Reads, long Writes, bool RanOutOfWriteData)[] results;
            try
            {
                for (var i = 0; i < benchmarkMultis.Length; i++)
                {
                    benchmarkMultis[i] = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                    _ = benchmarkMultis[i].GetDatabase().Ping();
                }

                using var start = new SemaphoreSlim(0, benchmarkMultis.Length);
                using var started = new SemaphoreSlim(0, benchmarkMultis.Length);
                var commands = new Task<(TimeSpan Duration, long Reads, long Writes, bool RanOutOfWriteData)>[benchmarkMultis.Length];

                for (var i = 0; i < benchmarkMultis.Length; i++)
                {
                    var benchRedis = benchmarkMultis[i];
                    var benchDb = benchRedis.GetDatabase();
                    var writePath = string.Format(PathToWrite, i);
                    commands[i] =
                        Task.Run(
                            async () =>
                            {
                                _ = started.Release();

                                await start.WaitAsync();

                                var benchSw = Stopwatch.StartNew();
                                var benchRes = (string)benchDb.Execute("BENCHRWMIX", [key, PathToQuery, writePath, "64", "0.1", "64", "500", BenchmarkDurationSeconds.ToString()]); // 50% writes, until we run out of data
                                benchSw.Stop();
                                var benchParts = benchRes.Split(' ');
                                ClassicAssert.AreEqual(4, benchParts.Length);
                                var benchTime = TimeSpan.FromMilliseconds(double.Parse(benchParts[0]));
                                var benchReads = long.Parse(benchParts[1]);
                                var benchWrites = long.Parse(benchParts[2]);
                                var ranOutOfWriteData = bool.Parse(benchParts[3]);
                                ClassicAssert.IsTrue(benchSw.Elapsed >= TimeSpan.FromSeconds(BenchmarkDurationSeconds));
                                ClassicAssert.IsTrue(benchTime >= TimeSpan.FromSeconds(BenchmarkDurationSeconds));
                                ClassicAssert.IsTrue(benchReads > 0);
                                ClassicAssert.IsTrue(benchWrites > 0);

                                return (benchTime, benchReads, benchWrites, ranOutOfWriteData);
                            }
                        );
                }

                // Wait for all the tasks to init
                for (var i = 0; i < benchmarkMultis.Length; i++)
                {
                    await started.WaitAsync();
                }

                // Release all task and wait for bench commands to complete
                _ = start.Release(benchmarkMultis.Length);
                results = await Task.WhenAll(commands);
            }
            finally
            {
                foreach (var toDispose in benchmarkMultis)
                {
                    toDispose?.Dispose();
                }
            }

            var totalQueries = results.Sum(static x => x.Reads);
            var totalWrites = results.Sum(static x => x.Writes);
            var ranOutOfWriteData = results.Any(static x => x.RanOutOfWriteData);
            var qps = totalQueries / (double)BenchmarkDurationSeconds;
            var ips = totalWrites / (double)BenchmarkDurationSeconds;

            TestContext.Progress.WriteLine($"Total queries: {qps}");
            TestContext.Progress.WriteLine($"Queries per second: {qps}");
            TestContext.Progress.WriteLine($"Total inserts: {totalWrites}");
            TestContext.Progress.WriteLine($"Inserts per second: {ips}");
            TestContext.Progress.WriteLine($"Ran out of write data: {ranOutOfWriteData}");
        }

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "vectorManager")]
        private static extern ref VectorManager GetVectorManager(GarnetServer server);
    }
}