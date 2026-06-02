// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.server;
using GarnetRoaringBitmap;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// End-to-end integration tests that exercise the four R.* commands through the
    /// real RESP protocol via StackExchange.Redis. These complement the pure
    /// data-structure tests in <c>RoaringBitmapDataTests</c>.
    /// </summary>
    [AllureNUnit]
    [TestFixture]
    public class RespRoaringBitmapTests : AllureTestBase
    {
        private GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: true);
            RegisterRoaringBitmap(server);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        private static void RegisterRoaringBitmap(GarnetServer server)
        {
            var factory = new RoaringBitmapFactory();
            server.Register.NewType(factory);
            server.Register.NewCommand("R.SETBIT", CommandType.ReadModifyWrite, factory, new RSetBit(), new RespCommandsInfo { Arity = 4 });
            server.Register.NewCommand("R.GETBIT", CommandType.ReadModifyWrite, factory, new RGetBit(), new RespCommandsInfo { Arity = 3 });
            server.Register.NewCommand("R.BITCOUNT", CommandType.ReadModifyWrite, factory, new RBitCount(), new RespCommandsInfo { Arity = 2 });
            server.Register.NewCommand("R.BITPOS", CommandType.ReadModifyWrite, factory, new RBitPos(), new RespCommandsInfo { Arity = -3 });
        }

        [Test]
        public void GetBitOnMissingKey_Returns0()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var r = (long)db.Execute("R.GETBIT", "rb", "12345");
            ClassicAssert.AreEqual(0, r);
            // Missing-key read must not have created a key.
            ClassicAssert.IsFalse(db.KeyExists("rb"));
        }

        [Test]
        public void BitCountOnMissingKey_Returns0()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var r = (long)db.Execute("R.BITCOUNT", "rb");
            ClassicAssert.AreEqual(0, r);
            ClassicAssert.IsFalse(db.KeyExists("rb"));
        }

        [Test]
        public void BitPosOnMissingKey_Bit1ReturnsMinus1()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var r = (long)db.Execute("R.BITPOS", "rb", "1");
            ClassicAssert.AreEqual(-1L, r);
            ClassicAssert.IsFalse(db.KeyExists("rb"));
        }

        [Test]
        public void BitPosOnMissingKey_Bit0ReturnsFromOrZero()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var r = (long)db.Execute("R.BITPOS", "rb", "0");
            ClassicAssert.AreEqual(0L, r);

            var r2 = (long)db.Execute("R.BITPOS", "rb", "0", "100");
            ClassicAssert.AreEqual(100L, r2);
        }

        [Test]
        public void SetBitGetBit_Basics()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            ClassicAssert.AreEqual(0, (long)db.Execute("R.SETBIT", "rb", "42", "1"));
            ClassicAssert.AreEqual(1, (long)db.Execute("R.SETBIT", "rb", "42", "1"));
            ClassicAssert.AreEqual(1, (long)db.Execute("R.GETBIT", "rb", "42"));
            ClassicAssert.AreEqual(0, (long)db.Execute("R.GETBIT", "rb", "41"));

            // Clear it.
            ClassicAssert.AreEqual(1, (long)db.Execute("R.SETBIT", "rb", "42", "0"));
            ClassicAssert.AreEqual(0, (long)db.Execute("R.GETBIT", "rb", "42"));
        }

        [Test]
        public void BitCount_Basics()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            for (uint i = 0; i < 10; i++)
                db.Execute("R.SETBIT", "rb", i.ToString(), "1");

            ClassicAssert.AreEqual(10L, (long)db.Execute("R.BITCOUNT", "rb"));

            db.Execute("R.SETBIT", "rb", "5", "0");
            ClassicAssert.AreEqual(9L, (long)db.Execute("R.BITCOUNT", "rb"));
        }

        [Test]
        public void BitPos_Basics()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("R.SETBIT", "rb", "100", "1");
            db.Execute("R.SETBIT", "rb", "200", "1");
            db.Execute("R.SETBIT", "rb", "70000", "1");

            ClassicAssert.AreEqual(100L, (long)db.Execute("R.BITPOS", "rb", "1"));
            ClassicAssert.AreEqual(200L, (long)db.Execute("R.BITPOS", "rb", "1", "150"));
            ClassicAssert.AreEqual(70000L, (long)db.Execute("R.BITPOS", "rb", "1", "300"));
            ClassicAssert.AreEqual(-1L, (long)db.Execute("R.BITPOS", "rb", "1", "70001"));

            // First unset bit after a fully-set range.
            ClassicAssert.AreEqual(0L, (long)db.Execute("R.BITPOS", "rb", "0"));
            ClassicAssert.AreEqual(101L, (long)db.Execute("R.BITPOS", "rb", "0", "100"));
        }

        [Test]
        public void Errors_BadOffset()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var ex = Assert.Throws<RedisServerException>(() => db.Execute("R.SETBIT", "rb", "notanumber", "1"));
            StringAssert.Contains("offset", ex.Message);

            ex = Assert.Throws<RedisServerException>(() => db.Execute("R.SETBIT", "rb", "-5", "1"));
            StringAssert.Contains("offset", ex.Message);

            ex = Assert.Throws<RedisServerException>(() => db.Execute("R.SETBIT", "rb", "5", "2"));
            StringAssert.Contains("0 or 1", ex.Message);
        }

        [Test]
        public void StringKeyAndCustomObjectKey_AreSeparate()
        {
            // On dev's unified store, a string and a custom-object cannot share a key:
            // attempting a custom-object op on a string-typed key returns WRONGTYPE.
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.StringSet("rb", "hello");
            var ex = Assert.Throws<RedisServerException>(() => db.Execute("R.SETBIT", "rb", "10", "1"));
            StringAssert.Contains("WRONGTYPE", ex.Message);
            ClassicAssert.AreEqual("hello", (string)db.StringGet("rb"));
        }

        [Test]
        public void Errors_BitPosBadBit()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var ex = Assert.Throws<RedisServerException>(() => db.Execute("R.BITPOS", "rb", "2"));
            StringAssert.Contains("0 or 1", ex.Message);
        }

        [Test]
        public void Errors_WrongArity()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            // R.SETBIT requires exactly 3 args (arity 4 incl command).
            Assert.Throws<RedisServerException>(() => db.Execute("R.SETBIT", "rb", "1"));
            // R.BITCOUNT requires exactly 1 arg.
            Assert.Throws<RedisServerException>(() => db.Execute("R.BITCOUNT", "rb", "extra"));
        }

        [Test]
        public void OracleParity_RandomMix()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var rng = new Random(1234);
            var oracle = new HashSet<uint>();

            for (var i = 0; i < 5000; i++)
            {
                var off = (uint)rng.Next(0, 200_000);
                var set = rng.Next(2) == 1;
                var prevExpected = oracle.Contains(off) ? 1 : 0;
                var prevActual = (int)(long)db.Execute("R.SETBIT", "rb", off.ToString(), set ? "1" : "0");
                ClassicAssert.AreEqual(prevExpected, prevActual);
                if (set) oracle.Add(off); else oracle.Remove(off);
            }

            ClassicAssert.AreEqual((long)oracle.Count, (long)db.Execute("R.BITCOUNT", "rb"));

            for (var i = 0; i < 200; i++)
            {
                var off = (uint)rng.Next(0, 200_000);
                var expected = oracle.Contains(off) ? 1 : 0;
                ClassicAssert.AreEqual(expected, (int)(long)db.Execute("R.GETBIT", "rb", off.ToString()));
            }
        }

        [Test]
        public void DeleteAndRecreate()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.Execute("R.SETBIT", "rb", "5", "1");
            ClassicAssert.AreEqual(1L, (long)db.Execute("R.BITCOUNT", "rb"));

            db.KeyDelete("rb");
            ClassicAssert.AreEqual(0L, (long)db.Execute("R.BITCOUNT", "rb"));

            db.Execute("R.SETBIT", "rb", "9", "1");
            ClassicAssert.AreEqual(0, (long)db.Execute("R.GETBIT", "rb", "5"));
            ClassicAssert.AreEqual(1, (long)db.Execute("R.GETBIT", "rb", "9"));
        }

        [Test]
        public void LargeOffsetsAndChunkBoundaries()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Chunk-boundary offsets.
            uint[] interesting = [0u, 65535u, 65536u, 131071u, 131072u, (uint)int.MaxValue, uint.MaxValue];
            foreach (var off in interesting)
            {
                ClassicAssert.AreEqual(0, (long)db.Execute("R.SETBIT", "rb", off.ToString(), "1"));
                ClassicAssert.AreEqual(1, (long)db.Execute("R.GETBIT", "rb", off.ToString()));
            }
            ClassicAssert.AreEqual((long)interesting.Length, (long)db.Execute("R.BITCOUNT", "rb"));
            ClassicAssert.AreEqual(0L, (long)db.Execute("R.BITPOS", "rb", "1"));
            ClassicAssert.AreEqual((long)uint.MaxValue, (long)db.Execute("R.BITPOS", "rb", "1", ((uint)int.MaxValue + 1u).ToString()));
        }

        [Test]
        public async Task ConcurrentSetBit_FromMultipleClients_Consistent()
        {
            // Five clients each set a disjoint range of 2000 unique bits; total population
            // count must be 10_000. Demonstrates command linearizability for a custom RMW.
            const int clients = 5;
            const int perClient = 2000;
            var tasks = new Task[clients];
            for (var c = 0; c < clients; c++)
            {
                var clientIndex = c;
                tasks[c] = Task.Run(() =>
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                    var db = redis.GetDatabase(0);
                    for (var i = 0; i < perClient; i++)
                    {
                        var off = (uint)(clientIndex * perClient + i);
                        db.Execute("R.SETBIT", "rb", off.ToString(), "1");
                    }
                });
            }
            await Task.WhenAll(tasks);

            using var verifier = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var vdb = verifier.GetDatabase(0);
            ClassicAssert.AreEqual((long)(clients * perClient), (long)vdb.Execute("R.BITCOUNT", "rb"));
            for (var i = 0; i < clients * perClient; i += 7)
            {
                ClassicAssert.AreEqual(1, (long)vdb.Execute("R.GETBIT", "rb", i.ToString()));
            }
        }

        [Test]
        public void DenseBitmapPromotion_StaysCorrect()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Insert > 4096 bits in single chunk to force bitmap-container promotion.
            for (uint i = 0; i < 5000; i++)
                db.Execute("R.SETBIT", "rb", i.ToString(), "1");
            ClassicAssert.AreEqual(5000L, (long)db.Execute("R.BITCOUNT", "rb"));

            // Remove down to 4096 — demote back to array.
            for (uint i = 4096; i < 5000; i++)
                db.Execute("R.SETBIT", "rb", i.ToString(), "0");
            ClassicAssert.AreEqual(4096L, (long)db.Execute("R.BITCOUNT", "rb"));
            ClassicAssert.AreEqual(1, (long)db.Execute("R.GETBIT", "rb", "4095"));
            ClassicAssert.AreEqual(0, (long)db.Execute("R.GETBIT", "rb", "4096"));
        }

        [Test]
        public async Task ConcurrentReadsWhileWrites_StayConsistent()
        {
            // Pre-populate bits 0..999 set.
            using var setup = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var setupDb = setup.GetDatabase(0);
            for (uint i = 0; i < 1000; i++)
                setupDb.Execute("R.SETBIT", "rb", i.ToString(), "1");
            ClassicAssert.AreEqual(1000L, (long)setupDb.Execute("R.BITCOUNT", "rb"));

            var exceptions = new System.Collections.Concurrent.ConcurrentBag<Exception>();

            // 4 reader tasks: each runs 5000 R.GETBIT / R.BITCOUNT operations alternating.
            var readerTasks = new Task[4];
            for (var r = 0; r < 4; r++)
            {
                readerTasks[r] = Task.Run(() =>
                {
                    try
                    {
                        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                        var db = redis.GetDatabase(0);
                        var rng = new Random(Environment.CurrentManagedThreadId);
                        for (var i = 0; i < 5000; i++)
                        {
                            if (i % 2 == 0)
                            {
                                var off = (uint)rng.Next(0, 1000);
                                var val = (long)db.Execute("R.GETBIT", "rb", off.ToString());
                                // bits [0,999] should always be set
                                ClassicAssert.AreEqual(1, val);
                            }
                            else
                            {
                                var count = (long)db.Execute("R.BITCOUNT", "rb");
                                // At least 1000 bits (the pre-populated ones) must always be there.
                                ClassicAssert.GreaterOrEqual(count, 1000L);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                    }
                });
            }

            // 1 writer task: toggle bits 1000..1999 in a loop for the same duration.
            var writerTask = Task.Run(() =>
            {
                try
                {
                    using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                    var db = redis.GetDatabase(0);
                    for (var round = 0; round < 5; round++)
                    {
                        for (uint i = 1000; i < 2000; i++)
                            db.Execute("R.SETBIT", "rb", i.ToString(), "1");
                        for (uint i = 1000; i < 2000; i++)
                            db.Execute("R.SETBIT", "rb", i.ToString(), "0");
                    }
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            });

            await Task.WhenAll([.. readerTasks, writerTask]);

            ClassicAssert.IsEmpty(exceptions, $"Exceptions during concurrent ops: {string.Join("; ", exceptions)}");

            // Final R.BITCOUNT for bits [0,999] is 1000
            using var verifier = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var vdb = verifier.GetDatabase(0);
            ClassicAssert.AreEqual(1000L, (long)vdb.Execute("R.BITCOUNT", "rb"));
        }
    }
}