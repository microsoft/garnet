// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Linq;
using System.Runtime.CompilerServices;
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

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", "abc", "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "4", "4.0", "3.0", "2.0", "1.0", "def", "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res2);

            // TODO: exact duplicates - what does Redis do?
        }

        [Test]
        public void VEMB()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", "abc", "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = (string[])db.Execute("VEMB", ["foo", "abc"]);
            ClassicAssert.AreEqual(4, res2.Length);
            ClassicAssert.AreEqual(float.Parse("1.0"), float.Parse(res2[0]));
            ClassicAssert.AreEqual(float.Parse("2.0"), float.Parse(res2[1]));
            ClassicAssert.AreEqual(float.Parse("3.0"), float.Parse(res2[2]));
            ClassicAssert.AreEqual(float.Parse("4.0"), float.Parse(res2[3]));

            var res3 = (string[])db.Execute("VEMB", ["foo", "def"]);
            ClassicAssert.AreEqual(0, res3.Length);
        }

        [Test]
        public void VectorSetOpacity()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", "abc", "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = ClassicAssert.Throws<RedisServerException>(() => db.StringGet("foo"));
            ClassicAssert.True(res2.Message.Contains("WRONGTYPE"));
        }

        [Test]
        public void VectorElementOpacity()
        {
            // Check that we can't touch an element with GET despite it also being in the main store

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", "abc", "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = (string)db.StringGet("abc");
            ClassicAssert.IsNull(res2);

            var res3 = db.KeyDelete("abc");
            ClassicAssert.IsFalse(res3);

            var res4 = db.StringSet("abc", "def", when: When.NotExists);
            ClassicAssert.IsTrue(res4);

            Span<byte> buffer = stackalloc byte[128];

            // Attempt read and writes against the "true" element key names
            var manager = GetVectorManager(server);
            var ctx = manager.HighestContext();
            for (var i = 0UL; i <= ctx; i++)
            {
                VectorManager.DistinguishVectorElementKey(i, "abc"u8, ref buffer, out var rented);

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
            var res7 = (string[])db.Execute("VEMB", ["foo", "abc"]);
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
            var db = redis.GetDatabase(0);

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", "abc", "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "4", "4.0", "3.0", "2.0", "1.0", "def", "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res2);

            var res3 = (string[])db.Execute("VSIM", ["foo", "VALUES", "4", "2.1", "2.2", "2.3", "2.4", "COUNT", "5", "EPSILON", "1.0", "EF", "40"]);
            ClassicAssert.AreEqual(2, res3.Length);
            ClassicAssert.IsTrue(res3.Contains("abc"));
            ClassicAssert.IsTrue(res3.Contains("def"));

            var res4 = (string[])db.Execute("VSIM", ["foo", "ELE", "abc", "COUNT", "5", "EPSILON", "1.0", "EF", "40"]);
            ClassicAssert.AreEqual(2, res4.Length);
            ClassicAssert.IsTrue(res4.Contains("abc"));
            ClassicAssert.IsTrue(res4.Contains("def"));

            // TODO: WITHSCORES
            // TODO: WITHATTRIBS
        }

        [Test]
        public void VDIM()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "3", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", "abc", "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VDIM", "foo");
            ClassicAssert.AreEqual(3, (int)res2);

            var res3 = db.Execute("VADD", ["bar", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", "abc", "CAS", "Q8", "EF", "16", "M", "32"]);
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
            var db = redis.GetDatabase(0);

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "3", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", "abc", "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.KeyDelete("foo");
            ClassicAssert.IsTrue(res2);

            var res3 = db.Execute("VADD", ["fizz", "REDUCE", "3", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", "abc", "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res3);

            var res4 = db.StringSet("buzz", "abc");
            ClassicAssert.IsTrue(res4);

            var res5 = db.KeyDelete(["fizz", "buzz"]);
            ClassicAssert.AreEqual(2, res5);
        }

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "vectorManager")]
        private static extern ref VectorManager GetVectorManager(GarnetServer server);
    }
}
