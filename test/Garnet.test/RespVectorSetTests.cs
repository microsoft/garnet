// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

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
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true);

            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void DisabledWithFeatureFlag()
        {
            // Restart with Vector Sets disabled
            TearDown();

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, enableVectorSetPreview: false);

            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            ReadOnlySpan<RespCommand> vectorSetCommands = [RespCommand.VADD, RespCommand.VCARD, RespCommand.VDIM, RespCommand.VEMB, RespCommand.VGETATTR, RespCommand.VINFO, RespCommand.VISMEMBER, RespCommand.VLINKS, RespCommand.VRANDMEMBER, RespCommand.VREM, RespCommand.VSETATTR, RespCommand.VSIM];
            foreach (var cmd in vectorSetCommands)
            {
                // Should all fault before any validation
                var exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute(cmd.ToString()));
                ClassicAssert.AreEqual("ERR Vector Set (preview) commands are not enabled", exc.Message);
            }
        }

        [Test]
        public void OversizedRejected()
        {
            var options = GetOpts(server);

            var overflowSizeBytes = (int)(GarnetServerOptions.ParseSize(options.PageSize, out _) * 2);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var oversizedVectorData = Enumerable.Repeat<byte>(1, overflowSizeBytes).ToArray();
            var oversideAttribute = Enumerable.Repeat<byte>(2, overflowSizeBytes).ToArray();

            var exc1 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", ["foo", "XB8", oversizedVectorData, new byte[] { 0, 0, 0, 0 }, "XPREQ8"]));
            ClassicAssert.AreEqual("ERR Vector exceed configured page size", exc1.Message);

            var basicVectorData = Enumerable.Repeat<byte>(3, 75).ToArray();

            var exc2 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", ["foo", "XB8", basicVectorData, new byte[] { 0, 0, 0, 1 }, "XPREQ8", "SETATTR", oversideAttribute]));
            ClassicAssert.AreEqual("ERR Attribute exceed configured page size", exc2.Message);
        }

        [Test]
        public void WrongTypeForVectorSetOpsOnNonVectorSetKeys()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var vectorSetCommands = Enum.GetValues<RespCommand>().Where(static t => t.IsLegalOnVectorSet() && !(t is RespCommand.DEL or RespCommand.UNLINK or RespCommand.DEBUG or RespCommand.RENAME or RespCommand.RENAMENX or RespCommand.TYPE));

            // Strings
            {
                var res = db.StringSet("foo", "bar");
                ClassicAssert.IsTrue(res);

                foreach (var cmd in vectorSetCommands)
                {
                    RedisServerException exc;
                    switch (cmd)
                    {
                        case RespCommand.VADD:
                            exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]));
                            break;
                        case RespCommand.VCARD:
                            // TODO: Implement when VCARD works
                            continue;
                        case RespCommand.VDIM:
                            exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VDIM", ["foo"]));
                            break;
                        case RespCommand.VEMB:
                            exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VEMB", ["foo", new byte[] { 0, 0, 0, 0 }]));
                            break;
                        case RespCommand.VGETATTR:
                            // TODO: Implement when VGETATTR works
                            continue;
                        case RespCommand.VINFO:
                            exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VINFO", ["foo"]));
                            break;
                        case RespCommand.VISMEMBER:
                            // TODO: Implement when VISMEMBER works
                            continue;
                        case RespCommand.VLINKS:
                            // TODO: Implement when VLINKS works
                            continue;
                        case RespCommand.VRANDMEMBER:
                            // TODO: Implement when VRANDMEMBER works
                            continue;
                        case RespCommand.VREM:
                            exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VREM", ["foo", new byte[] { 0, 0, 0, 0 }]));
                            break;
                        case RespCommand.VSETATTR:
                            // TODO: Implement when VSETATTR works
                            continue;
                        case RespCommand.VSIM:
                            exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VSIM", ["foo", "VALUES", "75", "110.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "COUNT", "5", "EPSILON", "1.0", "EF", "40"]));
                            break;
                        default:
                            throw new InvalidOperationException($"Unexpected Vector Set command: {cmd}");
                    }

                    ClassicAssert.AreEqual("WRONGTYPE Operation against a key holding the wrong kind of value", exc.Message, $"RESP Command: {cmd}");
                }
            }

            // TODO: Other objects - but we can wait for store v2 for that
        }

        [Test]
        public void VADD()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // VALUES
            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "100.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 1, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res2);

            var float3 = new float[75];
            float3[0] = 5f;
            for (var i = 1; i < float3.Length; i++)
            {
                float3[i] = float3[i - 1] + 1;
            }

            // FP32
            var res3 = db.Execute("VADD", ["foo", "REDUCE", "50", "FP32", MemoryMarshal.Cast<float, byte>(float3).ToArray(), new byte[] { 2, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res3);

            var byte4 = new byte[75];
            byte4[0] = 9;
            for (var i = 1; i < byte4.Length; i++)
            {
                byte4[i] = (byte)(byte4[i - 1] + 1);
            }

            // XB8
            var res4 = db.Execute("VADD", ["foo", "REDUCE", "50", "XB8", byte4, new byte[] { 3, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res4);

            // TODO: exact duplicates - what does Redis do?

            // Add without specifying reductions after first vector
            var res5 = db.Execute("VADD", ["fizz", "REDUCE", "50", "VALUES", "75", "150.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res5);

            var exc1 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", ["fizz", "VALUES", "4", "5.0", "6.0", "7.0", "8.0", new byte[] { 0, 0, 0, 1 }, "CAS", "Q8", "EF", "16", "M", "32"]));
            ClassicAssert.AreEqual("ERR Vector dimension mismatch - got 4 but set has 75", exc1.Message);

            // Add without specifying quantization after first vector
            var res6 = db.Execute("VADD", ["fizz", "REDUCE", "50", "VALUES", "75", "160.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 2 }, "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res6);

            // Add without specifying EF after first vector
            var res7 = db.Execute("VADD", ["fizz", "REDUCE", "50", "VALUES", "75", "170.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 3 }, "CAS", "Q8", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res7);

            // Add without specifying M after first vector
            var exc2 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", ["fizz", "REDUCE", "50", "VALUES", "75", "180.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 4 }, "CAS", "Q8", "EF", "16"]));
            ClassicAssert.AreEqual("ERR asked M value mismatch with existing vector set", exc2.Message);

            // Mismatch vector size for projection
            var exc3 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", ["fizz", "REDUCE", "50", "VALUES", "5", "1.0", "2.0", "3.0", "4.0", "5.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]));
            ClassicAssert.AreEqual("ERR Vector dimension mismatch - got 5 but set has 75", exc3.Message);
        }

        [Test]
        public void VADDXPREQB8()
        {
            // Extra validation is required for this extension quantifier
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // REDUCE not allowed
            var exc1 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", ["fizz", "REDUCE", "2", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", new byte[] { 0, 0, 0, 0 }, "XPREQ8"]));
            ClassicAssert.AreEqual("ERR asked quantization mismatch with existing vector set", exc1.Message);

            // Create a vector set
            var res1 = db.Execute("VADD", ["fizz", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "XPREQ8"]);
            ClassicAssert.AreEqual(1, (int)res1);

            // Element name too short
            var exc2 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", ["fizz", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", new byte[] { 0 }, "XPREQ8"]));
            ClassicAssert.AreEqual("ERR Vector dimension mismatch - got 4 but set has 75", exc2.Message);

            // Element name too long
            var exc3 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", ["fizz", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 1, 2, 3, 4, }, "XPREQ8"]));
            ClassicAssert.AreEqual("ERR XPREQ8 requires 4-byte element ids", exc3.Message);
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

            _ = db.Execute("VADD", [vectorSetKey, "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 1, 0 }, "NOQUANT", "EF", "6", "M", "10"]);

            var exc16 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "2", "1.0", "2.0", "fizz", "NOQUANT", "EF", "6", "M", "10"]));
            ClassicAssert.AreEqual("ERR Vector dimension mismatch - got 2 but set has 75", exc16.Message);
            var exc17 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "fizz", "Q8", "EF", "6", "M", "10"]));
            ClassicAssert.AreEqual("ERR asked quantization mismatch with existing vector set", exc17.Message);
            var exc18 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "fizz", "NOQUANT", "EF", "12", "M", "20"]));
            ClassicAssert.AreEqual("ERR asked M value mismatch with existing vector set", exc18.Message);

            // TODO: Redis doesn't appear to validate attributes... so that's weird

            // Empty Vector Set keys are forbidden (TODO: Remove this constraint)
            var exc19 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", ["", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "XPREQ8"]));
            ClassicAssert.AreEqual("ERR Vector Set key cannot be empty", exc19.Message);
        }

        [Test]
        public void VEMB()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = (string[])db.Execute("VEMB", ["foo", new byte[] { 0, 0, 0, 0 }]);
            ClassicAssert.AreEqual(75, res2.Length);
            for (var i = 0; i < 75; i += 4)
            {
                ClassicAssert.AreEqual(float.Parse("1.0"), float.Parse(res2[i + 0]));
                if (i + 1 < res2.Length)
                {
                    ClassicAssert.AreEqual(float.Parse("2.0"), float.Parse(res2[i + 1]));
                }

                if (i + 2 < res2.Length)
                {
                    ClassicAssert.AreEqual(float.Parse("3.0"), float.Parse(res2[i + 2]));
                }

                if (i + 3 < res2.Length)
                {
                    ClassicAssert.AreEqual(float.Parse("4.0"), float.Parse(res2[i + 3]));
                }
            }

            var res3 = (string[])db.Execute("VEMB", ["foo", new byte[] { 0, 0, 0, 1 }]);
            ClassicAssert.AreEqual(0, res3.Length);
        }

        [Test]
        public void VectorSetOpacity()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
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

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = (string)db.StringGet(new byte[] { 0, 0, 0, 0 });
            ClassicAssert.IsNull(res2);

            var res3 = db.KeyDelete(new byte[] { 0, 0, 0, 0 });
            ClassicAssert.IsFalse(res3);

            var res4 = db.StringSet(new byte[] { 0, 0, 0, 0 }, "def", when: When.NotExists);
            ClassicAssert.IsTrue(res4);

            Span<byte> buffer = stackalloc byte[128];

            // Check we haven't messed up the element
            var res7 = (string[])db.Execute("VEMB", ["foo", new byte[] { 0, 0, 0, 0 }]);
            ClassicAssert.AreEqual(75, res7.Length);
            for (var i = 0; i < res7.Length; i++)
            {
                var expected =
                    (i % 4) switch
                    {
                        0 => float.Parse("1.0"),
                        1 => float.Parse("2.0"),
                        2 => float.Parse("3.0"),
                        3 => float.Parse("4.0"),
                        _ => throw new InvalidOperationException(),
                    };

                ClassicAssert.AreEqual(expected, float.Parse(res7[i]));
            }
        }

        [Test]
        public void VSIM()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "100.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 1 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res2);

            var res3 = (byte[][])db.Execute("VSIM", ["foo", "VALUES", "75", "110.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "COUNT", "5", "EPSILON", "1.0", "EF", "40"]);
            ClassicAssert.AreEqual(2, res3.Length);
            ClassicAssert.IsTrue(res3.Any(static x => x.SequenceEqual(new byte[] { 0, 0, 0, 0 })));
            ClassicAssert.IsTrue(res3.Any(static x => x.SequenceEqual(new byte[] { 0, 0, 0, 1 })));

            var res4 = (byte[][])db.Execute("VSIM", ["foo", "ELE", new byte[] { 0, 0, 0, 0 }, "COUNT", "5", "EPSILON", "1.0", "EF", "40"]);
            ClassicAssert.AreEqual(2, res4.Length);
            ClassicAssert.IsTrue(res4.Any(static x => x.SequenceEqual(new byte[] { 0, 0, 0, 0 })));
            ClassicAssert.IsTrue(res4.Any(static x => x.SequenceEqual(new byte[] { 0, 0, 0, 1 })));

            // FP32
            var float5 = new float[75];
            float5[0] = 3;
            for (var i = 1; i < float5.Length; i++)
            {
                float5[i] = float5[i - 1] + 0.1f;
            }
            var res5 = (byte[][])db.Execute("VSIM", ["foo", "FP32", MemoryMarshal.Cast<float, byte>(float5).ToArray(), "COUNT", "5", "EPSILON", "1.0", "EF", "40"]);
            ClassicAssert.AreEqual(2, res5.Length);
            ClassicAssert.IsTrue(res5.Any(static x => x.SequenceEqual(new byte[] { 0, 0, 0, 0 })));
            ClassicAssert.IsTrue(res5.Any(static x => x.SequenceEqual(new byte[] { 0, 0, 0, 1 })));

            // XB8
            var byte6 = new byte[75];
            byte6[0] = 10;
            for (var i = 1; i < byte6.Length; i++)
            {
                byte6[i] = (byte)(byte6[i - 1] + 1);
            }
            var res6 = (byte[][])db.Execute("VSIM", ["foo", "XB8", byte6, "COUNT", "5", "EPSILON", "1.0", "EF", "40"]);
            ClassicAssert.AreEqual(2, res6.Length);
            ClassicAssert.IsTrue(res6.Any(static x => x.SequenceEqual(new byte[] { 0, 0, 0, 0 })));
            ClassicAssert.IsTrue(res6.Any(static x => x.SequenceEqual(new byte[] { 0, 0, 0, 1 })));

            // COUNT > EF
            var byte7 = new byte[75];
            byte7[0] = 20;
            for (var i = 1; i < byte7.Length; i++)
            {
                byte7[i] = (byte)(byte7[i - 1] + 1);
            }
            var res7 = (byte[][])db.Execute("VSIM", ["foo", "XB8", byte7, "COUNT", "100", "EPSILON", "1.0", "EF", "40"]);
            ClassicAssert.AreEqual(2, res7.Length);
            ClassicAssert.IsTrue(res7.Any(static x => x.SequenceEqual(new byte[] { 0, 0, 0, 0 })));
            ClassicAssert.IsTrue(res7.Any(static x => x.SequenceEqual(new byte[] { 0, 0, 0, 1 })));

            // WITHSCORES
            var res8 = (byte[][])db.Execute("VSIM", ["foo", "XB8", byte7, "COUNT", "100", "EPSILON", "1.0", "EF", "40", "WITHSCORES"]);
            ClassicAssert.AreEqual(4, res8.Length);
            ClassicAssert.IsTrue(res8.Where(static (x, ix) => (ix % 2) == 0).Any(static x => x.SequenceEqual(new byte[] { 0, 0, 0, 0 })));
            ClassicAssert.IsTrue(res8.Where(static (x, ix) => (ix % 2) == 0).Any(static x => x.SequenceEqual(new byte[] { 0, 0, 0, 1 })));
            ClassicAssert.IsFalse(double.IsNaN(double.Parse(Encoding.UTF8.GetString(res8[1]))));
            ClassicAssert.IsFalse(double.IsNaN(double.Parse(Encoding.UTF8.GetString(res8[3]))));
        }

        [Test]
        public void VSIMWithAttribs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32", "SETATTR", "hello world"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "100.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 1 }, "CAS", "Q8", "EF", "16", "M", "32", "SETATTR", "fizz buzz"]);
            ClassicAssert.AreEqual(1, (int)res2);

            // Equivalent to no attribute
            var res3 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "110.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 2 }, "CAS", "Q8", "EF", "16", "M", "32", "SETATTR", ""]);
            ClassicAssert.AreEqual(1, (int)res3);

            // Actually no attribute
            var res4 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "120.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 3 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res4);

            // Very long attribute
            var bigAttr = Enumerable.Repeat((byte)'a', 1_024).ToArray();
            var res5 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "130.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 4 }, "CAS", "Q8", "EF", "16", "M", "32", "SETATTR", bigAttr]);
            ClassicAssert.AreEqual(1, (int)res5);

            var res6 = (byte[][])db.Execute("VSIM", ["foo", "VALUES", "75", "140.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "COUNT", "5", "EPSILON", "1.0", "EF", "40", "WITHATTRIBS"]);
            ClassicAssert.AreEqual(10, res6.Length);
            for (var i = 0; i < res6.Length; i += 2)
            {
                var id = res6[i];
                var attr = res6[i + 1];

                if (id.SequenceEqual(new byte[] { 0, 0, 0, 0 }))
                {
                    ClassicAssert.True(attr.SequenceEqual("hello world"u8.ToArray()));
                }
                else if (id.SequenceEqual(new byte[] { 0, 0, 0, 1 }))
                {
                    ClassicAssert.True(attr.SequenceEqual("fizz buzz"u8.ToArray()));
                }
                else if (id.SequenceEqual(new byte[] { 0, 0, 0, 2 }))
                {
                    ClassicAssert.AreEqual(0, attr.Length);
                }
                else if (id.SequenceEqual(new byte[] { 0, 0, 0, 3 }))
                {
                    ClassicAssert.AreEqual(0, attr.Length);
                }
                else if (id.SequenceEqual(new byte[] { 0, 0, 0, 4 }))
                {
                    ClassicAssert.True(bigAttr.SequenceEqual(attr));
                }
                else
                {
                    ClassicAssert.Fail("Unexpected id");
                }
            }

            // WITHSCORES
            var res7 = (byte[][])db.Execute("VSIM", ["foo", "VALUES", "75", "140.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "COUNT", "5", "EPSILON", "1.0", "EF", "40", "WITHATTRIBS", "WITHSCORES"]);
            ClassicAssert.AreEqual(15, res7.Length);
            for (var i = 0; i < res7.Length; i += 3)
            {
                var id = res7[i];
                var score = double.Parse(Encoding.UTF8.GetString(res7[i + 1]));
                var attr = res7[i + 2];

                ClassicAssert.IsFalse(double.IsNaN(score));

                if (id.SequenceEqual(new byte[] { 0, 0, 0, 0 }))
                {
                    ClassicAssert.True(attr.SequenceEqual("hello world"u8.ToArray()));
                }
                else if (id.SequenceEqual(new byte[] { 0, 0, 0, 1 }))
                {
                    ClassicAssert.True(attr.SequenceEqual("fizz buzz"u8.ToArray()));
                }
                else if (id.SequenceEqual(new byte[] { 0, 0, 0, 2 }))
                {
                    ClassicAssert.AreEqual(0, attr.Length);
                }
                else if (id.SequenceEqual(new byte[] { 0, 0, 0, 3 }))
                {
                    ClassicAssert.AreEqual(0, attr.Length);
                }
                else if (id.SequenceEqual(new byte[] { 0, 0, 0, 4 }))
                {
                    ClassicAssert.True(bigAttr.SequenceEqual(attr));
                }
                else
                {
                    ClassicAssert.Fail("Unexpected id");
                }
            }
        }

        [Test]
        public void VDIM()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "3", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VDIM", "foo");
            ClassicAssert.AreEqual(3, (int)res2);

            var res3 = db.Execute("VADD", ["bar", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res3);

            var res4 = db.Execute("VDIM", "bar");
            ClassicAssert.AreEqual(75, (int)res4);

            var exc1 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VDIM", "fizz"));
            ClassicAssert.IsTrue(exc1.Message.Contains("Key not found"));

            // TODO: Add WRONGTYPE behavior check once implemented
        }

        [Test]
        public void DeleteVectorSet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "3", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.KeyDelete("foo");
            ClassicAssert.IsTrue(res2);

            var res3 = db.Execute("VADD", ["fizz", "REDUCE", "3", "VALUES", "75", "100.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res3);

            var res4 = db.StringSet("buzz", "abc");
            ClassicAssert.IsTrue(res4);

            var res5 = db.KeyDelete(["fizz", "buzz"]);
            ClassicAssert.AreEqual(2, res5);
        }

        [Test]
        public void InteterruptedVectorSetDelete_AfterMark()
        => InterruptedVectorSetDelete(ExceptionInjectionType.VectorSet_Interrupt_Delete_0);

        [Test]
        public void InterruptedVectorSetDelete_AfterZeroingOut()
        => InterruptedVectorSetDelete(ExceptionInjectionType.VectorSet_Interrupt_Delete_1);

        [Test]
        public void InterruptedVectorSetDelete_AfterDelete()
        => InterruptedVectorSetDelete(ExceptionInjectionType.VectorSet_Interrupt_Delete_2);

        private void InterruptedVectorSetDelete(ExceptionInjectionType faultLocation)
        {
#if !DEBUG
            ClassicAssert.Ignore("Relies on ExceptionInjectionHelper, disable in non-DEBUG");
#endif

            var key = $"{nameof(InterruptedVectorSetDelete)}_{faultLocation}";

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase();

                var res1 = db.Execute("VADD", [key, "REDUCE", "3", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
                ClassicAssert.AreEqual(1, (int)res1);

                // TODO: we could use EXISTS here... except not all non-Vector Set commands understand Vector Sets, so that's a bit flaky
                ExceptionInjectionHelper.EnableException(faultLocation);
                try
                {
                    _ = ClassicAssert.Throws<RedisServerException>(() => db.KeyDelete(key));
                }
                finally
                {
                    ExceptionInjectionHelper.DisableException(faultLocation);
                }
            }

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase();

                var deleteWasEffective = false;

                try
                {
                    _ = (string)db.StringGet(key);
                    deleteWasEffective = true;
                }
                catch
                {
                }

                var vectorSetCommands = Enum.GetValues<RespCommand>().Where(static x => x.IsLegalOnVectorSet() && x is not (RespCommand.DEL or RespCommand.UNLINK or RespCommand.TYPE or RespCommand.DEBUG or RespCommand.RENAME or RespCommand.RENAMENX)).OrderBy(static x => x);

                if (!deleteWasEffective)
                {
                    // Check that all Vector Set commands on a partially deleted vector set give a reasonable error message OR succeed
                    //
                    // Success is possible if the delete failed early enough that we didn't actually being a "real" delete
                    //
                    // Such cases leave some trash around, but it'll be cleaned up either at restart or the next time a Vector Set is really deleted
                    foreach (var cmd in vectorSetCommands)
                    {
                        RedisServerException exc = null;
                        switch (cmd)
                        {
                            case RespCommand.VADD:
                                try
                                {
                                    var res = db.Execute("VADD", [key, "REDUCE", "3", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 1 }, "CAS", "Q8", "EF", "16", "M", "32"]);
                                    ClassicAssert.AreEqual(1, (int)res);
                                }
                                catch (RedisServerException e)
                                {
                                    exc = e;
                                }
                                break;
                            case RespCommand.VCARD:
                                // TODO: Implement once VCARD is implemented
                                continue;
                            case RespCommand.VDIM:
                                try
                                {
                                    var res = db.Execute("VDIM", [key]);
                                    ClassicAssert.AreEqual(3, (int)res);
                                }
                                catch (RedisServerException e)
                                {
                                    exc = e;
                                }
                                break;
                            case RespCommand.VEMB:
                                try
                                {
                                    var res = (string[])db.Execute("VEMB", [key, new byte[] { 0, 0, 0, 0 }]);
                                    ClassicAssert.AreEqual(75, res.Length);
                                }
                                catch (RedisServerException e)
                                {
                                    exc = e;
                                }
                                break;
                            case RespCommand.VGETATTR:
                                // TODO: Implement once VGETATTR is implemented
                                continue;
                            case RespCommand.VINFO:
                                try
                                {
                                    var res = (RedisValue[])db.Execute("VINFO", [key]);
                                    ClassicAssert.AreEqual(12, res.Length);
                                }
                                catch (RedisServerException e)
                                {
                                    exc = e;
                                }
                                break;
                            case RespCommand.VISMEMBER:
                                // TODO: Implement once VISMEMBER is implemented
                                continue;
                            case RespCommand.VLINKS:
                                // TODO: Implement once VLINKS is implemented
                                continue;
                            case RespCommand.VRANDMEMBER:
                                // TODO: Implement once VRANDMEMBER is implemented
                                continue;
                            case RespCommand.VREM:
                                try
                                {
                                    var res = db.Execute("VREM", [key, new byte[] { 0, 0, 0, 5 }]);
                                    ClassicAssert.AreEqual(0, (int)res);
                                }
                                catch (RedisServerException e)
                                {
                                    exc = e;
                                }
                                break;
                            case RespCommand.VSETATTR:
                                // TODO: Implement once VSETATTR is implemented
                                continue;
                            case RespCommand.VSIM:
                                try
                                {
                                    var res = (byte[][])db.Execute("VSIM", [key, "ELE", new byte[] { 0, 0, 0, 0 }]);
                                    ClassicAssert.IsTrue(res.Length > 0);
                                }
                                catch (RedisServerException e)
                                {
                                    exc = e;
                                }
                                break;
                            default:
                                Assert.Fail($"No test for command: {cmd}");
                                return;
                        }

                        if (exc != null)
                        {
                            ClassicAssert.AreEqual("ERR Vector Set is in a partially deleted state - re-execute DEL to complete deletion", exc.Message, $"For command: {cmd}");
                        }
                    }

                    // Delete again, this time we'll succeed
                    var delRes = db.KeyDelete(key);
                    ClassicAssert.IsTrue(delRes);
                }

                // Now accessing the key should give a null, no matter what happened
                var res2 = (string)db.StringGet(key);
                ClassicAssert.IsNull(res2);
            }
        }

        [Test]
        public void InteterruptedVectorSetDelete_AfterMark_Recovery()
        => InterruptedVectorSetDeleteRecovery(ExceptionInjectionType.VectorSet_Interrupt_Delete_0);

        [Test]
        public void InteterruptedVectorSetDelete_AfterZeroingOut_Recovery()
        => InterruptedVectorSetDeleteRecovery(ExceptionInjectionType.VectorSet_Interrupt_Delete_1);

        [Test]
        public void InteterruptedVectorSetDelete_AfterDelete_Recovery()
        => InterruptedVectorSetDeleteRecovery(ExceptionInjectionType.VectorSet_Interrupt_Delete_2);

        private void InterruptedVectorSetDeleteRecovery(ExceptionInjectionType faultLocation)
        {
#if !DEBUG
            ClassicAssert.Ignore("Relies on ExceptionInjectionHelper, disable in non-DEBUG");
#endif

            var key = $"{nameof(InterruptedVectorSetDeleteRecovery)}_{faultLocation}";

            // Create a partially deleted Vector Set, then take a checkpoint and shutdown
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                var res = db.Execute("VADD", [key, "REDUCE", "3", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
                ClassicAssert.AreEqual(1, (int)res);

                ExceptionInjectionHelper.EnableException(faultLocation);
                try
                {
                    _ = ClassicAssert.Throws<RedisServerException>(() => db.KeyDelete(key));
                }
                finally
                {
                    ExceptionInjectionHelper.DisableException(faultLocation);
                }
            }

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var s = redis.GetServers()[0];

#pragma warning disable CS0618 // Intentionally doing bad things
                s.Save(SaveType.ForegroundSave);
#pragma warning restore CS0618

                var commit = server.Store.WaitForCommit();
                ClassicAssert.IsTrue(commit);
            }

            // Restart Garnet, which should block applying any pending Vector Set deletes
            server.Dispose(deleteDir: false);

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true);
            server.Start();

            // Validate that Vector Set index key is gone, even if no Vector Set command ran
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                // Now accessing the key should give a null OR a WRONGTYPE (that still has data) if delete didn't get particularly far
                try
                {
                    var res = (string)db.StringGet(key);
                    ClassicAssert.IsNull(res);
                }
                catch (RedisServerException exc)
                {
                    ClassicAssert.IsTrue(exc.Message.StartsWith("WRONGTYPE "));

                    // If the value still exists, the Vector Set needs to still work
                    var res = (byte[][])db.Execute("VSIM", [key, "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0"]);
                    ClassicAssert.AreEqual(1, res.Length);
                }
            }
        }

        [Test]
        public void RepeatedVectorSetDeletes()
        {
            var bytes1 = new byte[75];
            var bytes2 = new byte[75];
            var bytes3 = new byte[75];
            bytes1[0] = 1;
            bytes2[0] = 75;
            bytes3[0] = 128;
            for (var i = 1; i < bytes1.Length; i++)
            {
                bytes1[i] = (byte)(bytes1[i - 1] + 1);
                bytes2[i] = (byte)(bytes2[i - 1] + 1);
                bytes3[i] = (byte)(bytes3[i - 1] + 1);
            }

            for (var i = 0; i < 1_000; i++)
            {
                using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
                {
                    var db = redis.GetDatabase();

                    var delRes = (int)db.Execute("DEL", ["foo"]);

                    if (i != 0)
                    {
                        ClassicAssert.AreEqual(1, delRes);
                    }
                    else
                    {
                        ClassicAssert.AreEqual(0, delRes);
                    }

                    var addRes1 = (int)db.Execute("VADD", ["foo", "XB8", bytes1, new byte[] { 0, 0, 0, 0 }, "XPREQ8"]);
                    ClassicAssert.AreEqual(1, addRes1);

                    var addRes2 = (int)db.Execute("VADD", ["foo", "XB8", bytes2, new byte[] { 0, 0, 0, 1 }, "XPREQ8"]);
                    ClassicAssert.AreEqual(1, addRes2);

                    var readExc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("GET", ["foo"]));
                    ClassicAssert.IsTrue(readExc.Message.Equals("WRONGTYPE Operation against a key holding the wrong kind of value."), $"In iteration: {i}");
                }

                // After an exception, get a clean connection
                using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
                {
                    var db = redis.GetDatabase();

                    var query = (byte[][])db.Execute("VSIM", ["foo", "XB8", bytes3]);

                    if (query is null)
                    {
                        try
                        {
                            var res = db.Execute("FOO");
                            Console.WriteLine($"After unexpected null, got: {res}");
                        }
                        catch { }
                    }
                    else if (query.Length != 2)
                    {
                        Console.WriteLine($"Wrong length {query.Length} != 2 response was");
                        for (var j = 0; j < query.Length; j++)
                        {
                            var txt = Encoding.UTF8.GetString(query[j]);
                            Console.WriteLine("---");
                            Console.WriteLine(txt);
                        }
                    }

                    ClassicAssert.AreEqual(2, query.Length, $"In iteration: {i}");
                }
            }
        }

        [Test]
        public unsafe void VectorReadBatchVariants()
        {
            // Single key, 4 byte keys
            {
                VectorInput input = default;
                input.Callback = 5678;
                input.CallbackContext = 9012;

                var data = new int[] { 4, 1234 };
                fixed (int* dataPtr = data)
                {
                    var keyData = SpanByte.FromPinnedPointer((byte*)dataPtr, data.Length * sizeof(int));
                    var batch = new VectorManager.VectorReadBatch(input.Callback, input.CallbackContext, 64, 1, keyData);

                    var iters = 0;
                    for (var i = 0; i < batch.Count; i++)
                    {
                        iters++;

                        // Validate Input
                        batch.GetInput(i, out var inputCopy);
                        ClassicAssert.AreEqual((nint)input.Callback, (nint)inputCopy.Callback);
                        ClassicAssert.AreEqual(input.CallbackContext, inputCopy.CallbackContext);
                        ClassicAssert.AreEqual(i, inputCopy.Index);

                        // Validate key
                        batch.GetKey(i, out var keyCopy);
                        ClassicAssert.AreEqual(64, keyCopy.GetNamespaceInPayload());
                        ClassicAssert.IsTrue(keyCopy.AsReadOnlySpan().SequenceEqual(MemoryMarshal.Cast<int, byte>(data.AsSpan().Slice(1, 1))));

                        // Validate output doesn't throw
                        batch.GetOutput(i, out _);
                    }

                    ClassicAssert.AreEqual(1, iters);
                }
            }

            // Multiple keys, 4 byte keys
            {
                VectorInput input = default;
                input.Callback = 5678;
                input.CallbackContext = 9012;

                var data = new int[] { 4, 1234, 4, 5678, 4, 0123, 4, 9999, 4, 0000, 4, int.MaxValue, 4, int.MinValue };
                fixed (int* dataPtr = data)
                {
                    var keyData = SpanByte.FromPinnedPointer((byte*)dataPtr, data.Length * sizeof(int));
                    var batch = new VectorManager.VectorReadBatch(input.Callback, input.CallbackContext, 32, 7, keyData);

                    var iters = 0;
                    for (var i = 0; i < batch.Count; i++)
                    {
                        iters++;

                        // Validate Input
                        batch.GetInput(i, out var inputCopy);
                        ClassicAssert.AreEqual((nint)input.Callback, (nint)inputCopy.Callback);
                        ClassicAssert.AreEqual(input.CallbackContext, inputCopy.CallbackContext);
                        ClassicAssert.AreEqual(i, inputCopy.Index);

                        // Validate key
                        batch.GetKey(i, out var keyCopy);
                        ClassicAssert.AreEqual(32, keyCopy.GetNamespaceInPayload());

                        var offset = i * 2 + 1;
                        var keyCopyData = keyCopy.AsReadOnlySpan();
                        var expectedData = MemoryMarshal.Cast<int, byte>(data.AsSpan().Slice(offset, 1));
                        ClassicAssert.IsTrue(keyCopyData.SequenceEqual(expectedData));

                        // Validate output doesn't throw
                        batch.GetOutput(i, out _);
                    }

                    ClassicAssert.AreEqual(7, iters);
                }
            }

            // Multiple keys, 4 byte keys, random order
            {
                VectorInput input = default;
                input.Callback = 5678;
                input.CallbackContext = 9012;

                var data = new int[] { 4, 1234, 4, 5678, 4, 0123, 4, 9999, 4, 0000, 4, int.MaxValue, 4, int.MinValue };
                fixed (int* dataPtr = data)
                {
                    var keyData = SpanByte.FromPinnedPointer((byte*)dataPtr, data.Length * sizeof(int));
                    var batch = new VectorManager.VectorReadBatch(input.Callback, input.CallbackContext, 16, 7, keyData);

                    var rand = new Random(2025_10_06_00);

                    for (var j = 0; j < 1_000; j++)
                    {
                        var i = rand.Next(batch.Count);

                        // Validate Input
                        batch.GetInput(i, out var inputCopy);
                        ClassicAssert.AreEqual((nint)input.Callback, (nint)inputCopy.Callback);
                        ClassicAssert.AreEqual(input.CallbackContext, inputCopy.CallbackContext);
                        ClassicAssert.AreEqual(i, inputCopy.Index);

                        // Validate key
                        batch.GetKey(i, out var keyCopy);
                        ClassicAssert.AreEqual(16, keyCopy.GetNamespaceInPayload());

                        var offset = i * 2 + 1;
                        var keyCopyData = keyCopy.AsReadOnlySpan();
                        var expectedData = MemoryMarshal.Cast<int, byte>(data.AsSpan().Slice(offset, 1));
                        ClassicAssert.IsTrue(keyCopyData.SequenceEqual(expectedData));

                        // Validate output doesn't throw
                        batch.GetOutput(i, out _);
                    }
                }
            }

            // Single key, variable length
            {
                VectorInput input = default;
                input.Callback = 5678;
                input.CallbackContext = 9012;

                var key0 = "hello"u8.ToArray();
                var data =
                    MemoryMarshal.Cast<int, byte>([key0.Length])
                        .ToArray()
                        .Concat(key0)
                        .ToArray();
                fixed (byte* dataPtr = data)
                {
                    var keyData = SpanByte.FromPinnedPointer((byte*)dataPtr, data.Length);
                    var batch = new VectorManager.VectorReadBatch(input.Callback, input.CallbackContext, 8, 1, keyData);

                    var iters = 0;
                    for (var i = 0; i < batch.Count; i++)
                    {
                        iters++;

                        // Validate Input
                        batch.GetInput(i, out var inputCopy);
                        ClassicAssert.AreEqual((nint)input.Callback, (nint)inputCopy.Callback);
                        ClassicAssert.AreEqual(input.CallbackContext, inputCopy.CallbackContext);
                        ClassicAssert.AreEqual(i, inputCopy.Index);

                        // Validate key
                        var expectedLength =
                            i switch
                            {
                                0 => key0.Length,
                                _ => throw new InvalidOperationException("Unexpected index"),
                            };
                        var expectedStart =
                            i switch
                            {
                                0 => 0 + 1 * sizeof(int),
                                _ => throw new InvalidOperationException("Unexpected index"),
                            };

                        batch.GetKey(i, out var keyCopy);
                        ClassicAssert.AreEqual(8, keyCopy.GetNamespaceInPayload());
                        var keyCopyData = keyCopy.AsReadOnlySpan();
                        var expectedData = data.AsSpan().Slice(expectedStart, expectedLength);
                        ClassicAssert.IsTrue(expectedData.SequenceEqual(keyCopyData));

                        // Validate output doesn't throw
                        batch.GetOutput(i, out _);
                    }

                    ClassicAssert.AreEqual(1, iters);
                }
            }

            // Multiple keys, variable length
            {
                VectorInput input = default;
                input.Callback = 5678;
                input.CallbackContext = 9012;

                var key0 = "hello"u8.ToArray();
                var key1 = "fizz"u8.ToArray();
                var key2 = "the quick brown fox jumps over the lazy dog"u8.ToArray();
                var key3 = "CF29E323-E376-4BC4-AB63-FCFD371EB445"u8.ToArray();
                var key4 = Array.Empty<byte>();
                var key5 = new byte[] { 1 };
                var key6 = new byte[] { 2, 3 };
                var key7 = new byte[] { 4, 5, 6 };
                var data =
                    MemoryMarshal.Cast<int, byte>([key0.Length])
                        .ToArray()
                        .Concat(key0)
                        .Concat(
                            MemoryMarshal.Cast<int, byte>([key1.Length]).ToArray()
                        )
                        .Concat(
                            key1
                        )
                        .Concat(
                            MemoryMarshal.Cast<int, byte>([key2.Length]).ToArray()
                        )
                        .Concat(
                            key2
                        )
                        .Concat(
                            MemoryMarshal.Cast<int, byte>([key3.Length]).ToArray()
                        )
                        .Concat(
                            key3
                        )
                        .Concat(
                            MemoryMarshal.Cast<int, byte>([key4.Length]).ToArray()
                        )
                        .Concat(
                            key4
                        )
                        .Concat(
                            MemoryMarshal.Cast<int, byte>([key5.Length]).ToArray()
                        )
                        .Concat(
                            key5
                        )
                        .Concat(
                            MemoryMarshal.Cast<int, byte>([key6.Length]).ToArray()
                        )
                        .Concat(
                            key6
                        )
                        .Concat(
                            MemoryMarshal.Cast<int, byte>([key7.Length]).ToArray()
                        )
                        .Concat(
                            key7
                        )
                        .ToArray();
                fixed (byte* dataPtr = data)
                {
                    var keyData = SpanByte.FromPinnedPointer((byte*)dataPtr, data.Length);
                    var batch = new VectorManager.VectorReadBatch(input.Callback, input.CallbackContext, 4, 8, keyData);

                    var iters = 0;
                    for (var i = 0; i < batch.Count; i++)
                    {
                        iters++;

                        // Validate Input
                        batch.GetInput(i, out var inputCopy);
                        ClassicAssert.AreEqual((nint)input.Callback, (nint)inputCopy.Callback);
                        ClassicAssert.AreEqual(input.CallbackContext, inputCopy.CallbackContext);
                        ClassicAssert.AreEqual(i, inputCopy.Index);

                        // Validate key
                        var expectedLength =
                            i switch
                            {
                                0 => key0.Length,
                                1 => key1.Length,
                                2 => key2.Length,
                                3 => key3.Length,
                                4 => key4.Length,
                                5 => key5.Length,
                                6 => key6.Length,
                                7 => key7.Length,
                                _ => throw new InvalidOperationException("Unexpected index"),
                            };
                        var expectedStart =
                            i switch
                            {
                                0 => 0 + 1 * sizeof(int),
                                1 => key0.Length + 2 * sizeof(int),
                                2 => key0.Length + key1.Length + 3 * sizeof(int),
                                3 => key0.Length + key1.Length + key2.Length + 4 * sizeof(int),
                                4 => key0.Length + key1.Length + key2.Length + key3.Length + 5 * sizeof(int),
                                5 => key0.Length + key1.Length + key2.Length + key3.Length + key4.Length + 6 * sizeof(int),
                                6 => key0.Length + key1.Length + key2.Length + key3.Length + key4.Length + key5.Length + 7 * sizeof(int),
                                7 => key0.Length + key1.Length + key2.Length + key3.Length + key4.Length + key5.Length + key6.Length + 8 * sizeof(int),
                                _ => throw new InvalidOperationException("Unexpected index"),
                            };

                        batch.GetKey(i, out var keyCopy);
                        ClassicAssert.AreEqual(4, keyCopy.GetNamespaceInPayload());
                        var keyCopyData = keyCopy.AsReadOnlySpan();
                        var expectedData = data.AsSpan().Slice(expectedStart, expectedLength);
                        ClassicAssert.IsTrue(expectedData.SequenceEqual(keyCopyData));

                        // Validate output doesn't throw
                        batch.GetOutput(i, out _);
                    }

                    ClassicAssert.AreEqual(8, iters);
                }
            }

            // Multiple keys, variable length, random access
            {
                VectorInput input = default;
                input.Callback = 5678;
                input.CallbackContext = 9012;

                var key0 = "hello"u8.ToArray();
                var key1 = "fizz"u8.ToArray();
                var key2 = "the quick brown fox jumps over the lazy dog"u8.ToArray();
                var key3 = "CF29E323-E376-4BC4-AB63-FCFD371EB445"u8.ToArray();
                var key4 = Array.Empty<byte>();
                var key5 = new byte[] { 1 };
                var key6 = new byte[] { 2, 3 };
                var key7 = new byte[] { 4, 5, 6 };
                var data =
                    MemoryMarshal.Cast<int, byte>([key0.Length])
                        .ToArray()
                        .Concat(key0)
                        .Concat(
                            MemoryMarshal.Cast<int, byte>([key1.Length]).ToArray()
                        )
                        .Concat(
                            key1
                        )
                        .Concat(
                            MemoryMarshal.Cast<int, byte>([key2.Length]).ToArray()
                        )
                        .Concat(
                            key2
                        )
                        .Concat(
                            MemoryMarshal.Cast<int, byte>([key3.Length]).ToArray()
                        )
                        .Concat(
                            key3
                        )
                        .Concat(
                            MemoryMarshal.Cast<int, byte>([key4.Length]).ToArray()
                        )
                        .Concat(
                            key4
                        )
                        .Concat(
                            MemoryMarshal.Cast<int, byte>([key5.Length]).ToArray()
                        )
                        .Concat(
                            key5
                        )
                        .Concat(
                            MemoryMarshal.Cast<int, byte>([key6.Length]).ToArray()
                        )
                        .Concat(
                            key6
                        )
                        .Concat(
                            MemoryMarshal.Cast<int, byte>([key7.Length]).ToArray()
                        )
                        .Concat(
                            key7
                        )
                        .ToArray();
                fixed (byte* dataPtr = data)
                {
                    var keyData = SpanByte.FromPinnedPointer((byte*)dataPtr, data.Length);
                    var batch = new VectorManager.VectorReadBatch(input.Callback, input.CallbackContext, 4, 8, keyData);

                    var rand = new Random(2025_10_06_01);

                    for (var j = 0; j < 1_000; j++)
                    {
                        var i = rand.Next(batch.Count);

                        // Validate Input
                        batch.GetInput(i, out var inputCopy);
                        ClassicAssert.AreEqual((nint)input.Callback, (nint)inputCopy.Callback);
                        ClassicAssert.AreEqual(input.CallbackContext, inputCopy.CallbackContext);
                        ClassicAssert.AreEqual(i, inputCopy.Index);

                        // Validate key
                        var expectedLength =
                            i switch
                            {
                                0 => key0.Length,
                                1 => key1.Length,
                                2 => key2.Length,
                                3 => key3.Length,
                                4 => key4.Length,
                                5 => key5.Length,
                                6 => key6.Length,
                                7 => key7.Length,
                                _ => throw new InvalidOperationException("Unexpected index"),
                            };
                        var expectedStart =
                            i switch
                            {
                                0 => 0 + 1 * sizeof(int),
                                1 => key0.Length + 2 * sizeof(int),
                                2 => key0.Length + key1.Length + 3 * sizeof(int),
                                3 => key0.Length + key1.Length + key2.Length + 4 * sizeof(int),
                                4 => key0.Length + key1.Length + key2.Length + key3.Length + 5 * sizeof(int),
                                5 => key0.Length + key1.Length + key2.Length + key3.Length + key4.Length + 6 * sizeof(int),
                                6 => key0.Length + key1.Length + key2.Length + key3.Length + key4.Length + key5.Length + 7 * sizeof(int),
                                7 => key0.Length + key1.Length + key2.Length + key3.Length + key4.Length + key5.Length + key6.Length + 8 * sizeof(int),
                                _ => throw new InvalidOperationException("Unexpected index"),
                            };

                        batch.GetKey(i, out var keyCopy);
                        ClassicAssert.AreEqual(4, keyCopy.GetNamespaceInPayload());
                        var keyCopyData = keyCopy.AsReadOnlySpan();
                        var expectedData = data.AsSpan().Slice(expectedStart, expectedLength);
                        ClassicAssert.IsTrue(expectedData.SequenceEqual(keyCopyData));

                        // Validate output doesn't throw
                        batch.GetOutput(i, out _);
                    }
                }
            }
        }

        [Test]
        public void RecreateIndexesOnRestore()
        {
            var addData1 = Enumerable.Range(0, 75).Select(static x => (byte)x).ToArray();
            var addData2 = Enumerable.Range(0, 75).Select(static x => (byte)(x * 2)).ToArray();
            var queryData = addData1.ToArray();
            queryData[0]++;

            // VADD
            {
                using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
                {
                    var s = redis.GetServers()[0];
                    var db = redis.GetDatabase(0);

                    _ = db.KeyDelete("foo");

                    var res1 = db.Execute("VADD", ["foo", "XB8", addData1, new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32", "SETATTR", "hello world"]);
                    ClassicAssert.AreEqual(1, (int)res1);

#pragma warning disable CS0618 // Intentionally doing bad things
                    s.Save(SaveType.ForegroundSave);
#pragma warning restore CS0618

                    var commit = server.Store.WaitForCommit();
                    ClassicAssert.IsTrue(commit);
                    server.Dispose(deleteDir: false);

                    server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true);
                    server.Start();
                }

                using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
                {
                    var db = redis.GetDatabase(0);

                    var res2 = db.Execute("VADD", ["foo", "XB8", addData2, new byte[] { 0, 0, 0, 1 }, "CAS", "Q8", "EF", "16", "M", "32", "SETATTR", "fizz buzz"]);
                    ClassicAssert.AreEqual(1, (int)res2);
                }
            }

            // VSIM with vector
            {
                byte[][] expectedVSimResult;
                using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
                {
                    var s = redis.GetServers()[0];
                    var db = redis.GetDatabase(0);

                    _ = db.KeyDelete("foo");

                    var res1 = db.Execute("VADD", ["foo", "XB8", addData1, new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32", "SETATTR", "hello world"]);
                    ClassicAssert.AreEqual(1, (int)res1);

                    expectedVSimResult = (byte[][])db.Execute("VSIM", ["foo", "XB8", queryData]);
                    ClassicAssert.AreEqual(1, expectedVSimResult.Length);
#pragma warning disable CS0618 // Intentionally doing bad things
                    s.Save(SaveType.ForegroundSave);
#pragma warning restore CS0618

                    var commit = server.Store.WaitForCommit();
                    ClassicAssert.IsTrue(commit);
                    server.Dispose(deleteDir: false);

                    server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true);
                    server.Start();
                }

                using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
                {
                    var db = redis.GetDatabase(0);

                    var res2 = (byte[][])db.Execute("VSIM", ["foo", "XB8", queryData]);
                    ClassicAssert.AreEqual(expectedVSimResult.Length, res2.Length);
                    for (var i = 0; i < res2.Length; i++)
                    {
                        ClassicAssert.IsTrue(expectedVSimResult[i].AsSpan().SequenceEqual(res2[i]));
                    }
                }
            }

            // VSIM with element
            {
                byte[][] expectedVSimResult;
                using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
                {
                    var s = redis.GetServers()[0];
                    var db = redis.GetDatabase(0);

                    _ = db.KeyDelete("foo");

                    var res1 = db.Execute("VADD", ["foo", "XB8", addData1, new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32", "SETATTR", "hello world"]);
                    ClassicAssert.AreEqual(1, (int)res1);

                    var res2 = db.Execute("VADD", ["foo", "XB8", addData2, new byte[] { 0, 0, 0, 1 }, "CAS", "Q8", "EF", "16", "M", "32", "SETATTR", "hello world"]);
                    ClassicAssert.AreEqual(1, (int)res1);

                    expectedVSimResult = (byte[][])db.Execute("VSIM", ["foo", "ELE", new byte[] { 0, 0, 0, 0 }]);
                    ClassicAssert.AreEqual(2, expectedVSimResult.Length);
#pragma warning disable CS0618 // Intentionally doing bad things
                    s.Save(SaveType.ForegroundSave);
#pragma warning restore CS0618

                    var commit = server.Store.WaitForCommit();
                    ClassicAssert.IsTrue(commit);
                    server.Dispose(deleteDir: false);

                    server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true);
                    server.Start();
                }

                using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
                {
                    var db = redis.GetDatabase(0);

                    var res2 = (byte[][])db.Execute("VSIM", ["foo", "ELE", new byte[] { 0, 0, 0, 0 }]);
                    ClassicAssert.AreEqual(expectedVSimResult.Length, res2.Length);
                    for (var i = 0; i < res2.Length; i++)
                    {
                        ClassicAssert.IsTrue(expectedVSimResult[i].AsSpan().SequenceEqual(res2[i]));
                    }
                }
            }

            // VDIM
            {
                using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
                {
                    var s = redis.GetServers()[0];
                    var db = redis.GetDatabase(0);

                    _ = db.KeyDelete("foo");

                    var res1 = db.Execute("VADD", ["foo", "XB8", addData1, new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32", "SETATTR", "hello world"]);
                    ClassicAssert.AreEqual(1, (int)res1);

#pragma warning disable CS0618 // Intentionally doing bad things
                    s.Save(SaveType.ForegroundSave);
#pragma warning restore CS0618

                    var commit = server.Store.WaitForCommit();
                    ClassicAssert.IsTrue(commit);
                    server.Dispose(deleteDir: false);

                    server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true);
                    server.Start();
                }

                using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
                {
                    var db = redis.GetDatabase(0);

                    var res2 = (int)db.Execute("VDIM", ["foo"]);
                    ClassicAssert.AreEqual(addData1.Length, res2);
                }
            }

            // VEMB
            {
                using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
                {
                    var s = redis.GetServers()[0];
                    var db = redis.GetDatabase(0);

                    _ = db.KeyDelete("foo");

                    var res1 = db.Execute("VADD", ["foo", "XB8", addData1, new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32", "SETATTR", "hello world"]);
                    ClassicAssert.AreEqual(1, (int)res1);

#pragma warning disable CS0618 // Intentionally doing bad things
                    s.Save(SaveType.ForegroundSave);
#pragma warning restore CS0618

                    var commit = server.Store.WaitForCommit();
                    ClassicAssert.IsTrue(commit);
                    server.Dispose(deleteDir: false);

                    server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true);
                    server.Start();
                }

                using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
                {
                    var db = redis.GetDatabase(0);

                    var res2 = (string[])db.Execute("VEMB", ["foo", new byte[] { 0, 0, 0, 0 }]);
                    ClassicAssert.AreEqual(res2.Length, addData1.Length);

                    for (var i = 0; i < res2.Length; i++)
                    {
                        ClassicAssert.AreEqual((float)addData1[i], float.Parse(res2[i]));
                    }
                }
            }

            // VREM
            {
                using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
                {
                    var s = redis.GetServers()[0];
                    var db = redis.GetDatabase(0);

                    _ = db.KeyDelete("foo");

                    var res1 = db.Execute("VADD", ["foo", "XB8", addData1, new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32", "SETATTR", "hello world"]);
                    ClassicAssert.AreEqual(1, (int)res1);

                    var res2 = db.Execute("VADD", ["foo", "XB8", addData2, new byte[] { 0, 0, 0, 1 }, "CAS", "Q8", "EF", "16", "M", "32", "SETATTR", "hello world"]);
                    ClassicAssert.AreEqual(1, (int)res1);

#pragma warning disable CS0618 // Intentionally doing bad things
                    s.Save(SaveType.ForegroundSave);
#pragma warning restore CS0618

                    var commit = server.Store.WaitForCommit();
                    ClassicAssert.IsTrue(commit);
                    server.Dispose(deleteDir: false);

                    server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true);
                    server.Start();
                }

                using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
                {
                    var db = redis.GetDatabase(0);

                    var res1 = (int)db.Execute("VREM", ["foo", new byte[] { 0, 0, 0, 0 }]);
                    ClassicAssert.AreEqual(1, res1);

                    var res2 = (string[])db.Execute("VEMB", ["foo", new byte[] { 0, 0, 0, 1 }]);
                    ClassicAssert.AreEqual(res2.Length, addData1.Length);

                    for (var i = 0; i < res2.Length; i++)
                    {
                        ClassicAssert.AreEqual((float)addData2[i], float.Parse(res2[i]));
                    }
                }
            }
        }

        // TODO: FLUSHDB needs to cleanup too...

        [Test]
        public void VINFO_NotFound()
        {
            // VINFO NotFound response depends on the RESP version used:
            // - Resp3: Null
            // - Resp2: Null array reply
            using var redisResp3 = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: RedisProtocol.Resp3));
            var resp3Result = redisResp3.GetDatabase().Execute("VINFO", ["nonexistent"]);
            ClassicAssert.IsTrue(resp3Result.IsNull);
            ClassicAssert.IsTrue(resp3Result.Resp3Type == ResultType.Null);

            using var redisResp2 = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: RedisProtocol.Resp2));
            var resp2Result = redisResp2.GetDatabase().Execute("VINFO", ["nonexistent"]);
            ClassicAssert.IsTrue(resp2Result.IsNull);
            ClassicAssert.IsTrue(resp2Result.Resp2Type == ResultType.Array);
        }

        [Test]
        public void VINFO()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            string[] quantizers = ["XPREQ8", "NOQUANT", "Q8", "BIN"];
            int[] reduceValues = [0, 5];
            int[] efValues = [0, 8];
            int[] mValues = [0, 16];
            int[] vectorDimensions = [9, 10];
            var testCnt = 0;

            foreach (var quantizer in quantizers)
            {
                var expectedQuantType = quantizer == "NOQUANT" ?
                    "f32" : quantizer.ToLower();

                foreach (var reduceValue in reduceValues)
                {
                    var reduceValueToUse = quantizer == "XPREQ8" ? 0 : reduceValue;
                    foreach (var ef in efValues)
                    {
                        foreach (var numLinks in mValues)
                        {
                            foreach (var vectorDim in vectorDimensions)
                            {
                                testCnt++;
                                string fooKey = $"foo:{testCnt}";

                                // Generate vector values based on dimension
                                var vectorValues = new List<object> { "VALUES", vectorDim.ToString() };
                                for (int i = 1; i <= vectorDim; i++)
                                {
                                    vectorValues.Add($"{i}.0");
                                }

                                // Create a vector set with known parameters
                                var res = db.Execute("VADD", GenerateVADDOptions(fooKey, quantizer, reduceValueToUse, ef, numLinks, vectorValues.ToArray(), [0, 0, 0, 0]));
                                ClassicAssert.AreEqual(1, (int)res);

                                string expectedEf = ef == 0 ? "200" : ef.ToString();
                                string expectedNumLinks = numLinks == 0 ? "16" : numLinks.ToString();

                                // Get VINFO - should return an array of 12 elements (6 key-value pairs)
                                var vinfoRes = (RedisValue[])db.Execute("VINFO", [fooKey]);
                                ClassicAssert.AreEqual(12, vinfoRes.Length);
                                var values = BuildDictionaryFromResponse(vinfoRes);
                                ClassicAssert.AreEqual(values["quant-type"], expectedQuantType);
                                ClassicAssert.AreEqual(values["input-vector-dimensions"], vectorDim.ToString());
                                ClassicAssert.AreEqual(values["reduced-dimensions"], reduceValueToUse.ToString());
                                ClassicAssert.AreEqual(values["build-exploration-factor"], expectedEf);
                                ClassicAssert.AreEqual(values["num-links"], expectedNumLinks);
                                // TODO: Update once DiskANN exposes card()
                                ClassicAssert.AreEqual(values["size"], "0");

                                // Modify first value for second add (change from "1.0" to "2.0")
                                vectorValues[2] = "2.0";

                                // Add another element and try again
                                res = db.Execute("VADD", GenerateVADDOptions(fooKey, quantizer, reduceValueToUse, ef, numLinks, vectorValues.ToArray(), [0, 0, 0, 1]));
                                ClassicAssert.AreEqual(1, (int)res);

                                vinfoRes = (RedisValue[])db.Execute(command: "VINFO", [fooKey]);
                                ClassicAssert.AreEqual(12, vinfoRes.Length);
                                values = BuildDictionaryFromResponse(vinfoRes);
                                ClassicAssert.AreEqual(values["quant-type"], expectedQuantType);
                                ClassicAssert.AreEqual(values["input-vector-dimensions"], vectorDim.ToString());
                                ClassicAssert.AreEqual(values["reduced-dimensions"], reduceValueToUse.ToString());
                                ClassicAssert.AreEqual(values["build-exploration-factor"], expectedEf);
                                ClassicAssert.AreEqual(values["num-links"], expectedNumLinks);
                                // TODO: Update once DiskANN exposes card()
                                ClassicAssert.AreEqual(values["size"], "0");

                                // Delete vector set
                                db.KeyDelete(fooKey);
                            }
                        }
                    }
                }
            }

            static object[] GenerateVADDOptions(string key, string quantizer, int reduce, int buildExplorationFactor, int numLinks, object[] values, byte[] elementId)
            {
                if (quantizer == "XPREQ8")
                {
                    reduce = 0;
                }

                List<object> opts = [key];
                if (reduce > 0)
                {
                    opts.Add("REDUCE");
                    opts.Add(reduce.ToString());
                }

                opts.AddRange(values);
                opts.Add(elementId);
                opts.Add(quantizer);
                if (buildExplorationFactor > 0)
                {
                    opts.Add("EF");
                    opts.Add(buildExplorationFactor.ToString());
                }

                if (numLinks > 0)
                {
                    opts.Add("M");
                    opts.Add(numLinks.ToString());
                }

                return opts.ToArray();
            }

            static Dictionary<string, string> BuildDictionaryFromResponse(RedisValue[] response)
            {
                Dictionary<string, string> values = new();
                for (var i = 0; i < response.Length; i += 2)
                {
                    values[response[i]] = response[i + 1];
                }

                return values;
            }
        }

        [Test]
        public void VREM()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Populate
            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "100.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 1, 0, 0, 0 }, "CAS", "Q8", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res2);

            // Remove on non-vector set fails
            // TODO: test against Redis, how do they respond (I expect WRONGTYPE, but needs verification)
            //_ = db.StringSet("fizz", "buzz");
            //var exc1 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VREM", "fizz", new byte[] { 0, 0, 0, 0 }));
            //ClassicAssert.AreEqual("", exc1.Message);

            // Remove exists
            var res3 = db.Execute("VREM", ["foo", new byte[] { 0, 0, 0, 0 }]);
            ClassicAssert.AreEqual(1, (int)res3);

            // Remove again fails
            var res4 = db.Execute("VREM", ["foo", new byte[] { 0, 0, 0, 0 }]);
            ClassicAssert.AreEqual(0, (int)res4);

            // Remove not present
            var res5 = db.Execute("VREM", ["foo", new byte[] { 1, 2, 3, 4 }]);
            ClassicAssert.AreEqual(0, (int)res5);

            // VSIM doesn't return removed element
            var res6 = (byte[][])db.Execute("VSIM", ["foo", "VALUES", "75", "110.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "COUNT", "5", "EPSILON", "1.0", "EF", "40"]);
            ClassicAssert.AreEqual(1, res6.Length);
            ClassicAssert.IsTrue(res6.Any(static x => x.SequenceEqual(new byte[] { 1, 0, 0, 0 })));
        }

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "opts")]
        private static extern ref GarnetServerOptions GetOpts(GarnetServer server);
    }
}