// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    [TestFixture(0)]
    [TestFixture(1_000)]
    public class RespVectorSetTests : TestBase
    {
        private const string DefaultAOFMemorySize = "2g";  // Very large because CI boxes have low IOPS, so try and flush to disk veeeeeery rarely

        private readonly int preAllocatedContexts;

        GarnetServer server;

        public RespVectorSetTests(int preAllocatedContexts)
        {
            this.preAllocatedContexts = preAllocatedContexts;
        }

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = CreateGarnetServer(tryRecover: false);

            server.Start();

            server.Provider.StoreWrapper.DefaultDatabase.VectorManager.AllocateTestContexts(preAllocatedContexts);
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        public void DisabledWithFeatureFlag()
        {
            // Restart with Vector Sets disabled
            TearDown();

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = CreateGarnetServer(tryRecover: false, enableVectorSetPreview: false);

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
                            exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]));
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
                            exc = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VGETATTR", ["foo", new byte[] { 0, 0, 0, 0 }]));
                            break;
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
            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "100.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 1, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res2);

            var float3 = new float[75];
            float3[0] = 5f;
            for (var i = 1; i < float3.Length; i++)
            {
                float3[i] = float3[i - 1] + 1;
            }

            // FP32
            var res3 = db.Execute("VADD", ["foo", "REDUCE", "50", "FP32", MemoryMarshal.Cast<float, byte>(float3).ToArray(), new byte[] { 2, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res3);

            var byte4 = new byte[75];
            byte4[0] = 9;
            for (var i = 1; i < byte4.Length; i++)
            {
                byte4[i] = (byte)(byte4[i - 1] + 1);
            }

            // XB8
            var res4 = db.Execute("VADD", ["foo", "REDUCE", "50", "XB8", byte4, new byte[] { 3, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res4);

            // TODO: exact duplicates - what does Redis do?

            // Add without specifying reductions after first vector
            var res5 = db.Execute("VADD", ["fizz", "REDUCE", "50", "VALUES", "75", "150.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res5);

            var exc1 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", ["fizz", "VALUES", "4", "5.0", "6.0", "7.0", "8.0", new byte[] { 0, 0, 0, 1 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]));
            ClassicAssert.AreEqual("ERR Vector dimension mismatch - got 4 but set has 75", exc1.Message);

            // Add without specifying EF after first vector
            var res6 = db.Execute("VADD", ["fizz", "REDUCE", "50", "VALUES", "75", "170.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 3 }, "CAS", "NOQUANT", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res6);

            // Add without specifying M after first vector
            var exc2 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", ["fizz", "REDUCE", "50", "VALUES", "75", "180.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 4 }, "CAS", "NOQUANT", "EF", "16"]));
            ClassicAssert.AreEqual("ERR asked M value mismatch with existing vector set", exc2.Message);

            // Mismatch vector size for projection
            var exc3 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", ["fizz", "REDUCE", "50", "VALUES", "5", "1.0", "2.0", "3.0", "4.0", "5.0", new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]));
            ClassicAssert.AreEqual("ERR REDUCE dimension must be <= vector dimensions", exc3.Message);
        }

        [Test]
        public void VADDVariableLengthElementIds()
        {
            const int MinElementLength = 1;
            const int MaxElementLength = 1024;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Always put a 0 length in as a stress test
            List<byte[]> ids = [[]];
            for (var len = MinElementLength; len <= MaxElementLength; len *= 2)
            {
                ids.Add(Enumerable.Range(0, len).Select(_ => (byte)len).ToArray());
            }

            foreach (var id in ids)
            {
                var addRes = (int)db.Execute("VADD", ["foo", "VALUES", "1", ((float)(byte)id.Length).ToString(), id, "XPREQ8"]);
                ClassicAssert.AreEqual(1, addRes);
            }

            foreach (var id in ids)
            {
                var embRes = (string[])db.Execute("VEMB", ["foo", id]);
                ClassicAssert.AreEqual(1, embRes.Length);
                ClassicAssert.AreEqual((float)(byte)id.Length, float.Parse(embRes[0]));
            }
        }

        [Test]
        public void VADDXPREQB8()
        {
            // Extra validation is required for this extension quantifier
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Build byte array for vector data (75 bytes)
            var vectorData1 = new byte[75];
            vectorData1[0] = 1;
            for (var i = 1; i < vectorData1.Length; i++)
            {
                vectorData1[i] = (byte)(vectorData1[i - 1] + 1);
            }

            var vectorData2 = new byte[75];
            vectorData2[0] = 100;
            for (var i = 1; i < vectorData2.Length; i++)
            {
                vectorData2[i] = (byte)(vectorData2[i - 1] + 1);
            }

            // Small vector for REDUCE test
            var smallVectorData = new byte[4];
            for (var i = 0; i < smallVectorData.Length; i++)
            {
                smallVectorData[i] = (byte)(i + 1);
            }

            // REDUCE not allowed with XPREQ8
            var exc1 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", ["fizz", "REDUCE", "2", "XB8", smallVectorData, new byte[] { 0, 0, 0, 0 }, "XPREQ8"]));
            ClassicAssert.AreEqual("ERR asked quantization mismatch with existing vector set", exc1.Message);

            // Create a vector set with XB8 + XPREQ8
            var res1 = db.Execute("VADD", ["fizz", "XB8", vectorData1, new byte[] { 0, 0, 0, 0 }, "XPREQ8"]);
            ClassicAssert.AreEqual(1, (int)res1);

            // Add another element
            var res2 = db.Execute("VADD", ["fizz", "XB8", vectorData2, new byte[] { 0, 0, 0, 1 }, "XPREQ8"]);
            ClassicAssert.AreEqual(1, (int)res2);

            // Verify the vector was stored correctly
            var embRes = (string[])db.Execute("VEMB", ["fizz", new byte[] { 0, 0, 0, 0 }]);
            ClassicAssert.AreEqual(75, embRes.Length);
            for (var i = 0; i < embRes.Length; i++)
            {
                ClassicAssert.AreEqual((float)vectorData1[i], float.Parse(embRes[i]));
            }
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
            ClassicAssert.AreEqual("ERR M must be an integer between 4 and 4096", exc13.Message);
            var exc14 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "1", "2.0", "bar", "M", "10000"]));
            ClassicAssert.AreEqual("ERR M must be an integer between 4 and 4096", exc14.Message);

            // Missing/bad option value
            var exc20 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "1", "2.0", "bar", "EF"]));
            ClassicAssert.AreEqual("ERR invalid option after element", exc20.Message);
            var exc21 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "1", "2.0", "bar", "EF", "0"]));
            ClassicAssert.AreEqual("ERR EF must be an integer between 1 and 1000000", exc21.Message);
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

            _ = db.Execute("VADD", [vectorSetKey, "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 1, 0 }, "NOQUANT", "EF", "6", "M", "10", "XDISTANCE_METRIC", "L2"]);

            var exc16 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "2", "1.0", "2.0", "fizz", "NOQUANT", "EF", "6", "M", "10"]));
            ClassicAssert.AreEqual("ERR Vector dimension mismatch - got 2 but set has 75", exc16.Message);
            var exc17 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "fizz", "XPREQ8", "EF", "6", "M", "10"]));
            ClassicAssert.AreEqual("ERR asked quantization mismatch with existing vector set", exc17.Message);
            var exc18 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "fizz", "NOQUANT", "EF", "12", "M", "20"]));
            ClassicAssert.AreEqual("ERR asked M value mismatch with existing vector set", exc18.Message);

            // TODO: Redis doesn't appear to validate attributes... so that's weird

            // Empty Vector Set keys are forbidden (TODO: Remove this constraint)
            var exc19 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", ["", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "XPREQ8"]));
            ClassicAssert.AreEqual("ERR Vector Set key cannot be empty", exc19.Message);

            // Malformed XDISTANCE_METRIC
            var exc31 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "bar", "NOQUANT", "XDISTANCE_METRIC"]));
            ClassicAssert.AreEqual("ERR invalid option after element", exc31.Message);
            var exc32 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "bar", "NOQUANT", "XDISTANCE_METRIC", "FOO"]));
            ClassicAssert.AreEqual("ERR invalid XDISTANCE_METRIC", exc32.Message);

            // Invalid vector type keyword (not FP32, VALUES, or XB8)
            var exc40 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", ["mykey", "GARBAGE", "data", "elem1"]));
            ClassicAssert.AreEqual("ERR invalid vector specification", exc40.Message);

            // VALUES count exceeding MaxVectorDimensions (65536) must be rejected
            var exc41 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", ["foo", "VALUES", "100000", "1.0", "elem"]));
            ClassicAssert.IsTrue(exc41.Message.Contains("maximum"), $"Expected dimension limit error, got: {exc41.Message}");

            // EF exceeding MaxExplorationFactor (1,000,000) must be rejected
            var exc42 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", ["foo", "VALUES", "3", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "2000000000", "M", "32"]));
            ClassicAssert.IsTrue(exc42.Message.Contains("EF must be an integer between"), $"Expected EF validation error, got: {exc42.Message}");

            // REDUCE dim exceeding vector dimensions must be rejected
            var exc43 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", ["foo", "REDUCE", "100000", "VALUES", "3", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]));
            ClassicAssert.IsTrue(exc43.Message.Contains("REDUCE dimension must be <= vector dimensions"), $"Expected REDUCE dimension limit error, got: {exc43.Message}");
            var exc33 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VADD", [vectorSetKey, "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "bar", "NOQUANT", "XDISTANCE_METRIC", "XCOSINE_NORMALIZED"]));
            ClassicAssert.AreEqual("ERR Distance metric mismatch - got XCosine_Normalized but set has L2", exc33.Message);
        }

        [Test]
        public void VEMB_FP32Storage()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            // Add a vector using VALUES format with NOQUANT (FP32 storage)
            var res1 = db.Execute("VADD", ["foo", "VALUES", "8", "1.0", "2.0", "3.0", "4.0", "5.0", "6.0", "7.0", "8.0", new byte[] { 0, 0, 0, 0 }, "NOQUANT"]);
            ClassicAssert.AreEqual(1, (int)res1);

            // Add a vector using XB8 format with NOQUANT (FP32 storage)
            byte[] vectorBytes = new byte[8];
            for (int i = 0; i < 8; i++)
            {
                vectorBytes[i] = (byte)(i + 10);
            }

            var res2 = db.Execute("VADD", ["foo", "XB8", vectorBytes, new byte[] { 0, 0, 0, 2 }, "NOQUANT"]);
            ClassicAssert.AreEqual(1, (int)res2);

            // Verify VEMB for XB8 input vector
            var res3 = (string[])db.Execute("VEMB", ["foo", new byte[] { 0, 0, 0, 2 }]);
            ClassicAssert.AreEqual(8, res3.Length);
            for (var i = 0; i < 8; i++)
            {
                ClassicAssert.AreEqual((float)vectorBytes[i], float.Parse(res3[i]));
            }

            // Verify VEMB for VALUES input vector
            var res4 = (string[])db.Execute("VEMB", ["foo", new byte[] { 0, 0, 0, 0 }]);
            ClassicAssert.AreEqual(8, res4.Length);
            for (var i = 0; i < 8; i++)
            {
                ClassicAssert.AreEqual((float)(i + 1), float.Parse(res4[i]));
            }

            // Verify non-existent element returns empty
            var res5 = (string[])db.Execute("VEMB", ["foo", new byte[] { 0, 0, 0, 1 }]);
            ClassicAssert.AreEqual(0, res5.Length);
        }

        [Test]
        public void VectorSetOpacity()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]);
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

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = (string)db.StringGet(new byte[] { 0, 0, 0, 0 });
            ClassicAssert.IsNull(res2);

            var res3 = db.KeyDelete(new byte[] { 0, 0, 0, 0 });
            ClassicAssert.IsFalse(res3);

            var res4 = db.StringSet(new byte[] { 0, 0, 0, 0 }, "def", when: When.NotExists);
            ClassicAssert.IsTrue(res4);

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

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "100.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 1 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]);
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

            // Large Count
            var res9 = (byte[][])db.Execute("VSIM", ["foo", "XB8", byte7, "COUNT", "1000"]);
            ClassicAssert.AreEqual(2, res9.Length);
            ClassicAssert.IsTrue(res9.Any(static x => x.SequenceEqual(new byte[] { 0, 0, 0, 0 })));
            ClassicAssert.IsTrue(res9.Any(static x => x.SequenceEqual(new byte[] { 0, 0, 0, 1 })));
        }

        [Test]
        public void VSIMResp3()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: RedisProtocol.Resp3));
            var db = redis.GetDatabase();

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "fizz", "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "{\"id\": 123}"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "100.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "buzz", "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "{\"id\": 456}"]);
            ClassicAssert.AreEqual(1, (int)res2);

            var res3 = (string[])db.Execute("VSIM", ["foo", "VALUES", "75", "110.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "COUNT", "5", "EPSILON", "1.0", "EF", "40"]);
            ClassicAssert.AreEqual(2, res3.Length);
            ClassicAssert.IsTrue(res3.Contains("fizz"));
            ClassicAssert.IsTrue(res3.Contains("buzz"));

            var res4 = (string[])db.Execute("VSIM", ["foo", "ELE", "fizz", "COUNT", "5", "EPSILON", "1.0", "EF", "40"]);
            ClassicAssert.AreEqual(2, res4.Length);
            ClassicAssert.IsTrue(res4.Contains("fizz"));
            ClassicAssert.IsTrue(res4.Contains("buzz"));

            // FP32
            var float5 = new float[75];
            float5[0] = 3;
            for (var i = 1; i < float5.Length; i++)
            {
                float5[i] = float5[i - 1] + 0.1f;
            }
            var res5 = (string[])db.Execute("VSIM", ["foo", "FP32", MemoryMarshal.Cast<float, byte>(float5).ToArray(), "COUNT", "5", "EPSILON", "1.0", "EF", "40"]);
            ClassicAssert.AreEqual(2, res5.Length);
            ClassicAssert.IsTrue(res5.Contains("fizz"));
            ClassicAssert.IsTrue(res5.Contains("buzz"));

            // XB8
            var byte6 = new byte[75];
            byte6[0] = 10;
            for (var i = 1; i < byte6.Length; i++)
            {
                byte6[i] = (byte)(byte6[i - 1] + 1);
            }
            var res6 = (string[])db.Execute("VSIM", ["foo", "XB8", byte6, "COUNT", "5", "EPSILON", "1.0", "EF", "40"]);
            ClassicAssert.AreEqual(2, res6.Length);
            ClassicAssert.IsTrue(res6.Contains("fizz"));
            ClassicAssert.IsTrue(res6.Contains("buzz"));

            // COUNT > EF
            var byte7 = new byte[75];
            byte7[0] = 20;
            for (var i = 1; i < byte7.Length; i++)
            {
                byte7[i] = (byte)(byte7[i - 1] + 1);
            }
            var res7 = (string[])db.Execute("VSIM", ["foo", "XB8", byte7, "COUNT", "100", "EPSILON", "1.0", "EF", "40"]);
            ClassicAssert.AreEqual(2, res7.Length);
            ClassicAssert.IsTrue(res7.Contains("fizz"));
            ClassicAssert.IsTrue(res7.Contains("buzz"));

            // WITHSCORES (a MAP in Resp3)
            var res8Raw = db.Execute("VSIM", ["foo", "XB8", byte7, "COUNT", "100", "EPSILON", "1.0", "EF", "40", "WITHSCORES"]);
            ClassicAssert.AreEqual(ResultType.Map, res8Raw.Resp3Type);

            var res8 = res8Raw.ToDictionary();
            ClassicAssert.IsTrue(res8.Values.All(static v => ResultType.Double == v.Resp3Type));

            ClassicAssert.AreEqual(2, res8.Count);
            ClassicAssert.IsTrue(res8.ContainsKey("fizz"));
            ClassicAssert.IsTrue(res8.ContainsKey("buzz"));
            ClassicAssert.IsFalse(res8.Values.Any(static x => double.IsNaN((double)x)));

            // WITHATTRIBS (a MAP in Resp3)
            var res9Raw = db.Execute("VSIM", ["foo", "XB8", byte7, "COUNT", "100", "EPSILON", "1.0", "EF", "40", "WITHATTRIBS"]);
            ClassicAssert.AreEqual(ResultType.Map, res9Raw.Resp3Type);

            var res9 = res9Raw.ToDictionary();
            ClassicAssert.IsTrue(res9.Values.All(static v => ResultType.BulkString == v.Resp3Type));

            ClassicAssert.AreEqual(2, res9.Count);
            ClassicAssert.IsTrue(res9.ContainsKey("fizz"));
            ClassicAssert.IsTrue(res9.ContainsKey("buzz"));
            ClassicAssert.AreEqual("{\"id\": 123}", (string)res9["fizz"]);
            ClassicAssert.AreEqual("{\"id\": 456}", (string)res9["buzz"]);

            // WITHSCORES and WITHATTRIBS (a MAP in Resp3)
            var res10Raw = db.Execute("VSIM", ["foo", "XB8", byte7, "COUNT", "100", "EPSILON", "1.0", "EF", "40", "WITHATTRIBS", "WITHSCORES"]);
            ClassicAssert.AreEqual(ResultType.Map, res10Raw.Resp3Type);

            var res10 = res10Raw.ToDictionary();
            ClassicAssert.IsTrue(res10.Values.All(static v => ResultType.Array == v.Resp3Type));

            ClassicAssert.AreEqual(2, res10.Count);
            ClassicAssert.IsTrue(res10.ContainsKey("fizz"));
            ClassicAssert.IsTrue(res10.ContainsKey("buzz"));

            var res10Fizz = (RedisResult[])res10["fizz"];
            var res10Buzz = (RedisResult[])res10["buzz"];
            ClassicAssert.AreEqual(2, res10Fizz.Length);
            ClassicAssert.AreEqual(ResultType.Double, res10Fizz[0].Resp3Type);
            ClassicAssert.AreEqual(ResultType.BulkString, res10Fizz[1].Resp3Type);
            ClassicAssert.AreEqual(2, res10Buzz.Length);
            ClassicAssert.AreEqual(ResultType.Double, res10Buzz[0].Resp3Type);
            ClassicAssert.AreEqual(ResultType.BulkString, res10Buzz[1].Resp3Type);

            ClassicAssert.IsFalse(double.IsNaN((double)res10Fizz[0]));
            ClassicAssert.AreEqual("{\"id\": 123}", (string)res10Fizz[1]);

            ClassicAssert.IsFalse(double.IsNaN((double)res10Buzz[0]));
            ClassicAssert.AreEqual("{\"id\": 456}", (string)res10Buzz[1]);

            // WITHSCORES and WITHATTRIBS (a MAP in Resp3)
            var res11Raw = db.Execute("VSIM", ["foo", "XB8", byte7, "COUNT", "100", "EPSILON", "1.0", "EF", "40", "WITHATTRIBS", "WITHSCORES", "FILTER", ".id > 200"]);
            ClassicAssert.AreEqual(ResultType.Map, res11Raw.Resp3Type);

            var res11 = res11Raw.ToDictionary();
            ClassicAssert.AreEqual(1, res11.Count);

            var res11Buzz = res11["buzz"];
            ClassicAssert.AreEqual(ResultType.Array, res11Buzz.Resp3Type);
            ClassicAssert.AreEqual(2, res11Buzz.Length);
            ClassicAssert.AreEqual(ResultType.Double, res11Buzz[0].Resp3Type);
            ClassicAssert.AreEqual(ResultType.BulkString, res11Buzz[1].Resp3Type);

            ClassicAssert.IsFalse(double.IsNaN((double)res11Buzz[0]));
            ClassicAssert.AreEqual("{\"id\": 456}", (string)res11Buzz[1]);
        }

        [Test]
        public void VSIMWithAttribs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "hello world"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "100.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 1 }, "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "fizz buzz"]);
            ClassicAssert.AreEqual(1, (int)res2);

            // Equivalent to no attribute
            var res3 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "110.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 2 }, "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", ""]);
            ClassicAssert.AreEqual(1, (int)res3);

            // Actually no attribute
            var res4 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "120.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 3 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res4);

            // Very long attribute
            var bigAttr = Enumerable.Repeat((byte)'a', 1_024).ToArray();
            var res5 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "130.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 4 }, "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", bigAttr]);
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

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "3", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VDIM", "foo");
            ClassicAssert.AreEqual(3, (int)res2);

            var res3 = db.Execute("VADD", ["bar", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res3);

            var res4 = db.Execute("VDIM", "bar");
            ClassicAssert.AreEqual(75, (int)res4);

            var exc1 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VDIM", "fizz"));
            ClassicAssert.IsTrue(exc1.Message.Contains("Key not found"));

            // TODO: Add WRONGTYPE behavior check once implemented
        }

        [Test]
        public void VSIMWithAttributeFiltering()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            _ = db.KeyDelete("foo");

            // Add first vector with year=1980
            var res1 = db.Execute("VADD", ["foo", "VALUES", "3", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 },
                "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "{\"year\":1980}"]);
            ClassicAssert.AreEqual(1, (int)res1);

            // Add second vector with year=1960
            var res2 = db.Execute("VADD", ["foo", "VALUES", "3", "2.0", "3.0", "4.0", new byte[] { 0, 0, 0, 1 },
                "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "{\"year\":1960}"]);
            ClassicAssert.AreEqual(1, (int)res2);

            // Add third vector with year=1940
            var res3 = db.Execute("VADD", ["foo", "VALUES", "3", "1.5", "2.5", "3.5", new byte[] { 0, 0, 0, 2 },
                "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "{\"year\":1940}"]);
            ClassicAssert.AreEqual(1, (int)res3);


            // Search with filter for year > 1950 - should return 2 results (years 1980 and 1960)
            var res5 = (byte[][])db.Execute("VSIM", ["foo", "VALUES", "3", "0.0", "0.0", "0.0",
                "FILTER", ".year > 1950", "COUNT", "3", "WITHATTRIBS"]);

            ClassicAssert.AreEqual(4, res5.Length,
                "Should return 2 results (2 pairs of id+attribute) for year > 1950");

            // Verify both results have year > 1950
            for (var i = 0; i < res5.Length; i += 2)
            {
                var attr = res5[i + 1];
                var attrStr = Encoding.UTF8.GetString(attr);
                ClassicAssert.IsTrue(attrStr.Contains("\"year\":1980") || attrStr.Contains("\"year\":1960"),
                    $"Result should have year > 1950, got: {attrStr}");
            }

            // Search with filter for year > 1990 - should return NO results since all years are < 1990
            var res4 = (byte[][])db.Execute("VSIM", ["foo", "VALUES", "3", "0.0", "0.0", "0.0",
                "FILTER", ".year > 1990", "COUNT", "3", "WITHATTRIBS"]);

            ClassicAssert.AreEqual(0, res4.Length,
                "Should return 0 results since no vectors have year > 1990");
        }

        [Test]
        public void VSIMCount()
        {
            const string VectorSet = "foo";
            const int VectorCount = 100;
            const int Select = 10;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            _ = db.KeyDelete(VectorSet);

            for (var i = 0; i < VectorCount; i++)
            {
                var elementId = new byte[sizeof(int)];
                BinaryPrimitives.WriteInt32LittleEndian(elementId, i);

                var res = (int)db.Execute("VADD", [VectorSet, "VALUES", "3", "1.0", "2.0", "3.0", elementId, "NOQUANT", "SETATTR", $"{{\"field\": {100 + i}}}"]);
                ClassicAssert.AreEqual(1, res);
            }

            var vsimNoFilter = (byte[][])db.Execute("VSIM", [VectorSet, "VALUES", "3", "1.0", "2.0", "3.0", "COUNT", Select]);
            ClassicAssert.AreEqual(Select, vsimNoFilter.Length);

            var vsimNoFilterWithAttribs = (byte[][])db.Execute("VSIM", [VectorSet, "VALUES", "3", "1.0", "2.0", "3.0", "COUNT", Select, "WITHATTRIBS"]);
            ClassicAssert.AreEqual(Select * 2, vsimNoFilterWithAttribs.Length);

            var vsimNoFilterWithAttribsWithScores = (byte[][])db.Execute("VSIM", [VectorSet, "VALUES", "3", "1.0", "2.0", "3.0", "COUNT", Select, "WITHATTRIBS", "WITHSCORES"]);
            ClassicAssert.AreEqual(Select * 3, vsimNoFilterWithAttribsWithScores.Length);

            var vsimWithFilter = (byte[][])db.Execute("VSIM", [VectorSet, "VALUES", "3", "1.0", "2.0", "3.0", "COUNT", Select, "FILTER", $".field >= 100 and .field <= {100 + (2 * Select)}"]);
            ClassicAssert.AreEqual(Select, vsimWithFilter.Length);

            var vsimWithFilterWithAttribs = (byte[][])db.Execute("VSIM", [VectorSet, "VALUES", "3", "1.0", "2.0", "3.0", "COUNT", Select, "FILTER", $".field >= 100 and .field <= {100 + (2 * Select)}", "WITHATTRIBS"]);
            ClassicAssert.AreEqual(Select * 2, vsimWithFilterWithAttribs.Length);

            var vsimWithFilterWithAttribsWithScores = (byte[][])db.Execute("VSIM", [VectorSet, "VALUES", "3", "1.0", "2.0", "3.0", "COUNT", Select, "FILTER", $".field >= 100 and .field <= {100 + (2 * Select)}", "WITHATTRIBS", "WITHSCORES"]);
            ClassicAssert.AreEqual(Select * 3, vsimWithFilterWithAttribsWithScores.Length);
        }

        [Test]
        public void VSIMWithFilterButWithoutWithAttribs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            _ = db.KeyDelete("foo");

            // Add vectors with attributes
            db.Execute("VADD", ["foo", "VALUES", "3", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 },
                "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "{\"year\":1980}"]);
            db.Execute("VADD", ["foo", "VALUES", "3", "2.0", "3.0", "4.0", new byte[] { 0, 0, 0, 1 },
                "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "{\"year\":1960}"]);
            db.Execute("VADD", ["foo", "VALUES", "3", "1.5", "2.5", "3.5", new byte[] { 0, 0, 0, 2 },
                "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "{\"year\":1940}"]);

            // FILTER without WITHATTRIBS should work: fetch attributes internally and apply filter
            var res = (byte[][])db.Execute("VSIM", ["foo", "VALUES", "3", "0.0", "0.0", "0.0",
                "FILTER", ".year > 1950", "COUNT", "3"]);

            // Should return only 2 element ids (no attributes since WITHATTRIBS not specified)
            ClassicAssert.AreEqual(2, res.Length,
                "Should return 2 element ids (year > 1950) without attributes");
        }

        [Test]
        public void VSIMWithAdvancedFiltering()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            _ = SeedMoviesForAdvancedFiltering(db);

            // Test logical AND
            var res4 = (byte[][])db.Execute("VSIM", ["movies", "VALUES", "3", "0.0", "0.0", "0.0",
                "FILTER", ".year > 1970 and .rating > 4.0", "COUNT", "3", "WITHATTRIBS"]);
            ClassicAssert.AreEqual(4, res4.Length, "Logical AND: year > 1970 AND rating > 4.0");

            // Test logical OR
            var res5 = (byte[][])db.Execute("VSIM", ["movies", "VALUES", "3", "0.0", "0.0", "0.0",
                "FILTER", ".year < 1970 or .year > 2000", "COUNT", "3", "WITHATTRIBS"]);
            ClassicAssert.AreEqual(4, res5.Length, "Logical OR: year < 1970 OR year > 2000");

            // Test string equality
            var res6 = (byte[][])db.Execute("VSIM", ["movies", "VALUES", "3", "0.0", "0.0", "0.0",
                "FILTER", ".genre == \"action\"", "COUNT", "3", "WITHATTRIBS"]);
            ClassicAssert.AreEqual(4, res6.Length, "String equality: genre == 'action'");

            // Test arithmetic expression
            var res7 = (byte[][])db.Execute("VSIM", ["movies", "VALUES", "3", "0.0", "0.0", "0.0",
                "FILTER", ".year / 10 >= 200", "COUNT", "3", "WITHATTRIBS"]);
            ClassicAssert.AreEqual(2, res7.Length, "Arithmetic: year / 10 >= 200");

            // Test parentheses grouping
            var res8 = (byte[][])db.Execute("VSIM", ["movies", "VALUES", "3", "0.0", "0.0", "0.0",
                "FILTER", "(.year > 2000 or .year < 1970) and .rating >= 4.0", "COUNT", "3", "WITHATTRIBS"]);
            ClassicAssert.AreEqual(2, res8.Length, "Parentheses grouping");

            // Test containment operator (in)
            var res9 = (byte[][])db.Execute("VSIM", ["movies", "VALUES", "3", "0.0", "0.0", "0.0",
                "FILTER", "\"classic\" in .tags", "COUNT", "3", "WITHATTRIBS"]);
            ClassicAssert.AreEqual(4, res9.Length, "Containment: 'classic' in tags");

            // Test NOT operator
            var res10 = (byte[][])db.Execute("VSIM", ["movies", "VALUES", "3", "0.0", "0.0", "0.0",
                "FILTER", "not (.genre == \"drama\")", "COUNT", "3", "WITHATTRIBS"]);
            ClassicAssert.AreEqual(4, res10.Length, "NOT operator: not (genre == 'drama')");

            // Test complex expression with multiple operators
            var res11 = (byte[][])db.Execute("VSIM", ["movies", "VALUES", "3", "0.0", "0.0", "0.0",
                "FILTER", ".rating * 2 > 8 and (.year >= 1980 or \"modern\" in .tags)", "COUNT", "3", "WITHATTRIBS"]);
            ClassicAssert.AreEqual(4, res11.Length, "Complex: rating*2 > 8 AND (year>=1980 OR 'modern' in tags)");
        }

        [Test]
        public void VSIMWithAdvancedFilteringELEWithAttribs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var queryElementId = SeedMoviesForAdvancedFiltering(db);

            var res1 = (byte[][])db.Execute("VSIM", ["movies", "ELE", queryElementId,
                "FILTER", ".genre == \"action\"", "COUNT", "3", "WITHATTRIBS"]);
            ClassicAssert.AreEqual(4, res1.Length, "ELE + FILTER + WITHATTRIBS: genre == 'action'");

            var res2 = (byte[][])db.Execute("VSIM", ["movies", "ELE", queryElementId,
                "FILTER", "\"classic\" in .tags", "COUNT", "3", "WITHATTRIBS"]);
            ClassicAssert.AreEqual(4, res2.Length, "ELE + FILTER + WITHATTRIBS: 'classic' in tags");

            var res3 = (byte[][])db.Execute("VSIM", ["movies", "ELE", queryElementId,
                "FILTER", ".rating / 2 > 2 and .year >= 1980", "COUNT", "3", "WITHATTRIBS"]);
            ClassicAssert.AreEqual(4, res3.Length, "ELE + FILTER + WITHATTRIBS: arithmetic and comparison");
        }

        [Test]
        public void VSIMWithAdvancedFilteringELEWithoutWithAttribs()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var queryElementId = SeedMoviesForAdvancedFiltering(db);

            var res1 = (byte[][])db.Execute("VSIM", ["movies", "ELE", queryElementId,
                "FILTER", ".genre == \"action\"", "COUNT", "3"]);
            ClassicAssert.AreEqual(2, res1.Length, "ELE + FILTER without WITHATTRIBS: genre == 'action'");

            var res2 = (byte[][])db.Execute("VSIM", ["movies", "ELE", queryElementId,
                "FILTER", "\"classic\" in .tags", "COUNT", "3"]);
            ClassicAssert.AreEqual(2, res2.Length, "ELE + FILTER without WITHATTRIBS: 'classic' in tags");

            var res3 = (byte[][])db.Execute("VSIM", ["movies", "ELE", queryElementId,
                "FILTER", ".rating / 2 > 2 and .year >= 1980", "COUNT", "3"]);
            ClassicAssert.AreEqual(2, res3.Length, "ELE + FILTER without WITHATTRIBS: arithmetic and comparison");
        }

        [Test]
        public void VSIMBadFilters()
        {
            const string VectorSet = "vs";
            const string CompileErr = "ERR Compiling filter failed";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            _ = db.KeyDelete(VectorSet);

            // Seed:
            //   ids 0..2  -> valid JSON attributes (year + genre)
            //   id  3     -> malformed JSON attribute
            //   id  4     -> no SETATTR at all
            var add0 = db.Execute("VADD", [VectorSet, "VALUES", "3", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "NOQUANT", "SETATTR", "{\"year\":1980,\"genre\":\"action\"}"]);
            ClassicAssert.AreEqual(1, (int)add0);
            var add1 = db.Execute("VADD", [VectorSet, "VALUES", "3", "1.1", "2.1", "3.1", new byte[] { 0, 0, 0, 1 }, "NOQUANT", "SETATTR", "{\"year\":1990,\"genre\":\"drama\"}"]);
            ClassicAssert.AreEqual(1, (int)add1);
            var add2 = db.Execute("VADD", [VectorSet, "VALUES", "3", "1.2", "2.2", "3.2", new byte[] { 0, 0, 0, 2 }, "NOQUANT", "SETATTR", "{\"year\":2000,\"genre\":\"sci-fi\"}"]);
            ClassicAssert.AreEqual(1, (int)add2);
            var add3 = db.Execute("VADD", [VectorSet, "VALUES", "3", "1.3", "2.3", "3.3", new byte[] { 0, 0, 0, 3 }, "NOQUANT", "SETATTR", "{not-valid-json"]);
            ClassicAssert.AreEqual(1, (int)add3);
            var add4 = db.Execute("VADD", [VectorSet, "VALUES", "3", "1.4", "2.4", "3.4", new byte[] { 0, 0, 0, 4 }, "NOQUANT"]);
            ClassicAssert.AreEqual(1, (int)add4);

            // ── Section A: compile-time errors ─────────────────────────────────
            // Every entry below must surface as "ERR Compiling filter failed".
            (string Filter, string Why)[] badFilters =
            [
                ("   ", "whitespace-only filter (compiler sees zero tokens)"),
                ("(.year > 1980", "unclosed opening paren"),
                (".year > 1980)", "extra closing paren"),
                ("()", "empty parens with no expression"),
                (".genre == \"action", "unterminated double-quoted string"),
                (".genre == 'action", "unterminated single-quoted string"),
                (". > 1", "bare-dot selector with no field name"),
                ("> 1980", "binary operator with no left operand"),
                (".year >", "binary operator with no right operand"),
                (".year > > 1980", "two consecutive binary operators"),
                (".year 1980", "two consecutive operands with no operator"),
                (".year > 1.2.3", "malformed number literal"),
                ("foobar", "unknown identifier"),
                ("@ > 1", "character not allowed in any token"),
                (".x in [1, 2", "unterminated tuple literal"),
                (".x in [1 2]", "tuple elements without a comma separator"),
                ("not", "unary 'not' with no operand"),
                ("in [1, 2]", "'in' operator with no left operand"),
                (".x in", "'in' operator with no right operand"),
                (">", "naked binary operator"),
            ];

            foreach (var (filter, why) in badFilters)
            {
                var exc = ClassicAssert.Throws<RedisServerException>(
                    () => db.Execute("VSIM", [VectorSet, "VALUES", "3", "0.0", "0.0", "0.0", "FILTER", filter, "COUNT", "10"]),
                    $"Expected compile failure for filter '{filter}' ({why})");
                ClassicAssert.AreEqual(CompileErr, exc.Message, $"Wrong error message for filter '{filter}' ({why})");
            }

            // ── Section B: documented "skip silently" behavior ─────────────────
            // Per the filter-expressions docs: "If a field is missing or invalid,
            // the element is skipped without error." None of the queries below
            // should raise an exception.

            // Empty FILTER string is treated as no filter at all by the VSIM
            // parser (length-0 check before compile), so it returns all elements.
            var emptyFilter = (byte[][])db.Execute("VSIM", [VectorSet, "VALUES", "3", "0.0", "0.0", "0.0", "FILTER", "", "COUNT", "10"]);
            ClassicAssert.AreEqual(5, emptyFilter.Length, "Empty FILTER string should behave as no filter");

            // Filter referencing a field no element has -> 0 results, no error.
            var missingField = (byte[][])db.Execute("VSIM", [VectorSet, "VALUES", "3", "0.0", "0.0", "0.0", "FILTER", ".nonexistent > 5", "COUNT", "10"]);
            ClassicAssert.AreEqual(0, missingField.Length, "Filter on a non-existent field should return zero results, not an error");

            // Type-mismatched comparisons must not raise. Exact result count
            // depends on whether the runner skips or coerces, which the spec
            // leaves unspecified, so we only assert "no error" and that the
            // result stays within the seeded population.
            var numCmpString = (byte[][])db.Execute("VSIM", [VectorSet, "VALUES", "3", "0.0", "0.0", "0.0", "FILTER", ".genre > 5", "COUNT", "10"]);
            ClassicAssert.IsNotNull(numCmpString, "Numeric comparison against a string field must not raise");
            ClassicAssert.LessOrEqual(numCmpString.Length, 5);

            var stringEqOnNum = (byte[][])db.Execute("VSIM", [VectorSet, "VALUES", "3", "0.0", "0.0", "0.0", "FILTER", ".year == \"hello\"", "COUNT", "10"]);
            ClassicAssert.IsNotNull(stringEqOnNum, "Comparing a numeric field to a string literal must not raise");
            ClassicAssert.LessOrEqual(stringEqOnNum.Length, 5);

            // A permissive valid filter should match the 3 well-formed elements
            // and silently skip the malformed-JSON (id 3) and no-attr (id 4)
            // elements, demonstrating both documented skip cases at once.
            var validFilter = (byte[][])db.Execute("VSIM", [VectorSet, "VALUES", "3", "0.0", "0.0", "0.0", "FILTER", ".year >= 1980", "COUNT", "10"]);
            ClassicAssert.AreEqual(3, validFilter.Length, "Only the 3 well-formed elements should match; malformed-JSON and no-attr elements must be skipped silently");
            var matchedIds = new HashSet<byte[]>(validFilter, ByteArrayComparer.Instance);
            ClassicAssert.IsTrue(matchedIds.Contains([0, 0, 0, 0]), "id 0 (valid attrs) should be in results");
            ClassicAssert.IsTrue(matchedIds.Contains([0, 0, 0, 1]), "id 1 (valid attrs) should be in results");
            ClassicAssert.IsTrue(matchedIds.Contains([0, 0, 0, 2]), "id 2 (valid attrs) should be in results");
            ClassicAssert.IsFalse(matchedIds.Contains([0, 0, 0, 3]), "id 3 (malformed JSON) should be silently skipped");
            ClassicAssert.IsFalse(matchedIds.Contains([0, 0, 0, 4]), "id 4 (no SETATTR) should be silently skipped");
        }

        [Test]
        public void VSIMComplexJsonAttributes()
        {
            const string VectorSet = "vs";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            _ = db.KeyDelete(VectorSet);

            // Seed 11 elements covering nested objects, booleans, null, arrays,
            // non-object top-level JSON, empty objects, dash-in-field-name, and
            // same-named top-level vs nested fields.
            //
            //   id 0  -> top-level year + nested meta.director
            //   id 1  -> year only exists at nested depth
            //   id 2  -> top-level boolean true
            //   id 3  -> top-level boolean false
            //   id 4  -> top-level null
            //   id 5  -> top-level number array
            //   id 6  -> non-object top-level JSON (whole attr is an array)
            //   id 7  -> empty object
            //   id 8  -> field name contains a dash
            //   id 9  -> same-named field both top-level (1980) and nested (2020)
            //   id 10 -> top-level string array + nested object value
            (byte[] Id, string Attr)[] seed =
            [
                ([0, 0, 0, 0],  "{\"year\":1980,\"meta\":{\"director\":\"Spielberg\"}}"),
                ([0, 0, 0, 1],  "{\"meta\":{\"year\":1980}}"),
                ([0, 0, 0, 2],  "{\"active\":true}"),
                ([0, 0, 0, 3],  "{\"active\":false}"),
                ([0, 0, 0, 4],  "{\"year\":null}"),
                ([0, 0, 0, 5],  "{\"scores\":[1,2,3]}"),
                ([0, 0, 0, 6],  "[1,2,3]"),
                ([0, 0, 0, 7],  "{}"),
                ([0, 0, 0, 8],  "{\"year-old\":1980}"),
                ([0, 0, 0, 9],  "{\"year\":1980,\"nested\":{\"year\":2020}}"),
                ([0, 0, 0, 10], "{\"tags\":[\"classic\"],\"director\":{\"name\":\"Spielberg\"}}"),
            ];

            for (var i = 0; i < seed.Length; i++)
            {
                var (id, attr) = seed[i];
                // Spread the vectors slightly so cosine/L2 doesn't collapse them on top of each other.
                var v0 = (1.0f + i * 0.1f).ToString();
                var v1 = (2.0f + i * 0.1f).ToString();
                var v2 = (3.0f + i * 0.1f).ToString();
                var res = db.Execute("VADD", [VectorSet, "VALUES", "3", v0, v1, v2, id, "NOQUANT", "SETATTR", attr]);
                ClassicAssert.AreEqual(1, (int)res, $"VADD for id {i} should succeed even with unusual attribute shape");
            }

            // Sanity: all 11 elements made it into the set.
            var info = (RedisValue[])db.Execute("VINFO", [VectorSet]);
            var infoMap = new Dictionary<string, string>();
            for (var i = 0; i < info.Length; i += 2)
                infoMap[info[i]] = info[i + 1];
            ClassicAssert.AreEqual("11", infoMap["size"], "All 11 elements must be present");

            // ── Case 1 + 9: top-level .year is visible; nested .year is not ───
            // Filter .year > 1900 should match id 0 and id 9 (both have top-level
            // year 1980). It must NOT match id 1 (nested-only) or id 4 (null).
            var byYear = MatchedIds(db.Execute("VSIM", [VectorSet, "VALUES", "3", "0.0", "0.0", "0.0", "FILTER", ".year > 1900", "COUNT", "20"]));
            AssertSameIds(ExpectIds([0, 0, 0, 0], [0, 0, 0, 9]), byYear, "Top-level .year > 1900 should match only ids 0 and 9");

            // ── Case 9 specifically: nested .year=2020 must be invisible ──────
            var byYear2000 = MatchedIds(db.Execute("VSIM", [VectorSet, "VALUES", "3", "0.0", "0.0", "0.0", "FILTER", ".year > 2000", "COUNT", "20"]));
            ClassicAssert.AreEqual(0, byYear2000.Count, ".year > 2000 must not see the nested year=2020 in id 9");

            var byYearRange = MatchedIds(db.Execute("VSIM", [VectorSet, "VALUES", "3", "0.0", "0.0", "0.0", "FILTER", ".year > 1900 and .year < 2000", "COUNT", "20"]));
            AssertSameIds(ExpectIds([0, 0, 0, 0], [0, 0, 0, 9]), byYearRange, "Range filter should still see only top-level .year for ids 0 and 9");

            // ── Case 1 sub: top-level field whose value is an object is unusable
            // id 0's .meta and id 1's .meta are objects. Comparing to a string
            // must yield 0 matches without raising.
            var metaEq = (byte[][])db.Execute("VSIM", [VectorSet, "VALUES", "3", "0.0", "0.0", "0.0", "FILTER", ".meta == \"Spielberg\"", "COUNT", "20"]);
            ClassicAssert.AreEqual(0, metaEq.Length, "Equality against an object-valued top-level field must yield 0 results");

            // Same idea for case 10: .director is an object on id 10.
            var directorEq = (byte[][])db.Execute("VSIM", [VectorSet, "VALUES", "3", "0.0", "0.0", "0.0", "FILTER", ".director == \"Spielberg\"", "COUNT", "20"]);
            ClassicAssert.AreEqual(0, directorEq.Length, "Equality against object-valued .director must yield 0 results");

            // ── Case 3: top-level booleans coerce to 1 / 0 ────────────────────
            var activeTrue = MatchedIds(db.Execute("VSIM", [VectorSet, "VALUES", "3", "0.0", "0.0", "0.0", "FILTER", ".active == 1", "COUNT", "20"]));
            AssertSameIds(ExpectIds([0, 0, 0, 2]), activeTrue, ".active == 1 should match only the element whose JSON value is true");

            var activeFalse = MatchedIds(db.Execute("VSIM", [VectorSet, "VALUES", "3", "0.0", "0.0", "0.0", "FILTER", ".active == 0", "COUNT", "20"]));
            AssertSameIds(ExpectIds([0, 0, 0, 3]), activeFalse, ".active == 0 should match only the element whose JSON value is false");

            // ── Case 4: top-level null does not match numeric > comparisons ───
            // (.year > 5 with year=null: id 4 must NOT appear; ids 0 and 9 do.)
            var yearGt5 = MatchedIds(db.Execute("VSIM", [VectorSet, "VALUES", "3", "0.0", "0.0", "0.0", "FILTER", ".year > 5", "COUNT", "20"]));
            AssertSameIds(ExpectIds([0, 0, 0, 0], [0, 0, 0, 9]), yearGt5, ".year > 5 must skip the null-valued id 4");

            // ── Case 5: top-level number array works with `in`, fails > silently
            var inHit = MatchedIds(db.Execute("VSIM", [VectorSet, "VALUES", "3", "0.0", "0.0", "0.0", "FILTER", "2 in .scores", "COUNT", "20"]));
            AssertSameIds(ExpectIds([0, 0, 0, 5]), inHit, "2 in .scores should match only id 5");

            var inMiss = (byte[][])db.Execute("VSIM", [VectorSet, "VALUES", "3", "0.0", "0.0", "0.0", "FILTER", "99 in .scores", "COUNT", "20"]);
            ClassicAssert.AreEqual(0, inMiss.Length, "99 in .scores should match nothing");

            var arrAsNum = (byte[][])db.Execute("VSIM", [VectorSet, "VALUES", "3", "0.0", "0.0", "0.0", "FILTER", ".scores > 5", "COUNT", "20"]);
            ClassicAssert.AreEqual(0, arrAsNum.Length, "Numeric comparison against an array-valued field must yield 0 results without raising");

            // ── Case 8: selector greedily includes '-' so .year-old is one name
            // The filter must NOT be interpreted as `.year - old > 1900`.
            var yearOld = MatchedIds(db.Execute("VSIM", [VectorSet, "VALUES", "3", "0.0", "0.0", "0.0", "FILTER", ".year-old > 1900", "COUNT", "20"]));
            AssertSameIds(ExpectIds([0, 0, 0, 8]), yearOld, ".year-old must be treated as a single selector and match only id 8");

            // ── Case 10: top-level string array still works with `in` ─────────
            var classicInTags = MatchedIds(db.Execute("VSIM", [VectorSet, "VALUES", "3", "0.0", "0.0", "0.0", "FILTER", "\"classic\" in .tags", "COUNT", "20"]));
            AssertSameIds(ExpectIds([0, 0, 0, 10]), classicInTags, "\"classic\" in .tags should match only id 10");

            // ── Case 6 + 7 (implicit): the above filters together demonstrate
            // that ids 6 (non-object top-level JSON) and 7 (empty object) never
            // appear in any field-based result and never cause an error.
            ClassicAssert.IsFalse(yearOld.Contains([0, 0, 0, 6], ByteArrayComparer.Instance), "Non-object top-level JSON (id 6) must be silently skipped, not error");
            ClassicAssert.IsFalse(yearOld.Contains([0, 0, 0, 7], ByteArrayComparer.Instance), "Empty-object JSON (id 7) must be silently skipped, not error");

            static HashSet<byte[]> MatchedIds(RedisResult res)
                => new((byte[][])res, ByteArrayComparer.Instance);

            static HashSet<byte[]> ExpectIds(params byte[][] ids)
                => new(ids, ByteArrayComparer.Instance);

            static void AssertSameIds(HashSet<byte[]> expected, HashSet<byte[]> actual, string message)
                => ClassicAssert.IsTrue(expected.SetEquals(actual), $"{message} (expected {Format(expected)}, got {Format(actual)})");

            static string Format(HashSet<byte[]> set)
                => "{" + string.Join(", ", set.Select(static b => "[" + string.Join(",", b) + "]")) + "}";
        }

        [Test]
        public void VSIMErrors()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            _ = db.KeyDelete("foo");

            // Add a vector so the key exists (needed for FILTER-EF test)
            var res1 = db.Execute("VADD", ["foo", "VALUES", "3", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "{\"year\":1980}"]);
            ClassicAssert.AreEqual(1, (int)res1);

            // FILTER-EF exceeding MaxFilteringScaleFactor must be rejected
            var exc1 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VSIM", ["foo", "VALUES", "3", "0.0", "0.0", "0.0", "FILTER", ".year > 1950", "FILTER-EF", "999999999", "COUNT", "3", "WITHATTRIBS"]));
            ClassicAssert.AreEqual("ERR FILTER-EF must be an integer between 4 and 256", exc1.Message);

            // COUNT exceeding MaxRetrieveCount must be rejected
            var exc2 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VSIM", ["foo", "VALUES", "3", "0.0", "0.0", "0.0", "COUNT", "999999999"]));
            ClassicAssert.AreEqual("ERR COUNT must be an integer between 0 and 100000000", exc2.Message);

            // VALUES count exceeding MaxVectorDimensions (65536) must be rejected
            var exc3 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VSIM", ["foo", "VALUES", "100000", "1.0"]));
            ClassicAssert.AreEqual("ERR vector exceeds maximum of 65536 dimensions", exc3.Message);

            // EF exceeding MaxExplorationFactor (1,000,000) must be rejected
            var exc4 = ClassicAssert.Throws<RedisServerException>(() => db.Execute("VSIM", ["foo", "VALUES", "3", "0.0", "0.0", "0.0", "EF", "2000000000"]));
            ClassicAssert.AreEqual("ERR EF must be an integer between 1 and 1000000", exc4.Message);
        }

        private static byte[] SeedMoviesForAdvancedFiltering(IDatabase db)
        {
            _ = db.KeyDelete("movies");

            var queryElementId = new byte[] { 0, 0, 0, 0 };
            var res1 = db.Execute("VADD", ["movies", "VALUES", "3", "1.0", "2.0", "3.0", queryElementId,
                "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "{\"year\":1980,\"rating\":4.5,\"genre\":\"action\",\"tags\":[\"classic\",\"popular\"]}"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VADD", ["movies", "VALUES", "3", "2.0", "3.0", "4.0", new byte[] { 0, 0, 0, 1 },
                "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "{\"year\":1960,\"rating\":3.8,\"genre\":\"drama\",\"tags\":[\"classic\"]}"]);
            ClassicAssert.AreEqual(1, (int)res2);

            var res3 = db.Execute("VADD", ["movies", "VALUES", "3", "1.5", "2.5", "3.5", new byte[] { 0, 0, 0, 2 },
                "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "{\"year\":2010,\"rating\":4.2,\"genre\":\"action\",\"tags\":[\"modern\"]}"]);
            ClassicAssert.AreEqual(1, (int)res3);

            return queryElementId;
        }

        [Test]
        public void DeleteVectorSet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "3", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.KeyDelete("foo");
            ClassicAssert.IsTrue(res2);

            var res3 = db.Execute("VADD", ["fizz", "REDUCE", "3", "VALUES", "75", "100.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res3);

            var res4 = db.StringSet("buzz", "abc");
            ClassicAssert.IsTrue(res4);

            var res5 = db.KeyDelete(["fizz", "buzz"]);
            ClassicAssert.AreEqual(2, res5);
        }

        [Test]
        public void FlushDB()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var s = redis.GetServers().Single();
            var db = redis.GetDatabase();

#if DEBUG
            var preAddCreateCalls = server.Provider.StoreWrapper.DefaultDatabase.VectorManager.Service.CreateIndexCalls;
#endif

            var res1 = db.Execute("VADD", ["foo", "REDUCE", "3", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            s.FlushDatabase(0);

#if DEBUG
            // Drops are requested and processed in the background, wait for them to drop
            var vectorManager = server.Provider.StoreWrapper.DefaultDatabase.VectorManager;
            vectorManager.WaitForDiskANNIndexDrop("foo"u8);

            var finalCreateCalls = server.Provider.StoreWrapper.DefaultDatabase.VectorManager.Service.CreateIndexCalls;
            var finalDropCalls = server.Provider.StoreWrapper.DefaultDatabase.VectorManager.Service.DropIndexCalls;

            // Check we actually dropped the index despite not touching the key explicitly
            ClassicAssert.AreEqual(preAddCreateCalls + 1, finalCreateCalls);
            ClassicAssert.AreEqual(finalDropCalls, finalCreateCalls);
#endif

            var res2 = db.KeyExists("foo");
            ClassicAssert.IsFalse(res2);
        }

        [Test]
        public async Task ExpirationAsync()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase();

            var res1 = await db.ExecuteAsync("VADD", ["foo", "REDUCE", "3", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]).ConfigureAwait(false);
            ClassicAssert.AreEqual(1, (int)res1);

#if DEBUG
            var preExpireDropCalls = server.Provider.StoreWrapper.DefaultDatabase.VectorManager.Service.DropIndexCalls;
#endif

            var res2 = await db.KeyExpireAsync("foo", TimeSpan.FromSeconds(0.5)).ConfigureAwait(false);
            ClassicAssert.IsTrue(res2);

            // Wait for expiration to pass
            await Task.Delay(TimeSpan.FromSeconds(2)).ConfigureAwait(false);

            // Force an expiration scan, check that at least one record was evicted
            var res3 = (int[])await db.ExecuteAsync("EXPDELSCAN");
            ClassicAssert.AreEqual(1, res3[0]);

            var res4 = await db.KeyExistsAsync("foo").ConfigureAwait(false);
            ClassicAssert.IsFalse(res4);

#if DEBUG
            var finalExpireDropCalls = server.Provider.StoreWrapper.DefaultDatabase.VectorManager.Service.DropIndexCalls;

            // Check that background cleanup was triggered, not just the key being removed
            ClassicAssert.AreEqual(preExpireDropCalls + 1, finalExpireDropCalls);
#endif
        }

        [Test]
        public void InterruptedVectorSetDelete_BeforeMark()
        => InterruptedVectorSetDelete(ExceptionInjectionType.VectorSet_Interrupt_Delete_0);


        [Test]
        public void InterruptedVectorSetDelete_DuringCleanup()
        => InterruptedVectorSetDelete(ExceptionInjectionType.VectorSet_Interrupt_Delete_1);

        [Test]
        public void InterruptedVectorSetDelete_AfterCleanup()
        => InterruptedVectorSetDelete(ExceptionInjectionType.VectorSet_Interrupt_Delete_2);

        [Test]
        public void InterruptedVectorSetDelete_AfterMark()
        => InterruptedVectorSetDelete(ExceptionInjectionType.VectorSet_Interrupt_Delete_3);

        private void InterruptedVectorSetDelete(ExceptionInjectionType faultLocation)
        {
#if !DEBUG
            ClassicAssert.Ignore("Relies on ExceptionInjectionHelper, disable in non-DEBUG");
#endif

            var key = $"{nameof(InterruptedVectorSetDelete)}_{faultLocation}";

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase();

                var res1 = db.Execute("VADD", [key, "REDUCE", "3", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]);
                ClassicAssert.AreEqual(1, (int)res1);

                ExceptionInjectionHelper.EnableException(faultLocation);
                try
                {
                    _ = db.KeyDelete(key);
                }
                catch
                {
                    // Exception is possible (but not guarnateed) and legal
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
                                    var res = db.Execute("VADD", [key, "REDUCE", "3", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 1 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]);
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
                                try
                                {
                                    var res = db.Execute("VGETATTR", [key, "wololo"]);
                                    ClassicAssert.IsTrue(res.IsNull);
                                }
                                catch (RedisServerException e)
                                {
                                    exc = e;
                                }
                                break;
                            case RespCommand.VINFO:
                                try
                                {
                                    var res = (RedisValue[])db.Execute("VINFO", [key]);
                                    ClassicAssert.AreEqual(14, res.Length);
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
        public Task InterruptedVectorSetDelete_BeforeMark_RecoveryAsync()
        => InterruptedVectorSetDeleteRecoveryAsync(ExceptionInjectionType.VectorSet_Interrupt_Delete_0);

        [Test]
        public Task InteterruptedVectorSetDelete_DuringCleanup_RecoveryAsync()
        => InterruptedVectorSetDeleteRecoveryAsync(ExceptionInjectionType.VectorSet_Interrupt_Delete_1);

        [Test]
        public Task InteterruptedVectorSetDelete_AfterCleanup_RecoveryAsync()
        => InterruptedVectorSetDeleteRecoveryAsync(ExceptionInjectionType.VectorSet_Interrupt_Delete_2);

        [Test]
        public Task InteterruptedVectorSetDelete_AfterMark_RecoveryAsync()
        => InterruptedVectorSetDeleteRecoveryAsync(ExceptionInjectionType.VectorSet_Interrupt_Delete_3);

        private async Task InterruptedVectorSetDeleteRecoveryAsync(ExceptionInjectionType faultLocation)
        {
#if !DEBUG
            ClassicAssert.Ignore("Relies on ExceptionInjectionHelper, disable in non-DEBUG");
#endif

            var key = $"{nameof(InterruptedVectorSetDeleteRecoveryAsync)}_{faultLocation}";

            // Create a partially deleted Vector Set, then take a checkpoint and shutdown
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                var res = db.Execute("VADD", [key, "REDUCE", "3", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]);
                ClassicAssert.AreEqual(1, (int)res);

                ExceptionInjectionHelper.EnableException(faultLocation);
                try
                {
                    _ = db.KeyDelete(key);
                }
                catch
                {
                    // Exception is possible (but not guarnateed) and legal
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

                var commit = await server.Store.WaitForCommitAsync();
                ClassicAssert.IsTrue(commit);
            }

            // Restart Garnet, which should block applying any pending Vector Set deletes
            server.Dispose(deleteDir: false);

            server = CreateGarnetServer(tryRecover: true);
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
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0302:Simplify collection initialization", Justification = "Collection initializers don't guarantee stackalloc, which is required in these tests")]
        public unsafe void VectorReadBatchVariants()
        {
            // Single key, 4 byte keys
            {
                VectorInput input = default;
                input.Callback = 5678;
                input.CallbackContext = 9012;

                ReadOnlySpan<byte> namespaceBytes = stackalloc byte[1] { 64 };

                var data = new int[] { 4, 1234 };
                var dataCopy = data.ToArray();
                fixed (int* dataPtr = data)
                {
                    var keyData = PinnedSpanByte.FromPinnedPointer((byte*)dataPtr, data.Length * sizeof(int));
                    var batch = new VectorManager.VectorReadBatch(input.Callback, input.CallbackContext, 1, keyData, namespaceBytes);

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
                        ClassicAssert.AreEqual(64, keyCopy.NamespaceBytes[0]);
                        ClassicAssert.IsTrue(keyCopy.KeyBytes.SequenceEqual(MemoryMarshal.Cast<int, byte>(data.AsSpan().Slice(1, 1))));

                        // Validate output doesn't throw
                        batch.GetOutput(i, out _);
                    }

                    ClassicAssert.AreEqual(1, iters);

                    BasicContext<
                        Garnet.common.VectorElementKey,
                        Garnet.server.VectorInput,
                        Garnet.server.VectorOutput,
                        long, Garnet.server.VectorSessionFunctions,
                        Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
                        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>
                    > ignored = default;
                    batch.CompletePending(ref ignored);
                }
                ClassicAssert.IsTrue(dataCopy.SequenceEqual(data));
            }

            // Multiple keys, 4 byte keys
            {
                VectorInput input = default;
                input.Callback = 5678;
                input.CallbackContext = 9012;

                ReadOnlySpan<byte> namespaceBytes = stackalloc byte[1] { 32 };

                var data = new int[] { 4, 1234, 4, 5678, 4, 0123, 4, 9999, 4, 0000, 4, int.MaxValue, 4, int.MinValue };
                var dataCopy = data.ToArray();
                fixed (int* dataPtr = data)
                {
                    var keyData = PinnedSpanByte.FromPinnedPointer((byte*)dataPtr, data.Length * sizeof(int));
                    var batch = new VectorManager.VectorReadBatch(input.Callback, input.CallbackContext, 7, keyData, namespaceBytes);

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
                        ClassicAssert.AreEqual(32, keyCopy.NamespaceBytes[0]);

                        var offset = i * 2 + 1;
                        var keyCopyData = keyCopy.KeyBytes;
                        var expectedData = MemoryMarshal.Cast<int, byte>(data.AsSpan().Slice(offset, 1));
                        ClassicAssert.IsTrue(keyCopyData.SequenceEqual(expectedData));

                        // Validate output doesn't throw
                        batch.GetOutput(i, out _);
                    }

                    ClassicAssert.AreEqual(7, iters);

                    BasicContext<
                        Garnet.common.VectorElementKey,
                        Garnet.server.VectorInput,
                        Garnet.server.VectorOutput,
                        long, Garnet.server.VectorSessionFunctions,
                        Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
                        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>
                    > ignored = default;
                    batch.CompletePending(ref ignored);
                }
                ClassicAssert.IsTrue(dataCopy.SequenceEqual(data));
            }

            // Multiple keys, 4 byte keys, random order
            {
                VectorInput input = default;
                input.Callback = 5678;
                input.CallbackContext = 9012;

                ReadOnlySpan<byte> namespaceBytes = stackalloc byte[1] { 16 };

                var data = new int[] { 4, 1234, 4, 5678, 4, 0123, 4, 9999, 4, 0000, 4, int.MaxValue, 4, int.MinValue };
                var dataCopy = data.ToArray();
                fixed (int* dataPtr = data)
                {
                    var keyData = PinnedSpanByte.FromPinnedPointer((byte*)dataPtr, data.Length * sizeof(int));
                    var batch = new VectorManager.VectorReadBatch(input.Callback, input.CallbackContext, 7, keyData, namespaceBytes);

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
                        ClassicAssert.AreEqual(16, keyCopy.NamespaceBytes[0]);

                        var offset = i * 2 + 1;
                        var keyCopyData = keyCopy.KeyBytes;
                        var expectedData = MemoryMarshal.Cast<int, byte>(data.AsSpan().Slice(offset, 1));
                        ClassicAssert.IsTrue(keyCopyData.SequenceEqual(expectedData));

                        // Validate output doesn't throw
                        batch.GetOutput(i, out _);
                    }

                    BasicContext<
                        Garnet.common.VectorElementKey,
                        Garnet.server.VectorInput,
                        Garnet.server.VectorOutput,
                        long, Garnet.server.VectorSessionFunctions,
                        Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
                        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>
                    > ignored = default;
                    batch.CompletePending(ref ignored);
                }
                ClassicAssert.IsTrue(dataCopy.SequenceEqual(data));
            }

            // Single key, variable length
            {
                VectorInput input = default;
                input.Callback = 5678;
                input.CallbackContext = 9012;

                ReadOnlySpan<byte> namespaceBytes = stackalloc byte[1] { 8 };

                var key0 = "hello"u8.ToArray();
                var data =
                    MemoryMarshal.Cast<int, byte>([key0.Length])
                        .ToArray()
                        .Concat(key0)
                        .ToArray();
                var dataCopy = data.ToArray();
                fixed (byte* dataPtr = data)
                {
                    var keyData = PinnedSpanByte.FromPinnedPointer((byte*)dataPtr, data.Length);
                    var batch = new VectorManager.VectorReadBatch(input.Callback, input.CallbackContext, 1, keyData, namespaceBytes);

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
                        ClassicAssert.AreEqual(8, keyCopy.NamespaceBytes[0]);
                        var keyCopyData = keyCopy.KeyBytes;
                        var expectedData = data.AsSpan().Slice(expectedStart, expectedLength);
                        ClassicAssert.IsTrue(expectedData.SequenceEqual(keyCopyData));

                        // Validate output doesn't throw
                        batch.GetOutput(i, out _);
                    }

                    ClassicAssert.AreEqual(1, iters);

                    BasicContext<
                        Garnet.common.VectorElementKey,
                        Garnet.server.VectorInput,
                        Garnet.server.VectorOutput,
                        long, Garnet.server.VectorSessionFunctions,
                        Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
                        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>
                    > ignored = default;
                    batch.CompletePending(ref ignored);
                }
                ClassicAssert.IsTrue(dataCopy.SequenceEqual(data));
            }

            // Multiple keys, variable length
            {
                VectorInput input = default;
                input.Callback = 5678;
                input.CallbackContext = 9012;

                ReadOnlySpan<byte> namespaceBytes = stackalloc byte[1] { 4 };

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
                var dataCopy = data.ToArray();
                fixed (byte* dataPtr = data)
                {
                    var keyData = PinnedSpanByte.FromPinnedPointer((byte*)dataPtr, data.Length);
                    var batch = new VectorManager.VectorReadBatch(input.Callback, input.CallbackContext, 8, keyData, namespaceBytes);

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
                        ClassicAssert.AreEqual(4, keyCopy.NamespaceBytes[0]);
                        var keyCopyData = keyCopy.KeyBytes;
                        var expectedData = data.AsSpan().Slice(expectedStart, expectedLength);
                        ClassicAssert.IsTrue(expectedData.SequenceEqual(keyCopyData));

                        // Validate output doesn't throw
                        batch.GetOutput(i, out _);
                    }

                    ClassicAssert.AreEqual(8, iters);

                    BasicContext<
                        Garnet.common.VectorElementKey,
                        Garnet.server.VectorInput,
                        Garnet.server.VectorOutput,
                        long, Garnet.server.VectorSessionFunctions,
                        Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
                        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>
                    > ignored = default;
                    batch.CompletePending(ref ignored);
                }
                ClassicAssert.IsTrue(dataCopy.SequenceEqual(data));
            }

            // Multiple keys, variable length, random access
            {
                VectorInput input = default;
                input.Callback = 5678;
                input.CallbackContext = 9012;

                ReadOnlySpan<byte> namespaceBytes = stackalloc byte[1] { 2 };

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
                var dataCopy = data.ToArray();
                fixed (byte* dataPtr = data)
                {
                    var keyData = PinnedSpanByte.FromPinnedPointer((byte*)dataPtr, data.Length);
                    var batch = new VectorManager.VectorReadBatch(input.Callback, input.CallbackContext, 8, keyData, namespaceBytes);

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
                        ClassicAssert.AreEqual(2, keyCopy.NamespaceBytes[0]);
                        var keyCopyData = keyCopy.KeyBytes;
                        var expectedData = data.AsSpan().Slice(expectedStart, expectedLength);
                        ClassicAssert.IsTrue(expectedData.SequenceEqual(keyCopyData));

                        // Validate output doesn't throw
                        batch.GetOutput(i, out _);
                    }

                    BasicContext<
                        Garnet.common.VectorElementKey,
                        Garnet.server.VectorInput,
                        Garnet.server.VectorOutput,
                        long, Garnet.server.VectorSessionFunctions,
                        Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>,
                        Tsavorite.core.ObjectAllocator<Tsavorite.core.StoreFunctions<Garnet.common.GarnetKeyComparer, Garnet.server.GarnetRecordTriggers>>
                    > ignored = default;
                    batch.CompletePending(ref ignored);
                }
                ClassicAssert.IsTrue(dataCopy.SequenceEqual(data));
            }
        }

        [Test]
        public unsafe void MakeVectorElementKey()
        {
            var data = new int[] { 4, 1234 };
            fixed (int* intPtr = data)
            {
                var bytePtr = (byte*)intPtr;
                var span = VectorManager.MakeVectorElementKey(8, (nint)(bytePtr + 4), 4);
                ClassicAssert.AreEqual(8, span.NamespaceBytes[0]);
                ClassicAssert.AreEqual(1234, MemoryMarshal.Cast<byte, int>(span.KeyBytes)[0]);
            }
        }

        [Test]
        public async Task RecreateIndexesOnRestoreAsync()
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

                    var res1 = db.Execute("VADD", ["foo", "XB8", addData1, new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "hello world"]);
                    ClassicAssert.AreEqual(1, (int)res1);

#pragma warning disable CS0618 // Intentionally doing bad things
                    s.Save(SaveType.ForegroundSave);
#pragma warning restore CS0618

                    var commit = await server.Store.WaitForCommitAsync();
                    ClassicAssert.IsTrue(commit);
                    server.Dispose(deleteDir: false);

                    server = CreateGarnetServer(tryRecover: true);
                    server.Start();
                }

                using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
                {
                    var db = redis.GetDatabase(0);

                    var res2 = db.Execute("VADD", ["foo", "XB8", addData2, new byte[] { 0, 0, 0, 1 }, "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "fizz buzz"]);
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

                    var res1 = db.Execute("VADD", ["foo", "XB8", addData1, new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "hello world"]);
                    ClassicAssert.AreEqual(1, (int)res1);

                    expectedVSimResult = (byte[][])db.Execute("VSIM", ["foo", "XB8", queryData]);
                    ClassicAssert.AreEqual(1, expectedVSimResult.Length);
#pragma warning disable CS0618 // Intentionally doing bad things
                    s.Save(SaveType.ForegroundSave);
#pragma warning restore CS0618

                    var commit = await server.Store.WaitForCommitAsync();
                    ClassicAssert.IsTrue(commit);
                    server.Dispose(deleteDir: false);

                    server = CreateGarnetServer(tryRecover: true);
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

                    var res1 = db.Execute("VADD", ["foo", "XB8", addData1, new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "hello world"]);
                    ClassicAssert.AreEqual(1, (int)res1);

                    var res2 = db.Execute("VADD", ["foo", "XB8", addData2, new byte[] { 0, 0, 0, 1 }, "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "hello world"]);
                    ClassicAssert.AreEqual(1, (int)res2);

                    expectedVSimResult = (byte[][])db.Execute("VSIM", ["foo", "ELE", new byte[] { 0, 0, 0, 0 }]);
                    ClassicAssert.AreEqual(2, expectedVSimResult.Length);
#pragma warning disable CS0618 // Intentionally doing bad things
                    s.Save(SaveType.ForegroundSave);
#pragma warning restore CS0618

                    var commit = await server.Store.WaitForCommitAsync();
                    ClassicAssert.IsTrue(commit);
                    server.Dispose(deleteDir: false);

                    server = CreateGarnetServer(tryRecover: true);
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

                    var res1 = db.Execute("VADD", ["foo", "XB8", addData1, new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "hello world"]);
                    ClassicAssert.AreEqual(1, (int)res1);

#pragma warning disable CS0618 // Intentionally doing bad things
                    s.Save(SaveType.ForegroundSave);
#pragma warning restore CS0618

                    var commit = await server.Store.WaitForCommitAsync();
                    ClassicAssert.IsTrue(commit);
                    server.Dispose(deleteDir: false);

                    server = CreateGarnetServer(tryRecover: true);
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

                    var res1 = db.Execute("VADD", ["foo", "XB8", addData1, new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "hello world"]);
                    ClassicAssert.AreEqual(1, (int)res1);

#pragma warning disable CS0618 // Intentionally doing bad things
                    s.Save(SaveType.ForegroundSave);
#pragma warning restore CS0618

                    var commit = await server.Store.WaitForCommitAsync();
                    ClassicAssert.IsTrue(commit);
                    server.Dispose(deleteDir: false);

                    server = CreateGarnetServer(tryRecover: true);
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

                    var res1 = db.Execute("VADD", ["foo", "XB8", addData1, new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "hello world"]);
                    ClassicAssert.AreEqual(1, (int)res1);

                    var res2 = db.Execute("VADD", ["foo", "XB8", addData2, new byte[] { 0, 0, 0, 1 }, "CAS", "NOQUANT", "EF", "16", "M", "32", "SETATTR", "hello world"]);
                    ClassicAssert.AreEqual(1, (int)res2);

#pragma warning disable CS0618 // Intentionally doing bad things
                    s.Save(SaveType.ForegroundSave);
#pragma warning restore CS0618

                    var commit = await server.Store.WaitForCommitAsync();
                    ClassicAssert.IsTrue(commit);
                    server.Dispose(deleteDir: false);

                    server = CreateGarnetServer(tryRecover: true);
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

            string[] quantizers = ["NOQUANT", "Q8", "BIN", "XNOQUANT_U8", "XNOQUANT_I8", "XBIN_I8", "XBIN_U8"];
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
                    var isExtensionQuantizer = quantizer[0] == 'X';
                    var reduceValueToUse = isExtensionQuantizer ? 0 : reduceValue;
                    foreach (var ef in efValues)
                    {
                        foreach (var numLinks in mValues)
                        {
                            foreach (var vectorDim in vectorDimensions)
                            {
                                testCnt++;
                                string fooKey = $"foo:{testCnt}";

                                // Generate vector data based on quantizer type
                                // XPREQ8 requires XB8 format, NOQUANT uses VALUES format
                                object vectorData1;
                                object vectorData2;

                                if (isExtensionQuantizer)
                                {
                                    // XB8 format: byte array
                                    var bytes1 = new byte[vectorDim];
                                    var bytes2 = new byte[vectorDim];
                                    for (int i = 0; i < vectorDim; i++)
                                    {
                                        bytes1[i] = (byte)(i + 1);
                                        bytes2[i] = (byte)(i + 2);
                                    }
                                    vectorData1 = bytes1;
                                    vectorData2 = bytes2;
                                }
                                else
                                {
                                    // VALUES format: list of float strings
                                    var values1 = new List<object> { "VALUES", vectorDim.ToString() };
                                    var values2 = new List<object> { "VALUES", vectorDim.ToString() };
                                    for (int i = 1; i <= vectorDim; i++)
                                    {
                                        values1.Add($"{i}.0");
                                        values2.Add($"{i + 1}.0");
                                    }
                                    vectorData1 = values1.ToArray();
                                    vectorData2 = values2.ToArray();
                                }

                                // Create a vector set with known parameters
                                var opts = GenerateVADDOptions(fooKey, quantizer, reduceValueToUse, ef, numLinks, vectorData1, [0, 0, 0, 0]);
                                var res = db.Execute("VADD", opts);
                                ClassicAssert.AreEqual(1, (int)res);

                                string expectedEf = ef == 0 ? "200" : ef.ToString();
                                string expectedNumLinks = numLinks == 0 ? "16" : numLinks.ToString();

                                // Get VINFO - should return an array of 14 elements (6 key-value pairs)
                                var vinfoRes = (RedisValue[])db.Execute("VINFO", [fooKey]);
                                ClassicAssert.AreEqual(14, vinfoRes.Length);
                                var values = BuildDictionaryFromResponse(vinfoRes);
                                ClassicAssert.AreEqual(values["quant-type"], expectedQuantType);
                                ClassicAssert.AreEqual(values["distance-metric"], "l2");
                                ClassicAssert.AreEqual(values["input-vector-dimensions"], vectorDim.ToString());
                                ClassicAssert.AreEqual(values["reduced-dimensions"], reduceValueToUse.ToString());
                                ClassicAssert.AreEqual(values["build-exploration-factor"], expectedEf);
                                ClassicAssert.AreEqual(values["num-links"], expectedNumLinks);
                                ClassicAssert.AreEqual(values["size"], "1");

                                // Add another element and try again
                                res = db.Execute("VADD", GenerateVADDOptions(fooKey, quantizer, reduceValueToUse, ef, numLinks, vectorData2, [0, 0, 0, 1]));
                                ClassicAssert.AreEqual(1, (int)res);

                                vinfoRes = (RedisValue[])db.Execute(command: "VINFO", [fooKey]);
                                ClassicAssert.AreEqual(14, vinfoRes.Length);
                                values = BuildDictionaryFromResponse(vinfoRes);
                                ClassicAssert.AreEqual(values["quant-type"], expectedQuantType);
                                ClassicAssert.AreEqual(values["distance-metric"], "l2");
                                ClassicAssert.AreEqual(values["input-vector-dimensions"], vectorDim.ToString());
                                ClassicAssert.AreEqual(values["reduced-dimensions"], reduceValueToUse.ToString());
                                ClassicAssert.AreEqual(values["build-exploration-factor"], expectedEf);
                                ClassicAssert.AreEqual(values["num-links"], expectedNumLinks);
                                ClassicAssert.AreEqual(values["size"], "2");

                                // Delete vector set
                                db.KeyDelete(fooKey);
                            }
                        }
                    }
                }
            }

            static object[] GenerateVADDOptions(string key, string quantizer, int reduce, int buildExplorationFactor, int numLinks, object vectorData, byte[] elementId)
            {
                var isExtensionQuantizer = quantizer[0] == 'X';

                if (isExtensionQuantizer)
                {
                    reduce = 0;
                }

                List<object> opts = [key];
                if (reduce > 0)
                {
                    opts.Add("REDUCE");
                    opts.Add(reduce.ToString());
                }

                // Add vector data based on quantizer type
                if (isExtensionQuantizer)
                {
                    // XU8 format for extension methods
                    opts.Add("XU8");
                    opts.Add(vectorData);
                }
                else
                {
                    // VALUES format for NOQUANT
                    opts.AddRange((object[])vectorData);
                }

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
        public void VGETATTR_NotFound()
        {
            var vectorSetKey = "foo";
            var elementId1 = new byte[] { 0, 0, 0, 0 };
            var nonExistentElementId = new byte[] { 9, 9, 9, 9 };

            // Test not found case - non-existent vector set (RESP3)
            using var redisResp3 = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: RedisProtocol.Resp3));
            var dbResp3 = redisResp3.GetDatabase();

            var resp3Result1 = dbResp3.Execute("VGETATTR", [vectorSetKey, elementId1]);
            ClassicAssert.IsTrue(resp3Result1.IsNull);
            ClassicAssert.IsTrue(resp3Result1.Resp3Type == ResultType.Null);

            // Test not found case - non-existent vector set (RESP2)
            using var redisResp2 = ConnectionMultiplexer.Connect(TestUtils.GetConfig(protocol: RedisProtocol.Resp2));
            var dbResp2 = redisResp2.GetDatabase();

            var resp2Result1 = dbResp2.Execute("VGETATTR", [vectorSetKey, elementId1]);
            ClassicAssert.IsTrue(resp2Result1.IsNull);
            ClassicAssert.IsTrue(resp2Result1.Resp2Type == ResultType.BulkString);

            // Create a vector set with first element
            var res1 = dbResp3.Execute("VADD", ["foo", "VALUES", "3", "1.0", "2.0", "3.0", elementId1, "NOQUANT"]);
            ClassicAssert.AreEqual(1, (int)res1);

            // Test not found case - non-existent element (RESP3)
            var resp3Result2 = dbResp3.Execute("VGETATTR", [vectorSetKey, nonExistentElementId]);
            ClassicAssert.IsTrue(resp3Result2.IsNull);
            ClassicAssert.IsTrue(resp3Result2.Resp3Type == ResultType.Null);

            // Test not found case - non-existent element (RESP2)
            var resp2Result2 = dbResp2.Execute("VGETATTR", [vectorSetKey, nonExistentElementId]);
            ClassicAssert.IsTrue(resp2Result2.IsNull);
            ClassicAssert.IsTrue(resp2Result2.Resp2Type == ResultType.BulkString);
        }

        [Test]
        public void VGETATTR()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var vectorSetKey = "foo";
            var elementId1 = new byte[] { 0, 0, 0, 0 };

            // Create a vector set with first element (no attribute)
            var res1 = db.Execute("VADD", ["foo", "VALUES", "3", "1.0", "2.0", "3.0", elementId1, "NOQUANT"]);
            ClassicAssert.AreEqual(1, (int)res1);

            // Test success case - element with no attribute
            var res2 = (byte[])db.Execute("VGETATTR", [vectorSetKey, elementId1]);
            ClassicAssert.AreEqual(0, res2.Length);

            // Test various attribute sizes
            int[] attributeSizes = [64, 128, 256, 257, 512, 1024];

            for (var i = 0; i < attributeSizes.Length; i++)
            {
                var attrSize = attributeSizes[i];
                var attrData = Enumerable.Repeat((byte)(i + '0'), attrSize).ToArray();
                var elementId = new byte[] { 0, 0, 0, (byte)(i + 1) };

                // Add element with attribute of specific size
                var addRes = db.Execute("VADD", ["foo", "VALUES", "3", "4.0", "5.0", "6.0", elementId, "NOQUANT", "SETATTR", attrData]);
                ClassicAssert.AreEqual(1, (int)addRes);

                // Get and validate attribute
                var getAttrRes = (byte[])db.Execute(command: "VGETATTR", [vectorSetKey, elementId]);
                ClassicAssert.AreEqual(attrSize, getAttrRes.Length, $"Attribute size mismatch for size {attrSize}");
                ClassicAssert.IsTrue(attrData.SequenceEqual(getAttrRes), $"Attribute content mismatch for size {attrSize}");
            }

            // Test empty string attribute (equivalent to no attribute)
            var emptyAttrElement = new byte[] { 0, 0, 0, 99 };
            var res3 = db.Execute("VADD", ["foo", "VALUES", "3", "7.0", "8.0", "9.0", emptyAttrElement, "NOQUANT", "SETATTR", ""]);
            ClassicAssert.AreEqual(1, (int)res3);

            var res4 = (byte[])db.Execute("VGETATTR", [vectorSetKey, emptyAttrElement]);
            ClassicAssert.AreEqual(0, res4.Length);
        }

        [Test]
        public void VGETATTR_BinaryAttributes()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            var vectorSetKey = "binattr";

            // Attribute containing CR, LF, and CRLF sequences
            var binaryAttr = new byte[] {
                (byte)'{', (byte)'"', (byte)'k', (byte)'"', (byte)':', (byte)'"',
                0x0D, 0x0A,  // CR LF
                (byte)'"', (byte)'}',
            };
            var elem1 = new byte[] { 0, 0, 0, 1 };
            var addRes1 = db.Execute("VADD", [vectorSetKey, "VALUES", "3", "1.0", "2.0", "3.0", elem1, "NOQUANT", "SETATTR", binaryAttr]);
            ClassicAssert.AreEqual(1, (int)addRes1);

            var getRes1 = (byte[])db.Execute("VGETATTR", [vectorSetKey, elem1]);
            ClassicAssert.IsTrue(binaryAttr.SequenceEqual(getRes1), "Binary attribute with CRLF round-trip mismatch");

            // Attribute containing null bytes and high bytes
            var binaryAttr2 = new byte[] { 0x00, 0xFF, 0x0D, 0x0A, 0x01, 0xFE };
            var elem2 = new byte[] { 0, 0, 0, 2 };
            var addRes2 = db.Execute("VADD", [vectorSetKey, "VALUES", "3", "4.0", "5.0", "6.0", elem2, "NOQUANT", "SETATTR", binaryAttr2]);
            ClassicAssert.AreEqual(1, (int)addRes2);

            var getRes2 = (byte[])db.Execute("VGETATTR", [vectorSetKey, elem2]);
            ClassicAssert.IsTrue(binaryAttr2.SequenceEqual(getRes2), "Binary attribute with null/high bytes round-trip mismatch");
        }

        [Test]
        public void VREM()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Populate
            var res1 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 0, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VADD", ["foo", "REDUCE", "50", "VALUES", "75", "100.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", "4.0", "1.0", "2.0", "3.0", new byte[] { 1, 0, 0, 0 }, "CAS", "NOQUANT", "EF", "16", "M", "32"]);
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

            // VEMB doesn't return removed element
            var res7 = (string[])db.Execute("VEMB", "foo", new byte[] { 0, 0, 0, 0 });
            ClassicAssert.IsEmpty(res7);
        }

        [Test]
        public void SimpleInternalIdReuse()
        {
            const string Key = "SimpleInternalIdReuse";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Both these adds get internal id 1 due the interleaves remove
            ExpectSuccess(db.Execute("VADD", [Key, "XB8", new byte[] { 36, 127, 75, 189, 65, 104, 32, 98, 182, 97, 52, 85, 16, 176, 0, 233, 236, 90, 153, 239, 88, 107, 60, 191, 208, 50, 60, 241, 27, 21, 30, 233, 23, 9, 23, 6, 152, 179, 206, 168, 117, 201, 179, 226, 72, 114, 149, 45, 95, 5, 57, 230, 72, 50, 83, 184, 67, 140, 236, 15, 43, 46, 71, 161, 67, 75, 62, 7, 152, 249, 80, 57, 139, 241, 121 }, new byte[] { 143 }, "XPREQ8"]));
            ExpectSuccess(db.Execute("VREM", [Key, new byte[] { 143 }]));
            ExpectSuccess(db.Execute("VADD", [Key, "XB8", new byte[] { 176, 79, 173, 190, 74, 104, 121, 238, 209, 182, 91, 37, 70, 231, 58, 20, 151, 19, 62, 38, 143, 52, 79, 148, 24, 98, 242, 192, 96, 39, 76, 254, 82, 13, 217, 35, 79, 91, 9, 141, 41, 169, 86, 220, 64, 191, 98, 105, 38, 131, 145, 14, 198, 28, 190, 124, 0, 24, 165, 231, 117, 184, 142, 170, 106, 93, 210, 56, 14, 22, 197, 60, 10, 177, 253 }, new byte[] { 230, 221, 114, 84, 89, 0, 137, 154, 220, 149, 61 }, "XPREQ8"]));
            var shouldBeEmpty = (string[])db.Execute("VEMB", [Key, new byte[] { 143 }]);
            ClassicAssert.IsEmpty(shouldBeEmpty);

            static void ExpectSuccess(dynamic res)
            {
                ClassicAssert.AreEqual(1, (int)res);
            }
        }

        [Test]
        public void StressInternalIdReuse()
        {
            const int Vectors = 1_000;
            const int Deletes = 200;
            const string Key = "StressInternalIdReuse";

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Build some repeatably random data for inserts
            var vectors = new List<(byte[] Id, byte[] Data)>();
            var toDeleteVectors = new HashSet<byte[]>(ByteArrayComparer.Instance);
            var pendingAdd = new List<(byte[] Id, byte[] Data)>();
            var pendingRemove = new List<byte[]>();
            var alreadyRemoved = new List<byte[]>();
            var r = new Random(2026_01_21);
            {
                for (var i = 0; i < Vectors; i++)
                {
                    var id = new byte[r.Next(16) + 1];
                    var data = new byte[75];
                    r.NextBytes(data);
                    r.NextBytes(id);

                    if (vectors.Any(t => t.Id.SequenceEqual(id)))
                    {
                        i--;
                        continue;
                    }

                    vectors.Add((id, data));
                }

                while (toDeleteVectors.Count < Deletes)
                {
                    _ = toDeleteVectors.Add(vectors[r.Next(vectors.Count)].Id);
                }

                pendingAdd.AddRange(vectors);
                pendingRemove.AddRange(toDeleteVectors);
            }

            // Randomly interleave adds and removes
            while (pendingAdd.Count > 0 || pendingRemove.Count > 0)
            {
                if (r.Next(2) == 0 && pendingAdd.Count > 0)
                {
                    var addIx = r.Next(pendingAdd.Count);
                    var (id, data) = pendingAdd[addIx];

                    var addRes = (int)db.Execute("VADD", [Key, "XB8", data, id, "XPREQ8"]);
                    ClassicAssert.AreEqual(1, addRes);

                    pendingAdd.RemoveAt(addIx);
                }
                else if (pendingRemove.Count > 0)
                {
                    var removeIx = r.Next(pendingRemove.Count);
                    var id = pendingRemove[removeIx];

                    var shouldSucceed = !pendingAdd.Any(t => t.Id.SequenceEqual(id));

                    var remRes = (int)db.Execute("VREM", [Key, id]);

                    if (shouldSucceed)
                    {
                        ClassicAssert.AreEqual(1, remRes);

                        var embRes = (string[])db.Execute("VEMB", [Key, id]);
                        ClassicAssert.IsEmpty(embRes);

                        pendingRemove.RemoveAt(removeIx);
                        alreadyRemoved.Add(id);
                    }
                    else
                    {
                        ClassicAssert.AreEqual(0, remRes);
                    }
                }

                // Check that prior deletes remain deleted
                foreach (var id in alreadyRemoved)
                {
                    var embRes = (string[])db.Execute("VEMB", [Key, id]);
                    ClassicAssert.IsEmpty(embRes);
                }
            }

            // Validate final state
            foreach (var (id, data) in vectors)
            {
                var shouldExists = !toDeleteVectors.Contains(id);

                var embRes = (string[])db.Execute("VEMB", [Key, id]);

                if (shouldExists)
                {
                    ClassicAssert.AreEqual(data.Length, embRes.Length);
                    for (var i = 0; i < data.Length; i++)
                    {
                        ClassicAssert.AreEqual(data[i], byte.Parse(embRes[i]));
                    }
                }
                else
                {
                    ClassicAssert.IsEmpty(embRes);
                }
            }
        }

        [Test]
        [CancelAfter(30_000)]
        public async Task WithQuantizationBackfillAsync(
            [Values(VectorQuantType.NoQuant, VectorQuantType.Bin, VectorQuantType.Q8, VectorQuantType.XNoQuant_I8, VectorQuantType.XNoQuant_U8, VectorQuantType.XBin_I8, VectorQuantType.XBin_U8)] VectorQuantType quantType,
            [Values(true)] bool concurrentAdds,
            [Values(true)] bool concurrentSearches,
            CancellationToken cancellation)
        {
            const int Vectors = 5_000;
            const int Dimensions = 64;
            const string Key = nameof(WithQuantizationBackfillAsync);
            const int Count = 30;

            var connections = new ConnectionMultiplexer[concurrentAdds ? Environment.ProcessorCount : 1];

            try
            {
                var dbs = new IDatabaseAsync[connections.Length];
                for (var i = 0; i < connections.Length; i++)
                {
                    connections[i] = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                    dbs[i] = connections[i].GetDatabase(0);
                }

                using var cts = new CancellationTokenSource();

                List<Task<int>> searchTasks;
                if (concurrentSearches)
                {
                    searchTasks = [];

                    for (var i = 0; i < connections.Length; i++)
                    {
                        var searchTask =
                            Task.Run(
                                async () =>
                                {
                                    var idBytes = new byte[sizeof(int)];

                                    var count = 0;

                                    var ix = 0;
                                    while (!cts.IsCancellationRequested)
                                    {
                                        var db = dbs[ix % dbs.Length];
                                        ix++;

                                        var id = Random.Shared.Next(Vectors);

                                        BinaryPrimitives.WriteInt32LittleEndian(idBytes, id);

                                        var expectedValues = new float[Dimensions];
                                        expectedValues.AsSpan().Fill((byte)id % 128);

                                        // Perform a search, but ignore response since we don't know what we'll find
                                        var searchRes = (byte[][])await db.ExecuteAsync("VSIM", Key, "FP32", MemoryMarshal.AsBytes(expectedValues.AsSpan()).ToArray(), "COUNT", Count.ToString()).ConfigureAwait(false);

                                        // Get embedding, if not null check for validiting
                                        var vembRes = (string[])await db.ExecuteAsync("VEMB", Key, idBytes).ConfigureAwait(false);
                                        if (vembRes.Length > 0)
                                        {
                                            ClassicAssert.AreEqual(Dimensions, vembRes.Length);
                                            for (var i = 0; i < vembRes.Length; i++)
                                            {
                                                var actual = (byte)float.Parse(vembRes[i]);
                                                var expected = (byte)expectedValues[i];
                                                ClassicAssert.AreEqual(expected, actual);
                                            }
                                        }

                                        if (searchRes != null && searchRes.Length > 0 && vembRes.Length > 0)
                                        {
                                            count++;
                                        }
                                    }

                                    return count;
                                },
                                cancellation
                            );

                        searchTasks.Add(searchTask);
                    }
                }
                else
                {
                    searchTasks = [];
                }

                var addTasks = new List<Task<RedisResult>>();

                var vectorManager = server.Provider.StoreWrapper.DefaultDatabase.VectorManager;

                var quantTableStart = vectorManager.QuantizationRequestsProcessed;
                var quantBackfillStart = vectorManager.QuantizationBackfillsProcessed;

                for (var id = 0; id < Vectors; id++)
                {
                    var idBytes = new byte[sizeof(int)];
                    BinaryPrimitives.WriteInt32LittleEndian(idBytes, id);

                    var db = dbs[id % connections.Length];

                    var values = new float[Dimensions];
                    values.AsSpan().Fill((byte)id % 128);

                    var vaddArgs = new List<object>() { Key };

                    var format = (VectorValueType)(id % 4);
                    ClassicAssert.IsTrue(Enum.IsDefined(format));

                    switch (format)
                    {
                        // Treat this as VALUES
                        case VectorValueType.Invalid:
                            vaddArgs.AddRange(["VALUES", Dimensions.ToString()]);
                            for (var i = 0; i < values.Length; i++)
                            {
                                vaddArgs.Add(values[i].ToString());
                            }
                            break;
                        case VectorValueType.FP32:
                            vaddArgs.Add("FP32");
                            vaddArgs.Add(MemoryMarshal.AsBytes(values.AsSpan()).ToArray());
                            break;
                        case VectorValueType.XI8:
                            vaddArgs.Add("XI8");
                            vaddArgs.Add(values.Select(static t => (byte)t).ToArray());
                            break;
                        case VectorValueType.XU8:
                            vaddArgs.Add("XU8");
                            vaddArgs.Add(values.Select(static t => (byte)t).ToArray());
                            break;
                        default:
                            ClassicAssert.Fail($"Unexpected format: {format}");
                            break;
                    }

                    vaddArgs.Add(idBytes);

                    vaddArgs.Add(quantType.ToString());

                    var idCopy = id;
                    var addTask = db.ExecuteAsync("VADD", [.. vaddArgs]);

                    // Wait immediately if non-concurrent, otherwise queue for later
                    if (concurrentAdds)
                    {
                        addTasks.Add(addTask);
                    }
                    else
                    {
                        var res = (int)await addTask.ConfigureAwait(false);
                        ClassicAssert.AreEqual(1, res);
                    }
                }

                // If concurrent, validate everything succeeded
                if (concurrentAdds)
                {
                    var reses = await Task.WhenAll(addTasks).ConfigureAwait(false);
                    foreach (var r in reses)
                    {
                        var res = (int)r;
                        ClassicAssert.AreEqual(1, res);
                    }
                }

                // Wait for concurrent searches (if any) to complete
                if (concurrentSearches)
                {
                    cts.Cancel();
                    var successes = await Task.WhenAll(searchTasks).ConfigureAwait(false);

                    foreach (var s in successes)
                    {
                        ClassicAssert.IsTrue(s > 0);
                    }
                }

                // Wait for quantization to complete
                var noQuantizationNeeded = quantType.ToString().Contains("NOQUANT", StringComparison.OrdinalIgnoreCase) || quantType == VectorQuantType.Q8;
                var quantizationExpected = !noQuantizationNeeded;
                if (quantizationExpected)
                {
                    // We expect 1 _succesful_ table build
                    while (vectorManager.QuantizationRequestsProcessed != (quantTableStart + 1))
                    {
                        await Task.Delay(1_000, cancellation).ConfigureAwait(false);
                    }

                    // No explicit config is set, so we expect Environment.ProcessorCount _successful_ backfills after the table build
                    while (vectorManager.QuantizationBackfillsProcessed != (quantBackfillStart + Environment.ProcessorCount))
                    {
                        await Task.Delay(1_000, cancellation).ConfigureAwait(false);
                    }
                }

                // Check all vectors still present
                for (var id = 0; id < Vectors; id++)
                {
                    var idBytes = new byte[sizeof(int)];
                    var db = dbs[id % dbs.Length];

                    BinaryPrimitives.WriteInt32LittleEndian(idBytes, id);

                    var expectedValues = new float[Dimensions];
                    expectedValues.AsSpan().Fill((byte)id % 128);

                    var vembRes = (string[])await db.ExecuteAsync("VEMB", Key, idBytes).ConfigureAwait(false);
                    ClassicAssert.AreEqual(Dimensions, vembRes.Length);

                    for (var i = 0; i < vembRes.Length; i++)
                    {
                        var actual = (byte)float.Parse(vembRes[i]);
                        var expected = (byte)expectedValues[i];
                        ClassicAssert.AreEqual(expected, actual);
                    }

                    // Search should succeed
                    var vsimRes = (byte[][])await db.ExecuteAsync("VSIM", Key, "FP32", MemoryMarshal.AsBytes(expectedValues.AsSpan()).ToArray(), "COUNT", Count.ToString()).ConfigureAwait(false);
                    ClassicAssert.AreEqual(Count, vsimRes.Length);
                }
            }
            finally
            {
                foreach (var con in connections)
                {
                    con?.Dispose();
                }
            }
        }

        /// <summary>
        /// Regression test for namespace corruption on the storage-tiered (disk-backed) Vector Set path.
        ///
        /// With a tiny main-log (lowMemory) and storage tiering enabled, vector records spill to disk and
        /// are read back via pending (async-IO) RMW during DiskANN graph construction. The namespaced key
        /// carried across that pending boundary must round-trip its namespace byte intact. A regression here
        /// surfaces server-side as "Extended namespace not yet supported" (the namespace byte is read back
        /// with bit 7 set), which kills the connection mid-load.
        /// </summary>
        [Test]
        public void VADDLowMemoryStorageTierForcesDiskSpill()
        {
            // Recreate the server with a tiny main log + storage tiering so inserts spill to disk.
            TearDown();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, enableVectorSetPreview: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const string Key = "lowmem-vs";
            const int Dim = 16;
            const int Count = 4000;     // far exceeds the low-memory main log, forcing records to disk
            var rng = new Random(0);

            var id = new byte[4];
            for (var i = 0; i < Count; i++)
            {
                var vec = new float[Dim];
                for (var d = 0; d < Dim; d++)
                    vec[d] = (float)rng.NextDouble();

                BinaryPrimitives.WriteInt32LittleEndian(id, i);

                // VADD of a namespaced record; once data spills to disk, building the graph reads
                // earlier records back via pending RMW (the path that corrupts the namespace).
                var res = db.Execute("VADD", [Key, "FP32", MemoryMarshal.Cast<float, byte>(vec).ToArray(), id, "NOQUANT", "EF", "16", "M", "16"]);
                ClassicAssert.AreEqual(1, (int)res, $"VADD #{i} should succeed (server must not crash on the disk-backed path)");
            }

            // The disk-backed set must still be searchable.
            var query = new float[Dim];
            for (var d = 0; d < Dim; d++)
                query[d] = (float)rng.NextDouble();

            var sim = (RedisResult[])db.Execute("VSIM", [Key, "FP32", MemoryMarshal.Cast<float, byte>(query).ToArray(), "COUNT", "10", "EF", "64"]);
            ClassicAssert.IsNotEmpty(sim);
        }

        [Test]
        public async Task RenamesAsync([Values(false, true)] bool runRenameInTransaction)
        {
            const string SourceKey = nameof(RenamesAsync) + "_source";
            const string DestKey = nameof(RenamesAsync) + "_dest";

            ushort sourceHashSlot;
            ushort destHashSlot;

            unsafe
            {
                fixed (byte* sourcePtr = Encoding.ASCII.GetBytes(SourceKey))
                fixed (byte* destPtr = Encoding.ASCII.GetBytes(DestKey))
                {
                    sourceHashSlot = HashSlotUtils.HashSlot(PinnedSpanByte.FromPinnedPointer(sourcePtr, SourceKey.Length));
                    destHashSlot = HashSlotUtils.HashSlot(PinnedSpanByte.FromPinnedPointer(destPtr, DestKey.Length));
                    ClassicAssert.AreNotEqual(sourceHashSlot, destHashSlot);
                }
            }

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase();

            // Old key is Vector Set, and new key does not exist
            {
                _ = db.KeyDelete(SourceKey);
                _ = db.KeyDelete(DestKey);

                var vaddRes = (int)await db.ExecuteAsync("VADD", SourceKey, "VALUES", "3", "1", "2", "3", "foo");
                ClassicAssert.AreEqual(1, vaddRes);

                AssertExpectedHashSlot(server, SourceKey, sourceHashSlot);

                var renameRes = await DoRenameAsync(db, runRenameInTransaction, nx: false);
                ClassicAssert.IsTrue(renameRes);

                AssertExpectedHashSlot(server, DestKey, destHashSlot);

                var existsRes = await db.KeyExistsAsync(SourceKey);
                ClassicAssert.IsFalse(existsRes);

                var vembRes = (string[])await db.ExecuteAsync("VEMB", DestKey, "foo");
                ClassicAssert.AreEqual(3, vembRes.Length);
                ClassicAssert.AreEqual(1f, float.Parse(vembRes[0]));
                ClassicAssert.AreEqual(2f, float.Parse(vembRes[1]));
                ClassicAssert.AreEqual(3f, float.Parse(vembRes[2]));
            }

            // Old key is Vector Set, new key exists and is NOT a Vector Set
            {
                _ = db.KeyDelete(SourceKey);
                _ = db.KeyDelete(DestKey);

                var vaddRes = (int)await db.ExecuteAsync("VADD", SourceKey, "VALUES", "3", "1", "2", "3", "foo");
                ClassicAssert.AreEqual(1, vaddRes);

                AssertExpectedHashSlot(server, SourceKey, sourceHashSlot);

                var setRes = await db.StringSetAsync(DestKey, "fizzbuzz");
                ClassicAssert.IsTrue(setRes);

                // RENAMENX should fail
                var renameNxRes = await DoRenameAsync(db, runRenameInTransaction, nx: true);
                ClassicAssert.IsFalse(renameNxRes);

                var renameRes = await DoRenameAsync(db, runRenameInTransaction, nx: false);
                ClassicAssert.IsTrue(renameRes);

                AssertExpectedHashSlot(server, DestKey, destHashSlot);

                var existsRes = await db.KeyExistsAsync(SourceKey);
                ClassicAssert.IsFalse(existsRes);

                var vembRes = (string[])await db.ExecuteAsync("VEMB", DestKey, "foo");
                ClassicAssert.AreEqual(3, vembRes.Length);
                ClassicAssert.AreEqual(1f, float.Parse(vembRes[0]));
                ClassicAssert.AreEqual(2f, float.Parse(vembRes[1]));
                ClassicAssert.AreEqual(3f, float.Parse(vembRes[2]));
            }

            // Old key is Vector Set, new key exists and IS a Vector Set
            {
                _ = db.KeyDelete(SourceKey);
                _ = db.KeyDelete(DestKey);

                var vaddRes = (int)await db.ExecuteAsync("VADD", SourceKey, "VALUES", "3", "1", "2", "3", "foo");
                ClassicAssert.AreEqual(1, vaddRes);

                AssertExpectedHashSlot(server, SourceKey, sourceHashSlot);

                var vaddRes2 = (int)await db.ExecuteAsync("VADD", DestKey, "VALUES", "3", "4", "5", "6", "foo");
                ClassicAssert.AreEqual(1, vaddRes2);

                // RENAMENX should fail
                var renameNxRes = await DoRenameAsync(db, runRenameInTransaction, nx: true);
                ClassicAssert.IsFalse(renameNxRes);

                var renameRes = await DoRenameAsync(db, runRenameInTransaction, nx: false);
                ClassicAssert.IsTrue(renameRes);

                AssertExpectedHashSlot(server, DestKey, destHashSlot);

                var existsRes = await db.KeyExistsAsync(SourceKey);
                ClassicAssert.IsFalse(existsRes);

                var vembRes = (string[])await db.ExecuteAsync("VEMB", DestKey, "foo");
                ClassicAssert.AreEqual(3, vembRes.Length);
                ClassicAssert.AreEqual(1f, float.Parse(vembRes[0]));
                ClassicAssert.AreEqual(2f, float.Parse(vembRes[1]));
                ClassicAssert.AreEqual(3f, float.Parse(vembRes[2]));
            }

            // Old key is NOT a Vector Set, new key exists and IS a Vector Set
            {
                _ = db.KeyDelete(SourceKey);
                _ = db.KeyDelete(DestKey);

                var setRes = await db.StringSetAsync(SourceKey, "fizzbuzz");
                ClassicAssert.IsTrue(setRes);

                var vaddRes = (int)await db.ExecuteAsync("VADD", DestKey, "VALUES", "3", "4", "5", "6", "foo");
                ClassicAssert.AreEqual(1, vaddRes);

                AssertExpectedHashSlot(server, DestKey, destHashSlot);

                // RENAMENX should fail
                var renameNxRes = await DoRenameAsync(db, runRenameInTransaction, nx: true);
                ClassicAssert.IsFalse(renameNxRes);

                var renameRes = await DoRenameAsync(db, runRenameInTransaction, nx: false);
                ClassicAssert.IsTrue(renameRes);

                var existsRes = await db.KeyExistsAsync(SourceKey);
                ClassicAssert.IsFalse(existsRes);

                var getRes = (string)await db.StringGetAsync(DestKey);
                ClassicAssert.AreEqual("fizzbuzz", getRes);
            }

            // Perform rename, optionally in a transaction, optionally with NX
            static async Task<bool> DoRenameAsync(IDatabase db, bool inTransaction, bool nx)
            {
                var when = nx ? When.NotExists : When.Always;

                if (inTransaction)
                {
                    var tran = db.CreateTransaction();
                    var renameTask = tran.KeyRenameAsync(SourceKey, DestKey, when);
                    var tranRes = await tran.ExecuteAsync();

                    ClassicAssert.IsTrue(tranRes);

                    return await renameTask;
                }

                return await db.KeyRenameAsync(SourceKey, DestKey, when);
            }

            // Check that the hash slot stored in context metadata matches expected
            static void AssertExpectedHashSlot(GarnetServer server, string indexKey, ushort expectedHashSlot)
            {
                var store = server.Provider.StoreWrapper;
                var vectorManager = store.DefaultDatabase.VectorManager;

                unsafe
                {
                    fixed (byte* indexKeyPtr = Encoding.ASCII.GetBytes(indexKey))
                    {
                        var indexKeySpan = PinnedSpanByte.FromPinnedPointer(indexKeyPtr, indexKey.Length);

                        var namespaceForKey = vectorManager.GetNamespacesForKeys(store, [indexKeySpan], []);
                        ClassicAssert.AreEqual(VectorManager.ContextStep, namespaceForKey.Count);

                        var namespacesForHashSlot = vectorManager.GetNamespacesForHashSlots([expectedHashSlot]);
                        ClassicAssert.AreEqual(VectorManager.ContextStep, namespacesForHashSlot.Count);

                        foreach (var ns in namespaceForKey)
                        {
                            ClassicAssert.IsTrue(namespacesForHashSlot.Contains(ns));
                        }
                    }
                }
            }
        }

        [Test]
        public async Task LotsOfVectorSetsAsync()
        {
            const int NumVectorSets = 1_000;

            var connections = new ConnectionMultiplexer[Environment.ProcessorCount];
            try
            {
                var dbs = new IDatabase[connections.Length];
                for (var i = 0; i < connections.Length; i++)
                {
                    connections[i] = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig());
                    dbs[i] = connections[i].GetDatabase();
                }

                // Create them all
                {
                    var allAdds = new List<Task>();
                    for (var i = 0; i < NumVectorSets; i++)
                    {
                        TestContext.Progress.WriteLine(i);

                        var keyName = $"{nameof(LotsOfVectorSetsAsync)}_{i}";
                        var elemName = $"x{i}";
                        var vector = new byte[(i * 3) + 1];
                        vector.AsSpan().Fill((byte)i);

                        var task = CreateVectorSetAsync(dbs[i % dbs.Length], keyName, elemName, vector);

                        allAdds.Add(task);
                    }

                    await Task.WhenAll(allAdds);
                }

                // Validate them all
                {
                    var allReads = new List<Task>();
                    for (var i = 0; i < NumVectorSets; i++)
                    {
                        var keyName = $"{nameof(LotsOfVectorSetsAsync)}_{i}";
                        var elemName = $"x{i}";
                        var vector = new byte[(i * 3) + 1];
                        vector.AsSpan().Fill((byte)i);

                        var task = ReadVectorSetAsync(dbs[i % dbs.Length], keyName, elemName, vector);

                        allReads.Add(task);
                    }

                    await Task.WhenAll(allReads);
                }
            }
            finally
            {
                foreach (var con in connections)
                {
                    if (con != null)
                    {
                        await con.DisposeAsync();
                    }
                }
            }

            static async Task CreateVectorSetAsync(IDatabase db, string key, string elem, byte[] data)
            {
                var res = (int)await db.ExecuteAsync("VADD", [key, "XU8", data, elem, "NOQUANT"]);
                ClassicAssert.AreEqual(1, res);
            }

            static async Task ReadVectorSetAsync(IDatabase db, string key, string elem, byte[] data)
            {
                var sim = (string[])await db.ExecuteAsync("VSIM", [key, "XU8", data]);
                ClassicAssert.AreEqual(1, sim.Length);
                ClassicAssert.AreEqual(elem, sim[0]);

                var emb = (string[])await db.ExecuteAsync("VEMB", [key, elem]);
                ClassicAssert.AreEqual(data.Length, emb.Length);
                for (var i = 0; i < data.Length; i++)
                {
                    var expected = data[i];
                    var actual = (byte)float.Parse(emb[i]);

                    ClassicAssert.AreEqual(expected, actual);
                }
            }
        }

        [Test]
        [CancelAfter(30_000)]
        public async Task VEMBRawAsync([Values("NOQUANT", "Q8", "BIN", "XNOQUANT_U8", "XNOQUANT_I8", "XBIN_I8", "XBIN_U8")] string quantizer, CancellationToken cancellation)
        {
            const string VectorSetName = nameof(VEMBRawAsync);
            const string ElementName = nameof(ElementName);
            const int VectorsForQuantization = 2_000;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var quantizationNeeded = !(quantizer.Contains("NOQUANT", StringComparison.OrdinalIgnoreCase) || quantizer == "Q8");

            var addRes = (int)await db.ExecuteAsync("VADD", [VectorSetName, "VALUES", "3", "1.0", "2.0", "3.0", ElementName, quantizer]).ConfigureAwait(false);
            ClassicAssert.AreEqual(1, addRes);

            await CheckRAWAsync(db, quantizer).ConfigureAwait(false);

            if (quantizationNeeded)
            {
                // Trigger quantization if it's possible
                var vectorManager = server.Provider.StoreWrapper.DefaultDatabase.VectorManager;

                var quantTableStart = vectorManager.QuantizationRequestsProcessed;
                var quantBackfillStart = vectorManager.QuantizationBackfillsProcessed;

                var addsTriggeringQuantization = new Task<RedisResult>[VectorsForQuantization];

                for (var i = 0; i < addsTriggeringQuantization.Length; i++)
                {
                    addsTriggeringQuantization[i] = db.ExecuteAsync("VADD", [VectorSetName, "VALUES", "3", "4.0", "5.0", "6.0", $"{ElementName}_{i}", quantizer]);
                }

                _ = await Task.WhenAll(addsTriggeringQuantization).ConfigureAwait(false);
                foreach (var t in addsTriggeringQuantization)
                {
                    ClassicAssert.AreEqual(1, (int)await t.ConfigureAwait(false));
                }

                // We expect 1 _succesful_ table build
                while (vectorManager.QuantizationRequestsProcessed != (quantTableStart + 1))
                {
                    await Task.Delay(1_000, cancellation).ConfigureAwait(false);
                }

                // No explicit config is set, so we expect Environment.ProcessorCount _successful_ backfills after the table build
                while (vectorManager.QuantizationBackfillsProcessed != (quantBackfillStart + Environment.ProcessorCount))
                {
                    await Task.Delay(1_000, cancellation).ConfigureAwait(false);
                }

                await CheckRAWAsync(db, quantizer).ConfigureAwait(false);
            }

            static async Task CheckRAWAsync(IDatabase db, string quantizer)
            {
                var preQuantVEMBRaw = (byte[][])await db.ExecuteAsync("VEMB", [VectorSetName, ElementName, "RAW"]).ConfigureAwait(false);
                var expectedLength = quantizer == "Q8" ? 4 : 3;

                ClassicAssert.AreEqual(expectedLength, preQuantVEMBRaw.Length);

                var expectedQType =
                    quantizer switch
                    {
                        "NOQUANT" => "fp32",
                        "Q8" or "XNOQUANT_I8" or "XNOQUANT_U8" => "q8",
                        "BIN" or "XBIN_I8" or "XBIN_U8" => "bin",
                        _ => throw new InvalidOperationException($"Unexpected quantizer: {quantizer}"),
                    };
                ClassicAssert.AreEqual(expectedQType, Encoding.ASCII.GetString(preQuantVEMBRaw[0]));

                ClassicAssert.AreNotEqual(0, preQuantVEMBRaw[1].Length);
                ClassicAssert.IsFalse(double.IsNaN(double.Parse(Encoding.ASCII.GetString(preQuantVEMBRaw[2]))));

                if (expectedLength > 3)
                {
                    ClassicAssert.IsFalse(double.IsNaN(double.Parse(Encoding.ASCII.GetString(preQuantVEMBRaw[3]))));
                }
            }
        }

        [Test]
        public async Task HideInternalRecordsAsync()
        {
            // DBSIZE, INFO KEYSPACE, KEYS, SCAN shouldn't return the internal (ie. namespaced) records created for Vector Sets

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var server = redis.GetServers().Single();
            var db = redis.GetDatabase(0);

            var setRes = await db.StringSetAsync("foo", "bar").ConfigureAwait(false);
            ClassicAssert.IsTrue(setRes);

            var addRes = await db.ExecuteAsync("VADD", [nameof(HideInternalRecordsAsync), "VALUES", "3", "1.0", "2.0", "3.0", "foo"]).ConfigureAwait(false);
            ClassicAssert.AreEqual(1, (int)addRes);

            // DBSIZE
            {
                var keyCount = await server.DatabaseSizeAsync(0).ConfigureAwait(false);
                ClassicAssert.AreEqual(2, keyCount);
            }

            // INFO KEYSPACE
            {
                var info = await server.InfoAsync("keyspace").ConfigureAwait(false);
                var keyspace = info.Single();
                ClassicAssert.AreEqual("Keyspace", keyspace.Key);

                var db0Data = keyspace.Single(static kv => kv.Key.Equals("db0", StringComparison.Ordinal)).Value;
                var db0Values = db0Data.Split(",").ToDictionary(kv => kv.Split('=')[0], kv => kv.Split('=')[1]);
                ClassicAssert.AreEqual("2", db0Values["keys"]);
            }

            // KEYS
            {
                var allKeys = (string[])await db.ExecuteAsync("KEYS", "*").ConfigureAwait(false);

                ClassicAssert.AreEqual(2, allKeys.Length);
                ClassicAssert.IsTrue(allKeys.Contains("foo"));
                ClassicAssert.IsTrue(allKeys.Contains(nameof(HideInternalRecordsAsync)));
            }

            // SCAN
            {
                var allKeys = new List<string>();

                var scan = server.KeysAsync(0).ConfigureAwait(false);
                await foreach (var key in scan.ConfigureAwait(false))
                {
                    allKeys.Add(key);
                }

                ClassicAssert.AreEqual(2, allKeys.Count);
                ClassicAssert.IsTrue(allKeys.Contains("foo"));
                ClassicAssert.IsTrue(allKeys.Contains(nameof(HideInternalRecordsAsync)));
            }
        }

        /// <summary>
        /// Create a new GarnetServer instance with common parameters.
        /// </summary>
        private static GarnetServer CreateGarnetServer(bool tryRecover, bool enableVectorSetPreview = true)
        => TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, tryRecover: tryRecover, aofMemorySize: DefaultAOFMemorySize, enableVectorSetPreview: enableVectorSetPreview);

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "opts")]
        private static extern ref GarnetServerOptions GetOpts(GarnetServer server);
    }
}