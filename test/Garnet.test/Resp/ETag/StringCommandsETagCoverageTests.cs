// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.Resp.ETag
{
    [TestFixture]
    public class StringCommandsETagCoverageTests : EtagCoverageTestsBase
    {
        static readonly RedisKey[] StringKeys = [KeysWithEtag[0], "key2", "key3"];

        static readonly RedisKey[] HllKeys = [KeysWithEtag[1], "hllKey2"];

        static readonly RedisKey[] BitmapKeys = [KeysWithEtag[2], "bmKey2", "bmKey3"];

        static readonly string[] StringData = ["1", "2", "3"];

        static readonly byte[][] BitmapData = [[0x00], [0xF0], [0x0F]];

        [Test]
        public async Task AppendETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], StringData[1] };

            await CheckCommandAsync(RespCommand.APPEND, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(StringData[0].Length + StringData[1].Length, (long)result);
            }
        }

        [Test]
        public async Task BitFieldETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { BitmapKeys[0], "SET", "u8", 0, 42 };

            await CheckCommandAsync(RespCommand.BITFIELD, cmdArgs, VerifyResult, [2]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(0, (long)result);
            }
        }

        [Test]
        public async Task BitOpETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { "AND", BitmapKeys[0], BitmapKeys[1], BitmapKeys[2] };

            await CheckCommandAsync(RespCommand.BITOP, cmdArgs, VerifyResult, [2]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task DecrETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0] };

            await CheckCommandAsync(RespCommand.DECR, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(long.Parse(StringData[0]) - 1, (long)result);
            }
        }

        [Test]
        public async Task DecrByETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], 2 };

            await CheckCommandAsync(RespCommand.DECRBY, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(long.Parse(StringData[0]) - 2, (long)result);
            }
        }

        [Test]
        public async Task GetExETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], "EX", 2 };

            await CheckCommandAsync(RespCommand.GETEX, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(StringData[0], (string)result);
            }
        }

        [Test]
        public async Task GetSetETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], StringData[1] };

            await CheckCommandAsync(RespCommand.GETSET, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(StringData[0], (string)result);
            }
        }

        [Test]
        public async Task IncrETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0] };

            await CheckCommandAsync(RespCommand.INCR, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(long.Parse(StringData[0]) + 1, (long)result);
            }
        }

        [Test]
        public async Task IncrByETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], 2 };

            await CheckCommandAsync(RespCommand.INCRBY, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(long.Parse(StringData[0]) + 2, (double)result);
            }
        }

        [Test]
        public async Task IncrByFloatETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], 2.2 };

            await CheckCommandAsync(RespCommand.INCRBYFLOAT, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(long.Parse(StringData[0]) + 2.2, (double)result);
            }
        }

        [Test]
        public async Task MSetETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], StringData[1], StringKeys[1], StringData[0] };

            await CheckCommandAsync(RespCommand.MSET, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual("OK", (string)result);
            }
        }

        [Test]
        public async Task PFAddETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { HllKeys[0], StringData[1] };

            await CheckCommandAsync(RespCommand.PFADD, cmdArgs, VerifyResult, [1]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task PFMergeETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { HllKeys[0], HllKeys[1] };

            await CheckCommandAsync(RespCommand.PFMERGE, cmdArgs, VerifyResult, [1]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual("OK", (string)result);
            }
        }

        [Test]
        public async Task PSetExETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], 2000, StringData[1] };

            await CheckCommandAsync(RespCommand.PSETEX, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual("OK", (string)result);
            }
        }

        [Test]
        public async Task SetETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], StringData[1] };

            await CheckCommandAsync(RespCommand.SET, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual("OK", (string)result);
            }
        }

        [Test]
        public async Task SetBitETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { BitmapKeys[0], 1, 1 };

            await CheckCommandAsync(RespCommand.SETBIT, cmdArgs, VerifyResult, [2]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(0, (long)result);
            }
        }

        [Test]
        public async Task SetExETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], 2, StringData[1] };

            await CheckCommandAsync(RespCommand.SETEX, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual("OK", (string)result);
            }
        }

        [Test]
        public async Task SetRangeETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], 0, StringData[1] };

            await CheckCommandAsync(RespCommand.SETRANGE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        public override void DataSetUp()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.KeyDelete(StringKeys);
            db.KeyDelete(HllKeys);
            db.KeyDelete(BitmapKeys);

            var setCmdArgs = new object[] { "SET", StringKeys[0], StringData[0] };
            var results = (RedisResult[])db.Execute("EXECWITHETAG", setCmdArgs);

            ClassicAssert.AreEqual(2, results.Length);
            ClassicAssert.AreEqual("OK", (string)results[0]);
            ClassicAssert.AreEqual(1, (long)results[1]); // Etag 1

            var pfAddCmdArgs = new object[] { "PFADD", HllKeys[0], StringData[0] };
            results = (RedisResult[])db.Execute("EXECWITHETAG", pfAddCmdArgs);

            ClassicAssert.AreEqual(2, results.Length);
            ClassicAssert.AreEqual(1, (long)results[0]);
            ClassicAssert.AreEqual(1, (long)results[1]); // Etag 1

            var success = db.HyperLogLogAdd(HllKeys[1], StringData[2]);
            ClassicAssert.IsTrue(success);

            setCmdArgs = ["SET", BitmapKeys[0], BitmapData[0]];
            results = (RedisResult[])db.Execute("EXECWITHETAG", setCmdArgs);

            ClassicAssert.AreEqual(2, results.Length);
            ClassicAssert.AreEqual("OK", (string)results[0]);
            ClassicAssert.AreEqual(1, (long)results[1]); // Etag 1

            success = db.StringSet(BitmapKeys[1], BitmapData[1]);
            ClassicAssert.IsTrue(success);
            success = db.StringSet(BitmapKeys[2], BitmapData[2]);
            ClassicAssert.IsTrue(success);
        }
    }
}
