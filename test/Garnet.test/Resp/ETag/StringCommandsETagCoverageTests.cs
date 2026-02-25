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

        static readonly byte[][] BitmapData = [[0xFF, 0xF0, 0x00], [0xF0, 0x00, 0xFF], [0x0F, 0xF0, 0xFF]];

        static readonly byte[][] DumpData =
        [
            [0x00, 0x01, 0x31, 0x0B, 0x00, 0xA1, 0x00, 0x1D, 0x3D, 0xDD, 0xE1, 0xAE, 0x20],
            [0x00, 0x01, 0x32, 0x0B, 0x00, 0x08, 0xDA, 0x4A, 0x37, 0xF4, 0x34, 0xC1, 0x17]
        ];

        [Test]
        public async Task AppendETagTestAsync([Values(true, false)]bool nxKey)
        {
            var cmdArgs = new object[] { StringKeys[0], StringData[1] };

            await CheckCommandAsync(RespCommand.APPEND, cmdArgs, VerifyResult, nxKey: nxKey);

            void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(nxKey ? StringData[1].Length : StringData[0].Length + StringData[1].Length, (long)result);
            }
        }

        [Test]
        public async Task BitCountETagTestAsync()
        {
            var cmdArgs = new object[] { BitmapKeys[0], 0, -1, "BIT"};

            await CheckCommandAsync(RespCommand.BITCOUNT, cmdArgs, VerifyResult, [2], isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(8, (long)result);
            }
        }

        [Test]
        public async Task BitFieldETagTestAsync([Values(true, false)] bool nxKey)
        {
            var cmdArgs = new object[] { BitmapKeys[0], "SET", "u8", 0, 42 };

            await CheckCommandAsync(RespCommand.BITFIELD, cmdArgs, VerifyResult, [2], nxKey: nxKey);

            if (!nxKey)
            {
                cmdArgs = [BitmapKeys[0], "GET", "u8", 0];

                await CheckCommandAsync(RespCommand.BITFIELD, cmdArgs, VerifyResult, [2], isReadOnly: true);
            }

            void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(nxKey ? 0 : 255, (long)result);
            }
        }

        [Test]
        public async Task BitFieldROETagTestAsync()
        {
            var cmdArgs = new object[] { BitmapKeys[0], "GET", "u8", 0 };

            await CheckCommandAsync(RespCommand.BITFIELD_RO, cmdArgs, VerifyResult, [2], isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(255, (long)result);
            }
        }

        [Test]
        public async Task BitOpETagTestAsync()
        {
            var cmdArgs = new object[] { "AND", BitmapKeys[0], BitmapKeys[1], BitmapKeys[2] };

            await CheckCommandAsync(RespCommand.BITOP, cmdArgs, VerifyResult, [2]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(3, (long)result);
            }
        }

        [Test]
        public async Task BitPosETagTestAsync()
        {
            var cmdArgs = new object[] { BitmapKeys[0], 0 };

            await CheckCommandAsync(RespCommand.BITPOS, cmdArgs, VerifyResult, [2], isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(12, (long)result);
            }
        }

        [Test]
        public async Task DecrETagTestAsync([Values(true, false)] bool nxKey)
        {
            var cmdArgs = new object[] { StringKeys[0] };

            await CheckCommandAsync(RespCommand.DECR, cmdArgs, VerifyResult, nxKey: nxKey);

            void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(nxKey ? -1 : long.Parse(StringData[0]) - 1, (long)result);
            }
        }

        [Test]
        public async Task DecrByETagTestAsync([Values(true, false)] bool nxKey)
        {
            var cmdArgs = new object[] { StringKeys[0], 2 };

            await CheckCommandAsync(RespCommand.DECRBY, cmdArgs, VerifyResult, nxKey: nxKey);

            void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(nxKey ? -2 : long.Parse(StringData[0]) - 2, (long)result);
            }
        }

        [Test]
        public async Task DumpETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0] };

            await CheckCommandAsync(RespCommand.DUMP, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.IsTrue(ByteArrayComparer.Instance.Equals(DumpData[0], (byte[])result));
            }
        }

        [Test]
        public async Task GetETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0] };

            await CheckCommandAsync(RespCommand.GET, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(StringData[0], (string)result);
            }
        }

        [Test]
        public async Task GetBitETagTestAsync()
        {
            var cmdArgs = new object[] { BitmapKeys[0], 0 };

            await CheckCommandAsync(RespCommand.GETBIT, cmdArgs, VerifyResult, [2], isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task GetDelETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0] };

            await CheckCommandAsync(RespCommand.GETDEL, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(StringData[0], (string)result);
            }
        }

        [Test]
        public async Task GetExETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], "EX", 2 };

            await CheckCommandAsync(RespCommand.GETEX, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(StringData[0], (string)result);
            }
        }

        [Test]
        public async Task GetRangeETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], 0, -1 };

            await CheckCommandAsync(RespCommand.GETRANGE, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(StringData[0], (string)result);
            }
        }

        [Test]
        public async Task GetSetETagTestAsync([Values(true, false)] bool nxKey)
        {
            var cmdArgs = new object[] { StringKeys[0], StringData[1] };

            await CheckCommandAsync(RespCommand.GETSET, cmdArgs, VerifyResult, nxKey: nxKey);

            void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(nxKey ? null : StringData[0], (string)result);
            }
        }

        [Test]
        public async Task IncrETagTestAsync([Values(true, false)] bool nxKey)
        {
            var cmdArgs = new object[] { StringKeys[0] };

            await CheckCommandAsync(RespCommand.INCR, cmdArgs, VerifyResult, nxKey: nxKey);
            
            void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(nxKey ? 1 : long.Parse(StringData[0]) + 1, (long)result);
            }
        }

        [Test]
        public async Task IncrByETagTestAsync([Values(true, false)] bool nxKey)
        {
            var cmdArgs = new object[] { StringKeys[0], 2 };

            await CheckCommandAsync(RespCommand.INCRBY, cmdArgs, VerifyResult, nxKey: nxKey);

            void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(nxKey ? 2 : long.Parse(StringData[0]) + 2, (double)result);
            }
        }

        [Test]
        public async Task IncrByFloatETagTestAsync([Values(true, false)] bool nxKey)
        {
            var cmdArgs = new object[] { StringKeys[0], 2.2 };

            await CheckCommandAsync(RespCommand.INCRBYFLOAT, cmdArgs, VerifyResult, nxKey: nxKey);

            void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(nxKey ? 2.2 : long.Parse(StringData[0]) + 2.2, (double)result);
            }
        }

        [Test]
        public async Task LCSETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], StringKeys[1] };

            await CheckCommandAsync(RespCommand.LCS, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(string.Empty, (string)result);
            }
        }

        [Test]
        public async Task MGetETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], StringKeys[1] };

            await CheckCommandAsync(RespCommand.MGET, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, result.Length);
                ClassicAssert.AreEqual(StringData[0], (string)result[0]);
                ClassicAssert.AreEqual(StringData[1], (string)result[1]);
            }
        }

        [Test]
        public async Task MSetETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], StringData[1], StringKeys[1], StringData[0] };

            await CheckCommandAsync(RespCommand.MSET, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual("OK", (string)result);
            }
        }

        [Test]
        public async Task MSetNxETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], StringData[1], StringKeys[1], StringData[0] };

            await CheckCommandAsync(RespCommand.MSETNX, cmdArgs, VerifyResult, nxKey: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, (string)result);
            }
        }

        [Test]
        public async Task PFAddETagTestAsync([Values(true, false)] bool nxKey)
        {
            var cmdArgs = new object[] { HllKeys[0], StringData[1] };

            await CheckCommandAsync(RespCommand.PFADD, cmdArgs, VerifyResult, [1], nxKey: nxKey);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task PFCountETagTestAsync()
        {
            var cmdArgs = new object[] { HllKeys[0] };

            await CheckCommandAsync(RespCommand.PFCOUNT, cmdArgs, VerifyResult, [1], isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(StringData[0].Length, (long)result);
            }
        }

        [Test]
        public async Task PFMergeETagTestAsync()
        {
            var cmdArgs = new object[] { HllKeys[0], HllKeys[1] };

            await CheckCommandAsync(RespCommand.PFMERGE, cmdArgs, VerifyResult, [1]);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual("OK", (string)result);
            }
        }

        [Test]
        public async Task PSetExETagTestAsync([Values(true, false)] bool nxKey)
        {
            var cmdArgs = new object[] { StringKeys[0], 2000, StringData[1] };

            await CheckCommandAsync(RespCommand.PSETEX, cmdArgs, VerifyResult, nxKey: nxKey);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual("OK", (string)result);
            }
        }

        [Test]
        public async Task RestoreETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], 0, DumpData[1] };

            await CheckCommandAsync(RespCommand.RESTORE, cmdArgs, VerifyResult, nxKey: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual("OK", (string)result);
            }
        }

        [Test]
        public async Task SetETagTestAsync([Values(true, false)] bool nxKey)
        {
            var cmdArgs = new object[] { StringKeys[0], StringData[1] };

            await CheckCommandAsync(RespCommand.SET, cmdArgs, VerifyResult, nxKey: nxKey);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual("OK", (string)result);
            }
        }

        [Test]
        public async Task SetBitETagTestAsync([Values(true, false)] bool nxKey)
        {
            var cmdArgs = new object[] { BitmapKeys[0], 1, 1 };

            await CheckCommandAsync(RespCommand.SETBIT, cmdArgs, VerifyResult, [2], nxKey: nxKey);

            void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(nxKey ? 0 :  1, (long)result);
            }
        }

        [Test]
        public async Task SetExETagTestAsync([Values(true, false)] bool nxKey)
        {
            var cmdArgs = new object[] { StringKeys[0], 2, StringData[1] };

            await CheckCommandAsync(RespCommand.SETEX, cmdArgs, VerifyResult, nxKey: nxKey);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual("OK", (string)result);
            }
        }

        [Test]
        public async Task SetNxETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], StringData[1] };

            await CheckCommandAsync(RespCommand.SETNX, cmdArgs, VerifyResult, nxKey: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task SetRangeETagTestAsync([Values(true, false)] bool nxKey)
        {
            var cmdArgs = new object[] { StringKeys[0], 0, StringData[1] };

            await CheckCommandAsync(RespCommand.SETRANGE, cmdArgs, VerifyResult, nxKey: nxKey);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task StrLenETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0] };

            await CheckCommandAsync(RespCommand.STRLEN, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(StringData[0].Length, (long)result);
            }
        }

        [Test]
        public async Task SubStrETagTestAsync()
        {
            var cmdArgs = new object[] { StringKeys[0], 0, -1 };

            await CheckCommandAsync(RespCommand.SUBSTR, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(StringData[0], (string)result);
            }
        }

        public override void DataSetUp(bool nxKey = false)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.KeyDelete(StringKeys);
            db.KeyDelete(HllKeys);
            db.KeyDelete(BitmapKeys);

            var success = db.StringSet(StringKeys[1], StringData[1]);
            ClassicAssert.IsTrue(success);

            if (nxKey) return;

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

            success = db.HyperLogLogAdd(HllKeys[1], StringData[2]);
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
