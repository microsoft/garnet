// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.Resp.ETag
{
    [TestFixture]
    public class SortedSetCommandsETagCoverageTests : EtagCoverageTestsBase
    {
        static readonly RedisKey[] SortedSetKeys = [KeysWithEtag[0], "ssKey2", "ssKey3"];

        static readonly SortedSetEntry[][] SortedSetData =
        [
            [new SortedSetEntry("a1", 1.1), new SortedSetEntry("a2", 1.2), new SortedSetEntry("a3", 1.3)],
                [new SortedSetEntry("b1", 2.1), new SortedSetEntry("b2", 2.2)],
                [new SortedSetEntry("b2", 2.2), new SortedSetEntry("c1", 3.1)]
        ];

        [Test]
        public async Task ZAddETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0] }.Union(SortedSetData[1]
                    .SelectMany(e => new[] { e.Score.ToString(), e.Element.ToString() })).ToArray();

            await CheckCommandsAsync(RespCommand.ZADD, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, (long)result);
            }
        }

        [Test]
        public async Task ZCollectETagAdvancedTestAsync()
        {
            // Wait for the expiration of one member in the sorted set
            Thread.Sleep(TimeSpan.FromSeconds(5));

            var cmdArgs = new object[] { SortedSetKeys[0] };
            await CheckCommandsAsync(RespCommand.ZCOLLECT, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual("OK", result.ToString());
            }
        }

        [Test]
        public async Task ZDiffStoreETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 2, SortedSetKeys[1], SortedSetKeys[2] };
            await CheckCommandsAsync(RespCommand.ZDIFFSTORE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task ZExpireETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 2, "MEMBERS", 1, SortedSetData[0][0].Element };
            await CheckCommandsAsync(RespCommand.ZEXPIRE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(1, results!.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
            }
        }

        [Test]
        public async Task ZExpireAtETagAdvancedTestAsync()
        {
            var expireAt = DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeSeconds();
            var cmdArgs = new object[] { SortedSetKeys[0], expireAt, "MEMBERS", 1, SortedSetData[0][0].Element };
            await CheckCommandsAsync(RespCommand.ZEXPIREAT, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(1, results!.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
            }
        }

        [Test]
        public async Task ZIncrByETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 2, SortedSetData[0][0].Element };

            await CheckCommandsAsync(RespCommand.ZINCRBY, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(SortedSetData[0][0].Score + 2, (double)result);
            }
        }

        [Test]
        public async Task ZInterStoreETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 2, SortedSetKeys[1], SortedSetKeys[2] };
            await CheckCommandsAsync(RespCommand.ZINTERSTORE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task ZMPopETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { 1, SortedSetKeys[0], "MAX" };

            await CheckCommandsAsync(RespCommand.ZMPOP, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(2, results!.Length);
                ClassicAssert.AreEqual(SortedSetKeys[0], results[0].ToString());
                var entries = (RedisResult[])results[1];
                ClassicAssert.AreEqual(1, entries!.Length);
                var entry = entries[0];
                ClassicAssert.AreEqual(2, entry!.Length);
                ClassicAssert.AreEqual(SortedSetData[0].Last().Element, entry[0].ToString());
                ClassicAssert.AreEqual(SortedSetData[0].Last().Score, (double)entry[1]);
            }
        }

        [Test]
        public async Task ZRemRangeByLexETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], "[a1", "(a3" };
            await CheckCommandsAsync(RespCommand.ZREMRANGEBYLEX, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, (long)result);
            }
        }

        [Test]
        public async Task ZRemRangeByRankETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 1, 2};
            await CheckCommandsAsync(RespCommand.ZREMRANGEBYRANK, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, (long)result);
            }
        }

        [Test]
        public async Task ZRemRangeByScoreETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 0, 1.25 };
            await CheckCommandsAsync(RespCommand.ZREMRANGEBYSCORE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, (long)result);
            }
        }

        [Test]
        public async Task ZPExpireETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 2000, "MEMBERS", 1, SortedSetData[0][0].Element };
            await CheckCommandsAsync(RespCommand.ZPEXPIRE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(1, results!.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
            }
        }

        [Test]
        public async Task ZPExpireAtETagAdvancedTestAsync()
        {
            var expireAt = DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeMilliseconds();
            var cmdArgs = new object[] { SortedSetKeys[0], expireAt, "MEMBERS", 1, SortedSetData[0][0].Element };
            await CheckCommandsAsync(RespCommand.ZPEXPIREAT, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(1, results!.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
            }
        }

        [Test]
        public async Task ZPersistETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], "MEMBERS", 1, SortedSetData[0][0].Element };
            await CheckCommandsAsync(RespCommand.ZPERSIST, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(1, results!.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
            }
        }

        [Test]
        public async Task ZPopMaxETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0] };

            await CheckCommandsAsync(RespCommand.ZPOPMAX, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var entry = (RedisResult[])result;
                ClassicAssert.AreEqual(2, entry!.Length);
                ClassicAssert.AreEqual(SortedSetData[0].Last().Element, entry[0].ToString());
                ClassicAssert.AreEqual(SortedSetData[0].Last().Score, (double)entry[1]);
            }
        }

        [Test]
        public async Task ZPopMinETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0] };

            await CheckCommandsAsync(RespCommand.ZPOPMIN, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var entry = (RedisResult[])result;
                ClassicAssert.AreEqual(2, entry!.Length);
                ClassicAssert.AreEqual(SortedSetData[0].First().Element, entry[0].ToString());
                ClassicAssert.AreEqual(SortedSetData[0].First().Score, (double)entry[1]);
            }
        }

        [Test]
        public async Task ZRangeStoreETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], SortedSetKeys[2], 0, 3, "BYSCORE" };
            await CheckCommandsAsync(RespCommand.ZRANGESTORE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task ZRemETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], SortedSetData[0][0].Element };
            await CheckCommandsAsync(RespCommand.ZREM, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task ZUnionStoreETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 2, SortedSetKeys[1], SortedSetKeys[2] };
            await CheckCommandsAsync(RespCommand.ZUNIONSTORE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(3, (long)result);
            }
        }

        [Test]
        public async Task BZMPopETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { 5, 2, SortedSetKeys[0], SortedSetKeys[1], "MAX", "COUNT", 2 };

            await CheckBlockingCommandsAsync(RespCommand.BZMPOP, cmdArgs, VerifyResult);

            static void VerifyResult(byte[] result)
            {
                var key1 = SortedSetKeys[0].ToString();
                var elem1 = SortedSetData[0][2]; 
                var elem2 = SortedSetData[0][1];
                var btExpectedResponse =
                    $"*2\r\n${key1.Length}\r\n{key1}\r\n*2\r\n*2\r\n${elem1.Element.ToString().Length}\r\n{elem1.Element}\r\n${elem1.Score.ToString().Length}\r\n{elem1.Score}\r\n*2\r\n${elem2.Element.ToString().Length}\r\n{elem2.Element}\r\n${elem2.Score.ToString().Length}\r\n{elem2.Score}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, result);
            }
        }

        [Test]
        public async Task BZPopMaxETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], SortedSetKeys[1], 5 };

            await CheckBlockingCommandsAsync(RespCommand.BZPOPMAX, cmdArgs, VerifyResult);

            static void VerifyResult(byte[] result)
            {
                var key1 = SortedSetKeys[0].ToString();
                var elem1 = SortedSetData[0][^1];
                var btExpectedResponse =
                    $"*3\r\n${key1.Length}\r\n{key1}\r\n${elem1.Element.ToString().Length}\r\n{elem1.Element}\r\n${elem1.Score.ToString().Length}\r\n{elem1.Score}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, result);
            }
        }

        [Test]
        public async Task BZPopMinETagAdvancedTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], SortedSetKeys[1], 5 };

            await CheckBlockingCommandsAsync(RespCommand.BZPOPMIN, cmdArgs, VerifyResult);

            static void VerifyResult(byte[] result)
            {
                var key1 = SortedSetKeys[0].ToString();
                var elem1 = SortedSetData[0][0];
                var btExpectedResponse =
                    $"*3\r\n${key1.Length}\r\n{key1}\r\n${elem1.Element.ToString().Length}\r\n{elem1.Element}\r\n${elem1.Score.ToString().Length}\r\n{elem1.Score}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, result);
            }
        }

        public override void DataSetUp()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.KeyDelete(SortedSetKeys);

            var zaddCmdArgs = new object[] { "ZADD", SortedSetKeys[0] }.Union(SortedSetData[0]
                .SelectMany(e => new[] { e.Score.ToString(), e.Element.ToString() })).ToArray();
            var results = (string[])db.Execute("EXECWITHETAG", zaddCmdArgs);
            ClassicAssert.AreEqual(2, results!.Length);
            ClassicAssert.AreEqual(SortedSetData[0].Length, long.Parse(results[0]!));
            ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // Etag 1

            var result = (long)db.Execute("ZEXPIRE", SortedSetKeys[0], 3, "MEMBERS", 1, SortedSetData[0][0].Element);

            result = db.SortedSetAdd(SortedSetKeys[1], SortedSetData[1]);
            ClassicAssert.AreEqual(SortedSetData[1].Length, result);

            result = db.SortedSetAdd(SortedSetKeys[2], SortedSetData[2]);
            ClassicAssert.AreEqual(SortedSetData[2].Length, result);
        }
    }
}
