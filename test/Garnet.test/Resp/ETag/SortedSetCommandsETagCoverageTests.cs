// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.Resp.ETag
{
    [AllureNUnit]
    [TestFixture]
    public class SortedSetCommandsETagCoverageTests : ETagCoverageTestsBase
    {
        static readonly RedisKey[] SortedSetKeys = [KeysWithEtag[0], "ssKey2", "ssKey3"];

        static readonly SortedSetEntry[][] SortedSetData =
        [
            [new SortedSetEntry("a1", 1.1), new SortedSetEntry("a2", 1.2), new SortedSetEntry("a3", 1.3)],
                [new SortedSetEntry("b1", 2.1), new SortedSetEntry("b2", 2.2)],
                [new SortedSetEntry("b2", 2.2), new SortedSetEntry("c1", 3.1)]
        ];

        [Test]
        public async Task ZAddETagTestAsync([Values(true, false)] bool nxKey)
        {
            var cmdArgs = new object[] { SortedSetKeys[0] }.Union(SortedSetData[1]
                    .SelectMany(e => new[] { e.Score.ToString(), e.Element.ToString() })).ToArray();

            await CheckCommandAsync(RespCommand.ZADD, cmdArgs, VerifyResult, nxKey: nxKey);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, (long)result);
            }
        }

        [Test]
        public async Task ZCardETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0] };

            await CheckCommandAsync(RespCommand.ZCARD, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(SortedSetData[0].Length, (long)result);
            }
        }

        [Test]
        public async Task ZCountETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 1.15, 1.25 };

            await CheckCommandAsync(RespCommand.ZCOUNT, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task ZCollectETagTestAsync()
        {
            // Wait for the expiration of one member in the sorted set
            Thread.Sleep(TimeSpan.FromSeconds(5));

            var cmdArgs = new object[] { SortedSetKeys[0] };
            await CheckCommandAsync(RespCommand.ZCOLLECT, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual("OK", result.ToString());
            }
        }

        [Test]
        public async Task ZDiffETagTestAsync()
        {
            var cmdArgs = new object[] { 2, SortedSetKeys[0], SortedSetKeys[1] };
            await CheckCommandAsync(RespCommand.ZDIFF, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(SortedSetData[0].Length, result.Length);
            }
        }

        [Test]
        public async Task ZDiffStoreETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 2, SortedSetKeys[1], SortedSetKeys[2] };
            await CheckCommandAsync(RespCommand.ZDIFFSTORE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task ZExpireETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 2, "MEMBERS", 1, SortedSetData[0][0].Element };
            await CheckCommandAsync(RespCommand.ZEXPIRE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(1, results!.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
            }
        }

        [Test]
        public async Task ZExpireAtETagTestAsync()
        {
            var expireAt = DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeSeconds();
            var cmdArgs = new object[] { SortedSetKeys[0], expireAt, "MEMBERS", 1, SortedSetData[0][0].Element };
            await CheckCommandAsync(RespCommand.ZEXPIREAT, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(1, results!.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
            }
        }

        [Test]
        public async Task ZExpireTimeETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], "MEMBERS", 1, SortedSetData[0][0].Element };
            await CheckCommandAsync(RespCommand.ZEXPIRETIME, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeSeconds();
                ClassicAssert.GreaterOrEqual(expireTimestamp, (long)result[0]);
                ClassicAssert.Less(0, (long)result[0]);
            }
        }

        [Test]
        public async Task ZIncrByETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 2, SortedSetData[0][0].Element };

            await CheckCommandAsync(RespCommand.ZINCRBY, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(SortedSetData[0][0].Score + 2, (double)result);
            }
        }

        [Test]
        public async Task ZInterETagTestAsync()
        {
            var cmdArgs = new object[] { 2, SortedSetKeys[0], SortedSetKeys[1] };
            await CheckCommandAsync(RespCommand.ZINTER, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(0, result.Length);
            }
        }

        [Test]
        public async Task ZInterCardETagTestAsync()
        {
            var cmdArgs = new object[] { 2, SortedSetKeys[0], SortedSetKeys[1] };
            await CheckCommandAsync(RespCommand.ZINTERCARD, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(0, (long)result);
            }
        }

        [Test]
        public async Task ZInterStoreETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 2, SortedSetKeys[1], SortedSetKeys[2] };
            await CheckCommandAsync(RespCommand.ZINTERSTORE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task ZLexCountETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], "-", "+" };
            await CheckCommandAsync(RespCommand.ZLEXCOUNT, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(SortedSetData[0].Length, (long)result);
            }
        }

        [Test]
        public async Task ZMPopETagTestAsync()
        {
            var cmdArgs = new object[] { 1, SortedSetKeys[0], "MAX" };

            await CheckCommandAsync(RespCommand.ZMPOP, cmdArgs, VerifyResult);

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
        public async Task ZMScoreETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], SortedSetData[0][0].Element, SortedSetData[0][1].Element };
            await CheckCommandAsync(RespCommand.ZMSCORE, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, result.Length);
                ClassicAssert.AreEqual(SortedSetData[0][0].Score, (double)result[0]);
                ClassicAssert.AreEqual(SortedSetData[0][1].Score, (double)result[1]);
            }
        }

        [Test]
        public async Task ZPExpireETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 2000, "MEMBERS", 1, SortedSetData[0][0].Element };
            await CheckCommandAsync(RespCommand.ZPEXPIRE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(1, results!.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
            }
        }

        [Test]
        public async Task ZPExpireAtETagTestAsync()
        {
            var expireAt = DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeMilliseconds();
            var cmdArgs = new object[] { SortedSetKeys[0], expireAt, "MEMBERS", 1, SortedSetData[0][0].Element };
            await CheckCommandAsync(RespCommand.ZPEXPIREAT, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(1, results!.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
            }
        }

        [Test]
        public async Task ZPExpireTimeETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], "MEMBERS", 1, SortedSetData[0][0].Element };
            await CheckCommandAsync(RespCommand.ZPEXPIRETIME, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                var expireTimestamp = DateTimeOffset.UtcNow.AddSeconds(3).ToUnixTimeMilliseconds();
                ClassicAssert.GreaterOrEqual(expireTimestamp, (long)result[0]);
                ClassicAssert.Less(0, (long)result[0]);
            }
        }

        [Test]
        public async Task ZPersistETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], "MEMBERS", 1, SortedSetData[0][0].Element };
            await CheckCommandAsync(RespCommand.ZPERSIST, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(1, results!.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
            }
        }

        [Test]
        public async Task ZPopMaxETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0] };

            await CheckCommandAsync(RespCommand.ZPOPMAX, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var entry = (RedisResult[])result;
                ClassicAssert.AreEqual(2, entry!.Length);
                ClassicAssert.AreEqual(SortedSetData[0].Last().Element, entry[0].ToString());
                ClassicAssert.AreEqual(SortedSetData[0].Last().Score, (double)entry[1]);
            }
        }

        [Test]
        public async Task ZPopMinETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0] };

            await CheckCommandAsync(RespCommand.ZPOPMIN, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                var entry = (RedisResult[])result;
                ClassicAssert.AreEqual(2, entry!.Length);
                ClassicAssert.AreEqual(SortedSetData[0].First().Element, entry[0].ToString());
                ClassicAssert.AreEqual(SortedSetData[0].First().Score, (double)entry[1]);
            }
        }

        [Test]
        public async Task ZPTtlETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], "MEMBERS", 1, SortedSetData[0][0].Element };
            await CheckCommandAsync(RespCommand.ZPTTL, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                var ttl = TimeSpan.FromSeconds(3).TotalMilliseconds;
                ClassicAssert.GreaterOrEqual(ttl, (long)result[0]);
                ClassicAssert.Less(0, (long)result[0]);
            }
        }

        [Test]
        public async Task ZRandMemberETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0] };
            await CheckCommandAsync(RespCommand.ZRANDMEMBER, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                CollectionAssert.Contains(SortedSetData[0].Select(d => (string)d.Element), (string)result);
            }
        }

        [Test]
        public async Task ZRangeETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 0, 1 };
            await CheckCommandAsync(RespCommand.ZRANGE, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, result.Length);
                ClassicAssert.AreEqual((string)SortedSetData[0][0].Element, (string)result[0]);
                ClassicAssert.AreEqual((string)SortedSetData[0][1].Element, (string)result[1]);
            }
        }

        [Test]
        public async Task ZRangeByLexETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], "[a1", "[a2" };
            await CheckCommandAsync(RespCommand.ZRANGEBYLEX, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, result.Length);
                ClassicAssert.AreEqual((string)SortedSetData[0][0].Element, (string)result[0]);
                ClassicAssert.AreEqual((string)SortedSetData[0][1].Element, (string)result[1]);
            }
        }

        [Test]
        public async Task ZRangeByScoreETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 1, 1.25 };
            await CheckCommandAsync(RespCommand.ZRANGEBYSCORE, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, result.Length);
                ClassicAssert.AreEqual((string)SortedSetData[0][0].Element, (string)result[0]);
                ClassicAssert.AreEqual((string)SortedSetData[0][1].Element, (string)result[1]);
            }
        }

        [Test]
        public async Task ZRangeStoreETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], SortedSetKeys[2], 0, 3, "BYSCORE" };
            await CheckCommandAsync(RespCommand.ZRANGESTORE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task ZRankETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], SortedSetData[0][1].Element };
            await CheckCommandAsync(RespCommand.ZRANK, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task ZRemETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], SortedSetData[0][0].Element };
            await CheckCommandAsync(RespCommand.ZREM, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task ZRemRangeByLexETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], "[a1", "(a3" };
            await CheckCommandAsync(RespCommand.ZREMRANGEBYLEX, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, (long)result);
            }
        }

        [Test]
        public async Task ZRemRangeByRankETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 1, 2 };
            await CheckCommandAsync(RespCommand.ZREMRANGEBYRANK, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, (long)result);
            }
        }

        [Test]
        public async Task ZRemRangeByScoreETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 0, 1.25 };
            await CheckCommandAsync(RespCommand.ZREMRANGEBYSCORE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, (long)result);
            }
        }

        [Test]
        public async Task ZRevRangeETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 1, 2 };
            await CheckCommandAsync(RespCommand.ZREVRANGE, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, result.Length);
                ClassicAssert.AreEqual((string)SortedSetData[0][1].Element, (string)result[0]);
                ClassicAssert.AreEqual((string)SortedSetData[0][0].Element, (string)result[1]);
            }
        }

        [Test]
        public async Task ZRevRangeByLexETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], "[a2", "[a1" };
            await CheckCommandAsync(RespCommand.ZREVRANGEBYLEX, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, result.Length);
                ClassicAssert.AreEqual((string)SortedSetData[0][1].Element, (string)result[0]);
                ClassicAssert.AreEqual((string)SortedSetData[0][0].Element, (string)result[1]);
            }
        }

        [Test]
        public async Task ZRevRangeByScoreETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 1.25, 1 };
            await CheckCommandAsync(RespCommand.ZREVRANGEBYSCORE, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, result.Length);
                ClassicAssert.AreEqual((string)SortedSetData[0][1].Element, (string)result[0]);
                ClassicAssert.AreEqual((string)SortedSetData[0][0].Element, (string)result[1]);
            }
        }

        [Test]
        public async Task ZRevRankETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], SortedSetData[0][1].Element };
            await CheckCommandAsync(RespCommand.ZREVRANK, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(1, (long)result);
            }
        }

        [Test]
        public async Task ZScanETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 0 };
            await CheckCommandAsync(RespCommand.ZSCAN, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(2, result.Length);
            }
        }

        [Test]
        public async Task ZScoreETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], SortedSetData[0][0].Element };
            await CheckCommandAsync(RespCommand.ZSCORE, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(SortedSetData[0][0].Score, (double)result);
            }
        }

        [Test]
        public async Task ZTtlETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], "MEMBERS", 1, SortedSetData[0][0].Element };
            await CheckCommandAsync(RespCommand.ZTTL, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.GreaterOrEqual(3, (long)result[0]);
                ClassicAssert.Less(0, (long)result[0]);
            }
        }

        [Test]
        public async Task ZUnionETagTestAsync()
        {
            var cmdArgs = new object[] { 2, SortedSetKeys[0], SortedSetKeys[1] };
            await CheckCommandAsync(RespCommand.ZUNION, cmdArgs, VerifyResult, isReadOnly: true);

            static void VerifyResult(RedisResult result)
            {
                var results = (string[])result;
                CollectionAssert.AreEquivalent(SortedSetData[0].Union(SortedSetData[1]).Select(d => (string)d.Element), results!);
            }
        }

        [Test]
        public async Task ZUnionStoreETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], 2, SortedSetKeys[1], SortedSetKeys[2] };
            await CheckCommandAsync(RespCommand.ZUNIONSTORE, cmdArgs, VerifyResult);

            static void VerifyResult(RedisResult result)
            {
                ClassicAssert.AreEqual(3, (long)result);
            }
        }

        [Test]
        public async Task BZMPopETagTestAsync()
        {
            var cmdArgs = new object[] { 5, 2, SortedSetKeys[0], SortedSetKeys[1], "MAX", "COUNT", 2 };

            await CheckBlockingCommandAsync(RespCommand.BZMPOP, cmdArgs, VerifyResult);

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
        public async Task BZPopMaxETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], SortedSetKeys[1], 5 };

            await CheckBlockingCommandAsync(RespCommand.BZPOPMAX, cmdArgs, VerifyResult);

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
        public async Task BZPopMinETagTestAsync()
        {
            var cmdArgs = new object[] { SortedSetKeys[0], SortedSetKeys[1], 5 };

            await CheckBlockingCommandAsync(RespCommand.BZPOPMIN, cmdArgs, VerifyResult);

            static void VerifyResult(byte[] result)
            {
                var key1 = SortedSetKeys[0].ToString();
                var elem1 = SortedSetData[0][0];
                var btExpectedResponse =
                    $"*3\r\n${key1.Length}\r\n{key1}\r\n${elem1.Element.ToString().Length}\r\n{elem1.Element}\r\n${elem1.Score.ToString().Length}\r\n{elem1.Score}\r\n";
                TestUtils.AssertEqualUpToExpectedLength(btExpectedResponse, result);
            }
        }

        public override void DataSetUp(bool nxKey = false)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.KeyDelete(SortedSetKeys);
            if (nxKey) return;

            var zaddCmdArgs = new object[] { SortedSetKeys[0] }.Concat(SortedSetData[0]
                .SelectMany(e => new[] { e.Score.ToString(), e.Element.ToString() })).ToArray();
            var results = (string[])db.ExecWithEtag("ZADD", zaddCmdArgs);
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