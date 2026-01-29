// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public abstract class RespEtagCoverageTestsBase
    {
        private GarnetServer server;

        protected static readonly string KeyWithEtag = "keyWithEtag";
        protected HashSet<RespCommand> MultiKeyCommands;
        
        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);

            server.Start();

            ClassicAssert.IsTrue(RespCommandsInfo.TryGetSimpleRespCommandsInfo(out var allSimpleInfo), "Couldn't load all command details");
            
            MultiKeyCommands = new HashSet<RespCommand>();

            for (var i = 0; i < allSimpleInfo.Length; i++)
            {
                var cmd = (RespCommand)(ushort)i;
                var simpleRespCommandInfo = allSimpleInfo[i];

                if (!cmd.IsDataCommand() || simpleRespCommandInfo.KeySpecs == null)
                    continue;

                if (simpleRespCommandInfo.IsMultiKeyCommand())
                    MultiKeyCommands.Add(cmd);
            }
        }

        public abstract void DataSetUp();

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void AllCommandsCovered()
        {
            var tests =
                typeof(RespEtagCoverageTestsBase).Assembly.GetTypes()
                    .Where(t =>
                        t != typeof(RespEtagCoverageTestsBase) &&
                        typeof(RespEtagCoverageTestsBase).IsAssignableFrom(t))
                    .SelectMany(t => t.GetMethods()).Where(static mtd => mtd.GetCustomAttribute<TestAttribute>() != null);

            HashSet<string> covered = new();

            foreach (var test in tests)
            {
                if (test.Name == nameof(AllCommandsCovered))
                    continue;

                ClassicAssert.IsTrue(test.Name.EndsWith("ETagAdvancedTestAsync"), $"Expected all tests in {nameof(RespCommandTests)} except {nameof(AllCommandsCovered)} to be per-command and end with ETagAdvancedTestAsync, unexpected test: {test.Name}");

                var command = test.Name[..^"ETagAdvancedTestAsync".Length];
                covered.Add(command);
            }

            // Check tests against RespCommand
            {
                var allWriteCommands = Enum.GetValues<RespCommand>().Where(c => c.IsWriteOnly()).Select(static x => x.NormalizeForACLs()).Distinct();
                var notCovered = allWriteCommands.Where(cmd => !covered.Contains(cmd.ToString().Replace("_", ""), StringComparer.OrdinalIgnoreCase));

                ClassicAssert.IsEmpty(notCovered, $"Commands in RespCommand not covered by ETag Tests:{Environment.NewLine}{string.Join(Environment.NewLine, notCovered.OrderBy(static x => x))}");
            }
        }

        [TestFixture]
        public class SortedSetCommandsETagCoverageTests : RespEtagCoverageTestsBase
        {
            static readonly RedisKey[] SortedSetKeys = [KeyWithEtag, "ssKey2", "ssKey3"];

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
                var cmdArgs = new object[] { 1, SortedSetKeys[0], "MAX"};

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

                var result = db.SortedSetAdd(SortedSetKeys[1], SortedSetData[1]);
                ClassicAssert.AreEqual(SortedSetData[1].Length, result);

                result = db.SortedSetAdd(SortedSetKeys[2], SortedSetData[2]);
                ClassicAssert.AreEqual(SortedSetData[2].Length, result);
            }
        }

        private async Task CheckCommandsAsync(RespCommand command, object[] commandArgs, Action<RedisResult> verifyResult)
        {
            await using var redis = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            DataSetUp();

            var etag = (long)await db.ExecuteAsync("GETETAG", KeyWithEtag);
            ClassicAssert.AreEqual(1, etag);

            var result = await db.ExecuteAsync(command.ToString(), commandArgs);
            verifyResult(result);

            etag = (long)await db.ExecuteAsync("GETETAG", KeyWithEtag);
            ClassicAssert.AreEqual(2, etag);

            // Multi-key commands do not support meta-commands yet
            if (!MultiKeyCommands.Contains(command))
            {
                DataSetUp();
                var args = new object[] { command.ToString() }.Union(commandArgs).ToArray();
                var results = (RedisResult[])await db.ExecuteAsync("EXECWITHETAG", args);
                ClassicAssert.AreEqual(2, results!.Length);
                verifyResult(results[0]);
                ClassicAssert.AreEqual(2, (long)results[1]);
            }
        }
    }
}
