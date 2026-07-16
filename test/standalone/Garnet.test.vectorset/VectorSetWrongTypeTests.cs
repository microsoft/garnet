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
    /// <summary>
    /// Tests for all key-taking commands correctly error if a Vector Set is used
    /// when not expected, or when a Vector Set is not used when expected.
    /// </summary>
    [TestFixture]
    public class VectorSetWrongTypeTests : TestBase
    {
        private enum KeyType
        {
            String,
            Geo,
            Hash,
            List,
            Set,
            SortedSet,
            VectorSet,
            RangeIndex,
        }

        private static KeyType[] NonVectorSetKeyTypes { get; } = [.. Enum.GetValues<KeyType>().Except([KeyType.VectorSet])];

        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = CreateGarnetServer(tryRecover: false);

            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        // Meta tests - check that we've covered all supported commands

        [Test]
        public void AllVectorSetCommandsCovered()
        {
            var toCheck = GetVectorSetCommands();

            var missing = new List<RespCommand>();
            foreach (var cmd in toCheck)
            {
                var mtd = GetType().GetMethod($"{cmd.ToString()}Async");
                if (mtd == null || mtd.GetCustomAttribute<TestAttribute>() == null)
                {
                    missing.Add(cmd);
                }
            }

            if (missing.Count > 0)
            {
                var missingCmds = string.Join(", ", missing.OrderBy(static x => x.ToString()));

                ClassicAssert.Fail($"Missing tests for {missing.Count:N0} commands: {missingCmds}");
            }
        }

        [Test]
        public void AllOtherCommandsCovered()
        {
            var toCheck = GetNonVectorSetCommands();

            var missing = new List<RespCommand>();
            foreach (var cmd in toCheck)
            {
                var mtd = GetType().GetMethod($"{cmd.ToString()}Async");
                if (mtd == null || mtd.GetCustomAttribute<TestAttribute>() == null)
                {
                    missing.Add(cmd);
                }
            }

            if (missing.Count > 0)
            {
                var missingCmds = string.Join(", ", missing.OrderBy(static x => x.ToString()));

                ClassicAssert.Fail($"Missing tests for {missing.Count:N0} commands: {missingCmds}");
            }
        }

        /// <summary>
        /// Return all Vector Set commands, these need to WRONGTYPE when run against (or with) non-Vector Set keys.
        /// </summary>
        /// <returns></returns>
        internal static IEnumerable<RespCommand> GetVectorSetCommands()
        {
            ClassicAssert.IsTrue(RespCommandsInfo.TryGetRespCommandsInfo(out var info, externalOnly: true));

            var pending = new Stack<RespCommandsInfo>(info.Values);
            var toCheck = new List<RespCommand>();

            while (pending.TryPop(out var cmd))
            {
                if ((cmd.SubCommands?.Length ?? 0) > 0)
                {
                    foreach (var sub in cmd.SubCommands)
                    {
                        pending.Push(sub);
                    }

                    continue;
                }

                if (!cmd.AclCategories.HasFlag(RespAclCategories.Vector))
                {
                    continue;
                }

                if (cmd.FirstKey > 0)
                {
                    toCheck.Add(cmd.Command);
                }
            }

            return toCheck;
        }

        /// <summary>
        /// Return all commands that need to WRONGTYPE when run against (or with) a Vector Set key.
        /// </summary>
        internal static IEnumerable<RespCommand> GetNonVectorSetCommands()
        {
            ClassicAssert.IsTrue(RespCommandsInfo.TryGetRespCommandsInfo(out var info, externalOnly: true));

            HashSet<RespCommand> knownSafeOnVectorSets = [
                RespCommand.COMMITAOF,
                RespCommand.DEL,
                RespCommand.DELIFGREATER,
                RespCommand.EXPDELSCAN,
                RespCommand.EXISTS,
                RespCommand.EXPIRE,
                RespCommand.EXPIREAT,
                RespCommand.EXPIRETIME,
                RespCommand.MEMORY_USAGE,
                RespCommand.MIGRATE,
                RespCommand.PERSIST,
                RespCommand.PEXPIRE,
                RespCommand.PEXPIREAT,
                RespCommand.PEXPIRETIME,
                RespCommand.PTTL,
                RespCommand.PUBLISH,
                RespCommand.PURGEBP,
                RespCommand.REGISTERCS,
                RespCommand.RENAME,
                RespCommand.RENAMENX,
                RespCommand.RESTORE,
                RespCommand.RICREATE,
                RespCommand.RUNTXP,
                RespCommand.MSETNX,
                RespCommand.SETNX,
                RespCommand.SPUBLISH,
                RespCommand.SSUBSCRIBE,
                RespCommand.SUBSCRIBE,
                RespCommand.TTL,
                RespCommand.TYPE,
                RespCommand.UNLINK,
                RespCommand.WATCH,
                RespCommand.WATCHMS,
                RespCommand.WATCHOS,
            ];

            var pending = new Stack<RespCommandsInfo>(info.Values);
            var toCheck = new List<RespCommand>();

            while (pending.TryPop(out var cmd))
            {
                if (cmd.Command == RespCommand.CLUSTER)
                {
                    continue;
                }

                if ((cmd.SubCommands?.Length ?? 0) > 0)
                {
                    foreach (var sub in cmd.SubCommands)
                    {
                        pending.Push(sub);
                    }

                    continue;
                }

                if (cmd.AclCategories.HasFlag(RespAclCategories.Vector))
                {
                    continue;
                }

                if (cmd.FirstKey > 0)
                {
                    toCheck.Add(cmd.Command);
                }
            }

            _ = toCheck.RemoveAll(knownSafeOnVectorSets.Contains);
            _ = toCheck.RemoveAll(GetOverwritingCommands().Contains);

            return toCheck;
        }

        /// <summary>
        /// Return all commands that can safely run against a Vector Set key, but cause that key to be overwritten.
        /// </summary>
        internal static IEnumerable<RespCommand> GetOverwritingCommands()
        => [RespCommand.MSET, RespCommand.PSETEX, RespCommand.SET, RespCommand.SETEX, RespCommand.SETIFGREATER, RespCommand.SETIFMATCH, RespCommand.SETWITHETAG];

        // Vector Set commands - these WRONGTYPE against non-Vector Set keys

        [Test]
        public Task VADDAsync()
        {
            return TestVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.VectorSetAddAsync(againstKey, VectorSetAddRequest.Member("foo", new float[] { 1, 2, 3 }));
        }

        [Test]
        public Task VREMAsync()
        {
            return TestVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.VectorSetRemoveAsync(againstKey, "foo");
        }

        [Test]
        public Task VCARDAsync()
        {
            return TestVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.VectorSetLengthAsync(againstKey);
        }

        [Test]
        public Task VDIMAsync()
        {
            return TestVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.VectorSetDimensionAsync(againstKey);
        }

        [Test]
        public Task VEMBAsync()
        {
            return TestVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.VectorSetGetApproximateVectorAsync(againstKey, "foo");
        }

        [Test]
        public Task VGETATTRAsync()
        {
            return TestVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.VectorSetGetAttributesJsonAsync(againstKey, "foo");
        }

        [Test]
        public Task VINFOAsync()
        {
            return TestVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.VectorSetInfoAsync(againstKey);
        }

        [Test]
        public Task VISMEMBERAsync()
        {
            return TestVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.VectorSetContainsAsync(againstKey, "foo");
        }

        [Test]
        public Task VLINKSAsync()
        {
            return TestVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.VectorSetGetLinksAsync(againstKey, "foo");
        }

        [Test]
        public Task VRANDMEMBERAsync()
        {
            return TestVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.VectorSetRandomMemberAsync(againstKey);
        }

        [Test]
        public Task VSETATTRAsync()
        {
            return TestVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.VectorSetSetAttributesJsonAsync(againstKey, "foo", "{\"fizz\":\"buzz\"}");
        }

        [Test]
        public async Task VSIMAsync()
        {
            await TestVectorSetCommandAsync(RunCommandByElement).ConfigureAwait(false);
            await TestVectorSetCommandAsync(RunCommandByVector).ConfigureAwait(false);

            static Task RunCommandByElement(IDatabase db, RedisKey againstKey)
            => db.VectorSetSimilaritySearchAsync(againstKey, VectorSetSimilaritySearchRequest.ByMember("foo"));

            static Task RunCommandByVector(IDatabase db, RedisKey againstKey)
            => db.VectorSetSimilaritySearchAsync(againstKey, VectorSetSimilaritySearchRequest.ByVector(new float[] { 1, 2, 3 }));
        }

        /// <summary>
        /// Common code for all Vector Set commands against non-Vector Set keys
        /// </summary>
        private async Task TestVectorSetCommandAsync(Func<IDatabase, RedisKey, Task> runCommand)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase();

            foreach (var keyType in NonVectorSetKeyTypes)
            {
                var key = await CreateKeyWithTypeAsync(db, keyType).ConfigureAwait(false);

                try
                {
                    await runCommand(db, key).ConfigureAwait(false);
                    ClassicAssert.Fail("Should have raised WRONGTYPE error");
                }
                catch (RedisServerException exc)
                {
                    ClassicAssert.IsTrue(exc.Message.StartsWith("WRONGTYPE "));
                }
            }
        }

        // Non-Vector Set commands - these WRONGTYPE against Vector Set keys

        [Test]
        public Task GETAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringGetAsync(againstKey);
        }

        [Test]
        public Task APPENDAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringAppendAsync(againstKey, "foo");
        }

        [Test]
        public Task BITCOUNTAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringBitCountAsync(againstKey);
        }

        [Test]
        public async Task BITFIELDAsync()
        {
            await TestNonVectorSetCommandAsync(RunCommand).ConfigureAwait(false);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("BITFIELD", [againstKey, "GET", "u8", "0"]);
        }

        [Test]
        public async Task BITFIELD_ROAsync()
        {
            await TestNonVectorSetCommandAsync(RunCommand).ConfigureAwait(false);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("BITFIELD_RO", [againstKey, "GET", "u8", "0"]);
        }

        [Test]
        public async Task BITOPAsync()
        {
            await TestNonVectorSetCommandAsync(RunAndWithVectorSet).ConfigureAwait(false);
            await TestNonVectorSetCommandAsync(RunOrWithVectorSet).ConfigureAwait(false);
            await TestNonVectorSetCommandAsync(RunXorWithVectorSet).ConfigureAwait(false);
            await TestNonVectorSetCommandAsync(RunNotWithVectorSet).ConfigureAwait(false);

            static Task RunAndWithVectorSet(IDatabase db, RedisKey againstKey)
            => db.StringBitOperationAsync(Bitwise.And, "BITOP_dest_and", againstKey);

            static Task RunOrWithVectorSet(IDatabase db, RedisKey againstKey)
            => db.StringBitOperationAsync(Bitwise.Or, "BITOP_dest_or", againstKey);

            static Task RunXorWithVectorSet(IDatabase db, RedisKey againstKey)
            => db.StringBitOperationAsync(Bitwise.Xor, "BITOP_dest_xor", againstKey);

            static Task RunNotWithVectorSet(IDatabase db, RedisKey againstKey)
            => db.StringBitOperationAsync(Bitwise.Not, "BITOP_dest_not", againstKey);
        }

        [Test]
        public Task BITPOSAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringBitPositionAsync(againstKey, true);
        }

        [Test]
        public async Task BLMOVEAsync()
        {
            await TestNonVectorSetCommandAsync(RunVectorSetSource).ConfigureAwait(false);
            await TestNonVectorSetCommandAsync(RunVectorSetDest).ConfigureAwait(false);

            static async Task RunVectorSetSource(IDatabase db, RedisKey againstKey)
            {
                var otherKey = againstKey.Append("_dest");
                ClassicAssert.AreEqual(1, await db.ListLeftPushAsync(otherKey, "foo").ConfigureAwait(false));

                _ = await db.ExecuteAsync("BLMOVE", againstKey, otherKey, "LEFT", "RIGHT", "30");
            }

            static async Task RunVectorSetDest(IDatabase db, RedisKey againstKey)
            {
                var otherKey = againstKey.Append("_source");
                ClassicAssert.AreEqual(1, await db.ListLeftPushAsync(otherKey, "foo").ConfigureAwait(false));

                _ = await db.ExecuteAsync("BLMOVE", otherKey, againstKey, "LEFT", "RIGHT", "30");
            }
        }

        [Test]
        public Task BLPOPAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("BLPOP", againstKey, "30");
        }

        [Test]
        public Task BRPOPAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("BRPOP", againstKey, "30");
        }

        [Test]
        public async Task BRPOPLPUSHAsync()
        {
            await TestNonVectorSetCommandAsync(RunCommandVectorSetSource);
            await TestNonVectorSetCommandAsync(RunCommandVectorSetDest);

            static async Task RunCommandVectorSetSource(IDatabase db, RedisKey againstKey)
            {
                var otherKey = againstKey.Append("_dest");
                ClassicAssert.AreEqual(1, await db.ListLeftPushAsync(otherKey, "foo").ConfigureAwait(false));

                _ = await db.ExecuteAsync("BRPOPLPUSH", againstKey, otherKey, "30");
            }

            static async Task RunCommandVectorSetDest(IDatabase db, RedisKey againstKey)
            {
                var otherKey = againstKey.Append("_dest");
                ClassicAssert.AreEqual(1, await db.ListLeftPushAsync(otherKey, "foo").ConfigureAwait(false));

                _ = await db.ExecuteAsync("BRPOPLPUSH", otherKey, againstKey, "30");
            }
        }

        [Test]
        public Task BZPOPMAXAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("BZPOPMAX", againstKey, "30");
        }

        [Test]
        public Task BZPOPMINAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("BZPOPMIN", againstKey, "30");
        }

        [Test]
        public Task DECRAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringDecrementAsync(againstKey);
        }

        [Test]
        public Task DECRBYAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringDecrementAsync(againstKey, 2L);
        }

        [Test]
        public Task DUMPAsync()
        {
            // Very technically DUMP'ing is supported on Vector Sets in Redis.
            //
            // But any reasonably sized Vector Set will be too large to return
            // so we WRONGTYPE it and deviate from Redis behavior.

            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("DUMP", againstKey);
        }

        [Test]
        public Task GETBITAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringGetBitAsync(againstKey, 0);
        }

        [Test]
        public Task GETDELAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringGetDeleteAsync(againstKey);
        }

        [Test]
        public Task GETEXAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringGetSetExpiryAsync(againstKey, TimeSpan.FromSeconds(10));
        }

        [Test]
        public Task GETRANGEAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringGetRangeAsync(againstKey, 0, -1);
        }

        [Test]
        public Task GETSETAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringGetSetAsync(againstKey, "foo");
        }

        [Test]
        public Task INCRAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringIncrementAsync(againstKey);
        }

        [Test]
        public Task INCRBYAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringIncrementAsync(againstKey, 2L);
        }

        [Test]
        public Task INCRBYFLOATAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringIncrementAsync(againstKey, 1.5);
        }

        [Test]
        public async Task LCSAsync()
        {
            await TestNonVectorSetCommandAsync(RunCommandFirstVectorSet).ConfigureAwait(false);
            await TestNonVectorSetCommandAsync(RunCommandSecondVectorSet).ConfigureAwait(false);

            static async Task RunCommandFirstVectorSet(IDatabase db, RedisKey againstKey)
            {
                var otherKey = againstKey.Append("_other");
                Assert.True(await db.StringSetAsync(otherKey, "foo").ConfigureAwait(false));

                await db.StringLongestCommonSubsequenceAsync(againstKey, otherKey);
            }

            static async Task RunCommandSecondVectorSet(IDatabase db, RedisKey againstKey)
            {
                var otherKey = againstKey.Append("_other");
                Assert.True(await db.StringSetAsync(otherKey, "foo").ConfigureAwait(false));

                await db.StringLongestCommonSubsequenceAsync(otherKey, againstKey);
            }
        }

        [Test]
        public async Task MGETAsync()
        {
            // MGET is special, and returns NULL when one of the keys it fetches is not string instead of raising WRONGTYPE

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase();

            var key = await CreateKeyWithTypeAsync(db, KeyType.VectorSet).ConfigureAwait(false);

            var res = await db.StringGetAsync([key, key]).ConfigureAwait(false);
            ClassicAssert.AreEqual(2, res.Length);
            ClassicAssert.IsTrue(res[0].IsNull);
            ClassicAssert.IsTrue(res[1].IsNull);
        }

        [Test]
        public Task SETBITAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringSetBitAsync(againstKey, 0, true);
        }

        [Test]
        public Task SETRANGEAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringSetRangeAsync(againstKey, 0, "foo");
        }

        [Test]
        public Task STRLENAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringLengthAsync(againstKey);
        }

        [Test]
        public Task SUBSTRAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringGetRangeAsync(againstKey, 0, -1);
        }

        [Test]
        public Task HDELAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashDeleteAsync(againstKey, "foo");
        }

        [Test]
        public Task HEXISTSAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashExistsAsync(againstKey, "foo");
        }

        [Test]
        public Task HEXPIREAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashFieldExpireAsync(againstKey, ["foo"], TimeSpan.FromSeconds(10));
        }

        [Test]
        public Task HEXPIREATAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashFieldExpireAsync(againstKey, ["foo"], DateTime.UtcNow.AddMinutes(10));
        }

        [Test]
        public Task HEXPIRETIMEAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashFieldGetExpireDateTimeAsync(againstKey, ["foo"]);
        }

        [Test]
        public Task HGETAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashGetAsync(againstKey, "foo");
        }

        [Test]
        public Task HGETALLAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashGetAllAsync(againstKey);
        }

        [Test]
        public Task HINCRBYAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashIncrementAsync(againstKey, "foo", 1L);
        }

        [Test]
        public Task HINCRBYFLOATAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashIncrementAsync(againstKey, "foo", 1.5);
        }

        [Test]
        public Task HKEYSAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashKeysAsync(againstKey);
        }

        [Test]
        public Task HLENAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashLengthAsync(againstKey);
        }

        [Test]
        public Task HMGETAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashGetAsync(againstKey, ["foo"]);
        }

        [Test]
        public Task HMSETAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashSetAsync(againstKey, [new HashEntry("foo", "bar")]);
        }

        [Test]
        public Task HPERSISTAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashFieldPersistAsync(againstKey, ["foo"]);
        }

        [Test]
        public Task HPEXPIREAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashFieldExpireAsync(againstKey, ["foo"], TimeSpan.FromMilliseconds(10000));
        }

        [Test]
        public Task HPEXPIREATAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashFieldExpireAsync(againstKey, ["foo"], DateTime.UtcNow.AddMinutes(10));
        }

        [Test]
        public Task HPEXPIRETIMEAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashFieldGetExpireDateTimeAsync(againstKey, ["foo"]);
        }

        [Test]
        public Task HPTTLAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashFieldGetTimeToLiveAsync(againstKey, ["foo"]);
        }

        [Test]
        public Task HRANDFIELDAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashRandomFieldAsync(againstKey);
        }

        [Test]
        public Task HSCANAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static async Task RunCommand(IDatabase db, RedisKey againstKey)
            {
                await using (var e = db.HashScanAsync(againstKey).GetAsyncEnumerator())
                {
                    _ = await e.MoveNextAsync().ConfigureAwait(false);
                }
            }
        }

        [Test]
        public Task HSETAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashSetAsync(againstKey, "foo", "bar");
        }

        [Test]
        public Task HSETNXAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashSetAsync(againstKey, "foo", "bar", When.NotExists);
        }

        [Test]
        public Task HSTRLENAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashStringLengthAsync(againstKey, "foo");
        }

        [Test]
        public Task HTTLAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashFieldGetTimeToLiveAsync(againstKey, ["foo"]);
        }

        [Test]
        public Task HVALSAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HashValuesAsync(againstKey);
        }

        [Test]
        public Task LINDEXAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ListGetByIndexAsync(againstKey, 0);
        }

        [Test]
        public async Task LINSERTAsync()
        {
            await TestNonVectorSetCommandAsync(RunBefore).ConfigureAwait(false);
            await TestNonVectorSetCommandAsync(RunAfter).ConfigureAwait(false);

            static Task RunBefore(IDatabase db, RedisKey againstKey)
            => db.ListInsertBeforeAsync(againstKey, "pivot", "foo");

            static Task RunAfter(IDatabase db, RedisKey againstKey)
            => db.ListInsertAfterAsync(againstKey, "pivot", "foo");
        }

        [Test]
        public Task LLENAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ListLengthAsync(againstKey);
        }

        [Test]
        public async Task LMOVEAsync()
        {
            await TestNonVectorSetCommandAsync(RunVectorSetSource).ConfigureAwait(false);
            await TestNonVectorSetCommandAsync(RunVectorSetDest).ConfigureAwait(false);

            static async Task RunVectorSetSource(IDatabase db, RedisKey againstKey)
            {
                var otherKey = againstKey.Append("_dest");
                ClassicAssert.AreEqual(1, await db.ListLeftPushAsync(otherKey, "foo").ConfigureAwait(false));

                _ = await db.ListMoveAsync(againstKey, otherKey, ListSide.Left, ListSide.Right).ConfigureAwait(false);
            }

            static async Task RunVectorSetDest(IDatabase db, RedisKey againstKey)
            {
                var otherKey = againstKey.Append("_source");
                ClassicAssert.AreEqual(1, await db.ListLeftPushAsync(otherKey, "foo").ConfigureAwait(false));

                _ = await db.ListMoveAsync(otherKey, againstKey, ListSide.Left, ListSide.Right).ConfigureAwait(false);
            }
        }

        [Test]
        public Task LPOPAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ListLeftPopAsync(againstKey);
        }

        [Test]
        public Task LPOSAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ListPositionAsync(againstKey, "foo");
        }

        [Test]
        public Task LPUSHAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ListLeftPushAsync(againstKey, (RedisValue)"foo");
        }

        [Test]
        public Task LPUSHXAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ListLeftPushAsync(againstKey, (RedisValue)"foo", When.Exists);
        }

        [Test]
        public Task LRANGEAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ListRangeAsync(againstKey);
        }

        [Test]
        public Task LREMAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ListRemoveAsync(againstKey, "foo");
        }

        [Test]
        public Task LSETAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ListSetByIndexAsync(againstKey, 0, "foo");
        }

        [Test]
        public Task LTRIMAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ListTrimAsync(againstKey, 0, -1);
        }

        [Test]
        public Task RPOPAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ListRightPopAsync(againstKey);
        }

        [Test]
        public Task RPOPLPUSHAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ListRightPopLeftPushAsync(againstKey, "RPOPLPUSH_dest");
        }

        [Test]
        public Task RPUSHAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ListRightPushAsync(againstKey, (RedisValue)"foo");
        }

        [Test]
        public Task RPUSHXAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ListRightPushAsync(againstKey, (RedisValue)"foo", When.Exists);
        }

        [Test]
        public Task SADDAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SetAddAsync(againstKey, "foo");
        }

        [Test]
        public Task SCARDAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SetLengthAsync(againstKey);
        }

        [Test]
        public Task SDIFFAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SetCombineAsync(StackExchange.Redis.SetOperation.Difference, [againstKey]);
        }

        [Test]
        public Task SDIFFSTOREAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SetCombineAndStoreAsync(StackExchange.Redis.SetOperation.Difference, "SDIFFSTORE_dest", [againstKey]);
        }

        [Test]
        public Task SINTERAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SetCombineAsync(StackExchange.Redis.SetOperation.Intersect, [againstKey]);
        }

        [Test]
        public Task SINTERSTOREAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SetCombineAndStoreAsync(StackExchange.Redis.SetOperation.Intersect, "SINTERSTORE_dest", [againstKey]);
        }

        [Test]
        public Task SISMEMBERAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SetContainsAsync(againstKey, "foo");
        }

        [Test]
        public Task SMEMBERSAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SetMembersAsync(againstKey);
        }

        [Test]
        public Task SMISMEMBERAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SetContainsAsync(againstKey, ["foo"]);
        }

        [Test]
        public Task SMOVEAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SetMoveAsync(againstKey, "SMOVE_dest", "foo");
        }

        [Test]
        public Task SPOPAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SetPopAsync(againstKey);
        }

        [Test]
        public Task SRANDMEMBERAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SetRandomMemberAsync(againstKey);
        }

        [Test]
        public Task SREMAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SetRemoveAsync(againstKey, "foo");
        }

        [Test]
        public Task SSCANAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static async Task RunCommand(IDatabase db, RedisKey againstKey)
            {
                await using (var e = db.SetScanAsync(againstKey).GetAsyncEnumerator())
                {
                    _ = await e.MoveNextAsync().ConfigureAwait(false);
                }
            }
        }

        [Test]
        public Task SUNIONAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SetCombineAsync(StackExchange.Redis.SetOperation.Union, [againstKey]);
        }

        [Test]
        public Task SUNIONSTOREAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SetCombineAndStoreAsync(StackExchange.Redis.SetOperation.Union, "SUNIONSTORE_dest", [againstKey]);
        }

        [Test]
        public Task ZADDAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetAddAsync(againstKey, "foo", 1.0);
        }

        [Test]
        public Task ZCARDAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetLengthAsync(againstKey);
        }

        [Test]
        public Task ZCOUNTAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetLengthAsync(againstKey, 1, 10);
        }

        [Test]
        public Task ZDIFFSTOREAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetCombineAndStoreAsync(StackExchange.Redis.SetOperation.Difference, "ZDIFFSTORE_dest", [againstKey]);
        }

        [Test]
        public Task ZINCRBYAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetIncrementAsync(againstKey, "foo", 1.0);
        }

        [Test]
        public Task ZINTERSTOREAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetCombineAndStoreAsync(StackExchange.Redis.SetOperation.Intersect, "ZINTERSTORE_dest", [againstKey]);
        }

        [Test]
        public Task ZLEXCOUNTAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetLengthByValueAsync(againstKey, "a", "z");
        }

        [Test]
        public Task ZMSCOREAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetScoresAsync(againstKey, ["foo"]);
        }

        [Test]
        public Task ZPOPMAXAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetPopAsync(againstKey, Order.Descending);
        }

        [Test]
        public Task ZPOPMINAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetPopAsync(againstKey, Order.Ascending);
        }

        [Test]
        public Task ZRANDMEMBERAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetRandomMemberAsync(againstKey);
        }

        [Test]
        public Task ZRANGEAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetRangeByRankAsync(againstKey);
        }

        [Test]
        public Task ZRANGEBYLEXAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetRangeByValueAsync(againstKey);
        }

        [Test]
        public Task ZRANGEBYSCOREAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetRangeByScoreAsync(againstKey);
        }

        [Test]
        public Task ZRANGESTOREAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetRangeAndStoreAsync(againstKey, "ZRANGESTORE_dest", 0, -1);
        }

        [Test]
        public Task ZRANKAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetRankAsync(againstKey, "foo");
        }

        [Test]
        public Task ZREMAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetRemoveAsync(againstKey, "foo");
        }

        [Test]
        public Task ZREMRANGEBYLEXAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetRemoveRangeByValueAsync(againstKey, "a", "z");
        }

        [Test]
        public Task ZREMRANGEBYRANKAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetRemoveRangeByRankAsync(againstKey, 0, -1);
        }

        [Test]
        public Task ZREMRANGEBYSCOREAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetRemoveRangeByScoreAsync(againstKey, 0, 100);
        }

        [Test]
        public Task ZREVRANGEAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetRangeByRankAsync(againstKey, 0, -1, Order.Descending);
        }

        [Test]
        public Task ZREVRANGEBYLEXAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetRangeByValueAsync(againstKey, order: Order.Descending);
        }

        [Test]
        public Task ZREVRANGEBYSCOREAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetRangeByScoreAsync(againstKey, order: Order.Descending);
        }

        [Test]
        public Task ZREVRANKAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetRankAsync(againstKey, "foo", Order.Descending);
        }

        [Test]
        public Task ZSCANAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static async Task RunCommand(IDatabase db, RedisKey againstKey)
            {
                await using (var e = db.SortedSetScanAsync(againstKey).GetAsyncEnumerator())
                {
                    _ = await e.MoveNextAsync().ConfigureAwait(false);
                }
            }
        }

        [Test]
        public Task ZSCOREAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetScoreAsync(againstKey, "foo");
        }

        [Test]
        public Task ZUNIONSTOREAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.SortedSetCombineAndStoreAsync(StackExchange.Redis.SetOperation.Union, "ZUNIONSTORE_dest", [againstKey]);
        }

        [Test]
        public Task GEOADDAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.GeoAddAsync(againstKey, 12.3, 45.6, "foo");
        }

        [Test]
        public Task GEODISTAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.GeoDistanceAsync(againstKey, "foo", "bar");
        }

        [Test]
        public Task GEOHASHAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.GeoHashAsync(againstKey, "foo");
        }

        [Test]
        public Task GEOPOSAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.GeoPositionAsync(againstKey, "foo");
        }

        [Test]
        public Task GEORADIUSAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.GeoRadiusAsync(againstKey, 12.3, 45.6, 100);
        }

        [Test]
        public Task GEORADIUS_ROAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("GEORADIUS_RO", againstKey, 12.3, 45.6, 100, "M");
        }

        [Test]
        public Task GEORADIUSBYMEMBERAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.GeoRadiusAsync(againstKey, "foo", 100);
        }

        [Test]
        public Task GEORADIUSBYMEMBER_ROAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("GEORADIUSBYMEMBER_RO", againstKey, "foo", 100, "M");
        }

        [Test]
        public async Task GEOSEARCHAsync()
        {
            await TestNonVectorSetCommandAsync(RunByMember).ConfigureAwait(false);
            await TestNonVectorSetCommandAsync(RunByCoordinate).ConfigureAwait(false);

            static Task RunByMember(IDatabase db, RedisKey againstKey)
            => db.GeoSearchAsync(againstKey, "foo", new GeoSearchCircle(100));

            static Task RunByCoordinate(IDatabase db, RedisKey againstKey)
            => db.GeoSearchAsync(againstKey, 12.3, 45.6, new GeoSearchCircle(100));
        }

        [Test]
        public Task GEOSEARCHSTOREAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.GeoSearchAndStoreAsync(againstKey, "GEOSEARCHSTORE_dest", "foo", new GeoSearchCircle(100));
        }

        [Test]
        public Task PFADDAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HyperLogLogAddAsync(againstKey, "foo");
        }

        [Test]
        public Task PFCOUNTAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HyperLogLogLengthAsync(againstKey);
        }

        [Test]
        public Task PFMERGEAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.HyperLogLogMergeAsync("PFMERGE_dest", [againstKey]);
        }

        // Garnet extensions

        [Test]
        public Task COSCANAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("CUSTOMOBJECTSCAN", againstKey, "0");
        }

        [Test]
        public Task GETIFNOTMATCHAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("GETIFNOTMATCH", againstKey, "0");
        }

        [Test]
        public Task GETWITHETAGAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("GETWITHETAG", againstKey);
        }

        [Test]
        public Task HCOLLECTAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("HCOLLECT", againstKey);
        }

        [Test]
        public Task RICONFIGAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("RI.CONFIG", againstKey);
        }

        [Test]
        public Task RIDELAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("RI.DEL", againstKey, "foo");
        }

        [Test]
        public Task RIEXISTSAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("RI.EXISTS", againstKey);
        }

        [Test]
        public Task RIGETAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("RI.GET", againstKey, "foo");
        }

        [Test]
        public Task RIMETRICSAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("RI.METRICS", againstKey);
        }

        [Test]
        public Task RIRANGEAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("RI.RANGE", againstKey, "a", "z");
        }

        [Test]
        public Task RISCANAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("RI.SCAN", againstKey, "a", "COUNT", "5");
        }

        [Test]
        public Task RISETAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("RI.SET", againstKey, "foo", "bar");
        }

        [Test]
        public Task ZCOLLECTAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("ZCOLLECT", againstKey);
        }

        [Test]
        public Task ZEXPIREAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("ZEXPIRE", againstKey, "10", "NX", "MEMBERS", "1", "foo");
        }

        [Test]
        public Task ZEXPIREATAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("ZEXPIREAT", againstKey, "10", "NX", "MEMBERS", "1", "foo");
        }

        [Test]
        public Task ZEXPIRETIMEAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("ZEXPIRETIME", againstKey, "MEMBERS", "1", "foo");
        }

        [Test]
        public Task ZPERSISTAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("ZPERSIST", againstKey, "MEMBERS", "1", "foo");
        }

        [Test]
        public Task ZPEXPIREAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("ZPEXPIRE", againstKey, "10", "NX", "MEMBERS", "1", "foo");
        }

        [Test]
        public Task ZPEXPIREATAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("ZPEXPIREAT", againstKey, "10", "NX", "MEMBERS", "1", "foo");
        }

        [Test]
        public Task ZPEXPIRETIMEAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("ZPEXPIRETIME", againstKey, "MEMBERS", "1", "foo");
        }

        [Test]
        public Task ZPTTLAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("ZPTTL", againstKey, "MEMBERS", "1", "foo");
        }

        [Test]
        public Task ZTTLAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.ExecuteAsync("ZTTL", againstKey, "MEMBERS", "1", "foo");
        }

        /// <summary>
        /// Common code for all Vector Set commands against non-Vector Set keys
        /// </summary>
        private async Task TestNonVectorSetCommandAsync(Func<IDatabase, RedisKey, Task> runCommand)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase();

            var key = await CreateKeyWithTypeAsync(db, KeyType.VectorSet).ConfigureAwait(false);

            try
            {
                await runCommand(db, key).ConfigureAwait(false);
                ClassicAssert.Fail("Should have raised WRONGTYPE error");
            }
            catch (RedisServerException exc)
            {
                ClassicAssert.IsTrue(exc.Message.StartsWith("WRONGTYPE "));
            }
        }

        // Utilities

        /// <summary>
        /// Create a new key with the given type.
        /// </summary>
        private async Task<RedisKey> CreateKeyWithTypeAsync(IDatabase db, KeyType type)
        {
            var name = $"{type}_{Guid.NewGuid()}";

            switch (type)
            {
                case KeyType.String:
                    {
                        var res = await db.StringSetAsync(name, "0").ConfigureAwait(false);
                        ClassicAssert.IsTrue(res);
                    }
                    break;
                case KeyType.Geo:
                    {
                        var res = await db.GeoAddAsync(name, 12.3, 45.6, "fizzbuzz").ConfigureAwait(false);
                        ClassicAssert.IsTrue(res);
                    }
                    break;
                case KeyType.Hash:
                    {
                        var res = await db.HashSetAsync(name, "fizz", "buzz").ConfigureAwait(false);
                        ClassicAssert.IsTrue(res);
                    }
                    break;
                case KeyType.List:
                    {
                        var res = await db.ListLeftPushAsync(name, "fizzbuzz").ConfigureAwait(false);
                        ClassicAssert.AreEqual(1, res);
                    }
                    break;
                case KeyType.Set:
                    {
                        var res = await db.SetAddAsync(name, "fizzbuzz").ConfigureAwait(false);
                        ClassicAssert.IsTrue(res);
                    }
                    break;
                case KeyType.SortedSet:
                    {
                        var res = await db.SortedSetAddAsync(name, "fizzbuzz", 1.0).ConfigureAwait(false);
                        ClassicAssert.IsTrue(res);
                    }
                    break;
                case KeyType.VectorSet:
                    {
                        var res = await db.VectorSetAddAsync(name, VectorSetAddRequest.Member("fizzbuzz", new[] { 1.0f, 2.0f, 3.0f }, "{\"foo\":\"bar\"}")).ConfigureAwait(false);
                        ClassicAssert.IsTrue(res);
                    }
                    break;
                case KeyType.RangeIndex:
                    {
                        var res = (string)await db.ExecuteAsync("RI.CREATE", name, "MEMORY").ConfigureAwait(false);
                        ClassicAssert.AreEqual("OK", res);
                    }
                    break;
                default:
                    throw new InvalidOperationException($"Unexpected KeyType: {type}");
            }

            return name;
        }

        /// <summary>
        /// Create a new GarnetServer instance with common parameters.
        /// </summary>
        private static GarnetServer CreateGarnetServer(bool tryRecover)
        => TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, tryRecover: tryRecover, enableVectorSetPreview: true, enableRangeIndexPreview: true);
    }
}