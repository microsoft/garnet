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
        public void OtherCommandsCovered()
        {
            ClassicAssert.IsTrue(RespCommandsInfo.TryGetRespCommandsInfo(out var info, externalOnly: true));

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

        // The following commands from the requested list are intentionally SKIPPED because they have no
        // high-level StackExchange.Redis equivalent method (and we deliberately avoid ExecuteAsync).
        // They are either Garnet-specific commands or standard Redis commands that SE.Redis does not model:
        //
        //   No SE.Redis method / not modeled: BITFIELD, BITFIELD_RO, MEMORY_USAGE, GEORADIUS_RO,
        //       GEORADIUSBYMEMBER_RO, UNLINK (KeyDelete emits DEL), WATCH
        //   Blocking commands (no SE.Redis blocking API): BLMOVE, BLPOP, BRPOP, BRPOPLPUSH, BZPOPMAX, BZPOPMIN
        //   Garnet ETag commands: DELIFGREATER, GETIFNOTMATCH, GETWITHETAG, SETIFGREATER, SETIFMATCH, SETWITHETAG
        //   Garnet Range Index commands: RICONFIG, RICREATE, RIDEL, RIEXISTS, RIGET, RIMETRICS, RIRANGE, RISCAN, RISET
        //   Garnet sorted-set field TTL commands: ZEXPIRE, ZEXPIREAT, ZEXPIRETIME, ZPERSIST, ZPEXPIRE,
        //       ZPEXPIREAT, ZPEXPIRETIME, ZPTTL, ZTTL
        //   Other Garnet-specific commands: COMMITAOF, COSCAN, EXPDELSCAN, HCOLLECT, ZCOLLECT, PURGEBP,
        //       REGISTERCS, RUNTXP, SPUBLISH, SSUBSCRIBE, WATCHMS, WATCHOS

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
        public async Task BITOPAsync()
        {
            await TestNonVectorSetCommandAsync(RunAnd).ConfigureAwait(false);
            await TestNonVectorSetCommandAsync(RunOr).ConfigureAwait(false);
            await TestNonVectorSetCommandAsync(RunXor).ConfigureAwait(false);
            await TestNonVectorSetCommandAsync(RunNot).ConfigureAwait(false);

            static Task RunAnd(IDatabase db, RedisKey againstKey)
            => db.StringBitOperationAsync(Bitwise.And, "BITOP_dest", againstKey);

            static Task RunOr(IDatabase db, RedisKey againstKey)
            => db.StringBitOperationAsync(Bitwise.Or, "BITOP_dest", againstKey);

            static Task RunXor(IDatabase db, RedisKey againstKey)
            => db.StringBitOperationAsync(Bitwise.Xor, "BITOP_dest", againstKey);

            static Task RunNot(IDatabase db, RedisKey againstKey)
            => db.StringBitOperationAsync(Bitwise.Not, "BITOP_dest", againstKey);
        }

        [Test]
        public Task BITPOSAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringBitPositionAsync(againstKey, true);
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
        public Task LCSAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringLongestCommonSubsequenceAsync(againstKey, "LCS_other");
        }

        [Test]
        public Task MGETAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringGetAsync([againstKey]);
        }

        [Test]
        public Task MSETAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringSetAsync([new KeyValuePair<RedisKey, RedisValue>(againstKey, "foo")]);
        }

        [Test]
        public Task MSETNXAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringSetAsync([new KeyValuePair<RedisKey, RedisValue>(againstKey, "foo")], When.NotExists);
        }

        [Test]
        public Task PSETEXAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringSetAsync(againstKey, "foo", TimeSpan.FromMilliseconds(10000), When.Always);
        }

        [Test]
        public Task SETAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringSetAsync(againstKey, "foo");
        }

        [Test]
        public Task SETBITAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringSetBitAsync(againstKey, 0, true);
        }

        [Test]
        public Task SETEXAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringSetAsync(againstKey, "foo", TimeSpan.FromSeconds(10), When.Always);
        }

        [Test]
        public Task SETNXAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.StringSetAsync(againstKey, "foo", null, When.NotExists);
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
            await TestNonVectorSetCommandAsync(RunLeftRight).ConfigureAwait(false);
            await TestNonVectorSetCommandAsync(RunRightLeft).ConfigureAwait(false);

            static Task RunLeftRight(IDatabase db, RedisKey againstKey)
            => db.ListMoveAsync(againstKey, "LMOVE_dest", ListSide.Left, ListSide.Right);

            static Task RunRightLeft(IDatabase db, RedisKey againstKey)
            => db.ListMoveAsync(againstKey, "LMOVE_dest", ListSide.Right, ListSide.Left);
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
                    _ = e.MoveNextAsync().ConfigureAwait(false);
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
        public Task GEORADIUSBYMEMBERAsync()
        {
            return TestNonVectorSetCommandAsync(RunCommand);

            static Task RunCommand(IDatabase db, RedisKey againstKey)
            => db.GeoRadiusAsync(againstKey, "foo", 100);
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