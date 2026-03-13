// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
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
    public class GeneralETagCoverageTests : ETagCoverageTestsBase
    {
        protected HashSet<RespCommand> ExcludedDataCommands =
        [
            RespCommand.WATCH,
            RespCommand.WATCHMS,
            RespCommand.WATCHOS,
            RespCommand.SPUBLISH,
            RespCommand.SSUBSCRIBE
        ];

        static readonly RedisKey[] StringKeys = [KeysWithETag[0], KeysWithETag[1], KeysWithETag[2]];

        static readonly string[] StringData = ["1", "2", "3"];

        [Test]
        public void AllDataCommandsCovered()
        {
            var tests =
                typeof(ETagCoverageTestsBase).Assembly.GetTypes()
                    .Where(t =>
                        t != typeof(ETagCoverageTestsBase) &&
                        typeof(ETagCoverageTestsBase).IsAssignableFrom(t))
                    .SelectMany(t => t.GetMethods()).Where(static mtd => mtd.GetCustomAttribute<TestAttribute>() != null);

            HashSet<string> covered = new();

            foreach (var test in tests)
            {
                if (test.Name == nameof(AllDataCommandsCovered))
                    continue;

                var command = test.Name[..^"ETagTestAsync".Length];
                covered.Add(command);
            }

            // Check tests against RespCommand
            var allDataCommands = Enum.GetValues<RespCommand>().Where(c => c.IsDataCommand()).Except(NoKeyCommands)
                .Except(ExcludedDataCommands)
                .Select(static x => x.NormalizeForACLs()).Distinct();
            var notCovered = allDataCommands.Where(cmd =>
                !covered.Contains(cmd.ToString().Replace("_", ""), StringComparer.OrdinalIgnoreCase));

            ClassicAssert.IsEmpty(notCovered,
                $"Commands in RespCommand not covered by ETag Tests:{Environment.NewLine}{string.Join(Environment.NewLine, notCovered.OrderBy(static x => x))}");
        }

        [Test]
        public async Task MultiExecTransactionWithETagAsyncTest()
        {
            await using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            DataSetUp();

            var txn = db.CreateTransaction();

            _ = txn.StringAppendAsync(StringKeys[0], StringData[1]);
            _ = txn.StringAppendAsync(StringKeys[0], StringData[2]);

            await txn.ExecuteAsync();

            var result = await db.ExecWithETagAsync(nameof(RespCommand.GET), StringKeys[0]);
            VerifyResultAndETag(result,
                r => ClassicAssert.AreEqual(StringData[0] + StringData[1] + StringData[2], (string)r), 3);

            DataSetUp();

            txn = db.CreateTransaction();

            _ = txn.ExecIfMatchAsync(1, nameof(RespCommand.INCR), StringKeys[0]);
            _ = txn.ExecIfMatchAsync(1, nameof(RespCommand.INCR), StringKeys[0]);

            await txn.ExecuteAsync();

            result = await db.ExecWithETagAsync(nameof(RespCommand.GET), StringKeys[0]);
            VerifyResultAndETag(result,
                r => ClassicAssert.AreEqual(2, (long)r), 2);
        }

        [Test]
        public async Task CustomCommandWithETagUnsupportedTestAsync()
        {
            server.Register.NewCommand("MY.SETIFPM", CommandType.ReadModifyWrite, new SetIfPMCustomCommand(), new RespCommandsInfo { Arity = 4 });

            DataSetUp();

            await using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            Assert.ThrowsAsync<RedisServerException>(async () =>
                    await db.ExecuteAsync("MY.SETIFPM", StringKeys[0], StringData[1], StringData[0]),
                Encoding.ASCII.GetString(CmdStrings.RESP_ERR_ETAG_ON_CUSTOM_PROC));
        }

        [Test]
        public async Task NonDataCommandWithMetaCommandUnsupportedTestAsync()
        {
            await using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            Assert.ThrowsAsync<RedisServerException>(async () =>
                await db.ExecWithETagAsync(nameof(RespCommand.SWAPDB), [1, 0]),
                Encoding.ASCII.GetString(CmdStrings.RESP_ERR_ETAG_META_CMD_EXPECTS_DATA_CMD));
        }

        [Test]
        public async Task ExcludedDataCommandWithMetaCommandUnsupportedTestAsync()
        {
            await using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var result = await db.ExecWithETagAsync(nameof(RespCommand.SSUBSCRIBE), "hello");
            VerifyErrorResult(result, string.Format(CmdStrings.GenericErrCmdUnsupportedWithMetaCommand, nameof(RespCommand.SSUBSCRIBE),
                nameof(RespCommand.EXECWITHETAG).ToUpper()));
        }

        public override void DataSetUp(bool nxKey = false)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            db.KeyDelete(StringKeys);
            if (nxKey) return;

            for (var i = 0; i < 2; i++)
            {
                var setCmdArgs = new object[] { StringKeys[i], StringData[i] };
                var results = (string[])db.ExecWithETag("SET", setCmdArgs);
                ClassicAssert.AreEqual(2, results!.Length);
                ClassicAssert.IsNull(results[0]);
                ClassicAssert.AreEqual(1, long.Parse(results[1]!)); // ETag 1
            }

            var expirationSet = db.KeyExpire(StringKeys[0], TimeSpan.FromMinutes(3));
            ClassicAssert.IsTrue(expirationSet);
        }
    }
}