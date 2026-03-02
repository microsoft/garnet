// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.Resp.ETag
{
    [TestFixture]
    public abstract class ETagCoverageTestsBase
    {
        protected GarnetServer server;

        protected static readonly string[] KeysWithEtag = ["keyWithEtag1", "keyWithEtag2", "keyWithEtag3", "keyWithEtag4", "keyWithEtag5", "keyWithEtag6"];
        protected HashSet<RespCommand> NoKeyCommands = new();
        protected HashSet<RespCommand> OverwriteCommands = new();
        protected HashSet<RespCommand> DeleteCommands = [RespCommand.DEL, RespCommand.GETDEL, RespCommand.UNLINK];
        protected HashSet<RespCommand> SetCommands = [RespCommand.SET, RespCommand.SETEX, RespCommand.PSETEX, RespCommand.GETSET];
        protected HashSet<RespCommand> MetadataCommands =
        [
            RespCommand.GETEX,
            RespCommand.EXPIRE,
            RespCommand.EXPIREAT,
            RespCommand.PEXPIRE,
            RespCommand.PEXPIREAT,
            RespCommand.ZEXPIRE,
            RespCommand.ZEXPIREAT,
            RespCommand.ZPEXPIRE,
            RespCommand.ZPEXPIREAT,
            RespCommand.HEXPIRE,
            RespCommand.HEXPIREAT,
            RespCommand.HPEXPIRE,
            RespCommand.HPEXPIREAT,
            RespCommand.PERSIST,
            RespCommand.ZPERSIST,
            RespCommand.HPERSIST,
            RespCommand.ZCOLLECT,
            RespCommand.HCOLLECT
        ];

        protected HashSet<RespCommand> MetaCommandUnsupportedCommands =
        [
            RespCommand.BITOP, 
            RespCommand.BLMOVE, 
            RespCommand.BLMPOP, 
            RespCommand.BLPOP, 
            RespCommand.BRPOP,
            RespCommand.BRPOPLPUSH, 
            RespCommand.BZMPOP, 
            RespCommand.BZPOPMAX, 
            RespCommand.BZPOPMIN, 
            RespCommand.DEL,
            RespCommand.EVAL,
            RespCommand.EVALSHA, 
            RespCommand.EXISTS,
            RespCommand.GETSET,
            RespCommand.GEORADIUS, 
            RespCommand.GEORADIUSBYMEMBER, 
            RespCommand.GEOSEARCHSTORE,
            RespCommand.HCOLLECT, 
            RespCommand.LCS, 
            RespCommand.LMOVE, 
            RespCommand.LMPOP, 
            RespCommand.MGET,
            RespCommand.MSET, 
            RespCommand.MSETNX, 
            RespCommand.PFCOUNT, 
            RespCommand.PFMERGE, 
            RespCommand.RPOPLPUSH,
            RespCommand.SDIFF, 
            RespCommand.SDIFFSTORE, 
            RespCommand.SINTER, 
            RespCommand.SINTERCARD,
            RespCommand.SINTERSTORE, 
            RespCommand.SMOVE, 
            RespCommand.SUNION, 
            RespCommand.SUNIONSTORE, 
            RespCommand.UNLINK,
            RespCommand.ZCOLLECT, 
            RespCommand.ZDIFF, 
            RespCommand.ZDIFFSTORE, 
            RespCommand.ZINTER, 
            RespCommand.ZINTERCARD,
            RespCommand.ZINTERSTORE, 
            RespCommand.ZMPOP, 
            RespCommand.ZRANGESTORE, 
            RespCommand.ZUNION,
            RespCommand.ZUNIONSTORE
        ];

        // Multi-key commands that support meta-commands when called with a single-key
        protected HashSet<RespCommand> MultiKeyCommandsSupportingSingleKey = [RespCommand.DEL, RespCommand.UNLINK, RespCommand.EXISTS];

        protected RespMetaCommand CurrMetaCommand;
        protected bool CommandExecuted;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableLua: true);

            server.Start();

            ClassicAssert.IsTrue(RespCommandsInfo.TryGetSimpleRespCommandsInfo(out var allSimpleInfo), "Couldn't load all command details");
            
            for (var i = 0; i < allSimpleInfo.Length; i++)
            {
                var cmd = (RespCommand)(ushort)i;
                var simpleRespCommandInfo = allSimpleInfo[i];

                if (!cmd.IsDataCommand())
                    continue;

                if (simpleRespCommandInfo.KeySpecs == null || simpleRespCommandInfo.KeySpecs.Length == 0)
                {
                    NoKeyCommands.Add(cmd);
                    continue;
                }

                if (simpleRespCommandInfo.IsOverwriteCommand() || cmd == RespCommand.SET)
                    OverwriteCommands.Add(cmd);
            }
        }

        public abstract void DataSetUp(bool nxKey = false);

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        protected async Task CheckCommandAsync(RespCommand command, object[] commandArgs,
            Action<RedisResult> verifyResult, int[] checkKeysWithEtag = null, bool nxKey = false,
            bool isReadOnly = false, int? totalKeys = null)
        {
            await using var redis = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            DataSetUp(nxKey);

            if (!nxKey)
            {
                checkKeysWithEtag ??= [0];

                // Verify all keys set-up with ETags have an ETag of 1
                foreach (var i in checkKeysWithEtag)
                {
                    var etag = (long)await db.ExecuteAsync("GETETAG", KeysWithEtag[i]);
                    ClassicAssert.AreEqual(1, etag);
                }

                // Check running the command without a meta-command
                await CheckCommandWithoutMetaCommand(db, command, commandArgs, verifyResult, checkKeysWithEtag, isReadOnly);
            }

            // Check running the command with different meta-commands
            // Note: Multi-key commands do not support meta-commands yet
            if (!MetaCommandUnsupportedCommands.Contains(command) || (MultiKeyCommandsSupportingSingleKey.Contains(command) && totalKeys == 1))
            {
                // Check running the command with EXECWITHETAG meta-command
                await CheckCommandWithExecWithEtag(db, command, commandArgs, verifyResult, nxKey, isReadOnly);

                // Check running the command with EXECIFMATCH meta-command
                await CheckCommandWithExecIfMatch(db, command, commandArgs, verifyResult, nxKey, isReadOnly);

                // Check running the command with EXECIFNOTMATCH meta-command
                await CheckCommandWithExecIfNotMatch(db, command, commandArgs, verifyResult, nxKey, isReadOnly);

                // Check running the command with EXECIFGREATER meta-command
                await CheckCommandWithExecIfGreater(db, command, commandArgs, verifyResult, nxKey, isReadOnly);
            }
            else
            {
                await CheckCommandWithUnsupportedMetaCommandAsync(db, command, commandArgs);
            }
        }

        private async Task CheckCommandWithoutMetaCommand(IDatabase db, RespCommand command, object[] commandArgs,
            Action<RedisResult> verifyResult, int[] checkKeysWithEtag, bool isReadOnly)
        {
            CurrMetaCommand = RespMetaCommand.None;
            CommandExecuted = true;

            // Run the command and verify the result
            var result = await db.ExecuteAsync(command.ToString(), commandArgs);
            verifyResult(result);

            // Verify ETags post-command
            foreach (var i in checkKeysWithEtag)
            {
                // Get the current etag
                var etag = (long)await db.ExecuteAsync("GETETAG", KeysWithEtag[i]);

                // Verify expected ETag -
                // 1. If command overwrites / deletes the value - ETag should be 0 
                // 2. If command only changes the metadata of the record or is a read-only command - ETag should remain 1
                // 3. Otherwise, ETag should advance to 2
                var expectedEtag = OverwriteCommands.Contains(command) || DeleteCommands.Contains(command) ? 0 : isReadOnly || MetadataCommands.Contains(command) ? 1 : 2;
                ClassicAssert.AreEqual(expectedEtag, etag);
            }
        }

        private async Task CheckCommandWithExecWithEtag(IDatabase db, RespCommand command, object[] commandArgs,
            Action<RedisResult> verifyResult, bool nxKey, bool isReadOnly)
        {
            CurrMetaCommand = RespMetaCommand.ExecWithEtag;
            CommandExecuted = true;

            // Reset the data
            DataSetUp(nxKey);

            // Add the EXECWITHETAG meta-command
            var result = await db.ExecWithEtagAsync(command.ToString(), commandArgs);

            // Verify result & expected ETag
            var expectedEtag = DeleteCommands.Contains(command) || isReadOnly || nxKey || MetadataCommands.Contains(command) ? 1 : 2;
            VerifyResultAndETag(result, verifyResult, expectedEtag);
        }

        private async Task CheckCommandWithExecIfMatch(IDatabase db, RespCommand command, object[] commandArgs,
            Action<RedisResult> verifyResult, bool nxKey, bool isReadOnly)
        {
            CurrMetaCommand = RespMetaCommand.ExecIfMatch;
            CommandExecuted = true;

            // Reset the data
            DataSetUp(nxKey);

            // Add the EXECIFMATCH meta-command with existing ETag (should succeed)
            var result = await db.ExecIfMatchAsync(nxKey ? 0 : 1, command.ToString(), commandArgs);

            // Verify result & expected ETag
            var expectedEtag = DeleteCommands.Contains(command) || nxKey || isReadOnly || MetadataCommands.Contains(command) ? 1 : 2;
            VerifyResultAndETag(result, verifyResult, expectedEtag);

            // Reset the data
            DataSetUp(nxKey);

            // Add the EXECIFMATCH meta-command with ETag 3 (should fail only if key exists)
            result = await db.ExecIfMatchAsync(3, command.ToString(), commandArgs);
            CommandExecuted = nxKey;

            if (!nxKey)
            {
                if (SetCommands.Contains(command))
                    VerifyResultAndETag(result, verifyResult, 1);
                else
                {
                    // Verify null result & expected ETag unchanged
                    VerifyNullResultAndETag(result, 1);
                }
            }
            else
            {
                VerifyResultAndETag(result, verifyResult, 4);
            }
        }

        private async Task CheckCommandWithExecIfNotMatch(IDatabase db, RespCommand command, object[] commandArgs,
            Action<RedisResult> verifyResult, bool nxKey, bool isReadOnly)
        {
            CurrMetaCommand = RespMetaCommand.ExecIfNotMatch;
            CommandExecuted = true;

            // Reset the data
            DataSetUp(nxKey);

            // Add the EXECIFMATCH meta-command with different ETag (should succeed)
            var result = await db.ExecIfNotMatchAsync(2, command.ToString(), commandArgs);

            // Verify result & expected ETag
            var expectedEtag = DeleteCommands.Contains(command) || nxKey || isReadOnly || MetadataCommands.Contains(command) ? 1 : 2;
            VerifyResultAndETag(result, verifyResult, expectedEtag);

            // Reset the data
            DataSetUp(nxKey);

            // Add the EXECIFMATCH meta-command with current ETag (should succeed only if key exists)
            result = await db.ExecIfNotMatchAsync(nxKey ? 0 : 1, command.ToString(), commandArgs);
            CommandExecuted = nxKey;

            if (!nxKey)
            {
                if (SetCommands.Contains(command))
                    VerifyResultAndETag(result, verifyResult, 1);
                else
                {
                    // Verify null result & expected ETag unchanged
                    VerifyNullResultAndETag(result, 1);
                }
            }
            else
            {
                VerifyResultAndETag(result, verifyResult, 1);
            }
        }

        private async Task CheckCommandWithExecIfGreater(IDatabase db, RespCommand command, object[] commandArgs,
            Action<RedisResult> verifyResult, bool nxKey, bool isReadOnly)
        {
            CurrMetaCommand = RespMetaCommand.ExecIfGreater;
            CommandExecuted = true;

            // Reset the data
            DataSetUp(nxKey);

            // Add the EXECIFGREATER meta-command with ETag 3 (should succeed)
            var result = await db.ExecIfGreaterAsync(3, command.ToString(), commandArgs);

            // Verify result & expected ETag
            var expectedEtag = DeleteCommands.Contains(command) || isReadOnly || MetadataCommands.Contains(command) ? 1 : 3;
            VerifyResultAndETag(result, verifyResult, expectedEtag);

            // Reset the data
            DataSetUp(nxKey);

            // Add the EXECIFGREATER meta-command with ETag 1 (should fail only if key exists)
            result = await db.ExecIfGreaterAsync(1, command.ToString(), commandArgs);
            CommandExecuted = nxKey;

            if (!nxKey)
            {
                if (SetCommands.Contains(command))
                    VerifyResultAndETag(result, verifyResult, 1);
                else
                {
                    // Verify null result & expected ETag unchanged
                    VerifyNullResultAndETag(result, 1);
                }
            }
            else
            {
                VerifyResultAndETag(result, verifyResult, 1);
            }
        }

        protected async Task CheckCommandWithUnsupportedMetaCommandAsync(IDatabase db, RespCommand command, object[] commandArgs)
        {
            // Add the EXECWITHETAG meta-command (error expected)
            var result = await db.ExecWithEtagAsync(command.ToString(), commandArgs);
            VerifyErrorResult(result, string.Format(CmdStrings.GenericErrCmdUnsupportedWithMetaCommand, command.ToString(), "EXECWITHETAG"));

            // Add the EXECIFMATCH meta-command (error expected)
            result = await db.ExecIfMatchAsync(0, command.ToString(), commandArgs);
            VerifyErrorResult(result, string.Format(CmdStrings.GenericErrCmdUnsupportedWithMetaCommand, command.ToString(), "EXECIFMATCH"));

            // Add the EXECIFNOTMATCH meta-command (error expected)
            result = await db.ExecIfNotMatchAsync(0, command.ToString(), commandArgs);
            VerifyErrorResult(result, string.Format(CmdStrings.GenericErrCmdUnsupportedWithMetaCommand, command.ToString(), "EXECIFNOTMATCH"));

            // Add the EXECIFGREATER meta-command (error expected)
            result = await db.ExecIfGreaterAsync(1, command.ToString(), commandArgs);
            VerifyErrorResult(result, string.Format(CmdStrings.GenericErrCmdUnsupportedWithMetaCommand, command.ToString(), "EXECIFGREATER"));
        }

        internal void VerifyResultAndETag(RedisResult result, Action<RedisResult> verifyResult, long expectedETag)
        {
            ClassicAssert.AreEqual(2, result!.Length);
            verifyResult(result[0]);
            ClassicAssert.AreEqual(expectedETag, (long)result[1]);
        }

        internal void VerifyNullResultAndETag(RedisResult result, long expectedETag)
            => VerifyResultAndETag(result, r => ClassicAssert.IsNull((string)r), expectedETag);

        internal void VerifyErrorResult(RedisResult result, string expectedError)
            => VerifyResultAndETag(result, r => ClassicAssert.AreEqual(expectedError, r.ToString()), -1);

        protected async Task CheckBlockingCommandAsync(RespCommand command, object[] commandArgs, Action<byte[]> verifyResult, int[] checkKeysWithEtag = null)
        {
            await using var redis = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            checkKeysWithEtag ??= [0];

            var blockingTask = Task.Run(() =>
            {
                using var lcr = TestUtils.CreateRequest();
                var btResponse = lcr.SendCommand($"{command} {string.Join(' ', commandArgs)}");
                verifyResult(btResponse);
            });

            var releasingTask = Task.Run(() =>
            {
                Task.Delay(TimeSpan.FromSeconds(2)).Wait();
                DataSetUp();
            });

            var timeout = TimeSpan.FromSeconds(5);
            try
            {
                Task.WaitAll([blockingTask, releasingTask], timeout);
            }
            catch (AggregateException)
            {
                Assert.Fail();
            }

            ClassicAssert.IsTrue(blockingTask.IsCompletedSuccessfully);
            ClassicAssert.IsTrue(releasingTask.IsCompletedSuccessfully);

            foreach (var i in checkKeysWithEtag)
            {
                var exists = await db.KeyExistsAsync(KeysWithEtag[i]);
                ClassicAssert.IsTrue(exists);

                var etag = (long)await db.ExecuteAsync("GETETAG", KeysWithEtag[i]);

                var expectedEtag = OverwriteCommands.Contains(command) || DeleteCommands.Contains(command) ? 0 : MetadataCommands.Contains(command) ? 1 : 2;
                ClassicAssert.AreEqual(expectedEtag, etag);
            }
        }
    }
}
