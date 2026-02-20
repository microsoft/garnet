// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.Resp.ETag
{
    [TestFixture]
    public abstract class EtagCoverageTestsBase
    {
        private GarnetServer server;

        protected static readonly string[] KeysWithEtag = ["keyWithEtag1", "keyWithEtag2", "keyWithEtag3", "keyWithEtag4"];
        protected HashSet<RespCommand> NoKeyDataCommands = new();
        protected HashSet<RespCommand> OverwriteCommands = new();
        
        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);

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
                    NoKeyDataCommands.Add(cmd);
                    continue;
                }

                if (simpleRespCommandInfo.IsOverwriteCommand() || cmd == RespCommand.SET)
                    OverwriteCommands.Add(cmd);
            }
        }

        public abstract void DataSetUp();

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        protected async Task CheckCommandAsync(RespCommand command, object[] commandArgs, Action<RedisResult> verifyResult, int[] checkKeysWithEtag = null)
        {
            await using var redis = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            DataSetUp();

            checkKeysWithEtag ??= [0];

            // Verify all keys set-up with ETags have an ETag of 1
            foreach (var i in checkKeysWithEtag)
            {
                var etag = (long)await db.ExecuteAsync("GETETAG", KeysWithEtag[i]);
                ClassicAssert.AreEqual(1, etag);
            }

            // Check running the command without a meta-command
            await CheckCommandWithoutMetaCommand(db, command, commandArgs, verifyResult, checkKeysWithEtag);

            // Check running the command with different meta-commands
            // Note: Multi-key commands do not support meta-commands yet
            if (!command.IsMultiKeyCommand())
            {
                // Check running the command with EXECWITHETAG meta-command
                await CheckCommandWithExecWithEtag(db, command, commandArgs, verifyResult);

                // Check running the command with EXECIFMATCH meta-command
                await CheckCommandWithExecIfMatch(db, command, commandArgs, verifyResult);

                // Check running the command with EXECIFGREATER meta-command
                await CheckCommandWithExecIfGreater(db, command, commandArgs, verifyResult);
            }
        }

        private async Task CheckCommandWithoutMetaCommand(IDatabase db, RespCommand command, object[] commandArgs,
            Action<RedisResult> verifyResult, int[] checkKeysWithEtag)
        {
            // Run the command and verify the result
            var result = await db.ExecuteAsync(command.ToString(), commandArgs);
            verifyResult(result);

            // Verify ETags post-command
            foreach (var i in checkKeysWithEtag)
            {
                // Verify that key exists first
                var exists = await db.KeyExistsAsync(KeysWithEtag[i]);
                ClassicAssert.IsTrue(exists);

                // Get the current etag
                var etag = (long)await db.ExecuteAsync("GETETAG", KeysWithEtag[i]);

                // Verify expected ETag -
                // 1. If command overwrites the value - ETag should be 0 
                // 2. If command only changes the metadata of the record - ETag should remain 1
                // 3. Otherwise, ETag should advance to 2
                var expectedEtag = OverwriteCommands.Contains(command) ? 0 : command.IsMetadataCommand() ? 1 : 2;
                ClassicAssert.AreEqual(expectedEtag, etag);
            }
        }

        private async Task CheckCommandWithExecWithEtag(IDatabase db, RespCommand command, object[] commandArgs,
            Action<RedisResult> verifyResult)
        {
            // Reset the data
            DataSetUp();

            // Add the EXECWITHETAG meta-command
            var args = new object[] { command.ToString() }.Concat(commandArgs).ToArray();
            var result = await db.ExecuteAsync("EXECWITHETAG", args);

            // Verify result & expected ETag
            var expectedEtag = command.IsMetadataCommand() ? 1 : 2;
            VerifyResultAndETag(result, verifyResult, expectedEtag);
        }

        private async Task CheckCommandWithExecIfMatch(IDatabase db, RespCommand command, object[] commandArgs,
            Action<RedisResult> verifyResult)
        {
            // Reset the data
            DataSetUp();

            // Add the EXECIFMATCH meta-command with ETag 1 (should succeed)
            var args = new object[] { 1, command.ToString() }.Concat(commandArgs).ToArray();
            var result = await db.ExecuteAsync("EXECIFMATCH", args);

            // Verify result & expected ETag
            var expectedEtag = command.IsMetadataCommand() ? 1 : 2;
            VerifyResultAndETag(result, verifyResult, expectedEtag);

            // Reset the data
            DataSetUp();

            // Add the EXECIFMATCH meta-command with ETag 3 (should fail)
            args = new object[] { 3, command.ToString() }.Concat(commandArgs).ToArray();
            result = await db.ExecuteAsync("EXECIFMATCH", args);

            // Verify null result & expected ETag unchanged
            VerifyNullResultAndETag(result, 1);
        }

        private async Task CheckCommandWithExecIfGreater(IDatabase db, RespCommand command, object[] commandArgs,
            Action<RedisResult> verifyResult)
        {
            // Reset the data
            DataSetUp();

            // Add the EXECIFGREATER meta-command with ETag 3 (should succeed)
            var args = new object[] { 3, command.ToString() }.Concat(commandArgs).ToArray();
            var result = await db.ExecuteAsync("EXECIFGREATER", args);

            // Verify result & expected ETag
            var expectedEtag = command.IsMetadataCommand() ? 1 : 2;
            VerifyResultAndETag(result, verifyResult, expectedEtag);

            // Reset the data
            DataSetUp();

            // Add the EXECIFGREATER meta-command with ETag 1 (should fail)
            args = new object[] { 1, command.ToString() }.Concat(commandArgs).ToArray();
            result = await db.ExecuteAsync("EXECIFGREATER", args);

            // Verify null result & expected ETag unchanged
            VerifyNullResultAndETag(result, 1);
        }

        private void VerifyResultAndETag(RedisResult result, Action<RedisResult> verifyResult, long expectedETag)
        {
            ClassicAssert.AreEqual(2, result!.Length);
            verifyResult(result[0]);
            ClassicAssert.AreEqual(expectedETag, (long)result[1]);
        }

        private void VerifyNullResultAndETag(RedisResult result, long expectedETag)
            => VerifyResultAndETag(result, r => ClassicAssert.IsNull((string)r), expectedETag);

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

                var expectedEtag = OverwriteCommands.Contains(command) ? 0 : command.IsMetadataCommand() ? 1 : 2;
                ClassicAssert.AreEqual(expectedEtag, etag);
            }
        }
    }
}
