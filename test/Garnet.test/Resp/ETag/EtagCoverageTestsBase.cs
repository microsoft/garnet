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

        protected static readonly string[] KeysWithEtag = ["keyWithEtag1", "keyWithEtag2"];
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

                if (simpleRespCommandInfo.IsOverwriteCommand())
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

            foreach (var i in checkKeysWithEtag)
            {
                var etag = (long)await db.ExecuteAsync("GETETAG", KeysWithEtag[i]);
                ClassicAssert.AreEqual(1, etag);
            }

            var result = await db.ExecuteAsync(command.ToString(), commandArgs);
            verifyResult(result);

            foreach (var i in checkKeysWithEtag)
            {
                var exists = await db.KeyExistsAsync(KeysWithEtag[i]);
                ClassicAssert.IsTrue(exists);

                var etag = (long)await db.ExecuteAsync("GETETAG", KeysWithEtag[i]);

                var expectedEtag = OverwriteCommands.Contains(command) ? 0 : command.IsMetadataCommand() ? 1 : 2;
                ClassicAssert.AreEqual(expectedEtag, etag);
            }

            // Multi-key commands do not support meta-commands yet
            if (!command.IsMultiKeyCommand())
            {
                DataSetUp();
                var args = new object[] { command.ToString() }.Union(commandArgs).ToArray();
                var results = await db.ExecuteAsync("EXECWITHETAG", args);
                ClassicAssert.AreEqual(2, results!.Length);
                verifyResult(results[0]);
                var expectedEtag = command.IsMetadataCommand() ? 1 : 2;
                ClassicAssert.AreEqual(expectedEtag, (long)results[1]);
            }
        }

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
