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

        protected static readonly string KeyWithEtag = "keyWithEtag";
        protected HashSet<RespCommand> MultiKeyCommands = new();
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

                if (!cmd.IsDataCommand() || simpleRespCommandInfo.KeySpecs == null)
                    continue;

                if (simpleRespCommandInfo.IsMultiKeyCommand())
                    MultiKeyCommands.Add(cmd);

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

        protected async Task CheckCommandsAsync(RespCommand command, object[] commandArgs, Action<RedisResult> verifyResult)
        {
            await using var redis = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            DataSetUp();

            var etag = (long)await db.ExecuteAsync("GETETAG", KeyWithEtag);
            ClassicAssert.AreEqual(1, etag);

            var result = await db.ExecuteAsync(command.ToString(), commandArgs);
            verifyResult(result);

            var exists = await db.KeyExistsAsync(KeyWithEtag);
            ClassicAssert.IsTrue(exists);

            etag = (long)await db.ExecuteAsync("GETETAG", KeyWithEtag);

            var expectedEtag = OverwriteCommands.Contains(command) ? 0 : command.IsMetadataCommand() ? 1 : 2;
            ClassicAssert.AreEqual(expectedEtag, etag);

            // Multi-key commands do not support meta-commands yet
            if (!MultiKeyCommands.Contains(command))
            {
                DataSetUp();
                var args = new object[] { command.ToString() }.Union(commandArgs).ToArray();
                var results = await db.ExecuteAsync("EXECWITHETAG", args);
                ClassicAssert.AreEqual(2, results!.Length);
                verifyResult(results[0]);
                expectedEtag = command.IsMetadataCommand() ? 1 : 2;
                ClassicAssert.AreEqual(expectedEtag, (long)results[1]);
            }
        }
    }
}
