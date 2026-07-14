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