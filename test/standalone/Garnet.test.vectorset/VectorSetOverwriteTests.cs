// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Tests that RESP commands that _overwrite_ Vector Sets correctly cause them to be cleaned up.
    /// </summary>
    [TestFixture]
    public class VectorSetOverwriteTests : TestBase
    {
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

        /// <summary>
        /// Create a new GarnetServer instance with common parameters.
        /// </summary>
        private static GarnetServer CreateGarnetServer(bool tryRecover)
        => TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, tryRecover: tryRecover, enableVectorSetPreview: true, enableRangeIndexPreview: true);

        [Test]
        public void AllOverwritingCommandsCovered()
        {
            var toCheck = VectorSetWrongTypeTests.GetOverwritingCommands();

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
        public Task MSETAsync()
        {
            return TestVectorSetOverwrittenCommandAsync(RunCommand);

            static async Task RunCommand(IDatabase db, RedisKey againstKey)
            {
                var res = await db.ExecuteAsync("MSET", [againstKey, "foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", (string)res);

                var finalValue = await db.StringGetAsync(againstKey).ConfigureAwait(false);
                ClassicAssert.AreEqual("foo", finalValue);
            }
        }

        [Test]
        public Task PSETEXAsync()
        {
            return TestVectorSetOverwrittenCommandAsync(RunCommand);

            static async Task RunCommand(IDatabase db, RedisKey againstKey)
            {
                var res = await db.ExecuteAsync("PSETEX", [againstKey, "10000", "foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", (string)res);

                var finalValue = await db.StringGetAsync(againstKey).ConfigureAwait(false);
                ClassicAssert.AreEqual("foo", finalValue);
            }
        }

        [Test]
        public Task SETEXAsync()
        {
            return TestVectorSetOverwrittenCommandAsync(RunCommand);

            static async Task RunCommand(IDatabase db, RedisKey againstKey)
            {
                var res = await db.ExecuteAsync("SETEX", [againstKey, "10", "foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", (string)res);

                var finalValue = await db.StringGetAsync(againstKey).ConfigureAwait(false);
                ClassicAssert.AreEqual("foo", finalValue);
            }
        }

        // Infrastructure

        private async Task TestVectorSetOverwrittenCommandAsync(Func<IDatabase, RedisKey, Task> runCommand)
        {
            var vectorManager = server.Provider.StoreWrapper.DefaultDatabase.VectorManager;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase();

            var name = $"VectorSet_{Guid.NewGuid()}";
            var nameBytes = Encoding.ASCII.GetBytes(name);

            var res = await db.VectorSetAddAsync(name, VectorSetAddRequest.Member("fizzbuzz", new[] { 1.0f, 2.0f, 3.0f }, "{\"foo\":\"bar\"}")).ConfigureAwait(false);
            ClassicAssert.IsTrue(res);

            ulong vectorSetContext;
            unsafe
            {
                fixed (byte* namePtr = nameBytes)
                {
                    vectorSetContext = vectorManager.GetNamespacesForKeys(server.Provider.StoreWrapper, [PinnedSpanByte.FromPinnedPointer(namePtr, nameBytes.Length)], []).Min();
                }
            }

            await runCommand(db, name);

            vectorManager.GetContextState(vectorSetContext, out var inUse, out var isCleaningUp, out _);

            ClassicAssert.IsTrue(!inUse || isCleaningUp, "Context should be free'd or in the process of being cleaned up");
        }
    }
}