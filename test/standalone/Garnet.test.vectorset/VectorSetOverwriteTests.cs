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
    [TestFixture(false, false)]
    [TestFixture(true, false)]
    [TestFixture(false, true)]
    [TestFixture(true, true)]
    public class VectorSetOverwriteTests(bool RunInTransaction, bool EvictToDisk) : TestBase
    {
        private GarnetServer server;

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
            return TestVectorSetOverwrittenCommandAsync(RunCommandAsync);

            static async Task RunCommandAsync(IDatabaseAsync executeDB, IDatabaseAsync readDB, RedisKey againstKey)
            {
                var res = await executeDB.ExecuteAsync("MSET", [againstKey, "foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", (string)res);

                var finalValue = await readDB.StringGetAsync(againstKey).ConfigureAwait(false);
                ClassicAssert.AreEqual("foo", finalValue);
            }
        }

        [Test]
        public Task PSETEXAsync()
        {
            return TestVectorSetOverwrittenCommandAsync(RunCommandAsync);

            static async Task RunCommandAsync(IDatabaseAsync executeDB, IDatabaseAsync readDB, RedisKey againstKey)
            {
                var res = await executeDB.ExecuteAsync("PSETEX", [againstKey, "10000", "foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", (string)res);

                var finalValue = await readDB.StringGetAsync(againstKey).ConfigureAwait(false);
                ClassicAssert.AreEqual("foo", finalValue);
            }
        }

        [Test]
        public Task SETAsync()
        {
            return TestVectorSetOverwrittenCommandAsync(RunCommandAsync);

            static async Task RunCommandAsync(IDatabaseAsync executeDB, IDatabaseAsync readDB, RedisKey againstKey)
            {
                var res = await executeDB.StringSetAsync(againstKey, "foo").ConfigureAwait(false);
                ClassicAssert.IsTrue(res);

                var finalValue = await readDB.StringGetAsync(againstKey).ConfigureAwait(false);
                ClassicAssert.AreEqual("foo", finalValue);
            }
        }

        [Test]
        public Task SETEXAsync()
        {
            return TestVectorSetOverwrittenCommandAsync(RunCommandAsync);

            static async Task RunCommandAsync(IDatabaseAsync executeDB, IDatabaseAsync readDB, RedisKey againstKey)
            {
                var res = await executeDB.ExecuteAsync("SETEX", [againstKey, "10", "foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual("OK", (string)res);

                var finalValue = await readDB.StringGetAsync(againstKey).ConfigureAwait(false);
                ClassicAssert.AreEqual("foo", finalValue);
            }
        }

        [Test]
        public Task SETIFGREATERAsync()
        {
            return TestVectorSetOverwrittenCommandAsync(RunCommandAsync);

            static async Task RunCommandAsync(IDatabaseAsync executeDB, IDatabaseAsync readDB, RedisKey againstKey)
            {
                var res = (string[])await executeDB.ExecuteAsync("SETIFGREATER", [againstKey, "foo", "1234"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(2, res.Length);
                ClassicAssert.AreEqual("1234", res[0]);
                ClassicAssert.IsNull(res[1]);

                var finalValue = await readDB.StringGetAsync(againstKey).ConfigureAwait(false);
                ClassicAssert.AreEqual("foo", finalValue);
            }
        }

        [Test]
        public Task SETIFMATCHAsync()
        {
            // Vector Sets have no ETAG, so SETIFMATCH will succeed

            return TestVectorSetOverwrittenCommandAsync(RunCommandAsync);

            static async Task RunCommandAsync(IDatabaseAsync executeDB, IDatabaseAsync readDB, RedisKey againstKey)
            {
                var res = (string[])await executeDB.ExecuteAsync("SETIFMATCH", [againstKey, "foo", "1234"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(2, res.Length);
                ClassicAssert.AreEqual("1235", res[0]); // +1 since we're starting from empty
                ClassicAssert.IsNull(res[1]);

                var finalValue = await readDB.StringGetAsync(againstKey).ConfigureAwait(false);
                ClassicAssert.AreEqual("foo", finalValue);
            }
        }

        [Test]
        public Task SETWITHETAGAsync()
        {
            return TestVectorSetOverwrittenCommandAsync(RunCommandAsync);

            static async Task RunCommandAsync(IDatabaseAsync executeDB, IDatabaseAsync readDB, RedisKey againstKey)
            {
                var res = (string[])await executeDB.ExecuteAsync("SETWITHETAG", [againstKey, "foo"]).ConfigureAwait(false);
                ClassicAssert.AreEqual(1, res.Length);
                ClassicAssert.AreEqual("1", res[0]);

                var finalValue = await readDB.StringGetAsync(againstKey).ConfigureAwait(false);
                ClassicAssert.AreEqual("foo", finalValue);
            }
        }

        // Infrastructure

        private async Task TestVectorSetOverwrittenCommandAsync(Func<IDatabaseAsync, IDatabaseAsync, RedisKey, Task> runCommand)
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

            if (EvictToDisk)
            {
                var evictAndFlushRes = (string)await db.ExecuteAsync("DEBUG", "FLUSHANDEVICT").ConfigureAwait(false);
                ClassicAssert.IsTrue(evictAndFlushRes.StartsWith("OK "));
            }

            if (RunInTransaction)
            {
                var trans = db.CreateTransaction();
                var queuedTask = runCommand(trans, db, name);

                var transRes = await trans.ExecuteAsync().ConfigureAwait(false);
                ClassicAssert.IsTrue(transRes);

                await queuedTask.ConfigureAwait(false);
            }
            else
            {
                await runCommand(db, db, name).ConfigureAwait(false);
            }

            vectorManager.GetContextState(vectorSetContext, out var inUse, out var isCleaningUp, out _);

            ClassicAssert.IsTrue(!inUse || isCleaningUp, "Context should be free'd or in the process of being cleaned up");
        }
    }
}