// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Tests that a transaction ends when an exception interrupts a command EXEC runs: its keys are unlocked, it no longer
    /// counts as active, which a checkpoint or index grow waits for, and the AOF records it as committed.
    /// </summary>
    [TestFixture]
    public class TransactionExceptionTests : TestBase
    {
        /// <summary>
        /// How long an operation that should not wait on another one is given before it is judged blocked.
        /// </summary>
        private static readonly TimeSpan NotBlockedTimeout = TimeSpan.FromSeconds(10);

        private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(30);

        private GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Transaction_Fail_During_Exec);
            ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Transaction_Throw_During_Exec);
            ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Transaction_Fail_Locking_Keys);
            ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Transaction_Throw_After_Command);

            server?.Dispose();
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// Where <see cref="ExceptionThatEndsSessionEndsTransactionAsync"/> raises its exception.
        /// </summary>
        public enum TransactionFault
        {
            /// <summary>A <see cref="GarnetException"/> from a command EXEC runs.</summary>
            GarnetExceptionInCommand,

            /// <summary>An exception that is not a <see cref="GarnetException"/> from a command EXEC runs.</summary>
            OtherExceptionInCommand,

            /// <summary>A <see cref="GarnetException"/> while EXEC locks the transaction's keys.</summary>
            GarnetExceptionLockingKeys,
        }

        /// <summary>
        /// An exception that ends the session while EXEC runs the transaction must still end the transaction.
        /// </summary>
        [Test]
        public async Task ExceptionThatEndsSessionEndsTransactionAsync([Values] TransactionFault fault)
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            var injection = fault switch
            {
                TransactionFault.GarnetExceptionInCommand => ExceptionInjectionType.Transaction_Fail_During_Exec,
                TransactionFault.OtherExceptionInCommand => ExceptionInjectionType.Transaction_Throw_During_Exec,
                _ => ExceptionInjectionType.Transaction_Fail_Locking_Keys,
            };

            StartServer();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            using var redisOther = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var dbOther = redisOther.GetDatabase(0);

            ExceptionInjectionHelper.EnableException(injection);
            try
            {
                var transaction = db.CreateTransaction();
                _ = transaction.StringSetAsync("key1", "value1");
                _ = transaction.StringSetAsync("key2", "value2");
                _ = Assert.CatchAsync<RedisException>(() => transaction.ExecuteAsync().WaitAsync(WaitTimeout));
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(injection);
            }

            AssertTransactionEnded("key1", "key2");

            // A checkpoint waits for the transactions of the version it ends.
            ClassicAssert.AreEqual("OK", (string)await dbOther.ExecuteAsync("SAVE").WaitAsync(WaitTimeout));

            ClassicAssert.IsTrue(await dbOther.StringSetAsync("key1", "other").WaitAsync(WaitTimeout));
            ClassicAssert.AreEqual("other", (string)await dbOther.StringGetAsync("key1").WaitAsync(WaitTimeout));
        }

        /// <summary>
        /// A <see cref="GarnetException"/> that leaves the session usable fails only the command that threw it: EXEC replies
        /// with that command's error among the other results, runs the rest of the transaction, and ends it.
        /// </summary>
        [Test]
        public async Task ExceptionThatKeepsSessionFailsOnlyItsCommandAsync([Values] bool failingCommandLast)
        {
            // Object commands throw such an exception when the object store is disabled.
            StartServer(disableObjects: true);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var transaction = db.CreateTransaction();
            var set1 = transaction.StringSetAsync("key1", "value1");
            var hashSet = failingCommandLast ? null : transaction.HashSetAsync("hash", "field", "value");
            var set2 = transaction.StringSetAsync("key2", "value2");
            hashSet ??= transaction.HashSetAsync("hash", "field", "value");
            var exec = transaction.ExecuteAsync();

            var execCompleted = await CompletesWithinAsync(exec, NotBlockedTimeout);
            var transactionEnded = execCompleted && TransactionEnded("key1", "key2", "hash");

            if (!transactionEnded)
            {
                // The rest of the transaction is still in the session's receive buffer, and the next request on the
                // connection runs it and commits it. Send one so that the transaction does not outlive the test.
                _ = await db.PingAsync().WaitAsync(WaitTimeout);
                _ = await exec.WaitAsync(WaitTimeout);

                Assert.Fail(execCompleted
                    ? "EXEC replied, but the transaction kept its keys locked until the next request on the connection"
                    : "EXEC did not reply until the next request on the connection, and kept the transaction's keys locked meanwhile");
            }

            ClassicAssert.IsTrue(await exec, "EXEC was aborted");
            ClassicAssert.IsTrue(await set1);
            ClassicAssert.IsTrue(await set2);
            var error = Assert.ThrowsAsync<RedisServerException>(() => hashSet);
            StringAssert.Contains("Object store is disabled", error.Message);

            // The session is outside the transaction and usable.
            ClassicAssert.AreEqual("value1", (string)await db.StringGetAsync("key1").WaitAsync(WaitTimeout));
            ClassicAssert.AreEqual("value2", (string)await db.StringGetAsync("key2").WaitAsync(WaitTimeout));

            var next = db.CreateTransaction();
            var nextSet = next.StringSetAsync("key1", "value3");
            ClassicAssert.IsTrue(await next.ExecuteAsync().WaitAsync(WaitTimeout));
            ClassicAssert.IsTrue(await nextSet);
            ClassicAssert.AreEqual("value3", (string)await db.StringGetAsync("key1").WaitAsync(WaitTimeout));
            AssertTransactionEnded("key1");
        }

        /// <summary>
        /// A transaction that an exception interrupts once some of its commands have applied is committed in the AOF too, so
        /// that recovering from the AOF applies exactly what the transaction applied.
        /// </summary>
        [Test]
        public async Task InterruptedTransactionIsRecoveredFromAofAsync()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // The transaction applies its first command, and an exception then ends it and its session.
                ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Transaction_Throw_After_Command);
                try
                {
                    var transaction = db.CreateTransaction();
                    _ = transaction.StringSetAsync("key1", "value1");
                    _ = transaction.StringSetAsync("key2", "value2");
                    _ = Assert.CatchAsync<RedisException>(() => transaction.ExecuteAsync().WaitAsync(WaitTimeout));
                }
                finally
                {
                    ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Transaction_Throw_After_Command);
                }
            }

            AssertTransactionEnded("key1", "key2");

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual("value1", (string)await db.StringGetAsync("key1").WaitAsync(WaitTimeout));
                ClassicAssert.IsNull((string)await db.StringGetAsync("key2").WaitAsync(WaitTimeout));

                var next = db.CreateTransaction();
                _ = next.StringSetAsync("key3", "value3");
                ClassicAssert.IsTrue(await next.ExecuteAsync().WaitAsync(WaitTimeout));
            }

            _ = await server.Store.CommitAOFAsync(default);
            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, failOnRecoveryError: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual("value1", (string)await db.StringGetAsync("key1").WaitAsync(WaitTimeout),
                    "Recovering from the AOF did not apply what the interrupted transaction applied");
                ClassicAssert.IsNull((string)await db.StringGetAsync("key2").WaitAsync(WaitTimeout),
                    "Recovering from the AOF applied a command the interrupted transaction did not run");
                ClassicAssert.AreEqual("value3", (string)await db.StringGetAsync("key3").WaitAsync(WaitTimeout));
            }
        }

        private void StartServer(bool disableObjects = false)
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disableObjects: disableObjects);
            server.Start();
        }

        /// <summary>
        /// True if no transaction is active and none of <paramref name="keys"/> is locked.
        /// </summary>
        private bool TransactionEnded(params string[] keys)
        {
            if (TestUtils.GetActiveTransactionCount(server.Provider.StoreWrapper.DefaultDatabase.StateMachineDriver) != 0)
                return false;

            foreach (var key in keys)
            {
                if (TestUtils.IsKeyBucketLocked(server.Provider.StoreWrapper.store, key))
                    return false;
            }

            return true;
        }

        private void AssertTransactionEnded(params string[] keys)
        {
            // The session ends the transaction before it replies or closes the connection; allow for a slow server thread.
            ClassicAssert.IsTrue(SpinWait.SpinUntil(() => TransactionEnded(keys), NotBlockedTimeout),
                "The interrupted transaction is still active, or still holds the locks on its keys");
        }

        private static async Task<bool> CompletesWithinAsync(Task task, TimeSpan timeout)
            => await Task.WhenAny(task, Task.Delay(timeout)) == task;
    }
}