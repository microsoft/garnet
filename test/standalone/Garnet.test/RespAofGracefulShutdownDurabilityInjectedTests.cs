// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
using System;
using System.Threading;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Forces the auto-commit (<c>CommitFrequencyMs = 0</c>) durability race deterministically: a SET is acked
    /// without waiting for its commit, its device page flush is dropped, and a graceful shutdown then loses it.
    /// DEBUG-only: relies on <see cref="ExceptionInjectionHelper"/> and the injected page-flush hook.
    /// </summary>
    [TestFixture]
    public class RespAofGracefulShutdownDurabilityInjectedTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// With auto-commit on, the SET's page flush is dropped so the bytes never reach the device and
        /// <c>CommittedUntilAddress</c> lags <c>TailAddress</c> - exactly the observed failure signature. A graceful
        /// shutdown in that window loses the acked write, proving auto-commit is not durable on graceful shutdown when
        /// the flush has not completed.
        /// </summary>
        [Test]
        public void AutoCommitUnflushedWriteLostOnGracefulShutdown()
        {
            const ExceptionInjectionType dropFlush = ExceptionInjectionType.Aof_AutoCommit_Drop_Page_Flush;
            using var flushDropped = new ManualResetEventSlim(false);

            TsavoriteLogAllocatorImpl.DropPageFlushTestHook = () =>
            {
                if (!ExceptionInjectionHelper.IsEnabled(dropFlush))
                    return false;

                flushDropped.Set();
                return true;
            };

            try
            {
                server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, commitFrequencyMs: 0);
                server.Start();

                ExceptionInjectionHelper.EnableException(dropFlush);

                using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
                {
                    var db = redis.GetDatabase(0);
                    ClassicAssert.IsTrue(db.StringSet("k", "v"), "SET should be acknowledged to the client");
                }

                ClassicAssert.IsTrue(flushDropped.Wait(TimeSpan.FromSeconds(10)),
                    "the auto-commit page flush should have been attempted (and dropped) before shutdown");

                server.Dispose(false);
                server = null;
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(dropFlush);
                TsavoriteLogAllocatorImpl.DropPageFlushTestHook = null;
            }

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, commitFrequencyMs: 0);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.IsFalse(db.KeyExists("k"),
                    "acked write must be LOST: its auto-commit flush had not reached the device when graceful shutdown occurred");
            }
        }
    }
}
#endif
