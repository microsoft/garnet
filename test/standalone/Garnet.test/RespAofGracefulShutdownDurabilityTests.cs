// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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
    /// Proves that a write which was acknowledged to the client but whose AOF commit has not become
    /// durable is LOST on a graceful shutdown. Graceful <c>Dispose</c> cancels the commit task and tears
    /// the log down without flushing, so recovery only sees data up to <c>CommittedUntilAddress</c>.
    /// </summary>
    [TestFixture]
    public class RespAofGracefulShutdownDurabilityTests : TestBase
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
        /// With a high commit frequency (auto-commit disabled and the periodic commit far from firing),
        /// a SET is acked but never committed. A graceful shutdown before the periodic commit loses it.
        /// </summary>
        [Test]
        public void HighCommitFrequencyLosesAckedWriteOnGracefulShutdown()
        {
            const int farOffCommitMs = 60 * 60 * 1000;

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, commitFrequencyMs: farOffCommitMs);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.IsTrue(db.StringSet("k", "v"), "SET should be acknowledged to the client");
            }

            server.Dispose(false);
            server = null;

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, commitFrequencyMs: farOffCommitMs);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.IsFalse(db.KeyExists("k"),
                    "acked write must be LOST: it was never committed and graceful shutdown does not flush the AOF");
            }
        }

#if DEBUG
        /// <summary>
        /// With auto-commit on (<c>CommitFrequencyMs = 0</c>), a SET is acked without waiting for its commit. The
        /// page flush that would make it durable is dropped (via <see cref="TsavoriteLogAllocatorImpl.DropPageFlushTestHook"/>
        /// gated by an <see cref="ExceptionInjectionHelper"/> flag), so the bytes never reach the device and
        /// <c>CommittedUntilAddress</c> lags <c>TailAddress</c> - exactly the observed failure signature. A graceful
        /// shutdown in that window loses the acked write, proving auto-commit is not durable on graceful shutdown when
        /// the flush has not completed. DEBUG-only: the injection flag is a no-op in Release.
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
#endif
    }
}
