// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers a <c>CONFIG SET</c> of a background-task frequency, which stops the running instance of the task before
    /// starting one with the new frequency. The old instance is held mid-scan, so the stop waits for it.
    /// </summary>
    /// <remarks>
    /// The waiting <c>CONFIG SET</c> parks its session and holds no thread, yet still replies only once the old instance
    /// has stopped. Lifecycle changes that arrive meanwhile run after it, so no new instance starts while the old one is
    /// stopping: the instances share one storage session.
    /// </remarks>
    [TestFixture]
    [NonParallelizable]
    public class ConfigSetTaskReconcileTests : TestBase
    {
        const string Ok = "+OK\r\n";
        const string Pong = "+PONG\r\n";
        const string Option = "expired-key-deletion-scan-freq";
        const ExceptionInjectionType Hold = ExceptionInjectionType.ExpiredKeyDeletion_Hold_Scan;

        static readonly TimeSpan StallLimit = TimeSpan.FromSeconds(20);

        GarnetServer server;

        TaskManager Tasks => server.Provider.StoreWrapper.TaskManager;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, expiredKeyDeletionScanFrequencySecs: 1);

            // The first scan cycle runs as the server starts, so it is held from the outset
            ExceptionInjectionHelper.EnableException(Hold);
            server.Start();
            ClassicAssert.AreEqual(1, ExceptionInjectionHelper.WaitingOnClear(Hold), "The first scan was not held.");
        }

        [TearDown]
        public void TearDown()
        {
            // Released before disposing, which waits for the held scan to stop
            ExceptionInjectionHelper.DisableException(Hold);
            server?.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// Waits until a <c>CONFIG SET</c> has begun stopping the held scan, which it does by unregistering it.
        /// </summary>
        void WaitUntilStopping()
        {
            var sw = Stopwatch.StartNew();
            while (Tasks.IsRegistered(TaskType.ExpiredKeyDeletionTask))
            {
                if (sw.Elapsed > StallLimit)
                    Assert.Fail("CONFIG SET did not begin stopping the held scan.");
                Thread.Sleep(1);
            }
        }

        static string ReadReplyWithin(RawRespSession session, string what)
        {
            ClassicAssert.IsTrue(session.WaitForReply(StallLimit), $"{what} was not answered within {StallLimit.TotalSeconds:F0}s.");
            return session.ReadReply();
        }

        /// <summary>
        /// The pool is cut to a single free worker, which a <c>CONFIG SET</c> that waited in place for the scan would
        /// hold, leaving the other session no thread. It still replies only once the scan has stopped.
        /// </summary>
        [Test]
        public void AWaitingConfigSetDoesNotStallOtherSessions()
        {
            const int PingCount = 20;

            using var setter = new RawRespSession();
            using var pinger = new RawRespSession();
            ClassicAssert.AreEqual(Pong, setter.Execute("PING"));
            ClassicAssert.AreEqual(Pong, pinger.Execute("PING"));

            using (ThreadPoolCap.ApplyFree(1))
            {
                setter.Send("CONFIG", "SET", Option, "2");
                WaitUntilStopping();

                for (var i = 0; i < PingCount; i++)
                {
                    pinger.Send("PING");
                    ClassicAssert.AreEqual(Pong, ReadReplyWithin(pinger, $"PING {i}, sent while a CONFIG SET waited on a pool of one thread,"));
                }

                ClassicAssert.IsFalse(setter.WaitForReply(TimeSpan.FromMilliseconds(500)), "CONFIG SET replied before the scan it stops had stopped.");

                ExceptionInjectionHelper.DisableException(Hold);
                ClassicAssert.AreEqual(Ok, ReadReplyWithin(setter, "CONFIG SET"));
            }

            ClassicAssert.IsTrue(Tasks.IsRegistered(TaskType.ExpiredKeyDeletionTask), "The scan was not restarted.");
        }

        /// <summary>
        /// A second <c>CONFIG SET</c> of the same option waits behind the first, so it cannot start an instance while the
        /// first is still stopping the old one.
        /// </summary>
        [Test]
        public void ConfigSetsOfATaskRunOneAtATime()
        {
            using var first = new RawRespSession();
            using var second = new RawRespSession();
            using var observer = new RawRespSession();

            first.Send("CONFIG", "SET", Option, "2");
            WaitUntilStopping();

            second.Send("CONFIG", "SET", Option, "3");
            var sw = Stopwatch.StartNew();
            while (!observer.Execute("CONFIG", "GET", Option).EndsWith("\r\n3\r\n", StringComparison.Ordinal))
            {
                if (sw.Elapsed > StallLimit)
                    Assert.Fail("The second CONFIG SET was not applied.");
            }

            ClassicAssert.IsFalse(second.WaitForReply(TimeSpan.FromSeconds(1)), "The second CONFIG SET finished while the first was still stopping the scan.");
            ClassicAssert.AreEqual(1, ExceptionInjectionHelper.WaitingOnClear(Hold), "A scan started while the old one was still stopping.");

            ExceptionInjectionHelper.DisableException(Hold);
            ClassicAssert.AreEqual(Ok, ReadReplyWithin(first, "The first CONFIG SET"));
            ClassicAssert.AreEqual(Ok, ReadReplyWithin(second, "The second CONFIG SET"));
            ClassicAssert.IsTrue(Tasks.IsRegistered(TaskType.ExpiredKeyDeletionTask), "The scan was not restarted.");
        }

        /// <summary>
        /// Starting the primary tasks, as a failover does, waits behind a <c>CONFIG SET</c> that is still stopping one of
        /// them, so it cannot start an instance alongside the one stopping.
        /// </summary>
        [Test]
        public void StartingPrimaryTasksWaitsForAPendingConfigSet()
        {
            using var setter = new RawRespSession();

            setter.Send("CONFIG", "SET", Option, "2");
            WaitUntilStopping();

            // Run off the test thread: a start that waited in place, holding the lifecycle lock, would hang the test
            var started = Task.Run(server.Provider.StoreWrapper.StartPrimaryTasksAsync);
            ClassicAssert.IsFalse(started.Wait(TimeSpan.FromSeconds(1)), "The primary tasks started while a CONFIG SET was still stopping one of them.");
            ClassicAssert.AreEqual(1, ExceptionInjectionHelper.WaitingOnClear(Hold), "A scan started while the old one was still stopping.");

            ExceptionInjectionHelper.DisableException(Hold);
            ClassicAssert.AreEqual(Ok, ReadReplyWithin(setter, "CONFIG SET"));
            ClassicAssert.IsTrue(started.Wait(StallLimit), "The primary tasks were not started once the CONFIG SET finished.");
            ClassicAssert.IsTrue(Tasks.IsRegistered(TaskType.ExpiredKeyDeletionTask), "The scan was not restarted.");
        }
    }
}
#endif