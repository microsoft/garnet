// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [TestFixture]
    public class CooperativeDisposeGuardTests : TestBase
    {
        // CooperativeDisposeGuard is a mutable struct; concurrent code must operate on a single
        // shared addressable location. This holder gives the field stable storage that instance
        // method calls mutate in place across threads.
        private sealed class Holder
        {
            public CooperativeDisposeGuard Guard;
        }

        [Test]
        public void DefaultIsNotDisposed()
        {
            var guard = new CooperativeDisposeGuard();
            ClassicAssert.IsFalse(guard.IsDisposed);
        }

        [Test]
        public void TryDispose_WhenIdle_ReturnsCleanupNow()
        {
            var guard = new CooperativeDisposeGuard();

            ClassicAssert.AreEqual(CooperativeDisposeGuard.DisposeResult.CleanupNow, guard.TryDispose());
            ClassicAssert.IsTrue(guard.IsDisposed);
        }

        [Test]
        public void TryDispose_SecondCall_ReturnsAlreadyDisposed()
        {
            var guard = new CooperativeDisposeGuard();

            ClassicAssert.AreEqual(CooperativeDisposeGuard.DisposeResult.CleanupNow, guard.TryDispose());
            ClassicAssert.AreEqual(CooperativeDisposeGuard.DisposeResult.AlreadyDisposed, guard.TryDispose());
        }

        [Test]
        public void TryEnter_WhenNotDisposed_AllowsEntryAndExitReportsNoCleanup()
        {
            var guard = new CooperativeDisposeGuard();

            ClassicAssert.IsTrue(guard.TryEnter());
            // No dispose happened while inside the critical section.
            ClassicAssert.IsFalse(guard.ExitAndCheckShouldCleanup());
            ClassicAssert.IsFalse(guard.IsDisposed);
        }

        [Test]
        public void TryEnter_AfterDispose_ReturnsFalse()
        {
            var guard = new CooperativeDisposeGuard();

            _ = guard.TryDispose();
            ClassicAssert.IsFalse(guard.TryEnter());
        }

        [Test]
        public void TryDispose_WhileWorkerActive_DefersToWorker()
        {
            var guard = new CooperativeDisposeGuard();

            // Worker enters the critical section.
            ClassicAssert.IsTrue(guard.TryEnter());

            // Dispose happens while the worker is in-flight: cleanup is deferred to the worker.
            ClassicAssert.AreEqual(CooperativeDisposeGuard.DisposeResult.DeferredToWorker, guard.TryDispose());
            ClassicAssert.IsTrue(guard.IsDisposed);

            // Worker exit observes the disposal and takes responsibility for cleanup.
            ClassicAssert.IsTrue(guard.ExitAndCheckShouldCleanup());
        }

        [Test]
        public void DisposeBeforeAndAfterWorker_OnlyFirstCleansUp()
        {
            var guard = new CooperativeDisposeGuard();

            ClassicAssert.IsTrue(guard.TryEnter());
            ClassicAssert.AreEqual(CooperativeDisposeGuard.DisposeResult.DeferredToWorker, guard.TryDispose());
            ClassicAssert.IsTrue(guard.ExitAndCheckShouldCleanup());

            // A redundant dispose after the worker already cleaned up is a no-op.
            ClassicAssert.AreEqual(CooperativeDisposeGuard.DisposeResult.AlreadyDisposed, guard.TryDispose());
        }

        [Test]
        public void ConcurrentDisposeVsWorker_CleansUpExactlyOnce()
        {
            // Race a single disposer against a worker that repeatedly enters/exits the critical
            // section. Regardless of interleaving, exactly one party (the disposer via CleanupNow,
            // or the worker via ExitAndCheckShouldCleanup) must perform cleanup — never zero, never two.
            const int Trials = 5000;

            for (var trial = 0; trial < Trials; trial++)
            {
                var holder = new Holder();
                var cleanupCount = 0;
                using var ready = new ManualResetEventSlim(false);

                var worker = new Thread(() =>
                {
                    ready.Set();
                    while (true)
                    {
                        if (!holder.Guard.TryEnter())
                            break; // disposed — per contract, do not call Exit

                        try
                        {
                            // Tiny critical-section body to widen the race window.
                            Thread.SpinWait(1);
                        }
                        finally
                        {
                            if (holder.Guard.ExitAndCheckShouldCleanup())
                                Interlocked.Increment(ref cleanupCount);
                        }
                    }
                });

                worker.Start();
                ready.Wait();

                if (holder.Guard.TryDispose() == CooperativeDisposeGuard.DisposeResult.CleanupNow)
                    Interlocked.Increment(ref cleanupCount);

                worker.Join();

                ClassicAssert.AreEqual(1, cleanupCount, $"Trial {trial}: cleanup must run exactly once");
                ClassicAssert.IsTrue(holder.Guard.IsDisposed);
            }
        }
    }
}
