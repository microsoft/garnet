// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test
{
    /// <summary>
    /// Pins down <see cref="CompletionEvent"/>'s signalling contract, which is easy to misuse.
    /// <see cref="CompletionEvent.Set"/> retires the current semaphore generation and installs a fresh,
    /// unsignaled one, so a signal is only observed by a waiter holding a copy of the generation that was
    /// current when the signal was raised. Callers must therefore copy the struct before doing the work that
    /// may race with a signal, and copy it afresh on each pass; every <c>flushEvent</c> caller in the allocator
    /// and in TsavoriteLog does exactly that.
    /// </summary>
    [TestFixture]
    internal class CompletionEventTests : TestBase
    {
        static readonly TimeSpan ShortWait = TimeSpan.FromMilliseconds(250);
        static readonly TimeSpan LongWait = TimeSpan.FromSeconds(10);

        #region Capture discipline

        [Test]
        [Category("Smoke")]
        public void NoCapture_SignalBeforeWait_IsLost()
        {
            CompletionEvent evt = default;
            evt.Initialize();
            try
            {
                // Re-reading the field at wait time: the signal retired the generation we would have waited on, and we
                // pick up the fresh one instead.
                evt.Set();
                var signaled = evt.Wait(ShortWait);
                Assert.That(signaled, Is.False, "expected the signal to be lost when the field is re-read at wait time");
            }
            finally { evt.Dispose(); }
        }

        [Test]
        [Category("Smoke")]
        public void CaptureBeforeSignal_SignalBeforeWait_IsPreserved()
        {
            CompletionEvent evt = default;
            evt.Initialize();
            try
            {
                // Copy the struct (capturing the current generation) BEFORE the work that may race with Set, then wait
                // on the copy.
                var captured = evt;
                evt.Set();
                var signaled = captured.Wait(ShortWait);
                Assert.That(signaled, Is.True, "a capture taken before the signal must observe it");
            }
            finally { evt.Dispose(); }
        }

        [Test]
        [Category("Smoke")]
        public void CaptureBeforeSignal_SignalFromAnotherThread_IsPreserved()
        {
            CompletionEvent evt = default;
            evt.Initialize();
            try
            {
                var captured = evt;
                var t = new Thread(() => { Thread.Sleep(50); evt.Set(); });
                t.Start();
                var signaled = captured.Wait(LongWait);
                t.Join();
                Assert.That(signaled, Is.True, "a capture must observe a signal that arrives after it, from any thread");
            }
            finally { evt.Dispose(); }
        }

        [Test]
        [Category("Smoke")]
        public void StaleCapture_ReturnsImmediatelyForever()
        {
            CompletionEvent evt = default;
            evt.Initialize();
            try
            {
                // Why the capture must be refreshed each pass -- though not for the reason one might expect. Set()
                // releases int.MaxValue permits on the generation it retires, so a capture that has been signaled once
                // satisfies every later wait immediately. A caller that keeps waiting on a consumed capture does not
                // miss signals; it stops blocking at all and spins.
                var captured = evt;
                evt.Set();
                Assert.That(captured.Wait(ShortWait), Is.True, "first signal is observed by the capture");

                for (var i = 0; i < 5; i++)
                    Assert.That(captured.Wait(ShortWait), Is.True, $"consumed capture still returns immediately (iteration {i})");

                // Meanwhile the live field is a fresh generation with no permits.
                Assert.That(evt.Wait(ShortWait), Is.False, "the current generation is unsignaled");
            }
            finally { evt.Dispose(); }
        }

        #endregion

        #region Disposal

        /// <summary>
        /// Disposal used to dispose the semaphore and null the field, which stranded anyone already parked
        /// (<see cref="SemaphoreSlim.Dispose()"/> does not pulse), threw <see cref="ObjectDisposedException"/> from a
        /// capture taken beforehand -- including from inside an operation with its epoch suspended -- and turned a
        /// wait on the live field into a <see cref="NullReferenceException"/>. It now permanently signals instead.
        /// </summary>
        [Test]
        [Category("Smoke")]
        public void Dispose_ReleasesParkedWaitersAndNeverThrows()
        {
            CompletionEvent evt = default;
            evt.Initialize();

            var captured = evt;
            var asyncWait = captured.WaitAsync();

            var woke = false;
            var t = new Thread(() => { captured.Wait(); woke = true; });
            t.Start();
            Thread.Sleep(50);

            evt.Dispose();

            Assert.That(t.Join(LongWait), Is.True, "Dispose must wake a parked blocking waiter rather than strand it");
            Assert.That(woke, Is.True);
            Assert.That(asyncWait.Wait(LongWait), Is.True, "Dispose must complete the pending async wait");

            // Every post-dispose member reports signaled rather than throwing.
            Assert.That(captured.Wait(TimeSpan.Zero), Is.True, "a capture taken before disposal falls through");
            Assert.That(evt.Wait(TimeSpan.Zero), Is.True, "a wait on the live field falls through");
            Assert.That(evt.WaitAsync().Wait(LongWait), Is.True, "an async wait after disposal falls through");

            var afterDispose = evt;
            Assert.That(afterDispose.Wait(TimeSpan.Zero), Is.True, "a capture taken after disposal falls through");

            // Signalling after disposal must stay a no-op: installing a fresh generation would let a later waiter
            // block on something nothing will signal, and re-releasing the tombstone would throw SemaphoreFullException.
            for (var i = 0; i < 100; i++)
                evt.Set();
            Assert.That(evt.Wait(TimeSpan.Zero), Is.True, "Set after disposal must not un-signal the event");

            evt.Dispose();  // idempotent
        }

        [Test]
        [Category("Smoke")]
        public void Dispose_ReleasesEveryWaiter()
        {
            CompletionEvent evt = default;
            evt.Initialize();
            const int Waiters = 8;

            var started = new CountdownEvent(Waiters);
            var finished = new CountdownEvent(Waiters);
            var threads = new List<Thread>();
            var asyncWaits = new List<Task>();

            for (var i = 0; i < Waiters; i++)
            {
                var captured = evt;
                var t = new Thread(() =>
                {
                    _ = started.Signal();
                    captured.Wait();
                    _ = finished.Signal();
                });
                t.Start();
                threads.Add(t);
                asyncWaits.Add(captured.WaitAsync());
            }

            Assert.That(started.Wait(LongWait), Is.True);
            Thread.Sleep(50);
            evt.Dispose();

            Assert.That(finished.Wait(LongWait), Is.True, "Dispose must release every parked blocking waiter");
            Assert.That(Task.WhenAll(asyncWaits).Wait(LongWait), Is.True, "Dispose must release every async waiter");
            foreach (var t in threads)
                t.Join();
        }

        #endregion

        #region Reset

        /// <summary>
        /// <c>AllocatorBase.ResetCore</c> used to call <c>Initialize()</c> here, which overwrites the field without
        /// releasing the outgoing semaphore, so anyone parked on it was never woken -- every later Set() signals the
        /// new instance. It now calls Set(), which retires the generation and releases its waiters.
        /// </summary>
        [Test]
        [Category("Smoke")]
        public void Set_WakesWaitersWhereInitializeWouldStrandThem()
        {
            CompletionEvent evt = default;
            evt.Initialize();
            try
            {
                var captured = evt;
                var asyncWait = captured.WaitAsync();

                var woke = false;
                var t = new Thread(() => { captured.Wait(); woke = true; });
                t.Start();
                Thread.Sleep(50);

                evt.Set();

                Assert.That(t.Join(LongWait), Is.True, "Set must wake a waiter parked on the retired generation");
                Assert.That(woke, Is.True);
                Assert.That(asyncWait.Wait(LongWait), Is.True, "Set must wake an async waiter too");
                Assert.That(evt.Wait(ShortWait), Is.False, "the generation installed by Set is unsignaled");
            }
            finally { evt.Dispose(); }
        }

        #endregion

        #region Coalesced-signal protocol

        /// <summary>
        /// Pins down the ordering <c>LogSizeTracker</c>'s signal coalescing depends on: the consumer must capture
        /// BEFORE it clears the pending flag. Capturing after the clear lets a producer that slips into the window
        /// have its signal consumed by the capture rather than by the wait, while the flag stays latched so every
        /// later producer coalesces itself away -- leaving the consumer parked with work outstanding, for the full
        /// <see cref="LogSizeTracker.ResizeTaskDelaySeconds"/>.
        /// </summary>
        [Test]
        [Category("Smoke")]
        public void CoalescedSignal_CaptureBeforeClear_NeverParksWithWorkOutstanding(
                [Values(true, false)] bool captureBeforeClear)
        {
            CompletionEvent evt = default;
            evt.Initialize();
            try
            {
                const int Producers = 4;
                var produced = 0L;
                var consumed = 0L;
                var pending = 0;
                var stop = false;
                var stalls = 0;

                // The consumer models ResizerTask: (capture, clear) or (clear, capture), then sample, then wait.
                var consumer = new Thread(() =>
                {
                    while (!Volatile.Read(ref stop))
                    {
                        CompletionEvent token;
                        if (captureBeforeClear)
                        {
                            token = evt;
                            _ = Interlocked.Exchange(ref pending, 0);
                        }
                        else
                        {
                            _ = Interlocked.Exchange(ref pending, 0);
                            token = evt;
                        }

                        Volatile.Write(ref consumed, Interlocked.Read(ref produced));

                        // A real resizer waits ResizeTaskDelaySeconds here. One second keeps the test quick while
                        // staying well clear of the only false-positive window: a producer descheduled between
                        // publishing its increment and reading the pending flag. Timing out while producers are
                        // still running and work is outstanding is the stall signature.
                        if (!token.Wait(TimeSpan.FromSeconds(1))
                                && !Volatile.Read(ref stop)
                                && Interlocked.Read(ref produced) != Volatile.Read(ref consumed))
                        {
                            _ = Interlocked.Increment(ref stalls);
                        }
                    }
                });
                consumer.Start();

                var producers = new List<Thread>();
                for (var i = 0; i < Producers; i++)
                {
                    var t = new Thread(() =>
                    {
                        while (!Volatile.Read(ref stop))
                        {
                            // Interlocked publishes the update and supplies the fence the handshake needs.
                            _ = Interlocked.Increment(ref produced);
                            if (Volatile.Read(ref pending) == 0 && Interlocked.Exchange(ref pending, 1) == 0)
                                evt.Set();
                            Thread.SpinWait(200);
                        }
                    });
                    t.Start();
                    producers.Add(t);
                }

                Thread.Sleep(4000);
                Volatile.Write(ref stop, true);
                evt.Set();
                foreach (var t in producers)
                    Assert.That(t.Join(LongWait), Is.True);
                Assert.That(consumer.Join(LongWait), Is.True);

                if (captureBeforeClear)
                {
                    Assert.That(Volatile.Read(ref stalls), Is.Zero,
                        "capturing before the clear must never leave the consumer parked while updates are outstanding");
                }
                else
                {
                    // Not asserted as a guaranteed failure -- the losing window is two adjacent instructions, so this
                    // arm documents the hazard rather than pinning a reproduction.
                    TestContext.Out.WriteLine($"clear-before-capture stalls observed: {Volatile.Read(ref stalls)}");
                }
            }
            finally { evt.Dispose(); }
        }

        #endregion
    }
}