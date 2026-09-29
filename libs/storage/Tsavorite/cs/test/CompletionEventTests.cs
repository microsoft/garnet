// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test
{
    /// <summary>
    /// Pins down <see cref="CompletionEvent"/>'s signalling contract, which is easy to misuse. <see cref="CompletionEvent.Set"/>
    /// retires the current semaphore generation and installs a fresh, unsignaled one, so a signal is only observed by a waiter
    /// holding a copy of the generation that was current when the signal was raised. Callers must therefore copy the struct
    /// before doing the work that may race with a signal, and copy it afresh on each pass; every <c>flushEvent</c> caller in the
    /// allocator and in TsavoriteLog does exactly that.
    /// </summary>
    [TestFixture]
    internal class CompletionEventTests
    {
        static readonly TimeSpan ShortWait = TimeSpan.FromMilliseconds(250);

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
                var signaled = captured.Wait(TimeSpan.FromSeconds(5));
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
    }
}