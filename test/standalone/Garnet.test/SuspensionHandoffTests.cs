// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers the two state protocols that make command suspension safe: the handoff that decides which
    /// thread drives a resume, and the gate that keeps a resume out of a session that is being torn down.
    /// Both are decided by a CAS against a thread arriving a few instructions later, so a test driving them
    /// through a server would reach the interesting interleavings only by luck, and their failure modes --
    /// a resume driven twice, a resume running on a reclaimed buffer -- are silent rather than loud.
    /// </summary>
    [TestFixture]
    public class SuspensionHandoffTests : TestBase
    {
        /// <summary>
        /// Upper bound on any wait here. Nothing in this fixture blocks for longer than one thread handoff,
        /// so the limit only exists to fail a deadlock instead of hanging the run.
        /// </summary>
        static readonly TimeSpan WaitLimit = TimeSpan.FromSeconds(30);

        [Test]
        public void AResumeArrivingWhileArmingIsDrivenByTheParkingThread()
        {
            var handoff = new SuspensionHandoff();
            handoff.Arm();

            // The parking thread is still inside the resource scope, so the completing thread must stand down.
            ClassicAssert.IsFalse(handoff.ClaimResume());

            // ... and the parking thread picks the resume up on its way out.
            ClassicAssert.IsTrue(handoff.Park());
        }

        [Test]
        public void AResumeArrivingAfterParkingIsDrivenByTheResumingThread()
        {
            var handoff = new SuspensionHandoff();
            handoff.Arm();

            ClassicAssert.IsFalse(handoff.Park());
            ClassicAssert.IsTrue(handoff.ClaimResume());
        }

        [Test]
        public void ASecondSuspensionOnTheSameSessionStartsClean()
        {
            var handoff = new SuspensionHandoff();

            for (var i = 0; i < 4; i++)
            {
                handoff.Arm();
                ClassicAssert.IsFalse(handoff.Park());
                ClassicAssert.IsTrue(handoff.ClaimResume());
                handoff.Complete();
            }
        }

        [Test]
        public async Task ExactlyOneThreadDrivesEachResume()
        {
            var handoff = new SuspensionHandoff();

            for (var round = 0; round < 20_000; round++)
            {
                handoff.Arm();

                var parked = false;
                var claimed = false;
                using var start = new SemaphoreSlim(0, 2);

                var parking = Task.Run(() =>
                {
                    start.Wait();
                    parked = handoff.Park();
                });
                var resuming = Task.Run(() =>
                {
                    start.Wait();
                    claimed = handoff.ClaimResume();
                });

                start.Release(2);
                await Task.WhenAll(parking, resuming).WaitAsync(WaitLimit);

                // Both would mean the body's state machine is driven twice; neither would park it forever.
                ClassicAssert.IsTrue(parked ^ claimed,
                    $"round {round}: parked={parked} claimed={claimed}");

                handoff.Complete();
            }
        }

        [Test]
        public void AResumeEntersAnOpenGateAndLeavesItOpen()
        {
            var gate = new ResumeGate();

            ClassicAssert.IsTrue(gate.TryEnter());
            gate.Exit();
            ClassicAssert.IsTrue(gate.TryEnter());
            gate.Exit();
        }

        [Test]
        public void OnlyOneResumeIsInsideTheGateAtATime()
        {
            var gate = new ResumeGate();

            ClassicAssert.IsTrue(gate.TryEnter());
            ClassicAssert.IsFalse(gate.TryEnter());
            gate.Exit();
            ClassicAssert.IsTrue(gate.TryEnter());
        }

        [Test]
        public void AClosedGateRefusesEveryResume()
        {
            var gate = new ResumeGate();
            gate.Close();

            // Past teardown the receive buffer is no longer the session's to touch.
            ClassicAssert.IsFalse(gate.TryEnter());
            ClassicAssert.IsFalse(gate.TryEnter());
        }

        [Test]
        public async Task ClosingWaitsForTheResumeAlreadyInside()
        {
            var gate = new ResumeGate();
            ClassicAssert.IsTrue(gate.TryEnter());

            var entered = new SemaphoreSlim(0, 1);
            var closing = Task.Run(() =>
            {
                entered.Release();
                gate.Close();
            });

            await entered.WaitAsync(WaitLimit);

            // Teardown must not reclaim the session's buffers while a resume is mid-batch.
            ClassicAssert.IsFalse(closing.Wait(TimeSpan.FromMilliseconds(100)));

            gate.Exit();
            await closing.WaitAsync(WaitLimit);

            ClassicAssert.IsFalse(gate.TryEnter());
        }

        [Test]
        public async Task ClosingRacesAResumeWithoutLosingEitherOutcome()
        {
            for (var round = 0; round < 20_000; round++)
            {
                var gate = new ResumeGate();
                var entered = false;
                using var start = new SemaphoreSlim(0, 2);

                var resuming = Task.Run(() =>
                {
                    start.Wait();
                    entered = gate.TryEnter();
                    if (entered)
                        gate.Exit();
                });
                var closing = Task.Run(() =>
                {
                    start.Wait();
                    gate.Close();
                });

                start.Release(2);

                // Close hangs if it loses track of a resume that is inside, and TryEnter succeeding after
                // Close returned would mean a resume ran on a session that teardown had finished with.
                await Task.WhenAll(resuming, closing).WaitAsync(WaitLimit);
                ClassicAssert.IsFalse(gate.TryEnter(), $"round {round}: gate reopened after closing");
            }
        }
    }
}