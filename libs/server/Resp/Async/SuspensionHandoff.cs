// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;

namespace Garnet.server
{
    /// <summary>
    /// Decides which of two threads drives a suspended command body: the one that parked it, or the one whose
    /// completion released it.
    /// </summary>
    /// <remarks>
    /// A body hands its resume delegate to an awaiter while the parking thread is still inside the session's
    /// resource scope, holding the response object and, in cluster mode, the epoch. A completion that arrives
    /// in that window cannot resume the body, because resuming re-enters the scope. It also cannot be dropped.
    /// So it is recorded, and the parking thread drains it on its way out. Once the parking thread has left
    /// the scope, the roles reverse and the completing thread drains.
    ///
    /// The window is a few instructions wide, which is why the handoff is a state machine rather than a lock:
    /// whichever thread arrives second has to be able to tell that it is second.
    ///
    /// Driving a resume is a claim, not an observation, so that a suspension is driven by exactly one thread
    /// however many completions reach it.
    /// </remarks>
    internal struct SuspensionHandoff
    {
        /// <summary>No suspension is in flight.</summary>
        const int Idle = 0;

        /// <summary>
        /// A body has handed its resume delegate to an awaiter, but the parking thread is still inside the
        /// resource scope. A completion arriving now must be deferred.
        /// </summary>
        const int Arming = 1;

        /// <summary>The parking thread has left the scope. A completion may resume immediately.</summary>
        const int Parked = 2;

        /// <summary>A completion arrived while arming. The parking thread drains it on its way out.</summary>
        const int ResumePending = 3;

        /// <summary>
        /// A resume has been claimed, and the thread that claimed it is driving the body. Distinct from
        /// <see cref="Parked"/> so that the claim is exclusive: see <see cref="ClaimResume"/>.
        /// </summary>
        const int Resuming = 4;

        int state;

        /// <summary>
        /// Marks a suspension as in flight, before the body's resume delegate becomes reachable.
        /// </summary>
        internal void Arm() => Volatile.Write(ref state, Arming);

        /// <summary>
        /// Called by the parking thread once it has left the resource scope.
        /// </summary>
        /// <returns>True if a completion already arrived and the caller must drive the resume.</returns>
        internal bool Park()
        {
            // The common case: no completion has arrived, so the suspension is left for one to claim.
            if (Interlocked.CompareExchange(ref state, Parked, Arming) == Arming)
                return false;

            // Otherwise a completion arrived while arming and this thread drives it. It is claimed here
            // rather than published as parked, so that it is driven by this thread alone.
            return Interlocked.CompareExchange(ref state, Resuming, ResumePending) == ResumePending;
        }

        /// <summary>
        /// Called by the thread completing the operation the body awaited.
        /// </summary>
        /// <returns>True if the caller must drive the resume, false if the parking thread will.</returns>
        /// <remarks>
        /// A resume is claimed by taking the suspension out of <see cref="Parked"/>, not merely by observing
        /// it there, so that exactly one thread drives a given suspension. A completion that finds any other
        /// state has nothing to drive: the suspension has already been claimed, has already finished, or was
        /// never in flight. Driving one anyway would re-enter the session with no body to resume, which
        /// <c>ConsumeCore</c> reads as a fresh batch -- replaying the receive buffer from the start, and so
        /// running the batch's commands, and writing their replies, a second time.
        /// </remarks>
        internal bool ClaimResume()
        {
            // While the parking thread is still inside the resource scope, the completion is recorded for it
            // to drain on its way out and this thread stands down.
            if (Interlocked.CompareExchange(ref state, ResumePending, Arming) == Arming)
                return false;

            return Interlocked.CompareExchange(ref state, Resuming, Parked) == Parked;
        }

        /// <summary>
        /// Marks the suspension finished, so the session may suspend again.
        /// </summary>
        internal void Complete() => Volatile.Write(ref state, Idle);
    }

    /// <summary>
    /// Admits resumes into a session's resource scope, and closes permanently when the session is torn down.
    /// </summary>
    /// <remarks>
    /// A resume arrives on whatever thread completed the awaited operation, which is unrelated to the network
    /// thread that may be disposing the session at that moment. Past teardown the receive buffer and response
    /// object are no longer the session's to touch, so a resume must be refused rather than allowed to run on
    /// reclaimed memory; and teardown must not reclaim while a resume is mid-batch, so closing waits for one
    /// that is already inside. That wait is bounded by the work of a single batch, not by whatever the
    /// suspended command was waiting on, because the command has already been released by then.
    /// </remarks>
    internal struct ResumeGate
    {
        /// <summary>Nothing inside; resumes are admitted.</summary>
        const int Free = 0;

        /// <summary>A resume is inside the scope.</summary>
        const int Busy = 1;

        /// <summary>Teardown is waiting for the resume inside to leave.</summary>
        const int Closing = 2;

        /// <summary>Torn down; no resume will be admitted again.</summary>
        const int Closed = 3;

        int state;

        /// <summary>
        /// Managed thread id of the resume inside the scope, or zero. Only ever set to a given id by the
        /// thread that owns it, and cleared by that same thread before it releases, so a thread reading its
        /// own id here is necessarily still inside.
        /// </summary>
        int holder;

        /// <summary>
        /// Claims the right to run inside the session scope.
        /// </summary>
        /// <returns>True if the caller may proceed; false if the session is torn down or already busy.</returns>
        internal bool TryEnter()
        {
            if (Interlocked.CompareExchange(ref state, Busy, Free) != Free)
                return false;

            Volatile.Write(ref holder, Environment.CurrentManagedThreadId);
            return true;
        }

        /// <summary>
        /// Releases the scope, handing it to a waiting teardown if there is one.
        /// </summary>
        internal void Exit()
        {
            Volatile.Write(ref holder, 0);

            if (Interlocked.CompareExchange(ref state, Free, Busy) == Busy)
                return;

            Debug.Assert(Volatile.Read(ref state) == Closing, "The gate was left by a thread that did not hold it");
            Volatile.Write(ref state, Closed);
        }

        /// <summary>
        /// Closes the gate permanently, waiting for any resume already inside to leave.
        /// </summary>
        /// <remarks>
        /// <see cref="Closed"/> is only ever published by a thread that observed the gate free, or by the
        /// resume that was asked to hand it over. A resume leaving between this thread's two attempts may be
        /// followed immediately by another entering -- <c>DrainResumes</c> exits and re-enters on
        /// adjacent instructions -- so the attempt restarts rather than assuming the gate is still free.
        /// <para>
        /// Teardown can also be reached from within the scope: a fault in a resumed batch disposes the
        /// network sender, which for a TLS connection is the network handler itself and so disposes the
        /// session. Waiting there would be waiting on the calling thread. The gate is left in
        /// <see cref="Closing"/> instead, which already refuses new entrants, and the resume publishes
        /// <see cref="Closed"/> through <see cref="Exit"/> as it unwinds.
        /// </para>
        /// </remarks>
        internal void Close()
        {
            while (true)
            {
                var observed = Interlocked.CompareExchange(ref state, Closed, Free);
                if (observed != Busy)
                    return;

                if (Interlocked.CompareExchange(ref state, Closing, Busy) != Busy)
                    continue;

                // The gate is now Closing, so the holder observed above cannot be replaced by another
                // entrant, and reading our own id means that holder is this thread.
                if (Volatile.Read(ref holder) == Environment.CurrentManagedThreadId)
                    return;

                var spin = new SpinWait();
                while (Volatile.Read(ref state) != Closed)
                    spin.SpinOnce();
                return;
            }
        }
    }
}