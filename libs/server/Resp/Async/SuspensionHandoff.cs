// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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

        int state;

        /// <summary>
        /// Marks a suspension as in flight, before the body's resume delegate becomes reachable.
        /// </summary>
        internal void Arm() => Volatile.Write(ref state, Arming);

        /// <summary>
        /// Called by the parking thread once it has left the resource scope.
        /// </summary>
        /// <returns>True if a completion already arrived and the caller must drive the resume.</returns>
        internal bool Park() => Interlocked.Exchange(ref state, Parked) == ResumePending;

        /// <summary>
        /// Called by the thread completing the operation the body awaited.
        /// </summary>
        /// <returns>True if the caller must drive the resume, false if the parking thread will.</returns>
        internal bool ClaimResume()
        {
            var observed = Interlocked.CompareExchange(ref state, ResumePending, Arming);
            if (observed == Arming)
                return false;

            Debug.Assert(observed == Parked, "A resume arrived for a suspension that was not in flight");
            return true;
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
        /// Claims the right to run inside the session scope.
        /// </summary>
        /// <returns>True if the caller may proceed; false if the session is torn down or already busy.</returns>
        internal bool TryEnter() => Interlocked.CompareExchange(ref state, Busy, Free) == Free;

        /// <summary>
        /// Releases the scope, handing it to a waiting teardown if there is one.
        /// </summary>
        internal void Exit()
        {
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

                var spin = new SpinWait();
                while (Volatile.Read(ref state) != Closed)
                    spin.SpinOnce();
                return;
            }
        }
    }
}