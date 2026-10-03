// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// <c>DEBUG BLOCK</c>: reference implementation of a blocking command built on session parking.
    /// </summary>
    internal sealed partial class RespServerSession
    {
        /// <summary>
        /// Waits without holding a thread, by handing the wait to the timer queue and parking the session on
        /// it. Written as the template for real blocking commands: all state the reply needs is captured
        /// here before parking, and nothing session-owned is retained.
        /// </summary>
        sealed class DebugBlockCommandContext : BlockingCommandContext, IThreadPoolWorkItem
        {
            // Cached so arming the wait allocates only the timer itself.
            static readonly TimerCallback OnElapsed = static state => ((DebugBlockCommandContext)state).Finish(aborted: false);

            // An intrusive stack, so registering a waiter allocates nothing: the link lives on the context
            // the caller already has. This is the shape a real broker takes -- BLPOP's per-key waiter list is
            // the same structure -- which is why the gate is worth having beyond the test that uses it.
            static DebugBlockCommandContext gateHead;
            static int gatedCount;

            // Long enough that a kill issued after the command is sent lands inside OnStart rather than
            // before or after it.
            const int SlowStartMilliseconds = 300;

            // Stands in for a result the operation owns and must hand back: a rented reply buffer, a
            // borrowed object reference, a lease on a pooled allocation. Counted rather than pooled,
            // because what has to be observable is whether it was reclaimed, not where it came from.
            static int outstandingResults;

            // Monotonic, so a test can tell "the publisher has produced what it owes" from "it has not got
            // there yet". Without it, a reclamation check run immediately after releasing a pinned
            // publisher reads the pre-publication zero and passes before the thing it is checking happened.
            static int resultsAcquired;

            // Pins a winning completion between claiming the outcome and publishing its result, so a test
            // can tear the connection down inside that window instead of hoping to hit it.
            static readonly SemaphoreSlim ClaimHeld = new(initialCount: 0);
            static readonly SemaphoreSlim ClaimProceed = new(initialCount: 0);

            // The same instrument for the other outcome owner: an abort also claims and publishes, so it
            // needs the same protection from disposal and the same way of being observed holding it.
            static readonly SemaphoreSlim AbortHeld = new(initialCount: 0);
            static readonly SemaphoreSlim AbortProceed = new(initialCount: 0);

            DebugBlockCommandContext gateNext;

            readonly int millisecondsDelay;
            readonly StartMode startMode;
            Timer timer;
            object result;
            bool aborted;

            internal DebugBlockCommandContext(int millisecondsDelay, StartMode startMode = StartMode.Normal)
            {
                this.millisecondsDelay = millisecondsDelay;
                this.startMode = startMode;
            }

            /// <summary>
            /// Number of waiters currently held by the gate.
            /// </summary>
            internal static int GatedCount => Volatile.Read(ref gatedCount);

            /// <summary>
            /// Number of owned results that have been acquired and not yet handed back. Any value other
            /// than zero once every connection is gone is a leak.
            /// </summary>
            internal static int OutstandingResults => Volatile.Read(ref outstandingResults);

            /// <summary>
            /// Number of results produced since the server started. Strictly increasing, so a test can wait
            /// for publication to have happened rather than assuming it has.
            /// </summary>
            internal static int ResultsAcquired => Volatile.Read(ref resultsAcquired);

            /// <summary>
            /// Whether a completion is currently pinned between claiming the outcome and publishing its
            /// result, consuming the signal if so.
            /// </summary>
            /// <remarks>
            /// Consuming matters: a signal that merely stayed raised would still read as set on the next
            /// attempt, so a repeated test would see the previous iteration's publisher and tear down a
            /// connection that had not reached the window at all.
            /// </remarks>
            internal static bool TryConsumeClaimHeld() => ClaimHeld.Wait(0);

            /// <summary>
            /// Lets a pinned completion carry on into publication.
            /// </summary>
            internal static void ProceedPastClaim() => ClaimProceed.Release();

            /// <summary>
            /// Whether an abort is currently pinned between claiming the outcome and producing its result,
            /// consuming the signal if so.
            /// </summary>
            internal static bool TryConsumeAbortHeld() => AbortHeld.Wait(0);

            /// <summary>
            /// Lets a pinned abort carry on into publication.
            /// </summary>
            internal static void ProceedPastAbort() => AbortProceed.Release();

            /// <summary>
            /// Aborts every waiter the gate is holding, from the thread pool rather than from the caller.
            /// </summary>
            /// <remarks>
            /// The thread matters. An abort delivered on the caller's session thread, or on the thread
            /// tearing the server down, would be pinned by <c>HOLDABORT</c> along with whatever else that
            /// thread owed -- deadlocking the very teardown the test is trying to run underneath it.
            /// </remarks>
            internal static int AbortGate()
            {
                var context = Interlocked.Exchange(ref gateHead, null);
                var aborted = 0;

                while (context != null)
                {
                    var next = context.gateNext;
                    context.gateNext = null;
                    var victim = context;
                    ThreadPool.UnsafeQueueUserWorkItem(static v => v.Abort(), victim, preferLocal: false);
                    aborted++;
                    context = next;
                }

                return aborted;
            }

            // Acquired by the winner, handed back either by the reply that consumes it or -- if the
            // connection died first and there is no reply to write -- by disposal.
            void AcquireResult()
            {
                result = new object();
                _ = Interlocked.Increment(ref outstandingResults);
                _ = Interlocked.Increment(ref resultsAcquired);
            }

            void ReleaseResult()
            {
                if (Interlocked.Exchange(ref result, null) != null)
                    _ = Interlocked.Decrement(ref outstandingResults);
            }

            /// <summary>
            /// Completes every waiter the gate is holding, and reports how many it woke.
            /// </summary>
            /// <remarks>
            /// A waiter whose connection died while gated stays linked until this drains it, because an abort
            /// has no cheap way to unlink from a lock-free stack. It is skipped rather than completed twice,
            /// and the reference it holds lives only until the next release -- acceptable for a debug command,
            /// and the reason a production broker wants a removable registration instead.
            /// </remarks>
            internal static int ReleaseGate()
            {
                var context = Interlocked.Exchange(ref gateHead, null);
                var released = 0;

                while (context != null)
                {
                    var next = context.gateNext;
                    context.gateNext = null;
                    if (context.Finish(aborted: false))
                        released++;
                    context = next;
                }

                return released;
            }

            void EnterGate()
            {
                var head = Volatile.Read(ref gateHead);
                while (true)
                {
                    gateNext = head;
                    var seen = Interlocked.CompareExchange(ref gateHead, this, head);
                    if (seen == head)
                        break;
                    head = seen;
                }

                // Counted only once membership is published, never before. Counting first would let
                // GATECOUNT report a waiter that is not yet on the stack, so a test polling for N would
                // release N-1 and the straggler would link afterwards and wait forever. Counting after
                // makes the reported figure a lower bound on membership, which is the direction that keeps
                // "all N are held" a fact rather than a hope.
                _ = Interlocked.Increment(ref gatedCount);
            }

            // Runs once per context, from whichever of the two mutually exclusive outcome paths wins.
            void LeaveGate()
            {
                if (startMode is StartMode.Gate or StartMode.HoldAbort)
                    _ = Interlocked.Decrement(ref gatedCount);
            }

            /// <summary>
            /// Which shape of start to exercise. One byte-wide field rather than several, because the
            /// context's size is the per-park allocation this pattern is measured on, and a reader will copy
            /// whatever this file does.
            /// </summary>
            internal enum StartMode : byte
            {
                Normal,
                SlowStart,
                FailStart,
                FailStartAndRecovery,
                Gate,
                HoldClaim,
                HoldAbort,
                BadStorage,
                BadExit
            }

            /// <summary>
            /// A timer, not a sleeping thread: the whole point is that a waiting connection costs no thread.
            /// Armed only after the timer exists, so a zero-length wait cannot fire into a half-built object.
            /// Execution context flow is suppressed for the same reason the resume path uses
            /// <c>UnsafeQueueUserWorkItem</c>: it is pure allocation here.
            /// </summary>
            protected override void OnStart()
            {
                // A real operation fails here when the thing it is waiting on cannot be reached: a broker
                // refusing a registration, an allocation failing. The session is already parked by then, so
                // the base class has to unpark it rather than leaving the connection silent forever.
                if (startMode is StartMode.FailStart or StartMode.FailStartAndRecovery)
                    throw new GarnetException("DEBUG BLOCK FAILSTART");

                // Two deliberate breaches of the authoring rules, so that the debug-build checks guarding
                // them have something that trips them. Both are what a mistaken command would do, and both
                // have to degrade into a reported violation rather than into a session whose storage is
                // released underneath it.
                if (startMode == StartMode.BadStorage)
                {
                    // Storage before the outcome is claimed. The claim is what makes this operation the
                    // session's only user, so without it the scope guarantees nothing.
                    if (TryEnterStorageScope())
                        ExitStorageScope();
                }
                else if (startMode == StartMode.BadExit)
                {
                    // An exit with no entry behind it, as a stray finally would produce.
                    ExitStorageScope();
                }

                if (startMode is StartMode.Gate or StartMode.HoldAbort)
                {
                    // Parks until something else wakes it, which is what every real blocking command does.
                    // A test can then hold every connection in the parked state at once and observe that it
                    // did, instead of hoping a short wait overlapped.
                    EnterGate();
                    return;
                }

                if (startMode == StartMode.SlowStart)
                {
                    // SLOWSTART widens the window in which the operation is still starting, so that an abort
                    // arriving here is deterministically deferred rather than racing a half-built operation.
                    // The completion it then queues is uncancellable -- the shape a broker handoff or an
                    // in-flight read has, where OnAbort cannot un-produce a result that is already on its
                    // way -- which is what makes the deferred-abort path observable from a test.
                    Thread.Sleep(SlowStartMilliseconds);
                    ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);
                    return;
                }

                if (startMode == StartMode.HoldClaim)
                {
                    ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);
                    return;
                }

                // A zero-length wait needs no timer, and a timer is the only thing this command allocates
                // beyond the context itself. Completing through the thread pool instead keeps the
                // zero-delay path allocation-free, which is what makes the allocation test able to assert
                // the machinery's cost directly rather than inferring it.
                if (millisecondsDelay == 0)
                {
                    ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);
                    return;
                }

                // Suppressed because capturing it would allocate to carry ambient state a server-side wait
                // has no use for.
                using (ExecutionContext.SuppressFlow())
                {
                    timer = new Timer(OnElapsed, this, Timeout.Infinite, Timeout.Infinite);
                }

                _ = timer.Change(millisecondsDelay, Timeout.Infinite);
            }

            void IThreadPoolWorkItem.Execute() => _ = Finish(aborted: false);

            /// <summary>
            /// Publishes the outcome and releases the session, but only if this caller won the race against
            /// every other way the operation can end. A loser must not write <see cref="aborted"/>: the
            /// winner's reply may already be on the wire.
            /// </summary>
            bool Finish(bool aborted)
            {
                if (!TryClaimOutcome())
                    return false;

                if (startMode == StartMode.HoldClaim)
                {
                    // Pinned between winning the claim and producing the result, which is the window in
                    // which a disposal must not run: the result does not exist yet, so disposal would find
                    // nothing to reclaim and the publication that follows would leak.
                    ClaimHeld.Release();
                    ClaimProceed.Wait();
                }

                this.aborted = aborted;

                // Only this mode, so that the reference command's steady-state cost stays exactly its
                // context -- which is what the allocation test measures.
                if (!aborted && startMode == StartMode.HoldClaim)
                    AcquireResult();

                LeaveGate();
                ReleaseSession();
                return true;
            }

            internal override void WriteResponse(RespServerSession session)
            {
                // A nested context can use the session's own writers, which is why blocking contexts are
                // declared inside the partial that owns their command.
                if (aborted)
                {
                    session.WriteError(CmdStrings.RESP_ERR_BLOCKING_ABORTED);
                    return;
                }

                session.WriteDirect(CmdStrings.RESP_OK);

                // The reply is what the result was for, so writing it hands the result back. Disposal
                // reclaims it only on the paths where no reply is ever written.
                ReleaseResult();
            }

            /// <summary>
            /// The connection going away is an outcome, not a failure: it is recorded and reported from
            /// <see cref="WriteResponse"/>. The base class guarantees this runs at most once and never
            /// alongside a natural completion, so it can publish the outcome directly.
            /// </summary>
            protected override void OnAbort()
            {
                if (startMode == StartMode.HoldAbort)
                {
                    // An abort is an outcome, so it occupies the same publication window a completion does.
                    // Pinning here lets a test dispose the connection underneath an abort that has claimed
                    // the outcome but not yet produced what it owes.
                    AbortHeld.Release();
                    AbortProceed.Wait();
                    AcquireResult();
                }

                aborted = true;
                LeaveGate();
            }

            /// <summary>
            /// Recovery failing on top of a failed start must not strand the session: the base class owns
            /// the release either way, and owns the reply because this command never got far enough to have
            /// one of its own.
            /// </summary>
            protected override void OnStartFailed(Exception exception)
            {
                if (startMode == StartMode.FailStartAndRecovery)
                    throw new GarnetException("DEBUG BLOCK FAILSTART recovery", exception);
            }

            protected override void OnDispose()
            {
                timer?.Dispose();
                ReleaseResult();
            }
        }

        /// <summary>
        /// DEBUG BLOCK seconds
        ///     [SYNC|SLOWSTART|FAILSTART|FAILSTART2|FAILBATCH|GATE|HOLDCLAIM|HOLDABORT
        ///      |BADSTORAGE|BADEXIT|RELEASE|GATECOUNT|CLAIMHELD|CLAIMPROCEED|LEAKCOUNT|GETLEAKCOUNT|ACQCOUNT
        ///      |ABORTGATED|ABORTHELD|ABORTPROCEED
        ///      |GETACQCOUNT|GETHOLDVALUE|GETVALUEHELD|GETVALUEPROCEED
        ///      |GETHOLDREAD|GETREADHELD|GETREADPROCEED]
        /// </summary>
        /// <remarks>
        /// Without <c>SYNC</c> the session parks for the duration, so the connection holds no thread while
        /// waiting and the server sustains far more concurrently blocked connections than it has threads.
        /// <c>SYNC</c> forces the in-place sleep instead, which is what parking exists to avoid; it is there
        /// to make the difference measurable from a client.
        /// </remarks>
        unsafe bool NetworkDebugBlock()
        {
            if (parseState.Count is < 2 or > 3)
            {
                return AbortWithWrongNumberOfArgumentsOrUnknownSubcommand(nameof(CmdStrings.BLOCK),
                                                                          nameof(RespCommand.DEBUG));
            }

            // Gate control is not itself a blocking command, so it is answered before anything is parsed as
            // a wait: these never park and always reply immediately.
            if (parseState.Count == 3 &&
                TryRunBlockControl(parseState.GetArgSliceByRef(2).ReadOnlySpan, out var controlReply))
            {
                while (!RespWriteUtils.TryWriteInt32(controlReply, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (!parseState.TryGetDouble(1, out var seconds) || double.IsNaN(seconds) || double.IsInfinity(seconds))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_TIMEOUT_NOT_VALID_FLOAT);
            }

            if (seconds < 0)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_TIMEOUT_IS_NEGATIVE);
            }

            var forceSync = false;
            var failBatch = false;
            var startMode = DebugBlockCommandContext.StartMode.Normal;
            if (parseState.Count == 3)
            {
                var modifier = parseState.GetArgSliceByRef(2).ReadOnlySpan;
                if (modifier.EqualsUpperCaseSpanIgnoringCase(CmdStrings.SYNC))
                    forceSync = true;
                else if (modifier.EqualsUpperCaseSpanIgnoringCase(CmdStrings.SLOWSTART))
                    startMode = DebugBlockCommandContext.StartMode.SlowStart;
                else if (modifier.EqualsUpperCaseSpanIgnoringCase(CmdStrings.FAILSTART))
                    startMode = DebugBlockCommandContext.StartMode.FailStart;
                else if (modifier.EqualsUpperCaseSpanIgnoringCase(CmdStrings.FAILSTART2))
                    startMode = DebugBlockCommandContext.StartMode.FailStartAndRecovery;
                else if (modifier.EqualsUpperCaseSpanIgnoringCase(CmdStrings.BADSTORAGE))
                    startMode = DebugBlockCommandContext.StartMode.BadStorage;
                else if (modifier.EqualsUpperCaseSpanIgnoringCase(CmdStrings.BADEXIT))
                    startMode = DebugBlockCommandContext.StartMode.BadExit;
                else if (modifier.EqualsUpperCaseSpanIgnoringCase(CmdStrings.FAILBATCH))
                {
                    startMode = DebugBlockCommandContext.StartMode.Gate;
                    failBatch = true;
                }
                else if (modifier.EqualsUpperCaseSpanIgnoringCase(CmdStrings.GATE))
                    startMode = DebugBlockCommandContext.StartMode.Gate;
                else if (modifier.EqualsUpperCaseSpanIgnoringCase(CmdStrings.HOLDCLAIM))
                    startMode = DebugBlockCommandContext.StartMode.HoldClaim;
                else if (modifier.EqualsUpperCaseSpanIgnoringCase(CmdStrings.HOLDABORT))
                    startMode = DebugBlockCommandContext.StartMode.HoldAbort;
                else if (!modifier.EqualsUpperCaseSpanIgnoringCase(CmdStrings.ASYNC))
                    return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
            }

            var milliseconds = (int)Math.Min(seconds * 1000, int.MaxValue);

            if (!forceSync)
            {
                // Eligibility first, context second. Everything CanParkSession reads is this session's own
                // state on this thread, so asking before building is reliable rather than a racy hint, and
                // it keeps the refusal path -- which a transaction or script takes on every call -- from
                // allocating a context only to drop it. TryParkSession repeats the check as a cheap guard.
                if (CanParkSession)
                {
                    var context = new DebugBlockCommandContext(milliseconds, startMode);
                    if (TryParkSession(context))
                    {
                        // Stands in for anything that can throw between the park and the end of the batch --
                        // a Send that fails on replies staged ahead of the park, cluster bookkeeping, the
                        // slow log. The session is parked and the batch is lost, which is the one close path
                        // that reaches neither the park's own abort nor the QUIT tail of section 5.7, so it
                        // needs the same reconciliation and nothing else schedules it.
                        if (failBatch)
                            throw new GarnetException("DEBUG BLOCK FAILBATCH");

                        return true;
                    }

                    context.Dispose();
                }

                // The session cannot park, so a script or transaction is driving it. Every blocking command
                // owes a non-blocking answer here, and for this one that is simply the reply it would have
                // sent at the end of the wait. Waiting in place is what this pattern exists to remove, so it
                // stays reserved for an explicit SYNC.
                WriteDirect(CmdStrings.RESP_OK);
                return true;
            }

            Thread.Sleep(milliseconds);
            WriteDirect(CmdStrings.RESP_OK);
            return true;
        }

        /// <summary>
        /// Runs a test-instrumentation control, if the token names one.
        /// </summary>
        /// <remarks>
        /// Each control is run exactly once, here, before the caller writes anything. Several of them
        /// release waiters or consume a one-shot signal, and the caller's write retries after flushing a
        /// full buffer -- running one in that retry condition would run it a second time and report the
        /// second, empty result.
        /// </remarks>
        /// <param name="control">Control token following the duration argument.</param>
        /// <param name="reply">Integer reply the control produced.</param>
        /// <returns>True if the token named a control.</returns>
        static bool TryRunBlockControl(ReadOnlySpan<byte> control, out int reply)
        {
            if (control.EqualsUpperCaseSpanIgnoringCase(CmdStrings.RELEASE))
                reply = DebugBlockCommandContext.ReleaseGate();
            else if (control.EqualsUpperCaseSpanIgnoringCase(CmdStrings.GATECOUNT))
                reply = DebugBlockCommandContext.GatedCount;
            else if (control.EqualsUpperCaseSpanIgnoringCase(CmdStrings.CLAIMHELD))
                reply = DebugBlockCommandContext.TryConsumeClaimHeld() ? 1 : 0;
            else if (control.EqualsUpperCaseSpanIgnoringCase(CmdStrings.CLAIMPROCEED))
            {
                DebugBlockCommandContext.ProceedPastClaim();
                reply = 1;
            }
            else if (control.EqualsUpperCaseSpanIgnoringCase(CmdStrings.LEAKCOUNT))
                reply = DebugBlockCommandContext.OutstandingResults;
            else if (control.EqualsUpperCaseSpanIgnoringCase(CmdStrings.GETLEAKCOUNT))
                reply = DebugBlockGetValues.Count;
            else if (control.EqualsUpperCaseSpanIgnoringCase(CmdStrings.GETACQCOUNT))
                reply = DebugBlockGetValues.Acquired;
            else if (control.EqualsUpperCaseSpanIgnoringCase(CmdStrings.GETHOLDVALUE))
            {
                DebugBlockGetValues.ArmHold();
                reply = 1;
            }
            else if (control.EqualsUpperCaseSpanIgnoringCase(CmdStrings.GETVALUEHELD))
                reply = DebugBlockGetValues.TryConsumeHeld() ? 1 : 0;
            else if (control.EqualsUpperCaseSpanIgnoringCase(CmdStrings.GETVALUEPROCEED))
            {
                DebugBlockGetValues.Proceed();
                reply = 1;
            }
            else if (control.EqualsUpperCaseSpanIgnoringCase(CmdStrings.GETHOLDREAD))
            {
                DebugBlockGetValues.ArmHoldRead();
                reply = 1;
            }
            else if (control.EqualsUpperCaseSpanIgnoringCase(CmdStrings.GETREADHELD))
                reply = DebugBlockGetValues.TryConsumeReadHeld() ? 1 : 0;
            else if (control.EqualsUpperCaseSpanIgnoringCase(CmdStrings.GETREADPROCEED))
            {
                DebugBlockGetValues.ProceedRead();
                reply = 1;
            }
            else if (control.EqualsUpperCaseSpanIgnoringCase(CmdStrings.ACQCOUNT))
                reply = DebugBlockCommandContext.ResultsAcquired;
            else if (control.EqualsUpperCaseSpanIgnoringCase(CmdStrings.ABORTGATED))
                reply = DebugBlockCommandContext.AbortGate();
            else if (control.EqualsUpperCaseSpanIgnoringCase(CmdStrings.ABORTHELD))
                reply = DebugBlockCommandContext.TryConsumeAbortHeld() ? 1 : 0;
            else if (control.EqualsUpperCaseSpanIgnoringCase(CmdStrings.ABORTPROCEED))
            {
                DebugBlockCommandContext.ProceedPastAbort();
                reply = 1;
            }
            else
            {
                reply = 0;
                return false;
            }

            return true;
        }
    }
}