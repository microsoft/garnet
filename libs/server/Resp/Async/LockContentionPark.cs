// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    // Not declared unsafe: an async method body cannot sit in an unsafe context, and the unsafe modifier
    // on a partial type declaration applies only to the part that carries it.
    internal sealed partial class RespServerSession : ILockContentionHandler
    {
        /// <summary>
        /// How a command that finds a key latched by another session waits for it.
        /// </summary>
        internal enum LockWaitClass : byte
        {
            /// <summary>
            /// Retries on the thread it runs on until the latch is released: running it again, or continuing it later,
            /// would not be the same as running it once.
            /// </summary>
            InPlace,

            /// <summary>
            /// Parks the session and is run again from its parse once the latch is released, provided it has changed no
            /// record and sent none of its reply (see <see cref="CommandRestartPoint"/>).
            /// </summary>
            Restart,

            /// <summary>
            /// As <see cref="Restart"/>, and once it has changed a record, parks and is continued from the progress it
            /// records for itself.
            /// </summary>
            Progress,
        }

        /// <summary>
        /// The <see cref="LockWaitClass"/> of each command, indexed by the command.
        /// </summary>
        static readonly LockWaitClass[] LockWaitClasses = CreateLockWaitClasses();

        /// <summary>
        /// Where the command being run can be run again from. Shared with the functions state of every storage session of
        /// this session, whose store callbacks disarm it as they change a record.
        /// </summary>
        internal readonly CommandRestartPoint restartPoint = new();

        /// <summary>
        /// Thrown out of a store operation to unwind the command that found a key latched, so that the session can park.
        /// One per session, as a session unwinds at most one command at a time.
        /// </summary>
        LockContendedException lockContended;

        /// <summary>
        /// Set as <see cref="lockContended"/> is thrown, and cleared by the park it unwinds to: 1 to run the command again
        /// from its parse, 2 to continue it from its recorded progress, 3 to run it again from its parse once the Vector Set
        /// index it found pending a drop has been dropped (see <see cref="ParkUntilIndexDropped"/>).
        /// </summary>
        internal byte lockContendedRaised;

        /// <summary>
        /// The command to continue from its recorded progress when the session resumes, or <see cref="RespCommand.NONE"/>.
        /// </summary>
        RespCommand lockContinuationCmd;

        /// <summary>
        /// Progress a command of class <see cref="LockWaitClass.Progress"/> recorded as it unwound: the index of the first
        /// argument it had not finished with, and a count it had accumulated over the arguments before it.
        /// </summary>
        int lockProgressIndex, lockProgressCount;

        /// <summary>
        /// Reusable wait for a session parked until another session lets go of a key's latch, released when this session
        /// is torn down.
        /// </summary>
        /// <remarks>
        /// Created on first use, and deliberately not disposed, for the same reasons as <see cref="SessionDelay"/>.
        /// </remarks>
        LockReleaseWait LockReleaseWait
        {
            get
            {
                var wait = lockReleaseWait;
                if (wait is null)
                {
                    wait = new LockReleaseWait();
                    var existing = Interlocked.CompareExchange(ref lockReleaseWait, wait, null);
                    if (existing is not null)
                        wait = existing;
                    else if (sessionTornDown)
                        wait.Cancel();
                }
                return wait;
            }
        }

        LockReleaseWait lockReleaseWait;

        static LockWaitClass[] CreateLockWaitClasses()
        {
            var classes = new LockWaitClass[(int)RespCommandExtensions.LastValidCommand + 1];

            // A data command that has changed nothing can be run again
            for (var cmd = RespCommandExtensions.FirstDataCommand; cmd <= RespCommandExtensions.LastDataCommand; cmd++)
                classes[(int)cmd] = LockWaitClass.Restart;

            // A transaction or procedure finds a key latched as it locks its keys, before it has changed anything
            foreach (var cmd in (ReadOnlySpan<RespCommand>)[
                RespCommand.EXEC, RespCommand.RUNTXP, RespCommand.CustomTxn, RespCommand.CustomRawStringCmd, RespCommand.CustomObjCmd])
                classes[(int)cmd] = LockWaitClass.Restart;

            // These change one key after another, and count what they changed, so they continue from the key they reached
            foreach (var cmd in (ReadOnlySpan<RespCommand>)[RespCommand.DEL, RespCommand.UNLINK, RespCommand.MSET])
                classes[(int)cmd] = LockWaitClass.Progress;

            // A script changes keys through a session of its own, so what it has changed does not disarm this one's point
            classes[(int)RespCommand.EVAL] = classes[(int)RespCommand.EVALSHA] = LockWaitClass.InPlace;

            // These have effects outside the store and the reply, or hold state across store operations
            foreach (var cmd in (ReadOnlySpan<RespCommand>)[
                RespCommand.BLPOP, RespCommand.BRPOP, RespCommand.BLMOVE, RespCommand.BRPOPLPUSH, RespCommand.BLMPOP,
                RespCommand.BZMPOP, RespCommand.BZPOPMAX, RespCommand.BZPOPMIN,
                RespCommand.MIGRATE, RespCommand.FLUSHALL, RespCommand.FLUSHDB, RespCommand.SWAPDB,
                RespCommand.VADD, RespCommand.VREM, RespCommand.VSETATTR, RespCommand.VCARD, RespCommand.VDIM, RespCommand.VEMB,
                RespCommand.VGETATTR, RespCommand.VINFO, RespCommand.VISMEMBER, RespCommand.VLINKS, RespCommand.VRANDMEMBER,
                RespCommand.VSIM, RespCommand.XVCREATE, RespCommand.XVIMPORT,
                RespCommand.RICREATE, RespCommand.RIDEL, RespCommand.RIPROMOTE, RespCommand.RIRESTORE, RespCommand.RISET,
                RespCommand.RICONFIG, RespCommand.RIEXISTS, RespCommand.RIGET, RespCommand.RIMETRICS, RespCommand.RIRANGE,
                RespCommand.RISCAN,
                RespCommand.WATCH, RespCommand.WATCHMS, RespCommand.WATCHOS, RespCommand.SSUBSCRIBE, RespCommand.SPUBLISH,
                RespCommand.KEYS, RespCommand.SCAN, RespCommand.COSCAN, RespCommand.DBSIZE,
                RespCommand.HSCAN, RespCommand.SSCAN, RespCommand.ZSCAN, RespCommand.HCOLLECT, RespCommand.ZCOLLECT])
                classes[(int)cmd] = LockWaitClass.InPlace;

            return classes;
        }

        /// <summary>
        /// Rethrows the exception that parks the session on a latched key, if code the session does not control has caught it.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void RethrowLockContention()
        {
            if (lockContendedRaised != 0)
                ThrowLockContended();
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void ThrowLockContended() => throw lockContended;

        /// <summary>
        /// Called by the store, with the epoch released, when an operation of this session finds its key's bucket latched by
        /// another session. Parks the session if the command can be run again or continued, and otherwise retries in place.
        /// </summary>
        /// <param name="contention">The contended operation.</param>
        /// <remarks>
        /// <para>
        /// Parking unwinds the command with <see cref="LockContendedException"/>, which the store lets out with the
        /// operation undone, up to <see cref="ConsumeCore"/>. The session then waits, holding no thread, until the latch
        /// is released, and runs the command again from its parse, or continues it, when it resumes.
        /// </para>
        /// <para>
        /// A session that cannot park, or whose command cannot be run again, retries on its own thread as it does without
        /// a handler: a consume with no receive loop to unwind to, one inside a transaction or a script, one with
        /// operations at the device that the command has yet to complete, and one not running a command at all, such as
        /// a session replaying the AOF.
        /// </para>
        /// </remarks>
        public unsafe void OnLockContended(in LockContention contention)
        {
            var cmd = restartPoint.Command;
            var cls = (uint)cmd < (uint)LockWaitClasses.Length ? LockWaitClasses[(int)cmd] : LockWaitClass.InPlace;

            if (cls == LockWaitClass.InPlace
                || (!restartPoint.IsArmed && cls != LockWaitClass.Progress)
                || !CanSuspend
                || DenyBlocking
                || recvBufferPtr == null
                || storageSession.PendingOperationCount != 0)
            {
                _ = Thread.Yield();
                return;
            }

            // A latch taken for a single operation is released within microseconds, while parking costs a thread-pool hop
            // and running the command again, so the session parks only on a latch that outlasts a short progressive spin.
            // Sleep is disabled: a holder that is still busy after the spin is waited for without the thread.
            var spin = ConfiguredSpinWait.BoundedProgressive(LockSpinIterations, sleep1Threshold: -1);
            while (spin.TryWait())
            {
                if (!contention.IsContended)
                    return;
            }

            LockReleaseWait.Prepare(in contention);
            lockContendedRaised = restartPoint.IsArmed ? (byte)1 : (byte)2;
            throw lockContended ??= new LockContendedException();
        }

        /// <summary>
        /// Iterations of the progressive spin a session takes on a latched key before it parks.
        /// </summary>
        const int LockSpinIterations = 32;

        /// <summary>
        /// Parks the session on the latch its command found held, or on the Vector Set index drop it found pending, once the
        /// command has unwound to <see cref="ConsumeCore"/>.
        /// </summary>
        /// <remarks>
        /// <para>
        /// A command to be run again goes back to the start of its frame and to the reply position it started at, so it is
        /// parsed, authorized and verified again on resume, against fresh state. The park does not retain the batch scope:
        /// the cluster epoch is released for the wait, and the replies of the commands before it go out.
        /// </para>
        /// <para>
        /// A command to be continued has changed records under the slot verification and cluster epoch it began with, so
        /// the park retains the batch scope, as it does across a device read: it continues under the same epoch, at the
        /// same reply position. The wait is bounded by the holder of the latch, which holds it for an operation or a
        /// transaction that does not itself wait on this session.
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.NoInlining)]
        unsafe void ParkOnLockContention()
        {
            Debug.Assert(!asyncSuspended, "A command unwound for a latched key while the session was suspended");

            var raised = lockContendedRaised;
            lockContendedRaised = 0;

            if (raised is 1 or 3)
            {
                endReadHead = readHead = restartPoint.ReadHead;
                dcurr = restartPoint.replyStart != null ? restartPoint.replyStart : networkSender.GetResponseObjectHead();
                commandErrorWritten = false;

                // A transaction that deferred to a migration before it found the key latched verifies its keys again
                csvi.mustWait = csvi.mustOffload = false;

                if (raised == 1)
                    ParkForLockRelease(retainBatchScope: false);
                else
                    ParkForIndexDrop();
            }
            else
            {
                Debug.Assert(raised == 2, "A command unwound for a latched key without the handler having parked it");
                lockContinuationCmd = restartPoint.Command;
                ParkForLockRelease(retainBatchScope: true);
            }

            if (!SuspendedHoldingBatchScope)
                FlushBatch();
            recvBufferPtr = null;
        }

        void ParkForLockRelease(bool retainBatchScope)
        {
            ValueTask body;
            using (BeginAsyncCommand(retainBatchScope))
                body = WaitForLockReleaseAsync();

            _ = CompleteAsyncCommand(body);
            Debug.Assert(asyncSuspended, "A lock wait completed without suspending");
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask WaitForLockReleaseAsync()
        {
            var wait = LockReleaseWait;
            wait.Arm();
            wait.Register(storeWrapper.lockReleaseSignal);
            try
            {
                // Suspends even when the latch was released before the waiter was listed: the command that found it
                // latched has already unwound, and runs again only when the session resumes
                await new AlwaysSuspend(wait.WaitAsync());
            }
            finally
            {
                wait.Finish();
            }
        }

        /// <summary>
        /// Continues a command of class <see cref="LockWaitClass.Progress"/> that parked on a latched key, from the
        /// progress it recorded, and accounts for it as the dispatch loop does for a command it runs.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        void ContinueAfterLockRelease<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var cmd = lockContinuationCmd;
            lockContinuationCmd = RespCommand.NONE;
            restartPoint.Continue(cmd);

            _ = ProcessBasicCommands(cmd, ref storageApi);

            if (commandStats != null)
            {
                commandStats.IncrementCalls(cmd);
                if (commandErrorWritten)
                {
                    commandStats.IncrementFailed(cmd);
                    commandErrorWritten = false;
                }
            }

            readHead = endReadHead;

            if (LatencyMetrics != null) opCount++;
            if (slowLogThreshold > 0)
            {
                if (asyncSuspended)
                    slowLogSuspendedCmd = cmd;
                else
                    HandleSlowLog(cmd);
            }
            if (sessionMetrics != null)
            {
                sessionMetrics.total_commands_processed++;
                sessionMetrics.total_write_commands_processed += cmd.OneIfWrite();
                sessionMetrics.total_read_commands_processed += cmd.OneIfRead();
            }
            if (SessionAsking != 0)
                SessionAsking = (byte)(SessionAsking - 1);
        }

        /// <summary>
        /// Awaits a pending wait without completing synchronously, even when the wait has already completed: the
        /// continuation is then queued to the thread pool by the wait's source.
        /// </summary>
        readonly struct AlwaysSuspend(ValueTask pending) : ICriticalNotifyCompletion
        {
            public AlwaysSuspend GetAwaiter() => this;

            public bool IsCompleted => false;

            public void GetResult() => pending.GetAwaiter().GetResultGuarded();

            public void OnCompleted(Action continuation) => pending.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(continuation);

            public void UnsafeOnCompleted(Action continuation) => pending.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(continuation);
        }
    }

    /// <summary>
    /// Unwinds a command that found a key latched by another session, so that its session can park until the latch is
    /// released (see <see cref="RespServerSession.OnLockContended"/>). Never seen by a client.
    /// </summary>
    internal sealed class LockContendedException : Exception
    {
    }
}