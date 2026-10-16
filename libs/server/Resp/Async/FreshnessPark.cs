// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;

namespace Garnet.server
{
    // Not declared unsafe: an async method body cannot sit in an unsafe context, and the unsafe modifier
    // on a partial type declaration applies only to the part that carries it.
    internal sealed partial class RespServerSession
    {
        /// <summary>
        /// Reusable wait for a session parked until replay has caught up with what the session has already read,
        /// released when this session is torn down.
        /// </summary>
        /// <remarks>
        /// Created on first use, and deliberately not disposed, for the same reasons as
        /// <see cref="SessionDelay"/>.
        /// </remarks>
        internal FreshnessWait FreshnessWait
        {
            get
            {
                var wait = freshnessWait;
                if (wait is null)
                {
                    wait = new FreshnessWait(storeWrapper.serverOptions.ReplicaSyncTimeout);
                    var existing = Interlocked.CompareExchange(ref freshnessWait, wait, null);
                    if (existing is not null)
                        wait = existing;
                    else if (sessionTornDown)
                        wait.Cancel();
                }
                return wait;
            }
        }

        FreshnessWait freshnessWait;

        /// <summary>
        /// Whether <paramref name="cmd"/>, which has just been parsed, reads a key whose read has to wait for replay to
        /// catch up with what this session has already read, and this session can park to wait.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This answers from the keys the command names, in the order it reads them, the way the read itself does, so
        /// the wait is taken here rather than on a thread inside the read. A read that does not wait here can still
        /// wait inside the read, which is the guard that must hold: replay can move a key's sequence number between
        /// the two, and a command can read its keys in another order or fewer of them than it names. A consume that
        /// cannot park, a script's or one inside a transaction, waits only there.
        /// </para>
        /// <para>
        /// A command that has to wait leaves what it waits for in <see cref="FreshnessWait"/>.
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool MustWaitForFreshness(RespCommand cmd)
        {
            if (!CanSuspend || readSessionState is null)
                return false;

            SimpleRespKeySpec[] keySpecs;
            bool isSubCommand;
            if (cmd.IsReadOnly())
            {
                // Same selection as CanServeSlot: the commands that name keys
                if (!cmd.IsDataCommand())
                    return false;

                cmd = cmd.NormalizeForACLs();
                if (!RespCommandsInfo.TryGetSimpleRespCommandInfo(cmd, out var cmdInfo) || cmdInfo.KeySpecs is not { Length: > 0 })
                    return false;

                keySpecs = cmdInfo.KeySpecs;
                // BITOP's operation argument is consumed by the parser, so key indices need the subcommand offset
                isSubCommand = cmdInfo.IsSubCommand || cmd == RespCommand.BITOP;
            }
            else if (cmd == RespCommand.CustomRawStringCmd ? currentCustomRawStringCommand.type == CommandType.Read
                : cmd == RespCommand.CustomObjCmd && currentCustomObjectCommand.type == CommandType.Read)
            {
                // Custom commands sit outside the read range but still read a user key
                keySpecs = CustomCommandSingleKeySpec;
                isSubCommand = false;
            }
            else
            {
                return false;
            }

            if (!readSessionState.MustWaitForReplay(ref parseState, keySpecs, isSubCommand, out var manager, out var virtualSublogIdx, out var sequenceNumber))
                return false;

            FreshnessWait.Prepare(manager, virtualSublogIdx, sequenceNumber);
            return true;
        }

        /// <summary>
        /// Set by a KEYS or SCAN that stopped to wait for replay to catch up. It has written no reply: the dispatch loop
        /// parks the session, and runs the command again once replay has.
        /// </summary>
        bool freshnessScanPending;

        /// <summary>
        /// Starts a KEYS or SCAN that, on a replica that enforces consistent reads, parks the session when it would have
        /// to wait for replay, rather than waiting on a thread inside the scan.
        /// </summary>
        /// <returns>
        /// False if the scan has to wait inside the scan, which is the case off the consistent-read path, where the
        /// session cannot park, and after the scan has been run again too many times in a row.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool BeginParkableScan<TGarnetApi>()
            where TGarnetApi : IGarnetApi
            => typeof(TGarnetApi) == typeof(ConsistentReadGarnetApi) && CanSuspend && readSessionState.BeginScanProbe();

        /// <summary>
        /// Ends a scan started by <see cref="BeginParkableScan{TGarnetApi}"/>.
        /// </summary>
        /// <param name="completed">Whether the scan ran to the end, as opposed to throwing.</param>
        /// <returns>True if the scan stopped to wait for replay, in which case the command is to return without a reply.</returns>
        bool EndParkableScan(bool completed)
        {
            if (!readSessionState.EndScanProbe(completed, out var manager, out var virtualSublogIdx, out var sequenceNumber))
                return false;

            FreshnessWait.Prepare(manager, virtualSublogIdx, sequenceNumber);
            freshnessScanPending = true;
            return true;
        }

        /// <summary>
        /// Parks the session until replay has caught up with what it has already read, so a read it cannot take yet waits
        /// without holding a thread the way waiting inside the read would.
        /// </summary>
        /// <returns>True if the session parked, in which case the caller must stop processing the batch.</returns>
        /// <remarks>
        /// <para>
        /// Called from between commands with the read cursor rewound to the start of the command, so the command is
        /// parsed, authorized and verified again from the receive buffer when the wait is over: the slot it
        /// targets can have moved, and the node can have changed role, while the session waited. A command that has
        /// to wait has written no reply and touched no data.
        /// </para>
        /// <para>
        /// The park does not retain the batch scope, so the cluster epoch is released for the wait. The wait lasts as
        /// long as replay takes to catch up, and an epoch transition cannot finish while a session holds the epoch.
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.NoInlining)]
        bool ParkForFreshness()
        {
            ValueTask body;
            using (BeginAsyncCommand())
                body = WaitForFreshnessAsync();

            _ = CompleteAsyncCommand(body);
            return asyncSuspended;
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask WaitForFreshnessAsync()
        {
            var wait = FreshnessWait;
            wait.Arm();

            // The registration is refused if replay has caught up since the check that sent the session here, and the
            // command is then run straight away. A wake that finds replay behind again costs one more park.
            if (!wait.TryRegister())
                return;

            await wait.WaitAsync().ConfigureAwait(false);
            wait.Finish();
        }
    }

    /// <summary>
    /// Await source for a session parked because a read on a replica has to wait for replay to catch up with what the
    /// session has already read.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Replay calls <see cref="OnReplayReached"/> from its own thread, under the lock of the sublog it advances.
    /// Completing the wait only queues the session's continuation to the thread pool, so replay is not held up by the
    /// sessions it wakes.
    /// </para>
    /// <para>
    /// A wait ends in one of four ways: replay reaches what the session waits for, the wait times out, the manager the
    /// session is listed with is replaced, or the session is torn down. Taking the waiter off the sublog's list decides
    /// between them: whoever takes it off completes the wait, so there is exactly one.
    /// </para>
    /// <para>
    /// <see cref="gate"/> orders registering, timing out and releasing against each other. The lock order is this
    /// gate, then a sublog's lock; replay holds the sublog's lock when it wakes the wait and never takes the gate.
    /// </para>
    /// </remarks>
    internal sealed class FreshnessWait : SessionWaitSource, IParkedReplayWaiter
    {
        readonly ReadSessionWaiter node;
        readonly TimeSpan timeout;
        readonly object gate = new();

        ReadConsistencyManager manager;
        int virtualSublogIdx;
        long sequenceNumber;

        // Guarded by the gate
        bool timedOut;
        long deadline;
        Timer timer;

        /// <summary>
        /// Creates the wait of one session.
        /// </summary>
        /// <param name="timeout">How long a read waits for replay before it fails, or <see cref="Timeout.InfiniteTimeSpan"/> to wait for ever.</param>
        internal FreshnessWait(TimeSpan timeout)
        {
            this.timeout = timeout;
            node = new ReadSessionWaiter(this);
        }

        /// <summary>
        /// Records what the next wait is for. Called from the session's own thread, before the wait is armed.
        /// </summary>
        /// <param name="manager">The manager whose replay state the session waits on.</param>
        /// <param name="virtualSublogIdx">The virtual sublog it waits on.</param>
        /// <param name="sequenceNumber">The sequence number replay of that sublog has to pass.</param>
        internal void Prepare(ReadConsistencyManager manager, int virtualSublogIdx, long sequenceNumber)
        {
            this.manager = manager;
            this.virtualSublogIdx = virtualSublogIdx;
            this.sequenceNumber = sequenceNumber;
            timedOut = false;
        }

        /// <summary>
        /// Arms the source for one wait. Called before the waiter is listed: a wake that arrives before it is armed is
        /// lost.
        /// </summary>
        internal new void Arm() => base.Arm();

        /// <summary>
        /// Lists the waiter with the sublog and starts the timeout.
        /// </summary>
        /// <returns>False if there is nothing to wait for, in which case nothing is listed.</returns>
        internal bool TryRegister()
        {
            lock (gate)
            {
                if (!manager.TryRegisterFreshnessWaiter(virtualSublogIdx, sequenceNumber, node))
                    return false;

                if (timeout != Timeout.InfiniteTimeSpan)
                {
                    deadline = Environment.TickCount64 + (long)timeout.TotalMilliseconds;
                    timer ??= new Timer(static self => ((FreshnessWait)self).OnTimer(), this, Timeout.Infinite, Timeout.Infinite);
                    timer.Change(timeout, Timeout.InfiniteTimeSpan);
                }

                return true;
            }
        }

        /// <summary>
        /// Waits for a registered waiter. The returned task completes when replay has reached what the session waits for,
        /// and faults if the session is torn down first.
        /// </summary>
        /// <returns>A task representing the wait.</returns>
        internal ValueTask WaitAsync()
        {
            // Teardown may have run between registering and here, in which case it found nothing listed and nothing
            // else will take the waiter off the list or complete the wait.
            if (Cancelled)
            {
                Unlist();
                _ = Complete(canceled: true);
            }

            return Pending;
        }

        /// <summary>
        /// Ends a wait that has completed: stops the timeout, and fails the read if the wait timed out.
        /// </summary>
        /// <exception cref="TimeoutException">Replay did not catch up in time.</exception>
        internal void Finish()
        {
            lock (gate)
            {
                timer?.Change(Timeout.Infinite, Timeout.Infinite);
                if (!timedOut)
                    return;
            }

            ExceptionUtils.ThrowException(new TimeoutException("Consistent read timed out waiting for replay to catch up."));
        }

        /// <inheritdoc/>
        public void OnReplayReached() => _ = Complete(canceled: false);

        /// <inheritdoc/>
        protected override void OnCancelling()
        {
            Unlist();
            lock (gate)
                timer?.Change(Timeout.Infinite, Timeout.Infinite);
        }

        /// <summary>
        /// Takes the waiter off the sublog's list if it is on it. There is nothing to take off before the first wait.
        /// </summary>
        void Unlist()
        {
            lock (gate)
                _ = manager?.TryRemoveFreshnessWaiter(virtualSublogIdx, node);
        }

        /// <summary>
        /// Fails the wait if it has outlived its deadline. Run by the timeout timer.
        /// </summary>
        internal void OnTimer()
        {
            lock (gate)
            {
                // The timer of an earlier wait can fire during a later one, which has a later deadline
                var remaining = deadline - Environment.TickCount64;
                if (remaining > 0)
                {
                    timer.Change(remaining, Timeout.Infinite);
                    return;
                }

                // Whoever takes the waiter off the list completes the wait, so a replay that got there first has
                // already woken it and the timeout is moot
                if (manager.TryRemoveFreshnessWaiter(virtualSublogIdx, node))
                {
                    timedOut = true;
                    _ = Complete(canceled: false);
                }
            }
        }
    }
}