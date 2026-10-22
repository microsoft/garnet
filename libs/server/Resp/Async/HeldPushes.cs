// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - writers that find the session's response object taken
    /// </summary>
    /// <remarks>
    /// <para>
    /// The response object, and the sender lock that guards it, is held by the session for each batch it runs, by
    /// a command suspended with an operation in flight until the operation completes (see
    /// <see cref="BeginAsyncCommand(bool)"/>), and briefly by each push from another thread. Nothing waits on a
    /// thread for it. A pusher holding its own session's lock while it waits for this one could otherwise wait
    /// on a session that is waiting for it, as two sessions publishing to each other's channels do. A pusher that
    /// finds the lock taken spins briefly, which covers another push, and then:
    /// </para>
    /// <list type="bullet">
    /// <item>A publisher that may wait, finding the session suspended holding the lock (<see cref="pushesHeldBack"/>),
    /// parks on the sender's capacity waiters, which the session notifies when it resumes, and delivers then.</item>
    /// <item>Any other publisher leaves a copy of the message on <see cref="heldPushes"/>.</item>
    /// <item>The async GET processor marks itself in <see cref="senderWaiters"/> and waits on its event.</item>
    /// </list>
    /// <para>
    /// Whoever lets go of the lock then serves what was left (<see cref="ServeSenderWaiters"/>). A pusher counts
    /// what it left and then tries the lock again, and every holder exits the lock and then reads the count; both
    /// steps of each are full fences, so one of the two sees the other.
    /// </para>
    /// </remarks>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>A message left for the session to write out.</summary>
        /// <param name="Pattern">The pattern it matched, or null if it was published to a channel subscribed to by name.</param>
        /// <param name="Key">The channel.</param>
        /// <param name="Value">The message.</param>
        readonly record struct HeldPush(byte[] Pattern, byte[] Key, byte[] Value);

        /// <summary>
        /// Messages left by pushers, in the order they were left, and the lock that orders the additions.
        /// </summary>
        sealed class HeldPushQueue
        {
            /// <summary>Messages not yet taken for writing.</summary>
            public List<HeldPush> Pending = [];

            /// <summary>The emptied list the next drain swaps in, owned by whoever holds the sender lock.</summary>
            public List<HeldPush> Spare = [];
        }

        /// <summary>Bit of <see cref="senderWaiters"/> set while the async GET processor waits for the sender lock.</summary>
        const int ProcessorWaitsForSender = 1;

        /// <summary>What each message on <see cref="heldPushes"/> adds to <see cref="senderWaiters"/>.</summary>
        const int OneHeldPush = 2;

        /// <summary>
        /// Set while a command is suspended holding the per-batch scope, so that a publisher that may wait parks
        /// rather than leave its message for the whole suspension.
        /// </summary>
        volatile bool pushesHeldBack;

        /// <summary>Allocated by the first push that finds the sender lock taken.</summary>
        HeldPushQueue heldPushes;

        /// <summary>
        /// What is waiting for the sender lock to be let go: <see cref="ProcessorWaitsForSender"/>, plus
        /// <see cref="OneHeldPush"/> for each message on <see cref="heldPushes"/>.
        /// </summary>
        int senderWaiters;

        /// <inheritdoc />
        internal override bool PushesHeldBack => pushesHeldBack;

        /// <summary>
        /// Marks the session as suspended holding the per-batch scope. Called by the suspending pass, last.
        /// </summary>
        void HoldBackPushes() => pushesHeldBack = true;

        /// <summary>
        /// Ends the hold that <see cref="HoldBackPushes"/> set, as the session resumes. A publisher parked on the
        /// hold is woken: it delivers once the batch lets go of the response object.
        /// </summary>
        void ReleaseHeldBackPushes()
        {
            pushesHeldBack = false;

            // A parked publisher registers and then reads the mark, so one of the two sees the other
            Interlocked.MemoryBarrier();
            networkSender.NotifySendCapacityWaiters();
        }

        /// <summary>
        /// Ends the hold for a session torn down while suspended holding the per-batch scope, dropping the messages
        /// nobody will now read.
        /// </summary>
        void DropHeldBackPushes()
        {
            pushesHeldBack = false;
            DropHeldPushes();
        }

        /// <summary>
        /// Takes the sender lock for a push from another thread, spinning briefly if it is taken.
        /// </summary>
        /// <returns>False if it is still taken, or the session is suspended holding it.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool TryEnterSenderForPush() => networkSender.TryEnter() || SpinToEnterSenderForPush();

        /// <summary>
        /// Spins for a lock held by another push, which writes one message and lets go.
        /// </summary>
        /// <remarks>
        /// CPU-local only: what follows is leaving a copy of the message or a wait on an event, either cheaper than
        /// yielding the thread, and a lock held for a batch or a suspension is not worth spinning on at all.
        /// </remarks>
        [MethodImpl(MethodImplOptions.NoInlining)]
        bool SpinToEnterSenderForPush()
        {
            var spinner = ConfiguredSpinWait.CpuLocal();
            while (!pushesHeldBack && spinner.TryWait())
            {
                if (networkSender.TryEnter())
                    return true;
            }

            return false;
        }

        /// <summary>
        /// Leaves a message for whoever holds the sender lock, and serves it here if the lock was let go first.
        /// </summary>
        void LeavePush(bool isPattern, ReadOnlySpan<byte> pattern, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            var queue = heldPushes ?? AllocateHeldPushes();
            var push = new HeldPush(isPattern ? pattern.ToArray() : null, key.ToArray(), value.ToArray());
            lock (queue)
            {
                queue.Pending.Add(push);
                _ = Interlocked.Add(ref senderWaiters, OneHeldPush);
            }

            ServeSenderWaitersSlow();
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        HeldPushQueue AllocateHeldPushes()
            => Interlocked.CompareExchange(ref heldPushes, new(), null) ?? heldPushes;

        /// <summary>
        /// Called after every exit of the sender lock by this session's writers: serves what was left while the
        /// lock was held.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void ServeSenderWaiters()
        {
            if (Volatile.Read(ref senderWaiters) != 0)
                ServeSenderWaitersSlow();
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void ServeSenderWaitersSlow()
        {
            while (true)
            {
                var waiters = Volatile.Read(ref senderWaiters);
                if ((waiters & ProcessorWaitsForSender) != 0
                    && (Interlocked.And(ref senderWaiters, ~ProcessorWaitsForSender) & ProcessorWaitsForSender) != 0)
                    asyncWaiter.Signal();

                // A holder that took the lock since serves what is left when it lets go
                if (waiters < OneHeldPush || !networkSender.TryEnter())
                    return;

                WriteHeldPushesAndExit();
            }
        }

        /// <summary>
        /// Writes out the messages left, holding the sender lock and letting go of it after.
        /// </summary>
        void WriteHeldPushesAndExit()
        {
            try
            {
                networkSender.GetResponseObject();
            }
            catch
            {
                // The connection is gone, and the messages with it
                DropHeldPushes();
                networkSender.Exit();
                return;
            }

            dcurr = networkSender.GetResponseObjectHead();
            dend = networkSender.GetResponseObjectTail();
            try
            {
                writingOutOfBand = true;
                WriteHeldPushes();
                if (dcurr > networkSender.GetResponseObjectHead())
                    SendNow(networkSender.GetResponseObjectHead());
            }
            catch
            {
                // The connection is gone
            }
            finally
            {
                writingOutOfBand = false;
                networkSender.ExitAndReturnResponseObject();
            }
        }

        /// <summary>
        /// Writes the messages left so far into the response object, holding the sender lock, ahead of whatever the
        /// holder writes next, so that each publisher's messages still arrive in the order it made them.
        /// </summary>
        void WriteHeldPushes()
        {
            if (heldPushes is not { } queue)
                return;

            List<HeldPush> taken;
            lock (queue)
            {
                taken = queue.Pending;
                if (taken.Count == 0)
                    return;

                queue.Pending = queue.Spare;
                queue.Spare = null;
                _ = Interlocked.Add(ref senderWaiters, -OneHeldPush * taken.Count);
            }

            try
            {
                foreach (var push in taken)
                    WritePublishMessage(push.Pattern is not null, push.Pattern, push.Key, push.Value);
            }
            finally
            {
                taken.Clear();
                queue.Spare = taken;
            }
        }

        /// <summary>
        /// Drops the messages left, for a connection that is gone.
        /// </summary>
        void DropHeldPushes()
        {
            if (heldPushes is not { } queue)
                return;

            lock (queue)
            {
                _ = Interlocked.Add(ref senderWaiters, -OneHeldPush * queue.Pending.Count);
                queue.Pending.Clear();
            }
        }
    }
}