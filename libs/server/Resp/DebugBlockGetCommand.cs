// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// <c>DEBUG BLOCKGET</c>: reference implementation of a blocking command that reads the store on
    /// completion.
    /// </summary>
    internal sealed partial class RespServerSession
    {
        /// <summary>
        /// Waits without holding a thread and then performs a real <c>GET</c> against the parked session's
        /// own storage, from the thread the wait completes on.
        /// </summary>
        /// <remarks>
        /// <para>
        /// <see cref="DebugBlockCommandContext"/> knows its reply before it blocks, which is the easy half
        /// of the pattern. This is the other half, and the one real blocking commands need: the reply is not
        /// merely delivered late, it is *computed* late, from the store, after the wait. <c>BLPOP</c> has
        /// exactly this shape -- it waits for an element to exist and then pops it -- so a pattern that
        /// could not express it would not be a pattern for blocking commands at all.
        /// </para>
        /// <para>
        /// The read runs on the timer or thread-pool thread that ends the wait, directly against
        /// <see cref="RespServerSession.storageSession"/>. That is sound because the session is parked: it
        /// has stopped parsing, so nothing else is driving its Tsavorite session, and
        /// <see cref="RespServerSession.CanParkSession"/> has already refused the one case where something
        /// would be. The ordering rule from <see cref="BlockingCommandContext.OnStart"/> is what keeps it
        /// sound -- the read completes before the session is released, never after, because the release
        /// hands the same storage back to the parse loop.
        /// </para>
        /// </remarks>
        sealed class DebugBlockGetCommandContext : BlockingCommandContext, IThreadPoolWorkItem
        {
            readonly int millisecondsDelay;

            /// <summary>
            /// Copy of the key, pinned for the life of the operation.
            /// </summary>
            /// <remarks>
            /// The key arrives as a span into the receive buffer, and the receive buffer is not stable
            /// across a park: the bytes the client pipelined behind this command stay in it, and the resume
            /// is free to compact them down over the region this command occupied. Anything the operation
            /// still needs after the parse loop returns has to be copied out of that buffer first, which is
            /// the general rule for blocking command state and the reason this is captured at park time on
            /// the parse thread rather than read later.
            /// </remarks>
            readonly byte[] key;
            readonly int keyLength;

            Timer timer;
            GarnetStatus status;
            MemoryResult<byte> value;
            bool aborted;
            bool readFailed;

            /// <summary>
            /// Whether <see cref="value"/> was counted as outstanding, so that the return is matched to the
            /// rent even if the read threw after renting.
            /// </summary>
            bool valueCounted;

            /// <summary>
            /// Claims the pooled buffer behind <see cref="value"/>, so that exactly one of the reply and
            /// disposal returns it.
            /// </summary>
            int valueTaken;

            static int outstandingValues;

            /// <summary>
            /// Number of pooled value buffers read from the store and not yet returned. Any value other than
            /// zero once every connection is gone is a leak, which is what makes the abort and teardown
            /// paths testable rather than merely argued.
            /// </summary>
            internal static int OutstandingValues => Volatile.Read(ref outstandingValues);

            /// <param name="millisecondsDelay">How long to wait before reading.</param>
            /// <param name="key">Key to read once the wait elapses.</param>
            internal DebugBlockGetCommandContext(int millisecondsDelay, ReadOnlySpan<byte> key)
            {
                this.millisecondsDelay = millisecondsDelay;
                keyLength = key.Length;

                // Pinned because the read below hands it to Tsavorite as a PinnedSpanByte, and length is
                // floored at one so that an empty key still yields a referenceable array.
                this.key = GC.AllocateUninitializedArray<byte>(Math.Max(1, key.Length), pinned: true);
                key.CopyTo(this.key);
            }

            protected override void OnStart()
            {
                if (millisecondsDelay == 0)
                {
                    ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);
                    return;
                }

                using (ExecutionContext.SuppressFlow())
                {
                    timer = new Timer(static state => ((DebugBlockGetCommandContext)state).Finish(aborted: false),
                                      this, Timeout.Infinite, Timeout.Infinite);
                }

                _ = timer.Change(millisecondsDelay, Timeout.Infinite);
            }

            void IThreadPoolWorkItem.Execute() => Finish(aborted: false);

            /// <summary>
            /// Ends the wait: claims the outcome, reads the store, publishes the result and resumes the
            /// session, in that order.
            /// </summary>
            /// <param name="aborted">True when the connection is going away rather than the wait elapsing.</param>
            void Finish(bool aborted)
            {
                if (!TryClaimOutcome())
                    return;

                this.aborted = aborted;

                // Between the claim and the release is the whole of this operation's exclusive access to the
                // session's storage, so the read goes here. An abort skips it: the connection is being torn
                // down and the session's storage is no longer this operation's to touch.
                if (!aborted)
                    ReadValue();

                ReleaseSession();
            }

            /// <summary>
            /// Reads the key through the parked session's string context.
            /// </summary>
            /// <remarks>
            /// Failures are recorded rather than thrown. The outcome is already claimed by the time this
            /// runs, so letting an exception escape would skip the release in <see cref="Finish"/> and leave
            /// the session parked on a wait that has already ended.
            /// </remarks>
            void ReadValue()
            {
                var session = Owner;

                try
                {
                    var storage = session.storageSession;
                    status = storage.GET(PinnedSpanByte.FromPinnedSpan(key.AsSpan(0, keyLength)),
                                         out value, ref storage.stringBasicContext);
                }
                catch (Exception ex)
                {
                    readFailed = true;
                    session.Logger?.LogError(ex, "DEBUG BLOCKGET failed to read the store");
                }
                finally
                {
                    // In a finally because a read that threw after renting still has a buffer to give back.
                    if (value.MemoryOwner != null)
                    {
                        valueCounted = true;
                        _ = Interlocked.Increment(ref outstandingValues);
                    }
                }
            }

            /// <summary>
            /// Takes ownership of the pooled buffer for the single caller that must return it.
            /// </summary>
            bool TryClaimValue() => Interlocked.Exchange(ref valueTaken, 1) == 0;

            /// <summary>
            /// Returns the pooled buffer. Only valid for the caller that won <see cref="TryClaimValue"/>.
            /// </summary>
            void ReleaseValue()
            {
                if (valueCounted)
                    _ = Interlocked.Decrement(ref outstandingValues);

                value.Dispose();
            }

            internal override void WriteResponse(RespServerSession session)
            {
                if (aborted)
                {
                    session.WriteError(CmdStrings.RESP_ERR_BLOCKING_ABORTED);
                    return;
                }

                if (readFailed)
                {
                    session.WriteError(CmdStrings.RESP_ERR_BLOCKING_FAILED);
                    return;
                }

                if (status != GarnetStatus.OK)
                {
                    session.WriteNull();
                    return;
                }

                if (!TryClaimValue())
                    return;

                try
                {
                    session.WriteBulkString(value.Span);
                }
                finally
                {
                    ReleaseValue();
                }
            }

            protected override void OnAbort() => Finish(aborted: true);

            /// <summary>
            /// Returns the pooled buffer on the paths where no reply is ever written.
            /// </summary>
            protected override void OnDispose()
            {
                timer?.Dispose();

                if (TryClaimValue())
                    ReleaseValue();
            }
        }

        /// <summary>
        /// DEBUG BLOCKGET seconds key
        /// </summary>
        /// <remarks>
        /// Waits <c>seconds</c> without holding a thread, then reads <c>key</c> and replies with its value,
        /// or a null bulk string if it does not exist. Exists to demonstrate -- and to let tests assert --
        /// that a parked operation may use the session's storage, which is what distinguishes this pattern
        /// from one that can only defer a reply it already knows.
        /// </remarks>
        bool NetworkDebugBlockGet()
        {
            if (parseState.Count != 3)
            {
                return AbortWithWrongNumberOfArgumentsOrUnknownSubcommand(nameof(CmdStrings.BLOCKGET),
                                                                          nameof(RespCommand.DEBUG));
            }

            if (!parseState.TryGetDouble(1, out var seconds) || double.IsNaN(seconds) || double.IsInfinity(seconds))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_TIMEOUT_NOT_VALID_FLOAT);
            }

            if (seconds < 0)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_TIMEOUT_IS_NEGATIVE);
            }

            var key = parseState.GetArgSliceByRef(2);
            var milliseconds = (int)Math.Min(seconds * 1000, int.MaxValue);

            if (CanParkSession)
            {
                var context = new DebugBlockGetCommandContext(milliseconds, key.ReadOnlySpan);
                if (TryParkSession(context))
                    return true;

                context.Dispose();
            }

            // Parking was refused, so something else is driving this session -- a transaction, a script, or
            // an async GET processor. The non-blocking answer for this command is the read without the wait,
            // which is what any caller that cannot block gets, and it runs inline like any other command.
            var storage = storageSession;
            var status = storage.GET(key, out MemoryResult<byte> value, ref storage.stringBasicContext);

            if (status != GarnetStatus.OK)
            {
                WriteNull();
                return true;
            }

            try
            {
                WriteBulkString(value.Span);
            }
            finally
            {
                value.Dispose();
            }

            return true;
        }
    }
}