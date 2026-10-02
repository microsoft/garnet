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
        /// Rent and return accounting for <c>DEBUG BLOCKGET</c>, held outside the generic context so that
        /// every instantiation of it shares one counter.
        /// </summary>
        internal static class DebugBlockGetValues
        {
            internal static int Outstanding;

            /// <summary>
            /// How long a gate holds before giving up. Long enough that a test never races it, short enough
            /// that a test which fails before opening it does not wedge a thread for the whole run.
            /// </summary>
            const int GateTimeoutMs = 30_000;

            // Monotonic, so a test can tell "the read has produced a buffer" from "it has not got there
            // yet". Without it, a reclamation check that runs right after a teardown reads the same zero
            // either way and passes without the path it is checking ever having been taken.
            static int acquired;

            // Pins a completion between reading the value and releasing the session, which is the window a
            // teardown must not silently drop a buffer in. Arming is one-shot so that exactly one operation
            // is held, rather than every subsequent one on the server.
            static int holdNext;
            static readonly SemaphoreSlim ValueHeld = new(initialCount: 0);
            static readonly SemaphoreSlim ValueProceed = new(initialCount: 0);

            // The same instrument one step earlier: pins a completion inside the storage scope, which is
            // the window a teardown must wait out rather than disposing the storage underneath it.
            static int holdReadNext;
            static readonly SemaphoreSlim ReadHeld = new(initialCount: 0);
            static readonly SemaphoreSlim ReadProceed = new(initialCount: 0);

            /// <summary>
            /// Number of pooled value buffers read from the store and not yet returned. Any value other than
            /// zero once every connection is gone is a leak, which is what makes the abort and teardown
            /// paths testable rather than merely argued.
            /// </summary>
            internal static int Count => Volatile.Read(ref Outstanding);

            /// <summary>
            /// Number of pooled value buffers read since the server started. Strictly increasing, so a test
            /// can wait for a read to have happened rather than assuming it has.
            /// </summary>
            internal static int Acquired => Volatile.Read(ref acquired);

            /// <summary>
            /// Records a rented buffer.
            /// </summary>
            internal static void Rent()
            {
                _ = Interlocked.Increment(ref Outstanding);
                _ = Interlocked.Increment(ref acquired);
            }

            /// <summary>
            /// Arms the next operation to pause between reading its value and releasing its session.
            /// </summary>
            internal static void ArmHold() => Volatile.Write(ref holdNext, 1);

            /// <summary>
            /// Takes the arming, if it is set, so that it applies to one operation only.
            /// </summary>
            internal static bool TryTakeHold() => Interlocked.Exchange(ref holdNext, 0) == 1;

            /// <summary>
            /// Pauses the calling operation until a test lets it proceed.
            /// </summary>
            internal static void Hold()
            {
                ValueHeld.Release();

                // Bounded, because the release travels over a connection to a server that may be in the
                // middle of being disposed. A gate that is never opened must give the thread back rather
                // than hold it for the life of the process.
                _ = ValueProceed.Wait(GateTimeoutMs);
            }

            /// <summary>
            /// Whether an operation is currently pinned in that window, consuming the signal if so.
            /// </summary>
            /// <remarks>
            /// Consuming matters: a signal that merely stayed raised would still read as set on the next
            /// attempt, so a repeated test would observe the previous iteration's operation and tear down a
            /// connection that had not reached the window at all.
            /// </remarks>
            internal static bool TryConsumeHeld() => ValueHeld.Wait(0);

            /// <summary>
            /// Lets a pinned operation carry on.
            /// </summary>
            internal static void Proceed() => ValueProceed.Release();

            /// <summary>
            /// Arms the next operation to pause inside its storage scope, before it reads.
            /// </summary>
            internal static void ArmHoldRead() => Volatile.Write(ref holdReadNext, 1);

            /// <summary>
            /// Takes that arming, if it is set, so that it applies to one operation only.
            /// </summary>
            internal static bool TryTakeHoldRead() => Interlocked.Exchange(ref holdReadNext, 0) == 1;

            /// <summary>
            /// Pauses the calling operation inside its storage scope until a test lets it proceed.
            /// </summary>
            internal static void HoldRead()
            {
                ReadHeld.Release();
                _ = ReadProceed.Wait(GateTimeoutMs);
            }

            /// <summary>
            /// Whether an operation is currently pinned inside its storage scope, consuming the signal if so.
            /// </summary>
            internal static bool TryConsumeReadHeld() => ReadHeld.Wait(0);

            /// <summary>
            /// Lets an operation pinned inside its storage scope carry on.
            /// </summary>
            internal static void ProceedRead() => ReadProceed.Release();
        }

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
        /// The read runs on the timer or thread-pool thread that ends the wait, through the storage API the
        /// command was dispatched with. That API is captured by value because it arrives by <c>ref</c> and
        /// the park outlives the frame holding it; capturing it, rather than reaching for a context of this
        /// context's own choosing, is what keeps the read on the right one -- on a replica the dispatched
        /// API is the consistent-read API, and a hard-coded basic context would silently bypass it.
        /// </para>
        /// <para>
        /// Two things make that sound. The session is parked, so it has stopped parsing and nothing else is
        /// driving its Tsavorite session, and <see cref="RespServerSession.CanParkSession"/> has already
        /// refused the cases where something would be. And the read happens inside
        /// <see cref="BlockingCommandContext.TryEnterStorageScope"/> and before
        /// <see cref="BlockingCommandContext.ReleaseSession"/>, which bound it against the two things that
        /// can take the storage away: teardown disposing it, and the resume handing it back to the parse
        /// loop for whatever the client pipelined behind this command.
        /// </para>
        /// </remarks>
        /// <typeparam name="TGarnetApi">Storage API the command was dispatched with.</typeparam>
        sealed class DebugBlockGetCommandContext<TGarnetApi> : BlockingCommandContext, IThreadPoolWorkItem
            where TGarnetApi : IGarnetApi
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

            /// <summary>
            /// Storage API this command was dispatched with, copied out of the parse frame.
            /// </summary>
            readonly TGarnetApi storage;

            Timer timer;
            GarnetStatus status;
            MemoryResult<byte> value;
            bool aborted;
            bool readFailed;

            /// <summary>
            /// Whether the session was being torn down by the time the wait ended, so the read never ran.
            /// </summary>
            bool storageUnavailable;

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

            /// <summary>
            /// Whether this operation pauses between reading its value and releasing its session, so that a
            /// test can tear the connection down inside that window instead of hoping to hit it.
            /// </summary>
            readonly bool holdValue;

            /// <summary>
            /// Whether this operation pauses inside its storage scope, so that a test can tear the session
            /// down while its storage is demonstrably in use.
            /// </summary>
            readonly bool holdRead;

            /// <param name="millisecondsDelay">How long to wait before reading.</param>
            /// <param name="key">Key to read once the wait elapses.</param>
            /// <param name="storage">Storage API the command was dispatched with.</param>
            internal DebugBlockGetCommandContext(int millisecondsDelay, ReadOnlySpan<byte> key, ref TGarnetApi storage)
            {
                this.millisecondsDelay = millisecondsDelay;
                this.storage = storage;
                holdValue = DebugBlockGetValues.TryTakeHold();
                holdRead = DebugBlockGetValues.TryTakeHoldRead();
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
                    timer = new Timer(static state => ((DebugBlockGetCommandContext<TGarnetApi>)state).Finish(aborted: false),
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

                // The release is owed from here on: a claim that is won and then abandoned leaves the
                // session parked on a wait that has already ended, with no receive outstanding to notice.
                try
                {
                    this.aborted = aborted;

                    // Between the claim and the release is the whole of this operation's exclusive access
                    // to the session's storage, so the read goes here. An abort skips it: the connection is
                    // being torn down and the session's storage is no longer this operation's to touch.
                    if (!aborted)
                    {
                        ReadValue();

                        if (holdValue)
                        {
                            // Pinned between owning the buffer and releasing the session, which is the
                            // window in which a teardown must still reclaim it: the reply does not exist
                            // yet, so nothing has returned it, and nothing will if the publication that
                            // follows is discarded.
                            DebugBlockGetValues.Hold();
                        }
                    }
                }
                finally
                {
                    ReleaseSession();
                }
            }

            /// <summary>
            /// Reads the key through the storage API the command was dispatched with.
            /// </summary>
            /// <remarks>
            /// Failures are recorded rather than thrown, because this runs on a timer or thread-pool thread
            /// with no handler above it.
            /// </remarks>
            void ReadValue()
            {
                // Teardown can be disposing the session's storage underneath this. The scope is what makes
                // the read and that disposal exclusive of one another; losing it is not a failure, it just
                // means the connection is already gone and there is nothing left to read for.
                if (!TryEnterStorageScope())
                {
                    storageUnavailable = true;
                    return;
                }

                try
                {
                    if (holdRead)
                    {
                        // Pinned with the scope held, which is the state a teardown has to wait out: the
                        // storage this is about to read is the storage that teardown is about to dispose.
                        DebugBlockGetValues.HoldRead();
                    }

                    status = storage.GETForMemoryResult(PinnedSpanByte.FromPinnedSpan(key.AsSpan(0, keyLength)), out value);
                }
                catch (Exception ex)
                {
                    readFailed = true;

                    // Contained: a logger that threw here would escape the callback and take the process
                    // down, turning a failed read into a failed server.
                    try
                    {
                        Owner.Logger?.LogError(ex, "DEBUG BLOCKGET failed to read the store");
                    }
                    catch
                    {
                        // Nothing useful remains if reporting the failure itself fails.
                    }
                }
                finally
                {
                    // In a finally because a read that threw after renting still has a buffer to give back.
                    if (value.MemoryOwner != null)
                    {
                        valueCounted = true;
                        DebugBlockGetValues.Rent();
                    }

                    ExitStorageScope();
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
                    _ = Interlocked.Decrement(ref DebugBlockGetValues.Outstanding);

                value.Dispose();
            }

            internal override void WriteResponse(RespServerSession session)
            {
                if (aborted || storageUnavailable)
                {
                    session.WriteError(CmdStrings.RESP_ERR_BLOCKING_ABORTED);
                    return;
                }

                if (readFailed)
                {
                    session.WriteError(CmdStrings.RESP_ERR_BLOCKING_FAILED);
                    return;
                }

                if (status == GarnetStatus.WRONGTYPE)
                {
                    session.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
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
                    // A successful read of a zero-length value has no pooled buffer behind it, and
                    // MemoryResult.Span dereferences the owner unconditionally.
                    session.WriteBulkString(value.MemoryOwner == null ? ReadOnlySpan<byte>.Empty : value.Span);
                }
                finally
                {
                    ReleaseValue();
                }
            }

            /// <summary>
            /// Cancels the wait. The base class has already claimed the outcome and releases the session
            /// itself, so this only records the outcome and stops the timer.
            /// </summary>
            protected override void OnAbort()
            {
                aborted = true;
                timer?.Dispose();
            }

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
        /// <typeparam name="TGarnetApi">Storage API this command was dispatched with.</typeparam>
        /// <param name="storageApi">Storage API this command was dispatched with.</param>
        bool NetworkDebugBlockGet<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 3)
            {
                return AbortWithWrongNumberOfArgumentsOrUnknownSubcommand(nameof(CmdStrings.BLOCKGET),
                                                                          nameof(RespCommand.DEBUG));
            }

            // A transaction locks the keys its commands declare in their key specs, and DEBUG declares
            // none -- its key position depends on the subcommand. Reading inside one would therefore be
            // unlocked, and reading it through a basic context would deadlock against the transaction's own
            // exclusive lock. Refusing is the honest answer for a debug command; a real blocking command
            // belongs in the dispatch table with key specs, and then its non-blocking fallback inside a
            // transaction works like any other command's.
            if (txnManager.state != TxnState.None)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_BLOCKGET_IN_TXN);
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
                var context = new DebugBlockGetCommandContext<TGarnetApi>(milliseconds, key.ReadOnlySpan, ref storageApi);
                if (TryParkSession(context))
                    return true;

                context.Dispose();
            }

            // Parking was refused, so something else is driving this session -- an async GET processor, a
            // replication stream, or a transport that cannot suspend its receive loop. The non-blocking
            // answer for this command is the read without the wait, which is what any caller that cannot
            // block gets, and it runs inline like any other command.
            var status = storageApi.GETForMemoryResult(key, out var value);

            if (status == GarnetStatus.WRONGTYPE)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_WRONG_TYPE);
            }

            if (status != GarnetStatus.OK)
            {
                WriteNull();
                return true;
            }

            try
            {
                WriteBulkString(value.MemoryOwner == null ? ReadOnlySpan<byte>.Empty : value.Span);
            }
            finally
            {
                value.Dispose();
            }

            return true;
        }
    }
}