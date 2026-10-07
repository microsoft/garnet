// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    internal sealed partial class RespServerSession
    {
        /// <summary>
        /// Context this connection last parked <c>DEBUG BLOCKASYNC</c> on, reused when it is free.
        /// </summary>
        DebugBlockAsyncCommandContext cachedBlockAsyncContext;

        /// <summary>
        /// Context this connection last parked <c>DEBUG BLOCKGETASYNC</c> on, reused when it is free.
        /// </summary>
        /// <remarks>
        /// Held as the base type because the context is generic over the storage API the command was
        /// dispatched with, and a connection can be dispatched through more than one of those.
        /// </remarks>
        AsyncBlockingCommandContext cachedBlockGetAsyncContext;

        /// <summary>
        /// <c>DEBUG BLOCKASYNC</c> written against <see cref="AsyncBlockingCommandContext"/>.
        /// </summary>
        /// <remarks>
        /// The same operation as <c>DEBUG BLOCK</c>: park, wait without holding a thread, reply. Written
        /// against the adapter it is the body below, and the pair exists so the difference is measurable
        /// rather than asserted -- the two commands are held to the same tests.
        /// </remarks>
        sealed class DebugBlockAsyncCommandContext : AsyncBlockingCommandContext
        {
            int millisecondsDelay;

            /// <summary>
            /// Sets up a park. Separate from the constructor because a connection reuses one context.
            /// </summary>
            /// <param name="millisecondsDelay">How long to wait before replying.</param>
            internal void Configure(int millisecondsDelay) => this.millisecondsDelay = millisecondsDelay;

            /// <remarks>
            /// Pooled so that a park costs no allocation. The default builder boxes its state machine the
            /// first time the method suspends, and this method always suspends.
            /// </remarks>
            [AsyncMethodBuilder(typeof(ParkedValueTaskMethodBuilder))]
            protected override async ValueTask RunAsync()
            {
                await Delay(millisecondsDelay);
                await ResumeSession();

                Session.WriteDirect(CmdStrings.RESP_OK);
            }
        }

        /// <summary>
        /// <c>DEBUG BLOCKGETASYNC</c> written against <see cref="AsyncBlockingCommandContext"/>.
        /// </summary>
        /// <remarks>
        /// The storage-touching counterpart of <see cref="DebugBlockAsyncCommandContext"/>, and the case
        /// that shows what the adapter is for. Reading the store leaves a pooled buffer that exactly one
        /// path must return, and in a hand-written context the read and the reply are different methods, so
        /// that buffer needs an interlocked claim and a disposal hook to be returned once on each of the
        /// several paths a park can end on. Here both are one scope and a <c>finally</c> covers all of them,
        /// including the abort that throws out of <see cref="AsyncBlockingCommandContext.ResumeSession"/>.
        /// </remarks>
        /// <typeparam name="TGarnetApi">Storage API this command was dispatched with.</typeparam>
        sealed class DebugBlockGetAsyncCommandContext<TGarnetApi> : AsyncBlockingCommandContext
            where TGarnetApi : IGarnetApi
        {
            int millisecondsDelay;

            /// <summary>
            /// Copy of the key, pinned for the life of the operation.
            /// </summary>
            /// <remarks>
            /// The key arrives as a span into the receive buffer, which is not stable across a park: the
            /// commands pipelined behind this one stay in that buffer and the resume may compact them down
            /// over the region this command occupied. Copied at park time, on the parse thread, and kept
            /// across parks so that a connection's second blocking call allocates nothing.
            /// </remarks>
            byte[] key;
            int keyLength;

            /// <summary>
            /// Storage API this command was dispatched with, copied out of the parse frame.
            /// </summary>
            TGarnetApi storage;

            /// <summary>
            /// Sets up a park. Separate from the constructor because a connection reuses one context.
            /// </summary>
            /// <param name="millisecondsDelay">How long to wait before reading.</param>
            /// <param name="commandKey">Key to read once the wait elapses.</param>
            /// <param name="storageApi">Storage API the command was dispatched with.</param>
            internal void Configure(int millisecondsDelay, ReadOnlySpan<byte> commandKey, ref TGarnetApi storageApi)
            {
                this.millisecondsDelay = millisecondsDelay;
                storage = storageApi;
                keyLength = commandKey.Length;

                // Pinned because the read hands it to Tsavorite as a PinnedSpanByte, and floored at one byte
                // so that an empty key still yields a referenceable array.
                if (key == null || key.Length < commandKey.Length)
                    key = GC.AllocateUninitializedArray<byte>(Math.Max(1, commandKey.Length), pinned: true);

                commandKey.CopyTo(key);
            }

            /// <remarks>
            /// Pooled so that a park costs no allocation. The default builder boxes its state machine the
            /// first time the method suspends, and this method always suspends.
            /// </remarks>
            [AsyncMethodBuilder(typeof(ParkedValueTaskMethodBuilder))]
            protected override async ValueTask RunAsync()
            {
                await Delay(millisecondsDelay);

                var status = GarnetStatus.NOTFOUND;
                var value = default(MemoryResult<byte>);
                var read = false;

                // The claim is what makes the parked session's storage this operation's to use, so it comes
                // first; losing it means the connection is already going away and there is nothing to read
                // for. The scope is what makes the read and a concurrent teardown exclusive of one another.
                if (TryClaim() && TryEnterStorage())
                {
                    try
                    {
                        read = true;
                        status = storage.GETForMemoryResult(PinnedSpanByte.FromPinnedSpan(key.AsSpan(0, keyLength)),
                                                            out value);
                    }
                    finally
                    {
                        ExitStorage();
                    }
                }

                try
                {
                    await ResumeSession();

                    WriteValue(read, status, value);
                }
                finally
                {
                    // Covers every way this method can end: the reply, a torn-down connection unwinding out
                    // of ResumeSession, and a read that threw.
                    value.Dispose();
                }
            }

            /// <summary>
            /// Writes the read's outcome. Runs on the resuming thread with the response buffer held.
            /// </summary>
            /// <param name="read">Whether the read ran at all.</param>
            /// <param name="status">Status the read returned.</param>
            /// <param name="value">Value the read returned.</param>
            void WriteValue(bool read, GarnetStatus status, MemoryResult<byte> value)
            {
                if (!read)
                {
                    Session.WriteError(CmdStrings.RESP_ERR_BLOCKING_ABORTED);
                    return;
                }

                if (status == GarnetStatus.WRONGTYPE)
                {
                    Session.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
                    return;
                }

                if (status != GarnetStatus.OK)
                {
                    Session.WriteNull();
                    return;
                }

                // A successful read of a zero-length value has no pooled buffer behind it, and
                // MemoryResult.Span dereferences the owner unconditionally.
                Session.WriteBulkString(value.MemoryOwner == null ? ReadOnlySpan<byte>.Empty : value.Span);
            }
        }

        /// <summary>
        /// DEBUG BLOCKASYNC seconds
        /// </summary>
        /// <remarks>
        /// Waits <c>seconds</c> without holding a thread, then replies <c>OK</c>. Behaves exactly as
        /// <c>DEBUG BLOCK</c> does; it exists to exercise the adapter on the same tests.
        /// </remarks>
        bool NetworkDebugAsyncBlock()
        {
            if (parseState.Count != 2)
            {
                return AbortWithWrongNumberOfArgumentsOrUnknownSubcommand(nameof(CmdStrings.BLOCKASYNC),
                                                                          nameof(RespCommand.DEBUG));
            }

            if (!TryGetBlockingDelay(out var milliseconds, out var errorReply))
            {
                return AbortWithErrorMessage(errorReply);
            }

            // Eligibility first, context second: everything CanParkSession reads is this session's own state
            // on this thread, so asking before building keeps the refusal path from preparing a park it
            // cannot take. TryParkSession repeats the check as a cheap guard.
            if (CanParkSession)
            {
                // One context per connection rather than one per call. A parked session parses no further
                // commands, so it has at most one operation outstanding, and the context it parked on last
                // time is free to be parked on again once nothing can still reach it -- which is what
                // TryBeginReuse decides.
                var context = cachedBlockAsyncContext;
                if (context == null || !context.TryBeginReuse())
                    cachedBlockAsyncContext = context = new DebugBlockAsyncCommandContext();

                context.Configure(milliseconds);

                if (TryParkSession(context))
                    return true;

                context.Dispose();
                cachedBlockAsyncContext = null;
            }

            // The session cannot park, so a script or transaction is driving it. Every blocking command owes
            // a non-blocking answer here, and for this one that is the reply it would have sent at the end
            // of the wait.
            WriteDirect(CmdStrings.RESP_OK);
            return true;
        }

        /// <summary>
        /// DEBUG BLOCKGETASYNC seconds key
        /// </summary>
        /// <remarks>
        /// Waits <c>seconds</c> without holding a thread, then reads <c>key</c> and replies with its value,
        /// or a null bulk string if it does not exist. Behaves exactly as <c>DEBUG BLOCKGET</c> does; it
        /// exists to show that a command written against the adapter may still drive the parked session's
        /// storage from its continuation.
        /// </remarks>
        /// <typeparam name="TGarnetApi">Storage API this command was dispatched with.</typeparam>
        /// <param name="storageApi">Storage API this command was dispatched with.</param>
        bool NetworkDebugAsyncBlockGet<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 3)
            {
                return AbortWithWrongNumberOfArgumentsOrUnknownSubcommand(nameof(CmdStrings.BLOCKGETASYNC),
                                                                          nameof(RespCommand.DEBUG));
            }

            // A transaction locks the keys its commands declare in their key specs, and DEBUG declares none
            // -- its key position depends on the subcommand. Reading inside one would therefore be unlocked,
            // and reading it through a basic context would deadlock against the transaction's own exclusive
            // lock. Refusing is the honest answer for a debug command.
            if (txnManager.state != TxnState.None)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_BLOCKGET_IN_TXN);
            }

            if (!TryGetBlockingDelay(out var milliseconds, out var errorReply))
            {
                return AbortWithErrorMessage(errorReply);
            }

            var key = parseState.GetArgSliceByRef(2);

            if (CanParkSession)
            {
                var context = cachedBlockGetAsyncContext as DebugBlockGetAsyncCommandContext<TGarnetApi>;
                if (context == null || !context.TryBeginReuse())
                    cachedBlockGetAsyncContext = context = new DebugBlockGetAsyncCommandContext<TGarnetApi>();

                context.Configure(milliseconds, key.ReadOnlySpan, ref storageApi);

                if (TryParkSession(context))
                    return true;

                context.Dispose();
                cachedBlockGetAsyncContext = null;
            }

            // Parking was refused, so something else is driving this session. The non-blocking answer for
            // this command is the read without the wait, and it runs inline like any other command.
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

        /// <summary>
        /// Parses the duration argument shared by the blocking debug commands.
        /// </summary>
        /// <param name="milliseconds">Duration in milliseconds.</param>
        /// <param name="errorReply">Error to reply with when the duration does not parse.</param>
        /// <returns>True if the duration parsed.</returns>
        bool TryGetBlockingDelay(out int milliseconds, out ReadOnlySpan<byte> errorReply)
        {
            milliseconds = 0;

            if (!parseState.TryGetDouble(1, out var seconds) || double.IsNaN(seconds) || double.IsInfinity(seconds))
            {
                errorReply = CmdStrings.RESP_ERR_TIMEOUT_NOT_VALID_FLOAT;
                return false;
            }

            if (seconds < 0)
            {
                errorReply = CmdStrings.RESP_ERR_TIMEOUT_IS_NEGATIVE;
                return false;
            }

            milliseconds = (int)Math.Min(seconds * 1000, int.MaxValue);
            errorReply = default;
            return true;
        }
    }
}