// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    internal sealed partial class RespServerSession
    {
        /// <summary>
        /// A <see cref="BlockingCommandContext"/> whose suspended state is written by the compiler instead of by
        /// hand. Implementations override <see cref="RunAsync"/> alone and write the command as straight-line
        /// code: wait, optionally read the store, resume, reply.
        /// </summary>
        /// <remarks>
        /// The base class asks a command to split itself across <see cref="BlockingCommandContext.OnStart"/>, a
        /// completion callback, <see cref="BlockingCommandContext.WriteResponse"/> and
        /// <see cref="BlockingCommandContext.OnAbort"/>, carrying everything that has to survive the wait in
        /// fields it declares itself. That split is what makes a blocking command error-prone to author: the code
        /// that decides the reply is severed from the code that writes it, and the two drift apart as the command
        /// changes.
        /// <para>
        /// This class keeps the ownership rules exactly as they are -- they are properties of the park, not of
        /// the control flow -- and removes only the hand-written state machine. The receive path is untouched:
        /// nothing here runs until a command has already parked, so ordinary traffic pays nothing for it.
        /// </para>
        /// <para>
        /// The shape of an implementation is fixed by where the park's guarantees apply:
        /// <code>
        /// protected override async ValueTask RunAsync()
        /// {
        ///     await Delay(milliseconds);                 // or any awaitable; no session state is held here
        ///     if (TryClaim() &amp;&amp; TryEnterStorage())      // claim first: it is what makes the storage ours
        ///     {
        ///         try { value = ReadTheStore(); } finally { ExitStorage(); }
        ///     }
        ///     await ResumeSession();                     // hands the rest of this method to the resume path
        ///     Session.WriteDirect(value);                // runs with the response buffer held
        /// }
        /// </code>
        /// Everything before <see cref="ResumeSession"/> runs on whatever thread the operation completed on,
        /// with the session parked. Everything after it runs on the resuming thread, inside
        /// <see cref="WriteResponse"/>, with the session's response buffer acquired and its sender lock held --
        /// which is why the tail must only serialize an outcome that already exists, never reach the store.
        /// </para>
        /// <para>
        /// <see cref="ResumeSession"/> may be awaited at most once, and the method must not suspend again after
        /// it: the resuming thread writes the reply, flushes it and disposes the context as soon as the
        /// continuation returns, so a second suspension would let all three happen before the reply was written.
        /// Debug builds report it rather than leaving a silently truncated reply stream.
        /// </para>
        /// <para>
        /// Abort is delivered as cancellation. <see cref="Token"/> is cancelled when the connection goes away,
        /// any wait started through <see cref="Delay"/> or <see cref="YieldToPool"/> is released, and
        /// <see cref="ResumeSession"/> throws so the method unwinds without writing anything. An implementation
        /// does not have to catch that: the adapter expects it.
        /// </para>
        /// </remarks>
        internal abstract class AsyncBlockingCommandContext : BlockingCommandContext, IThreadPoolWorkItem
        {
            /// <summary>
            /// Continuation of <see cref="RunAsync"/> waiting on <see cref="Delay"/> or
            /// <see cref="YieldToPool"/>. Claimed with an interlocked exchange, so the timer firing and an abort
            /// releasing the wait cannot both resume the method.
            /// </summary>
            Action pendingWait;

            /// <summary>
            /// Continuation of <see cref="RunAsync"/> waiting on <see cref="ResumeSession"/>, run by
            /// <see cref="WriteResponse"/> on the resuming thread.
            /// </summary>
            Action resumeContinuation;

            /// <summary>Cached delegate used to observe <see cref="RunAsync"/> finishing.</summary>
            Action onBodyCompleted;

            /// <summary>Timer backing <see cref="Delay"/>, kept across parks so a reused context re-arms it.</summary>
            Timer delayTimer;

            /// <summary>Cancelled by <see cref="OnAbort"/>; recycled across parks with <c>TryReset</c>.</summary>
            CancellationTokenSource cancellation;

            /// <summary>The in-flight <see cref="RunAsync"/>, awaited once through <see cref="ObserveBody"/>.</summary>
            ValueTask body;

            /// <summary>Set once this context has won <see cref="BlockingCommandContext.TryClaimOutcome"/>.</summary>
            int claimed;

            /// <summary>Set once <see cref="ResumeSession"/> has handed the session back.</summary>
            int resumed;

            /// <summary>Set once <see cref="RunAsync"/> has run to completion, however it completed.</summary>
            int bodyCompleted;

            /// <summary>
            /// The state machine box this context's last <see cref="RunAsync"/> suspended into, held so the next
            /// park reuses it. Typed as <see cref="object"/> because the box is generic over a state machine type
            /// the base class cannot name.
            /// </summary>
            object cachedStateMachineBox;

            /// <summary>
            /// The context whose <see cref="RunAsync"/> is being started on this thread, read by
            /// <see cref="ParkedValueTaskMethodBuilder.Create"/>.
            /// </summary>
            /// <remarks>
            /// The builder is created by compiler-generated code, which takes no arguments and so has no other
            /// way to find the context that owns the state machine it is about to build. Reading it from the
            /// thread is sound because the generated prologue -- create the builder, then <c>Start</c> -- runs
            /// synchronously on the thread that called <see cref="RunAsync"/>, which is only ever
            /// <see cref="OnStart"/>, and the window is closed before that returns.
            /// </remarks>
            [ThreadStatic]
            static AsyncBlockingCommandContext startingContext;

            /// <inheritdoc cref="startingContext"/>
            internal static AsyncBlockingCommandContext StartingContext => startingContext;

            /// <summary>
            /// Takes the cached state machine box, or makes one.
            /// </summary>
            /// <typeparam name="TStateMachine">State machine the compiler generated for <see cref="RunAsync"/>.</typeparam>
            /// <returns>A box with no suspended state in it.</returns>
            /// <remarks>
            /// Per context rather than per thread, which is what <see cref="PoolingAsyncValueTaskMethodBuilder"/>
            /// offers and what makes it miss here: a park is rented on the thread that finished the parse loop
            /// and given back on whichever thread completed the operation, so the two caches it keeps -- one per
            /// thread, one per core -- are looked up on a thread that never filled them. A context is pinned to
            /// one connection and runs one body at a time, so caching it here hits every time instead.
            /// </remarks>
            internal ParkedStateMachineBox<TStateMachine> RentStateMachineBox<TStateMachine>()
                where TStateMachine : IAsyncStateMachine
                => Interlocked.Exchange(ref cachedStateMachineBox, null) as ParkedStateMachineBox<TStateMachine>
                   ?? new ParkedStateMachineBox<TStateMachine>(this);

            /// <summary>
            /// Gives a state machine box back for the next park to use.
            /// </summary>
            /// <param name="box">Box whose suspended state has been consumed.</param>
            internal void ReturnStateMachineBox(object box)
                => Interlocked.Exchange(ref cachedStateMachineBox, box);

            /// <summary>
            /// Session this command parked, for the reply tail and for storage work.
            /// </summary>
            protected RespServerSession Session => Owner;

            /// <summary>
            /// Cancelled when the connection is torn down. Pass it to anything the command waits on that is not
            /// one of this class's own helpers.
            /// </summary>
            protected CancellationToken Token => cancellation?.Token ?? default;

            /// <summary>
            /// The command's body. Called once per park, on the thread that finished the parse loop's cleanup,
            /// and the only member an implementation has to supply.
            /// </summary>
            protected abstract ValueTask RunAsync();

            /// <summary>
            /// Clears implementation state so the context can be parked on again. Optional: a command whose
            /// configuration is rewritten on every park needs nothing here.
            /// </summary>
            protected virtual void OnRecycle() { }

            /// <summary>
            /// Releases implementation resources. Optional, and runs exactly once.
            /// </summary>
            protected virtual void OnRelease() { }

            /// <summary>
            /// Writes the reply for a command whose connection went away before it could resume. The default
            /// reports the abort; the reply is almost always discarded with the connection.
            /// </summary>
            /// <param name="session">Session being resumed.</param>
            protected virtual void WriteAbortedResponse(RespServerSession session)
                => session.WriteError(CmdStrings.RESP_ERR_BLOCKING_ABORTED);

            /// <summary>
            /// Takes the outcome for this command, so that nothing else can complete it.
            /// </summary>
            /// <returns>
            /// True if this command is still live and owns both the outcome and the parked session's storage.
            /// False if the connection is going away, in which case the body should return without touching the
            /// session.
            /// </returns>
            /// <remarks>
            /// Idempotent, unlike <see cref="BlockingCommandContext.TryClaimOutcome"/>, because a body that
            /// claims before reading the store then reaches <see cref="ResumeSession"/>, which claims again.
            /// </remarks>
            protected bool TryClaim()
            {
                if (Volatile.Read(ref claimed) != 0)
                    return true;

                if (!TryClaimOutcome())
                    return false;

                Volatile.Write(ref claimed, 1);
                return true;
            }

            /// <summary>
            /// Takes the parked session's storage. Must be called after <see cref="TryClaim"/> has succeeded,
            /// and paired with <see cref="ExitStorage"/> from a <c>finally</c>.
            /// </summary>
            /// <returns>True if the storage may be used.</returns>
            protected bool TryEnterStorage() => TryEnterStorageScope();

            /// <summary>
            /// Gives the parked session's storage back.
            /// </summary>
            protected void ExitStorage() => ExitStorageScope();

            /// <summary>
            /// Waits without holding a thread, releasing early if the connection is torn down.
            /// </summary>
            /// <param name="milliseconds">How long to wait. Zero hands off to the thread pool instead.</param>
            /// <remarks>
            /// Allocation-free after the first use on a connection: the timer is owned by the context and
            /// re-armed on each park, and the continuation is the state machine's own cached delegate.
            /// </remarks>
            protected WaitHandoff Delay(int milliseconds) => new(this, milliseconds);

            /// <summary>
            /// Moves the rest of the command onto a thread-pool thread without allocating.
            /// </summary>
            protected WaitHandoff YieldToPool() => new(this, 0);

            /// <summary>
            /// Hands the rest of the command to the session's resume path.
            /// </summary>
            /// <remarks>
            /// Claims the outcome, releases the session, and continues on the resuming thread inside
            /// <see cref="WriteResponse"/>. Throws <see cref="BlockingCommandAbortedException"/> instead if the
            /// connection has gone away, which unwinds the body without writing a reply.
            /// </remarks>
            protected ResumeHandoff ResumeSession() => new(this);

            protected sealed override void OnStart()
            {
                if (cancellation == null)
                {
                    // Flow is suppressed so the token's registrations do not capture the parse loop's ambient
                    // state, which a server-side wait has no use for and would allocate to carry.
                    using (ExecutionContext.SuppressFlow())
                    {
                        cancellation = new CancellationTokenSource();
                    }
                }

                // Registered before the body starts, so a context is never recycled while its state machine can
                // still reach it. Released by ObserveBody on every path the body can end on.
                AddCallback();
                CountRunningBody(1);

                // Scoped to the call rather than set once, because the generated prologue reads it while
                // building the state machine and nothing after that should see it.
                var enclosing = startingContext;
                startingContext = this;
                try
                {
                    body = RunAsync();
                }
                finally
                {
                    startingContext = enclosing;
                }

                if (body.IsCompleted)
                {
                    ObserveBody();
                    return;
                }

                body.GetAwaiter().UnsafeOnCompleted(onBodyCompleted ??= ObserveBody);
            }

            /// <summary>
            /// Observes <see cref="RunAsync"/> finishing, and stands in for a body that ended without resuming.
            /// </summary>
            /// <remarks>
            /// A body that returns without reaching <see cref="ResumeSession"/> has usually lost its claim to an
            /// abort, which already released the session. The remaining case is a command that returned early by
            /// mistake, and leaving that one parked would hold a connection open with no receive outstanding to
            /// ever notice; claiming and releasing here turns it into an error reply instead.
            /// </remarks>
            void ObserveBody()
            {
                try
                {
                    try
                    {
#pragma warning disable VSTHRD002 // Observed from the body's own completion callback; never blocks.
                        body.GetAwaiter().GetResult();
#pragma warning restore VSTHRD002
                    }
                    catch (BlockingCommandAbortedException)
                    {
                        // The connection went away while the body was waiting. Expected, and already handled by
                        // whichever thread delivered the abort.
                    }
                    catch (OperationCanceledException) when (Token.IsCancellationRequested)
                    {
                        // Same, for a body that observed the token directly.
                    }
                    catch (Exception failure)
                    {
                        Owner?.Logger?.LogError(failure, "Blocking command failed");
                    }
                }
                finally
                {
                    Volatile.Write(ref bodyCompleted, 1);
                    body = default;
                    CountRunningBody(-1);
                    CallbackCompleted();
                }

                if (Volatile.Read(ref resumed) == 0 && TryClaim())
                {
                    ReportContractViolation("A blocking command's body returned without resuming its session.");
                    ReleaseSession();
                }
            }

            protected sealed override void OnAbort()
            {
                try
                {
                    cancellation?.Cancel();
                }
                catch (Exception failure)
                {
                    Owner?.Logger?.LogWarning(failure, "Blocking command failed to cancel");
                }

                // Releases a body parked on Delay or YieldToPool so it unwinds now, rather than when a timer it
                // can no longer usefully wait for eventually fires. ResumeSession throws on the way out, because
                // the claim this abort holds is the one the body would have needed.
                ReleaseWait();
            }

            internal sealed override void WriteResponse(RespServerSession session)
            {
                var continuation = Interlocked.Exchange(ref resumeContinuation, null);
                if (continuation == null)
                {
                    WriteAbortedResponse(session);
                    return;
                }

                // Runs the tail of RunAsync here, on this thread, with the response buffer held. The reply the
                // command owes is written by the body itself rather than by a separate method that has to
                // rediscover what the body decided.
                continuation();

                AssertBodyCompletedOnResume();
            }

            /// <summary>
            /// Trips if the body suspended again after <see cref="ResumeSession"/>.
            /// </summary>
            /// <remarks>
            /// The caller writes the reply, flushes it and disposes this context the moment
            /// <see cref="WriteResponse"/> returns, so a body that is still running has lost its chance to
            /// write -- and will then touch a context that has been recycled. The symptom is a reply missing
            /// from the middle of the stream, which is worth naming where it happens.
            /// </remarks>
            [Conditional("DEBUG")]
            void AssertBodyCompletedOnResume()
            {
                if (Volatile.Read(ref bodyCompleted) == 0)
                    ReportContractViolation("A blocking command suspended again after resuming its session.");
            }

            /// <summary>
            /// Resumes a body waiting on <see cref="Delay"/> or <see cref="YieldToPool"/>, if one is waiting.
            /// </summary>
            /// <returns>True if a waiting body was resumed by this call.</returns>
            bool ReleaseWait()
            {
                var continuation = Interlocked.Exchange(ref pendingWait, null);
                if (continuation == null)
                    return false;

                try
                {
                    continuation();
                }
                finally
                {
                    CallbackCompleted();
                }

                return true;
            }

            /// <summary>
            /// Arms a wait. The continuation is claimed by whichever of the timer and an abort reaches it first.
            /// </summary>
            /// <param name="continuation">Rest of the command.</param>
            /// <param name="milliseconds">How long to wait, or zero to hand off to the thread pool.</param>
            void ArmWait(Action continuation, int milliseconds)
            {
                // Paired with the CallbackCompleted in ReleaseWait, and registered before the wait is armed so a
                // timer that fires immediately cannot outrun its own registration.
                AddCallback();
                Volatile.Write(ref pendingWait, continuation);

                if (milliseconds == 0)
                {
                    ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);
                    return;
                }

                if (delayTimer == null)
                {
                    using (ExecutionContext.SuppressFlow())
                    {
                        delayTimer = new Timer(static state => ((AsyncBlockingCommandContext)state).ReleaseWait(),
                                               this, Timeout.Infinite, Timeout.Infinite);
                    }
                }

                _ = delayTimer.Change(milliseconds, Timeout.Infinite);
            }

            void IThreadPoolWorkItem.Execute() => ReleaseWait();

            protected sealed override void OnReset()
            {
                pendingWait = null;
                resumeContinuation = null;
                body = default;
                claimed = 0;
                resumed = 0;
                bodyCompleted = 0;

                OnRecycle();
            }

            protected sealed override void OnDispose()
            {
                delayTimer?.Dispose();
                delayTimer = null;

                // Kept across a park when it can still be reset, because this runs after every park rather
                // than only when the context is retired: disposing here would make each park allocate a
                // fresh source. TryReset refuses only once the source has been cancelled, and a cancelled
                // source means the connection is going away, so nothing parks on this context again.
                if (cancellation != null && !cancellation.TryReset())
                {
                    cancellation.Dispose();
                    cancellation = null;
                }

                pendingWait = null;
                resumeContinuation = null;
                body = default;

                // cachedStateMachineBox is deliberately kept: this runs after every park, and the whole point
                // of the cache is that the next park on this connection finds it.

                OnRelease();
            }

            static int runningBodies;

            /// <summary>
            /// Bodies started and not yet finished, across the process. Always zero in release builds, where
            /// the count is not kept.
            /// </summary>
            /// <remarks>
            /// Returns to zero promptly when a connection is torn down, because the abort releases whatever
            /// wait the body was parked on rather than leaving it to a timer that can no longer do anything
            /// useful. A command told to wait for a minute therefore stops costing anything the moment its
            /// client goes away, instead of a minute later.
            /// </remarks>
            internal static int RunningBodies => Volatile.Read(ref runningBodies);

            [Conditional("DEBUG")]
            static void CountRunningBody(int delta) => Interlocked.Add(ref runningBodies, delta);

            /// <summary>
            /// Awaitable returned by <see cref="Delay"/> and <see cref="YieldToPool"/>.
            /// </summary>
            protected readonly struct WaitHandoff
            {
                readonly AsyncBlockingCommandContext context;
                readonly int milliseconds;

                internal WaitHandoff(AsyncBlockingCommandContext context, int milliseconds)
                {
                    this.context = context;
                    this.milliseconds = milliseconds;
                }

                /// <summary>Gets the awaiter for this wait.</summary>
                public Awaiter GetAwaiter() => new(context, milliseconds);

                /// <summary>
                /// Suspends the command until the wait elapses or the connection is torn down.
                /// </summary>
                public readonly struct Awaiter : ICriticalNotifyCompletion
                {
                    readonly AsyncBlockingCommandContext context;
                    readonly int milliseconds;

                    internal Awaiter(AsyncBlockingCommandContext context, int milliseconds)
                    {
                        this.context = context;
                        this.milliseconds = milliseconds;
                    }

                    /// <summary>
                    /// Always false: the point of the wait is to give up the thread, and a wait that completed
                    /// inline would run the rest of the command on the thread that started it.
                    /// </summary>
                    public bool IsCompleted => false;

                    /// <summary>Completes the wait.</summary>
                    public void GetResult() { }

                    /// <inheritdoc />
                    public void OnCompleted(Action continuation) => context.ArmWait(continuation, milliseconds);

                    /// <inheritdoc />
                    public void UnsafeOnCompleted(Action continuation) => context.ArmWait(continuation, milliseconds);
                }
            }

            /// <summary>
            /// Awaitable returned by <see cref="ResumeSession"/>.
            /// </summary>
            protected readonly struct ResumeHandoff
            {
                readonly AsyncBlockingCommandContext context;

                internal ResumeHandoff(AsyncBlockingCommandContext context) => this.context = context;

                /// <summary>
                /// Gets the awaiter for the resume, taking the outcome as it does so.
                /// </summary>
                /// <remarks>
                /// The claim happens here because the compiler calls this exactly once per <c>await</c>, before
                /// it asks whether to suspend. Losing it means an abort got here first, and the awaiter then
                /// completes immediately so that <see cref="Awaiter.GetResult"/> can unwind the body.
                /// </remarks>
                public Awaiter GetAwaiter() => new(context, context.TryClaim());

                /// <summary>
                /// Suspends the command until the session resumes, and continues inside
                /// <see cref="WriteResponse"/>.
                /// </summary>
                public readonly struct Awaiter : ICriticalNotifyCompletion
                {
                    readonly AsyncBlockingCommandContext context;
                    readonly bool owned;

                    internal Awaiter(AsyncBlockingCommandContext context, bool owned)
                    {
                        this.context = context;
                        this.owned = owned;
                    }

                    /// <summary>
                    /// True only when the outcome was lost, so that the body resumes immediately and throws
                    /// rather than parking on a session that has already been released.
                    /// </summary>
                    public bool IsCompleted => !owned;

                    /// <summary>
                    /// Completes the resume, or reports that the connection went away.
                    /// </summary>
                    public void GetResult()
                    {
                        if (!owned)
                            ThrowAborted();
                    }

                    /// <inheritdoc />
                    public void OnCompleted(Action continuation) => UnsafeOnCompleted(continuation);

                    /// <inheritdoc />
                    public void UnsafeOnCompleted(Action continuation)
                    {
                        // Published before the release, because the release can resume the session on another
                        // thread immediately and WriteResponse reads this.
                        Volatile.Write(ref context.resumeContinuation, continuation);
                        Volatile.Write(ref context.resumed, 1);
                        context.ReleaseSession();
                    }

                    [MethodImpl(MethodImplOptions.NoInlining)]
                    static void ThrowAborted() => throw new BlockingCommandAbortedException();
                }
            }
        }
    }

    /// <summary>
    /// The non-generic face of <see cref="ParkedStateMachineBox{TStateMachine}"/>, so
    /// <see cref="ParkedValueTaskMethodBuilder"/> can complete a box without naming the state machine inside it.
    /// </summary>
    internal interface IParkedStateMachineBox : IValueTaskSource
    {
        /// <summary>Token identifying the current use of this box.</summary>
        short Version { get; }

        /// <summary>Completes the body successfully.</summary>
        void SetResult();

        /// <summary>Completes the body with a failure.</summary>
        /// <param name="error">Exception the body ended on.</param>
        void SetException(Exception error);
    }

    /// <summary>
    /// The suspended state of a blocking command's body, owned by the command's context rather than by the
    /// thread that happened to suspend it.
    /// </summary>
    /// <typeparam name="TStateMachine">State machine the compiler generated for the body.</typeparam>
    /// <remarks>
    /// This is the object every <c>async</c> method needs once it suspends, and the only per-park allocation the
    /// pattern would otherwise have. Keeping it on the context -- which is itself reused for the life of a
    /// connection -- is what makes a park allocate nothing at all.
    /// </remarks>
    internal sealed class ParkedStateMachineBox<TStateMachine> : IParkedStateMachineBox
        where TStateMachine : IAsyncStateMachine
    {
        /// <summary>
        /// The suspended body. A field rather than a property because the compiler mutates it in place through
        /// <see cref="IAsyncStateMachine.MoveNext"/>; copying it out would lose the progress the body made.
        /// </summary>
        public TStateMachine StateMachine;

        ManualResetValueTaskSourceCore<bool> core;

        Action moveNext;

        readonly RespServerSession.AsyncBlockingCommandContext owner;

        /// <summary>
        /// Creates a box.
        /// </summary>
        /// <param name="owner">Context to give the box back to, or null if it is not to be reused.</param>
        internal ParkedStateMachineBox(RespServerSession.AsyncBlockingCommandContext owner)
        {
            this.owner = owner;

            // The body continues on whichever thread completed the operation, which is the behaviour the park
            // already has: the alternative queues a second hop for every resume.
            core.RunContinuationsAsynchronously = false;
        }

        /// <summary>
        /// Delegate that resumes the body, created once and reused for the life of the box.
        /// </summary>
        internal Action MoveNextAction => moveNext ??= Resume;

        void Resume() => StateMachine.MoveNext();

        /// <inheritdoc />
        public short Version => core.Version;

        /// <inheritdoc />
        public void SetResult() => core.SetResult(true);

        /// <inheritdoc />
        public void SetException(Exception error) => core.SetException(error);

        /// <inheritdoc />
        public ValueTaskSourceStatus GetStatus(short token) => core.GetStatus(token);

        /// <inheritdoc />
        public void OnCompleted(Action<object> continuation, object state, short token,
                                ValueTaskSourceOnCompletedFlags flags)
            => core.OnCompleted(continuation, state, token, flags);

        /// <inheritdoc />
        /// <remarks>
        /// Reads the body's outcome and recycles the box in one step, which is sound because the body is awaited
        /// exactly once: the adapter observes it and nothing else holds the <see cref="ValueTask"/>.
        /// </remarks>
        public void GetResult(short token)
        {
            try
            {
                core.GetResult(token);
            }
            finally
            {
                // Reset first, so the box is pristine before anything can take it, and invalidate the token
                // against a second read of the task that has just been consumed.
                core.Reset();
                StateMachine = default;
                owner?.ReturnStateMachineBox(this);
            }
        }
    }

    /// <summary>
    /// Async method builder for a blocking command's body, which takes its state machine box from the command's
    /// context instead of from a thread-local cache.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Applied with <see cref="AsyncMethodBuilderAttribute"/> to a
    /// <see cref="RespServerSession.AsyncBlockingCommandContext.RunAsync"/> override. It differs from
    /// <see cref="PoolingAsyncValueTaskMethodBuilder"/> in two ways, both of which matter here and neither of
    /// which a general-purpose builder could assume.
    /// </para>
    /// <para>
    /// The box comes from the context, so it is reused whenever the context is -- that is, for the whole life of
    /// a connection that parks repeatedly. The pooling builder caches per thread and per core instead, and a
    /// park is rented on the thread that finished parsing and released on whichever thread completed the
    /// operation, so those caches are consulted on threads that never filled them.
    /// </para>
    /// <para>
    /// <see cref="ExecutionContext"/> is not captured or restored. A body resumes on an arbitrary server thread
    /// by design and reads nothing ambient; the context suppresses flow around the state it does create for the
    /// same reason.
    /// </para>
    /// </remarks>
    internal struct ParkedValueTaskMethodBuilder
    {
        IParkedStateMachineBox box;

        RespServerSession.AsyncBlockingCommandContext owner;

        Exception failure;

        short token;

        /// <summary>
        /// Creates a builder, binding it to the context whose body is being started on this thread.
        /// </summary>
        /// <returns>The builder.</returns>
        public static ParkedValueTaskMethodBuilder Create()
            => new() { owner = RespServerSession.AsyncBlockingCommandContext.StartingContext };

        /// <summary>
        /// Runs the body up to its first suspension.
        /// </summary>
        /// <typeparam name="TStateMachine">State machine type.</typeparam>
        /// <param name="stateMachine">State machine to run.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Start<TStateMachine>(ref TStateMachine stateMachine)
            where TStateMachine : IAsyncStateMachine
            => stateMachine.MoveNext();

        /// <summary>
        /// Unused: the box owns the state machine from its first suspension onwards.
        /// </summary>
        /// <param name="stateMachine">State machine the compiler is handing over.</param>
        public readonly void SetStateMachine(IAsyncStateMachine stateMachine)
            => ArgumentNullException.ThrowIfNull(stateMachine);

        /// <summary>
        /// Completes the body successfully.
        /// </summary>
        public readonly void SetResult() => box?.SetResult();

        /// <summary>
        /// Completes the body with a failure.
        /// </summary>
        /// <param name="exception">Exception the body ended on.</param>
        public void SetException(Exception exception)
        {
            if (box == null)
                failure = exception;
            else
                box.SetException(exception);
        }

        /// <summary>
        /// The body, as something the adapter can observe.
        /// </summary>
        public readonly ValueTask Task
        {
            get
            {
                if (box != null)
                    return new ValueTask(box, token);

                return failure == null ? default : ValueTask.FromException(failure);
            }
        }

        /// <summary>
        /// Suspends the body on an awaiter that flows <see cref="ExecutionContext"/>.
        /// </summary>
        /// <typeparam name="TAwaiter">Awaiter type.</typeparam>
        /// <typeparam name="TStateMachine">State machine type.</typeparam>
        /// <param name="awaiter">Awaiter to resume from.</param>
        /// <param name="stateMachine">State machine to suspend.</param>
        public void AwaitOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
            where TAwaiter : INotifyCompletion
            where TStateMachine : IAsyncStateMachine
            => awaiter.OnCompleted(Suspend(ref stateMachine));

        /// <summary>
        /// Suspends the body on an awaiter that does not flow <see cref="ExecutionContext"/>.
        /// </summary>
        /// <typeparam name="TAwaiter">Awaiter type.</typeparam>
        /// <typeparam name="TStateMachine">State machine type.</typeparam>
        /// <param name="awaiter">Awaiter to resume from.</param>
        /// <param name="stateMachine">State machine to suspend.</param>
        public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter,
                                                                    ref TStateMachine stateMachine)
            where TAwaiter : ICriticalNotifyCompletion
            where TStateMachine : IAsyncStateMachine
            => awaiter.UnsafeOnCompleted(Suspend(ref stateMachine));

        /// <summary>
        /// Moves the body into its box, the first time it suspends.
        /// </summary>
        /// <typeparam name="TStateMachine">State machine type.</typeparam>
        /// <param name="stateMachine">State machine to suspend.</param>
        /// <returns>The delegate that resumes the body.</returns>
        /// <remarks>
        /// The order is load-bearing for a state machine that is a struct, which is what optimized builds emit.
        /// This builder is a field of the state machine being copied, so the box and the token have to be
        /// published into it before the copy is taken, or the copy the box runs from would have neither and
        /// every later suspension would take a second box.
        /// </remarks>
        Action Suspend<TStateMachine>(ref TStateMachine stateMachine)
            where TStateMachine : IAsyncStateMachine
        {
            if (box is ParkedStateMachineBox<TStateMachine> existing)
                return existing.MoveNextAction;

            var created = owner?.RentStateMachineBox<TStateMachine>()
                          ?? new ParkedStateMachineBox<TStateMachine>(null);

            box = created;
            token = created.Version;
            created.StateMachine = stateMachine;

            return created.MoveNextAction;
        }
    }

    /// <summary>
    /// Raised inside a blocking command's body when its connection goes away, to unwind it without writing a
    /// reply. Expected, and swallowed by <see cref="RespServerSession.AsyncBlockingCommandContext"/>.
    /// </summary>
    internal sealed class BlockingCommandAbortedException : Exception
    {
        /// <summary>
        /// Creates the exception.
        /// </summary>
        internal BlockingCommandAbortedException()
            : base("The connection was torn down while a blocking command was waiting.")
        {
        }
    }
}