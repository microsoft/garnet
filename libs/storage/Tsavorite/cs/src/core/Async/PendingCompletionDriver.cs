// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace Tsavorite.core
{
    /// <summary>
    /// Non-generic base so a session can hold one pool per session functions wrapper type it uses.
    /// </summary>
    internal abstract class PendingCompletionDriverPool
    {
        /// <summary>The next pool held by the same session, for a different wrapper type.</summary>
        internal PendingCompletionDriverPool Next;
    }

    /// <summary>
    /// Per-session supply of <see cref="PendingCompletionDriver{TInput, TOutput, TContext, TStoreFunctions, TAllocator, TSessionFunctionsWrapper}"/>,
    /// which drive the pending-completion loop for one session functions wrapper type.
    /// </summary>
    /// <remarks>
    /// A session reaches the loop on every read that misses memory, so the drivers are pooled rather than
    /// allocated per call. The pool is an intrusive free list and is not thread-safe, matching the rest of
    /// <see cref="ClientSession{TKey, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator}"/>.
    /// It holds as many drivers as the session nests completions, which is two: resuming a session runs its
    /// continuation inline, and that continuation can issue the next read and park again before the driver
    /// that resumed it is back on the list.
    /// </remarks>
    internal sealed class PendingCompletionDriverPool<TInput, TOutput, TContext, TStoreFunctions, TAllocator, TSessionFunctionsWrapper> : PendingCompletionDriverPool
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
        where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
    {
        readonly TsavoriteKV<TStoreFunctions, TAllocator> store;
        PendingCompletionDriver<TInput, TOutput, TContext, TStoreFunctions, TAllocator, TSessionFunctionsWrapper> free;

        internal PendingCompletionDriverPool(TsavoriteKV<TStoreFunctions, TAllocator> store) => this.store = store;

        /// <summary>
        /// Completes the session's pending requests, collecting their outputs into <paramref name="completedOutputs"/>
        /// when it is non-null, and suspends rather than blocking while any of them are still in flight.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<CompletedOutputIterator<TInput, TOutput, TContext>> RunAsync(TSessionFunctionsWrapper sessionFunctions,
                CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs, CancellationToken token)
            => Rent().Run(sessionFunctions, completedOutputs, token);

        /// <summary>
        /// As <see cref="RunAsync"/>, for callers that do not read the outputs back.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask RunWithoutResultAsync(TSessionFunctionsWrapper sessionFunctions,
                CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs, CancellationToken token)
            => Rent().RunWithoutResult(sessionFunctions, completedOutputs, token);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        PendingCompletionDriver<TInput, TOutput, TContext, TStoreFunctions, TAllocator, TSessionFunctionsWrapper> Rent()
        {
            var driver = free;
            if (driver is null)
                return new PendingCompletionDriver<TInput, TOutput, TContext, TStoreFunctions, TAllocator, TSessionFunctionsWrapper>(this, store);

            free = driver.NextFree;
            driver.NextFree = null;
            return driver;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Return(PendingCompletionDriver<TInput, TOutput, TContext, TStoreFunctions, TAllocator, TSessionFunctionsWrapper> driver)
        {
            driver.NextFree = free;
            free = driver;
        }
    }

    /// <summary>
    /// Drives <see cref="TsavoriteKV{TStoreFunctions, TAllocator}"/>'s drain-and-wait loop for pending requests
    /// as a reusable <see cref="IValueTaskSource"/> rather than an async state machine, so a session that parks
    /// on a device read allocates nothing for the suspension itself.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The loop is the one an <c>async</c> method would compile to: drain whatever the device has completed,
    /// ask the session's execution context to wait for the rest, and resume at the top. Written by hand it
    /// costs one pooled object and one cached <see cref="Action"/> for the lifetime of a session, instead of a
    /// state machine box per miss.
    /// </para>
    /// <para>
    /// Continuations run inline on the thread that finishes the loop. That thread is already a thread pool
    /// thread rather than a device completion thread - the wait this drives hands off before resuming - so
    /// resuming the session here costs no further hop and cannot occupy the thread its next read needs.
    /// Running inline means the session can issue its next read, and park again, while this driver is still
    /// inside <c>SetResult</c>; the driver therefore returns itself to the pool only once that call has
    /// returned, so a nested park takes a different driver instead of resetting one that is still live.
    /// </para>
    /// </remarks>
    internal sealed class PendingCompletionDriver<TInput, TOutput, TContext, TStoreFunctions, TAllocator, TSessionFunctionsWrapper>
            : IValueTaskSource<CompletedOutputIterator<TInput, TOutput, TContext>>, IValueTaskSource, IThreadPoolWorkItem
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
        where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
    {
        readonly PendingCompletionDriverPool<TInput, TOutput, TContext, TStoreFunctions, TAllocator, TSessionFunctionsWrapper> pool;
        readonly TsavoriteKV<TStoreFunctions, TAllocator> store;

        /// <summary>Resumption for the cancellable wait, which cannot resume a work item; created on first use.</summary>
        Action resumeAction;

        ManualResetValueTaskSourceCore<CompletedOutputIterator<TInput, TOutput, TContext>> core;

        /// <summary>Links this driver into its pool's free list; null while the driver is in use.</summary>
        internal PendingCompletionDriver<TInput, TOutput, TContext, TStoreFunctions, TAllocator, TSessionFunctionsWrapper> NextFree;

        TSessionFunctionsWrapper sessionFunctions;
        CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs;
        CancellationToken token;
        ConfiguredValueTaskAwaitable.ConfiguredValueTaskAwaiter pendingWait;

        internal PendingCompletionDriver(PendingCompletionDriverPool<TInput, TOutput, TContext, TStoreFunctions, TAllocator, TSessionFunctionsWrapper> pool,
                TsavoriteKV<TStoreFunctions, TAllocator> store)
        {
            this.pool = pool;
            this.store = store;

            // The thread that finishes the loop is already a thread pool thread, so there is nothing to gain
            // from queueing the continuation to another one.
            core = new ManualResetValueTaskSourceCore<CompletedOutputIterator<TInput, TOutput, TContext>> { RunContinuationsAsynchronously = false };
        }

        internal ValueTask<CompletedOutputIterator<TInput, TOutput, TContext>> Run(TSessionFunctionsWrapper sessionFunctions,
                CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs, CancellationToken token)
        {
            var version = Begin(sessionFunctions, completedOutputs, token);
            if (Drive())
                return new ValueTask<CompletedOutputIterator<TInput, TOutput, TContext>>(this, version);

            // Everything was already complete, so nothing was published through the core and the driver is
            // immediately reusable.
            var outputs = this.completedOutputs;
            ReleaseAndReturn();
            return new ValueTask<CompletedOutputIterator<TInput, TOutput, TContext>>(outputs);
        }

        internal ValueTask RunWithoutResult(TSessionFunctionsWrapper sessionFunctions,
                CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs, CancellationToken token)
        {
            var version = Begin(sessionFunctions, completedOutputs, token);
            if (Drive())
                return new ValueTask(this, version);

            ReleaseAndReturn();
            return default;
        }

        /// <summary>
        /// Arms the driver for one run and returns the version a parked task has to carry. Drive can complete
        /// the core on another thread before it returns, so the version is read before the loop starts.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        short Begin(TSessionFunctionsWrapper sessionFunctions, CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs, CancellationToken token)
        {
            this.sessionFunctions = sessionFunctions;
            this.completedOutputs = completedOutputs;
            this.token = token;

            core.Reset();
            return core.Version;
        }

        /// <summary>
        /// Drains completed requests and waits for the rest, returning true once the wait has been handed to a
        /// continuation and false when there is nothing left pending.
        /// </summary>
        bool Drive()
        {
            while (true)
            {
                sessionFunctions.UnsafeResumeThread();
                try
                {
                    store.InternalCompletePendingRequests(sessionFunctions, completedOutputs);
                }
                finally
                {
                    sessionFunctions.UnsafeSuspendThread();
                }

                if (!token.CanBeCanceled)
                {
                    // Park with no task at all: the queue resumes this driver as a thread pool work item.
                    if (sessionFunctions.Ctx.TryWaitPending(this))
                        return true;
                }
                else
                {
                    var wait = sessionFunctions.Ctx.WaitPendingAsync(token);
                    if (!wait.IsCompleted)
                    {
                        pendingWait = wait.ConfigureAwait(false).GetAwaiter();
                        pendingWait.UnsafeOnCompleted(resumeAction ??= ResumeAfterWait);
                        return true;
                    }

                    wait.GetAwaiter().GetResult();
                }

                if (sessionFunctions.Ctx.HasNoPendingRequests)
                    return false;

                Thread.Yield();
            }
        }

        /// <summary>Resumes after a cancellable wait, which has to be observed for its outcome.</summary>
        void ResumeAfterWait()
        {
            try
            {
                pendingWait.GetResult();
                pendingWait = default;
            }
            catch (Exception ex)
            {
                Fault(ex);
                return;
            }

            Resume();
        }

        void IThreadPoolWorkItem.Execute() => Resume();

        void Resume()
        {
            CompletedOutputIterator<TInput, TOutput, TContext> outputs;
            try
            {
                if (!sessionFunctions.Ctx.HasNoPendingRequests)
                {
                    Thread.Yield();
                    if (Drive())
                        return;
                }

                outputs = completedOutputs;
            }
            catch (Exception ex)
            {
                Fault(ex);
                return;
            }

            Release();
            core.SetResult(outputs);

            // SetResult has returned, so any continuation it ran inline - which may itself have parked on
            // another driver - is finished with this one.
            pool.Return(this);
        }

        void Fault(Exception ex)
        {
            Release();
            core.SetException(ex);
            pool.Return(this);
        }

        /// <summary>Drops references the driver no longer needs so a pooled driver does not root them.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void Release()
        {
            sessionFunctions = default;
            completedOutputs = null;
            token = default;
            pendingWait = default;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void ReleaseAndReturn()
        {
            Release();
            pool.Return(this);
        }

        public CompletedOutputIterator<TInput, TOutput, TContext> GetResult(short token) => core.GetResult(token);

        public ValueTaskSourceStatus GetStatus(short token) => core.GetStatus(token);

        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
            => core.OnCompleted(continuation, state, token, flags);

        void IValueTaskSource.GetResult(short token) => _ = core.GetResult(token);
    }
}