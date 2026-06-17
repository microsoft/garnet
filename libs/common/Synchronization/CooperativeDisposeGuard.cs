// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// A lock-free, non-blocking dispose guard for protecting a critical section against
    /// concurrent disposal without blocking the disposing thread.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This guard returns immediately from <see cref="TryDispose"/> and defers cleanup to
    /// the worker's exit path. This is useful when both the worker and the disposer run on
    /// I/O or networking threads where blocking is undesirable.
    /// </para>
    /// <para>
    /// This guard assumes a single concurrent worker. If multiple workers may be active
    /// simultaneously, use <see cref="ActiveWorkerMonitor"/> instead.
    /// </para>
    /// <para>
    /// <b>Usage example:</b>
    /// <code>
    /// struct MySession : IDisposable
    /// {
    ///     CooperativeDisposeGuard disposeGuard;
    ///
    ///     public void ProcessRecord(...)
    ///     {
    ///         if (!disposeGuard.TryEnter())
    ///             throw new ObjectDisposedException(...);
    ///         try
    ///         {
    ///             // ... do work ...
    ///         }
    ///         finally
    ///         {
    ///             if (disposeGuard.ExitAndCheckShouldCleanup())
    ///                 Cleanup(); // Dispose was called while we were processing
    ///         }
    ///     }
    ///
    ///     public void Dispose()
    ///     {
    ///         if (disposeGuard.TryDispose() == DisposeResult.CleanupNow)
    ///             Cleanup();
    ///         // DeferredToWorker: ExitAndCheckShouldCleanup() will trigger cleanup
    ///         // AlreadyDisposed: nothing to do
    ///     }
    /// }
    /// </code>
    /// </para>
    /// </remarks>
    public struct CooperativeDisposeGuard
    {
        /// <summary>
        /// Result of calling <see cref="TryDispose"/>.
        /// </summary>
        public enum DisposeResult
        {
            /// <summary>The guard was already disposed by a prior call.</summary>
            AlreadyDisposed,

            /// <summary>Disposal was marked, but a worker is in-flight. Cleanup is deferred
            /// to the worker's exit path via <see cref="ExitAndCheckShouldCleanup"/>.</summary>
            DeferredToWorker,

            /// <summary>Disposal was marked and no worker is active. The caller should perform cleanup now.</summary>
            CleanupNow,
        }

        private const int FLAG_ACTIVE = 1 << 0;
        private const int FLAG_DISPOSED = 1 << 1;

        private int state;

        /// <summary>
        /// Returns <c>true</c> if <see cref="TryDispose"/> has been called.
        /// </summary>
        public readonly bool IsDisposed => (Volatile.Read(in state) & FLAG_DISPOSED) != 0;

        /// <summary>
        /// Atomically marks the guard as active and returns whether entry is allowed.
        /// Returns <c>false</c> if the guard has already been disposed.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryEnter()
        {
            var prev = Interlocked.Or(ref state, FLAG_ACTIVE);
            return (prev & FLAG_DISPOSED) == 0;
        }

        /// <summary>
        /// Atomically clears the active flag and returns whether the guard was disposed
        /// while the caller was inside the critical section. When this returns <c>true</c>,
        /// the caller is responsible for performing deferred cleanup.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ExitAndCheckShouldCleanup()
        {
            var prev = Interlocked.And(ref state, ~FLAG_ACTIVE);
            return (prev & FLAG_DISPOSED) != 0;
        }

        /// <summary>
        /// Atomically marks the guard as disposed and returns a <see cref="DisposeResult"/>
        /// indicating what the caller should do.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public DisposeResult TryDispose()
        {
            var prev = Interlocked.Or(ref state, FLAG_DISPOSED);
            if ((prev & FLAG_DISPOSED) != 0)
                return DisposeResult.AlreadyDisposed;
            return (prev & FLAG_ACTIVE) != 0
                ? DisposeResult.DeferredToWorker
                : DisposeResult.CleanupNow;
        }
    }
}