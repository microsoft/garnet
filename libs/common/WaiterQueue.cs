// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.common
{
    /// <summary>
    /// Tracks active resource usage for a <see cref="WaiterQueue{TRequest}"/>.
    /// </summary>
    /// <typeparam name="TRequest">Resource request type.</typeparam>
    public interface IResourceTracker<TRequest>
    {
        /// <summary>
        /// Validates that a request can eventually be admitted. Permanently invalid requests must throw rather
        /// than enter the wait queue.
        /// </summary>
        /// <param name="requestResource">Resource request.</param>
        void Validate(in TRequest requestResource);

        /// <summary>
        /// Attempts to reserve the requested resource. A successful call must update active resource accounting;
        /// false means the valid request is temporarily unable to proceed. This method must be thread-safe and
        /// may run concurrently with <see cref="Release"/>.
        /// </summary>
        /// <param name="requestResource">Resource request.</param>
        /// <returns>True when the resource was reserved.</returns>
        bool TryReserve(in TRequest requestResource);

        /// <summary>
        /// Releases a previously reserved resource. This method must be thread-safe and may run concurrently
        /// with <see cref="TryReserve"/>.
        /// </summary>
        /// <param name="requestResource">Resource request originally admitted.</param>
        void Release(in TRequest requestResource);
    }

    /// <summary>
    /// FIFO admission queue for requests governed by a custom resource tracker.
    /// </summary>
    /// <remarks>
    /// Direct admission and bounded retries do not acquire the queue lock. A request that remains blocked then
    /// attempts to enter a fixed-capacity FIFO log represented by monotonically increasing logical addresses
    /// managed by <see cref="LightBoundedFifoQueue{T}"/>.
    /// </remarks>
    /// <typeparam name="TRequest">Resource request type.</typeparam>
    public sealed class WaiterQueue<TRequest> : IDisposable
    {
        /// <summary>
        /// Default number of lock-free spin iterations before a request is enqueued.
        /// </summary>
        public const int DefaultSpinCount = 10;

        /// <summary>
        /// Default maximum number of ring-enqueue spin iterations.
        /// </summary>
        public const int DefaultMaxEnqueueSpinCount = 10;

        /// <summary>
        /// Default number of waiter slots in each ring page.
        /// </summary>
        public const int DefaultRingPageSize = 32;

        /// <summary>
        /// Default number of pages in the waiter ring.
        /// </summary>
        public const int DefaultRingPageCount = 4;

        sealed class Waiter
        {
            enum WaiterState
            {
                Pending,
                Granted,
                Canceled,
                Disposed,
            }

            readonly TaskCompletionSource<bool> signal;
            readonly WaiterQueue<TRequest> owner;
            readonly CancellationToken cancellationToken;
            CancellationTokenRegistration cancellationRegistration;
            int state;

            internal readonly TRequest requestResource;

            internal Waiter(WaiterQueue<TRequest> owner, in TRequest requestResource, CancellationToken cancellationToken)
            {
                this.owner = owner;
                this.requestResource = requestResource;
                this.cancellationToken = cancellationToken;
                signal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                if (cancellationToken.CanBeCanceled)
                    cancellationRegistration = cancellationToken.UnsafeRegister(static state => ((Waiter)state).Cancel(), this);
            }

            internal Task<bool> Task => signal.Task;

            internal bool IsCanceled => Volatile.Read(ref state) == (int)WaiterState.Canceled;

            void Cancel()
            {
                if (Interlocked.CompareExchange(ref state, (int)WaiterState.Canceled, (int)WaiterState.Pending) !=
                    (int)WaiterState.Pending)
                    return;

                signal.TrySetException(new OperationCanceledException(cancellationToken));
                owner.Drain();
            }

            internal bool TryGrant()
            {
                if (Interlocked.CompareExchange(ref state, (int)WaiterState.Granted, (int)WaiterState.Pending) !=
                    (int)WaiterState.Pending)
                    return false;

                signal.TrySetResult(true);
                return true;
            }

            internal void TryDispose(Exception exception)
            {
                if (Interlocked.CompareExchange(ref state, (int)WaiterState.Disposed, (int)WaiterState.Pending) ==
                    (int)WaiterState.Pending)
                    signal.TrySetException(exception);
            }

            internal void Release()
            {
                cancellationRegistration.Dispose();
                cancellationRegistration = default;
            }

            internal void Abort()
            {
                Interlocked.CompareExchange(ref state, (int)WaiterState.Disposed, (int)WaiterState.Pending);
                Release();
            }
        }

        readonly IResourceTracker<TRequest> tracker;
        readonly LightBoundedFifoQueue<Waiter> waiterQueue;
        readonly int spinCount;

        int drainWork;
        int disposed;

        /// <summary>
        /// Number of requests currently waiting for admission.
        /// </summary>
        public int WaiterCount => waiterQueue.Count;

        /// <summary>
        /// Creates a FIFO waiter queue over a custom resource tracker.
        /// </summary>
        /// <param name="tracker">Resource tracker used for reservation and release.</param>
        /// <param name="ringPageSize">Number of waiter slots in each ring page. Must be a power of two.</param>
        /// <param name="ringPageCount">Number of pages in the waiter ring. Must be a power of two.</param>
        /// <param name="spinCount">Lock-free spin iterations before enqueueing and parking.</param>
        /// <param name="maxEnqueueSpinCount">Maximum spin iterations while reserving a bounded FIFO slot.</param>
        public WaiterQueue(
            IResourceTracker<TRequest> tracker,
            int ringPageSize = DefaultRingPageSize,
            int ringPageCount = DefaultRingPageCount,
            int spinCount = DefaultSpinCount,
            int maxEnqueueSpinCount = DefaultMaxEnqueueSpinCount)
        {
            ArgumentNullException.ThrowIfNull(tracker);
            ArgumentOutOfRangeException.ThrowIfNegative(spinCount);
            ArgumentOutOfRangeException.ThrowIfNegative(maxEnqueueSpinCount);

            this.tracker = tracker;
            this.spinCount = spinCount;
            this.waiterQueue = new LightBoundedFifoQueue<Waiter>(
                ringPageSize,
                ringPageCount,
                maxEnqueueSpinCount);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0)
                return;

            List<Exception> exceptions = null;
            var drainAcquired = false;

            try
            {
                waiterQueue.CompleteAdding();

                var spinner = new SpinWait();
                while (Interlocked.CompareExchange(ref drainWork, 1, 0) != 0)
                    spinner.SpinOnce();
                drainAcquired = true;

                var disposeException = new ObjectDisposedException(nameof(WaiterQueue<>));
                while (waiterQueue.TryDequeue(out var waiter))
                {
                    try
                    {
                        waiter.TryDispose(disposeException);
                    }
                    catch (Exception ex)
                    {
                        (exceptions ??= []).Add(ex);
                    }

                    try
                    {
                        waiter.Release();
                    }
                    catch (Exception ex)
                    {
                        (exceptions ??= []).Add(ex);
                    }
                }
            }
            finally
            {
                if (drainAcquired)
                    Volatile.Write(ref drainWork, 0);
                try
                {
                    waiterQueue.Dispose();
                }
                catch (Exception ex)
                {
                    (exceptions ??= []).Add(ex);
                }
            }

            if (exceptions != null)
                throw new AggregateException("One or more waiters failed during disposal.", exceptions);
        }

        /// <summary>
        /// Admits a request in FIFO order, yielding asynchronously if it cannot be granted during the spin phase.
        /// </summary>
        /// <param name="requestResource">Resource request.</param>
        /// <param name="token">Token used to cancel the wait.</param>
        /// <returns>A task whose result is false when bounded waiter-slot reservation fails; otherwise true after admission.</returns>
        public ValueTask<bool> AdmitAsync(in TRequest requestResource, CancellationToken token = default)
        {
            tracker.Validate(requestResource);
            token.ThrowIfCancellationRequested();
            ThrowIfDisposed();

            if (TryReserveFast(requestResource, token))
                return ValueTask.FromResult(true);

            var waitTask = TryEnqueueWaiterAsync(requestResource, token);
            if (waitTask == null)
                return ValueTask.FromResult(false);

            Drain();
            return new ValueTask<bool>(waitTask);
        }

        /// <summary>
        /// Admits a request in FIFO order, blocking if it cannot be granted during the spin phase.
        /// </summary>
        /// <param name="requestResource">Resource request.</param>
        /// <param name="token">Token used to cancel the wait.</param>
        /// <returns>False when bounded waiter-slot reservation fails; otherwise true after admission.</returns>
        public bool Admit(in TRequest requestResource, CancellationToken token = default)
        {
            tracker.Validate(requestResource);
            token.ThrowIfCancellationRequested();
            ThrowIfDisposed();

            if (TryReserveFast(requestResource, token))
                return true;

            var waitTask = TryEnqueueWaiterAsync(requestResource, token);
            if (waitTask == null)
                return false;

            Drain();
#pragma warning disable VSTHRD002 // The synchronous admission contract blocks until the waiter signal is set.
            return waitTask.GetAwaiter().GetResult();
#pragma warning restore VSTHRD002
        }

        bool TryReserveFast(in TRequest requestResource, CancellationToken token)
        {
            if (tracker.TryReserve(requestResource))
                return true;

            var spinner = new SpinWait();
            for (var attempt = 0; attempt < spinCount; attempt++)
            {
                token.ThrowIfCancellationRequested();
                ThrowIfDisposed();
                spinner.SpinOnce();
                if (tracker.TryReserve(requestResource))
                    return true;
            }

            return false;
        }

        Task<bool> TryEnqueueWaiterAsync(in TRequest requestResource, CancellationToken token)
        {
            ThrowIfDisposed();
            token.ThrowIfCancellationRequested();

            var waiter = new Waiter(this, requestResource, token);
            var waitTask = waiter.Task;
            try
            {
                if (!waiterQueue.TryEnqueue(waiter))
                {
                    waiter.Abort();
                    return null;
                }
            }
            catch
            {
                waiter.Abort();
                throw;
            }

            return waitTask;
        }

        /// <summary>
        /// Releases a previously admitted request and drains newly available capacity.
        /// </summary>
        /// <param name="requestResource">Resource request originally admitted.</param>
        public void Release(in TRequest requestResource)
        {
            tracker.Release(requestResource);
            Drain();
        }

        /// <summary>
        /// Attempts to admit queued requests in FIFO order. Calls are coalesced so only one drain runner consumes
        /// the queue, while a request arriving during drain shutdown cannot be lost.
        /// </summary>
        public void Drain()
        {
            if (Volatile.Read(ref disposed) != 0)
                return;

            var pendingWork = Interlocked.Increment(ref drainWork);
            if (Volatile.Read(ref disposed) != 0)
            {
                Interlocked.Decrement(ref drainWork);
                return;
            }

            if (pendingWork != 1)
                return;

            do
                ReleaseWaiters();
            while (Interlocked.Decrement(ref drainWork) != 0);

            void ReleaseWaiters()
            {
                while (true)
                {
                    TRequest requestResource;
                    if (Volatile.Read(ref disposed) != 0 || !waiterQueue.TryPeek(out var waiter))
                        break;

                    if (waiter.IsCanceled)
                    {
                        // Cancellation completes the caller immediately, but its slot is reclaimed only when it
                        // reaches the FIFO head.
                        if (waiterQueue.TryDequeue(out waiter))
                            waiter.Release();
                        continue;
                    }

                    requestResource = waiter.requestResource;

                    // Preserve FIFO fairness: a live head that cannot reserve blocks every later waiter.
                    // Concurrent releases increment drainWork, forcing this owner or the releaser to retry.
                    if (!tracker.TryReserve(requestResource))
                        break;

                    // Disposal may begin after reservation; return capacity before yielding queue ownership.
                    if (Volatile.Read(ref disposed) != 0)
                    {
                        tracker.Release(requestResource);
                        break;
                    }

                    if (!waiterQueue.TryDequeue(out waiter))
                    {
                        // Never retain a reservation unless its waiter was removed from the queue.
                        tracker.Release(requestResource);
                        continue;
                    }

                    // Cancellation can win after the initial state check and before this grant transition.
                    if (!waiter.TryGrant())
                        tracker.Release(requestResource);

                    // The queue no longer references the waiter, so its cancellation registration can be released.
                    waiter.Release();
                }
            }
        }

        void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) != 0, this);
    }
}