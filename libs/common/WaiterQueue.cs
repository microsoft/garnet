// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
    /// managed by <see cref="LightBoundedFifoQueue{T}"/>. Waiter nodes are pooled by the queue and carry one completion signal.
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
        /// Default maximum number of waiter nodes retained for reuse.
        /// </summary>
        public const int DefaultMaxPooledWaiters = 128;

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
            TaskCompletionSource<bool> signal;
            CancellationTokenRegistration cancellationRegistration;
            WaiterQueue<TRequest> owner;

            // Queue completion can race producer setup; recycle only after both paths release the waiter.
            int remainingOwners;
            int registrationState;

            internal TRequest requestResource;
            internal LightBoundedFifoQueue<Waiter>.QueueEntryHandle queueHandle;

            internal void Prepare(WaiterQueue<TRequest> owner, in TRequest requestResource)
            {
                this.owner = owner;
                this.requestResource = requestResource;
                this.queueHandle = LightBoundedFifoQueue<Waiter>.QueueEntryHandle.Invalid;
                signal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                remainingOwners = 2;
                registrationState = 0;
            }

            internal void RegisterCancellation(CancellationToken cancellationToken)
            {
                if (cancellationToken.CanBeCanceled)
                {
                    var registration = cancellationToken.UnsafeRegister(
                        static (state, token) => ((Waiter)state).Cancel(token), this);
                    cancellationRegistration = registration;
                    if (Interlocked.CompareExchange(ref registrationState, 1, 0) != 0)
                        registration.Dispose();
                }
            }

            void Cancel(CancellationToken token) => owner.CancelWaiter(this, token);

            internal Task<bool> Task => signal.Task;

            internal void Complete(Exception exception)
            {
                var toSignal = signal;
                if (exception == null)
                    toSignal.SetResult(true);
                else
                    toSignal.SetException(exception);

                if (Interlocked.Exchange(ref registrationState, 2) == 1)
                    cancellationRegistration.Dispose();
                cancellationRegistration = default;
                ReleaseOwnership();
            }

            internal void Clear()
            {
                owner = null;
                requestResource = default;
                signal = null;
                queueHandle = LightBoundedFifoQueue<Waiter>.QueueEntryHandle.Invalid;
            }

            internal void Recycle()
            {
                var queue = owner.waiterQueue;
                Clear();
                queue.Return(this);
            }

            internal void ReleaseOwnership()
            {
                if (Interlocked.Decrement(ref remainingOwners) == 0)
                    Recycle();
            }

            internal void Abandon()
            {
                remainingOwners = 0;
                Recycle();
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
        /// <param name="maxPooledWaiters">Maximum waiter nodes retained for reuse.</param>
        public WaiterQueue(
            IResourceTracker<TRequest> tracker,
            int ringPageSize = DefaultRingPageSize,
            int ringPageCount = DefaultRingPageCount,
            int spinCount = DefaultSpinCount,
            int maxEnqueueSpinCount = DefaultMaxEnqueueSpinCount,
            int maxPooledWaiters = DefaultMaxPooledWaiters)
        {
            ArgumentNullException.ThrowIfNull(tracker);
            ArgumentOutOfRangeException.ThrowIfNegative(spinCount);
            ArgumentOutOfRangeException.ThrowIfNegative(maxEnqueueSpinCount);
            ArgumentOutOfRangeException.ThrowIfNegative(maxPooledWaiters);

            this.tracker = tracker;
            this.spinCount = spinCount;
            this.waiterQueue = new LightBoundedFifoQueue<Waiter>(
                ringPageSize,
                ringPageCount,
                maxEnqueueSpinCount,
                itemFactory: static () => new Waiter(),
                maxPooledItems: maxPooledWaiters);
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

            if (TryReserveBeforeQueue(requestResource, token))
                return ValueTask.FromResult(true);

            var waitTask = TryEnqueueAsync(requestResource, token);
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

            if (TryReserveBeforeQueue(requestResource, token))
                return true;

            var waitTask = TryEnqueueAsync(requestResource, token);
            if (waitTask == null)
                return false;

            Drain();
#pragma warning disable VSTHRD002 // The synchronous admission contract blocks until the waiter signal is set.
            return waitTask.GetAwaiter().GetResult();
#pragma warning restore VSTHRD002
        }

        bool TryReserveBeforeQueue(in TRequest requestResource, CancellationToken token)
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

        Task<bool> TryEnqueueAsync(in TRequest requestResource, CancellationToken token)
        {
            ThrowIfDisposed();
            token.ThrowIfCancellationRequested();

            var waiter = waiterQueue.Rent();
            waiter.Prepare(this, requestResource);
            var waitTask = waiter.Task;
            if (!waiterQueue.TryEnqueue(waiter, out var handle))
            {
                waiter.Abandon();
                return null;
            }

            waiter.queueHandle = handle;
            waiter.RegisterCancellation(token);
            waiter.ReleaseOwnership();
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
                GrantWaiters();
            while (Interlocked.Decrement(ref drainWork) != 0);
        }

        void GrantWaiters()
        {
            while (true)
            {
                TRequest requestResource;
                if (Volatile.Read(ref disposed) != 0 || !waiterQueue.TryPeek(out var handle, out var waiter))
                    break;

                requestResource = waiter.requestResource;

                if (!tracker.TryReserve(requestResource))
                    break;

                if (Volatile.Read(ref disposed) != 0 || !waiterQueue.TryDequeue(handle, out waiter))
                {
                    tracker.Release(requestResource);
                    continue;
                }

                waiter.Complete(exception: null);
            }
        }

        void CancelWaiter(Waiter waiter, CancellationToken token)
        {
            if (!waiterQueue.TryRemove(waiter.queueHandle, out var removedWaiter) ||
                !ReferenceEquals(waiter, removedWaiter))
                return;

            waiter.Complete(new OperationCanceledException(token));
            Drain();
        }

        void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) != 0, this);

        /// <inheritdoc />
        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0)
                return;

            waiterQueue.CompleteAdding();

            var spinner = new SpinWait();
            while (Interlocked.CompareExchange(ref drainWork, 1, 0) != 0)
                spinner.SpinOnce();

            try
            {
                var exception = new ObjectDisposedException(nameof(WaiterQueue<TRequest>));
                while (waiterQueue.TryPeek(out var handle, out var waiter))
                {
                    if (!waiterQueue.TryDequeue(handle, out waiter))
                        continue;

                    waiter.Complete(exception);
                }
            }
            finally
            {
                Volatile.Write(ref drainWork, 0);
            }

            waiterQueue.Dispose();
        }
    }
}