// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// Async IO context for PMM
    /// </summary>
    public unsafe struct AsyncIOContext<TKey, TValue>
    {
        /// <summary>
        /// Id
        /// </summary>
        public long id;

        /// <summary>
        /// Key
        /// </summary>
        public IHeapContainer<TKey> request_key;

        /// <summary>
        /// Retrieved key
        /// </summary>
        public TKey key;

        /// <summary>
        /// Retrieved value
        /// </summary>
        public TValue value;

        /// <summary>
        /// Logical address
        /// </summary>
        public long logicalAddress;

        /// <summary>
        /// Minimum Logical address to resolve Key in
        /// </summary>
        public long minAddress;

        /// <summary>
        /// Record buffer
        /// </summary>
        public SectorAlignedMemory record;

        /// <summary>
        /// Object buffer
        /// </summary>
        public SectorAlignedMemory objBuffer;

        /// <summary>
        /// Callback queue
        /// </summary>
        public AsyncQueue<AsyncIOContext<TKey, TValue>> callbackQueue;

        /// <summary>
        /// Async Operation ValueTask backer
        /// </summary>
        public TaskCompletionSource<AsyncIOContext<TKey, TValue>> asyncOperation;

        /// <summary>
        /// Synchronous completion event
        /// </summary>
        internal AsyncIOContextCompletionEvent<TKey, TValue> completionEvent;

        /// <summary>
        /// Indicates whether this is a default instance with no pending operation
        /// </summary>
        public bool IsDefault() => callbackQueue is null && asyncOperation is null && completionEvent is null;

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            // Do not dispose request_key as it is a shallow copy of the key in pendingContext
            record?.Return();
            record = null;
        }
    }

    // Wrapper class so we can communicate back the context.record even if it has to retry due to incomplete records.
    internal sealed class AsyncIOContextCompletionEvent<TKey, TValue> : IDisposable
    {
        internal SemaphoreSlim semaphore;
        internal Exception exception;
        internal AsyncIOContext<TKey, TValue> request;

        internal AsyncIOContextCompletionEvent()
        {
            semaphore = new SemaphoreSlim(0);
            request.id = -1;
            request.minAddress = Constants.kInvalidAddress;
            request.completionEvent = this;
        }

        internal void Prepare(IHeapContainer<TKey> request_key, long logicalAddress)
        {
            request.Dispose();
            request.request_key = request_key;
            request.logicalAddress = logicalAddress;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Set(ref AsyncIOContext<TKey, TValue> ctx)
        {
            request.Dispose();
            request = ctx;
            exception = null;
            semaphore.Release(1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetException(Exception ex)
        {
            request.Dispose();
            request = default;
            exception = ex;
            semaphore.Release(1);
        }

        internal void Wait(CancellationToken token = default) => semaphore.Wait(token);

        /// <inheritdoc/>
        public void Dispose()
        {
            request.Dispose();
            semaphore?.Dispose();
        }
    }

    internal sealed class SimpleReadContext
    {
        public long logicalAddress;
        public SectorAlignedMemory record;
        public SemaphoreSlim completedRead;
    }
}