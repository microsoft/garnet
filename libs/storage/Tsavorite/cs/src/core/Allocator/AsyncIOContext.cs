// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    using static LogAddress;

    /// <summary>
    /// Async IO context for PMM. Reference type so per-pending-IO storage (the
    /// <see cref="AsyncGetFromDiskResult{TContext}"/> wrapper, completion-event slot,
    /// dictionary values) holds an 8-byte reference rather than a ~112-byte struct with
    /// 5+ object refs. The previous struct-typed implementation caused
    /// <c>Buffer.BulkMoveWithWriteBarrier</c> on every <c>asyncResult.context = context</c>
    /// store, which carried JIT-inserted GC poll loops eating ~18% of the worker thread's
    /// CPU at single-thread libaio random reads (each barrier-set in the bulk-move had a
    /// chance of taking the slow path, and there were ~5 ref fields per copy).
    /// </summary>
    public sealed class AsyncIOContext
    {
        /// <summary>
        /// Id
        /// </summary>
        public long id;

        /// <summary>
        /// Key; this is a shallow copy of the key in pendingContext, pointing to its requestKey.
        /// </summary>
        public ConditionallyHoistedKey requestKey;

        /// The retrieved record, including deserialized ValueObject if RecordDataHeader.ValueIsObject, and key or value Overflows
        public DiskLogRecord diskLogRecord;

        /// <summary>
        /// Logical address that was requested
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
        internal AsyncQueue<AsyncGetFromDiskResult<AsyncIOContext>> callbackQueue;

        /// <summary>
        /// Synchronous completion event
        /// </summary>
        internal AsyncIOContextCompletionEvent completionEvent;

        /// <summary>
        /// Reset all fields to default (for pooling). The receiver instance stays alive so the
        /// pool keeps a stable identity; only the fields change.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Reset()
        {
            id = 0;
            requestKey = default;
            diskLogRecord = default;
            logicalAddress = 0;
            minAddress = 0;
            record = null;
            objBuffer = null;
            callbackQueue = null;
            completionEvent = null;
        }

        /// <summary>
        /// Indicates whether this is a default instance with no pending operation
        /// </summary>
        public bool IsDefault() => callbackQueue is null && completionEvent is null;

        /// <summary>
        /// Dispose
        /// </summary>
        public void DisposeRecord()
        {
            // Do not dispose requestKey as it is a shallow copy of the key in pendingContext
            diskLogRecord.Dispose();
            diskLogRecord = default;
            record?.Return();
            record = null;
        }

        /// <inheritdoc/>
        public override string ToString()
            => $"id {id}, key {requestKey}, LogAddr {AddressString(logicalAddress)}, MinAddr {minAddress}, LogRec [{diskLogRecord}]";
    }

    // Wrapper class so we can communicate back the context.record even if it has to retry due to incomplete records.
    internal sealed class AsyncIOContextCompletionEvent : IDisposable
    {
        internal SemaphoreSlim semaphore;
        internal Exception exception;
        internal AsyncIOContext request;

        internal AsyncIOContextCompletionEvent()
        {
            semaphore = new SemaphoreSlim(0);
            request = new AsyncIOContext { id = -1, minAddress = kInvalidAddress };
            request.completionEvent = this;
        }

        /// <summary>
        /// Prepares to issue an async IO. <paramref name="requestKey"/>
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="requestKey"/> MUST be non-movable, such as on the stack, or pinned for the life of the IO operation.
        /// </remarks>
        internal void Prepare<TKey>(TKey requestKey, long logicalAddress, SectorAlignedBufferPool bufferPool)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            request.DisposeRecord();
            request.requestKey.Dispose();

            request.requestKey = ConditionallyHoistedKey.Create(requestKey, bufferPool);
            request.logicalAddress = logicalAddress;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Set(AsyncIOContext ctx)
        {
            request.DisposeRecord();

            // Class-typed AsyncIOContext: assignment is a single reference store.
            request = ctx;
            exception = null;
            _ = semaphore.Release(1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetException(Exception ex)
        {
            request.DisposeRecord();
            request.requestKey.Dispose();

            request.Reset();
            exception = ex;
            _ = semaphore.Release(1);
        }

        internal void Wait(CancellationToken token = default) => semaphore.Wait(token);

        /// <inheritdoc/>
        public void Dispose()
        {
            request.DisposeRecord();
            request.requestKey.Dispose();
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