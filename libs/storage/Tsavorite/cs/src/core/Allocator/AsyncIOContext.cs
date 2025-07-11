// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    using static LogAddress;

    /// <summary>
    /// Async IO context for PMM
    /// </summary>
    public unsafe struct AsyncIOContext
    {
        /// <summary>
        /// Id
        /// </summary>
        public long id;

        /// <summary>
        /// Key; this is a shallow copy of the key in pendingContext, pointing to its diskLogRecord
        /// </summary>
        public PinnedSpanByte request_key;

        /// <summary>
        /// The retrieved record, including deserialized ValueObject if RecordInfo.ValueIsObject, and key or value Overflows
        /// </summary>
        public DiskLogRecord diskLogRecord;

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
        /// Callback queue
        /// </summary>
        public AsyncQueue<AsyncIOContext> callbackQueue;

        /// <summary>
        /// Synchronous completion event
        /// </summary>
        internal AsyncIOContextCompletionEvent completionEvent;

        /// <summary>
        /// Indicates whether this is a default instance with no pending operation
        /// </summary>
        public readonly bool IsDefault() => callbackQueue is null && completionEvent is null;

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            // Do not dispose request_key as it is a shallow copy of the key in pendingContext.
            // If we transferred the record buffer to the DiskLogRecord, it will be null here.
            record?.Return();
            record = null;

            diskLogRecord.Dispose();
            diskLogRecord = default;
        }
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
            request.id = -1;
            request.minAddress = kInvalidAddress;
            request.completionEvent = this;
        }

        /// <summary>
        /// Prepares to issue an async IO. <paramref name="request_key"/>
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="request_key"/> MUST be non-movable, such as on the stack, or pinned for the life of the IO operation.
        /// </remarks>
        internal void Prepare(PinnedSpanByte request_key, long logicalAddress)
        {
            request.Dispose();
            request.request_key = request_key;
            request.logicalAddress = logicalAddress;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Set(ref AsyncIOContext ctx)
        {
            request.Dispose();
            request = ctx;
            exception = null;
            _ = semaphore.Release(1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetException(Exception ex)
        {
            request.Dispose();
            request = default;
            exception = ex;
            _ = semaphore.Release(1);
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