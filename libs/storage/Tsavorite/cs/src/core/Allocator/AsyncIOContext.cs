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
    public struct AsyncIOContext
    {
        /// <summary>
        /// Id
        /// </summary>
        public long id;

        /// <summary>
        /// Key; this is a shallow copy of the key in pendingContext, pointing to its requestKey.
        /// </summary>
        public PinnedSpanByte requestKey;

        /// <summary>
        /// Namespace; this is a shallow copy of the namespace in pendingContext, pointing to its requestKey.
        /// </summary>
        public SpanByteAndMemory requestNamespace;

        /// The retrieved record, including deserialized ValueObject if RecordInfo.ValueIsObject, and key or value Overflows
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
        public void DisposeRecord()
        {
            // Do not dispose requestKey as it is a shallow copy of the key in pendingContext
            diskLogRecord.Dispose();
            diskLogRecord = default;
            record?.Return();
            record = null;
        }

        /// <inheritdoc/>
        public override readonly string ToString()
            => $"id {id}, key {requestKey}, ns {requestNamespace}, LogAddr {AddressString(logicalAddress)}, MinAddr {minAddress}, LogRec [{diskLogRecord}]";
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
        /// Prepares to issue an async IO. <paramref name="requestKey"/>
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="requestKey"/> MUST be non-movable, such as on the stack, or pinned for the life of the IO operation.
        /// </remarks>
        internal void Prepare(PinnedSpanByte requestKey, SpanByteAndMemory requestNamespace, long logicalAddress)
        {
            request.DisposeRecord();
            request.requestNamespace.Memory?.Dispose();

            request.requestKey = requestKey;
            request.requestNamespace = requestNamespace;
            request.logicalAddress = logicalAddress;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Set(ref AsyncIOContext ctx)
        {
            request.DisposeRecord();
            request.requestNamespace.Memory?.Dispose();

            request = ctx;
            exception = null;
            _ = semaphore.Release(1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetException(Exception ex)
        {
            request.DisposeRecord();
            request.requestNamespace.Memory?.Dispose();

            request = default;
            exception = ex;
            _ = semaphore.Release(1);
        }

        internal void Wait(CancellationToken token = default) => semaphore.Wait(token);

        /// <inheritdoc/>
        public void Dispose()
        {
            request.DisposeRecord();
            request.requestNamespace.Memory?.Dispose();

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