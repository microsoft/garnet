// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    public partial class TsavoriteKV<Key, Value> : TsavoriteBase
    {
        // All 4 operations can go pending when they generate Flush operations on BlockAllocate when inserting new records at the tail.
        // Read and RMW can also go pending with a disk operation.

        internal interface IAsyncOperation<Input, Output, Context, TAsyncResult>
        {
            /// <summary>
            /// This creates an instance of the <typeparamref name="TAsyncResult"/>, for example <see cref="RmwAsyncResult{Input, Output, Context}"/>
            /// </summary>
            /// <param name="status">The status code; for this variant of <typeparamref name="TAsyncResult"/> intantiation, this will not be pending</param>
            /// <param name="output">The completed output of the operation, if any</param>
            /// <param name="recordMetadata">The record metadata from the operation (currently used by RMW only)</param>
            /// <returns></returns>
            TAsyncResult CreateCompletedResult(Status status, Output output, RecordMetadata recordMetadata);

            /// <summary>
            /// This performs the low-level synchronous operation for the implementation class of <typeparamref name="TAsyncResult"/>; for example,
            /// <see cref="TsavoriteKV{Key, Value}.InternalRMW"/>.
            /// </summary>
            /// <param name="tsavoriteKV">The <see cref="TsavoriteKV{Key, Value}"/> instance the async call was made on</param>
            /// <param name="pendingContext">The <see cref="PendingContext{Input, Output, Context}"/> for the pending operation</param>
            /// <param name="tsavoriteSession">The <see cref="ITsavoriteSession{Key, Value, Input, Output, Context}"/> for this operation</param>
            /// <param name="output">The output to be populated by this operation</param>
            /// <returns></returns>
            Status DoFastOperation(TsavoriteKV<Key, Value> tsavoriteKV, ref PendingContext<Input, Output, Context> pendingContext, ITsavoriteSession<Key, Value, Input, Output, Context> tsavoriteSession,
                                            out Output output);
            /// <summary>
            /// Performs the asynchronous operation. This may be a wait for either a page-flush or a disk-read IO.
            /// </summary>
            /// <param name="tsavoriteKV">The <see cref="TsavoriteKV{Key, Value}"/> instance the async call was made on</param>
            /// <param name="tsavoriteSession">The <see cref="ITsavoriteSession{Key, Value, Input, Output, Context}"/> for this operation</param>
            /// <param name="pendingContext">The <see cref="PendingContext{Input, Output, Context}"/> for the pending operation</param>
            /// <param name="token">The cancellation token, if any</param>
            /// <returns></returns>
            ValueTask<TAsyncResult> DoSlowOperation(TsavoriteKV<Key, Value> tsavoriteKV, ITsavoriteSession<Key, Value, Input, Output, Context> tsavoriteSession,
                                            PendingContext<Input, Output, Context> pendingContext, CancellationToken token);

            /// <summary>
            /// For RMW only, indicates whether there is a pending IO; no-op for other implementations.
            /// </summary>
            bool HasPendingIO { get; }
        }

        internal sealed class AsyncOperationInternal<Input, Output, Context, TAsyncOperation, TAsyncResult>
            where TAsyncOperation : IAsyncOperation<Input, Output, Context, TAsyncResult>
        {
            const int Completed = 1;
            const int Pending = 0;
            ExceptionDispatchInfo _exception;
            readonly TsavoriteKV<Key, Value> _tsavoriteKV;
            readonly ITsavoriteSession<Key, Value, Input, Output, Context> _tsavoriteSession;

#pragma warning disable IDE0044 // Add readonly modifier
            // This cannot be readonly or it defensively copies and we will modify the internal state of the *temporary*
            TAsyncOperation _asyncOperation;
#pragma warning restore IDE0044 // Add readonly modifier

            PendingContext<Input, Output, Context> _pendingContext;
            int CompletionComputeStatus;

            internal AsyncOperationInternal(TsavoriteKV<Key, Value> tsavoriteKV, ITsavoriteSession<Key, Value, Input, Output, Context> tsavoriteSession,
                                      PendingContext<Input, Output, Context> pendingContext, ExceptionDispatchInfo exceptionDispatchInfo, TAsyncOperation asyncOperation)
            {
                _exception = exceptionDispatchInfo;
                _tsavoriteKV = tsavoriteKV;
                _tsavoriteSession = tsavoriteSession;
                _pendingContext = pendingContext;
                _asyncOperation = asyncOperation;
                CompletionComputeStatus = Pending;
            }

            internal ValueTask<TAsyncResult> CompleteAsync(CancellationToken token = default)
            {
                // Note: We currently do not await anything here, and we must never do any post-await work inside CompleteAsync; this includes any code in
                // a 'finally' block. All post-await work must be re-initiated by end user on the mono-threaded session.

                if (TryCompleteAsyncState(out var asyncResult))
                    return new ValueTask<TAsyncResult>(asyncResult);

                if (_exception != default)
                    _exception.Throw();

                // DoSlowOperation returns a new XxxAsyncResult, which contains a new UpdateAsyncInternal with a pendingContext with a default flushEvent
                return _asyncOperation.DoSlowOperation(_tsavoriteKV, _tsavoriteSession, _pendingContext, token);
            }

            internal TAsyncResult CompleteSync()
            {
                _pendingContext.IsAsync = false;    // We are now operating in sync mode for any subsequent IO or ALLOCATE_FAILED
                if (!TryCompleteAsyncState(out TAsyncResult asyncResult))
                {
                    while (true)
                    {
                        if (_exception != default)
                            _exception.Throw();

                        bool isPending = false;
                        if (!_pendingContext.flushEvent.IsDefault())
                        {
                            _pendingContext.flushEvent.Wait();
                            _pendingContext.flushEvent = default;
                        }
                        else if (TryCompletePendingSyncIO(out asyncResult, out isPending))
                            break;

                        if (!isPending && TryCompleteSync(out asyncResult))
                            break;
                    }
                }
                return asyncResult;
            }

            private bool TryCompleteAsyncState(out TAsyncResult asyncResult)
            {
                // This makes one attempt to complete the async operation's synchronous state, and clears the async pending counters.
                if (CompletionComputeStatus != Completed
                    && Interlocked.CompareExchange(ref CompletionComputeStatus, Completed, Pending) == Pending)
                {
                    bool hasPendingIO = _asyncOperation.HasPendingIO;
                    var pendingId = _pendingContext.id;     // _pendingContext.id is overwritten if TryCompleteSync enqueues another IO request
                    try
                    {
                        if (_exception == default)
                            return TryCompleteSync(out asyncResult);
                    }
                    finally
                    {
                        if (hasPendingIO)
                        {
                            _tsavoriteSession.Ctx.ioPendingRequests.Remove(pendingId);
                            _tsavoriteSession.Ctx.asyncPendingCount--;
                            _tsavoriteSession.Ctx.pendingReads.Remove();
                        }
                    }
                }

                _pendingContext.flushEvent = default;
                asyncResult = default;
                return false;
            }

            private bool TryCompleteSync(out TAsyncResult asyncResult)
            {
                _tsavoriteSession.UnsafeResumeThread();
                try
                {
                    Status status = _asyncOperation.DoFastOperation(_tsavoriteKV, ref _pendingContext, _tsavoriteSession, out Output output);

                    if (!status.IsPending)
                    {
                        _pendingContext.Dispose();
                        asyncResult = _asyncOperation.CreateCompletedResult(status, output, new RecordMetadata(_pendingContext.recordInfo, _pendingContext.logicalAddress));
                        return true;
                    }
                }
                catch (Exception e)
                {
                    _exception = ExceptionDispatchInfo.Capture(e);
                    _pendingContext.flushEvent = default;
                }
                finally
                {
                    _tsavoriteSession.UnsafeSuspendThread();
                }

                asyncResult = default;
                return false;
            }

            bool TryCompletePendingSyncIO(out TAsyncResult asyncResult, out bool isPending)
            {
                asyncResult = default;
                isPending = false;
                if (!_asyncOperation.HasPendingIO)
                    return false;

                // Because we've set pendingContext.IsAsync false, CompletePending() will Wait() on any flushEvent if it encounters OperationStatus.ALLOCATE_FAILED.
                Status status;
                Output output = default;
                if (!_tsavoriteSession.CompletePendingWithOutputs(out var completedOutputs, wait: true, spinWaitForCommit: false))
                    status = new(StatusCode.Error);
                else
                {
                    if (!completedOutputs.Next())
                        status = new(StatusCode.Error);
                    else
                    {
                        status = completedOutputs.Current.Status;
                        output = completedOutputs.Current.Output;
                    }
                    completedOutputs.Dispose();
                    isPending = status.IsPending;
                    if (isPending)
                        return false;
                }

                // We have a result or an error state, so we have completed the operation.
                _pendingContext.Dispose();
                asyncResult = _asyncOperation.CreateCompletedResult(status, output, new RecordMetadata(_pendingContext.recordInfo, _pendingContext.logicalAddress));
                return true;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static Status TranslateStatus(OperationStatus internalStatus)
        {
            if (OperationStatusUtils.TryConvertToCompletedStatusCode(internalStatus, out Status status))
                return status;
            Debug.Assert(internalStatus == OperationStatus.ALLOCATE_FAILED);
            return new(StatusCode.Pending);
        }

        // This takes flushEvent as a parameter because we can't pass by ref to an async method.
        private static async ValueTask<ExceptionDispatchInfo> WaitForFlushCompletionAsync(TsavoriteKV<Key, Value> @this, CompletionEvent flushEvent, CancellationToken token)
        {
            ExceptionDispatchInfo exceptionDispatchInfo = default;
            try
            {
                token.ThrowIfCancellationRequested();

                if (@this.epoch.ThisInstanceProtected())
                    throw new NotSupportedException("Async operations not supported over protected epoch");

                await flushEvent.WaitAsync(token).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                exceptionDispatchInfo = ExceptionDispatchInfo.Capture(e);
            }
            return exceptionDispatchInfo;
        }

        // This takes flushEvent as a parameter because we can't pass by ref to an async method.
        private static async ValueTask<(AsyncIOContext<Key, Value> diskRequest, ExceptionDispatchInfo edi)> WaitForFlushOrIOCompletionAsync<Input, Output, Context>(
                        TsavoriteKV<Key, Value> @this, TsavoriteExecutionContext<Input, Output, Context> sessionCtx,
                        CompletionEvent flushEvent, AsyncIOContext<Key, Value> diskRequest, CancellationToken token)
        {
            ExceptionDispatchInfo exceptionDispatchInfo = default;
            try
            {
                token.ThrowIfCancellationRequested();

                if (@this.epoch.ThisInstanceProtected())
                    throw new NotSupportedException("Async operations not supported over protected epoch");

                // If we are here because of flushEvent, then _diskRequest is default--there is no pending disk operation.
                if (diskRequest.IsDefault())
                {
                    Debug.Assert(!flushEvent.IsDefault());
                    await flushEvent.WaitAsync(token).ConfigureAwait(false);
                }
                else
                {
                    Debug.Assert(flushEvent.IsDefault());
                    sessionCtx.asyncPendingCount++;
                    sessionCtx.pendingReads.Add();

                    using (token.Register(() => diskRequest.asyncOperation.TrySetCanceled()))
                        diskRequest = await diskRequest.asyncOperation.Task.WithCancellationAsync(token).ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                exceptionDispatchInfo = ExceptionDispatchInfo.Capture(e);
            }
            return (diskRequest, exceptionDispatchInfo);
        }
    }
}