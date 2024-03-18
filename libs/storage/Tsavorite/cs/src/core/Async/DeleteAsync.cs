// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    public partial class TsavoriteKV<Key, Value> : TsavoriteBase
    {
        internal struct DeleteAsyncOperation<Input, Output, Context> : IAsyncOperation<Input, Output, Context, DeleteAsyncResult<Input, Output, Context>>
        {
            DeleteOptions deleteOptions;

            internal DeleteAsyncOperation(ref DeleteOptions deleteOptions)
            {
                this.deleteOptions = deleteOptions;
            }

            /// <inheritdoc/>
            public DeleteAsyncResult<Input, Output, Context> CreateCompletedResult(Status status, Output output, RecordMetadata recordMetadata) => new DeleteAsyncResult<Input, Output, Context>(status);

            /// <inheritdoc/>
            public Status DoFastOperation(TsavoriteKV<Key, Value> tsavoriteKV, ref PendingContext<Input, Output, Context> pendingContext, ITsavoriteSession<Key, Value, Input, Output, Context> tsavoriteSession,
                                            out Output output)
            {
                OperationStatus internalStatus;
                ref var key = ref pendingContext.key.Get();
                var keyHash = deleteOptions.KeyHash ?? tsavoriteKV.comparer.GetHashCode64(ref key);
                do
                {
                    internalStatus = tsavoriteKV.InternalDelete(ref key, keyHash, ref pendingContext.userContext, ref pendingContext, tsavoriteSession, pendingContext.serialNum);
                } while (tsavoriteKV.HandleImmediateRetryStatus(internalStatus, tsavoriteSession, ref pendingContext));
                output = default;
                return TranslateStatus(internalStatus);
            }

            /// <inheritdoc/>
            public ValueTask<DeleteAsyncResult<Input, Output, Context>> DoSlowOperation(TsavoriteKV<Key, Value> tsavoriteKV, ITsavoriteSession<Key, Value, Input, Output, Context> tsavoriteSession,
                                            PendingContext<Input, Output, Context> pendingContext, CancellationToken token)
                => SlowDeleteAsync(tsavoriteKV, tsavoriteSession, pendingContext, deleteOptions, token);

            /// <inheritdoc/>
            public bool HasPendingIO => false;
        }

        /// <summary>
        /// State storage for the completion of an async Delete, or the result if the Delete was completed synchronously
        /// </summary>
        public struct DeleteAsyncResult<Input, Output, Context>
        {
            internal readonly AsyncOperationInternal<Input, Output, Context, DeleteAsyncOperation<Input, Output, Context>, DeleteAsyncResult<Input, Output, Context>> updateAsyncInternal;

            /// <summary>Current status of the Upsert operation</summary>
            public Status Status { get; }

            internal DeleteAsyncResult(Status status)
            {
                Status = status;
                updateAsyncInternal = default;
            }

            internal DeleteAsyncResult(TsavoriteKV<Key, Value> tsavoriteKV, ITsavoriteSession<Key, Value, Input, Output, Context> tsavoriteSession,
                PendingContext<Input, Output, Context> pendingContext, ref DeleteOptions deleteOptions, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                Status = new(StatusCode.Pending);
                updateAsyncInternal = new AsyncOperationInternal<Input, Output, Context, DeleteAsyncOperation<Input, Output, Context>, DeleteAsyncResult<Input, Output, Context>>(
                                        tsavoriteKV, tsavoriteSession, pendingContext, exceptionDispatchInfo, new(ref deleteOptions));
            }

            /// <summary>Complete the Delete operation, issuing additional allocation asynchronously if needed. It is usually preferable to use Complete() instead of this.</summary>
            /// <returns>ValueTask for Delete result. User needs to await again if result status is Status.PENDING.</returns>
            public ValueTask<DeleteAsyncResult<Input, Output, Context>> CompleteAsync(CancellationToken token = default)
                => Status.IsPending
                    ? updateAsyncInternal.CompleteAsync(token)
                    : new ValueTask<DeleteAsyncResult<Input, Output, Context>>(new DeleteAsyncResult<Input, Output, Context>(Status));

            /// <summary>Complete the Delete operation, issuing additional I/O synchronously if needed.</summary>
            /// <returns>Status of Delete operation</returns>
            public Status Complete() => Status.IsPending ? updateAsyncInternal.CompleteSync().Status : Status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<DeleteAsyncResult<Input, Output, Context>> DeleteAsync<Input, Output, Context, TsavoriteSession>(TsavoriteSession tsavoriteSession,
                ref Key key, ref DeleteOptions deleteOptions, Context userContext, long serialNo, CancellationToken token = default)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            var pcontext = new PendingContext<Input, Output, Context> { IsAsync = true };

            tsavoriteSession.UnsafeResumeThread();
            try
            {
                OperationStatus internalStatus;
                var keyHash = deleteOptions.KeyHash ?? comparer.GetHashCode64(ref key);
                do
                {
                    internalStatus = InternalDelete(ref key, keyHash, ref userContext, ref pcontext, tsavoriteSession, serialNo);
                } while (HandleImmediateRetryStatus(internalStatus, tsavoriteSession, ref pcontext));

                if (OperationStatusUtils.TryConvertToCompletedStatusCode(internalStatus, out Status status))
                    return new ValueTask<DeleteAsyncResult<Input, Output, Context>>(new DeleteAsyncResult<Input, Output, Context>(status));
                Debug.Assert(internalStatus == OperationStatus.ALLOCATE_FAILED);
            }
            finally
            {
                Debug.Assert(serialNo >= tsavoriteSession.Ctx.serialNum, "Operation serial numbers must be non-decreasing");
                tsavoriteSession.Ctx.serialNum = serialNo;
                tsavoriteSession.UnsafeSuspendThread();
            }

            return SlowDeleteAsync(this, tsavoriteSession, pcontext, deleteOptions, token);
        }

        private static async ValueTask<DeleteAsyncResult<Input, Output, Context>> SlowDeleteAsync<Input, Output, Context>(
            TsavoriteKV<Key, Value> @this,
            ITsavoriteSession<Key, Value, Input, Output, Context> tsavoriteSession,
            PendingContext<Input, Output, Context> pcontext, DeleteOptions deleteOptions, CancellationToken token = default)
        {
            ExceptionDispatchInfo exceptionDispatchInfo = await WaitForFlushCompletionAsync(@this, pcontext.flushEvent, token).ConfigureAwait(false);
            pcontext.flushEvent = default;
            return new DeleteAsyncResult<Input, Output, Context>(@this, tsavoriteSession, pcontext, ref deleteOptions, exceptionDispatchInfo);
        }
    }
}