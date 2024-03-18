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
        internal struct RmwAsyncOperation<Input, Output, Context> : IAsyncOperation<Input, Output, Context, RmwAsyncResult<Input, Output, Context>>
        {
            AsyncIOContext<Key, Value> diskRequest;
            RMWOptions rmwOptions;

            internal RmwAsyncOperation(AsyncIOContext<Key, Value> diskRequest, ref RMWOptions rmwOptions)
            {
                this.diskRequest = diskRequest;
                this.rmwOptions = rmwOptions;
            }

            /// <inheritdoc/>
            public RmwAsyncResult<Input, Output, Context> CreateCompletedResult(Status status, Output output, RecordMetadata recordMetadata) => new(status, output, recordMetadata);

            /// <inheritdoc/>
            public Status DoFastOperation(TsavoriteKV<Key, Value> tsavoriteKV, ref PendingContext<Input, Output, Context> pendingContext, ITsavoriteSession<Key, Value, Input, Output, Context> tsavoriteSession,
                                            out Output output)
            {
                Status status = !diskRequest.IsDefault()
                    ? tsavoriteKV.InternalCompletePendingRequestFromContext(tsavoriteSession, diskRequest, ref pendingContext, out AsyncIOContext<Key, Value> newDiskRequest)
                    : tsavoriteKV.CallInternalRMW(tsavoriteSession, ref pendingContext, ref pendingContext.key.Get(), ref pendingContext.input.Get(), ref pendingContext.output, ref rmwOptions,
                                    pendingContext.userContext, pendingContext.serialNum, out newDiskRequest);
                output = pendingContext.output;
                diskRequest = newDiskRequest;
                return status;
            }

            /// <inheritdoc/>
            public ValueTask<RmwAsyncResult<Input, Output, Context>> DoSlowOperation(TsavoriteKV<Key, Value> tsavoriteKV, ITsavoriteSession<Key, Value, Input, Output, Context> tsavoriteSession,
                                            PendingContext<Input, Output, Context> pendingContext, CancellationToken token)
                => SlowRmwAsync(tsavoriteKV, tsavoriteSession, pendingContext, rmwOptions, diskRequest, token);

            /// <inheritdoc/>
            public bool HasPendingIO => !diskRequest.IsDefault();
        }

        /// <summary>
        /// State storage for the completion of an async RMW, or the result if the RMW was completed synchronously
        /// </summary>
        public struct RmwAsyncResult<Input, TOutput, Context>
        {
            internal readonly AsyncOperationInternal<Input, TOutput, Context, RmwAsyncOperation<Input, TOutput, Context>, RmwAsyncResult<Input, TOutput, Context>> updateAsyncInternal;

            /// <summary>Current status of the RMW operation</summary>
            public Status Status { get; }

            /// <summary>Output of the RMW operation if current status is not pending</summary>
            public TOutput Output { get; }

            /// <summary>Metadata of the updated record</summary>
            public RecordMetadata RecordMetadata { get; }

            internal RmwAsyncResult(Status status, TOutput output, RecordMetadata recordMetadata)
            {
                Debug.Assert(!status.IsPending);
                Status = status;
                Output = output;
                RecordMetadata = recordMetadata;
                updateAsyncInternal = default;
            }

            internal RmwAsyncResult(TsavoriteKV<Key, Value> tsavoriteKV, ITsavoriteSession<Key, Value, Input, TOutput, Context> tsavoriteSession,
                PendingContext<Input, TOutput, Context> pendingContext, ref RMWOptions rmwOptions, AsyncIOContext<Key, Value> diskRequest, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                Status = new(StatusCode.Pending);
                Output = default;
                RecordMetadata = default;
                updateAsyncInternal = new AsyncOperationInternal<Input, TOutput, Context, RmwAsyncOperation<Input, TOutput, Context>, RmwAsyncResult<Input, TOutput, Context>>(
                                        tsavoriteKV, tsavoriteSession, pendingContext, exceptionDispatchInfo, new(diskRequest, ref rmwOptions));
            }

            /// <summary>Complete the RMW operation, issuing additional (rare) I/O asynchronously if needed. It is usually preferable to use Complete() instead of this.</summary>
            /// <returns>ValueTask for RMW result. User needs to await again if result status is pending.</returns>
            public ValueTask<RmwAsyncResult<Input, TOutput, Context>> CompleteAsync(CancellationToken token = default)
                => Status.IsPending
                    ? updateAsyncInternal.CompleteAsync(token)
                    : new ValueTask<RmwAsyncResult<Input, TOutput, Context>>(new RmwAsyncResult<Input, TOutput, Context>(Status, Output, RecordMetadata));

            /// <summary>Complete the RMW operation, issuing additional (rare) I/O synchronously if needed.</summary>
            /// <returns>Status of RMW operation</returns>
            public (Status status, TOutput output) Complete()
                => Complete(out _);

            /// <summary>Complete the RMW operation, issuing additional (rare) I/O synchronously if needed.</summary>
            /// <returns>Status of RMW operation</returns>
            public (Status status, TOutput output) Complete(out RecordMetadata recordMetadata)
            {
                if (!Status.IsPending)
                {
                    recordMetadata = RecordMetadata;
                    return (Status, Output);
                }
                var rmwAsyncResult = updateAsyncInternal.CompleteSync();
                recordMetadata = rmwAsyncResult.RecordMetadata;
                return (rmwAsyncResult.Status, rmwAsyncResult.Output);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<RmwAsyncResult<Input, Output, Context>> RmwAsync<Input, Output, Context, TsavoriteSession>(TsavoriteSession tsavoriteSession,
                ref Key key, ref Input input, ref RMWOptions rmwOptions, Context context, long serialNo, CancellationToken token = default)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            var pcontext = new PendingContext<Input, Output, Context> { IsAsync = true };
            var diskRequest = default(AsyncIOContext<Key, Value>);

            tsavoriteSession.UnsafeResumeThread();
            try
            {
                Output output = default;
                var status = CallInternalRMW(tsavoriteSession, ref pcontext, ref key, ref input, ref output, ref rmwOptions, context, serialNo, out diskRequest);
                if (!status.IsPending)
                    return new ValueTask<RmwAsyncResult<Input, Output, Context>>(new RmwAsyncResult<Input, Output, Context>(status, output, new RecordMetadata(pcontext.recordInfo, pcontext.logicalAddress)));
            }
            finally
            {
                Debug.Assert(serialNo >= tsavoriteSession.Ctx.serialNum, "Operation serial numbers must be non-decreasing");
                tsavoriteSession.Ctx.serialNum = serialNo;
                tsavoriteSession.UnsafeSuspendThread();
            }

            return SlowRmwAsync(this, tsavoriteSession, pcontext, rmwOptions, diskRequest, token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status CallInternalRMW<Input, Output, Context>(ITsavoriteSession<Key, Value, Input, Output, Context> tsavoriteSession, ref PendingContext<Input, Output, Context> pcontext,
                    ref Key key, ref Input input, ref Output output, ref RMWOptions rmwOptions, Context context, long serialNo, out AsyncIOContext<Key, Value> diskRequest)
        {
            OperationStatus internalStatus;
            var keyHash = rmwOptions.KeyHash ?? comparer.GetHashCode64(ref key);
            do
                internalStatus = InternalRMW(ref key, keyHash, ref input, ref output, ref context, ref pcontext, tsavoriteSession, serialNo);
            while (HandleImmediateRetryStatus(internalStatus, tsavoriteSession, ref pcontext));

            return HandleOperationStatus(tsavoriteSession.Ctx, ref pcontext, internalStatus, out diskRequest);
        }

        private static async ValueTask<RmwAsyncResult<Input, Output, Context>> SlowRmwAsync<Input, Output, Context>(
            TsavoriteKV<Key, Value> @this, ITsavoriteSession<Key, Value, Input, Output, Context> tsavoriteSession,
            PendingContext<Input, Output, Context> pcontext, RMWOptions rmwOptions, AsyncIOContext<Key, Value> diskRequest, CancellationToken token = default)
        {
            ExceptionDispatchInfo exceptionDispatchInfo;
            (diskRequest, exceptionDispatchInfo) = await WaitForFlushOrIOCompletionAsync(@this, tsavoriteSession.Ctx, pcontext.flushEvent, diskRequest, token);
            pcontext.flushEvent = default;
            return new RmwAsyncResult<Input, Output, Context>(@this, tsavoriteSession, pcontext, ref rmwOptions, diskRequest, exceptionDispatchInfo);
        }
    }
}