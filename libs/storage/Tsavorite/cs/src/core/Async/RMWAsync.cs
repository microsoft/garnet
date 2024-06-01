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
            public Status DoFastOperation(TsavoriteKV<Key, Value> tsavoriteKV, ref PendingContext<Input, Output, Context> pendingContext, ISessionFunctionsWrapper<Key, Value, Input, Output, Context> sessionFunctions,
                                            out Output output)
            {
                Status status = !diskRequest.IsDefault()
                    ? tsavoriteKV.InternalCompletePendingRequestFromContext(sessionFunctions, diskRequest, ref pendingContext, out AsyncIOContext<Key, Value> newDiskRequest)
                    : tsavoriteKV.CallInternalRMW(sessionFunctions, ref pendingContext, ref pendingContext.key.Get(), ref pendingContext.input.Get(), ref pendingContext.output, ref rmwOptions,
                                    pendingContext.userContext, out newDiskRequest);
                output = pendingContext.output;
                diskRequest = newDiskRequest;
                return status;
            }

            /// <inheritdoc/>
            public ValueTask<RmwAsyncResult<Input, Output, Context>> DoSlowOperation(TsavoriteKV<Key, Value> tsavoriteKV, ISessionFunctionsWrapper<Key, Value, Input, Output, Context> sessionFunctions,
                                            PendingContext<Input, Output, Context> pendingContext, CancellationToken token)
                => SlowRmwAsync(tsavoriteKV, sessionFunctions, pendingContext, rmwOptions, diskRequest, token);

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

            internal RmwAsyncResult(TsavoriteKV<Key, Value> tsavoriteKV, ISessionFunctionsWrapper<Key, Value, Input, TOutput, Context> sessionFunctions,
                PendingContext<Input, TOutput, Context> pendingContext, ref RMWOptions rmwOptions, AsyncIOContext<Key, Value> diskRequest, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                Status = new(StatusCode.Pending);
                Output = default;
                RecordMetadata = default;
                updateAsyncInternal = new AsyncOperationInternal<Input, TOutput, Context, RmwAsyncOperation<Input, TOutput, Context>, RmwAsyncResult<Input, TOutput, Context>>(
                                        tsavoriteKV, sessionFunctions, pendingContext, exceptionDispatchInfo, new(diskRequest, ref rmwOptions));
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
        internal ValueTask<RmwAsyncResult<Input, Output, Context>> RmwAsync<Input, Output, Context, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref Key key, ref Input input, ref RMWOptions rmwOptions, Context context, CancellationToken token = default)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            var pcontext = new PendingContext<Input, Output, Context> { IsAsync = true };
            var diskRequest = default(AsyncIOContext<Key, Value>);

            sessionFunctions.UnsafeResumeThread();
            try
            {
                Output output = default;
                var status = CallInternalRMW(sessionFunctions, ref pcontext, ref key, ref input, ref output, ref rmwOptions, context, out diskRequest);
                if (!status.IsPending)
                    return new ValueTask<RmwAsyncResult<Input, Output, Context>>(new RmwAsyncResult<Input, Output, Context>(status, output, new RecordMetadata(pcontext.recordInfo, pcontext.logicalAddress)));
            }
            finally
            {
                sessionFunctions.UnsafeSuspendThread();
            }

            return SlowRmwAsync(this, sessionFunctions, pcontext, rmwOptions, diskRequest, token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status CallInternalRMW<Input, Output, Context>(ISessionFunctionsWrapper<Key, Value, Input, Output, Context> sessionFunctions, ref PendingContext<Input, Output, Context> pcontext,
                    ref Key key, ref Input input, ref Output output, ref RMWOptions rmwOptions, Context context, out AsyncIOContext<Key, Value> diskRequest)
        {
            OperationStatus internalStatus;
            var keyHash = rmwOptions.KeyHash ?? comparer.GetHashCode64(ref key);
            do
                internalStatus = InternalRMW(ref key, keyHash, ref input, ref output, ref context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions, ref pcontext));

            return HandleOperationStatus(sessionFunctions.Ctx, ref pcontext, internalStatus, out diskRequest);
        }

        private static async ValueTask<RmwAsyncResult<Input, Output, Context>> SlowRmwAsync<Input, Output, Context>(
            TsavoriteKV<Key, Value> @this, ISessionFunctionsWrapper<Key, Value, Input, Output, Context> sessionFunctions,
            PendingContext<Input, Output, Context> pcontext, RMWOptions rmwOptions, AsyncIOContext<Key, Value> diskRequest, CancellationToken token = default)
        {
            ExceptionDispatchInfo exceptionDispatchInfo;
            (diskRequest, exceptionDispatchInfo) = await WaitForFlushOrIOCompletionAsync(@this, sessionFunctions.Ctx, pcontext.flushEvent, diskRequest, token);
            pcontext.flushEvent = default;
            return new RmwAsyncResult<Input, Output, Context>(@this, sessionFunctions, pcontext, ref rmwOptions, diskRequest, exceptionDispatchInfo);
        }
    }
}