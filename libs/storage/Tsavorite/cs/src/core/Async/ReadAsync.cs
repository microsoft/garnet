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
        internal struct ReadAsyncOperation<Input, Output, Context> : IAsyncOperation<Input, Output, Context, ReadAsyncResult<Input, Output, Context>>
        {
            AsyncIOContext<Key, Value> diskRequest;
            long readAtAddress;
            ReadOptions readOptions;

            internal ReadAsyncOperation(AsyncIOContext<Key, Value> diskRequest, long readAtAddress, ref ReadOptions readOptions)
            {
                this.diskRequest = diskRequest;
                this.readAtAddress = readAtAddress;
                this.readOptions = readOptions;
            }

            /// <inheritdoc/>
            public ReadAsyncResult<Input, Output, Context> CreateCompletedResult(Status status, Output output, RecordMetadata recordMetadata) => new(status, output, recordMetadata);

            /// <inheritdoc/>
            public Status DoFastOperation(TsavoriteKV<Key, Value> tsavoriteKV, ref PendingContext<Input, Output, Context> pendingContext,
                                          ITsavoriteSession<Key, Value, Input, Output, Context> tsavoriteSession, out Output output)
            {
                Status status = !diskRequest.IsDefault()
                    ? tsavoriteKV.InternalCompletePendingRequestFromContext(tsavoriteSession, diskRequest, ref pendingContext, out var newDiskRequest)
                    : tsavoriteKV.CallInternalRead(tsavoriteSession, ref pendingContext, readAtAddress, ref pendingContext.key.Get(), ref pendingContext.input.Get(), ref pendingContext.output,
                                    ref readOptions, pendingContext.userContext, pendingContext.serialNum, out newDiskRequest);
                output = pendingContext.output;
                diskRequest = newDiskRequest;
                return status;
            }

            /// <inheritdoc/>
            public ValueTask<ReadAsyncResult<Input, Output, Context>> DoSlowOperation(TsavoriteKV<Key, Value> tsavoriteKV, ITsavoriteSession<Key, Value, Input, Output, Context> tsavoriteSession,
                                            PendingContext<Input, Output, Context> pendingContext, CancellationToken token)
                => SlowReadAsync(tsavoriteKV, tsavoriteSession, pendingContext, readAtAddress, readOptions, diskRequest, token);

            /// <inheritdoc/>
            public bool HasPendingIO => !diskRequest.IsDefault();
        }

        /// <summary>
        /// State storage for the completion of an async Read, or the result if the Read was completed synchronously
        /// </summary>
        public struct ReadAsyncResult<Input, TOutput, Context>
        {
            internal readonly AsyncOperationInternal<Input, TOutput, Context, ReadAsyncOperation<Input, TOutput, Context>, ReadAsyncResult<Input, TOutput, Context>> updateAsyncInternal;

            /// <summary>Current status of the RMW operation</summary>
            public Status Status { get; }

            /// <summary>Output of the RMW operation if current status is not pending</summary>
            public TOutput Output { get; }

            /// <summary>Metadata of the updated record</summary>
            public RecordMetadata RecordMetadata { get; }

            internal ReadAsyncResult(Status status, TOutput output, RecordMetadata recordMetadata)
            {
                Status = status;
                Output = output;
                RecordMetadata = recordMetadata;
                updateAsyncInternal = default;
            }

            internal ReadAsyncResult(TsavoriteKV<Key, Value> tsavoriteKV, ITsavoriteSession<Key, Value, Input, TOutput, Context> tsavoriteSession, PendingContext<Input, TOutput, Context> pendingContext,
                    long readAtAddress, ref ReadOptions readOptions, AsyncIOContext<Key, Value> diskRequest, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                Status = new(StatusCode.Pending);
                Output = default;
                RecordMetadata = default;
                updateAsyncInternal = new AsyncOperationInternal<Input, TOutput, Context, ReadAsyncOperation<Input, TOutput, Context>, ReadAsyncResult<Input, TOutput, Context>>(
                                        tsavoriteKV, tsavoriteSession, pendingContext, exceptionDispatchInfo, new ReadAsyncOperation<Input, TOutput, Context>(diskRequest, readAtAddress, ref readOptions));
            }

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
                var readAsyncResult = updateAsyncInternal.CompleteSync();
                recordMetadata = readAsyncResult.RecordMetadata;
                return (readAsyncResult.Status, readAsyncResult.Output);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<ReadAsyncResult<Input, Output, Context>> ReadAsync<Input, Output, Context>(ITsavoriteSession<Key, Value, Input, Output, Context> tsavoriteSession,
            ref Key key, ref Input input, ref ReadOptions readOptions, Context context, long serialNo, CancellationToken token, bool noKey = false)
        {
            var pcontext = new PendingContext<Input, Output, Context>(tsavoriteSession.Ctx.ReadCopyOptions, ref readOptions, isAsync: true, noKey: noKey);
            var diskRequest = default(AsyncIOContext<Key, Value>);

            tsavoriteSession.UnsafeResumeThread();
            try
            {
                Output output = default;
                var status = CallInternalRead(tsavoriteSession, ref pcontext, readAtAddress: 0L, ref key, ref input, ref output, ref readOptions, context, serialNo, out diskRequest);
                if (!status.IsPending)
                    return new ValueTask<ReadAsyncResult<Input, Output, Context>>(new ReadAsyncResult<Input, Output, Context>(status, output, new RecordMetadata(pcontext.recordInfo, pcontext.logicalAddress)));
            }
            finally
            {
                Debug.Assert(serialNo >= tsavoriteSession.Ctx.serialNum, "Operation serial numbers must be non-decreasing");
                tsavoriteSession.Ctx.serialNum = serialNo;
                tsavoriteSession.UnsafeSuspendThread();
            }

            return SlowReadAsync(this, tsavoriteSession, pcontext, readAtAddress: 0, readOptions, diskRequest, token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<ReadAsyncResult<Input, Output, Context>> ReadAtAddressAsync<Input, Output, Context>(ITsavoriteSession<Key, Value, Input, Output, Context> tsavoriteSession,
            long readAtAddress, ref Key key, ref Input input, ref ReadOptions readOptions, Context context, long serialNo, CancellationToken token, bool noKey = false)
        {
            var pcontext = new PendingContext<Input, Output, Context>(tsavoriteSession.Ctx.ReadCopyOptions, ref readOptions, isAsync: true, noKey: noKey);
            var diskRequest = default(AsyncIOContext<Key, Value>);

            tsavoriteSession.UnsafeResumeThread();
            try
            {
                Output output = default;
                var status = CallInternalRead(tsavoriteSession, ref pcontext, readAtAddress, ref key, ref input, ref output, ref readOptions, context, serialNo, out diskRequest);
                if (!status.IsPending)
                    return new ValueTask<ReadAsyncResult<Input, Output, Context>>(new ReadAsyncResult<Input, Output, Context>(status, output, new RecordMetadata(pcontext.recordInfo, pcontext.logicalAddress)));
            }
            finally
            {
                Debug.Assert(serialNo >= tsavoriteSession.Ctx.serialNum, "Operation serial numbers must be non-decreasing");
                tsavoriteSession.Ctx.serialNum = serialNo;
                tsavoriteSession.UnsafeSuspendThread();
            }

            return SlowReadAsync(this, tsavoriteSession, pcontext, readAtAddress, readOptions, diskRequest, token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status CallInternalRead<Input, Output, Context>(ITsavoriteSession<Key, Value, Input, Output, Context> tsavoriteSession,
                ref PendingContext<Input, Output, Context> pcontext, long readAtAddress, ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, Context context, long serialNo,
                out AsyncIOContext<Key, Value> diskRequest)
        {
            OperationStatus internalStatus;
            var keyHash = readOptions.KeyHash ?? comparer.GetHashCode64(ref key);
            do
            {
                if (readAtAddress == 0)
                    internalStatus = InternalRead(ref key, keyHash, ref input, ref output, context, serialNo, ref pcontext, tsavoriteSession);
                else
                    internalStatus = InternalReadAtAddress(readAtAddress, ref key, ref input, ref output, ref readOptions, context, serialNo, ref pcontext, tsavoriteSession);
            }
            while (HandleImmediateRetryStatus(internalStatus, tsavoriteSession, ref pcontext));

            return HandleOperationStatus(tsavoriteSession.Ctx, ref pcontext, internalStatus, out diskRequest);
        }

        private static async ValueTask<ReadAsyncResult<Input, Output, Context>> SlowReadAsync<Input, Output, Context>(
            TsavoriteKV<Key, Value> @this, ITsavoriteSession<Key, Value, Input, Output, Context> tsavoriteSession,
            PendingContext<Input, Output, Context> pcontext, long readAtAddress, ReadOptions readOptions, AsyncIOContext<Key, Value> diskRequest, CancellationToken token = default)
        {
            ExceptionDispatchInfo exceptionDispatchInfo;
            (diskRequest, exceptionDispatchInfo) = await WaitForFlushOrIOCompletionAsync(@this, tsavoriteSession.Ctx, pcontext.flushEvent, diskRequest, token);
            pcontext.flushEvent = default;
            return new ReadAsyncResult<Input, Output, Context>(@this, tsavoriteSession, pcontext, readAtAddress, ref readOptions, diskRequest, exceptionDispatchInfo);
        }
    }
}