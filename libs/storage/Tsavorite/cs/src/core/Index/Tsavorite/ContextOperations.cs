// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// The Tsavorite Key/Value store class
    /// </summary>
    public partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase, IDisposable
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextRead<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref TKey key, ref TInput input, ref TOutput output, TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = new PendingContext<TInput, TOutput, TContext>(sessionFunctions.ExecutionCtx.ReadCopyOptions);
            OperationStatus internalStatus;
            var keyHash = storeFunctions.GetKeyHashCode64(ref key);

            do
                internalStatus = InternalRead(ref key, keyHash, ref input, ref output, context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions.ExecutionCtx, ref pcontext));

            var status = HandleOperationStatus(sessionFunctions.ExecutionCtx, ref pcontext, internalStatus);

            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextRead<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext context,
                TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = new PendingContext<TInput, TOutput, TContext>(sessionFunctions.ExecutionCtx.ReadCopyOptions, ref readOptions);
            OperationStatus internalStatus;
            var keyHash = readOptions.KeyHash ?? storeFunctions.GetKeyHashCode64(ref key);

            do
                internalStatus = InternalRead(ref key, keyHash, ref input, ref output, context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions.ExecutionCtx, ref pcontext));

            var status = HandleOperationStatus(sessionFunctions.ExecutionCtx, ref pcontext, internalStatus);
            recordMetadata = status.IsCompletedSuccessfully ? new(pcontext.recordInfo, pcontext.logicalAddress) : default;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextReadAtAddress<TInput, TOutput, TContext, TSessionFunctionsWrapper>(long address, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = new PendingContext<TInput, TOutput, TContext>(sessionFunctions.ExecutionCtx.ReadCopyOptions, ref readOptions, noKey: true);
            TKey key = default;
            return ContextReadAtAddress(address, ref key, ref input, ref output, ref readOptions, out recordMetadata, context, ref pcontext, sessionFunctions);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextReadAtAddress<TInput, TOutput, TContext, TSessionFunctionsWrapper>(long address, ref TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = new PendingContext<TInput, TOutput, TContext>(sessionFunctions.ExecutionCtx.ReadCopyOptions, ref readOptions, noKey: false);
            return ContextReadAtAddress(address, ref key, ref input, ref output, ref readOptions, out recordMetadata, context, ref pcontext, sessionFunctions);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status ContextReadAtAddress<TInput, TOutput, TContext, TSessionFunctionsWrapper>(long address, ref TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata,
                TContext context, ref PendingContext<TInput, TOutput, TContext> pcontext, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            OperationStatus internalStatus;
            do
                internalStatus = InternalReadAtAddress(address, ref key, ref input, ref output, ref readOptions, context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions.ExecutionCtx, ref pcontext));

            var status = HandleOperationStatus(sessionFunctions.ExecutionCtx, ref pcontext, internalStatus);
            recordMetadata = status.IsCompletedSuccessfully ? new(pcontext.recordInfo, pcontext.logicalAddress) : default;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextUpsert<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref TKey key, long keyHash, ref TInput input, ref TValue value, ref TOutput output, TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = default(PendingContext<TInput, TOutput, TContext>);
            OperationStatus internalStatus;

            do
                internalStatus = InternalUpsert(ref key, keyHash, ref input, ref value, ref output, ref context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions.ExecutionCtx, ref pcontext));

            var status = HandleOperationStatus(sessionFunctions.ExecutionCtx, ref pcontext, internalStatus);
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextUpsert<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref TKey key, long keyHash, ref TInput input, ref TValue value, ref TOutput output, out RecordMetadata recordMetadata,
                                                                            TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = default(PendingContext<TInput, TOutput, TContext>);
            OperationStatus internalStatus;

            do
                internalStatus = InternalUpsert(ref key, keyHash, ref input, ref value, ref output, ref context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions.ExecutionCtx, ref pcontext));

            var status = HandleOperationStatus(sessionFunctions.ExecutionCtx, ref pcontext, internalStatus);
            recordMetadata = status.IsCompletedSuccessfully ? new(pcontext.recordInfo, pcontext.logicalAddress) : default;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextRMW<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref TKey key, long keyHash, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata,
                                                                          TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = default(PendingContext<TInput, TOutput, TContext>);
            OperationStatus internalStatus;

            do
                internalStatus = InternalRMW(ref key, keyHash, ref input, ref output, ref context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions.ExecutionCtx, ref pcontext));

            var status = HandleOperationStatus(sessionFunctions.ExecutionCtx, ref pcontext, internalStatus);
            recordMetadata = status.IsCompletedSuccessfully ? new(pcontext.recordInfo, pcontext.logicalAddress) : default;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextDelete<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref TKey key, long keyHash, TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var pcontext = default(PendingContext<TInput, TOutput, TContext>);
            OperationStatus internalStatus;

            do
                internalStatus = InternalDelete(ref key, keyHash, ref context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus(internalStatus, sessionFunctions.ExecutionCtx, ref pcontext));

            var status = HandleOperationStatus(sessionFunctions.ExecutionCtx, ref pcontext, internalStatus);
            return status;
        }

    }
}