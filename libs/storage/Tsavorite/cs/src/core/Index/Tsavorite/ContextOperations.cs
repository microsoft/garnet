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
        internal Status ContextRead<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKernelSession, TKeyLocker, TEpochGuard>(
                ref TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext context,
                TSessionFunctionsWrapper sessionFunctions, ref TKernelSession kernelSession)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<TKernelSession>
        {
            // Called by Single Tsavorite instance configurations, so we must do Kernel entry, lock, etc. here
            var keyHash = readOptions.KeyHash ?? storeFunctions.GetKeyHashCode64(ref key);
            var status = Kernel.EnterForRead<TKernelSession, TKeyLocker, TEpochGuard>(ref kernelSession, keyHash, partitionId, out var hei);
            if (status.NotFound)
            {
                output = default;
                recordMetadata = default;
                return status;
            }
            try
            {
                return ContextRead<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(
                        ref hei, ref key, ref input, ref output, ref readOptions, out recordMetadata, context, sessionFunctions);
            }
            finally
            {
                Kernel.ExitForRead<TKernelSession, TKeyLocker, TEpochGuard>(ref kernelSession, ref hei);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextRead<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(
                ref HashEntryInfo hei, ref TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext context,
                TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKeyLocker : struct, ISessionLocker
        {
            // Called by Single or Dual Tsavorite instances; Kernel entry/exit is handled by caller
            OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = new(ref hei);
            var pcontext = new PendingContext<TInput, TOutput, TContext>(sessionFunctions.ExecutionCtx.ReadCopyOptions, ref readOptions);
            OperationStatus internalStatus;

            do
                internalStatus = InternalRead<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(ref stackCtx, ref key, ref input, ref output, context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus<TInput, TOutput, TContext, TKeyLocker>(ref hei, internalStatus, sessionFunctions.ExecutionCtx, ref pcontext));

            recordMetadata = new(pcontext.recordInfo, pcontext.logicalAddress);
            return HandleOperationStatus(sessionFunctions.ExecutionCtx, ref pcontext, internalStatus);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextReadAtAddress<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKernelSession, TKeyLocker, TEpochGuard>(
                long address, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata,
                TContext context, TSessionFunctionsWrapper sessionFunctions, ref TKernelSession kernelSession)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<TKernelSession>
        {
            // Called by Single Tsavorite instance configurations (this API is called only for specific store instances)
            var pcontext = new PendingContext<TInput, TOutput, TContext>(sessionFunctions.ExecutionCtx.ReadCopyOptions, ref readOptions, noKey: true);
            TKey key = default;
            return ContextReadAtAddress<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKernelSession, TKeyLocker, TEpochGuard>(
                    address, ref key, ref input, ref output, ref readOptions, out recordMetadata, context, ref pcontext, sessionFunctions, ref kernelSession);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextReadAtAddress<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKernelSession, TKeyLocker, TEpochGuard>(
                long address, ref TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, 
                TContext context, TSessionFunctionsWrapper sessionFunctions, ref TKernelSession kernelSession)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<TKernelSession>
        {
            // Called by Single Tsavorite instance configurations (this API is called only for specific store instances)
            var pcontext = new PendingContext<TInput, TOutput, TContext>(sessionFunctions.ExecutionCtx.ReadCopyOptions, ref readOptions, noKey: false);
            return ContextReadAtAddress<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKernelSession, TKeyLocker, TEpochGuard>(
                    address, ref key, ref input, ref output, ref readOptions, out recordMetadata, context, ref pcontext, sessionFunctions, ref kernelSession);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status ContextReadAtAddress<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKernelSession, TKeyLocker, TEpochGuard>(
                long address, ref TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata,
                TContext context, ref PendingContext<TInput, TOutput, TContext> pcontext, TSessionFunctionsWrapper sessionFunctions, ref TKernelSession kernelSession)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<TKernelSession>
        {
            // Called by Single Tsavorite instance configurations (this API is called only for specific store instances), so we must do Kernel entry, lock, etc. here
            var keyHash = readOptions.KeyHash ?? storeFunctions.GetKeyHashCode64(ref key);
            var status = Kernel.EnterForRead<TKernelSession, TKeyLocker, TEpochGuard>(ref kernelSession, keyHash, partitionId, out var hei);
            if (status.NotFound)
            {
                output = default;
                recordMetadata = default;
                return status;
            }
            OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = new(ref hei);

            try
            {
                OperationStatus internalStatus;
                do
                    internalStatus = InternalReadAtAddress<TKernelSession, TKeyLocker>(ref stackCtx, address, ref key, ref input, ref output, ref readOptions, context, ref pcontext, sessionFunctions, ref kernelSession);
                while (HandleImmediateRetryStatus<TInput, TOutput, TContext, TKeyLocker>(ref stackCtx, internalStatus, sessionFunctions.ExecutionCtx, ref pcontext));

                recordMetadata = new(pcontext.recordInfo, pcontext.logicalAddress);
                return HandleOperationStatus(sessionFunctions.ExecutionCtx, ref pcontext, internalStatus);
            }
            finally
            {
                Kernel.ExitForRead<TKernelSession, TKeyLocker, TEpochGuard>(ref kernelSession, ref hei);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextUpsert<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKernelSession, TKeyLocker, TEpochGuard>(
                ref TKey key, long keyHash, ref TInput input, ref TValue value, 
                ref TOutput output, out RecordMetadata recordMetadata, TContext context, TSessionFunctionsWrapper sessionFunctions, ref TKernelSession kernelSession)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<TKernelSession>
        {
            // Called by Single Tsavorite instance configurations, so we must do Kernel entry, lock, etc. here
            var status = Kernel.EnterForUpdate<TKernelSession, TKeyLocker, TEpochGuard>(ref kernelSession, keyHash, partitionId, Log.BeginAddress, out var hei);
            if (status.NotFound)
            {
                output = default;
                recordMetadata = default;
                return status;
            }
 
            try
            {
                return ContextUpsert<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(
                        ref hei, ref key, ref input, ref value, ref output, out recordMetadata, context, sessionFunctions);
            }
            finally
            {
                Kernel.ExitForRead<TKernelSession, TKeyLocker, TEpochGuard>(ref kernelSession, ref hei);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextUpsert<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(
                ref HashEntryInfo hei, ref TKey key, ref TInput input, ref TValue value,
                ref TOutput output, out RecordMetadata recordMetadata, TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKeyLocker : struct, ISessionLocker
        {
            // Called by Single or Dual Tsavorite instances; Kernel entry/exit is handled by caller
            OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = new(ref hei);
            var pcontext = new PendingContext<TInput, TOutput, TContext>();

            OperationStatus internalStatus;
            do
                internalStatus = InternalUpsert<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(ref stackCtx, ref key, ref input, ref value, ref output, ref context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus<TInput, TOutput, TContext, TKeyLocker>(ref hei, internalStatus, sessionFunctions.ExecutionCtx, ref pcontext));

            recordMetadata = new(pcontext.recordInfo, pcontext.logicalAddress);
            return HandleOperationStatus(sessionFunctions.ExecutionCtx, ref pcontext, internalStatus);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextRMW<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKernelSession, TKeyLocker, TEpochGuard>(
                ref TKey key, long keyHash, ref TInput input, 
                ref TOutput output, out RecordMetadata recordMetadata, TContext context, TSessionFunctionsWrapper sessionFunctions, ref TKernelSession kernelSession)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<TKernelSession>
        {
            // Called by Single Tsavorite instance configurations, so we must do Kernel entry, lock, etc. here
            var status = Kernel.EnterForUpdate<TKernelSession, TKeyLocker, TEpochGuard>(ref kernelSession, keyHash, partitionId, Log.BeginAddress, out var hei);
            if (status.NotFound)
            {
                output = default;
                recordMetadata = default;
                return status;
            }

            try
            {
                return ContextRMW<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(
                        ref hei, ref key, ref input, ref output, out recordMetadata, context, sessionFunctions);
            }
            finally
            {
                Kernel.ExitForRead<TKernelSession, TKeyLocker, TEpochGuard>(ref kernelSession, ref hei);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextRMW<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(
                ref HashEntryInfo hei, ref TKey key, ref TInput input,
                ref TOutput output, out RecordMetadata recordMetadata, TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKeyLocker : struct, ISessionLocker
        {
            // Called by Single or Dual Tsavorite instances; Kernel entry/exit is handled by caller
            OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = new(ref hei);
            var pcontext = new PendingContext<TInput, TOutput, TContext>();

            OperationStatus internalStatus;
            do
                internalStatus = InternalRMW<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(ref stackCtx, ref key, ref input, ref output, context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus<TInput, TOutput, TContext, TKeyLocker>(ref hei, internalStatus, sessionFunctions.ExecutionCtx, ref pcontext));

            recordMetadata = new(pcontext.recordInfo, pcontext.logicalAddress);
            return HandleOperationStatus(sessionFunctions.ExecutionCtx, ref pcontext, internalStatus);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextDelete<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKernelSession, TKeyLocker, TEpochGuard>(
                ref TKey key, long keyHash, TContext context, TSessionFunctionsWrapper sessionFunctions, ref TKernelSession kernelSession)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<TKernelSession>
        {
            // Called by Single Tsavorite instance configurations, so we must do Kernel entry, lock, etc. here
            var status = Kernel.EnterForUpdate<TKernelSession, TKeyLocker, TEpochGuard>(ref kernelSession, keyHash, partitionId, Log.BeginAddress, out var hei);
            if (status.NotFound)
                return status;

            try
            {
                return ContextDelete<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(ref hei, ref key, context, sessionFunctions);
            }
            finally
            {
                Kernel.ExitForRead<TKernelSession, TKeyLocker, TEpochGuard>(ref kernelSession, ref hei);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextDelete<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(
                ref HashEntryInfo hei, ref TKey key, TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKeyLocker : struct, ISessionLocker
        {
            // Called by Single or Dual Tsavorite instances; Kernel entry/exit is handled by caller
            OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = new(ref hei);
            var pcontext = new PendingContext<TInput, TOutput, TContext>();

            OperationStatus internalStatus;
            do
                internalStatus = InternalDelete<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(ref stackCtx, ref key, context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus<TInput, TOutput, TContext, TKeyLocker>(ref hei, internalStatus, sessionFunctions.ExecutionCtx, ref pcontext));

            return HandleOperationStatus(sessionFunctions.ExecutionCtx, ref pcontext, internalStatus);
        }
    }
}