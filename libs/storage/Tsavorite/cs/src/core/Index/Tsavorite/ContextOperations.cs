// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;

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
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<TKernelSession>
        {
            // Called by Single Tsavorite instance configurations, so we must do Kernel entry, lock, etc. here
            var keyHash = readOptions.KeyHash ?? storeFunctions.GetKeyHashCode64(ref key);
            HashEntryInfo hei = default;

            try
            {
                var status = Kernel.EnterForRead<TKernelSession, TKeyLocker, TEpochGuard>(ref kernelSession, keyHash, partitionId, out hei);
                if (status.NotFound)
                {
                    output = default;
                    recordMetadata = default;
                    return status;
                }

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
            where TKeyLocker : struct, IKeyLocker
        {
            // Called by Single or Dual Tsavorite instances; Kernel entry/exit is handled by caller
            OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = new(ref hei);
            stackCtx.SetRecordSourceToHashEntry(hlogBase);

            var pcontext = new PendingContext<TInput, TOutput, TContext>(sessionFunctions.ExecutionCtx.ReadCopyOptions, ref readOptions);
            OperationStatus internalStatus;

            do
                internalStatus = InternalRead<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(ref stackCtx, ref key, ref input, ref output, context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus<TInput, TOutput, TContext, TKeyLocker>(ref hei, internalStatus, sessionFunctions.ExecutionCtx, ref pcontext));

            recordMetadata = new(pcontext.recordInfo, pcontext.logicalAddress);
            return HandleOperationStatus(sessionFunctions.ExecutionCtx, ref pcontext, internalStatus);
        }

        internal Status EnterKernelForReadAtAddress<TKernelSession, TKeyLocker, TEpochGuard>(
                ref TKernelSession kernelSession, ushort partitionId, long address, ref TKey expectedKey, long keyHash, bool isNoKey, out HashEntryInfo hei)
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : IEpochGuard<TKernelSession>
        {
            // The procedure for ReadAtAddress is complicated by the NoKey option (read at an address without knowing its key), because we must lock the bucket
            // for mutable records. When we do not have the key and the record is above ReadOnlyAddress, we must loop on:
            //    obtain the record at that address
            //    calculate its key hashcode
            //    lock the key's bucket
            //    calculate its key hashcode again
            //    if they match, we're done and keep the lock, else unlock and loop again
            // This is necessary due to revivification.

            hei = default;
            if (address < hlogBase.BeginAddress)
                return new(StatusCode.NotFound);

            // Not in memory, so ReadAtAddress() will issue pending IO.
            if (address < hlogBase.HeadAddress)
                return new(StatusCode.Found);

            // This can throw on lock timeout so make sure it is called within try/finally to ensure epoch is released
            TEpochGuard.BeginUnsafe(ref kernelSession);

            // CheckHashTableGrowth(); TODO

            // In mutable memory, so get the key, lock, verify the record was not deleted and re-keyed (i.e. revivification), and return.
            while (true)
            {
                var physicalAddress = hlog.GetPhysicalAddress(address);
                ref var recordKey = ref hlog.GetKey(physicalAddress);

                // If reviv freelist is active and we have an expected key, verify it.
                var checkKey = !isNoKey && RevivificationManager.UseFreeRecordPool;
                if (checkKey && !storeFunctions.KeysEqual(ref expectedKey, ref recordKey))
                    return new(StatusCode.NotFound);

                hei = new(GetKeyHash(recordKey), partitionId);
                if (!Kernel.hashTable.FindTag(ref hei))
                {
                    TEpochGuard.EndUnsafe(ref kernelSession);
                    return new(StatusCode.NotFound);
                }

                while (!TKeyLocker.TryLockTransientShared(Kernel, ref hei))
                {
                    kernelSession.Refresh<TKeyLocker>(ref hei);
                    _ = Thread.Yield();
                }

                var keyHash2 = GetKeyHash(ref hlog.GetKey(physicalAddress));
                if (hei.hash != keyHash2 || (checkKey && !storeFunctions.KeysEqual(ref expectedKey, ref recordKey)))
                {
                    // Key is not as expected; unlock and return false
                    TKeyLocker.UnlockTransientShared(Kernel, ref hei);
                    return new(StatusCode.NotFound);
                }
                return new(StatusCode.Found);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextReadAtAddress<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKernelSession, TKeyLocker, TEpochGuard>(
                long address, ref TKey key, bool isNoKey, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata,
                TContext context, TSessionFunctionsWrapper sessionFunctions, ref TKernelSession kernelSession)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<TKernelSession>
        {
            // Called by Single Tsavorite instance configurations (this API is called only for specific store instances)
            HashEntryInfo hei = default;
            try
            {
                var status = EnterKernelForReadAtAddress<TKernelSession, TKeyLocker, TEpochGuard>(ref kernelSession, PartitionId, address, ref key, readOptions.KeyHash ?? GetKeyHash(ref key), isNoKey, out hei);
                if (status.NotFound)
                {
                    output = default;
                    recordMetadata = default;
                    return status;
                }

                return ContextReadAtAddress<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(ref hei, ref key, isNoKey, ref input, ref output, ref readOptions, out recordMetadata, context, sessionFunctions);
            }
            finally
            {
                Kernel.ExitForRead<TKernelSession, TKeyLocker, TEpochGuard>(ref kernelSession, ref hei);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextReadAtAddress<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(
                ref HashEntryInfo hei, ref TKey key, bool isNoKey, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata,
                TContext context, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKeyLocker : struct, IKeyLocker
        {
            // Called by Single or Dual Tsavorite instances; Kernel entry/exit is handled by caller
            OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = new(ref hei);
            stackCtx.SetRecordSourceToHashEntry(hlogBase);

            var pcontext = new PendingContext<TInput, TOutput, TContext>(sessionFunctions.ExecutionCtx.ReadCopyOptions, ref readOptions, noKey: true);
            OperationStatus internalStatus;
            do
                internalStatus = InternalReadAtAddress<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(ref stackCtx, ref key, ref input, ref output, ref readOptions, context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus<TInput, TOutput, TContext, TKeyLocker>(ref stackCtx.hei, internalStatus, sessionFunctions.ExecutionCtx, ref pcontext));

            recordMetadata = new(pcontext.recordInfo, pcontext.logicalAddress);
            return HandleOperationStatus(sessionFunctions.ExecutionCtx, ref pcontext, internalStatus);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextUpsert<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKernelSession, TKeyLocker, TEpochGuard>(
                ref TKey key, long keyHash, ref TInput input, ref TValue value, 
                ref TOutput output, out RecordMetadata recordMetadata, TContext context, TSessionFunctionsWrapper sessionFunctions, ref TKernelSession kernelSession)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<TKernelSession>
        {
            // Called by Single Tsavorite instance configurations, so we must do Kernel entry, lock, etc. here
            HashEntryInfo hei = default;

            try
            {
                var status = Kernel.EnterForUpdate<TKernelSession, TKeyLocker, TEpochGuard>(ref kernelSession, keyHash, partitionId, Log.BeginAddress, out hei);
                if (status.NotFound)
                {
                    output = default;
                    recordMetadata = default;
                    return status;
                }

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
            where TKeyLocker : struct, IKeyLocker
        {
            // Called by Single or Dual Tsavorite instances; Kernel entry/exit is handled by caller
            OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = new(ref hei);
            stackCtx.SetRecordSourceToHashEntry(hlogBase);

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
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<TKernelSession>
        {
            // Called by Single Tsavorite instance configurations, so we must do Kernel entry, lock, etc. here
            HashEntryInfo hei = default;

            try
            {
                var status = Kernel.EnterForUpdate<TKernelSession, TKeyLocker, TEpochGuard>(ref kernelSession, keyHash, partitionId, Log.BeginAddress, out hei);
                if (status.NotFound)
                {
                    output = default;
                    recordMetadata = default;
                    return status;
                }

                return ContextRMW<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(ref hei, ref key, ref input, ref output, out recordMetadata, context, sessionFunctions);
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
            where TKeyLocker : struct, IKeyLocker
        {
            // Called by Single or Dual Tsavorite instances; Kernel entry/exit is handled by caller
            OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = new(ref hei);
            stackCtx.SetRecordSourceToHashEntry(hlogBase);

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
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<TKernelSession>
        {
            // Called by Single Tsavorite instance configurations, so we must do Kernel entry, lock, etc. here
            HashEntryInfo hei = default;

            try
            {
                var status = Kernel.EnterForUpdate<TKernelSession, TKeyLocker, TEpochGuard>(ref kernelSession, keyHash, partitionId, Log.BeginAddress, out hei);
                if (status.NotFound)
                    return status;

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
            where TKeyLocker : struct, IKeyLocker
        {
            // Called by Single or Dual Tsavorite instances; Kernel entry/exit is handled by caller
            OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = new(ref hei);
            stackCtx.SetRecordSourceToHashEntry(hlogBase);

            var pcontext = new PendingContext<TInput, TOutput, TContext>();

            OperationStatus internalStatus;
            do
                internalStatus = InternalDelete<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(ref stackCtx, ref key, context, ref pcontext, sessionFunctions);
            while (HandleImmediateRetryStatus<TInput, TOutput, TContext, TKeyLocker>(ref hei, internalStatus, sessionFunctions.ExecutionCtx, ref pcontext));

            return HandleOperationStatus(sessionFunctions.ExecutionCtx, ref pcontext, internalStatus);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextResetModified<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKernelSession, TKeyLocker, TEpochGuard>(ref TKey key, TSessionFunctionsWrapper sessionFunctions,
                ref TKernelSession kernelSession, out RecordInfo modifiedInfo, bool reset = true)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IEpochGuard<TKernelSession>
        {
            // Called by Single Tsavorite instance configurations, so we must do Kernel entry, lock, etc. here
            HashEntryInfo hei = default;

            try
            {
                var status = Kernel.EnterForUpdate<TKernelSession, TKeyLocker, TEpochGuard>(ref kernelSession, GetKeyHash(ref key), partitionId, Log.BeginAddress, out hei);
                if (status.NotFound)
                {
                    modifiedInfo = default;
                    return status;
                }

                return ContextResetModified<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(sessionFunctions, ref hei, ref key, out modifiedInfo, reset);
            }
            finally
            {
                Kernel.ExitForRead<TKernelSession, TKeyLocker, TEpochGuard>(ref kernelSession, ref hei);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextResetModified<TInput, TOutput, TContext, TSessionFunctionsWrapper, TKeyLocker>(TSessionFunctionsWrapper sessionFunctions, ref HashEntryInfo hei, ref TKey key,
                out RecordInfo modifiedInfo, bool reset = true)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TKeyLocker : struct, IKeyLocker
        {
            // Called by Single or Dual Tsavorite instances; Kernel entry/exit is handled by caller
            OperationStatus internalStatus;
            do
                internalStatus = InternalModifiedBitOperation(ref hei, ref key, out modifiedInfo, reset);
            while (HandleImmediateNonPendingRetryStatus(ref hei, internalStatus, sessionFunctions.ExecutionCtx));

            return new(StatusCode.Found);
        }
    }
}