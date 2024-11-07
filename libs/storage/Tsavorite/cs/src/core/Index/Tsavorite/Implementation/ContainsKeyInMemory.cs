// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status InternalContainsKeyInMemory<TInput, TOutput, TContext, TSessionFunctionsWrapper>(
            ref TKey key, TSessionFunctionsWrapper sessionFunctions, out long logicalAddress, long fromAddress = -1)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            HashEntryInfo hei = new(storeFunctions.GetKeyHashCode64(ref key), partitionId);
            OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = new(ref hei);

            if (sessionFunctions.ExecutionCtx.phase == Phase.IN_PROGRESS_GROW && !sessionFunctions.IsDual)
                SplitBuckets(stackCtx.hei.hash);

            if (Kernel.hashTable.FindTag(ref stackCtx.hei))
            {
                stackCtx.SetRecordSourceToHashEntry(hlogBase);

                if (UseReadCache)
                    SkipReadCache(ref stackCtx, out _);

                if (fromAddress < hlogBase.HeadAddress)
                    fromAddress = hlogBase.HeadAddress;

                if (TryFindRecordInMainLog(ref key, ref stackCtx, fromAddress) && !stackCtx.recSrc.GetInfo().Tombstone)
                {
                    logicalAddress = stackCtx.recSrc.LogicalAddress;
                    return new(StatusCode.Found);
                }
                logicalAddress = 0;
                return new(StatusCode.NotFound);
            }

            // no tag found
            logicalAddress = 0;
            return new(StatusCode.NotFound);
        }
    }
}