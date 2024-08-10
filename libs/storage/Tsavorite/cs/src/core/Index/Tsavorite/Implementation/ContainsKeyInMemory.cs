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
        internal Status InternalContainsKeyInMemory<Input, Output, Context, TSessionFunctionsWrapper>(
            ref TKey key, TSessionFunctionsWrapper sessionFunctions, out long logicalAddress, long fromAddress = -1)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, Input, Output, Context, TStoreFunctions, TAllocator>
        {
            OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = new(storeFunctions.GetKeyHashCode64(ref key));

            if (sessionFunctions.Ctx.phase == Phase.IN_PROGRESS_GROW)
                SplitBuckets(stackCtx.hei.hash);

            if (FindTag(ref stackCtx.hei))
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