// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status InternalContainsKeyInMemory<TInput, TOutput, TContext, TSessionFunctionsWrapper>(
            ReadOnlySpan<byte> key, ReadOnlySpan<byte> namespaceBytes, TSessionFunctionsWrapper sessionFunctions, out long logicalAddress, long fromAddress = -1)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(storeFunctions.GetKeyHashCode64(key, namespaceBytes));

            if (sessionFunctions.Ctx.phase == Phase.IN_PROGRESS_GROW)
                SplitBuckets(stackCtx.hei.hash);

            if (FindTag(ref stackCtx.hei))
            {
                stackCtx.SetRecordSourceToHashEntry(hlogBase);

                if (UseReadCache)
                    SkipReadCache(ref stackCtx, out _);

                if (fromAddress < hlogBase.HeadAddress)
                    fromAddress = hlogBase.HeadAddress;

                if (TraceBackForKeyMatch(key, namespaceBytes, ref stackCtx.recSrc, fromAddress) && !stackCtx.recSrc.GetInfo().Tombstone)
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