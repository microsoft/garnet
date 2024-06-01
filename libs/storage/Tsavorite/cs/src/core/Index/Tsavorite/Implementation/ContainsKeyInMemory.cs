// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<Key, Value> : TsavoriteBase
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status InternalContainsKeyInMemory<Input, Output, Context, TSessionFunctionsWrapper>(
            ref Key key, TSessionFunctionsWrapper sessionFunctions, out long logicalAddress, long fromAddress = -1)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));

            if (sessionFunctions.Ctx.phase == Phase.IN_PROGRESS_GROW)
                SplitBuckets(stackCtx.hei.hash);

            if (FindTag(ref stackCtx.hei))
            {
                stackCtx.SetRecordSourceToHashEntry(hlog);

                if (UseReadCache)
                    SkipReadCache(ref stackCtx, out _);

                if (fromAddress < hlog.HeadAddress)
                    fromAddress = hlog.HeadAddress;

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