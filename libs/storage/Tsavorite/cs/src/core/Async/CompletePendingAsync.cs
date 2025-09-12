// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// The Tsavorite key-value store
    /// </summary>
    public partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>
        /// Check if at least one (sync) request is ready for CompletePending to operate on
        /// </summary>
        /// <param name="sessionCtx"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        internal static ValueTask ReadyToCompletePendingAsync<TInput, TOutput, TContext>(TsavoriteExecutionContext<TInput, TOutput, TContext> sessionCtx, CancellationToken token = default)
            => sessionCtx.WaitPendingAsync(token);

        /// <summary>
        /// Complete outstanding pending operations that were issued synchronously
        /// Async operations (e.g., ReadAsync) need to be completed individually
        /// </summary>
        /// <returns></returns>
        internal async ValueTask CompletePendingAsync<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                                      CancellationToken token, CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            while (true)
            {
                sessionFunctions.UnsafeResumeThread();
                try
                {
                    InternalCompletePendingRequests(sessionFunctions, completedOutputs);
                }
                finally
                {
                    sessionFunctions.UnsafeSuspendThread();
                }

                await sessionFunctions.Ctx.WaitPendingAsync(token).ConfigureAwait(false);
                if (sessionFunctions.Ctx.HasNoPendingRequests)
                    return;

                InternalRefresh<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions);
                Thread.Yield();
            }
        }
    }
}