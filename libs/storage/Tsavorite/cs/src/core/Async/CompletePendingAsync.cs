// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// The Tsavorite key-value store
    /// </summary>
    /// <typeparam name="Key">Key</typeparam>
    /// <typeparam name="Value">Value</typeparam>
    public partial class TsavoriteKV<Key, Value> : TsavoriteBase
    {
        /// <summary>
        /// Check if at least one (sync) request is ready for CompletePending to operate on
        /// </summary>
        /// <param name="sessionCtx"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        internal static ValueTask ReadyToCompletePendingAsync<Input, Output, Context>(TsavoriteExecutionContext<Input, Output, Context> sessionCtx, CancellationToken token = default)
            => sessionCtx.WaitPendingAsync(token);

        /// <summary>
        /// Complete outstanding pending operations that were issued synchronously
        /// Async operations (e.g., ReadAsync) need to be completed individually
        /// </summary>
        /// <returns></returns>
        internal async ValueTask CompletePendingAsync<Input, Output, Context, TsavoriteSession>(TsavoriteSession tsavoriteSession,
                                      CancellationToken token, CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            while (true)
            {
                tsavoriteSession.UnsafeResumeThread();
                try
                {
                    InternalCompletePendingRequests(tsavoriteSession, completedOutputs);
                }
                finally
                {
                    tsavoriteSession.UnsafeSuspendThread();
                }

                await tsavoriteSession.Ctx.WaitPendingAsync(token).ConfigureAwait(false);

                if (tsavoriteSession.Ctx.HasNoPendingRequests) return;

                InternalRefresh<Input, Output, Context, TsavoriteSession>(tsavoriteSession);

                Thread.Yield();
            }
        }
    }
}