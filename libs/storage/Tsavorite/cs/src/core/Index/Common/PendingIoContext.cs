// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    public partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>
        /// A single pending-IO operation: the non-generic <see cref="AsyncIOContext"/> (the device-facing
        /// fields the allocator/completion path uses) plus the session-typed
        /// <see cref="PendingContext{TInput, TOutput, TContext}"/> it carries. Rented from the per-session
        /// pool on issue and returned on drain, so a pending op is one object — there is no separate
        /// ioPendingRequests dictionary entry keyed by id. The completion thread sees only the
        /// <see cref="AsyncIOContext"/> base; the run-thread drain downcasts to reach
        /// <see cref="pendingContext"/>.
        /// </summary>
        internal sealed class PendingIoContext<TInput, TOutput, TContext> : AsyncIOContext
        {
            internal PendingContext<TInput, TOutput, TContext> pendingContext;
        }
    }
}
