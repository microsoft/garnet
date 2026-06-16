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
        /// fields the allocator/completion path uses) plus the session-typed <see cref="basePendingContext"/>
        /// (in-memory hot-path bookkeeping) and <see cref="slot"/> (the pending-only payload carrying the
        /// key/input/output/diskLogRecord/etc.). Rented from the per-session pool on issue and returned on
        /// drain, so a pending op is one object — there is no separate ioPendingRequests dictionary entry
        /// keyed by id. The completion thread sees only the <see cref="AsyncIOContext"/> base; the run-thread
        /// drain downcasts to reach <see cref="basePendingContext"/> and <see cref="slot"/>.
        /// </summary>
        internal sealed class PendingIoContext<TInput, TOutput, TContext> : AsyncIOContext
        {
            /// <summary>The in-memory hot-path state copied here on RECORD_ON_DISK. Carries logical addresses,
            /// flags, retry/flush state. Kept small so the in-memory caller's stack copy is cheap.</summary>
            internal PendingContext<TInput, TOutput, TContext> basePendingContext;

            /// <summary>The pending-only payload (key, input, output, userContext, diskLogRecord, conditional-op
            /// addresses, scan-cursor state). Touched only after the operation goes pending; the in-memory hot
            /// path never references it.</summary>
            internal PendingIoSlot<TInput, TOutput, TContext> slot;
        }
    }
}