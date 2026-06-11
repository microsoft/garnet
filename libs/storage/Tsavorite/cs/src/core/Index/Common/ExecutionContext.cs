// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    public partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        internal sealed class TsavoriteExecutionContext<TInput, TOutput, TContext>
        {
            internal readonly int sessionID;

            // Control automatic Read copy operations. These flags override flags specified at the TsavoriteKV level, but may be overridden on the individual Read() operations
            internal ReadCopyOptions ReadCopyOptions;

            /// <summary>Initial IO record size for disk reads; <see cref="KVSettings.UseDefaultInitialIORecordSize"/> means inherit from store level.</summary>
            internal int InitialIORecordSize = KVSettings.UseDefaultInitialIORecordSize;

            public SystemState SessionState;
            internal long version => SessionState.Version;
            public Phase phase => SessionState.Phase;
            public long txnVersion;

            public long totalPending;

            /// <summary>
            /// Number of pending-IO ops currently in flight for this session (replaces the former
            /// ioPendingRequests.Count). Incremented just before a device IO is issued
            /// (HandleOperationStatus RECORD_ON_DISK) and decremented when the completed op is drained,
            /// so it is never transiently zero across a re-pend hop.
            /// </summary>
            public int pendingCount;
            public readonly AsyncCountDown pendingReads;

            /// <summary>
            /// Queue of completed pending-IO ops awaiting run-thread processing. Typed as the non-generic
            /// <see cref="AsyncIOContext"/> base because the allocator's IO-completion callback that enqueues
            /// here is not generic; every entry is in fact a <see cref="PendingIoContext{TInput, TOutput, TContext}"/>,
            /// recovered by downcast when drained on a thread that has the session's type arguments in scope.
            /// </summary>
            public readonly AsyncQueue<AsyncIOContext> readyResponses;

            /// <summary>
            /// Per-session pool of <see cref="PendingIoContext{TInput, TOutput, TContext}"/> instances — the
            /// pending op carrying both the device-facing <see cref="AsyncIOContext"/> fields and the
            /// <see cref="PendingContext{TInput, TOutput, TContext}"/>. Each pending IO rents one upfront, fills
            /// its fields in place, then returns it after completion processing. The reference-typed op is itself
            /// carried across threads through the <see cref="readyResponses"/> queue, so pooling it avoids the
            /// per-IO struct copy (Buffer.BulkMoveWithWriteBarrier) the previous value-typed AsyncIOContext incurred.
            /// </summary>
            public readonly Stack<PendingIoContext<TInput, TOutput, TContext>> asyncIOContextPool;

            /// <summary>
            /// Per-session pool of <see cref="IHeapContainer{TInput}"/> wrappers
            /// (either <see cref="SpanByteHeapContainer"/> or <see cref="StandardHeapContainer{T}"/>,
            /// determined by <typeparamref name="TInput"/>). Each pending operation rents one when
            /// copying its input into the <see cref="PendingContext{TInput, TOutput, TContext}"/>;
            /// the wrapper returns itself to this stack on <see cref="System.IDisposable.Dispose"/>.
            /// Sessions are single-threaded, so a non-concurrent <see cref="Stack{T}"/> suffices and
            /// the pool naturally settles around the in-flight pending IO count.
            /// </summary>
            public readonly Stack<IHeapContainer<TInput>> heapContainerPool;

            internal RevivificationStats RevivificationStats = new();
            public bool isAcquiredTransactional;

            public TsavoriteExecutionContext(int sessionID)
            {
                SessionState = SystemState.Make(Phase.REST, 1);
                this.sessionID = sessionID;
                readyResponses = new AsyncQueue<AsyncIOContext>();
                heapContainerPool = new Stack<IHeapContainer<TInput>>();
                asyncIOContextPool = new Stack<PendingIoContext<TInput, TOutput, TContext>>();
                pendingReads = new AsyncCountDown();
                isAcquiredTransactional = false;
            }

            /// <summary>
            /// Rent a pre-allocated <see cref="PendingIoContext{TInput, TOutput, TContext}"/> from the per-session
            /// pool. Single-threaded per session, so non-concurrent <see cref="Stack{T}"/> is safe. Caller fills the
            /// instance's fields directly and returns it via <see cref="ReturnAsyncIOContext"/> after the IO completes.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal PendingIoContext<TInput, TOutput, TContext> RentAsyncIOContext()
            {
                if (asyncIOContextPool.TryPop(out var ctx))
                    return ctx;
                return new PendingIoContext<TInput, TOutput, TContext>();
            }

            /// <summary>Return a pending op to the per-session pool after completion.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void ReturnAsyncIOContext(PendingIoContext<TInput, TOutput, TContext> ctx)
            {
                // Drop the session-typed context (its input container / disk record were already disposed or
                // moved by the drain) so a pooled op retains no stale references, then reset the base fields.
                // Uncapped, matching the sibling per-session heapContainerPool with the same rent-on-IO-issue /
                // return-on-drain lifecycle; the pool self-bounds at this session's peak concurrent pending depth.
                ctx.pendingContext = default;
                ctx.Reset();
                asyncIOContextPool.Push(ctx);
            }

            public int SyncIoPendingCount => pendingCount;

            public bool IsInV1
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    return phase switch
                    {
                        Phase.IN_PROGRESS => true,
                        Phase.WAIT_INDEX_CHECKPOINT => true,
                        Phase.WAIT_FLUSH => true,
                        _ => false,
                    };
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void MergeReadCopyOptions(ReadCopyOptions storeCopyOptions, ReadCopyOptions copyOptions)
                => ReadCopyOptions = ReadCopyOptions.Merge(storeCopyOptions, copyOptions);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void MergeRevivificationStatsTo(ref RevivificationStats to, bool reset) => RevivificationStats.MergeTo(ref to, reset);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void ResetRevivificationStats() => RevivificationStats.Reset();

            public bool HasNoPendingRequests => SyncIoPendingCount == 0;

            public void WaitPending(LightEpoch epoch)
            {
                if (SyncIoPendingCount > 0)
                {
                    try
                    {
                        epoch.Suspend();
                        readyResponses.WaitForEntry();
                    }
                    finally
                    {
                        epoch.Resume();
                    }
                }
            }

            public async ValueTask WaitPendingAsync(CancellationToken token = default)
            {
                if (SyncIoPendingCount > 0)
                    await readyResponses.WaitForEntryAsync(token).ConfigureAwait(false);
            }

            public bool InNewVersion => phase < Phase.REST;
        }
    }
}