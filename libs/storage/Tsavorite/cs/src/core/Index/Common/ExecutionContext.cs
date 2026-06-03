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

            public SystemState SessionState;
            internal long version => SessionState.Version;
            public Phase phase => SessionState.Phase;
            public long txnVersion;

            public long totalPending;
            public readonly Dictionary<long, PendingContextHolder<TInput, TOutput, TContext>> ioPendingRequests;
            public readonly AsyncCountDown pendingReads;
            public readonly AsyncQueue<AsyncGetFromDiskResult<AsyncIOContext>> readyResponses;

            /// <summary>
            /// Per-session pool of <see cref="AsyncIOContext"/> instances. Each pending IO rents one upfront,
            /// fills its fields in place, then returns it after completion processing.
            /// </summary>
            public readonly Stack<AsyncIOContext> asyncIOContextPool;

            /// <summary>
            /// Per-session pool of <see cref="PendingContextHolder{TInput,TOutput,TContext}"/> wrappers.
            /// The wrapper lets the ioPendingRequests dictionary store an 8-byte reference rather than
            /// bulk-copying the ~200-byte PendingContext struct (which triggers Buffer.BulkMoveWithWriteBarrier
            /// on Dictionary.Add and was ~12-13% of worker CPU at single-thread libaio random reads).
            /// </summary>
            public readonly Stack<PendingContextHolder<TInput, TOutput, TContext>> pendingContextHolderPool;

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
            public int asyncPendingCount;
            internal RevivificationStats RevivificationStats = new();
            public bool isAcquiredTransactional;

            public TsavoriteExecutionContext(int sessionID)
            {
                SessionState = SystemState.Make(Phase.REST, 1);
                this.sessionID = sessionID;
                readyResponses = new AsyncQueue<AsyncGetFromDiskResult<AsyncIOContext>>();
                ioPendingRequests = new Dictionary<long, PendingContextHolder<TInput, TOutput, TContext>>();
                heapContainerPool = new Stack<IHeapContainer<TInput>>();
                asyncIOContextPool = new Stack<AsyncIOContext>();
                pendingContextHolderPool = new Stack<PendingContextHolder<TInput, TOutput, TContext>>();
                pendingReads = new AsyncCountDown();
                isAcquiredTransactional = false;
            }

            /// <summary>
            /// Rent a pre-allocated <see cref="AsyncIOContext"/> from the per-session pool. Single-threaded
            /// per session, so non-concurrent <see cref="Stack{T}"/> is safe. Caller fills the instance's fields
            /// directly and returns it to the pool via <see cref="ReturnAsyncIOContext"/> after the IO completes.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal AsyncIOContext RentAsyncIOContext()
            {
                if (asyncIOContextPool.TryPop(out var ctx))
                    return ctx;
                return new AsyncIOContext();
            }

            /// <summary>Return an <see cref="AsyncIOContext"/> to the per-session pool after completion.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void ReturnAsyncIOContext(AsyncIOContext ctx)
            {
                ctx.Reset();
                asyncIOContextPool.Push(ctx);
            }

            /// <summary>Rent a <see cref="PendingContextHolder{TInput,TOutput,TContext}"/> from the per-session pool.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal PendingContextHolder<TInput, TOutput, TContext> RentPendingContextHolder()
            {
                if (pendingContextHolderPool.TryPop(out var holder))
                    return holder;
                return new PendingContextHolder<TInput, TOutput, TContext>();
            }

            /// <summary>Return a <see cref="PendingContextHolder{TInput,TOutput,TContext}"/> to the per-session pool.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void ReturnPendingContextHolder(PendingContextHolder<TInput, TOutput, TContext> holder)
            {
                holder.value = default;
                pendingContextHolderPool.Push(holder);
            }

            public int SyncIoPendingCount => ioPendingRequests.Count - asyncPendingCount;

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