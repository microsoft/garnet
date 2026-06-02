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
            public readonly Dictionary<long, PendingContext<TInput, TOutput, TContext>> ioPendingRequests;
            public readonly AsyncCountDown pendingReads;
            public readonly AsyncQueue<AsyncGetFromDiskResult<AsyncIOContext>> readyResponses;

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
                ioPendingRequests = new Dictionary<long, PendingContext<TInput, TOutput, TContext>>();
                heapContainerPool = new Stack<IHeapContainer<TInput>>();
                pendingReads = new AsyncCountDown();
                isAcquiredTransactional = false;
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