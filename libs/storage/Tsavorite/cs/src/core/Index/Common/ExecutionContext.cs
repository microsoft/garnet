﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    public partial class TsavoriteKV<Key, Value> : TsavoriteBase
    {
        internal sealed class TsavoriteExecutionContext<Input, Output, Context>
        {
            internal int sessionID;
            internal string sessionName;

            // Control automatic Read copy operations. These flags override flags specified at the TsavoriteKV level, but may be overridden on the individual Read() operations
            internal ReadCopyOptions ReadCopyOptions;

            internal long version;
            public Phase phase;

            public bool[] markers;
            public long totalPending;
            public Dictionary<long, PendingContext<Input, Output, Context>> ioPendingRequests;
            public AsyncCountDown pendingReads;
            public AsyncQueue<AsyncIOContext<Key, Value>> readyResponses;
            public int asyncPendingCount;
            public ISynchronizationStateMachine threadStateMachine;

            internal RevivificationStats RevivificationStats = new();

            public int SyncIoPendingCount => ioPendingRequests.Count - asyncPendingCount;

            public bool IsInV1 => phase switch
            {
                Phase.IN_PROGRESS => true,
                Phase.WAIT_INDEX_CHECKPOINT => true,
                Phase.WAIT_FLUSH => true,
                _ => false,
            };

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

            public TsavoriteExecutionContext<Input, Output, Context> prevCtx;
        }
    }
}