// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    internal sealed class CheckEmptyWorker<TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        // State control variables.
        internal struct State
        {
            internal const long Inactive = 0;       // The task has not started
            internal const long Active = 1;         // The task is active
            internal const long Complete = 2;       // The task has exited

            internal static string ToString(long state)
                => state switch
                {
                    Active => "Active",
                    Inactive => "Inactive",
                    Complete => "Complete",
                    _ => "Unknown"
                };
        }
        long state;
        bool disposed;

        CancellationTokenSource cts = new();

        readonly FreeRecordPool<TStoreFunctions, TAllocator> recordPool;

        internal CheckEmptyWorker(FreeRecordPool<TStoreFunctions, TAllocator> recordPool) => this.recordPool = recordPool;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe void Start()
        {
            if (state != State.Inactive)
                return;

            // Interlock because this is lazily called on successful TryAdd().
            if (Interlocked.CompareExchange(ref state, State.Active, State.Inactive) == State.Inactive)
                Task.Run(LaunchWorker);
        }

        private async void LaunchWorker()
        {
            while (!disposed)
            {
                try
                {
                    await Task.Delay(1000, cts.Token);
                    if (disposed)
                        break;
                    recordPool.ScanForEmpty(cts.Token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }

            state = State.Complete;
        }

        internal void Dispose()
        {
            // Any in-progress thread will stop when it sees this, thinking another thread is taking over.
            disposed = true;          // This prevents a newly-launched thread from even starting
            cts.Cancel();

            while (state == State.Active)
                Thread.Yield();

            cts.Dispose();
        }

        /// <inheritdoc/>
        public override string ToString() => $"state: {State.ToString(state)}, disposed {disposed}";
    }
}