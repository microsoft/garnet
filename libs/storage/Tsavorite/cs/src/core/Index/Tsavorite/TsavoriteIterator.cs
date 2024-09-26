// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        /// <summary>
        /// Push iteration of all (distinct) live key-values stored in Tsavorite
        /// </summary>
        /// <param name="scanFunctions">Functions receiving pushed records</param>
        /// <param name="scanner">Record scanner specific to allocator type</param>
        /// <param name="scanCursorState">The state of the underlying scan</param>
        /// <returns>True if the iteration completed to EndAddress, else false</returns>
        internal bool Scan<TScanFunctions, TRecordScanner>(UnsafeContext<TKey, TValue, Empty, Empty, Empty, LivenessCheckingSessionFunctions<TKey, TValue>, TStoreFunctions, TAllocator> uContext,
                TScanFunctions scanFunctions, ScanCursorState<TKey, TValue> scanCursorState, TRecordScanner scanner)
            where TScanFunctions : IScanIteratorFunctions<TKey, TValue>
            where TRecordScanner : IRecordScanner<TKey, TValue>
        {
            if (!scanFunctions.OnStart(scanner.BeginAddress, scanner.EndAddress))
                return false;

            while (!scanCursorState.stop && scanner.ScanNext())
                ;

            scanFunctions.OnStop(!scanCursorState.stop, scanCursorState.acceptedCount);
            return !scanCursorState.stop;
        }

        /// <summary>
        /// Push iteration of all (distinct) live key-values stored in Tsavorite
        /// </summary>
        /// <param name="scanFunctions">Functions receiving pushed records</param>
        /// <param name="scanner">Record scanner specific to allocator type</param>
        /// <param name="scanCursorState">The state of the underlying scan</param>
        /// <returns>True if the iteration completed to EndAddress, else false</returns>
        internal bool Iterate<TScanFunctions, TRecordScanner>(UnsafeContext<TKey, TValue, Empty, Empty, Empty, LivenessCheckingSessionFunctions<TKey, TValue>, TStoreFunctions, TAllocator> uContext,
                TScanFunctions scanFunctions, ScanCursorState<TKey, TValue> scanCursorState, TRecordScanner scanner)
            where TScanFunctions : IScanIteratorFunctions<TKey, TValue>
            where TRecordScanner : IRecordScanner<TKey, TValue>
        {
            TsavoriteKVIterator<TKey, TValue, TStoreFunctions, TAllocator, TRecordScanner, TScanFunctions> iter = new(uContext, scanner, scanCursorState);

            if (!scanFunctions.OnStart(iter.BeginAddress, iter.EndAddress))
                return false;

            while (!scanCursorState.stop && iter.PushNext())
                ;

            scanFunctions.OnStop(!scanCursorState.stop, scanCursorState.acceptedCount);
            return !scanCursorState.stop;
        }
    }

    internal sealed class TsavoriteKVIterator<TKey, TValue, TStoreFunctions, TAllocator, TRecordScanner, TScanFunctions>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
        where TRecordScanner : IRecordScanner<TKey, TValue>
        where TScanFunctions : IScanIteratorFunctions<TKey, TValue>
    {
        readonly ScanCursorState<TKey, TValue> scanCursorState;

        private readonly UnsafeContext<TKey, TValue, Empty, Empty, Empty, LivenessCheckingSessionFunctions<TKey, TValue>, TStoreFunctions, TAllocator> uContext;
        private readonly TRecordScanner scanner;

        public TsavoriteKVIterator(UnsafeContext<TKey, TValue, Empty, Empty, Empty, LivenessCheckingSessionFunctions<TKey, TValue>, TStoreFunctions, TAllocator> uContext,
                TRecordScanner scanner, ScanCursorState<TKey, TValue> scanCursorState)
        {
            this.uContext = uContext;
            this.scanCursorState = scanCursorState;
            this.scanner = scanner;
        }

        public long BeginAddress => scanner.BeginAddress;

        public long EndAddress => scanner.EndAddress;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool PushNext()
        {
            // Use bContext for CompletePending because it must acquire the epoch
            var bContext = uContext.Session.BasicContext;

            var numPending = 0;
            while (scanner.IterateNext(out var status))
            {
                if (status.IsPending)
                {
                    if (++numPending == 256)
                    {
                        _ = bContext.CompletePending(wait: true);
                        numPending = 0;
                    }
                }

                // Now see if we completed the enumeration.
                if (scanCursorState.stop)
                    goto IterationComplete;
            }

            // Drain any pending pushes. We have ended the iteration; there are no more records, so drop through to end it.
            if (numPending > 0)
                _ = bContext.CompletePending(wait: true);

            IterationComplete:
            return false;
        }
    }
}