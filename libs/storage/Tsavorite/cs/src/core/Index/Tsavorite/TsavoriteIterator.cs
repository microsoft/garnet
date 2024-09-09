// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    public partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        /// <summary>
        /// Pull iterator for all (distinct) live key-values stored in Tsavorite
        /// </summary>
        /// <param name="functions">Functions used to manage key-values during iteration</param>
        /// <param name="iterationType">Iteration type (whether we lookup records or scan log for liveness checking)</param>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>Tsavorite iterator</returns>
        public ITsavoriteScanIterator<TKey, TValue> Iterate<TInput, TOutput, TContext, TFunctions>(TFunctions functions, IterationType iterationType, long untilAddress = -1)
            where TFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
        {
            if (untilAddress == -1)
                untilAddress = Log.TailAddress;

            return iterationType switch
            {
                IterationType.Lookup =>
                    new TsavoriteKVLookupIterator<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions,
                        TAllocator>(this, functions, untilAddress, loggerFactory: loggerFactory),
                IterationType.Scan =>
                    new TsavoriteKVScanIterator<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions,
                        TAllocator>(this, functions, untilAddress, loggerFactory: loggerFactory),
                _ => throw new TsavoriteException("Invalid iteration type"),
            };
        }

        /// <summary>
        /// Push iteration of all (distinct) live key-values stored in Tsavorite
        /// </summary>
        /// <param name="functions">Functions used to manage key-values during iteration</param>
        /// <param name="scanFunctions">Functions receiving pushed records</param>
        /// <param name="iterationType">Iteration type (whether we lookup records or scan log for liveness checking)</param>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>Tsavorite iterator</returns>
        public bool Iterate<TInput, TOutput, TContext, TFunctions, TScanFunctions>(TFunctions functions, ref TScanFunctions scanFunctions, IterationType iterationType, long untilAddress = -1)
            where TFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
            where TScanFunctions : IScanIteratorFunctions<TKey, TValue>
        {
            using var iter =
                (ITsavoriteScanIteratorWithPush<TKey, TValue>)this.Iterate<TInput, TOutput, TContext, TFunctions>(functions, iterationType, untilAddress);

            if (!scanFunctions.OnStart(iter.BeginAddress, iter.EndAddress))
                return false;

            long numRecords = 1;
            bool stop = false;
            for (; !stop && iter.PushNext(ref scanFunctions, numRecords, out stop); ++numRecords)
                ;

            scanFunctions.OnStop(!stop, numRecords);
            return !stop;
        }

        /// <summary>
        /// Iterator for all (distinct) live key-values stored in Tsavorite
        /// </summary>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>Tsavorite iterator</returns>
        [Obsolete("Invoke Iterate() on a client session (ClientSession), or use store.Iterate overload with Functions provided as parameter")]
        public ITsavoriteScanIterator<TKey, TValue> Iterate(long untilAddress = -1)
            => throw new TsavoriteException("Invoke Iterate() on a client session (ClientSession), or use store.Iterate overload with Functions provided as parameter");

        /// <summary>
        /// Iterator for all (distinct) live key-values stored in Tsavorite
        /// </summary>
        /// <param name="compactionFunctions">User provided compaction functions (see <see cref="ICompactionFunctions{Key, Value}"/>).</param>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>Tsavorite iterator</returns>
        [Obsolete("Invoke Iterate() on a client session (ClientSession), or use store.Iterate overload with Functions provided as parameter")]
        public ITsavoriteScanIterator<TKey, TValue> Iterate<CompactionFunctions>(CompactionFunctions compactionFunctions, long untilAddress = -1)
            where CompactionFunctions : ICompactionFunctions<TKey, TValue>
            => throw new TsavoriteException("Invoke Iterate() on a client session (ClientSession), or use store.Iterate overload with Functions provided as parameter");
    }
}