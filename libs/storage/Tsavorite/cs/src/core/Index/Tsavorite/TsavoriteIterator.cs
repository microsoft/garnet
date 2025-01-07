// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    public partial class TsavoriteKV<TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TValue>
        where TAllocator : IAllocator<TValue, TStoreFunctions>
    {
        /// <summary>
        /// Pull iterator for all (distinct) live key-values stored in Tsavorite
        /// </summary>
        /// <param name="functions">Functions used to manage key-values during iteration</param>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>Tsavorite iterator</returns>
        public ITsavoriteScanIterator<TValue> Iterate<TInput, TOutput, TContext, TFunctions>(TFunctions functions, long untilAddress = -1)
            where TFunctions : ISessionFunctions<TValue, TInput, TOutput, TContext>
        {
            if (untilAddress == -1)
                untilAddress = Log.TailAddress;
            return new TsavoriteKVIterator<TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>(this, functions, untilAddress, loggerFactory: loggerFactory);
        }

        /// <summary>
        /// Push iteration of all (distinct) live key-values stored in Tsavorite
        /// </summary>
        /// <param name="functions">Functions used to manage key-values during iteration</param>
        /// <param name="scanFunctions">Functions receiving pushed records</param>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>Tsavorite iterator</returns>
        public bool Iterate<TInput, TOutput, TContext, TFunctions, TScanFunctions>(TFunctions functions, ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TFunctions : ISessionFunctions<TValue, TInput, TOutput, TContext>
            where TScanFunctions : IScanIteratorFunctions<TValue>
        {
            if (untilAddress == -1)
                untilAddress = Log.TailAddress;
            using TsavoriteKVIterator<TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> iter = new(this, functions, untilAddress, loggerFactory: loggerFactory);

            if (!scanFunctions.OnStart(iter.BeginAddress, iter.EndAddress))
                return false;

            long numRecords = 1;
            bool stop = false;
            for (; !stop && iter.PushNext(ref scanFunctions, numRecords, out stop); ++numRecords)
                ;

            scanFunctions.OnStop(!stop, numRecords);
            return !stop;
        }
    }

    internal sealed class TsavoriteKVIterator<TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> : ITsavoriteScanIterator<TValue>
        where TFunctions : ISessionFunctions<TValue, TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions<TValue>
        where TAllocator : IAllocator<TValue, TStoreFunctions>
    {
        private readonly TsavoriteKV<TValue, TStoreFunctions, TAllocator> store;
        private readonly TsavoriteKV<TValue, TStoreFunctions, TAllocator> tempKv;
        private readonly ClientSession<TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> tempKvSession;
        private readonly BasicContext<TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> tempbContext;
        private readonly ITsavoriteScanIterator<TValue> mainKvIter;
        private readonly IPushScanIterator pushScanIterator;
        private ITsavoriteScanIterator<TValue> tempKvIter;

        enum IterationPhase
        {
            MainKv,     // Iterating main store; if the record is the tailmost for the tag chain, then return it, else add it to tempKv.
            TempKv,     // Return records from tempKv.
            Done        // Done iterating tempKv; Iterator is complete.
        };
        private IterationPhase iterationPhase;

        public TsavoriteKVIterator(TsavoriteKV<TValue, TStoreFunctions, TAllocator> store, TFunctions functions, long untilAddress, ILoggerFactory loggerFactory = null)
        {
            this.store = store;
            iterationPhase = IterationPhase.MainKv;

            var tempKVSettings = new KVSettings(baseDir: null, loggerFactory: loggerFactory)
            {
                IndexSize = KVSettings.SetIndexSizeFromCacheLines(store.IndexSize),
                LogDevice = new NullDevice(),
                ObjectLogDevice = new NullDevice(),
                MutableFraction = 1,
                loggerFactory = loggerFactory
            };

            tempKv = new TsavoriteKV<TValue, TStoreFunctions, TAllocator>(tempKVSettings, store.storeFunctions, store.allocatorFactory);
            tempKvSession = tempKv.NewSession<TInput, TOutput, TContext, TFunctions>(functions);
            tempbContext = tempKvSession.BasicContext;
            mainKvIter = store.Log.Scan(store.Log.BeginAddress, untilAddress);
            pushScanIterator = mainKvIter as IPushScanIterator;
        }

        public long CurrentAddress => iterationPhase == IterationPhase.MainKv ? mainKvIter.CurrentAddress : tempKvIter.CurrentAddress;

        public long NextAddress => iterationPhase == IterationPhase.MainKv ? mainKvIter.NextAddress : tempKvIter.NextAddress;

        public long BeginAddress => iterationPhase == IterationPhase.MainKv ? mainKvIter.BeginAddress : tempKvIter.BeginAddress;

        public long EndAddress => iterationPhase == IterationPhase.MainKv ? mainKvIter.EndAddress : tempKvIter.EndAddress;

        public void Dispose()
        {
            mainKvIter?.Dispose();
            tempKvIter?.Dispose();
            tempKvSession?.Dispose();
            tempKv?.Dispose();
        }

        public ref TKey GetKey() => ref iterationPhase == IterationPhase.MainKv ? ref mainKvIter.GetKey() : ref tempKvIter.GetKey();

        public ref TValue GetValue() => ref iterationPhase == IterationPhase.MainKv ? ref mainKvIter.GetValue() : ref tempKvIter.GetValue();

        public bool GetNext(out RecordInfo recordInfo)
        {
            while (true)
            {
                if (iterationPhase == IterationPhase.MainKv)
                {
                    if (mainKvIter.GetNext(out recordInfo))
                    {
                        ref var key = ref mainKvIter.GetKey();
                        OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx = default;
                        if (IsTailmostMainKvRecord(ref key, recordInfo, ref stackCtx))
                            return true;

                        ProcessNonTailmostMainKvRecord(recordInfo, key);
                        continue;
                    }

                    // Done with MainKv; dispose mainKvIter, initialize tempKvIter, and drop through to TempKv iteration.
                    mainKvIter.Dispose();
                    iterationPhase = IterationPhase.TempKv;
                    tempKvIter = tempKv.Log.Scan(tempKv.Log.BeginAddress, tempKv.Log.TailAddress);
                }

                if (iterationPhase == IterationPhase.TempKv)
                {
                    if (tempKvIter.GetNext(out recordInfo))
                    {
                        if (!recordInfo.Tombstone)
                            return true;
                        continue;
                    }

                    // Done with TempKv iteration, so we're done. Drop through to Done handling.
                    tempKvIter.Dispose();
                    iterationPhase = IterationPhase.Done;
                }

                // We're done. This handles both the call that exhausted tempKvIter, and any subsequent calls on this outer iterator.
                recordInfo = default;
                return false;
            }
        }

        internal bool PushNext<TScanFunctions>(ref TScanFunctions scanFunctions, long numRecords, out bool stop)
            where TScanFunctions : IScanIteratorFunctions<TValue>
        {
            while (true)
            {
                if (iterationPhase == IterationPhase.MainKv)
                {
                    OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx = default;
                    if (mainKvIter.GetNext(out var recordInfo))
                    {
                        try
                        {
                            ref var key = ref mainKvIter.GetKey();
                            if (IsTailmostMainKvRecord(ref key, recordInfo, ref stackCtx))
                            {
                                // Push Iter records are in temp storage so do not need locks, but we'll call ConcurrentReader because, for example, GenericAllocator
                                // may need to know the object is in that region.
                                stop = mainKvIter.CurrentAddress >= store.hlogBase.ReadOnlyAddress
                                    ? !scanFunctions.ConcurrentReader(ref key, ref mainKvIter.GetValue(), new RecordMetadata(recordInfo, mainKvIter.CurrentAddress), numRecords, out _)
                                    : !scanFunctions.SingleReader(ref key, ref mainKvIter.GetValue(), new RecordMetadata(recordInfo, mainKvIter.CurrentAddress), numRecords, out _);
                                return !stop;
                            }

                            ProcessNonTailmostMainKvRecord(recordInfo, key);
                            continue;
                        }
                        catch (Exception ex)
                        {
                            scanFunctions.OnException(ex, numRecords);
                            throw;
                        }
                        finally
                        {
                            if (stackCtx.recSrc.HasLock)
                                store.UnlockForScan(ref stackCtx);
                        }
                    }

                    // Done with MainKv; dispose mainKvIter, initialize tempKvIter, and drop through to TempKv iteration.
                    mainKvIter.Dispose();
                    iterationPhase = IterationPhase.TempKv;
                    tempKvIter = tempKv.Log.Scan(tempKv.Log.BeginAddress, tempKv.Log.TailAddress);
                }

                if (iterationPhase == IterationPhase.TempKv)
                {
                    if (tempKvIter.GetNext(out var recordInfo))
                    {
                        if (!recordInfo.Tombstone)
                        {
                            stop = !scanFunctions.SingleReader(ref tempKvIter.GetKey(), ref tempKvIter.GetValue(), new RecordMetadata(recordInfo, tempKvIter.CurrentAddress), numRecords, out _);
                            return !stop;
                        }
                        continue;
                    }

                    // Done with TempKv iteration, so we're done. Drop through to Done handling.
                    tempKvIter.Dispose();
                    iterationPhase = IterationPhase.Done;
                }

                // We're done. This handles both the call that exhausted tempKvIter, and any subsequent calls on this outer iterator.
                stop = false;
                return false;
            }
        }

        private void ProcessNonTailmostMainKvRecord(RecordInfo recordInfo, TKey key)
        {
            // Not the tailmost record in the tag chain so add it to or remove it from tempKV (we want to return only the latest version).
            if (recordInfo.Tombstone)
            {
                // Check if it's in-memory first so we don't spuriously create a tombstone record.
                if (tempbContext.ContainsKeyInMemory(ref key, out _).Found)
                    _ = tempbContext.Delete(ref key);
            }
            else
                _ = tempbContext.Upsert(ref key, ref mainKvIter.GetValue());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool IsTailmostMainKvRecord(ref TKey key, RecordInfo mainKvRecordInfo, ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx)
        {
            stackCtx = new(store.storeFunctions.GetKeyHashCode64(ref key));
            if (store.FindTag(ref stackCtx.hei))
            {
                stackCtx.SetRecordSourceToHashEntry(store.hlogBase);
                if (store.UseReadCache)
                    store.SkipReadCache(ref stackCtx, out _);
                if (stackCtx.recSrc.LogicalAddress == mainKvIter.CurrentAddress)
                {
                    // The tag chain starts with this record, so we won't see this key again; remove it from tempKv if we've seen it before.
                    if (mainKvRecordInfo.PreviousAddress >= store.Log.BeginAddress)
                    {
                        // Check if it's in-memory first so we don't spuriously create a tombstone record.
                        if (tempbContext.ContainsKeyInMemory(ref key, out _).Found)
                            tempbContext.Delete(ref key);
                    }

                    // If the record is not deleted, we can let the caller process it directly within mainKvIter.
                    return !mainKvRecordInfo.Tombstone;
                }
            }
            return false;
        }

        public bool GetNext(out RecordInfo recordInfo, out TKey key, out TValue value)
        {
            if (GetNext(out recordInfo))
            {
                if (iterationPhase == IterationPhase.MainKv)
                {
                    key = mainKvIter.GetKey();
                    value = mainKvIter.GetValue();
                }
                else
                {
                    key = tempKvIter.GetKey();
                    value = tempKvIter.GetValue();
                }
                return true;
            }

            key = default;
            value = default;
            return false;
        }
    }
}