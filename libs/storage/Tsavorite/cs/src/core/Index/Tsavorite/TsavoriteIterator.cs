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
            var stop = false;
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
        private ITsavoriteScanIterator<TValue> mainKvIter;
        private readonly IPushScanIterator<TValue> pushScanIterator;
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
            pushScanIterator = mainKvIter as IPushScanIterator<TValue>;
        }

        ITsavoriteScanIterator<TValue> CurrentIter => iterationPhase == IterationPhase.MainKv ? mainKvIter : tempKvIter;

        public long CurrentAddress => CurrentIter.CurrentAddress;

        public long NextAddress => CurrentIter.NextAddress;

        public long BeginAddress => CurrentIter.BeginAddress;

        public long EndAddress => CurrentIter.EndAddress;

        public void Dispose()
        {
            mainKvIter?.Dispose();
            tempKvIter?.Dispose();
            tempKvSession?.Dispose();
            tempKv?.Dispose();
        }

        public bool GetNext()
        {
            while (true)
            {
                if (iterationPhase == IterationPhase.MainKv)
                {
                    if (mainKvIter.GetNext())
                    {
                        OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx = default;
                        if (IsTailmostMainKvRecord(mainKvIter.Key, mainKvIter.Info, ref stackCtx))
                            return true;

                        ProcessNonTailmostMainKvRecord(mainKvIter.Info, mainKvIter.Key);
                        continue;
                    }

                    // Done with MainKv; dispose mainKvIter, initialize tempKvIter, and drop through to TempKv iteration.
                    mainKvIter.Dispose();
                    iterationPhase = IterationPhase.TempKv;
                    tempKvIter = tempKv.Log.Scan(tempKv.Log.BeginAddress, tempKv.Log.TailAddress);
                }

                if (iterationPhase == IterationPhase.TempKv)
                {
                    if (tempKvIter.GetNext())
                    {
                        if (!tempKvIter.Info.Tombstone)
                            return true;
                        continue;
                    }

                    // Done with TempKv iteration, so we're done. Drop through to Done handling.
                    tempKvIter.Dispose();
                    iterationPhase = IterationPhase.Done;
                }

                // We're done. This handles both the call that exhausted tempKvIter, and any subsequent calls on this outer iterator.
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
                    if (mainKvIter.GetNext())
                    {
                        try
                        {
                            var key = mainKvIter.Key;
                            if (IsTailmostMainKvRecord(key, mainKvIter.Info, ref stackCtx))
                            {
                                // Push Iter records are in temp storage so do not need locks, but we'll call ConcurrentReader because, for example, GenericAllocator
                                // may need to know the object is in that region.
                                stop = mainKvIter.CurrentAddress >= store.hlogBase.ReadOnlyAddress
                                    ? !scanFunctions.ConcurrentReader(ref mainKvIter, new RecordMetadata(mainKvIter.CurrentAddress), numRecords, out _)
                                    : !scanFunctions.SingleReader(ref mainKvIter, new RecordMetadata(mainKvIter.CurrentAddress), numRecords, out _);
                                return !stop;
                            }

                            ProcessNonTailmostMainKvRecord(mainKvIter.Info, key);
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
                    if (tempKvIter.GetNext())
                    {
                        if (!tempKvIter.Info.Tombstone)
                        {
                            stop = !scanFunctions.SingleReader(ref tempKvIter, new RecordMetadata(tempKvIter.CurrentAddress), numRecords, out _);
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

        private void ProcessNonTailmostMainKvRecord(RecordInfo recordInfo, SpanByte key)
        {
            // Not the tailmost record in the tag chain so add it to or remove it from tempKV (we want to return only the latest version).
            if (recordInfo.Tombstone)
            {
                // Check if it's in-memory first so we don't spuriously create a tombstone record.
                if (tempbContext.ContainsKeyInMemory(key, out _).Found)
                    _ = tempbContext.Delete(key);
            }
            else
                _ = tempbContext.Upsert(key, mainKvIter.GetReadOnlyValueRef());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool IsTailmostMainKvRecord(SpanByte key, RecordInfo mainKvRecordInfo, ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx)
        {
            stackCtx = new(store.storeFunctions.GetKeyHashCode64(key));
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
                        if (tempbContext.ContainsKeyInMemory(key, out _).Found)
                            _ = tempbContext.Delete(key);
                    }

                    // If the record is not deleted, we can let the caller process it directly within mainKvIter.
                    return !mainKvRecordInfo.Tombstone;
                }
            }
            return false;
        }

        #region ISourceLogRecord
        /// <inheritdoc/>
        public bool IsObjectRecord => CurrentIter.IsObjectRecord;
        /// <inheritdoc/>
        public ref RecordInfo InfoRef => ref CurrentIter.InfoRef;
        /// <inheritdoc/>
        public RecordInfo Info => CurrentIter.Info;

        /// <inheritdoc/>
        public bool IsSet => !CurrentIter.IsSet;

        /// <inheritdoc/>
        public SpanByte Key => CurrentIter.Key;

        /// <inheritdoc/>
        public unsafe SpanByte ValueSpan => CurrentIter.ValueSpan;

        /// <inheritdoc/>
        public TValue ValueObject => CurrentIter.ValueObject;

        /// <inheritdoc/>
        public unsafe ref TValue GetReadOnlyValueRef() => ref CurrentIter.GetReadOnlyValueRef();

        /// <inheritdoc/>
        public long ETag => CurrentIter.ETag;

        /// <inheritdoc/>
        public long Expiration => CurrentIter.Expiration;

        /// <inheritdoc/>
        public LogRecord<TValue> AsLogRecord() => throw new TsavoriteException("Iterators cannot be converted to AsLogRecord");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordFieldInfo GetRecordFieldInfo() => CurrentIter.GetRecordFieldInfo();
        #endregion // ISourceLogRecord
    }
}