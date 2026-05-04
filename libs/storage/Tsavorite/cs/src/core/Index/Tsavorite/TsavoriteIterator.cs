// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    public partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>
        /// Pull iterator for all (distinct) live key-values stored in Tsavorite
        /// </summary>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>Tsavorite iterator</returns>
        public ITsavoriteScanIterator Iterate(long untilAddress = -1)
        {
            if (untilAddress == -1)
                untilAddress = Log.TailAddress;
            return new TsavoriteKVIterator<TStoreFunctions, TAllocator>(this, untilAddress, loggerFactory: loggerFactory);
        }

        /// <summary>
        /// Push iteration of all (distinct) live key-values stored in Tsavorite
        /// </summary>
        /// <param name="scanFunctions">Functions receiving pushed records</param>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>Tsavorite iterator</returns>
        public bool Iterate<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions
        {
            if (untilAddress == -1)
                untilAddress = Log.TailAddress;
            using TsavoriteKVIterator<TStoreFunctions, TAllocator> iter = new(this, untilAddress, loggerFactory: loggerFactory);

            if (!scanFunctions.OnStart(iter.BeginAddress, iter.EndAddress))
                return false;

            long numRecords = 1;
            var stop = false;
            for (; !stop && iter.PushNext(ref scanFunctions, numRecords, out stop); numRecords++)
                ;

            scanFunctions.OnStop(!stop, numRecords);
            return !stop;
        }
    }

    internal sealed class TsavoriteKVIterator<TStoreFunctions, TAllocator> : ITsavoriteScanIterator
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        private readonly TsavoriteKV<TStoreFunctions, TAllocator> store;
        private readonly TsavoriteKV<TStoreFunctions, TAllocator> tempKv;
        private readonly ClientSession<ITsavoriteScanIterator, LogRecordInput<ITsavoriteScanIterator>, Empty, Empty, LogRecordInternalSessionFunctions<ITsavoriteScanIterator>, TStoreFunctions, TAllocator> tempKvSession;
        private readonly BasicContext<ITsavoriteScanIterator, LogRecordInput<ITsavoriteScanIterator>, Empty, Empty, LogRecordInternalSessionFunctions<ITsavoriteScanIterator>, TStoreFunctions, TAllocator> tempbContext;
        private ITsavoriteScanIterator mainKvIter;
        private ITsavoriteScanIterator tempKvIter;

        enum IterationPhase
        {
            MainKv,     // Iterating main store; if the record is the tailmost for the tag chain, then return it, else add it to tempKv.
            TempKv,     // Return records from tempKv.
            Done        // Done iterating tempKv; Iterator is complete.
        };
        private IterationPhase iterationPhase;

        public TsavoriteKVIterator(TsavoriteKV<TStoreFunctions, TAllocator> store, long untilAddress, ILoggerFactory loggerFactory = null)
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

            tempKv = new TsavoriteKV<TStoreFunctions, TAllocator>(tempKVSettings, store.storeFunctions, store.allocatorFactory);
            tempKvSession = tempKv.NewSession<ITsavoriteScanIterator, LogRecordInput<ITsavoriteScanIterator>, Empty, Empty, LogRecordInternalSessionFunctions<ITsavoriteScanIterator>>(new());
            tempbContext = tempKvSession.BasicContext;
            mainKvIter = store.Log.Scan(store.Log.BeginAddress, untilAddress);
        }

        ITsavoriteScanIterator CurrentIter => iterationPhase == IterationPhase.MainKv ? mainKvIter : tempKvIter;

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
                        OperationStackContext<TStoreFunctions, TAllocator> stackCtx = default;
                        if (IsTailmostMainKvRecord(mainKvIter, mainKvIter.Info, ref stackCtx))
                            return true;

                        ProcessNonTailmostMainKvRecord(mainKvIter.Info, mainKvIter);
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
            where TScanFunctions : IScanIteratorFunctions
        {
            while (true)
            {
                if (iterationPhase == IterationPhase.MainKv)
                {
                    OperationStackContext<TStoreFunctions, TAllocator> stackCtx = default;
                    if (mainKvIter.GetNext())
                    {
                        try
                        {
                            if (IsTailmostMainKvRecord(mainKvIter, mainKvIter.Info, ref stackCtx))
                            {
                                // Push Iter records are in temp storage so do not need locks.
                                stop = !scanFunctions.Reader(in mainKvIter, new RecordMetadata(mainKvIter.CurrentAddress), numRecords, out _);
                                return !stop;
                            }

                            ProcessNonTailmostMainKvRecord(mainKvIter.Info, mainKvIter);
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
                            stop = !scanFunctions.Reader(in tempKvIter, new RecordMetadata(tempKvIter.CurrentAddress), numRecords, out _);
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

        private void ProcessNonTailmostMainKvRecord(RecordInfo recordInfo, ITsavoriteScanIterator key)
        {
            // Not the tailmost record in the tag chain so add it to or remove it from tempKV (we want to return only the latest version).
            if (recordInfo.Tombstone)
            {
                // Check if it's in-memory first so we don't spuriously create a tombstone record.
                if (tempbContext.ContainsKeyInMemory(key, out _).Found)
                    _ = tempbContext.Delete(key);
            }
            else
            {
                var logRecordInput = new LogRecordInput<ITsavoriteScanIterator> { SourceRecord = mainKvIter };
                _ = tempbContext.Upsert(mainKvIter, ref logRecordInput);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool IsTailmostMainKvRecord(ITsavoriteScanIterator key, RecordInfo mainKvRecordInfo, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
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
        public ref RecordInfo InfoRef => ref CurrentIter.InfoRef;
        /// <inheritdoc/>
        public RecordInfo Info => CurrentIter.Info;

        /// <inheritdoc/>
        public byte RecordType => CurrentIter.RecordType;

        /// <inheritdoc/>
        public ReadOnlySpan<byte> Namespace => CurrentIter.Namespace;

        /// <inheritdoc/>
        public ObjectIdMap ObjectIdMap => CurrentIter.ObjectIdMap;

        /// <inheritdoc/>
        public bool IsSet => !CurrentIter.IsSet;

        /// <inheritdoc/>
        public ReadOnlySpan<byte> Key => CurrentIter.Key;

        /// <inheritdoc/>
        public bool IsPinnedKey => CurrentIter.IsPinnedKey;

        /// <inheritdoc/>
        public unsafe byte* PinnedKeyPointer => CurrentIter.PinnedKeyPointer;

        /// <inheritdoc/>
        public OverflowByteArray KeyOverflow
        {
            get => CurrentIter.KeyOverflow;
            set => CurrentIter.KeyOverflow = value;
        }

        /// <inheritdoc/>
        public unsafe Span<byte> ValueSpan => CurrentIter.ValueSpan;

        /// <inheritdoc/>
        public IHeapObject ValueObject => CurrentIter.ValueObject;

        /// <inheritdoc/>
        public bool IsPinnedValue => CurrentIter.IsPinnedValue;

        /// <inheritdoc/>
        public unsafe byte* PinnedValuePointer => CurrentIter.PinnedValuePointer;

        /// <inheritdoc/>
        public OverflowByteArray ValueOverflow
        {
            get => CurrentIter.ValueOverflow;
            set => CurrentIter.ValueOverflow = value;
        }

        /// <inheritdoc/>
        public long ETag => CurrentIter.ETag;

        /// <inheritdoc/>
        public long Expiration => CurrentIter.Expiration;

        /// <inheritdoc/>
        public void ClearValueIfHeap() { }  // Not relevant for "iterator as logrecord"

        /// <inheritdoc/>
        public bool IsMemoryLogRecord => CurrentIter.IsMemoryLogRecord;

        /// <inheritdoc/>
        public unsafe ref LogRecord AsMemoryLogRecordRef() => throw new InvalidOperationException("Cannot cast a TsavoriteKVIterator to a memory LogRecord.");

        /// <inheritdoc/>
        public bool IsDiskLogRecord => CurrentIter.IsDiskLogRecord;

        /// <inheritdoc/>
        public unsafe ref DiskLogRecord AsDiskLogRecordRef() => ref CurrentIter.AsDiskLogRecordRef();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordFieldInfo GetRecordFieldInfo() => CurrentIter.GetRecordFieldInfo();

        /// <inheritdoc/>
        public int AllocatedSize => CurrentIter.AllocatedSize;

        /// <inheritdoc/>
        public int ActualSize => CurrentIter.ActualSize;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long CalculateHeapMemorySize() => CurrentIter.CalculateHeapMemorySize();
        #endregion // ISourceLogRecord

        #region IKey
        /// <inheritdoc/>
        public bool IsPinned => IsPinnedKey;

        /// <inheritdoc/>
        public ReadOnlySpan<byte> KeyBytes => Key;

        /// <inheritdoc/>
        public bool HasNamespace => CurrentIter.HasNamespace;

        /// <inheritdoc/>
        public ReadOnlySpan<byte> NamespaceBytes => CurrentIter.NamespaceBytes;
        #endregion
    }
}
