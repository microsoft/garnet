// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Compaction methods
    /// </summary>
    public partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <remarks>
        /// <paramref name="untilAddress"/> need not be a record boundary; compaction snaps it to the end of the last record that starts below it,
        /// which is the returned address.
        /// </remarks>
        /// <param name="cf">User provided compaction functions (see <see cref="ICompactionFunctions"/>).</param>
        /// <param name="untilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <returns>Address until which compaction was done</returns>
        internal long Compact<TInput, TOutput, TContext, TCompactionFunctions>(TCompactionFunctions cf, long untilAddress, CompactionType compactionType)
            where TCompactionFunctions : ICompactionFunctions
        {
            return compactionType switch
            {
                CompactionType.Scan => CompactScan<TInput, TOutput, TContext, TCompactionFunctions>(cf, untilAddress),
                CompactionType.Lookup => CompactLookup<TInput, TOutput, TContext, TCompactionFunctions>(cf, untilAddress),
                _ => throw new TsavoriteException("Invalid compaction type"),
            };
        }

        /// <summary>
        /// Verify that a copy to the tail succeeded. Compaction drops the source records by shifting BeginAddress past them, so a copy that
        /// failed would be permanent data loss; fail the compaction instead and leave BeginAddress where it is.
        /// </summary>
        private static void VerifyCompactionCopyStatus(Status status)
        {
            if (status.IsFaulted || status.IsCanceled)
                ThrowCompactionCopyFailed(status);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowCompactionCopyFailed(Status status)
            => throw new TsavoriteException($"Compaction could not copy a record to the tail: {status}");

        /// <summary>Drain the pending copies to the tail, verifying that each one succeeded.</summary>
        private static void CompletePendingCompactionCopies<TInput, TOutput, TContext>(
                BasicContext<ITsavoriteScanIterator, TInput, TOutput, TContext, NoOpSessionFunctions<TInput, TOutput, TContext>, TStoreFunctions, TAllocator> bContext)
        {
            _ = bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            using (completedOutputs)
            {
                while (completedOutputs.Next())
                    VerifyCompactionCopyStatus(completedOutputs.Current.Status);
            }
        }

        private long CompactLookup<TInput, TOutput, TContext, TCompactionFunctions>(TCompactionFunctions cf, long untilAddress)
            where TCompactionFunctions : ICompactionFunctions
        {
            if (untilAddress > hlogBase.SafeReadOnlyAddress)
                throw new TsavoriteException("Can compact only until Log.SafeReadOnlyAddress");

            using var storeSession = NewSession<ITsavoriteScanIterator, TInput, TOutput, TContext, NoOpSessionFunctions<TInput, TOutput, TContext>>(new());
            var storebContext = storeSession.BasicContext;

            using (var iter1 = Log.Scan(Log.BeginAddress, untilAddress))
            {
                long numPending = 0;
                while (iter1.GetNext())
                {
                    var key = iter1.Key;

                    if (!iter1.Info.Tombstone && !cf.IsDeleted(in iter1))
                    {
                        var iter1AsLogSource = iter1 as ISourceLogRecord;   // Can't use 'ref' on a 'using' variable
                        var status = storebContext.CompactionCopyToTail(in iter1AsLogSource, iter1.CurrentAddress, iter1.NextAddress);
                        if (status.IsPending)
                        {
                            if (++numPending > 256)
                            {
                                CompletePendingCompactionCopies(storebContext);
                                numPending = 0;
                            }
                        }
                        else
                            VerifyCompactionCopyStatus(status);
                    }

                    // Ensure address is at record boundary. The scan returns the last record that starts below untilAddress whole, even
                    // if it ends above it, so this can advance untilAddress past what the caller asked for.
                    untilAddress = iter1.NextAddress;
                }
                if (numPending > 0)
                    CompletePendingCompactionCopies(storebContext);
            }

            // Only reached when every record in the range was copied: a failed copy throws, leaving BeginAddress unchanged so no
            // original is dropped. Copies already at the tail are then ordinary older versions -- a later compaction finds the newer
            // tail version via minAddress and skips them -- so retrying is idempotent and the partial work costs only log space.
            Log.ShiftBeginAddress(untilAddress, false);
            return untilAddress;
        }

        private long CompactScan<TInput, TOutput, TContext, TCompactionFunctions>(TCompactionFunctions cf, long untilAddress)
            where TCompactionFunctions : ICompactionFunctions
        {
            if (untilAddress > hlogBase.SafeReadOnlyAddress)
                throw new TsavoriteException("Can compact only until Log.SafeReadOnlyAddress");

            var originalUntilAddress = untilAddress;

            using var storeSession = NewSession<ITsavoriteScanIterator, TInput, TOutput, TContext, NoOpSessionFunctions<TInput, TOutput, TContext>>(new());
            var storebContext = storeSession.BasicContext;

            var tempKVSettings = new KVSettings(baseDir: null, loggerFactory: loggerFactory)
            {
                IndexSize = KVSettings.SetIndexSizeFromCacheLines(IndexSize),
                LogDevice = new NullDevice(),
                ObjectLogDevice = new NullDevice()
            };

            using (var tempKv = new TsavoriteKV<TStoreFunctions, TAllocator>(tempKVSettings, storeFunctions, allocatorFactory))
            using (var tempKvSession = tempKv.NewSession<ITsavoriteScanIterator, TInput, TOutput, TContext, NoOpSessionFunctions<TInput, TOutput, TContext>>(new()))
            {
                var tempbContext = tempKvSession.BasicContext;
                using (var iter1 = Log.Scan(hlogBase.BeginAddress, untilAddress))
                {
                    while (iter1.GetNext())
                    {
                        if (iter1.Info.Tombstone || cf.IsDeleted(in iter1))
                            _ = tempbContext.Delete(iter1);
                        else
                        {
                            var iterLogRecord = iter1 as ISourceLogRecord;      // Can't use 'ref' on a 'using' variable
                            _ = tempbContext.Upsert(in iterLogRecord);
                        }
                    }
                    // Ensure address is at record boundary. The scan returns the last record that starts below untilAddress whole, even
                    // if it ends above it, so this can advance untilAddress past what the caller asked for.
                    untilAddress = originalUntilAddress = iter1.NextAddress;
                }

                // Scan until SafeReadOnlyAddress
                var scanUntil = hlogBase.SafeReadOnlyAddress;
                if (untilAddress < scanUntil)
                    ScanImmutableTailToRemoveFromTempKv(ref untilAddress, scanUntil, tempbContext);

                var numPending = 0;
                using var iter3 = tempKv.Log.Scan(tempKv.Log.BeginAddress, tempKv.Log.TailAddress);
                while (iter3.GetNext())
                {
                    if (iter3.Info.Tombstone)
                        continue;

                    // Try to ensure we have checked all immutable records
                    scanUntil = hlogBase.SafeReadOnlyAddress;
                    if (untilAddress < scanUntil)
                        ScanImmutableTailToRemoveFromTempKv(ref untilAddress, scanUntil, tempbContext);

                    // If record is not the latest in tempKv's memory for this key, ignore it (will not be returned if deleted)
                    if (!tempbContext.ContainsKeyInMemory(iter3, out var tempKeyAddress).Found || iter3.CurrentAddress != tempKeyAddress)
                        continue;

                    // As long as there's no record of the same key whose address is >= untilAddress (scan boundary), we are safe to copy the old record
                    // to the tail. We don't know the actualAddress of the key in the main kv, but we it will not be below untilAddress.
                    var iter3AsLogSource = iter3 as ISourceLogRecord;   // Can't use 'ref' on a 'using' variable
                    var status = storebContext.CompactionCopyToTail(in iter3AsLogSource, iter3.CurrentAddress, untilAddress - 1);
                    if (status.IsPending)
                    {
                        if (++numPending > 256)
                        {
                            CompletePendingCompactionCopies(storebContext);
                            numPending = 0;
                        }
                    }
                    else
                        VerifyCompactionCopyStatus(status);
                }
                if (numPending > 0)
                    CompletePendingCompactionCopies(storebContext);
            }

            // Only reached when every record in the range was copied: a failed copy throws, leaving BeginAddress unchanged so no
            // original is dropped. Copies already at the tail are then ordinary older versions -- a later compaction finds the newer
            // tail version via minAddress and skips them -- so retrying is idempotent and the partial work costs only log space.
            Log.ShiftBeginAddress(originalUntilAddress, false);
            return originalUntilAddress;
        }

        private void ScanImmutableTailToRemoveFromTempKv<TInput, TOutput, TContext, TFunctions>(ref long untilAddress, long scanUntil,
                BasicContext<ITsavoriteScanIterator, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> tempbContext)
            where TFunctions : ISessionFunctions<TInput, TOutput, TContext>
        {
            using var iter = Log.Scan(untilAddress, scanUntil);
            while (iter.GetNext())
            {
                _ = tempbContext.Delete(iter, default);
                untilAddress = iter.NextAddress;
            }
        }
    }
}