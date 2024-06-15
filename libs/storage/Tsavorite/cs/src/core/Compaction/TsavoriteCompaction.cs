// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Compaction methods
    /// </summary>
    public partial class TsavoriteKV<Key, Value, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<Key, Value>
        where TAllocator : IAllocator<Key, Value, TStoreFunctions>
    {
        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="functions">Functions used to manage key-values during compaction</param>
        /// <param name="cf">User provided compaction functions (see <see cref="ICompactionFunctions{Key, Value}"/>).</param>
        /// <param name="input">Input for SingleWriter</param>
        /// <param name="output">Output from SingleWriter; it will be called all records that are moved, before Compact() returns, so the user must supply buffering or process each output completely</param>
        /// <param name="untilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <returns>Address until which compaction was done</returns>
        internal long Compact<Input, Output, Context, Functions, CompactionFunctions>(Functions functions, CompactionFunctions cf, ref Input input, ref Output output, long untilAddress, CompactionType compactionType)
            where Functions : ISessionFunctions<Key, Value, Input, Output, Context>
            where CompactionFunctions : ICompactionFunctions<Key, Value>
        {
            return compactionType switch
            {
                CompactionType.Scan => CompactScan<Input, Output, Context, Functions, CompactionFunctions>(functions, cf, ref input, ref output, untilAddress),
                CompactionType.Lookup => CompactLookup<Input, Output, Context, Functions, CompactionFunctions>(functions, cf, ref input, ref output, untilAddress),
                _ => throw new TsavoriteException("Invalid compaction type"),
            };
        }

        private long CompactLookup<Input, Output, Context, Functions, CompactionFunctions>(Functions functions, CompactionFunctions cf, ref Input input, ref Output output, long untilAddress)
            where Functions : ISessionFunctions<Key, Value, Input, Output, Context>
            where CompactionFunctions : ICompactionFunctions<Key, Value>
        {
            if (untilAddress > hlogBase.SafeReadOnlyAddress)
                throw new TsavoriteException("Can compact only until Log.SafeReadOnlyAddress");

            var lf = new LogCompactionFunctions<Key, Value, Input, Output, Context, Functions>(functions);
            using var storeSession = NewSession<Input, Output, Context, LogCompactionFunctions<Key, Value, Input, Output, Context, Functions>>(lf);
            var storebContext = storeSession.BasicContext;

            using (var iter1 = Log.Scan(Log.BeginAddress, untilAddress))
            {
                long numPending = 0;
                while (iter1.GetNext(out var recordInfo))
                {
                    ref var key = ref iter1.GetKey();
                    ref var value = ref iter1.GetValue();

                    if (!recordInfo.Tombstone && !cf.IsDeleted(ref key, ref value))
                    {
                        var status = storebContext.CompactionCopyToTail(ref key, ref input, ref value, ref output, iter1.NextAddress);
                        if (status.IsPending && ++numPending > 256)
                        {
                            storebContext.CompletePending(wait: true);
                            numPending = 0;
                        }
                    }

                    // Ensure address is at record boundary
                    untilAddress = iter1.NextAddress;
                }
                if (numPending > 0)
                    storebContext.CompletePending(wait: true);
            }
            Log.ShiftBeginAddress(untilAddress, false);
            return untilAddress;
        }

        private long CompactScan<Input, Output, Context, Functions, CompactionFunctions>(Functions functions, CompactionFunctions cf, ref Input input, ref Output output, long untilAddress)
            where Functions : ISessionFunctions<Key, Value, Input, Output, Context>
            where CompactionFunctions : ICompactionFunctions<Key, Value>
        {
            if (untilAddress > hlogBase.SafeReadOnlyAddress)
                throw new TsavoriteException("Can compact only until Log.SafeReadOnlyAddress");

            var originalUntilAddress = untilAddress;

            var lf = new LogCompactionFunctions<Key, Value, Input, Output, Context, Functions>(functions);
            using var storeSession = NewSession<Input, Output, Context, LogCompactionFunctions<Key, Value, Input, Output, Context, Functions>>(lf);
            var storebContext = storeSession.BasicContext;

            var tempKVSettings = new TsavoriteKVSettings<Key, Value>(baseDir: null, loggerFactory: loggerFactory)
            {
                IndexSize = TsavoriteKVSettings<Key, Value>.SetIndexSizeFromCacheLines(IndexSize),
                LogDevice = new NullDevice(),
                ObjectLogDevice = new NullDevice()
            };

            using (var tempKv = new TsavoriteKV<Key, Value, TStoreFunctions, TAllocator>(tempKVSettings, storeFunctions, allocatorFactory))
            using (var tempKvSession = tempKv.NewSession<Input, Output, Context, Functions>(functions))
            {
                var tempbContext = tempKvSession.BasicContext;
                using (var iter1 = Log.Scan(hlogBase.BeginAddress, untilAddress))
                {
                    while (iter1.GetNext(out var recordInfo))
                    {
                        ref var key = ref iter1.GetKey();
                        ref var value = ref iter1.GetValue();

                        if (recordInfo.Tombstone || cf.IsDeleted(ref key, ref value))
                            tempbContext.Delete(ref key);
                        else
                            tempbContext.Upsert(ref key, ref value);
                    }
                    // Ensure address is at record boundary
                    untilAddress = originalUntilAddress = iter1.NextAddress;
                }

                // Scan until SafeReadOnlyAddress
                var scanUntil = hlogBase.SafeReadOnlyAddress;
                if (untilAddress < scanUntil)
                    ScanImmutableTailToRemoveFromTempKv(ref untilAddress, scanUntil, tempbContext);

                var numPending = 0;
                using var iter3 = tempKv.Log.Scan(tempKv.Log.BeginAddress, tempKv.Log.TailAddress);
                while (iter3.GetNext(out var recordInfo))
                {
                    if (recordInfo.Tombstone)
                        continue;

                    // Try to ensure we have checked all immutable records
                    scanUntil = hlogBase.SafeReadOnlyAddress;
                    if (untilAddress < scanUntil)
                        ScanImmutableTailToRemoveFromTempKv(ref untilAddress, scanUntil, tempbContext);

                    // If record is not the latest in tempKv's memory for this key, ignore it (will not be returned if deleted)
                    if (!tempbContext.ContainsKeyInMemory(ref iter3.GetKey(), out long tempKeyAddress).Found || iter3.CurrentAddress != tempKeyAddress)
                        continue;

                    // As long as there's no record of the same key whose address is >= untilAddress (scan boundary), we are safe to copy the old record
                    // to the tail. We don't know the actualAddress of the key in the main kv, but we it will not be below untilAddress.
                    var status = storebContext.CompactionCopyToTail(ref iter3.GetKey(), ref input, ref iter3.GetValue(), ref output, untilAddress - 1);
                    if (status.IsPending && ++numPending > 256)
                    {
                        storebContext.CompletePending(wait: true);
                        numPending = 0;
                    }
                }
                if (numPending > 0)
                    storebContext.CompletePending(wait: true);
            }
            Log.ShiftBeginAddress(originalUntilAddress, false);
            return originalUntilAddress;
        }

        private void ScanImmutableTailToRemoveFromTempKv<Input, Output, Context, Functions>(ref long untilAddress, long scanUntil,
                BasicContext<Key, Value, Input, Output, Context, Functions, TStoreFunctions, TAllocator> tempbContext)
            where Functions : ISessionFunctions<Key, Value, Input, Output, Context>
        {
            using var iter = Log.Scan(untilAddress, scanUntil);
            while (iter.GetNext(out var _))
            {
                tempbContext.Delete(ref iter.GetKey(), default);
                untilAddress = iter.NextAddress;
            }
        }
    }
}