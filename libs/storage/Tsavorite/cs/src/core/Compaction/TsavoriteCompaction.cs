// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Compaction methods
    /// </summary>
    public partial class TsavoriteKV<Key, Value> : TsavoriteBase
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
            where Functions : IFunctions<Key, Value, Input, Output, Context>
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
            where Functions : IFunctions<Key, Value, Input, Output, Context>
            where CompactionFunctions : ICompactionFunctions<Key, Value>
        {
            if (untilAddress > hlog.SafeReadOnlyAddress)
                throw new TsavoriteException("Can compact only until Log.SafeReadOnlyAddress");

            var lf = new LogCompactionFunctions<Key, Value, Input, Output, Context, Functions>(functions);
            using var storeSession = NewSession<Input, Output, Context, LogCompactionFunctions<Key, Value, Input, Output, Context, Functions>>(lf);

            using (var iter1 = Log.Scan(Log.BeginAddress, untilAddress))
            {
                long numPending = 0;
                while (iter1.GetNext(out var recordInfo))
                {
                    ref var key = ref iter1.GetKey();
                    ref var value = ref iter1.GetValue();

                    if (!recordInfo.Tombstone && !cf.IsDeleted(ref key, ref value))
                    {
                        var status = storeSession.CompactionCopyToTail(ref key, ref input, ref value, ref output, iter1.NextAddress);
                        if (status.IsPending && ++numPending > 256)
                        {
                            storeSession.CompletePending(wait: true);
                            numPending = 0;
                        }
                    }

                    // Ensure address is at record boundary
                    untilAddress = iter1.NextAddress;
                }
                if (numPending > 0)
                    storeSession.CompletePending(wait: true);
            }
            Log.ShiftBeginAddress(untilAddress, false);
            return untilAddress;
        }

        private long CompactScan<Input, Output, Context, Functions, CompactionFunctions>(Functions functions, CompactionFunctions cf, ref Input input, ref Output output, long untilAddress)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
            where CompactionFunctions : ICompactionFunctions<Key, Value>
        {
            if (untilAddress > hlog.SafeReadOnlyAddress)
                throw new TsavoriteException("Can compact only until Log.SafeReadOnlyAddress");

            var originalUntilAddress = untilAddress;

            var lf = new LogCompactionFunctions<Key, Value, Input, Output, Context, Functions>(functions);
            using var storeSession = NewSession<Input, Output, Context, LogCompactionFunctions<Key, Value, Input, Output, Context, Functions>>(lf);

            using (var tempKv = new TsavoriteKV<Key, Value>(IndexSize, new LogSettings { LogDevice = new NullDevice(), ObjectLogDevice = new NullDevice() }, comparer: Comparer, loggerFactory: loggerFactory))
            using (var tempKvSession = tempKv.NewSession<Input, Output, Context, Functions>(functions))
            {
                using (var iter1 = Log.Scan(hlog.BeginAddress, untilAddress))
                {
                    while (iter1.GetNext(out var recordInfo))
                    {
                        ref var key = ref iter1.GetKey();
                        ref var value = ref iter1.GetValue();

                        if (recordInfo.Tombstone || cf.IsDeleted(ref key, ref value))
                            tempKvSession.Delete(ref key);
                        else
                            tempKvSession.Upsert(ref key, ref value);
                    }
                    // Ensure address is at record boundary
                    untilAddress = originalUntilAddress = iter1.NextAddress;
                }

                // Scan until SafeReadOnlyAddress
                var scanUntil = hlog.SafeReadOnlyAddress;
                if (untilAddress < scanUntil)
                    ScanImmutableTailToRemoveFromTempKv(ref untilAddress, scanUntil, tempKvSession);

                var numPending = 0;
                using var iter3 = tempKv.Log.Scan(tempKv.Log.BeginAddress, tempKv.Log.TailAddress);
                while (iter3.GetNext(out var recordInfo))
                {
                    if (recordInfo.Tombstone)
                        continue;

                    // Try to ensure we have checked all immutable records
                    scanUntil = hlog.SafeReadOnlyAddress;
                    if (untilAddress < scanUntil)
                        ScanImmutableTailToRemoveFromTempKv(ref untilAddress, scanUntil, tempKvSession);

                    // If record is not the latest in tempKv's memory for this key, ignore it (will not be returned if deleted)
                    if (!tempKvSession.ContainsKeyInMemory(ref iter3.GetKey(), out long tempKeyAddress).Found || iter3.CurrentAddress != tempKeyAddress)
                        continue;

                    // As long as there's no record of the same key whose address is >= untilAddress (scan boundary), we are safe to copy the old record
                    // to the tail. We don't know the actualAddress of the key in the main kv, but we it will not be below untilAddress.
                    var status = storeSession.CompactionCopyToTail(ref iter3.GetKey(), ref input, ref iter3.GetValue(), ref output, untilAddress - 1);
                    if (status.IsPending && ++numPending > 256)
                    {
                        storeSession.CompletePending(wait: true);
                        numPending = 0;
                    }
                }
                if (numPending > 0)
                    storeSession.CompletePending(wait: true);
            }
            Log.ShiftBeginAddress(originalUntilAddress, false);
            return originalUntilAddress;
        }

        private void ScanImmutableTailToRemoveFromTempKv<Input, Output, Context, Functions>(ref long untilAddress, long scanUntil, ClientSession<Key, Value, Input, Output, Context, Functions> tempKvSession)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            using var iter = Log.Scan(untilAddress, scanUntil);
            while (iter.GetNext(out var _))
            {
                tempKvSession.Delete(ref iter.GetKey(), default, 0);
                untilAddress = iter.NextAddress;
            }
        }
    }
}