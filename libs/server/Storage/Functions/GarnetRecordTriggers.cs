// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Record lifecycle triggers for Garnet's unified store. Handles per-record cleanup
    /// on delete via <see cref="IRecordTriggers.OnDispose"/> and per-record heap-size
    /// accounting on page eviction via <see cref="IRecordTriggers.OnEvict"/>.
    /// </summary>
    public readonly struct GarnetRecordTriggers : IRecordTriggers
    {
        /// <summary>
        /// Cache size tracker for heap size accounting on delete and eviction.
        /// Created before the store and initialized after via <see cref="CacheSizeTracker.Initialize"/>.
        /// </summary>
        internal readonly CacheSizeTracker cacheSizeTracker;

        /// <summary>
        /// Creates a GarnetRecordTriggers with a cache size tracker.
        /// </summary>
        public GarnetRecordTriggers(CacheSizeTracker cacheSizeTracker)
        {
            this.cacheSizeTracker = cacheSizeTracker;
        }

        /// <inheritdoc/>
        public bool CallOnFlush => false;

        /// <inheritdoc/>
        // Drives per-record heap-size decrement on page eviction. Mirrors the work the
        // legacy SubscribeEvictions → LogSizeTracker.OnNext observer path used to perform
        // (see CacheSizeTracker.Initialize for the wiring change). Gated per source so
        // that enabling only a main-log or only a read-cache memory budget does not
        // force a per-record eviction walk on the other allocator.
        public bool CallOnEvict(EvictionSource source)
        {
            if (cacheSizeTracker is null)
                return false;
            return source == EvictionSource.ReadCache
                ? cacheSizeTracker.readCacheTracker is not null
                : cacheSizeTracker.mainLogTracker is not null;
        }

        /// <inheritdoc/>
        public bool CallOnDiskRead => false;

        /// <inheritdoc/>
        public void OnDispose(ref LogRecord logRecord, DisposeReason reason)
        {
            if (cacheSizeTracker is null)
                return;

            if (reason == DisposeReason.Deleted)
            {
                // Decrement only the VALUE-side heap (value overflow bytes or value-object heap).
                // Key overflow is NOT subtracted here because ClearHeapFields(clearKey=false) keeps the
                // key alive for hash-chain traversal. Key overflow is handled separately:
                //  - Mutable delete: record is tombstoned after OnDispose → OnEvict skips it → key overflow
                //    is a bounded phantom (freed by GC when the page is freed, same as pre-IRecordTriggers).
                //  - Immutable delete: source is sealed (not tombstoned) → OnEvict visits it and subtracts
                //    the remaining key overflow via CalculateHeapMemorySize.
                var size = logRecord.GetValueHeapMemorySize();
                if (size != 0)
                    cacheSizeTracker.AddHeapSize(-size);
            }
        }

        /// <inheritdoc/>
        // Transient records materialized from disk / network have no entry in cacheSizeTracker, so no
        // accounting decrement. Garnet's IHeapObject.Dispose() is a no-op today (Hash/List/Set/SortedSet
        // hold no external resources), so this hook is a no-op. If a future IHeapObject impl acquires
        // external resources the dispose call would go here, gated to skip scan-iterator wrappers that
        // share the value-object reference with the live on-log record.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnDisposeDiskRecord(ref DiskLogRecord logRecord, DisposeReason reason) { }

        /// <inheritdoc/>
        public void OnEvict(ref LogRecord logRecord, EvictionSource source)
        {
            if (cacheSizeTracker is null)
                return;

            // Decrement heap size by this record's heap contribution. Uses the same sizing
            // helper that LogSizeTracker.OnNext used to sum over an evicted iterator. Routes
            // through the standard AddHeapSize/AddReadCacheHeapSize path so the assertion
            // guarding against negative totals remains in force; creation sites on the main
            // log (RMW PostInitialUpdater/PostCopyUpdater and in-place grow/shrink) must emit
            // a matching positive bump so the account stays balanced.
            var size = MemoryUtils.CalculateHeapMemorySize(in logRecord);
            if (size == 0)
                return;
            if (source == EvictionSource.ReadCache)
                cacheSizeTracker.AddReadCacheHeapSize(-size);
            else
                cacheSizeTracker.AddHeapSize(-size);
        }
    }
}