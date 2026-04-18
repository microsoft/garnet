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
        // Heap-size accounting on eviction is now handled internally by Tsavorite's
        // EvictRecordsInRange via logSizeTracker. The OnEvict trigger is only needed
        // for app-level resource cleanup, which Garnet does not require.
        public bool CallOnEvict(EvictionSource source) => false;

        /// <inheritdoc/>
        public bool CallOnDiskRead => false;

        /// <inheritdoc/>
        public void OnDispose(ref LogRecord logRecord, DisposeReason reason)
        {
            // Heap-size accounting for destruction is handled internally by Tsavorite
            // (logSizeTracker). This trigger is available for app-level resource cleanup
            // (e.g. calling IDisposable.Dispose on value objects that hold external resources).
            // Garnet's IHeapObject implementations (Hash/List/Set/SortedSet) hold no external
            // resources, so this is a no-op.
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
            // Heap-size accounting for eviction is handled internally by Tsavorite
            // (logSizeTracker decrements both key overflow and value heap in EvictRecordsInRange).
            // This trigger is available for app-level resource cleanup on eviction.
            // Garnet has no app-level eviction cleanup, so this is a no-op.
        }
    }
}