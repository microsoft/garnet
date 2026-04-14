// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Record lifecycle triggers for Garnet's unified store. Handles per-record cleanup
    /// on delete via <see cref="IRecordTriggers.OnDispose"/>.
    /// </summary>
    public readonly struct GarnetRecordTriggers : IRecordTriggers
    {
        /// <summary>
        /// Cache size tracker for heap size accounting on delete.
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
        public bool CallOnEvict => false;

        /// <inheritdoc/>
        public bool CallOnDiskRead => false;

        /// <inheritdoc/>
        public void OnDisposeValueObject(IHeapObject valueObject, DisposeReason reason)
        {
            // Heap object disposal is handled by ClearHeapFields in ObjectAllocatorImpl
        }

        /// <inheritdoc/>
        public void OnDispose(ref LogRecord logRecord, DisposeReason reason)
        {
            // Handle heap objects: update cache size tracker on delete
            if (logRecord.Info.ValueIsObject && reason == DisposeReason.Deleted)
            {
                cacheSizeTracker?.AddHeapSize(-logRecord.ValueObject.HeapMemorySize);
            }
        }
    }
}