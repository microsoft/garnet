// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Record lifecycle triggers for Garnet's unified store. Handles per-record cleanup
    /// on delete via <see cref="IRecordTriggers.OnDispose"/>.
    /// </summary>
    public struct GarnetRecordTrigger : IRecordTriggers
    {
        /// <summary>
        /// Holder for cache size tracker reference. Uses a wrapper class so the reference
        /// can be set after store creation (CacheSizeTracker requires the store in its
        /// constructor, but GarnetRecordTrigger is created with the store).
        /// </summary>
        public sealed class CacheSizeTrackerHolder
        {
            /// <summary>The cache size tracker, set after store creation.</summary>
            public CacheSizeTracker Tracker;
        }

        /// <summary>
        /// Holder for cache size tracker, set after store creation.
        /// </summary>
        internal readonly CacheSizeTrackerHolder cacheSizeTrackerHolder;

        /// <summary>
        /// Creates a GarnetRecordTrigger.
        /// </summary>
        public GarnetRecordTrigger(CacheSizeTrackerHolder cacheSizeTrackerHolder)
        {
            this.cacheSizeTrackerHolder = cacheSizeTrackerHolder;
        }

        /// <inheritdoc/>
        public readonly bool CallOnFlush => false;

        /// <inheritdoc/>
        public readonly bool CallOnEvict => false;

        /// <inheritdoc/>
        public readonly bool CallOnDiskRead => false;

        /// <inheritdoc/>
        public readonly void OnDisposeValueObject(IHeapObject valueObject, DisposeReason reason)
        {
            // Heap object disposal is handled by ClearHeapFields in ObjectAllocatorImpl
        }

        /// <inheritdoc/>
        public readonly void OnDispose(ref LogRecord logRecord, DisposeReason reason)
        {
            // Handle heap objects: update cache size tracker on delete
            if (logRecord.Info.ValueIsObject && reason == DisposeReason.Deleted)
            {
                cacheSizeTrackerHolder?.Tracker?.AddHeapSize(-logRecord.ValueObject.HeapMemorySize);
            }
        }
    }
}