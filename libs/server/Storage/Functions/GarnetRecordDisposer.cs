// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Record disposer for Garnet's unified store. Enables per-record disposal
    /// on page eviction and record lifecycle events via <see cref="IRecordDisposer.DisposeRecord"/>.
    /// </summary>
    public struct GarnetRecordDisposer : IRecordDisposer
    {
        /// <inheritdoc/>
        public readonly bool DisposeOnPageEviction => false;

        /// <inheritdoc/>
        public readonly void DisposeValueObject(IHeapObject valueObject, DisposeReason reason)
        {
            // Heap object disposal is handled by ClearHeapFields in ObjectAllocatorImpl
        }

        /// <inheritdoc/>
        public readonly void DisposeRecord(ref LogRecord logRecord, DisposeReason reason)
        {
            // Dispatch based on RecordType for inline records with external resources.
            // Specific record type handlers (e.g. RangeIndex) will be added here.
        }
    }
}