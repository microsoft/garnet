// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.server.BfTreeInterop;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Manages RangeIndex (BfTree) instances stored as stubs in the main store.
    /// </summary>
    public sealed partial class RangeIndexManager : IDisposable
    {
        /// <summary>RecordType value for RangeIndex records in the unified store.</summary>
        internal const byte RangeIndexRecordType = 2;

        /// <summary>Size of the RangeIndex stub in bytes.</summary>
        internal const int IndexSizeBytes = RangeIndexStub.Size;

        /// <summary>
        /// Unique id for this <see cref="RangeIndexManager"/> instance.
        /// Used to detect stale pointers after process restart.
        /// </summary>
        private readonly Guid processInstanceId = Guid.NewGuid();

        /// <summary>Gets the process instance ID for this manager.</summary>
        internal Guid ProcessInstanceId => processInstanceId;

        private readonly ILogger logger;

        /// <summary>
        /// Creates a new <see cref="RangeIndexManager"/>.
        /// </summary>
        public RangeIndexManager(ILogger logger = null)
        {
            this.logger = logger;
        }

        /// <summary>
        /// Creates a new BfTree instance via the native interop layer.
        /// </summary>
        internal BfTreeService CreateBfTree(
            StorageBackendType storageBackend,
            string filePath,
            ulong cacheSize,
            uint minRecordSize,
            uint maxRecordSize,
            uint maxKeyLen,
            uint leafPageSize)
        {
            return new BfTreeService(
                storageBackend: storageBackend,
                filePath: filePath,
                cbSizeByte: cacheSize,
                cbMinRecordSize: minRecordSize,
                cbMaxRecordSize: maxRecordSize,
                cbMaxKeyLen: maxKeyLen,
                leafPageSize: leafPageSize);
        }

        /// <summary>
        /// Drops (frees) a BfTree instance.
        /// </summary>
        internal static void DropBfTree(BfTreeService tree)
        {
            tree?.Dispose();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            // Nothing to dispose at manager level; individual BfTree instances are
            // dropped via DisposeRecord callbacks when their store records are deleted.
        }
    }
}