// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using Garnet.server.BfTreeInterop;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

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

        /// <summary>
        /// Tracks live BfTreeService instances keyed by their native tree pointer.
        /// Only touched on CREATE (add) and DEL/Dispose (remove) — never on hot-path data ops.
        /// </summary>
        private readonly ConcurrentDictionary<nint, BfTreeService> liveIndexes = new();

        private readonly ILogger logger;

        private readonly LightEpoch rangeIndexEpoch;

        /// <summary>
        /// Creates a new <see cref="RangeIndexManager"/>.
        /// </summary>
        public RangeIndexManager(ILogger logger = null)
        {
            this.logger = logger;
            this.rangeIndexEpoch = new LightEpoch();
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
        /// Compute the leaf page size from the max record size.
        /// For max record size &lt;= 2KB: page size = 4KB.
        /// For larger (up to 16KB): 2.5x record size rounded to next power of 2, max 32KB.
        /// </summary>
        internal static uint ComputeLeafPageSize(uint maxRecordSize)
        {
            if (maxRecordSize <= 2048)
                return 4096;

            // 2.5x, capped at 32KB
            var target = (uint)(maxRecordSize * 2.5);
            if (target > 32768)
                target = 32768;

            // Round up to next power of 2
            return RoundUpToPowerOf2(target);
        }

        private static uint RoundUpToPowerOf2(uint v)
        {
            v--;
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            v++;
            return v;
        }

        /// <summary>
        /// Register a BfTreeService after successful index creation. Cold path only.
        /// </summary>
        internal void RegisterIndex(BfTreeService bfTree)
        {
            liveIndexes[bfTree.NativePtr] = bfTree;
        }

        /// <summary>
        /// Unregister and dispose a BfTreeService.
        /// Called while the caller already holds an exclusive lock.
        /// Returns true if the index was found and disposed.
        /// </summary>
        internal bool UnregisterIndex(nint treePtr)
        {
            if (liveIndexes.TryRemove(treePtr, out var bfTree))
            {
                bfTree.Dispose();
                return true;
            }
            return false;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            foreach (var kvp in liveIndexes)
            {
                try
                {
                    kvp.Value.Dispose();
                }
                catch (Exception ex)
                {
                    logger?.LogWarning(ex, "Failed to dispose BfTree with native pointer {Ptr}", kvp.Key);
                }
            }
            liveIndexes.Clear();
            rangeIndexEpoch.Dispose();
        }
    }
}