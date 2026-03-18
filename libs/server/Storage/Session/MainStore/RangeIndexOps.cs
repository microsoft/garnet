// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using Garnet.common;
using Garnet.server.BfTreeInterop;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Storage session methods for RangeIndex operations.
    /// </summary>
    sealed unsafe partial class StorageSession
    {
        /// <summary>
        /// Create a new RangeIndex. Creates the BfTree and persists the stub via RMW.
        /// No lock needed — RMW is atomic.
        /// </summary>
        public GarnetStatus RangeIndexCreate(
            PinnedSpanByte key,
            byte storageBackend,
            string filePath,
            ulong cacheSize,
            uint minRecordSize,
            uint maxRecordSize,
            uint maxKeyLen,
            uint leafPageSize,
            out RangeIndexResult result,
            out ReadOnlySpan<byte> errorMsg)
        {
            result = RangeIndexResult.Error;
            errorMsg = default;

            var rangeIndexManager = functionsState.rangeIndexManager;

            // Create the BfTree instance via interop
            BfTreeService bfTree;
            try
            {
                bfTree = rangeIndexManager.CreateBfTree(
                    (StorageBackendType)storageBackend, filePath, cacheSize,
                    minRecordSize, maxRecordSize, maxKeyLen, leafPageSize);
            }
            catch (Exception ex)
            {
                errorMsg = System.Text.Encoding.UTF8.GetBytes($"ERR {ex.Message}");
                return GarnetStatus.OK;
            }

            // Store the native BfTree pointer in the stub
            var treePtr = bfTree.NativePtr;

            var stub = new RangeIndexManager.RangeIndexStub
            {
                TreeHandle = treePtr,
                CacheSize = cacheSize,
                MinRecordSize = minRecordSize,
                MaxRecordSize = maxRecordSize,
                MaxKeyLen = maxKeyLen,
                LeafPageSize = leafPageSize,
                StorageBackend = storageBackend,
                Flags = 0,
                SerializationPhase = 0,
                ProcessInstanceId = rangeIndexManager.ProcessInstanceId
            };

            var psb = PinnedSpanByte.FromPinnedPointer((byte*)Unsafe.AsPointer(ref stub), RangeIndexManager.RangeIndexStub.Size);

            parseState.InitializeWithArgument(psb);

            var input = new StringInput(RespCommand.RICREATE, ref parseState);
            var output = new StringOutput();

            // RMW is atomic — no external lock needed
            var status = stringBasicContext.RMW((FixedSpanByteKey)key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForSession(ref status, ref output, ref stringBasicContext);

            if (status.Record.Created)
            {
                // Register for lifecycle management (cold path)
                rangeIndexManager.RegisterIndex(bfTree);
                result = RangeIndexResult.OK;
                return GarnetStatus.OK;
            }

            if (status.Record.InPlaceUpdated || status.Found)
            {
                // Key already existed — free the new tree and return error
                bfTree.Dispose();
                result = RangeIndexResult.Error;
                errorMsg = "ERR index already exists"u8;
                return GarnetStatus.OK;
            }

            // RMW failed - free the BfTree
            bfTree.Dispose();
            errorMsg = "ERR failed to create range index"u8;
            return GarnetStatus.OK;
        }

        /// <summary>
        /// RI.SET — insert or update a field in a range index.
        /// Hot path: acquires shared lock, reads stub, calls BfTreeService while lock is held.
        /// </summary>
        public GarnetStatus RangeIndexSet(
            PinnedSpanByte key, PinnedSpanByte field, PinnedSpanByte value,
            out RangeIndexResult result, out ReadOnlySpan<byte> errorMsg)
        {
            errorMsg = default;

            parseState.InitializeWithArgument(key);
            var input = new StringInput(RespCommand.RISET, ref parseState);
            Span<byte> stubSpan = stackalloc byte[RangeIndexManager.IndexSizeBytes];

            using (functionsState.rangeIndexManager.ReadRangeIndex(this, key, ref input, stubSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    result = RangeIndexResult.Error;
                    errorMsg = "ERR no such range index"u8;
                    return GarnetStatus.OK;
                }

                var treePtr = ExtractTreePtr(stubSpan);
                if (treePtr == 0)
                {
                    result = RangeIndexResult.Error;
                    errorMsg = "ERR no such range index"u8;
                    return GarnetStatus.OK;
                }

                var insertResult = BfTreeService.InsertByPtr(treePtr, field, value);
                if (insertResult == BfTreeInsertResult.InvalidKV)
                {
                    result = RangeIndexResult.Error;
                    errorMsg = "ERR invalid key or value size"u8;
                    return GarnetStatus.OK;
                }

                result = RangeIndexResult.OK;
                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// RI.GET — read a field from a range index.
        /// Hot path: acquires shared lock, reads stub, calls BfTreeService while lock is held.
        /// </summary>
        public GarnetStatus RangeIndexGet(
            PinnedSpanByte key, PinnedSpanByte field,
            out byte[] value, out RangeIndexResult result)
        {
            value = null;

            parseState.InitializeWithArgument(key);
            var input = new StringInput(RespCommand.RIGET, ref parseState);
            Span<byte> stubSpan = stackalloc byte[RangeIndexManager.IndexSizeBytes];

            using (functionsState.rangeIndexManager.ReadRangeIndex(this, key, ref input, stubSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    result = RangeIndexResult.Error;
                    return GarnetStatus.OK;
                }

                var treePtr = ExtractTreePtr(stubSpan);
                if (treePtr == 0)
                {
                    result = RangeIndexResult.Error;
                    return GarnetStatus.OK;
                }

                var readResult = BfTreeService.ReadByPtr(treePtr, field, out value);
                result = readResult == BfTreeReadResult.Found ? RangeIndexResult.OK : RangeIndexResult.NotFound;
                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// RI.DEL — delete a field from a range index.
        /// Hot path: acquires shared lock, reads stub, calls BfTreeService while lock is held.
        /// </summary>
        public GarnetStatus RangeIndexDel(
            PinnedSpanByte key, PinnedSpanByte field,
            out RangeIndexResult result)
        {
            parseState.InitializeWithArgument(key);
            var input = new StringInput(RespCommand.RIDEL, ref parseState);
            Span<byte> stubSpan = stackalloc byte[RangeIndexManager.IndexSizeBytes];

            using (functionsState.rangeIndexManager.ReadRangeIndex(this, key, ref input, stubSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    result = RangeIndexResult.Error;
                    return GarnetStatus.OK;
                }

                var treePtr = ExtractTreePtr(stubSpan);
                if (treePtr == 0)
                {
                    result = RangeIndexResult.Error;
                    return GarnetStatus.OK;
                }

                BfTreeService.DeleteByPtr(treePtr, field);
                result = RangeIndexResult.OK;
                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// Extract the native tree pointer from a stub span.
        /// </summary>
        private static nint ExtractTreePtr(Span<byte> stubSpan)
        {
            if (stubSpan.Length < RangeIndexManager.IndexSizeBytes)
                return 0;

            RangeIndexManager.ReadIndex(stubSpan,
                out var treePtr, out _, out _, out _, out _, out _, out _, out _, out _);

            return treePtr;
        }
    }
}