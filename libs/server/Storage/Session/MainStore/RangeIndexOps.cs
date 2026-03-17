// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
        /// Read the RangeIndex stub from the store and return the BfTreeService.
        /// Returns null if the key doesn't exist or isn't a RangeIndex.
        /// </summary>
        private BfTreeService GetBfTreeForKey(PinnedSpanByte key, out GarnetStatus status)
        {
            parseState.InitializeWithArgument(key);
            var input = new StringInput(RespCommand.RIGET, ref parseState);
            Span<byte> stubSpan = stackalloc byte[RangeIndexManager.IndexSizeBytes];
            var output = StringOutput.FromPinnedSpan(stubSpan);

            var readStatus = stringBasicContext.Read((FixedSpanByteKey)key, ref input, ref output);

            if (readStatus.IsPending)
                CompletePendingForSession(ref readStatus, ref output, ref stringBasicContext);

            if (!readStatus.Found)
            {
                status = GarnetStatus.NOTFOUND;
                return null;
            }

            // Extract TreeHandle from stub
            var outputSpan = output.SpanByteAndMemory.IsSpanByte
                ? output.SpanByteAndMemory.SpanByte.ReadOnlySpan
                : output.SpanByteAndMemory.MemorySpan;

            if (outputSpan.Length < RangeIndexManager.IndexSizeBytes)
            {
                status = GarnetStatus.NOTFOUND;
                return null;
            }

            RangeIndexManager.ReadIndex(outputSpan,
                out var treeHandle, out _, out _, out _, out _, out _, out _, out _, out _);

            if (treeHandle == 0)
            {
                status = GarnetStatus.NOTFOUND;
                return null;
            }

            var gcHandle = GCHandle.FromIntPtr(treeHandle);
            if (!gcHandle.IsAllocated)
            {
                status = GarnetStatus.NOTFOUND;
                return null;
            }

            status = GarnetStatus.OK;
            return (BfTreeService)gcHandle.Target;
        }

        /// <summary>
        /// Create a new RangeIndex. Creates the BfTree and persists the stub via RMW.
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

            // Get a GCHandle to keep the BfTreeService alive and get a stable pointer
            var handle = GCHandle.Alloc(bfTree);
            var treeHandle = GCHandle.ToIntPtr(handle);

            // Pack the entire stub into a single parseState argument so InitialUpdater can read it
            var stub = new RangeIndexManager.RangeIndexStub
            {
                TreeHandle = treeHandle,
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

            var status = stringBasicContext.RMW((FixedSpanByteKey)key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForSession(ref status, ref output, ref stringBasicContext);

            if (status.Record.Created)
            {
                result = RangeIndexResult.OK;
                return GarnetStatus.OK;
            }

            if (status.Record.InPlaceUpdated || status.Found)
            {
                // Key already existed — free the new tree and return error
                bfTree.Dispose();
                handle.Free();
                result = RangeIndexResult.Error;
                errorMsg = "ERR index already exists"u8;
                return GarnetStatus.OK;
            }

            // RMW failed - free the BfTree
            handle.Free();
            bfTree.Dispose();
            errorMsg = "ERR failed to create range index"u8;
            return GarnetStatus.OK;
        }

        /// <summary>
        /// RI.SET — insert or update a field in a range index.
        /// </summary>
        public GarnetStatus RangeIndexSet(
            PinnedSpanByte key, PinnedSpanByte field, PinnedSpanByte value,
            out RangeIndexResult result, out ReadOnlySpan<byte> errorMsg)
        {
            errorMsg = default;
            var bfTree = GetBfTreeForKey(key, out _);
            if (bfTree == null)
            {
                result = RangeIndexResult.Error;
                errorMsg = "ERR no such range index"u8;
                return GarnetStatus.OK;
            }

            var insertResult = bfTree.Insert(field, value);
            if (insertResult == BfTreeInsertResult.InvalidKV)
            {
                result = RangeIndexResult.Error;
                errorMsg = "ERR invalid key or value size"u8;
                return GarnetStatus.OK;
            }

            result = RangeIndexResult.OK;
            return GarnetStatus.OK;
        }

        /// <summary>
        /// RI.GET — read a field from a range index.
        /// </summary>
        public GarnetStatus RangeIndexGet(
            PinnedSpanByte key, PinnedSpanByte field,
            out byte[] value, out RangeIndexResult result)
        {
            value = null;
            var bfTree = GetBfTreeForKey(key, out _);
            if (bfTree == null)
            {
                result = RangeIndexResult.Error;
                return GarnetStatus.OK;
            }

            var readResult = bfTree.Read(field.ReadOnlySpan, out value);
            result = readResult == BfTreeReadResult.Found ? RangeIndexResult.OK : RangeIndexResult.NotFound;
            return GarnetStatus.OK;
        }

        /// <summary>
        /// RI.DEL — delete a field from a range index.
        /// </summary>
        public GarnetStatus RangeIndexDel(
            PinnedSpanByte key, PinnedSpanByte field,
            out RangeIndexResult result)
        {
            var bfTree = GetBfTreeForKey(key, out _);
            if (bfTree == null)
            {
                result = RangeIndexResult.Error;
                return GarnetStatus.OK;
            }

            bfTree.Delete(field.ReadOnlySpan);
            result = RangeIndexResult.OK;
            return GarnetStatus.OK;
        }
    }
}