// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Garnet.server.BfTreeInterop;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Storage session methods for RangeIndex operations.
    ///
    /// <para>Each method follows the same pattern: prepare a <see cref="StringInput"/> with
    /// the appropriate <see cref="RespCommand"/>, then either:
    /// (a) Issue an RMW for lifecycle ops (CREATE, PROMOTE, RESTORE), or
    /// (b) Acquire a shared lock via <see cref="RangeIndexManager.ReadRangeIndex"/>,
    ///     read the stub, operate on the native BfTree, and release the lock on dispose.</para>
    ///
    /// <para>WRONGTYPE handling: RI-specific commands on non-RI keys (and vice versa) are
    /// rejected by <c>ReadMethods.cs</c> / <c>RMWMethods.cs</c> type-safety checks, which
    /// return <see cref="GarnetStatus.WRONGTYPE"/>.</para>
    /// </summary>
    sealed unsafe partial class StorageSession
    {
        /// <summary>
        /// Create a new RangeIndex. Creates the BfTree via native interop, serializes the
        /// configuration into a <see cref="RangeIndexManager.RangeIndexStub"/>, and persists
        /// it via RMW (which triggers InitialUpdater in <c>RMWMethods.cs</c>).
        /// </summary>
        /// <remarks>
        /// <para>No external lock is needed — RMW is atomic at the store level.</para>
        /// <para>Corner case: If the key already exists (duplicate RI.CREATE), the RMW
        /// returns <c>InPlaceUpdated</c> (existing RI key) or <c>Found</c> (non-RI key).
        /// In both cases, the freshly created BfTree is disposed and an error is returned.</para>
        /// </remarks>
        /// <param name="key">The Garnet key for the new index.</param>
        /// <param name="storageBackend">0 for Disk, 1 for Memory.</param>
        /// <param name="cacheSize">BfTree circular buffer size in bytes.</param>
        /// <param name="minRecordSize">Minimum record size.</param>
        /// <param name="maxRecordSize">Maximum record size.</param>
        /// <param name="maxKeyLen">Maximum key length.</param>
        /// <param name="leafPageSize">Leaf page size (0 = auto-compute).</param>
        /// <param name="result">The operation result code.</param>
        /// <param name="errorMsg">Error message span if the operation failed.</param>
        /// <returns><see cref="GarnetStatus.OK"/> on success (check <paramref name="result"/> for details).</returns>
        public GarnetStatus RangeIndexCreate(
            PinnedSpanByte key,
            byte storageBackend,
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

            // Auto-compute leaf page size if not specified
            if (leafPageSize == 0)
                leafPageSize = RangeIndexManager.ComputeLeafPageSize(maxRecordSize);

            // Create the BfTree instance via interop
            BfTreeService bfTree;
            try
            {
                bfTree = rangeIndexManager.CreateBfTree(
                    (StorageBackendType)storageBackend, key.ReadOnlySpan, cacheSize,
                    minRecordSize, maxRecordSize, maxKeyLen, leafPageSize);
            }
            catch (Exception ex)
            {
                errorMsg = System.Text.Encoding.UTF8.GetBytes($"ERR {ex.Message}");
                return GarnetStatus.OK;
            }

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
                SerializationPhase = 0,
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
                var keyHash = stringBasicContext.GetKeyHash((FixedSpanByteKey)key);
                rangeIndexManager.RegisterIndex(bfTree, keyHash, key.ReadOnlySpan);
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
        /// Promote a RangeIndex stub from the read-only region to the mutable region (tail).
        /// Issues an RMW with RIPROMOTE; CopyUpdater copies the stub bytes and clears the flushed flag.
        /// Uses a local parseState to avoid corrupting the caller's parseState.
        /// </summary>
        internal void PromoteRangeIndexToTail(PinnedSpanByte key)
        {
            var promoteParseState = new SessionParseState();
            promoteParseState.InitializeWithArgument(key);
            var input = new StringInput(RespCommand.RIPROMOTE, ref promoteParseState);
            var output = new StringOutput();

            var status = stringBasicContext.RMW((FixedSpanByteKey)key, ref input, ref output);
            if (status.IsPending)
                CompletePendingForSession(ref status, ref output, ref stringBasicContext);
        }

        /// <summary>
        /// Restore a BfTree stub by updating TreeHandle in the mutable record.
        /// Issues an RMW with RIRESTORE; IPU sets the TreeHandle on the mutable stub.
        /// </summary>
        internal void RestoreRangeIndexStub(PinnedSpanByte key, nint newTreeHandle)
        {
            var restoreParseState = new SessionParseState();
            restoreParseState.InitializeWithArgument(key);
            var input = new StringInput(RespCommand.RIRESTORE, ref restoreParseState, arg1: (long)newTreeHandle);
            var output = new StringOutput();

            var status = stringBasicContext.RMW((FixedSpanByteKey)key, ref input, ref output);
            if (status.IsPending)
                CompletePendingForSession(ref status, ref output, ref stringBasicContext);
        }

        /// <summary>
        /// RI.SET — insert or update a field in a range index.
        /// </summary>
        /// <remarks>
        /// Hot path: acquires shared lock via <see cref="RangeIndexManager.ReadRangeIndex"/>,
        /// reads the stub to get the native BfTree pointer, calls <see cref="BfTreeService.InsertByPtr"/>
        /// while the lock is held, then injects a synthetic RMW for AOF logging.
        ///
        /// <para>Corner cases:</para>
        /// <list type="bullet">
        /// <item>Key doesn't exist → <c>NOTFOUND</c> from ReadRangeIndex → returns error.</item>
        /// <item>Key is not an RI key → <c>WRONGTYPE</c> from ReadMethods type-safety.</item>
        /// <item>Field/value exceeds configured limits → <c>InvalidKV</c> from native BfTree.</item>
        /// <item>TreeHandle is zero (evicted/recovering) → ReadRangeIndex handles lazy restore internally.</item>
        /// </list>
        /// </remarks>
        /// <param name="key">The Garnet key of the RangeIndex.</param>
        /// <param name="field">The field (BfTree key) to set.</param>
        /// <param name="value">The value to associate with the field.</param>
        /// <param name="result">The operation result code.</param>
        /// <param name="errorMsg">Error message span if the operation failed.</param>
        /// <returns><see cref="GarnetStatus.WRONGTYPE"/> if key exists but is not an RI key; <see cref="GarnetStatus.OK"/> otherwise.</returns>
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
                if (status == GarnetStatus.WRONGTYPE)
                {
                    result = RangeIndexResult.Error;
                    return GarnetStatus.WRONGTYPE;
                }

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
                    ref readonly var stub = ref RangeIndexManager.ReadIndex(stubSpan);
                    result = RangeIndexResult.Error;
                    errorMsg = System.Text.Encoding.UTF8.GetBytes(
                        $"ERR key+value size must be between {stub.MinRecordSize} and {stub.MaxRecordSize} bytes (got {field.Length + value.Length}), max key length {stub.MaxKeyLen} (got {field.Length})");
                    return GarnetStatus.OK;
                }

                result = RangeIndexResult.OK;

                functionsState.rangeIndexManager.ReplicateRangeIndexSet(
                    key, field, value, functionsState.appendOnlyFile,
                    stringBasicContext.Session.Version, stringBasicContext.Session.ID,
                    functionsState.StoredProcMode);

                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// RI.GET — read a field from a range index.
        /// </summary>
        /// <remarks>
        /// Hot path: acquires shared lock, reads stub, reads from BfTree,
        /// and writes the value as a RESP bulk string directly into <paramref name="output"/>.
        ///
        /// <para>Uses a two-path strategy to avoid allocations:</para>
        /// <list type="bullet">
        /// <item><b>Inline path</b>: If the network buffer has enough space, reads the BfTree
        /// value directly into the output buffer past a reserved RESP header, then backfills
        /// the exact header and adjusts pointers.</item>
        /// <item><b>Heap path</b>: If the network buffer is too small, rents a pooled buffer
        /// from <c>memoryPool</c>, reads into it, and sets <c>output.SpanByteAndMemory.Memory</c>.</item>
        /// </list>
        /// </remarks>
        /// <param name="key">The Garnet key of the RangeIndex.</param>
        /// <param name="field">The field (BfTree key) to read.</param>
        /// <param name="output">The output to write the RESP-formatted value into.</param>
        /// <param name="result">The operation result code.</param>
        /// <returns><see cref="GarnetStatus.WRONGTYPE"/> if the key is not an RI key; <see cref="GarnetStatus.OK"/> otherwise.</returns>
        public GarnetStatus RangeIndexGet(
            PinnedSpanByte key, PinnedSpanByte field,
            ref StringOutput output, out RangeIndexResult result)
        {
            parseState.InitializeWithArgument(key);
            var input = new StringInput(RespCommand.RIGET, ref parseState);
            Span<byte> stubSpan = stackalloc byte[RangeIndexManager.IndexSizeBytes];

            using (functionsState.rangeIndexManager.ReadRangeIndex(this, key, ref input, stubSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    result = RangeIndexResult.Error;
                    return status == GarnetStatus.WRONGTYPE ? GarnetStatus.WRONGTYPE : GarnetStatus.OK;
                }

                ref readonly var stub = ref RangeIndexManager.ReadIndex(stubSpan);
                if (stub.TreeHandle == 0)
                {
                    result = RangeIndexResult.Error;
                    return GarnetStatus.OK;
                }

                Debug.Assert(output.SpanByteAndMemory.IsSpanByte);

                var bufLen = output.SpanByteAndMemory.Length;
                var maxValueSize = (int)stub.MaxRecordSize;

                const int optimisticHeaderSize = 1 + 1 + 2; // $N\r\n
                const int trailerSize = 2; // \r\n
                const int maxHeaderSize = 1 + 10 + 2; // $<maxDigits>\r\n
                var minBufferNeeded = maxHeaderSize + maxValueSize + trailerSize; // $<maxDigits>\r\n + value + \r\n

                if (bufLen >= minBufferNeeded)
                {
                    // Read BfTree value directly into output buffer past the header reservation
                    var bufStart = output.SpanByteAndMemory.SpanByte.ToPointer();
                    var valueStart = bufStart + optimisticHeaderSize;
                    var readResult = BfTreeService.ReadByPtrInto(stub.TreeHandle, field, valueStart, maxValueSize, out var bytesWritten);

                    if (readResult != BfTreeReadResult.Found || bytesWritten < 0)
                    {
                        result = RangeIndexResult.NotFound;
                        return GarnetStatus.OK;
                    }

                    // Backfill exact header, append trailer, shift to eliminate gap
                    var numLength = NumUtils.CountDigits(bytesWritten);
                    var actualHeaderSize = 1 + numLength + 2; // $N\r\n
                    var actualValueStart = bufStart + actualHeaderSize;
                    if (valueStart != actualValueStart)
                        Buffer.MemoryCopy(valueStart, actualValueStart, bytesWritten, bytesWritten);

                    var tmp = bufStart;
                    *tmp++ = (byte)'$';
                    NumUtils.WriteInt32(bytesWritten, numLength, ref tmp);
                    *tmp++ = (byte)'\r';
                    *tmp = (byte)'\n';

                    var trailerPtr = actualValueStart + bytesWritten;
                    *trailerPtr++ = (byte)'\r';
                    *trailerPtr = (byte)'\n';

                    var totalLen = actualHeaderSize + bytesWritten + trailerSize;
                    output.SpanByteAndMemory.Length = totalLen;
                    result = RangeIndexResult.OK;
                    return GarnetStatus.OK;
                }
                else
                {
                    // Not enough space in network buffer — fall through to heap path
                    output.SpanByteAndMemory.ConvertToHeap();
                    var heapMemory = functionsState.memoryPool.Rent(minBufferNeeded);

                    fixed (byte* bufStart = heapMemory.Memory.Span)
                    {
                        var valueStart = bufStart + optimisticHeaderSize;
                        var readResult = BfTreeService.ReadByPtrInto(stub.TreeHandle, field, valueStart, maxValueSize, out var bytesWritten);

                        if (readResult != BfTreeReadResult.Found || bytesWritten < 0)
                        {
                            heapMemory.Dispose();
                            result = RangeIndexResult.NotFound;
                            return GarnetStatus.OK;
                        }

                        // Backfill exact header, append trailer, shift to eliminate gap
                        var numLength = NumUtils.CountDigits(bytesWritten);
                        var actualHeaderSize = 1 + numLength + 2; // $N\r\n
                        var actualValueStart = bufStart + actualHeaderSize;
                        if (valueStart != actualValueStart)
                            Buffer.MemoryCopy(valueStart, actualValueStart, bytesWritten, bytesWritten);

                        var tmp = bufStart;
                        *tmp++ = (byte)'$';
                        NumUtils.WriteInt32(bytesWritten, numLength, ref tmp);
                        *tmp++ = (byte)'\r';
                        *tmp = (byte)'\n';

                        var trailerPtr = actualValueStart + bytesWritten;
                        *trailerPtr++ = (byte)'\r';
                        *trailerPtr = (byte)'\n';

                        var totalLen = actualHeaderSize + bytesWritten + trailerSize;

                        output.SpanByteAndMemory.Length = totalLen;
                        output.SpanByteAndMemory.Memory = heapMemory;
                        result = RangeIndexResult.OK;
                        return GarnetStatus.OK;
                    }
                }
            }
        }

        /// <summary>
        /// RI.DEL — delete a field from a range index.
        /// </summary>
        /// <remarks>
        /// Hot path: acquires shared lock, reads stub, calls <see cref="BfTreeService.DeleteByPtr"/>
        /// while lock is held, then injects a synthetic RMW for AOF logging.
        /// Note: BfTree delete is a logical tombstone; the field cannot be read after deletion.
        /// </remarks>
        /// <param name="key">The Garnet key of the RangeIndex.</param>
        /// <param name="field">The field (BfTree key) to delete.</param>
        /// <param name="result">The operation result code.</param>
        /// <returns><see cref="GarnetStatus.WRONGTYPE"/> if the key is not an RI key; <see cref="GarnetStatus.OK"/> otherwise.</returns>
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
                    return status == GarnetStatus.WRONGTYPE ? GarnetStatus.WRONGTYPE : GarnetStatus.OK;
                }

                var treePtr = ExtractTreePtr(stubSpan);
                if (treePtr == 0)
                {
                    result = RangeIndexResult.Error;
                    return GarnetStatus.OK;
                }

                BfTreeService.DeleteByPtr(treePtr, field);
                result = RangeIndexResult.OK;

                functionsState.rangeIndexManager.ReplicateRangeIndexDel(
                    key, field, functionsState.appendOnlyFile,
                    stringBasicContext.Session.Version, stringBasicContext.Session.ID,
                    functionsState.StoredProcMode);

                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// RI.SCAN — scan entries starting at a key with a count limit.
        /// Acquires shared lock, reads stub, writes complete RESP array response into output while lock is held.
        /// </summary>
        /// <param name="key">The Garnet key of the RangeIndex.</param>
        /// <param name="startKey">The BfTree key to start scanning from (inclusive).</param>
        /// <param name="count">Maximum number of records to return.</param>
        /// <param name="returnField">Which fields to include in the response (Key, Value, or Both).</param>
        /// <param name="output">The output to write the RESP-formatted response into.</param>
        /// <param name="recordCount">On return, the number of records written.</param>
        /// <param name="result">The operation result code.</param>
        /// <returns><see cref="GarnetStatus.WRONGTYPE"/> if the key is not an RI key; <see cref="GarnetStatus.OK"/> otherwise.</returns>
        public GarnetStatus RangeIndexScan(
            PinnedSpanByte key, PinnedSpanByte startKey, int count,
            ScanReturnField returnField, ref StringOutput output,
            out int recordCount, out RangeIndexResult result)
        {
            recordCount = 0;

            parseState.InitializeWithArgument(key);
            var input = new StringInput(RespCommand.RISCAN, ref parseState);
            Span<byte> stubSpan = stackalloc byte[RangeIndexManager.IndexSizeBytes];

            using (functionsState.rangeIndexManager.ReadRangeIndex(this, key, ref input, stubSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    result = RangeIndexResult.Error;
                    return status == GarnetStatus.WRONGTYPE ? GarnetStatus.WRONGTYPE : GarnetStatus.OK;
                }

                var treePtr = ExtractTreePtr(stubSpan);
                if (treePtr == 0)
                {
                    result = RangeIndexResult.Error;
                    return GarnetStatus.OK;
                }

                if (RangeIndexManager.ReadIndex(stubSpan).StorageBackend == (byte)StorageBackendType.Memory)
                {
                    result = RangeIndexResult.MemoryModeNotSupported;
                    return GarnetStatus.OK;
                }

                WriteScanToOutput(treePtr, startKey.ReadOnlySpan, count, returnField,
                    isScanWithCount: true, [],
                    ref output, out recordCount);
                result = RangeIndexResult.OK;
                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// RI.RANGE — scan entries in the closed range [start, end] from a range index.
        /// Acquires shared lock, reads stub, writes complete RESP array response into output while lock is held.
        /// </summary>
        /// <param name="key">The Garnet key of the RangeIndex.</param>
        /// <param name="startKey">The BfTree key to start scanning from (inclusive).</param>
        /// <param name="endKey">The BfTree key to stop scanning at (inclusive).</param>
        /// <param name="returnField">Which fields to include in the response (Key, Value, or Both).</param>
        /// <param name="output">The output to write the RESP-formatted response into.</param>
        /// <param name="recordCount">On return, the number of records written.</param>
        /// <param name="result">The operation result code.</param>
        /// <returns><see cref="GarnetStatus.WRONGTYPE"/> if the key is not an RI key; <see cref="GarnetStatus.OK"/> otherwise.</returns>
        public GarnetStatus RangeIndexRange(
            PinnedSpanByte key, PinnedSpanByte startKey, PinnedSpanByte endKey,
            ScanReturnField returnField, ref StringOutput output,
            out int recordCount, out RangeIndexResult result)
        {
            recordCount = 0;

            parseState.InitializeWithArgument(key);
            var input = new StringInput(RespCommand.RIRANGE, ref parseState);
            Span<byte> stubSpan = stackalloc byte[RangeIndexManager.IndexSizeBytes];

            using (functionsState.rangeIndexManager.ReadRangeIndex(this, key, ref input, stubSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    result = RangeIndexResult.Error;
                    return status == GarnetStatus.WRONGTYPE ? GarnetStatus.WRONGTYPE : GarnetStatus.OK;
                }

                var treePtr = ExtractTreePtr(stubSpan);
                if (treePtr == 0)
                {
                    result = RangeIndexResult.Error;
                    return GarnetStatus.OK;
                }

                if (RangeIndexManager.ReadIndex(stubSpan).StorageBackend == (byte)StorageBackendType.Memory)
                {
                    result = RangeIndexResult.MemoryModeNotSupported;
                    return GarnetStatus.OK;
                }

                WriteScanToOutput(treePtr, startKey.ReadOnlySpan, 0, returnField,
                    isScanWithCount: false, endKey.ReadOnlySpan,
                    ref output, out recordCount);
                result = RangeIndexResult.OK;
                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// RI.EXISTS — check whether a key exists and is a RangeIndex.
        /// </summary>
        /// <remarks>
        /// Uses <see cref="Read_MainStore"/> with the <see cref="RespCommand.RIEXISTS"/> tag.
        /// Because RIEXISTS is in <see cref="RespCommandExtensions.IsRangeIndexCommand"/>,
        /// the MainStore Reader returns <see cref="GarnetStatus.WRONGTYPE"/> for non-RI keys,
        /// which this method maps to <c>exists = false</c> (no error to the client).
        /// Similarly, <see cref="GarnetStatus.NOTFOUND"/> maps to <c>exists = false</c>.
        /// </remarks>
        /// <param name="key">The Garnet key to check.</param>
        /// <param name="exists">On return, <c>true</c> if the key exists and is a RangeIndex.</param>
        /// <returns>Always returns <see cref="GarnetStatus.OK"/>.</returns>
        public GarnetStatus RangeIndexExists(PinnedSpanByte key, out bool exists)
        {
            exists = false;

            parseState.InitializeWithArgument(key);
            var input = new StringInput(RespCommand.RIEXISTS, ref parseState);
            Span<byte> stubSpan = stackalloc byte[RangeIndexManager.IndexSizeBytes];
            var output = StringOutput.FromPinnedSpan(stubSpan);

            var status = Read_RangeIndex(key.ReadOnlySpan, ref input, ref output, ref stringBasicContext);

            // OK means the key exists and IS a RangeIndex (WRONGTYPE/NOTFOUND for anything else)
            exists = status == GarnetStatus.OK;
            return GarnetStatus.OK;
        }

        /// <summary>
        /// RI.CONFIG — return the configuration stored in the RangeIndex stub.
        /// Reads the stub under a shared lock (same pattern as other RI read commands)
        /// and extracts all configuration fields.
        /// </summary>
        /// <param name="key">The Garnet key of the RangeIndex.</param>
        /// <param name="storageBackend">On return, the storage backend type (0=Disk, 1=Memory).</param>
        /// <param name="cacheSize">On return, the circular buffer size in bytes.</param>
        /// <param name="minRecordSize">On return, the minimum record size.</param>
        /// <param name="maxRecordSize">On return, the maximum record size.</param>
        /// <param name="maxKeyLen">On return, the maximum key length.</param>
        /// <param name="leafPageSize">On return, the leaf page size.</param>
        /// <param name="result">The operation result code.</param>
        /// <returns><see cref="GarnetStatus.WRONGTYPE"/> if the key is not an RI key; <see cref="GarnetStatus.OK"/> otherwise.</returns>
        public GarnetStatus RangeIndexConfig(PinnedSpanByte key,
            out byte storageBackend, out ulong cacheSize, out uint minRecordSize,
            out uint maxRecordSize, out uint maxKeyLen, out uint leafPageSize,
            out RangeIndexResult result)
        {
            storageBackend = 0;
            cacheSize = 0;
            minRecordSize = 0;
            maxRecordSize = 0;
            maxKeyLen = 0;
            leafPageSize = 0;

            parseState.InitializeWithArgument(key);
            var input = new StringInput(RespCommand.RICONFIG, ref parseState);
            Span<byte> stubSpan = stackalloc byte[RangeIndexManager.IndexSizeBytes];

            using (functionsState.rangeIndexManager.ReadRangeIndex(this, key, ref input, stubSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    result = RangeIndexResult.Error;
                    return status == GarnetStatus.WRONGTYPE ? GarnetStatus.WRONGTYPE : GarnetStatus.OK;
                }

                if (stubSpan.Length != RangeIndexManager.IndexSizeBytes)
                {
                    result = RangeIndexResult.Error;
                    return GarnetStatus.OK;
                }

                ref readonly var stub = ref RangeIndexManager.ReadIndex(stubSpan);
                storageBackend = stub.StorageBackend;
                cacheSize = stub.CacheSize;
                minRecordSize = stub.MinRecordSize;
                maxRecordSize = stub.MaxRecordSize;
                maxKeyLen = stub.MaxKeyLen;
                leafPageSize = stub.LeafPageSize;
                result = RangeIndexResult.OK;
                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// RI.METRICS — return runtime metrics from the stub and live index registry.
        /// Reports the tree handle, live/flushed/recovered status for diagnostic purposes.
        /// </summary>
        /// <param name="key">The Garnet key of the RangeIndex.</param>
        /// <param name="treeHandle">On return, the native tree pointer (0 if not live).</param>
        /// <param name="isLive">On return, <c>true</c> if the tree is registered in the live index dictionary.</param>
        /// <param name="isFlushed">On return, <c>true</c> if the stub has the Flushed flag set.</param>
        /// <param name="isRecovered">On return, <c>true</c> if the stub has the Recovered flag set.</param>
        /// <param name="result">The operation result code.</param>
        /// <returns><see cref="GarnetStatus.WRONGTYPE"/> if the key is not an RI key; <see cref="GarnetStatus.OK"/> otherwise.</returns>
        public GarnetStatus RangeIndexMetrics(PinnedSpanByte key,
            out nint treeHandle, out bool isLive, out bool isFlushed, out bool isRecovered,
            out RangeIndexResult result)
        {
            treeHandle = nint.Zero;
            isLive = false;
            isFlushed = false;
            isRecovered = false;

            parseState.InitializeWithArgument(key);
            var input = new StringInput(RespCommand.RIMETRICS, ref parseState);
            Span<byte> stubSpan = stackalloc byte[RangeIndexManager.IndexSizeBytes];

            using (functionsState.rangeIndexManager.ReadRangeIndex(this, key, ref input, stubSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    result = RangeIndexResult.Error;
                    return status == GarnetStatus.WRONGTYPE ? GarnetStatus.WRONGTYPE : GarnetStatus.OK;
                }

                if (stubSpan.Length != RangeIndexManager.IndexSizeBytes)
                {
                    result = RangeIndexResult.Error;
                    return GarnetStatus.OK;
                }

                ref readonly var stub = ref RangeIndexManager.ReadIndex(stubSpan);
                treeHandle = stub.TreeHandle;
                isLive = stub.TreeHandle != nint.Zero;
                isFlushed = stub.IsFlushed;
                isRecovered = stub.IsRecovered;
                result = RangeIndexResult.OK;
                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// Scan BfTree entries and write the complete RESP array response directly into <see cref="StringOutput"/>.
        /// </summary>
        /// <remarks>
        /// <para>Uses an optimistic header reservation strategy:</para>
        /// <list type="number">
        /// <item>Reserves 5 bytes for the array header (<c>*NN\r\n</c>, up to 99 results).</item>
        /// <item>Writes record bodies sequentially after the reservation.</item>
        /// <item>On buffer overflow, grows the buffer by renting from <c>memoryPool</c> and
        /// copying existing data (scan is not restarted).</item>
        /// <item>After scan completes, backfills the actual array count header. If the actual
        /// header differs in size from the reservation, shifts data accordingly.</item>
        /// </list>
        ///
        /// <para>Buffer transitions: Starts in the inline (network) buffer. On overflow,
        /// transitions to heap-allocated buffers from <c>memoryPool</c>. The final buffer
        /// is either kept inline or transferred to <c>output.SpanByteAndMemory.Memory</c>.</para>
        /// </remarks>
        /// <param name="treePtr">Native tree pointer for the BfTree.</param>
        /// <param name="startKey">Key to start scanning from (inclusive).</param>
        /// <param name="count">Max records for scan-with-count; 0 for range scan.</param>
        /// <param name="returnField">Which fields to include per record.</param>
        /// <param name="isScanWithCount"><c>true</c> for RI.SCAN; <c>false</c> for RI.RANGE.</param>
        /// <param name="endKey">End key for range scan (inclusive); ignored for scan-with-count.</param>
        /// <param name="output">The StringOutput to write the RESP response into.</param>
        /// <param name="recordCount">On return, the number of records written.</param>
        private void WriteScanToOutput(
            nint treePtr, ReadOnlySpan<byte> startKey, int count,
            ScanReturnField returnField, bool isScanWithCount,
            ReadOnlySpan<byte> endKey, ref StringOutput output, out int recordCount)
        {
            // Optimistically reserve for 2-digit count (*NN\r\n = 5 bytes).
            // Most scans return < 100 results, so the shift is 0 bytes in the common case.
            const int ReservedHeaderSize = 5; // *NN\r\n

            int recCount = 0;
            var rf = returnField;

            // State for the grow-in-place callback
            byte* curr;
            byte* bufEnd;
            byte* bufStart;
            bool isHeap = false;
            IMemoryOwner<byte> heapMemory = null;
            MemoryHandle heapHandle = default;
            var memoryPool = functionsState.memoryPool;

            if (output.SpanByteAndMemory.IsSpanByte)
            {
                bufStart = output.SpanByteAndMemory.SpanByte.ToPointer();
                bufEnd = bufStart + output.SpanByteAndMemory.Length;
                curr = bufStart + ReservedHeaderSize;
            }
            else
            {
                // Already on heap (shouldn't normally happen, but handle it)
                var heapSize = 64 * 1024;
                heapMemory = memoryPool.Rent(heapSize);
                heapHandle = heapMemory.Memory.Pin();
                bufStart = (byte*)heapHandle.Pointer;
                bufEnd = bufStart + heapSize;
                curr = bufStart + ReservedHeaderSize;
                isHeap = true;
            }

            ScanRecordAction callback = (k, v) =>
            {
                recCount++;

                if (TryWriteRecordResp(rf, k, v, ref curr, bufEnd))
                    return true;

                // Doesn't fit — grow the buffer and retry this record
                var writtenBytes = (int)(curr - bufStart);
                var newSize = Math.Max((int)(bufEnd - bufStart) * 2, writtenBytes + k.Length + v.Length + 256);

                var newMemory = memoryPool.Rent(newSize);
                var newHandle = newMemory.Memory.Pin();
                var newStart = (byte*)newHandle.Pointer;

                // Copy partial data written so far
                new Span<byte>(bufStart, writtenBytes).CopyTo(new Span<byte>(newStart, writtenBytes));

                // Release old heap buffer if we were on heap
                if (isHeap)
                {
                    heapHandle.Dispose();
                    heapMemory?.Dispose();
                }

                heapMemory = newMemory;
                heapHandle = newHandle;
                bufStart = newStart;
                bufEnd = newStart + newSize;
                curr = newStart + writtenBytes;
                isHeap = true;

                // Retry the record write — guaranteed to succeed now
                TryWriteRecordResp(rf, k, v, ref curr, bufEnd);
                return true;
            };

            try
            {
                if (isScanWithCount)
                    BfTreeService.ScanWithCountByPtrCallback(treePtr, startKey, count, returnField, callback);
                else
                    BfTreeService.ScanWithEndKeyByPtrCallback(treePtr, startKey, endKey, returnField, callback);

                // Backfill the array header
                var actualHeaderSize = 1 + NumUtils.CountDigits(recCount) + 2;
                var extraNeeded = actualHeaderSize - ReservedHeaderSize;

                if (!isHeap && extraNeeded > 0 && curr + extraNeeded > bufEnd)
                {
                    // Need more header space than reserved but inline buffer is full — move to heap
                    var writtenBytes = (int)(curr - bufStart);
                    var newSize = writtenBytes + extraNeeded + 64;
                    var newMemory = memoryPool.Rent(newSize);
                    var newHandle = newMemory.Memory.Pin();
                    var newStart = (byte*)newHandle.Pointer;
                    new Span<byte>(bufStart, writtenBytes).CopyTo(new Span<byte>(newStart, writtenBytes));

                    heapMemory = newMemory;
                    heapHandle = newHandle;
                    bufStart = newStart;
                    curr = newStart + writtenBytes;
                    isHeap = true;
                }

                if (!isHeap)
                {
                    BackfillArrayHeader(bufStart, curr, ReservedHeaderSize, recCount, ref output, out recordCount);
                }
                else
                {
                    output.SpanByteAndMemory.ConvertToHeap();
                    output.SpanByteAndMemory.Memory = heapMemory;
                    BackfillArrayHeader(bufStart, curr, ReservedHeaderSize, recCount, ref output, out recordCount);
                    heapHandle.Dispose();
                    heapMemory = null;
                }
            }
            finally
            {
                // Only dispose if we still own it (not transferred to output)
                if (isHeap && heapMemory != null)
                {
                    heapHandle.Dispose();
                    heapMemory.Dispose();
                }
            }
        }

        /// <summary>
        /// Try to write a single scan record as RESP directly into a buffer via pointer arithmetic.
        /// </summary>
        /// <param name="returnField">Which fields to write (Key, Value, or KeyAndValue).</param>
        /// <param name="keySpan">The key bytes from the scan iterator.</param>
        /// <param name="valueSpan">The value bytes from the scan iterator.</param>
        /// <param name="curr">Current write position in the buffer; advanced on success.</param>
        /// <param name="end">End-of-buffer pointer.</param>
        /// <returns><c>false</c> if there isn't enough space; <c>true</c> on success.</returns>
        private static bool TryWriteRecordResp(ScanReturnField returnField,
            ReadOnlySpan<byte> keySpan, ReadOnlySpan<byte> valueSpan,
            ref byte* curr, byte* end)
        {
            if (returnField == ScanReturnField.KeyAndValue)
            {
                if (!RespWriteUtils.TryWriteArrayLength(2, ref curr, end))
                    return false;
                if (!RespWriteUtils.TryWriteBulkString(keySpan, ref curr, end))
                    return false;
                if (!RespWriteUtils.TryWriteBulkString(valueSpan, ref curr, end))
                    return false;
            }
            else if (returnField == ScanReturnField.Key)
            {
                if (!RespWriteUtils.TryWriteBulkString(keySpan, ref curr, end))
                    return false;
            }
            else
            {
                if (!RespWriteUtils.TryWriteBulkString(valueSpan, ref curr, end))
                    return false;
            }
            return true;
        }

        /// <summary>
        /// Backfill the RESP array header (<c>*count\r\n</c>) into the reserved space before the record bodies.
        /// </summary>
        /// <remarks>
        /// Handles three cases:
        /// <list type="bullet">
        /// <item><b>headerGap &gt; 0</b>: Actual header is smaller than reserved space (common for ≤ 99 results).
        /// Writes header at offset, then shifts all data left to eliminate the gap.</item>
        /// <item><b>headerGap == 0</b>: Exact fit — writes header at the start, no shift needed.</item>
        /// <item><b>headerGap &lt; 0</b>: Actual header is larger than reserved (e.g., &gt; 99 results with
        /// 2-digit reservation). Shifts records right to make room, then writes header at start.</item>
        /// </list>
        /// </remarks>
        /// <param name="bufStart">Start of the output buffer.</param>
        /// <param name="curr">Current write position (past the last record body byte).</param>
        /// <param name="reservedHeaderSize">Number of bytes originally reserved for the header.</param>
        /// <param name="recCount">Actual number of records written.</param>
        /// <param name="output">The StringOutput to set the final length on.</param>
        /// <param name="recordCount">On return, set to <paramref name="recCount"/>.</param>
        private static void BackfillArrayHeader(byte* bufStart, byte* curr, int reservedHeaderSize, int recCount,
            ref StringOutput output, out int recordCount)
        {
            var countDigits = NumUtils.CountDigits(recCount);
            var actualHeaderSize = 1 + countDigits + 2; // *N\r\n
            var headerGap = reservedHeaderSize - actualHeaderSize;
            var recordBytes = (int)(curr - (bufStart + reservedHeaderSize));

            if (headerGap >= 0)
            {
                // Actual header fits in reserved space (or exact fit). Write header, shift left if needed.
                var headerStart = bufStart + headerGap;
                var tmp = headerStart;
                *tmp++ = (byte)'*';
                NumUtils.WriteInt32(recCount, countDigits, ref tmp);
                *tmp++ = (byte)'\r';
                *tmp++ = (byte)'\n';

                var totalLen = actualHeaderSize + recordBytes;
                if (headerGap > 0)
                    Buffer.MemoryCopy(headerStart, bufStart, totalLen, totalLen);

                output.SpanByteAndMemory.Length = totalLen;
            }
            else
            {
                // Actual header is larger than reserved (e.g., >99 results with 2-digit reservation).
                // Shift records right to make room, then write header at the start.
                var extraNeeded = -headerGap;
                var totalLen = actualHeaderSize + recordBytes;

                // Shift records right by extraNeeded bytes (overlapping memmove)
                var recordSrc = bufStart + reservedHeaderSize;
                var recordDst = bufStart + actualHeaderSize;
                Buffer.MemoryCopy(recordSrc, recordDst, recordBytes, recordBytes);

                // Write header at the start
                var tmp = bufStart;
                *tmp++ = (byte)'*';
                NumUtils.WriteInt32(recCount, countDigits, ref tmp);
                *tmp++ = (byte)'\r';
                *tmp++ = (byte)'\n';

                output.SpanByteAndMemory.Length = totalLen;
            }

            recordCount = recCount;
        }

        /// <summary>
        /// Extract the native tree pointer from a stub span.
        /// Returns <see cref="nint.Zero"/> if the span is too small to contain a valid stub.
        /// </summary>
        /// <param name="stubSpan">The span containing the serialized stub bytes.</param>
        /// <returns>The native tree pointer, or 0 if the stub is invalid or too small.</returns>
        private static nint ExtractTreePtr(Span<byte> stubSpan)
        {
            if (stubSpan.Length != RangeIndexManager.IndexSizeBytes)
                return 0;

            return RangeIndexManager.ReadIndex(stubSpan).TreeHandle;
        }
    }
}