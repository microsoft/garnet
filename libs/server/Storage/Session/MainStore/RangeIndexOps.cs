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

            // Auto-compute leaf page size if not specified
            if (leafPageSize == 0)
                leafPageSize = RangeIndexManager.ComputeLeafPageSize(maxRecordSize);

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

            functionsState.rangeIndexManager.AcquireShared();
            try
            {
                var status = functionsState.rangeIndexManager.ReadRangeIndex(this, key, ref input, stubSpan);
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
            finally
            {
                functionsState.rangeIndexManager.ReleaseShared();
            }
        }

        /// <summary>
        /// RI.GET — read a field from a range index.
        /// Hot path: acquires shared lock, reads stub, reads from BfTree,
        /// and writes the value as a RESP bulk string directly into <paramref name="output"/>.
        /// </summary>
        public GarnetStatus RangeIndexGet(
            PinnedSpanByte key, PinnedSpanByte field,
            ref StringOutput output, out RangeIndexResult result)
        {
            parseState.InitializeWithArgument(key);
            var input = new StringInput(RespCommand.RIGET, ref parseState);
            Span<byte> stubSpan = stackalloc byte[RangeIndexManager.IndexSizeBytes];

            functionsState.rangeIndexManager.AcquireShared();
            try
            {
                var status = functionsState.rangeIndexManager.ReadRangeIndex(this, key, ref input, stubSpan);
                if (status != GarnetStatus.OK)
                {
                    result = RangeIndexResult.Error;
                    return GarnetStatus.OK;
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

                    if (readResult != BfTreeReadResult.Found || bytesWritten <= 0)
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

                        if (readResult != BfTreeReadResult.Found || bytesWritten <= 0)
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
            finally
            {
                functionsState.rangeIndexManager.ReleaseShared();
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

            functionsState.rangeIndexManager.AcquireShared();
            try
            {
                var status = functionsState.rangeIndexManager.ReadRangeIndex(this, key, ref input, stubSpan);
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
            finally
            {
                functionsState.rangeIndexManager.ReleaseShared();
            }
        }

        /// <summary>
        /// RI.SCAN — scan entries starting at a key with a count limit.
        /// Acquires shared lock, reads stub, writes RESP response into output while lock is held.
        /// </summary>
        public GarnetStatus RangeIndexScan(
            PinnedSpanByte key, PinnedSpanByte startKey, int count,
            ScanReturnField returnField, ref StringOutput output,
            out int recordCount, out RangeIndexResult result)
        {
            recordCount = 0;

            parseState.InitializeWithArgument(key);
            var input = new StringInput(RespCommand.RISCAN, ref parseState);
            Span<byte> stubSpan = stackalloc byte[RangeIndexManager.IndexSizeBytes];

            functionsState.rangeIndexManager.AcquireShared();
            try
            {
                var status = functionsState.rangeIndexManager.ReadRangeIndex(this, key, ref input, stubSpan);
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

                WriteScanToOutput(treePtr, startKey.ReadOnlySpan, count, returnField,
                    isScanWithCount: true, [],
                    ref output, out recordCount);
                result = RangeIndexResult.OK;
                return GarnetStatus.OK;
            }
            finally
            {
                functionsState.rangeIndexManager.ReleaseShared();
            }
        }

        /// <summary>
        /// RI.RANGE — scan entries in [start, end] range from a range index.
        /// Acquires shared lock, reads stub, writes RESP response into output while lock is held.
        /// </summary>
        public GarnetStatus RangeIndexRange(
            PinnedSpanByte key, PinnedSpanByte startKey, PinnedSpanByte endKey,
            ScanReturnField returnField, ref StringOutput output,
            out int recordCount, out RangeIndexResult result)
        {
            recordCount = 0;

            parseState.InitializeWithArgument(key);
            var input = new StringInput(RespCommand.RIRANGE, ref parseState);
            Span<byte> stubSpan = stackalloc byte[RangeIndexManager.IndexSizeBytes];

            functionsState.rangeIndexManager.AcquireShared();
            try
            {
                var status = functionsState.rangeIndexManager.ReadRangeIndex(this, key, ref input, stubSpan);
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

                WriteScanToOutput(treePtr, startKey.ReadOnlySpan, 0, returnField,
                    isScanWithCount: false, endKey.ReadOnlySpan,
                    ref output, out recordCount);
                result = RangeIndexResult.OK;
                return GarnetStatus.OK;
            }
            finally
            {
                functionsState.rangeIndexManager.ReleaseShared();
            }
        }

        /// <summary>
        /// Scan BfTree and write the complete RESP array response directly into StringOutput.
        /// Optimistically reserves 5 bytes for the array header (*NN\r\n, up to 99 results).
        /// If the actual count exceeds the reservation, shifts data to accommodate.
        /// On overflow from inline (network buffer), transitions to heap without restarting the scan.
        /// </summary>
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
        /// Returns false if there isn't enough space.
        /// </summary>
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
        /// Backfill the RESP array header (*count\r\n) into the reserved space before the record bodies.
        /// Handles three cases:
        /// - headerGap > 0: actual header smaller than reserved → shift left (common for ≤99 results)
        /// - headerGap == 0: exact fit → no shift needed
        /// - headerGap &lt; 0: actual header larger than reserved → shift records right
        /// </summary>
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
        /// </summary>
        private static nint ExtractTreePtr(Span<byte> stubSpan)
        {
            if (stubSpan.Length < RangeIndexManager.IndexSizeBytes)
                return 0;

            return RangeIndexManager.ReadIndex(stubSpan).TreeHandle;
        }
    }
}