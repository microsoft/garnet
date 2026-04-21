// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Tsavorite.core;

namespace Garnet.server.BfTreeInterop
{
    /// <summary>
    /// Result codes for BfTree read operations.
    /// </summary>
    public enum BfTreeReadResult
    {
        /// <summary>Value was found.</summary>
        Found = 0,
        /// <summary>Key was not found.</summary>
        NotFound = -1,
        /// <summary>Key was found but has been deleted.</summary>
        Deleted = -2,
        /// <summary>The key is invalid (e.g. too long).</summary>
        InvalidKey = -3,
    }

    /// <summary>
    /// Result codes for BfTree insert operations.
    /// </summary>
    public enum BfTreeInsertResult
    {
        /// <summary>Insert succeeded.</summary>
        Success = 0,
        /// <summary>Key or value is invalid (e.g. exceeds configured limits).</summary>
        InvalidKV = 1,
    }

    /// <summary>
    /// Specifies which fields a scan operation should return.
    /// </summary>
    public enum ScanReturnField : byte
    {
        /// <summary>Return only keys.</summary>
        Key = 0,
        /// <summary>Return only values.</summary>
        Value = 1,
        /// <summary>Return both keys and values.</summary>
        KeyAndValue = 2,
    }

    /// <summary>
    /// Storage backend for the BfTree.
    /// </summary>
    public enum StorageBackendType : byte
    {
        /// <summary>
        /// Disk-backed tree (default). Base pages are stored in a data file on disk.
        /// The circular buffer acts as a hot-data cache. No data loss on
        /// eviction. Total capacity is limited by disk space.
        /// </summary>
        Disk = 0,

        /// <summary>
        /// Memory-only tree (maps to bf-tree's cache_only mode). All data lives
        /// in a bounded in-memory circular buffer. Snapshot and recovery will be
        /// supported in a future bf-tree release; currently throws at the FFI
        /// boundary.
        /// </summary>
        Memory = 1,
    }

    /// <summary>
    /// Callback for zero-allocation scan. Receives key and value as spans into the scan buffer.
    /// </summary>
    /// <param name="key">Key bytes (empty if ScanReturnField.Value).</param>
    /// <param name="value">Value bytes (empty if ScanReturnField.Key).</param>
    /// <returns>True to continue scanning, false to stop early.</returns>
    public delegate bool ScanRecordAction(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);

    /// <summary>
    /// A single record returned by a scan operation.
    /// </summary>
    public readonly struct ScanRecord
    {
        /// <summary>The key bytes (empty if ScanReturnField.Value was used).</summary>
        public ReadOnlyMemory<byte> Key { get; init; }

        /// <summary>The value bytes (empty if ScanReturnField.Key was used).</summary>
        public ReadOnlyMemory<byte> Value { get; init; }
    }

    /// <summary>
    /// High-level managed wrapper for the native bftree-garnet library.
    /// Provides safe C# access to BfTree lifecycle, point operations, scans,
    /// and snapshot/recovery.
    /// </summary>
    public sealed unsafe class BfTreeService : IDisposable
    {
        private nint _tree;
        private int _disposed;
        private readonly StorageBackendType _storageBackend;
        private readonly string _filePath;

        /// <summary>
        /// Gets the native tree pointer for storage in stubs and direct P/Invoke.
        /// </summary>
        public nint NativePtr => _tree;

        /// <summary>
        /// Gets the data file path for disk-backed trees, or null for memory-only trees.
        /// </summary>
        public string FilePath => _filePath;

        /// <summary>
        /// Gets the storage backend type (Disk or Memory).
        /// </summary>
        public StorageBackendType StorageBackend => _storageBackend;

        /// <summary>
        /// Creates a new BfTree with the given configuration.
        /// Pass 0 for any numeric parameter to use the bf-tree default.
        /// </summary>
        /// <param name="storageBackend">Disk (default, file-backed) or Memory (bounded in-memory).</param>
        /// <param name="filePath">Data file path for disk-backed trees. Ignored for memory-only.</param>
        /// <param name="cbSizeByte">Circular buffer size in bytes (hot-data cache for Disk; total capacity for Memory).</param>
        /// <param name="cbMinRecordSize">Minimum record size.</param>
        /// <param name="cbMaxRecordSize">Maximum record size.</param>
        /// <param name="cbMaxKeyLen">Maximum key length.</param>
        /// <param name="leafPageSize">Leaf page size.</param>
        public BfTreeService(
            StorageBackendType storageBackend = StorageBackendType.Disk,
            string filePath = null,
            ulong cbSizeByte = 0,
            uint cbMinRecordSize = 0,
            uint cbMaxRecordSize = 0,
            uint cbMaxKeyLen = 0,
            uint leafPageSize = 0)
        {
            _storageBackend = storageBackend;
            _filePath = filePath;
            if (storageBackend == StorageBackendType.Disk && string.IsNullOrEmpty(filePath))
                throw new ArgumentException("filePath is required for disk-backed trees.", nameof(filePath));
            byte[] pathBytes = filePath != null ? Encoding.UTF8.GetBytes(filePath) : null;
            fixed (byte* pp = pathBytes)
            {
                _tree = NativeBfTreeMethods.bftree_create(
                    cbSizeByte, cbMinRecordSize, cbMaxRecordSize, cbMaxKeyLen, leafPageSize,
                    (byte)storageBackend, pp, pathBytes?.Length ?? 0);
            }
            if (_tree == 0)
                throw new InvalidOperationException("Failed to create BfTree instance.");
        }

        /// <summary>
        /// Creates a BfTreeService wrapping an existing native tree pointer (e.g. from snapshot restore).
        /// Takes ownership of the pointer.
        /// </summary>
        internal BfTreeService(nint treePtr, StorageBackendType storageBackend, string filePath = null)
        {
            if (treePtr == 0)
                throw new ArgumentException("Tree pointer must not be null.", nameof(treePtr));
            _tree = treePtr;
            _storageBackend = storageBackend;
            _filePath = filePath;
        }

        // ---------------------------------------------------------------
        // Point operations — PinnedSpanByte (zero-overhead for Garnet hot paths)
        // ---------------------------------------------------------------

        /// <summary>
        /// Insert a key-value pair. Zero-overhead: passes pinned pointers directly to native code.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BfTreeInsertResult Insert(PinnedSpanByte key, PinnedSpanByte value)
        {
            return (BfTreeInsertResult)NativeBfTreeMethods.bftree_insert(
                _tree, key.ToPointer(), key.Length, value.ToPointer(), value.Length);
        }

        /// <summary>
        /// Read the value for a key into a pinned output buffer. Zero-overhead.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BfTreeReadResult Read(PinnedSpanByte key, byte* outputBuffer, int outputBufferLen, out int bytesWritten)
        {
            int valueLen = 0;
            var result = NativeBfTreeMethods.bftree_read(
                _tree, key.ToPointer(), key.Length, outputBuffer, outputBufferLen, &valueLen);
            bytesWritten = valueLen;
            return (BfTreeReadResult)result;
        }

        /// <summary>
        /// Delete a key. Zero-overhead.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Delete(PinnedSpanByte key)
        {
            NativeBfTreeMethods.bftree_delete(_tree, key.ToPointer(), key.Length);
        }

        /// <summary>
        /// No-op P/Invoke for measuring pure FFI transition overhead.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Noop(PinnedSpanByte key)
        {
            return NativeBfTreeMethods.bftree_noop(_tree, key.ToPointer(), key.Length);
        }

        // ---------------------------------------------------------------
        // Static pointer-based operations (for hot paths using native ptr from stub)
        // ---------------------------------------------------------------

        /// <summary>
        /// Insert via native pointer. For hot-path use when the caller has the native ptr from the stub.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BfTreeInsertResult InsertByPtr(nint treePtr, PinnedSpanByte key, PinnedSpanByte value)
        {
            return (BfTreeInsertResult)NativeBfTreeMethods.bftree_insert(
                treePtr, key.ToPointer(), key.Length, value.ToPointer(), value.Length);
        }

        /// <summary>
        /// Read via native pointer. Convenience overload that allocates output.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BfTreeReadResult ReadByPtr(nint treePtr, PinnedSpanByte key, out byte[] value)
        {
            value = [];
            Span<byte> buffer = stackalloc byte[4096];
            int bytesWritten;
            fixed (byte* bp = buffer)
            {
                int valueLen = 0;
                var rc = NativeBfTreeMethods.bftree_read(
                    treePtr, key.ToPointer(), key.Length, bp, buffer.Length, &valueLen);
                bytesWritten = valueLen;
                if (rc == (int)BfTreeReadResult.Found && bytesWritten > 0)
                {
                    value = buffer[..bytesWritten].ToArray();
                    return BfTreeReadResult.Found;
                }
                return (BfTreeReadResult)rc;
            }
        }

        /// <summary>
        /// Read via native pointer into a caller-provided buffer without allocating.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BfTreeReadResult ReadByPtrInto(nint treePtr, PinnedSpanByte key, byte* outputBuffer, int outputBufferLen, out int bytesWritten)
        {
            int valueLen = 0;
            var rc = NativeBfTreeMethods.bftree_read(
                treePtr, key.ToPointer(), key.Length, outputBuffer, outputBufferLen, &valueLen);
            bytesWritten = valueLen;
            return (BfTreeReadResult)rc;
        }

        /// <summary>
        /// Delete via native pointer.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void DeleteByPtr(nint treePtr, PinnedSpanByte key)
        {
            NativeBfTreeMethods.bftree_delete(treePtr, key.ToPointer(), key.Length);
        }

        /// <summary>
        /// Scan with count via native pointer using a zero-allocation callback.
        /// </summary>
        /// <returns>Number of records passed to the callback.</returns>
        public static int ScanWithCountByPtrCallback(nint treePtr, ReadOnlySpan<byte> startKey, int count, ScanReturnField returnField, ScanRecordAction onRecord)
        {
            nint handle;
            fixed (byte* skp = startKey)
            {
                handle = NativeBfTreeMethods.bftree_scan_with_count(
                    treePtr, skp, startKey.Length, count, (byte)returnField);
            }
            try
            {
                Span<byte> buffer = stackalloc byte[8192];
                return DrainScanIteratorWithCallback(handle, buffer, returnField, onRecord);
            }
            finally
            {
                NativeBfTreeMethods.bftree_scan_drop(handle);
            }
        }

        /// <summary>
        /// Scan with end key via native pointer using a zero-allocation callback.
        /// </summary>
        /// <returns>Number of records passed to the callback.</returns>
        public static int ScanWithEndKeyByPtrCallback(nint treePtr, ReadOnlySpan<byte> startKey, ReadOnlySpan<byte> endKey, ScanReturnField returnField, ScanRecordAction onRecord)
        {
            nint handle;
            fixed (byte* skp = startKey, ekp = endKey)
            {
                handle = NativeBfTreeMethods.bftree_scan_with_end_key(
                    treePtr, skp, startKey.Length, ekp, endKey.Length, (byte)returnField);
            }
            try
            {
                Span<byte> buffer = stackalloc byte[8192];
                return DrainScanIteratorWithCallback(handle, buffer, returnField, onRecord);
            }
            finally
            {
                NativeBfTreeMethods.bftree_scan_drop(handle);
            }
        }

        // ---------------------------------------------------------------
        // Point operations — span-based (safe wrappers: fixed → PinnedSpanByte → native)
        // ---------------------------------------------------------------

        /// <summary>
        /// Insert a key-value pair into the BfTree.
        /// </summary>
        public BfTreeInsertResult Insert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            fixed (byte* kp = key, vp = value)
                return Insert(
                    PinnedSpanByte.FromPinnedPointer(kp, key.Length),
                    PinnedSpanByte.FromPinnedPointer(vp, value.Length));
        }

        /// <summary>
        /// Read the value for a key into a caller-provided buffer.
        /// </summary>
        public BfTreeReadResult Read(ReadOnlySpan<byte> key, Span<byte> outputBuffer, out int bytesWritten)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            fixed (byte* kp = key, bp = outputBuffer)
                return Read(
                    PinnedSpanByte.FromPinnedPointer(kp, key.Length),
                    bp, outputBuffer.Length, out bytesWritten);
        }

        /// <summary>
        /// Read the value for a key. Convenience overload that allocates a byte array.
        /// For hot paths, prefer the PinnedSpanByte or span overloads.
        /// </summary>
        public BfTreeReadResult Read(ReadOnlySpan<byte> key, out byte[] value)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            value = [];
            Span<byte> buffer = stackalloc byte[4096];
            var result = Read(key, buffer, out int bytesWritten);
            if (result == BfTreeReadResult.Found && bytesWritten > 0)
                value = buffer[..bytesWritten].ToArray();
            return result;
        }

        /// <summary>
        /// Delete a key from the BfTree.
        /// </summary>
        public void Delete(ReadOnlySpan<byte> key)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            fixed (byte* kp = key)
                Delete(PinnedSpanByte.FromPinnedPointer(kp, key.Length));
        }

        /// <summary>
        /// Scan entries starting from <paramref name="startKey"/>, returning up to
        /// <paramref name="count"/> records. Invokes <paramref name="onRecord"/> for each
        /// record without allocating per-record. Zero-allocation on the hot path.
        /// </summary>
        /// <param name="startKey">Key to start scanning from (inclusive).</param>
        /// <param name="count">Maximum number of records to return.</param>
        /// <param name="scanBuffer">Caller-provided buffer for scan output (must be large enough for max key+value).</param>
        /// <param name="onRecord">Callback invoked for each record with key and value spans into <paramref name="scanBuffer"/>.</param>
        /// <param name="returnField">Which fields to return.</param>
        /// <returns>Number of records scanned.</returns>
        public int ScanWithCount(
            ReadOnlySpan<byte> startKey, int count,
            Span<byte> scanBuffer,
            ScanRecordAction onRecord,
            ScanReturnField returnField = ScanReturnField.KeyAndValue)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            nint handle;
            fixed (byte* skp = startKey)
            {
                handle = NativeBfTreeMethods.bftree_scan_with_count(
                    _tree, skp, startKey.Length, count, (byte)returnField);
            }
            try
            {
                return DrainScanIteratorWithCallback(handle, scanBuffer, returnField, onRecord);
            }
            finally
            {
                NativeBfTreeMethods.bftree_scan_drop(handle);
            }
        }

        /// <summary>
        /// Scan entries starting from <paramref name="startKey"/>, returning up to
        /// <paramref name="count"/> records. Convenience overload that returns a list.
        /// For hot paths, prefer the callback-based overload to avoid per-record allocations.
        /// </summary>
        public List<ScanRecord> ScanWithCount(
            ReadOnlySpan<byte> startKey, int count,
            ScanReturnField returnField = ScanReturnField.KeyAndValue)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            nint handle;
            fixed (byte* skp = startKey)
            {
                handle = NativeBfTreeMethods.bftree_scan_with_count(
                    _tree, skp, startKey.Length, count, (byte)returnField);
            }
            try
            {
                return DrainScanIteratorToList(handle, returnField);
            }
            finally
            {
                NativeBfTreeMethods.bftree_scan_drop(handle);
            }
        }

        /// <summary>
        /// Scan entries in the closed range [<paramref name="startKey"/>, <paramref name="endKey"/>].
        /// </summary>
        public List<ScanRecord> ScanWithEndKey(
            ReadOnlySpan<byte> startKey, ReadOnlySpan<byte> endKey,
            ScanReturnField returnField = ScanReturnField.KeyAndValue)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            nint handle;
            fixed (byte* skp = startKey, ekp = endKey)
            {
                handle = NativeBfTreeMethods.bftree_scan_with_end_key(
                    _tree, skp, startKey.Length, ekp, endKey.Length, (byte)returnField);
            }
            try
            {
                return DrainScanIteratorToList(handle, returnField);
            }
            finally
            {
                NativeBfTreeMethods.bftree_scan_drop(handle);
            }
        }

        private static readonly byte[] ScanAllStartKey = [0];

        /// <summary>
        /// Scan all entries in the tree, ordered by key.
        /// Internally scans from the minimum key (\x00) with count = int.MaxValue.
        /// Only supported for disk-backed trees (memory-only trees do not support scan).
        /// </summary>
        public List<ScanRecord> ScanAll(
            ScanReturnField returnField = ScanReturnField.KeyAndValue)
        {
            return ScanWithCount(ScanAllStartKey, int.MaxValue, returnField);
        }

        /// <summary>
        /// Snapshot the BfTree to a file.
        /// For disk-backed trees: drains the circular buffer and writes the index
        /// structure to the tree's own data file. <paramref name="snapshotPath"/> is ignored.
        /// For memory-only trees: serializes the tree to the given path.
        /// Currently throws <see cref="NotSupportedException"/> until bf-tree adds
        /// cache_only snapshot support.
        /// </summary>
        /// <param name="snapshotPath">Target snapshot file path (used for memory-only trees; ignored for disk).</param>
        public void Snapshot(string snapshotPath = null)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            if (_storageBackend == StorageBackendType.Disk)
            {
                int result = NativeBfTreeMethods.bftree_snapshot(_tree);
                if (result != 0)
                    throw new InvalidOperationException("Failed to snapshot disk-backed BfTree.");
            }
            else
            {
                if (string.IsNullOrEmpty(snapshotPath))
                    throw new ArgumentException("snapshotPath is required for memory-only trees.", nameof(snapshotPath));
                var pathBytes = Encoding.UTF8.GetBytes(snapshotPath);
                int result;
                fixed (byte* pp = pathBytes)
                {
                    result = NativeBfTreeMethods.bftree_snapshot_memory(_tree, pp, pathBytes.Length);
                }
                if (result != 0)
                    throw new NotSupportedException(
                        "Snapshot is not yet supported for memory-only trees. Pending bf-tree cache_only snapshot support.");
            }
        }

        /// <summary>
        /// Snapshot the BfTree to a specified target file for flush persistence.
        /// For disk-backed trees: performs in-place snapshot, then copies the data file to <paramref name="targetPath"/>.
        /// For memory-only trees: serializes directly to <paramref name="targetPath"/>.
        /// </summary>
        /// <param name="targetPath">Target file path for the snapshot.</param>
        public void SnapshotToFile(string targetPath)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            if (string.IsNullOrEmpty(targetPath))
                throw new ArgumentException("targetPath is required.", nameof(targetPath));

            if (_storageBackend == StorageBackendType.Disk)
            {
                // Snapshot in-place first (drains circular buffer to data file)
                int result = NativeBfTreeMethods.bftree_snapshot(_tree);
                if (result != 0)
                    throw new InvalidOperationException("Failed to snapshot disk-backed BfTree before file copy.");

                // Copy the data file to the target path
                System.IO.File.Copy(_filePath, targetPath, overwrite: true);
            }
            else
            {
                // For memory-only trees, serialize directly to the target path
                var pathBytes = Encoding.UTF8.GetBytes(targetPath);
                int result;
                fixed (byte* pp = pathBytes)
                {
                    result = NativeBfTreeMethods.bftree_snapshot_memory(_tree, pp, pathBytes.Length);
                }
                if (result != 0)
                    throw new NotSupportedException(
                        "Snapshot to file is not yet supported for memory-only trees. Pending bf-tree cache_only snapshot support.");
            }
        }

        /// <summary>
        /// Recover a BfTree from its snapshot.
        /// For disk-backed trees: reopens the existing data file and resumes.
        /// For memory-only trees: loads the snapshot from disk into a new cache_only tree.
        /// Currently throws <see cref="NotSupportedException"/> for memory-only until
        /// bf-tree adds cache_only recovery support.
        /// </summary>
        public static BfTreeService RecoverFromSnapshot(
            string filePath,
            StorageBackendType storageBackend = StorageBackendType.Disk,
            ulong cbSizeByte = 0,
            uint cbMinRecordSize = 0,
            uint cbMaxRecordSize = 0,
            uint cbMaxKeyLen = 0,
            uint leafPageSize = 0)
        {
            var pathBytes = Encoding.UTF8.GetBytes(filePath);
            nint treePtr;

            if (storageBackend == StorageBackendType.Disk)
            {
                fixed (byte* pp = pathBytes)
                {
                    treePtr = NativeBfTreeMethods.bftree_new_from_snapshot(
                        pp, pathBytes.Length,
                        cbSizeByte, cbMinRecordSize, cbMaxRecordSize, cbMaxKeyLen, leafPageSize);
                }
                if (treePtr == 0)
                    throw new InvalidOperationException($"Failed to recover disk-backed BfTree from '{filePath}'.");
            }
            else
            {
                fixed (byte* pp = pathBytes)
                {
                    treePtr = NativeBfTreeMethods.bftree_recover_memory(
                        pp, pathBytes.Length,
                        cbSizeByte, cbMinRecordSize, cbMaxRecordSize, cbMaxKeyLen, leafPageSize);
                }
                if (treePtr == 0)
                    throw new NotSupportedException(
                        "Recovery is not yet supported for memory-only trees. Pending bf-tree cache_only recovery support.");
            }
            return new BfTreeService(treePtr, storageBackend, filePath);
        }

        /// <summary>
        /// Drains scan iterator via callback — zero per-record allocation.
        /// </summary>
        private static int DrainScanIteratorWithCallback(
            nint handle, Span<byte> buffer, ScanReturnField returnField, ScanRecordAction onRecord)
        {
            int count = 0;
            while (true)
            {
                int keyLen = 0, valueLen = 0;
                int hasNext;
                fixed (byte* bp = buffer)
                    hasNext = NativeBfTreeMethods.bftree_scan_next(
                        handle, bp, buffer.Length, &keyLen, &valueLen);
                if (hasNext == 0)
                    break;

                var key = returnField != ScanReturnField.Value
                    ? buffer[..keyLen] : ReadOnlySpan<byte>.Empty;
                var value = returnField != ScanReturnField.Key
                    ? buffer[keyLen..(keyLen + valueLen)] : ReadOnlySpan<byte>.Empty;

                count++;
                if (!onRecord(key, value))
                    break;
            }
            return count;
        }

        /// <summary>
        /// Drains scan iterator into a list — convenience, allocates per record.
        /// </summary>
        private static List<ScanRecord> DrainScanIteratorToList(nint handle, ScanReturnField returnField)
        {
            var results = new List<ScanRecord>();
            Span<byte> buffer = stackalloc byte[8192];
            while (true)
            {
                int keyLen = 0, valueLen = 0;
                int hasNext;
                fixed (byte* bp = buffer)
                    hasNext = NativeBfTreeMethods.bftree_scan_next(
                        handle, bp, buffer.Length, &keyLen, &valueLen);
                if (hasNext == 0)
                    break;

                var record = new ScanRecord
                {
                    Key = returnField != ScanReturnField.Value
                        ? buffer[..keyLen].ToArray()
                        : ReadOnlyMemory<byte>.Empty,
                    Value = returnField != ScanReturnField.Key
                        ? buffer[keyLen..(keyLen + valueLen)].ToArray()
                        : ReadOnlyMemory<byte>.Empty,
                };
                results.Add(record);
            }
            return results;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                if (_tree != 0)
                {
                    NativeBfTreeMethods.bftree_drop(_tree);
                    _tree = 0;
                }
            }
        }
    }
}