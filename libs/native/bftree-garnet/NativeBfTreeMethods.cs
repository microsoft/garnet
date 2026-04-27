// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Garnet.server.BfTreeInterop
{
    /// <summary>
    /// P/Invoke declarations for the native bftree-garnet library.
    /// Uses source-generated LibraryImport for zero-overhead interop.
    /// </summary>
    internal static unsafe partial class NativeBfTreeMethods
    {
        private const string LibName = "bftree_garnet";

        // ---------------------------------------------------------------
        // Lifecycle
        // ---------------------------------------------------------------

        /// <summary>
        /// Create a new BfTree. Returns a native pointer, or IntPtr.Zero on failure.
        /// Pass 0 for any numeric parameter to use the bf-tree default.
        /// storage_backend: 0 = Disk, 1 = Memory.
        /// For disk-backed trees, file_path/file_path_len specify the data file path.
        /// For in-memory trees, file_path can be null.
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial nint bftree_create(
            ulong cb_size_byte,
            uint cb_min_record_size,
            uint cb_max_record_size,
            uint cb_max_key_len,
            uint leaf_page_size,
            byte storage_backend,
            byte* file_path,
            int file_path_len);

        /// <summary>
        /// Free a BfTree instance.
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial void bftree_drop(nint tree);

        // ---------------------------------------------------------------
        // Point operations
        // ---------------------------------------------------------------

        /// <summary>
        /// Insert a key-value pair. Returns 0 on success, 1 on invalid KV.
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial int bftree_insert(
            nint tree,
            byte* key, int key_len,
            byte* value, int value_len);

        /// <summary>
        /// Read the value for a key into out_buffer.
        /// Returns 0 (found), -1 (not found), -2 (deleted), -3 (invalid key).
        /// On success, out_value_len is set to the number of bytes written.
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial int bftree_read(
            nint tree,
            byte* key, int key_len,
            byte* out_buffer, int out_buffer_len,
            int* out_value_len);

        /// <summary>
        /// Delete a key from the tree.
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial void bftree_delete(
            nint tree,
            byte* key, int key_len);

        // ---------------------------------------------------------------
        // Scan operations
        // ---------------------------------------------------------------

        /// <summary>
        /// Begin a scan-with-count. Returns an opaque iterator handle.
        /// return_field: 0=Key, 1=Value, 2=KeyAndValue.
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial nint bftree_scan_with_count(
            nint tree,
            byte* start_key, int start_key_len,
            int count,
            byte return_field);

        /// <summary>
        /// Begin a scan-with-end-key. Returns an opaque iterator handle.
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial nint bftree_scan_with_end_key(
            nint tree,
            byte* start_key, int start_key_len,
            byte* end_key, int end_key_len,
            byte return_field);

        /// <summary>
        /// Advance the scan iterator. Returns 1 if a record was produced, 0 if exhausted.
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial int bftree_scan_next(
            nint handle,
            byte* out_buffer, int out_buffer_len,
            int* out_key_len, int* out_value_len);

        /// <summary>
        /// Free a scan iterator handle.
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial void bftree_scan_drop(nint handle);

        // ---------------------------------------------------------------
        // Snapshot / Recovery
        // ---------------------------------------------------------------

        /// <summary>
        /// Snapshot a disk-backed BfTree in place. Drains circular buffer and
        /// writes index structure to the tree's own data file.
        /// Returns 0 on success, -1 on failure.
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial int bftree_snapshot(nint tree);

        /// <summary>
        /// Recover a disk-backed BfTree from its snapshot file.
        /// If the file does not exist, creates a new empty tree.
        /// Returns a native pointer, or IntPtr.Zero on failure.
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial nint bftree_new_from_snapshot(
            byte* file_path, int file_path_len,
            ulong cb_size_byte,
            uint cb_min_record_size,
            uint cb_max_record_size,
            uint cb_max_key_len,
            uint leaf_page_size);

        /// <summary>
        /// Snapshot a memory-only (cache_only) BfTree to a file on disk.
        /// STUB: returns -1 until bf-tree adds cache_only snapshot support.
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial int bftree_snapshot_memory(
            nint tree,
            byte* path, int path_len);

        /// <summary>
        /// Recover a memory-only (cache_only) BfTree from a snapshot file.
        /// STUB: returns IntPtr.Zero until bf-tree adds cache_only recovery support.
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial nint bftree_recover_memory(
            byte* path, int path_len,
            ulong cb_size_byte,
            uint cb_min_record_size,
            uint cb_max_record_size,
            uint cb_max_key_len,
            uint leaf_page_size);

        /// <summary>
        /// No-op for measuring pure FFI transition overhead.
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial int bftree_noop(
            nint tree,
            byte* key, int key_len);
    }
}