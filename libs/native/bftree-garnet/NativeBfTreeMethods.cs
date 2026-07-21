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
        /// use_snapshot: non-zero enables CPR snapshots on the tree (the snapshot
        /// destination is supplied at <c>bftree_cpr_snapshot</c> call time). Pass 0 to disable.
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
            int file_path_len,
            byte use_snapshot);

        /// <summary>
        /// Free a BfTree instance.
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial void bftree_drop(nint tree);

        // ---------------------------------------------------------------
        // Point operations
        // ---------------------------------------------------------------

        /// <summary>
        /// Insert a key-value pair. Returns 0 on success, 1 on invalid KV (size limits),
        /// -1 on invalid arguments (null pointer or negative length).
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial int bftree_insert(
            nint tree,
            byte* key, int key_len,
            byte* value, int value_len);

        /// <summary>
        /// Read the value for a key into out_buffer.
        /// Returns 0 (found), -1 (not found), -2 (deleted), -3 (invalid key),
        /// -4 (invalid arguments: null pointer or negative length).
        /// On success, out_value_len is set to the number of bytes written.
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial int bftree_read(
            nint tree,
            byte* key, int key_len,
            byte* out_buffer, int out_buffer_len,
            int* out_value_len);

        /// <summary>
        /// Delete a key from the tree. Returns 0 on success, -1 on invalid arguments
        /// (null pointer or negative length).
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial int bftree_delete(
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
        // Snapshot / Recovery (CPR)
        // ---------------------------------------------------------------

        /// <summary>
        /// Take a CPR (Concurrent Prefix Recovery) snapshot of a BfTree, writing it to
        /// <paramref name="snapshot_path"/>. Synchronous; designed to be non-blocking to
        /// concurrent insert/read/delete callers. The snapshot destination is a call-time
        /// argument.
        ///
        /// Internal <c>snapshot_in_progress</c> AtomicBool serializes concurrent calls;
        /// losers no-op silently. Each call writes a fresh, self-contained snapshot file to
        /// the supplied <paramref name="snapshot_path"/>; callers that need concurrent
        /// snapshots of the same tree to all succeed must serialize externally.
        ///
        /// Returns 0 on success, -1 on panic or invalid/empty path.
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial int bftree_cpr_snapshot(
            nint tree,
            byte* snapshot_path, int snapshot_path_len);

        /// <summary>
        /// Recover a BfTree from a CPR snapshot file. Unified for disk-backed and
        /// memory-backed (cache_only) trees — the storage backend is recorded in the
        /// snapshot.
        ///
        /// recovery_path: source CPR snapshot file to recover from.
        /// use_snapshot: non-zero enables CPR snapshots on the recovered tree (the
        ///   destination is supplied at <c>bftree_cpr_snapshot</c> call time). Pass 0 to disable.
        /// buffer_ptr: optional pre-allocated buffer for the recovered tree's cache.
        ///   If null, bftree allocates and owns the buffer (freed on tree.Dispose).
        ///   If non-null, the caller owns the buffer.
        ///
        /// Returns a native pointer, or IntPtr.Zero on failure.
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial nint bftree_new_from_cpr_snapshot(
            byte* recovery_path, int recovery_path_len,
            byte use_snapshot,
            byte* buffer_ptr, nuint buffer_size);

        /// <summary>
        /// Returns 1 if all threads have moved past the snapshot's version barrier,
        /// 0 otherwise, -1 on panic. Useful for assertions/diagnostics.
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial int bftree_are_all_threads_in_next_version(nint tree);

        /// <summary>
        /// No-op for measuring pure FFI transition overhead.
        /// </summary>
        [LibraryImport(LibName)]
        internal static partial int bftree_noop(
            nint tree,
            byte* key, int key_len);
    }
}