// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//! C FFI wrapper over the bf-tree crate for Garnet P/Invoke interop.
//!
//! Every public function is `#[no_mangle] extern "C"` so it can be called
//! from C# via `[LibraryImport("bftree_garnet")]`.

use bf_tree::{BfTree, Config, LeafInsertResult, LeafReadResult, ScanIter, ScanReturnField, StorageBackend};
use std::path::Path;
use std::slice;

// ---------------------------------------------------------------------------
// Result codes returned to C#
// ---------------------------------------------------------------------------

/// Read result: value was found, the return value is the number of bytes.
const READ_FOUND: i32 = 0; // Actual byte count is in `out_value_len`.
const READ_NOT_FOUND: i32 = -1;
const READ_DELETED: i32 = -2;
const READ_INVALID_KEY: i32 = -3;

const INSERT_SUCCESS: i32 = 0;
const INSERT_INVALID_KV: i32 = 1;

// ---------------------------------------------------------------------------
// Storage backend constants (matches C# StorageBackendType enum)
// ---------------------------------------------------------------------------

/// Disk-backed tree: base pages are stored in a data file.
const _STORAGE_DISK: u8 = 0;
/// Memory-only tree (bf-tree cache_only mode): bounded in-memory circular buffer.
const STORAGE_MEMORY: u8 = 1;

/// Helper to apply common config fields.
unsafe fn apply_common_config(
    config: &mut Config,
    cb_size_byte: u64,
    cb_min_record_size: u32,
    cb_max_record_size: u32,
    cb_max_key_len: u32,
    leaf_page_size: u32,
) {
    if cb_size_byte > 0 {
        config.cb_size_byte(cb_size_byte as usize);
    }
    if cb_min_record_size > 0 {
        config.cb_min_record_size(cb_min_record_size as usize);
    }
    if cb_max_record_size > 0 {
        config.cb_max_record_size(cb_max_record_size as usize);
    }
    if cb_max_key_len > 0 {
        config.cb_max_key_len(cb_max_key_len as usize);
    }
    if leaf_page_size > 0 {
        config.leaf_page_size(leaf_page_size as usize);
    }
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// Create a new BfTree with the given configuration.
///
/// `storage_backend`: 0 = Disk (file-backed), 1 = Memory (cache_only).
/// For disk-backed trees, `file_path` / `file_path_len` specify the data file.
/// For memory-only trees, `file_path` is ignored.
///
/// Returns a pointer to a heap-allocated BfTree, or null on failure.
///
/// # Safety
/// The caller must eventually call `bftree_drop` to free the returned pointer.
#[no_mangle]
pub unsafe extern "C" fn bftree_create(
    cb_size_byte: u64,
    cb_min_record_size: u32,
    cb_max_record_size: u32,
    cb_max_key_len: u32,
    leaf_page_size: u32,
    storage_backend: u8,
    file_path: *const u8,
    file_path_len: i32,
) -> *mut BfTree {
    let mut config = Config::default();
    apply_common_config(
        &mut config,
        cb_size_byte, cb_min_record_size, cb_max_record_size,
        cb_max_key_len, leaf_page_size,
    );

    if storage_backend == STORAGE_MEMORY {
        // Maps to bf-tree's cache_only mode: StorageBackend::Memory + cache_only=true
        // Bounded in-memory circular buffer.
        config.cache_only(true);
    } else {
        // STORAGE_DISK (default): file-backed tree.
        if file_path.is_null() || file_path_len <= 0 {
            return std::ptr::null_mut();
        }
        let path_bytes = slice::from_raw_parts(file_path, file_path_len as usize);
        let path_str = match std::str::from_utf8(path_bytes) {
            Ok(s) => s,
            Err(_) => return std::ptr::null_mut(),
        };
        config.storage_backend(StorageBackend::Std);
        config.file_path(Path::new(path_str));
    }

    match BfTree::with_config(config, None) {
        Ok(tree) => Box::into_raw(Box::new(tree)),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Drop (free) a BfTree instance.
///
/// # Safety
/// `tree` must be a valid pointer returned by `bftree_create` and must not be
/// used after this call.
#[no_mangle]
pub unsafe extern "C" fn bftree_drop(tree: *mut BfTree) {
    if !tree.is_null() {
        drop(Box::from_raw(tree));
    }
}

// ---------------------------------------------------------------------------
// Point operations
// ---------------------------------------------------------------------------

/// Insert a key-value pair. Returns INSERT_SUCCESS (0) or INSERT_INVALID_KV (1).
///
/// # Safety
/// `tree` must be a valid BfTree pointer. `key`/`value` must point to valid
/// memory of the specified lengths.
#[no_mangle]
pub unsafe extern "C" fn bftree_insert(
    tree: *mut BfTree,
    key: *const u8,
    key_len: i32,
    value: *const u8,
    value_len: i32,
) -> i32 {
    let tree = &*tree;
    let key = slice::from_raw_parts(key, key_len as usize);
    let value = slice::from_raw_parts(value, value_len as usize);
    match tree.insert(key, value) {
        LeafInsertResult::Success => INSERT_SUCCESS,
        LeafInsertResult::InvalidKV(_) => INSERT_INVALID_KV,
    }
}

/// Read the value for a key into `out_buffer`.
///
/// On success, writes the value bytes into `out_buffer` and sets
/// `*out_value_len` to the number of bytes written. Returns READ_FOUND (0).
///
/// On failure, returns READ_NOT_FOUND (-1), READ_DELETED (-2), or
/// READ_INVALID_KEY (-3).
///
/// # Safety
/// All pointer arguments must be valid. `out_buffer` must have at least
/// `out_buffer_len` bytes available.
#[no_mangle]
pub unsafe extern "C" fn bftree_read(
    tree: *mut BfTree,
    key: *const u8,
    key_len: i32,
    out_buffer: *mut u8,
    out_buffer_len: i32,
    out_value_len: *mut i32,
) -> i32 {
    let tree = &*tree;
    let key = slice::from_raw_parts(key, key_len as usize);
    let buffer = slice::from_raw_parts_mut(out_buffer, out_buffer_len as usize);
    match tree.read(key, buffer) {
        LeafReadResult::Found(n) => {
            if !out_value_len.is_null() {
                *out_value_len = n as i32;
            }
            READ_FOUND
        }
        LeafReadResult::NotFound => READ_NOT_FOUND,
        LeafReadResult::Deleted => READ_DELETED,
        LeafReadResult::InvalidKey => READ_INVALID_KEY,
    }
}

/// Delete a key from the tree.
///
/// # Safety
/// `tree` must be a valid BfTree pointer. `key` must point to valid memory.
#[no_mangle]
pub unsafe extern "C" fn bftree_delete(
    tree: *mut BfTree,
    key: *const u8,
    key_len: i32,
) {
    let tree = &*tree;
    let key = slice::from_raw_parts(key, key_len as usize);
    tree.delete(key);
}

// ---------------------------------------------------------------------------
// Scan operations
//
// Scans are modeled as an opaque iterator that the caller advances one record
// at a time via `bftree_scan_next`, then frees with `bftree_scan_drop`.
//
// Because `ScanIter` borrows the `BfTree`, we box a helper struct that owns
// the necessary references.
// ---------------------------------------------------------------------------

/// Opaque scan iterator handle. Caller must not interpret the pointer.
pub struct ScanHandle<'a> {
    iter: ScanIter<'a, 'a>,
}

/// Begin a scan-with-count. Returns an opaque iterator handle.
///
/// `return_field`: 0 = Key, 1 = Value, 2 = KeyAndValue.
///
/// # Safety
/// `tree` must be a valid BfTree pointer that outlives the returned handle.
/// Caller must free the handle with `bftree_scan_drop`.
#[no_mangle]
pub unsafe extern "C" fn bftree_scan_with_count(
    tree: *mut BfTree,
    start_key: *const u8,
    start_key_len: i32,
    count: i32,
    return_field: u8,
) -> *mut ScanHandle<'static> {
    let tree = &*tree;
    let start = slice::from_raw_parts(start_key, start_key_len as usize);
    let rf = match return_field {
        0 => ScanReturnField::Key,
        1 => ScanReturnField::Value,
        _ => ScanReturnField::KeyAndValue,
    };
    let iter = ScanIter::new_with_scan_count(tree, start, count as usize, rf);
    // SAFETY: We transmute the lifetime to 'static. The caller is responsible
    // for ensuring the BfTree outlives this handle and calling bftree_scan_drop.
    let handle = Box::new(ScanHandle {
        iter: std::mem::transmute(iter),
    });
    Box::into_raw(handle)
}

/// Begin a scan-with-end-key. Returns an opaque iterator handle.
///
/// # Safety
/// Same requirements as `bftree_scan_with_count`.
#[no_mangle]
pub unsafe extern "C" fn bftree_scan_with_end_key(
    tree: *mut BfTree,
    start_key: *const u8,
    start_key_len: i32,
    end_key: *const u8,
    end_key_len: i32,
    return_field: u8,
) -> *mut ScanHandle<'static> {
    let tree = &*tree;
    let start = slice::from_raw_parts(start_key, start_key_len as usize);
    let end = slice::from_raw_parts(end_key, end_key_len as usize);
    let rf = match return_field {
        0 => ScanReturnField::Key,
        1 => ScanReturnField::Value,
        _ => ScanReturnField::KeyAndValue,
    };
    let iter = ScanIter::new_with_end_key(tree, start, end, rf);
    let handle = Box::new(ScanHandle {
        iter: std::mem::transmute(iter),
    });
    Box::into_raw(handle)
}

/// Advance the scan iterator by one record.
///
/// Writes the record data into `out_buffer` and sets `*out_key_len` and
/// `*out_value_len` to the lengths of the key and value portions within
/// `out_buffer`.
///
/// Returns 1 if a record was produced, 0 if the scan is exhausted.
///
/// When `return_field` was Key: `out_buffer[..key_len]` is the key,
/// `out_value_len` is 0.
/// When Value: `out_buffer[..value_len]` is the value, `out_key_len` is 0.
/// When KeyAndValue: `out_buffer[..key_len]` is the key,
/// `out_buffer[key_len..key_len+value_len]` is the value.
///
/// # Safety
/// `handle` must be a valid ScanHandle pointer.
#[no_mangle]
pub unsafe extern "C" fn bftree_scan_next(
    handle: *mut ScanHandle<'static>,
    out_buffer: *mut u8,
    out_buffer_len: i32,
    out_key_len: *mut i32,
    out_value_len: *mut i32,
) -> i32 {
    let handle = &mut *handle;
    let buffer = slice::from_raw_parts_mut(out_buffer, out_buffer_len as usize);
    match handle.iter.next(buffer) {
        Some((key_len, value_len)) => {
            if !out_key_len.is_null() {
                *out_key_len = key_len as i32;
            }
            if !out_value_len.is_null() {
                *out_value_len = value_len as i32;
            }
            1
        }
        None => 0,
    }
}

/// Free a scan iterator handle.
///
/// # Safety
/// `handle` must be a valid pointer returned by `bftree_scan_with_count` or
/// `bftree_scan_with_end_key`, and must not be used after this call.
#[no_mangle]
pub unsafe extern "C" fn bftree_scan_drop(handle: *mut ScanHandle<'static>) {
    if !handle.is_null() {
        drop(Box::from_raw(handle));
    }
}

// ---------------------------------------------------------------------------
// Snapshot / Recovery
// ---------------------------------------------------------------------------

/// Snapshot a disk-backed BfTree in place.
///
/// Drains the circular buffer and writes the index structure to the tree's
/// own data file.
///
/// Returns 0 on success, -1 on failure.
///
/// # Safety
/// `tree` must be a valid BfTree pointer for a disk-backed tree.
#[no_mangle]
pub unsafe extern "C" fn bftree_snapshot(tree: *mut BfTree) -> i32 {
    let tree = &*tree;
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tree.snapshot();
    })) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

/// Recover a disk-backed BfTree from its snapshot file.
///
/// The config's file_path must point to an existing snapshot file.
/// If the file does not exist, creates a new empty tree at that path.
///
/// Returns a pointer to the new BfTree, or null on failure.
///
/// # Safety
/// Caller must eventually call `bftree_drop` on the returned pointer.
#[no_mangle]
pub unsafe extern "C" fn bftree_new_from_snapshot(
    file_path: *const u8,
    file_path_len: i32,
    cb_size_byte: u64,
    cb_min_record_size: u32,
    cb_max_record_size: u32,
    cb_max_key_len: u32,
    leaf_page_size: u32,
) -> *mut BfTree {
    let path_bytes = slice::from_raw_parts(file_path, file_path_len as usize);
    let path_str = match std::str::from_utf8(path_bytes) {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };

    let mut config = Config::default();
    apply_common_config(
        &mut config,
        cb_size_byte, cb_min_record_size, cb_max_record_size,
        cb_max_key_len, leaf_page_size,
    );
    config.storage_backend(StorageBackend::Std);
    config.file_path(Path::new(path_str));

    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        BfTree::new_from_snapshot(config, None)
    })) {
        Ok(Ok(tree)) => Box::into_raw(Box::new(tree)),
        Ok(Err(_)) => std::ptr::null_mut(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Snapshot a memory-only (cache_only) BfTree to a file on disk.
///
/// STUB: Not yet implemented in bf-tree. Returns -1 (failure) unconditionally.
/// Will be implemented when bf-tree adds cache_only snapshot support.
///
/// # Safety
/// `tree` must be a valid BfTree pointer. `path` must point to valid UTF-8 bytes.
#[no_mangle]
pub unsafe extern "C" fn bftree_snapshot_memory(
    _tree: *mut BfTree,
    _path: *const u8,
    _path_len: i32,
) -> i32 {
    // TODO: Implement when bf-tree adds cache_only snapshot support.
    -1
}

/// Recover a memory-only (cache_only) BfTree from a snapshot file on disk.
///
/// STUB: Not yet implemented in bf-tree. Returns null unconditionally.
/// Will be implemented when bf-tree adds cache_only recovery support.
///
/// # Safety
/// `path` must point to valid UTF-8 bytes. Caller must eventually call
/// `bftree_drop` on the returned pointer.
#[no_mangle]
pub unsafe extern "C" fn bftree_recover_memory(
    _path: *const u8,
    _path_len: i32,
    _cb_size_byte: u64,
    _cb_min_record_size: u32,
    _cb_max_record_size: u32,
    _cb_max_key_len: u32,
    _leaf_page_size: u32,
) -> *mut BfTree {
    // TODO: Implement when bf-tree adds cache_only recovery support.
    std::ptr::null_mut()
}

/// No-op function for measuring pure FFI transition overhead.
#[no_mangle]
#[inline(never)]
pub unsafe extern "C" fn bftree_noop(
    _tree: *mut BfTree,
    _key: *const u8,
    _key_len: i32,
) -> i32 {
    0
}
