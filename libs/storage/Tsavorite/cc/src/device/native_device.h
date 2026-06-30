// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#define _SILENCE_EXPERIMENTAL_FILESYSTEM_DEPRECATION_WARNING 1;
#include "file_system_disk.h"
#include "native_device_error.h"
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <experimental/filesystem>
#include <system_error>
#include <thread>

#if !defined(_WIN32) && !defined(_WIN64)
#include <fcntl.h>
#include <string>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <cstdio>
#else
#include <Windows.h>
#include <winioctl.h>
#endif

namespace native_device {

/// Probe the device's required O_DIRECT alignment for `filename`, as a power of two, floor
/// 512 B. Cold path, called once per device at construction. Never throws.
///
/// Returns the kernel-required alignment (logical block size / statx STATX_DIOALIGN), never
/// physical_block_size — physical is a write-RMW hint that can be huge (e.g. 256 KiB on some
/// NVMe) and would over-align every IO, as the upper layer rounds each IO up to this value.
///
/// Linux: statx(STATX_DIOALIGN) on the file or nearest existing ancestor, else sysfs
///   logical_block_size. Windows: IOCTL_STORAGE_QUERY_PROPERTY BytesPerLogicalSector.
///
/// Shared by the NativeDeviceImpl ctor (device_alignment_, returned by sector_size()) and the
/// C ABI NativeDevice_ProbeAlignment (sizes the C# SectorSize for every local-disk device).
/// Matches File::GetDeviceAlignment, so the per-File DIO asserts and sector_size() agree.
inline uint32_t ProbeDioAlignment(const char* filename) {
    constexpr uint32_t kFallback = 512u;
    if (filename == nullptr || *filename == '\0') return kFallback;
#if defined(_WIN32) || defined(_WIN64)
    // Resolve the volume root for `filename` ("C:\foo\bar.dat" -> "\\.\C:"). UNC paths
    // (\\?\..., \\server\share\...) aren't supported — fall back to 512 in that case.
    if (filename[0] == '\0' || filename[1] != ':') return kFallback;
    char volume_path[8];
    volume_path[0] = '\\';
    volume_path[1] = '\\';
    volume_path[2] = '.';
    volume_path[3] = '\\';
    volume_path[4] = filename[0];
    volume_path[5] = ':';
    volume_path[6] = '\0';

    HANDLE h = ::CreateFileA(volume_path,
                             FILE_READ_ATTRIBUTES,
                             FILE_SHARE_READ | FILE_SHARE_WRITE,
                             NULL,
                             OPEN_EXISTING,
                             0,
                             NULL);
    if (h == INVALID_HANDLE_VALUE) return kFallback;

    STORAGE_PROPERTY_QUERY query{};
    query.PropertyId = StorageAccessAlignmentProperty;
    query.QueryType = PropertyStandardQuery;

    STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR descriptor{};
    DWORD bytes_returned = 0;
    BOOL ok = ::DeviceIoControl(h, IOCTL_STORAGE_QUERY_PROPERTY,
                                &query, sizeof(query),
                                &descriptor, sizeof(descriptor),
                                &bytes_returned, NULL);
    ::CloseHandle(h);
    if (!ok || bytes_returned < sizeof(descriptor)) return kFallback;

    // Logical sector = required O_DIRECT alignment; ignore BytesPerPhysicalSector (a write-RMW hint).
    uint32_t sec = descriptor.BytesPerLogicalSector;
    if (sec == 0) return kFallback;

    uint32_t pow2 = kFallback;
    while (pow2 < sec) pow2 <<= 1;
    return pow2;
#else
    namespace fs = std::experimental::filesystem;

    // Resolve `filename` (or its nearest existing ancestor — the data file may not exist yet).
    struct stat st;
    std::string probe_path{ filename };
    if (::stat(probe_path.c_str(), &st) != 0) {
        fs::path p{ filename };
        bool found = false;
        for (;;) {
            auto parent = p.parent_path();
            if (parent.empty() || parent == p) break;
            p = parent;
            if (::stat(p.c_str(), &st) == 0) { probe_path = p.string(); found = true; break; }
        }
        if (!found) return kFallback;
    }

    uint32_t required = 0;

    // Preferred: kernel-required O_DIRECT alignment for this inode (Linux 6.1+).
#if defined(STATX_DIOALIGN)
    {
        struct statx stx {};
        if (::statx(AT_FDCWD, probe_path.c_str(), 0, STATX_DIOALIGN, &stx) == 0)
            required = std::max(stx.stx_dio_offset_align, stx.stx_dio_mem_align);
    }
#endif

    // Fallback (pre-6.1 kernel, or statx returned 0 — e.g. the path is a directory): sysfs
    // logical_block_size via st_dev. Not physical_block_size.
    if (required == 0) {
        unsigned maj = major(st.st_dev);
        unsigned min = minor(st.st_dev);

        auto read_int_file = [](const char* path) -> int {
            FILE* f = std::fopen(path, "r");
            if (!f) return -1;
            int v = -1;
            int matched = std::fscanf(f, "%d", &v);
            std::fclose(f);
            return matched == 1 ? v : -1;
        };
        auto read_queue_field = [&](const char* field) -> int {
            char path[512];
            // Whole-disk devices (e.g. nvme0n1) expose queue/ directly under their dev node.
            std::snprintf(path, sizeof(path),
                          "/sys/dev/block/%u:%u/queue/%s", maj, min, field);
            int v = read_int_file(path);
            if (v > 0) return v;
            // Partitions (e.g. sda2) don't have their own queue/ — it lives on the parent
            // whole-disk symlink target. ../queue/ resolves to the parent's queue dir.
            std::snprintf(path, sizeof(path),
                          "/sys/dev/block/%u:%u/../queue/%s", maj, min, field);
            return read_int_file(path);
        };

        int logical = read_queue_field("logical_block_size");
        if (logical > 0) required = static_cast<uint32_t>(logical);
    }

    if (required == 0) return kFallback;

    // Round up to a power of two, floor 512.
    uint32_t pow2 = 512u;
    while (pow2 < required) pow2 <<= 1;
    return std::max(kFallback, pow2);
#endif
}

} // namespace native_device

/// Abstract interface for a native device. Allows a single C ABI to dispatch to
/// different IO backends (libaio, io_uring on Linux; ThreadPool on Windows).
class INativeDevice {
public:
    virtual ~INativeDevice() = default;

    virtual void Reset() = 0;
    virtual uint32_t sector_size() const = 0;
    /// Configured segment size in bytes. Surfaced to C# for ABI validation — the managed side
    /// asserts that what it requested matches what the native device was actually built with.
    virtual uint64_t segment_size_bytes() const = 0;

    virtual FASTER::core::Status ReadAsync(uint64_t source, void* dest, uint32_t length,
                                           FASTER::core::AsyncIOCallback callback, void* context) = 0;
    virtual FASTER::core::Status WriteAsync(const void* source, uint64_t dest, uint32_t length,
                                            FASTER::core::AsyncIOCallback callback, void* context) = 0;

    virtual int CreateDir(const std::string& dir, bool delete_existing) = 0;
    virtual bool TryComplete() = 0;
    virtual uint64_t GetFileSize(uint64_t segment) = 0;
    virtual void RemoveSegment(uint64_t segment) = 0;
    virtual int QueueRun(int timeout_secs) = 0;
    /// Per-context (shard) drain. ctx_idx in [0, num_io_contexts). Used by completion threads
    /// bound 1:1 to a context. Returns -1 if ctx_idx is out of range.
    virtual int QueueRunFor(int ctx_idx, int timeout_secs) = 0;
    /// Submit a no-op completion event so a thread blocked in QueueRunFor for `ctx_idx`
    /// wakes up immediately. Used by Dispose() to unblock the completion drainer without
    /// waiting on the QueueRunFor timeout (which is otherwise a per-context shutdown stall).
    /// Returns 0 on success, -1 on failure (out-of-range ctx_idx, unable to submit, etc).
    virtual int Wake(int ctx_idx) = 0;
    /// Number of submission/completion shards. >= 1.
    virtual int num_io_contexts() const = 0;
};

/// Templated native device implementation. HandlerT selects the IO backend:
///   - QueueIoHandler on Linux         -> libaio backend
///   - UringIoHandler on Linux         -> io_uring backend (requires FASTER_URING)
///   - ThreadPoolIoHandler on Windows  -> Windows ThreadPool backend
template <class HandlerT>
class NativeDeviceImpl : public INativeDevice {
public:
    typedef HandlerT handler_t;
    typedef FASTER::device::FileSystemSegmentedFile<handler_t> log_file_t;

private:
    class AsyncIoContext : public FASTER::core::IAsyncContext {
    public:
        AsyncIoContext(void* context_, FASTER::core::AsyncIOCallback callback_)
            : context{ context_ },
            callback{ callback_ } {
        }

        /// The deep-copy constructor
        AsyncIoContext(AsyncIoContext& other)
            : context{ other.context},
            callback{ other.callback } {
        }

    protected:
        FASTER::core::Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
            return IAsyncContext::DeepCopy_Internal(*this, context_copy);
        }

    public:
        void* context;
        FASTER::core::AsyncIOCallback callback;
    };

    /// RAII guard pairing Thread::acquire_id() + epoch_.ProtectAndDrain() (on entry) with
    /// epoch_.Unprotect() + Thread::release_id() (on scope exit), so BOTH the LightEpoch
    /// protection and the thread-id slot are returned even if the guarded body throws.
    ///
    /// Why this matters: the device entry points below run `log_.ReadAsync/WriteAsync/Close`,
    /// which can allocate (OpenSegment -> std::malloc / new bundle_t) and therefore throw
    /// std::bad_alloc; ProtectAndDrain can also run drain-list callbacks. Previously these
    /// methods released the epoch/id with two trailing statements, so any throw between
    /// acquire and release leaked the thread's epoch entry. A leaked entry permanently pins
    /// safe_to_reclaim_epoch (ComputeNewSafeToReclaimEpoch takes the min local epoch across
    /// the table), which stalls reclamation and eventually wedges BumpCurrentEpoch once the
    /// drain list fills — a silent, unrecoverable hang. The guard makes the pairing
    /// exception-safe; if ProtectAndDrain itself throws, the id is still released before the
    /// exception propagates.
    struct EpochGuard {
        FASTER::core::LightEpoch& epoch;
        explicit EpochGuard(FASTER::core::LightEpoch& e) : epoch(e) {
            FASTER::core::Thread::acquire_id();
            try {
                epoch.ProtectAndDrain();
            } catch (...) {
                FASTER::core::Thread::release_id();
                throw;
            }
        }
        ~EpochGuard() {
            epoch.Unprotect();
            FASTER::core::Thread::release_id();
        }
        EpochGuard(const EpochGuard&) = delete;
        EpochGuard& operator=(const EpochGuard&) = delete;
    };

    /// Runs an IO submission (`submit` returns the FileSystemSegmentedFile status) under epoch
    /// protection, retrying OUTSIDE the epoch whenever the submission reports a sustained
    /// full kernel submission ring.
    ///
    /// The file layer (QueueFile/UringFile::ScheduleOperation) signals a sustained-full ring by
    /// returning Status::Pending after a short in-epoch yield budget, having submitted nothing
    /// (its RAII guards free the per-op io_context). We must NOT spin on the full ring while
    /// holding the epoch: the epoch protects the segment file-handle bundle, and a thread
    /// parked there pins the thread-id slot AND safe_to_reclaim_epoch, which under enough
    /// concurrent submitters starves other submitters of slots and can wedge reclamation. So
    /// on Pending we drop the EpochGuard (releasing the slot + protection), back off, then
    /// re-acquire and retry the whole op — which safely re-resolves the bundle under fresh
    /// protection. Status::Pending is fully contained here and never surfaces to the managed
    /// caller. With the ring depth sized to the throttle limit this retry path is effectively
    /// never taken; it remains a correctness safety net for genuine kernel-side EAGAIN.
    template <typename SubmitFn>
    FASTER::core::Status SubmitWithEpoch(SubmitFn&& submit) {
        // Outside-epoch retry backoff: yield this many times (cheap) before falling back to a
        // short sleep, so a sustained-full ring doesn't burn a core. This budget is NOT held under
        // the epoch (the EpochGuard scope has already ended), so it only governs the retry cadence.
        constexpr int kRetryYieldBudget = 16;
        int retries = 0;
        while (true) {
            FASTER::core::Status result;
            {
                EpochGuard guard{ epoch_ };
                result = submit();
            }
            if (result != FASTER::core::Status::Pending) {
                return result;
            }
            // Sustained full ring; the epoch (and its thread-id slot) is now released. Back off
            // before retrying so other submitters can make progress and the drainer can free
            // ring space.
            if (retries < kRetryYieldBudget) {
                std::this_thread::yield();
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            ++retries;
        }
    }

public:
    NativeDeviceImpl(const std::string& file,
        uint64_t segment_size,
        bool omit_segment_id,
        int num_io_contexts = 1,
        bool enablePrivileges = false,
        bool unbuffered = true,
        bool delete_on_close = false,
        int max_events = 0)
        : epoch_ { }
        // max_events is the per-context kernel submission-ring depth (libaio io_setup /
        // io_uring SQ entries). The managed wrapper sizes it up from the device throttle limit
        // (NextPowerOf2) so the ring can hold the full in-flight burst the throttle permits;
        // this keeps io_submit / io_uring_get_sqe off their EAGAIN/ring-full backoff spins,
        // which would otherwise pin epoch slots. <= 0 means "use the handler's default depth".
        , handler_{ 16 /*max threads*/, num_io_contexts < 1 ? 1 : num_io_contexts, max_events }
        , default_file_options_{ unbuffered, delete_on_close }
        // FileSystemSegmentedFile validates segment_size internally (must be a positive power
        // of two) and throws std::invalid_argument otherwise. The C ABI wrapper wraps `new
        // NativeDeviceImpl(...)` in try/catch and converts the exception into a populated
        // last_error + nullptr return.
        , log_{ file, default_file_options_, &epoch_, segment_size, omit_segment_id }
        , segment_size_{ segment_size }
        , omit_segment_id_{ omit_segment_id }
        , device_alignment_{ native_device::ProbeDioAlignment(file.c_str()) }
        , init_status_{ FASTER::core::Status::Ok } {
        // First gate: handler init (io_setup / io_uring_queue_init) succeeded?
        if (handler_.init_errno() != 0) {
            int e = handler_.init_errno();
            native_device::set_last_error(
                "Native device IO handler init failed: errno %d (%s). "
                "Possible causes: (1) RLIMIT_AIO / fs.aio-max-nr exceeded "
                "(try: sudo sysctl -w fs.aio-max-nr=1048576); "
                "(2) io_uring disabled by kernel.io_uring_disabled or seccomp policy; "
                "(3) kernel too old (libaio < 2.4 / io_uring < 5.1).",
                e, std::strerror(e));
            init_status_ = FASTER::core::Status::IOError;
            return;
        }
        FASTER::core::Status result;
        {
            EpochGuard guard{ epoch_ };
            result = log_.Open(&handler_);
        }
        if (result != FASTER::core::Status::Ok) {
            // log_.Open's most common failures: directory missing, permission denied,
            // O_DIRECT unsupported on filesystem, sector-size mismatch (set by
            // File::GetDeviceAlignment when statx reports an alignment requirement > 512).
            // Preserve a more specific message if a deeper layer already populated last_error;
            // otherwise fall back to a generic actionable hint.
            if (native_device::last_error_storage().empty()) {
                native_device::set_last_error(
                    "log_.Open('%s') failed: Status %d. "
                    "Check that the parent directory exists and is writable, and that the "
                    "filesystem supports O_DIRECT (use ext4/xfs, or pass disableFileBuffering=false).",
                    file.c_str(), (int)result);
            }
            init_status_ = result;
            return;
        }

        // Recovery sanity check: ensure existing on-disk segment files match our configured
        // segment_size. If a previous run used a larger segment size and we open with a smaller
        // one now, the segment offset arithmetic would silently address the wrong file and
        // corrupt the log. Scan the parent directory for `<filename>.<id>` files and verify
        // none exceeds segment_size_. This is cold-path (one-time at construction) so the I/O
        // cost is acceptable.
        if (!ValidateRecoveredSegments(file)) {
            init_status_ = FASTER::core::Status::IOError;
            return;
        }
    }

private:
    /// Walks the parent directory of `file` looking for segment files matching
    /// `<basename>.<id>` and verifies each one is at most `segment_size_` bytes. If any file
    /// is larger, populates last_error with an actionable message and returns false.
    /// Cold path — runs once at construction, never on the IO hot path.
    ///
    /// In `omit_segment_id_` mode there is only a single bare-named file, so the per-segment
    /// recovery check is meaningless: a stale file from an earlier run is simply reopened as
    /// the single segment and any size mismatch is irrelevant (we're in unbounded mode and
    /// segment_size_ is 1<<63). Skip the check entirely.
    bool ValidateRecoveredSegments(const std::string& file) {
        if (omit_segment_id_) return true;
        namespace fs = std::experimental::filesystem;
        std::error_code ec;
        fs::path target{ file };
        fs::path parent = target.parent_path();
        std::string base = target.filename().string();
        if (parent.empty()) parent = fs::current_path(ec);
        if (ec) return true; // can't enumerate → don't block startup
        fs::directory_iterator it{ parent, ec };
        if (ec) return true;
        for (const auto& entry : it) {
            std::error_code ec2;
            auto name = entry.path().filename().string();
            if (name.size() <= base.size() + 1) continue;
            if (name.compare(0, base.size(), base) != 0) continue;
            if (name[base.size()] != '.') continue;
            // Must be all digits after the dot to be a real segment file.
            bool is_segment = true;
            for (size_t i = base.size() + 1; i < name.size(); ++i) {
                if (name[i] < '0' || name[i] > '9') { is_segment = false; break; }
            }
            if (!is_segment) continue;
            auto sz = fs::file_size(entry.path(), ec2);
            if (ec2) continue;
            if (sz > segment_size_) {
                native_device::set_last_error(
                    "Recovery mismatch: existing segment file '%s' is %llu bytes but configured "
                    "segment_size is %llu bytes. The data on disk was written with a larger "
                    "segment size; opening with the smaller one would corrupt the log. "
                    "Either delete the existing files or restart with the original segment size.",
                    entry.path().string().c_str(),
                    static_cast<unsigned long long>(sz),
                    static_cast<unsigned long long>(segment_size_));
                return false;
            }
        }
        return true;
    }

public:

    /// Runtime segment size in bytes. Set at construction and immutable thereafter.
    /// Exposed below via the INativeDevice::segment_size_bytes() override.

    /// Init outcome from the constructor. Status::Ok means the device is ready; any other
    /// value means construction failed and the caller should delete the half-constructed
    /// device (the destructor is safe to invoke on a failed-init instance).
    FASTER::core::Status init_status() const { return init_status_; }

    ~NativeDeviceImpl() override {
        // Only attempt log_.Close if Open succeeded — otherwise the file table is empty and
        // Close would be a no-op anyway, but skipping the epoch dance avoids spurious work
        // (and the assert in Debug builds).
        if (init_status_ != FASTER::core::Status::Ok) {
            return;
        }
        FASTER::core::Status result;
        {
            EpochGuard guard{ epoch_ };
            result = log_.Close();
        }
        assert(result == FASTER::core::Status::Ok);
    }

    /// Methods required by the (implicit) disk interface.
    void Reset() override {
        EpochGuard guard{ epoch_ };
        (void)log_.Close();
    }

    /// Methods required by the (implicit) disk interface.
    uint32_t sector_size() const override {
        // device_alignment_ = ProbeDioAlignment (same routine as the C# probe, so they agree).
        // Not log_.alignment(): FileSystemSegmentedFile hardcodes 512, under-reporting on
        // 4K-native disks.
        return device_alignment_;
    }

    uint64_t segment_size_bytes() const override {
        return segment_size_;
    }

    FASTER::core::Status ReadAsync(uint64_t source, void* dest, uint32_t length,
                                   FASTER::core::AsyncIOCallback callback, void* context) override {
        AsyncIoContext io_context{ context, callback };
        auto callback_ = [](FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result, size_t bytes_transferred) {
            FASTER::core::CallbackContext<AsyncIoContext> context{ ctxt };
            context->callback((FASTER::core::IAsyncContext*)context->context, result, bytes_transferred);
            };
        return SubmitWithEpoch([&]() {
            return log_.ReadAsync(source, dest, length, callback_, io_context);
        });
    }

    FASTER::core::Status WriteAsync(const void* source, uint64_t dest, uint32_t length,
                                    FASTER::core::AsyncIOCallback callback, void* context) override {
        AsyncIoContext io_context{ context, callback };
        auto callback_ = [](FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result, size_t bytes_transferred) {
            FASTER::core::CallbackContext<AsyncIoContext> context{ ctxt };
            context->callback((FASTER::core::IAsyncContext*)context->context, result, bytes_transferred);
            };
        return SubmitWithEpoch([&]() {
            return log_.WriteAsync(source, dest, length, callback_, io_context);
        });
    }

    const log_file_t& log() const {
        return log_;
    }
    log_file_t& log() {
        return log_;
    }

    /// Create a directory. When `delete_existing` is true the directory and its contents
    /// are removed first (recursive); otherwise existing contents are preserved. Returns 0
    /// on success, -1 on failure (the thread-local last-error is populated via
    /// NativeDevice_GetLastError).
    int CreateDir(const std::string& dir, bool delete_existing) override {
        std::error_code ec;
        std::experimental::filesystem::path path{ dir };
        if (delete_existing) {
            std::experimental::filesystem::remove_all(path, ec);
            // remove_all returns 0 + sets ec only for real errors. "path doesn't exist" is
            // not an error and ec is left clear.
            if (ec) {
                native_device::set_last_error(
                    "CreateDir: remove_all('%s') failed: %s. Check directory permissions.",
                    dir.c_str(), ec.message().c_str());
                return -1;
            }
        }
        std::experimental::filesystem::create_directories(path, ec);
        if (ec) {
            native_device::set_last_error(
                "CreateDir: create_directories('%s') failed: %s. Check parent path permissions and disk space.",
                dir.c_str(), ec.message().c_str());
            return -1;
        }
        return 0;
    }

    /// Implementation-specific accessor.
    handler_t& handler() {
        return handler_;
    }

    bool TryComplete() override {
        return handler_.TryComplete();
    }

    uint64_t GetFileSize(uint64_t segment) override {
        // log_.size() can lazily OpenSegment(), whose bundle-expand path calls
        // epoch_->BumpCurrentEpoch() (it must publish the new file bundle and defer freeing the
        // old one to the drain list). BumpCurrentEpoch requires the calling thread to hold epoch
        // protection — without it ProtectAndDrain dereferences an unreserved epoch-table slot and
        // crashes. So acquire the epoch here exactly as RemoveSegment/Read/WriteAsync do. (The
        // first-ever OpenSegment on a cold device takes a no-bump path, which is why a single
        // GetFileSize on a fresh device did not previously fault.)
        EpochGuard guard{ epoch_ };
        return log_.size(segment);
    }

    void RemoveSegment(uint64_t segment) override {
        EpochGuard guard{ epoch_ };
        log_.RemoveSegment(segment);
    }

    int QueueRun(int timeout_secs) override {
        return handler_.QueueRun(timeout_secs);
    }

    int QueueRunFor(int ctx_idx, int timeout_secs) override {
        return handler_.QueueRunFor(ctx_idx, timeout_secs);
    }

    int Wake(int ctx_idx) override {
        return handler_.Wake(ctx_idx);
    }

    int num_io_contexts() const override {
        return handler_.num_contexts();
    }

private:
    FASTER::core::LightEpoch epoch_;
    handler_t handler_;
    FASTER::environment::FileOptions default_file_options_;

    /// Store the data
    log_file_t log_;

    /// Runtime segment size in bytes, set at construction time. Surfaced to the C# caller via
    /// NativeDevice_GetSegmentSize so the managed side can validate that the native device was
    /// created with the segment size it asked for (defense in depth against ABI mismatches).
    const uint64_t segment_size_;

    /// When true, segment files are named just `<basename>` instead of `<basename>.<id>`.
    /// Only meaningful in unbounded single-segment mode (the managed wrapper passes
    /// `segment_size = 1<<63` together with this flag) — the bare filename can be opened by
    /// external readers that don't know about segment-id naming.
    const bool omit_segment_id_;

    /// Kernel-reported direct-I/O alignment for the device backing `file`. Probed once at
    /// construction via native_device::ProbeDioAlignment (statx STATX_DIOALIGN with parent-dir
    /// fallback). Always a power of two >= 512. Returned by sector_size(); the managed C#
    /// wrapper cross-checks this against its own probe of the same path so any ABI / runtime
    /// drift is caught before the first IO.
    const uint32_t device_alignment_;

    /// Result of construction. Status::Ok = ready; anything else = initialization failed and
    /// the caller (typically the C ABI wrapper) must delete the instance and surface the
    /// thread-local last-error message to the managed caller.
    FASTER::core::Status init_status_;
};

/// Backend identifiers exposed across the C ABI. Must stay in sync with the C# enum
/// NativeStorageDevice.IoBackend.
enum NativeDeviceBackend : int32_t {
    NativeDeviceBackend_Default = 0,  // Platform default (libaio on Linux, ThreadPool on Windows)
    NativeDeviceBackend_Libaio  = 1,  // Linux only
    NativeDeviceBackend_Uring   = 2,  // Linux only; requires the native lib to be built with FASTER_URING
};

#if defined(_WIN32) || defined(_WIN64)
typedef NativeDeviceImpl<FASTER::environment::ThreadPoolIoHandler> NativeDeviceDefault;
#else
typedef NativeDeviceImpl<FASTER::environment::QueueIoHandler> NativeDeviceLibaio;
typedef NativeDeviceLibaio NativeDeviceDefault;
#ifdef FASTER_URING
typedef NativeDeviceImpl<FASTER::environment::UringIoHandler> NativeDeviceUring;
#endif
#endif

/// Back-compat alias. New code should prefer INativeDevice + NativeDeviceImpl<...>.
typedef NativeDeviceDefault NativeDevice;
