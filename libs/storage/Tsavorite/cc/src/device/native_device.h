// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#define _SILENCE_EXPERIMENTAL_FILESYSTEM_DEPRECATION_WARNING 1;
#include "file_system_disk.h"
#include "native_device_error.h"
#include <cerrno>
#include <cstring>
#include <experimental/filesystem>
#include <system_error>

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

public:
    NativeDeviceImpl(const std::string& file,
        uint64_t segment_size,
        bool omit_segment_id,
        int num_io_contexts = 1,
        bool enablePrivileges = false,
        bool unbuffered = true,
        bool delete_on_close = false)
        : epoch_ { }
        , handler_{ 16 /*max threads*/, num_io_contexts < 1 ? 1 : num_io_contexts }
        , default_file_options_{ unbuffered, delete_on_close }
        // FileSystemSegmentedFile validates segment_size internally (must be a positive power
        // of two) and throws std::invalid_argument otherwise. The C ABI wrapper wraps `new
        // NativeDeviceImpl(...)` in try/catch and converts the exception into a populated
        // last_error + nullptr return.
        , log_{ file, default_file_options_, &epoch_, segment_size, omit_segment_id }
        , segment_size_{ segment_size }
        , omit_segment_id_{ omit_segment_id }
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
        FASTER::core::Thread::acquire_id();
        epoch_.ProtectAndDrain();
        FASTER::core::Status result = log_.Open(&handler_);
        epoch_.Unprotect();
        FASTER::core::Thread::release_id();
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
        FASTER::core::Thread::acquire_id();
        epoch_.ProtectAndDrain();
        FASTER::core::Status result = log_.Close();
        epoch_.Unprotect();
        FASTER::core::Thread::release_id();
        assert(result == FASTER::core::Status::Ok);
    }

    /// Methods required by the (implicit) disk interface.
    void Reset() override {
        FASTER::core::Thread::acquire_id();
        epoch_.ProtectAndDrain();
        FASTER::core::Status result = log_.Close();
        epoch_.Unprotect();
        FASTER::core::Thread::release_id();
    }

    /// Methods required by the (implicit) disk interface.
    uint32_t sector_size() const override {
        return static_cast<uint32_t>(log_.alignment());
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
        FASTER::core::Thread::acquire_id();
        epoch_.ProtectAndDrain();
        FASTER::core::Status status = log_.ReadAsync(source, dest, length, callback_, io_context);
        epoch_.Unprotect();
        FASTER::core::Thread::release_id();
        return status;
    }

    FASTER::core::Status WriteAsync(const void* source, uint64_t dest, uint32_t length,
                                    FASTER::core::AsyncIOCallback callback, void* context) override {
        AsyncIoContext io_context{ context, callback };
        auto callback_ = [](FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result, size_t bytes_transferred) {
            FASTER::core::CallbackContext<AsyncIoContext> context{ ctxt };
            context->callback((FASTER::core::IAsyncContext*)context->context, result, bytes_transferred);
            };
        FASTER::core::Thread::acquire_id();
        epoch_.ProtectAndDrain();
        FASTER::core::Status status = log_.WriteAsync(source, dest, length, callback_, io_context);
        epoch_.Unprotect();
        FASTER::core::Thread::release_id();
        return status;
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
        return log_.size(segment);
    }

    void RemoveSegment(uint64_t segment) override {
        FASTER::core::Thread::acquire_id();
        epoch_.ProtectAndDrain();
        log_.RemoveSegment(segment);
        epoch_.Unprotect();
        FASTER::core::Thread::release_id();
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
