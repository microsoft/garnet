// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#define _SILENCE_EXPERIMENTAL_FILESYSTEM_DEPRECATION_WARNING 1;
#include "file_system_disk.h"  

/// Abstract interface for a native device. Allows a single C ABI to dispatch to
/// different IO backends (libaio, io_uring on Linux; ThreadPool on Windows).
class INativeDevice {
public:
    virtual ~INativeDevice() = default;

    virtual void Reset() = 0;
    virtual uint32_t sector_size() const = 0;

    virtual FASTER::core::Status ReadAsync(uint64_t source, void* dest, uint32_t length,
                                           FASTER::core::AsyncIOCallback callback, void* context) = 0;
    virtual FASTER::core::Status WriteAsync(const void* source, uint64_t dest, uint32_t length,
                                            FASTER::core::AsyncIOCallback callback, void* context) = 0;

    virtual void CreateDir(const std::string& dir) = 0;
    virtual bool TryComplete() = 0;
    virtual uint64_t GetFileSize(uint64_t segment) = 0;
    virtual void RemoveSegment(uint64_t segment) = 0;
    virtual int QueueRun(int timeout_secs) = 0;
};

/// Templated native device implementation. HandlerT selects the IO backend:
///   - QueueIoHandler on Linux         -> libaio backend
///   - UringIoHandler on Linux         -> io_uring backend (requires FASTER_URING)
///   - ThreadPoolIoHandler on Windows  -> Windows ThreadPool backend
template <class HandlerT>
class NativeDeviceImpl : public INativeDevice {
public:
    typedef HandlerT handler_t;
    typedef FASTER::device::FileSystemSegmentedFile<handler_t, 1073741824L> log_file_t;

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
        bool enablePrivileges = false,
        bool unbuffered = true,
        bool delete_on_close = false)
        : handler_{ 16 /*max threads*/ }
        , epoch_ { }
        , default_file_options_{ unbuffered, delete_on_close }
        , log_{ file, default_file_options_, &epoch_ } {
        FASTER::core::Thread::acquire_id();
        epoch_.ProtectAndDrain();
        FASTER::core::Status result = log_.Open(&handler_);
        epoch_.Unprotect();
        FASTER::core::Thread::release_id();
        assert(result == FASTER::core::Status::Ok);
    }

    ~NativeDeviceImpl() override {
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

    void CreateDir(const std::string& dir) override {
        std::experimental::filesystem::path path{ dir };
        try {
            std::experimental::filesystem::remove_all(path);
        }
        catch (std::experimental::filesystem::filesystem_error&) {
            // Ignore; throws when path doesn't exist yet.
        }
        std::experimental::filesystem::create_directories(path);
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

private:
    FASTER::core::LightEpoch epoch_;
    handler_t handler_;
    FASTER::environment::FileOptions default_file_options_;

    /// Store the data
    log_file_t log_;
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
