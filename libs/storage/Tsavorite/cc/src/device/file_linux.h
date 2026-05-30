// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>
#include <fcntl.h>
#include <libaio.h>
#include "libaio_compat.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef FASTER_URING
#include <liburing.h>
#endif

#include "async.h"
#include "status.h"
#include "file_common.h"

namespace FASTER {
namespace environment {

constexpr const char* kPathSeparator = "/";

/// The File class encapsulates the OS file handle.
class File {
 protected:
  File()
    : fd_{ -1 }
    , device_alignment_{ 0 }
    , filename_{}
    , owner_{ false }
#ifdef IO_STATISTICS
    , bytes_written_ { 0 }
    , read_count_{ 0 }
    , bytes_read_{ 0 }
#endif
  {
  }

  File(const std::string& filename)
    : fd_{ -1 }
    , device_alignment_{ 0 }
    , filename_{ filename }
    , owner_{ false }
#ifdef IO_STATISTICS
    , bytes_written_ { 0 }
    , read_count_{ 0 }
    , bytes_read_{ 0 }
#endif
  {
  }

  ~File() {
    if(owner_) {
      core::Status s = Close();
    }
  }

  File(const File&) = delete;
  File &operator=(const File&) = delete;

  /// Move constructor.
  File(File&& other)
    : fd_{ other.fd_ }
    , device_alignment_{ other.device_alignment_ }
    , filename_{ std::move(other.filename_) }
    , owner_{ other.owner_ }
#ifdef IO_STATISTICS
    , bytes_written_ { other.bytes_written_ }
    , read_count_{ other.read_count_ }
    , bytes_read_{ other.bytes_read_ }
#endif
  {
    other.owner_ = false;
  }

  /// Move assignment operator.
  File& operator=(File&& other) {
    fd_ = other.fd_;
    device_alignment_ = other.device_alignment_;
    filename_ = std::move(other.filename_);
    owner_ = other.owner_;
#ifdef IO_STATISTICS
    bytes_written_ = other.bytes_written_;
    read_count_ = other.read_count_;
    bytes_read_ = other.bytes_read_;
#endif
    other.owner_ = false;
    return *this;
  }

 protected:
  core::Status Open(int flags, FileCreateDisposition create_disposition, bool* exists = nullptr);

 public:
  core::Status Close();
  core::Status Delete();

  uint64_t size() const {
    struct stat stat_buffer;
    int result = ::fstat(fd_, &stat_buffer);
    return (result == 0) ? stat_buffer.st_size : 0;
  }

  size_t device_alignment() const {
    return device_alignment_;
  }

  const std::string& filename() const {
    return filename_;
  }

#ifdef IO_STATISTICS
  uint64_t bytes_written() const {
    return bytes_written_.load();
  }
  uint64_t read_count() const {
    return read_count_.load();
  }
  uint64_t bytes_read() const {
    return bytes_read_.load();
  }
#endif

 private:
  core::Status GetDeviceAlignment();
  static int GetCreateDisposition(FileCreateDisposition create_disposition);

 protected:
  int fd_;

 private:
  size_t device_alignment_;
  std::string filename_;
  bool owner_;

#ifdef IO_STATISTICS
 protected:
  std::atomic<uint64_t> bytes_written_;
  std::atomic<uint64_t> read_count_;
  std::atomic<uint64_t> bytes_read_;
#endif
};

class QueueFile;

/// Handles libaio submission and completion via a single io_context. The sharded API
/// surface (TryCompleteFor / QueueRunFor / num_contexts / 2-arg ctor) is preserved for
/// symmetry with UringIoHandler so the shared C ABI and templated NativeDeviceImpl can
/// treat both backends uniformly; on the libaio path those methods operate on the single
/// io_context and num_contexts() always returns 1.
class QueueIoHandler {
 public:
  typedef QueueFile async_file_t;

 private:
  constexpr static int kMaxEvents = 128;

 public:
  QueueIoHandler()
    : io_object_{ 0 }
    , init_errno_{ 0 }
    , wake_fd_{ -1 } {
  }
  /// Construct + io_setup. On failure, io_object_ stays 0 and init_errno_ holds the positive
  /// errno value from io_setup so the caller can format an actionable error.
  QueueIoHandler(size_t /*max_threads*/)
    : io_object_{ 0 }
    , init_errno_{ 0 }
    , wake_fd_{ -1 } {
    int result = ::io_setup(kMaxEvents, &io_object_);
    if (result < 0) {
      init_errno_ = -result;
      io_object_ = 0;
    } else {
      OpenWakeFd();
    }
  }
  /// Cross-backend 2-arg overload; `num_contexts` is ignored (libaio uses a single io_context).
  QueueIoHandler(size_t /*max_threads*/, int /*num_contexts*/)
    : io_object_{ 0 }
    , init_errno_{ 0 }
    , wake_fd_{ -1 } {
    int result = ::io_setup(kMaxEvents, &io_object_);
    if (result < 0) {
      init_errno_ = -result;
      io_object_ = 0;
    } else {
      OpenWakeFd();
    }
  }

  /// Move constructor
  QueueIoHandler(QueueIoHandler&& other)
    : io_object_{ other.io_object_ }
    , init_errno_{ other.init_errno_ }
    , wake_fd_{ other.wake_fd_ } {
    other.io_object_ = 0;
    other.init_errno_ = 0;
    other.wake_fd_ = -1;
  }

  ~QueueIoHandler() {
    io_context_t io_object = io_object_;
    io_object_ = 0;
    if (io_object != 0)
      ::io_destroy(io_object);
    if (wake_fd_ >= 0) {
      ::close(wake_fd_);
      wake_fd_ = -1;
    }
  }

  /// Non-zero iff io_setup failed during construction. The value is the positive errno.
  int init_errno() const { return init_errno_; }
  bool initialized() const { return io_object_ != 0; }

  /// Number of io_context shards. Always 1 for libaio.
  int num_contexts() const { return 1; }

  /// Always returns the single io_context.
  io_context_t pick_context() { return io_object_; }

  /// Invoked whenever a Linux AIO completes.
  static void IoCompletionCallback(io_context_t ctx, struct iocb* iocb, long res, long res2);

  struct IoCallbackContext {
    IoCallbackContext(FileOperationType operation, int fd, size_t offset, uint32_t length,
                      uint8_t* buffer, core::IAsyncContext* context_, core::AsyncIOCallback callback_)
      : caller_context{ context_ }
      , callback{ callback_ } {
      if(FileOperationType::Read == operation) {
        ::io_prep_pread(&this->parent_iocb, fd, buffer, length, offset);
      } else {
        ::io_prep_pwrite(&this->parent_iocb, fd, buffer, length, offset);
      }
      ::io_set_callback(&this->parent_iocb, IoCompletionCallback);
    }

    // WARNING: "parent_iocb" must be the first field in AioCallbackContext. This class is a C-style
    // subclass of "struct iocb".

    /// The iocb structure for Linux AIO.
    struct iocb parent_iocb;

    /// Caller callback context.
    core::IAsyncContext* caller_context;

    /// The caller's asynchronous callback function
    core::AsyncIOCallback callback;
  };

  inline io_context_t io_object() const {
    return io_object_;
  }

  /// Try to execute the next IO completion on the queue, if any.
  bool TryComplete();
  /// Try to execute the next IO completion on context `idx`. Returns false if idx != 0.
  bool TryCompleteFor(int idx) { return idx == 0 ? TryComplete() : false; }

  // Process IO completions on queue with timeout
  int QueueRun(int timeout_secs);
  /// Process IO completions on context `idx` only. Returns -1 if idx != 0.
  int QueueRunFor(int idx, int timeout_secs) { return idx == 0 ? QueueRun(timeout_secs) : -1; }
  /// Submit a no-op IO that completes immediately so any thread blocked in
  /// io_getevents wakes up promptly. Used by Dispose() to unblock the completion
  /// drainer without waiting on the QueueRun timeout. Returns 0 on success, -1 on failure.
  int Wake(int idx);

 private:
  /// Open the wake-up file descriptor (/dev/null) used by Wake() to submit a no-op
  /// 0-byte read iocb. /dev/null reads always return 0 immediately and do not require
  /// O_DIRECT alignment, so this is the simplest fd to use as a wake-up target without
  /// interfering with the real segment files.
  void OpenWakeFd() {
    wake_fd_ = ::open("/dev/null", O_RDONLY);
  }

  /// The Linux AIO context used for IO completions.
  io_context_t io_object_;
  /// If non-zero, the positive errno from a failed io_setup() in the constructor. Checked by
  /// NativeDeviceImpl::Init() to surface an actionable error to the managed caller.
  int init_errno_;
  /// /dev/null fd used by Wake() to submit a no-op 0-byte read. -1 if not opened.
  int wake_fd_;
};

/// The QueueFile class encapsulates asynchronous reads and writes, using the specified AIO
/// context.
class QueueFile : public File {
 public:
  QueueFile()
    : File()
    , io_object_{ nullptr } {
  }
  QueueFile(const std::string& filename)
    : File(filename)
    , io_object_{ nullptr } {
  }
  /// Move constructor
  QueueFile(QueueFile&& other)
    : File(std::move(other))
    , io_object_{ other.io_object_ } {
  }
  /// Move assignment operator.
  QueueFile& operator=(QueueFile&& other) {
    File::operator=(std::move(other));
    io_object_ = other.io_object_;
    return *this;
  }

  core::Status Open(FileCreateDisposition create_disposition, const FileOptions& options,
              QueueIoHandler* handler, bool* exists = nullptr);

  core::Status Read(size_t offset, uint32_t length, uint8_t* buffer,
                    core::IAsyncContext& context, core::AsyncIOCallback callback) const;
  core::Status Write(size_t offset, uint32_t length, const uint8_t* buffer,
                     core::IAsyncContext& context, core::AsyncIOCallback callback);

 private:
  core::Status ScheduleOperation(FileOperationType operationType, uint8_t* buffer, size_t offset,
                           uint32_t length, core::IAsyncContext& context, core::AsyncIOCallback callback);

  io_context_t io_object_;
};

#ifdef FASTER_URING

// CPU pause/yield hint for SpinLock backoff. Emits PAUSE on x86, YIELD on aarch64,
// and a compiler barrier on other architectures.
inline void uring_cpu_relax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__) || defined(__arm__)
    asm volatile("yield" ::: "memory");
#else
    asm volatile("" ::: "memory");
#endif
}

class alignas(64) SpinLock {
public:
    SpinLock(): locked_(false) {}

    void Acquire() noexcept {
        for (;;) {
            if (!locked_.exchange(true, std::memory_order_acquire)) {
                return;
            }

            while (locked_.load(std::memory_order_relaxed)) {
                uring_cpu_relax();
            }
        }
    }

    void Release() noexcept {
        locked_.store(false, std::memory_order_release);
    }
private:
    std::atomic_bool locked_;
};

class UringFile;

/// Handles uring submission and completion across N independent io_uring instances.
/// pick_ring() distributes submissions via atomic round-robin. Each ring has its own
/// SQ spinlock (serializes get_sqe + prep + submit) and CQ spinlock (serializes
/// peek + cqe_seen). With one drainer thread per ring (the documented usage), CQ
/// contention is zero; the CQ lock guards against the legacy back-compat scanner
/// (TryComplete / QueueRun) racing with the dedicated drainer.
class UringIoHandler {
 public:
  typedef UringFile async_file_t;

 private:
  constexpr static int kMaxEvents = 128;

 public:
  UringIoHandler()
    : init_errno_{ 0 } {
    Init(1);
  }

  UringIoHandler(size_t /*max_threads*/)
    : init_errno_{ 0 } {
    Init(1);
  }

  /// Creates `num_rings` independent io_urings. With N>1 submissions are distributed across
  /// rings via pick_ring(); each ring requires its own completion drainer (see QueueRunFor).
  UringIoHandler(size_t /*max_threads*/, int num_rings)
    : init_errno_{ 0 } {
    Init(num_rings < 1 ? 1 : num_rings);
  }

  /// Move constructor
  UringIoHandler(UringIoHandler&& other)
    : rings_{ std::move(other.rings_) }
    , sq_locks_{ std::move(other.sq_locks_) }
    , cq_locks_{ std::move(other.cq_locks_) }
    , init_errno_{ other.init_errno_ } {
    other.rings_.clear();
    other.sq_locks_.clear();
    other.cq_locks_.clear();
    other.init_errno_ = 0;
  }

  UringIoHandler(const UringIoHandler&) = delete;
  UringIoHandler& operator=(const UringIoHandler&) = delete;
  UringIoHandler& operator=(UringIoHandler&&) = delete;

  ~UringIoHandler() {
    for (auto* r : rings_) {
      if (r != nullptr) {
        io_uring_queue_exit(r);
        delete r;
      }
    }
    rings_.clear();
    for (auto* l : sq_locks_) delete l;
    sq_locks_.clear();
    for (auto* l : cq_locks_) delete l;
    cq_locks_.clear();
  }

  /// Non-zero iff io_uring_queue_init failed during construction. The value is the positive errno.
  int init_errno() const { return init_errno_; }
  bool initialized() const { return !rings_.empty() && rings_[0] != nullptr; }

  /// Number of io_uring shards. >= 1 once initialized.
  int num_contexts() const { return static_cast<int>(rings_.size()); }

  /// Pick a (ring, sq_lock) pair for the next submission via per-thread affinity.
  /// Each calling thread is assigned a ring on first call (round-robin against other
  /// callers) and continues to use that same ring for every subsequent submission.
  /// Same-thread submits never contend on sq_lock with each other; different threads
  /// only contend if they got assigned the same ring (only happens when num threads >
  /// num rings). Match libaio's no-user-lock behavior when num_rings >= num_submitters.
  void pick_ring(struct io_uring*& ring_out, SpinLock*& lock_out) {
    if (rings_.size() == 1) {
      ring_out = rings_[0];
      lock_out = sq_locks_[0];
      return;
    }
    thread_local int my_ring_idx = -1;
    if (my_ring_idx < 0) {
      my_ring_idx = static_cast<int>(
          submit_counter_.fetch_add(1, std::memory_order_relaxed) % rings_.size());
    }
    ring_out = rings_[my_ring_idx];
    lock_out = sq_locks_[my_ring_idx];
  }

  struct IoCallbackContext {
    IoCallbackContext(bool is_read, int fd, uint8_t* buffer, size_t length, size_t offset, core::IAsyncContext* context_, core::AsyncIOCallback callback_)
      : is_read_(is_read)
      , fd_(fd)
      , vec_{buffer, length}
      , offset_(offset)
      , caller_context{ context_ }
      , callback{ callback_ } {}

    bool is_read_;

    int fd_;
    struct iovec vec_;
    size_t offset_;

    /// Caller callback context.
    core::IAsyncContext* caller_context;

    /// The caller's asynchronous callback function
    core::AsyncIOCallback callback;
  };

  /// Drain one completion from ring 0; sharded callers should use TryCompleteFor(idx).
  bool TryComplete();
  /// Drain one completion from ring `idx`.
  bool TryCompleteFor(int idx);
  /// Drain completions across all rings (back-compat for callers that do not know about sharding).
  int QueueRun(int timeout_secs);
  /// Drain completions on ring `idx` only.
  int QueueRunFor(int idx, int timeout_secs);
  /// Submit a no-op SQE to ring `idx` so any thread blocked in io_uring_wait_cqe_timeout wakes
  /// up. Used by Dispose() to unblock the completion drainer without waiting on the timeout.
  /// The CQE is dispatched with a sentinel context that the dispatcher skips. Returns 0 on
  /// success, -1 on failure.
  int Wake(int idx);

private:
  void Init(int num_rings) {
    struct RingDeleter {
      void operator()(struct io_uring* r) const noexcept {
        if (r != nullptr) {
          io_uring_queue_exit(r);
          delete r;
        }
      }
    };
    using RingPtr = std::unique_ptr<struct io_uring, RingDeleter>;

    std::vector<RingPtr> rings;
    std::vector<std::unique_ptr<SpinLock>> sq_locks;
    std::vector<std::unique_ptr<SpinLock>> cq_locks;
    rings.reserve(num_rings);
    sq_locks.reserve(num_rings);
    cq_locks.reserve(num_rings);

    for (int i = 0; i < num_rings; ++i) {
      auto raw_ring = new struct io_uring();
      int ret = io_uring_queue_init(kMaxEvents, raw_ring, 0);
      if (ret != 0) {
        init_errno_ = -ret;
        delete raw_ring;
        return;
      }
      rings.emplace_back(raw_ring);
      sq_locks.emplace_back(std::make_unique<SpinLock>());
      cq_locks.emplace_back(std::make_unique<SpinLock>());
    }

    rings_.reserve(num_rings);
    sq_locks_.reserve(num_rings);
    cq_locks_.reserve(num_rings);
    for (int i = 0; i < num_rings; ++i) {
      rings_.push_back(rings[i].release());
      sq_locks_.push_back(sq_locks[i].release());
      cq_locks_.push_back(cq_locks[i].release());
    }
  }

  /// The io_urings for all the I/Os. Size == num_contexts(). All entries non-null once initialized.
  std::vector<struct io_uring*> rings_;
  /// Per-ring SQ spinlocks. Owned (delete in dtor).
  std::vector<SpinLock*> sq_locks_;
  /// Per-ring CQ spinlock. Serialises io_uring_peek_cqe + io_uring_cqe_seen so the
  /// dedicated drainer for ring `i` cannot race with the legacy all-rings scanner
  /// (TryComplete / QueueRun) on the same ring.
  std::vector<SpinLock*> cq_locks_;
  /// Round-robin submit counter; only consulted when rings_.size() > 1.
  std::atomic<uint64_t> submit_counter_{ 0 };
  /// If non-zero, the positive errno from a failed io_uring_queue_init() in the constructor.
  int init_errno_;
};

/// Encapsulates async reads and writes. Holds a UringIoHandler* and picks a (ring,
/// sq_lock) pair per submission via the handler's atomic round-robin.
class UringFile : public File {
 public:
  UringFile()
    : File()
    , handler_{ nullptr } {
  }
  UringFile(const std::string& filename)
    : File(filename)
    , handler_{ nullptr } {
  }
  /// Move constructor
  UringFile(UringFile&& other)
    : File(std::move(other))
    , handler_{ other.handler_ } {
  }
  /// Move assignment operator.
  UringFile& operator=(UringFile&& other) {
    File::operator=(std::move(other));
    handler_ = other.handler_;
    return *this;
  }

  core::Status Open(FileCreateDisposition create_disposition, const FileOptions& options,
              UringIoHandler* handler, bool* exists = nullptr);

  core::Status Read(size_t offset, uint32_t length, uint8_t* buffer,
              core::IAsyncContext& context, core::AsyncIOCallback callback) const;
  core::Status Write(size_t offset, uint32_t length, const uint8_t* buffer,
               core::IAsyncContext& context, core::AsyncIOCallback callback);

 private:
  core::Status ScheduleOperation(FileOperationType operationType, uint8_t* buffer, size_t offset,
                                 uint32_t length, core::IAsyncContext& context, core::AsyncIOCallback callback);

  UringIoHandler* handler_;
};

#endif

}
} // namespace FASTER::environment
