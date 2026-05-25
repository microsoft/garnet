// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>
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

/// The QueueIoHandler class encapsulates completions for async file I/O, where the completions
/// are put on the AIO completion queue. Always uses a single libaio io_context — sharding
/// across multiple contexts was tested empirically (Device.benchmark random-read sweep, 1GB
/// file, 4K sectors, NVMe) and yielded no throughput improvement; in fact CT=4/CT=8 came in
/// 3-4% lower than CT=1 (extra atomic counter + cache traffic on shared iocb pool outweighed
/// any kernel-side benefit, because libaio's io_submit is already efficient per io_context).
/// The sharded API surface (TryCompleteFor / QueueRunFor / num_contexts / 2-arg ctor) is
/// preserved so the shared C ABI and templated NativeDeviceImpl don't need a separate code
/// path for libaio vs uring; but for libaio they always operate on the single io_context.
/// The C# wrapper queries NumIoContexts after init and spawns that many completion threads,
/// so passing --device-completion-threads N with libaio is equivalent to --device-completion-threads 1.
class QueueIoHandler {
 public:
  typedef QueueFile async_file_t;

 private:
  constexpr static int kMaxEvents = 128;

 public:
  QueueIoHandler()
    : io_object_{ 0 }
    , init_errno_{ 0 } {
  }
  /// Construct + io_setup. On failure, io_object_ stays 0 and init_errno_ holds the positive
  /// errno value (so the caller can format an actionable error message instead of crashing on
  /// an assert that's stripped in Release builds).
  QueueIoHandler(size_t /*max_threads*/)
    : io_object_{ 0 }
    , init_errno_{ 0 } {
    int result = ::io_setup(kMaxEvents, &io_object_);
    if (result < 0) {
      init_errno_ = -result;
      io_object_ = 0;
    }
  }
  /// 2-arg overload accepted for cross-backend symmetry with UringIoHandler. The
  /// `num_contexts` argument is silently ignored (libaio always uses a single io_context;
  /// see class doc).
  QueueIoHandler(size_t /*max_threads*/, int /*num_contexts*/)
    : io_object_{ 0 }
    , init_errno_{ 0 } {
    int result = ::io_setup(kMaxEvents, &io_object_);
    if (result < 0) {
      init_errno_ = -result;
      io_object_ = 0;
    }
  }

  /// Move constructor
  QueueIoHandler(QueueIoHandler&& other)
    : io_object_{ other.io_object_ }
    , init_errno_{ other.init_errno_ } {
    other.io_object_ = 0;
    other.init_errno_ = 0;
  }

  ~QueueIoHandler() {
    io_context_t io_object = io_object_;
    io_object_ = 0;
    if (io_object != 0)
      ::io_destroy(io_object);
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

 private:
  /// The Linux AIO context used for IO completions.
  io_context_t io_object_;
  /// If non-zero, the positive errno from a failed io_setup() in the constructor. Checked by
  /// NativeDeviceImpl::Init() to surface an actionable error to the managed caller.
  int init_errno_;
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

// Architecture-independent CPU pause/yield hint used by SpinLock spin loops.
// On x86_64 emits PAUSE (rep nop), on aarch64 emits YIELD; elsewhere acts as
// a compiler barrier only. Centralised here so the spinlock and any other
// uring-path spin waits stay portable across the architectures we ship for
// (linux-x64 is the only RID we currently publish a prebuilt for, but the
// source must build cleanly on aarch64 too — Tsavorite's CI matrix exercises
// ARM64 build configs).
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

/// The UringIoHandler class encapsulates completions for async file I/O. Sharded across N
/// independent io_uring instances (default N=1, identical to the unsharded build). Same
/// architecture as QueueIoHandler — pick_ring() returns a (ring, sq_lock) pair via atomic
/// round-robin so concurrent submitters distribute across rings; each completion thread
/// drains exactly one ring via TryCompleteFor/QueueRunFor.
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

  /// Sharded ctor: set up `num_rings` independent io_urings. Empirical sweep (Device.benchmark,
  /// 16GB random reads, NVMe) showed sharding rings is the only way to scale uring past the
  /// per-ring user-space SpinLock cap (single-ring ≤ ~357K regardless of completion-thread
  /// count; 4 rings + 4 drainers hit ~461K). Adding multiple completion drainers to a single
  /// ring REGRESSES throughput due to cq_lock contention — so the model here is strict 1:1
  /// (one drainer per ring, drainers bound exclusively via QueueRunFor(idx)).
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

  /// Pick a (ring, sq_lock) pair for the next submission via atomic round-robin. Returns the
  /// only pair under N=1, identical to the unsharded build.
  void pick_ring(struct io_uring*& ring_out, SpinLock*& lock_out) {
    if (rings_.size() == 1) {
      ring_out = rings_[0];
      lock_out = sq_locks_[0];
      return;
    }
    uint64_t idx = submit_counter_.fetch_add(1, std::memory_order_relaxed) % rings_.size();
    ring_out = rings_[idx];
    lock_out = sq_locks_[idx];
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

  /// Try to execute the next IO completion on the FIRST ring. Back-compat; new code should
  /// prefer TryCompleteFor(idx) which is per-ring.
  bool TryComplete();
  bool TryCompleteFor(int idx);
  /// Drain completions across all rings (back-compat for legacy single-thread drainers).
  int QueueRun(int timeout_secs);
  /// Drain completions on ring `idx` only.
  int QueueRunFor(int idx, int timeout_secs);

private:
  void Init(int num_rings) {
    rings_.assign(static_cast<size_t>(num_rings), nullptr);
    sq_locks_.assign(static_cast<size_t>(num_rings), nullptr);
    cq_locks_.assign(static_cast<size_t>(num_rings), nullptr);
    for (int i = 0; i < num_rings; ++i) {
      rings_[i] = new struct io_uring();
      int ret = io_uring_queue_init(kMaxEvents, rings_[i], 0);
      if (ret != 0) {
        init_errno_ = -ret;
        delete rings_[i];
        rings_[i] = nullptr;
        for (int j = 0; j < i; ++j) {
          if (rings_[j] != nullptr) {
            io_uring_queue_exit(rings_[j]);
            delete rings_[j];
            rings_[j] = nullptr;
          }
        }
        for (auto* l : sq_locks_) delete l;
        sq_locks_.clear();
        for (auto* l : cq_locks_) delete l;
        cq_locks_.clear();
        return;
      }
      sq_locks_[i] = new SpinLock();
      cq_locks_[i] = new SpinLock();
    }
  }

  /// The io_urings for all the I/Os. Size == num_contexts(). All entries non-null once initialized.
  std::vector<struct io_uring*> rings_;
  /// Per-ring SQ spinlocks. Owned (delete in dtor).
  std::vector<SpinLock*> sq_locks_;
  /// Per-ring CQ spinlocks. Required even though the typical caller pattern is
  /// "one dedicated drainer per ring": the legacy NativeDevice_TryComplete /
  /// NativeDevice_QueueRun path scans ALL rings and may run concurrently with the
  /// dedicated drainer, racing on io_uring_peek_cqe + io_uring_cqe_seen. The race
  /// double-dispatches the CQE to the user callback → double-free of the iocb context
  /// → malloc heap corruption. The lock cost is one uncontended acquire per CQE when
  /// each ring has exactly one drainer; when contention occurs (which is the bug case)
  /// the lock serialises the peek+seen pair atomically and prevents the double-dispatch.
  std::vector<SpinLock*> cq_locks_;
  /// Round-robin submit counter; only consulted when rings_.size() > 1.
  std::atomic<uint64_t> submit_counter_{ 0 };
  /// If non-zero, the positive errno from a failed io_uring_queue_init() in the constructor.
  int init_errno_;
};

/// The UringFile class encapsulates asynchronous reads and writes. Holds a UringIoHandler*
/// and calls pick_ring() per ScheduleOperation, picking a (ring, sq_lock) pair via the
/// handler's atomic round-robin. Under N=1 the layout/behaviour is identical to the
/// pre-sharded version (only one ring + one lock to choose from).
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
