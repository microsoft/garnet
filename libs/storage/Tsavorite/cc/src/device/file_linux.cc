// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include <algorithm>
#include <cstring>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <linux/fs.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <stdio.h>
#include <time.h>
#include "file_linux.h"
#include "native_device_error.h"

namespace FASTER {
namespace environment {

using namespace FASTER::core;

#ifdef _DEBUG
#define DCHECK_ALIGNMENT(o, l, b) \
do { \
  assert(reinterpret_cast<uintptr_t>(b) % device_alignment() == 0); \
  assert((o) % device_alignment() == 0); \
  assert((l) % device_alignment() == 0); \
} while (0)
#else
#define DCHECK_ALIGNMENT(o, l, b) do {} while(0)
#endif

Status File::Open(int flags, FileCreateDisposition create_disposition, bool* exists) {
  if(exists) {
    *exists = false;
  }

  int create_flags = GetCreateDisposition(create_disposition);

  // Capture file-existed state BEFORE the open() call. Probing errno after a successful
  // open() is undefined behavior (POSIX: errno is unspecified on success), and the prior
  // implementation racing through `errno == EEXIST` after a SUCCESSFUL open with O_CREAT
  // would routinely return stale values from earlier failed syscalls — falsely reporting
  // "exists" when the file had just been created. We deliberately accept a tiny TOCTOU
  // window between stat() and open(): if another process is concurrently creating this
  // file the `exists` flag is informational only and the device handles either outcome.
  bool file_existed_before_open = false;
  if (exists != nullptr) {
    struct stat st;
    file_existed_before_open = (::stat(filename_.c_str(), &st) == 0);
  }

  // OpenExisting on a missing file is a documented non-error: callers iterate the segment
  // file list and expect "missing" to be reported via *exists=false + Status::Ok (matches the
  // Windows path at file_windows.cc:45-49). Short-circuit to avoid an open() that would just
  // fail with ENOENT.
  if (exists != nullptr && create_disposition == FileCreateDisposition::OpenExisting && !file_existed_before_open) {
    *exists = false;
    return Status::Ok;
  }

  /// Always unbuffered (O_DIRECT).
  fd_ = ::open(filename_.c_str(), flags | O_RDWR | create_flags, S_IRUSR | S_IWUSR);

  if(fd_ == -1) {
    int saved_errno = errno;
    native_device::set_last_error(
        "open('%s') failed: %d (%s). %s",
        filename_.c_str(), saved_errno, std::strerror(saved_errno),
        saved_errno == EACCES ? "Check directory and file permissions." :
        saved_errno == ENOSPC ? "Disk is full." :
        saved_errno == EINVAL && (flags & O_DIRECT) ? "Filesystem may not support O_DIRECT — try ext4/xfs, or pass disableFileBuffering=false." :
        "");
    return Status::IOError;
  }

  if (exists != nullptr) {
    *exists = file_existed_before_open;
  }

  Status result = GetDeviceAlignment();
  if(result != Status::Ok) {
    Close();
    return result;
  }
  owner_ = true;
  return result;
}

Status File::Close() {
  if(fd_ != -1) {
    int result = ::close(fd_);
    fd_ = -1;
    if(result == -1) {
      int error = errno;
      return Status::IOError;
    }
  }
  owner_ = false;
  return Status::Ok;
}

Status File::Delete() {
  int result = ::remove(filename_.c_str());
  if(result == -1) {
    int error = errno;
    return Status::IOError;
  }
  return Status::Ok;
}

Status File::GetDeviceAlignment() {
  // Discovers the kernel's authoritative direct-I/O alignment requirement for this file and
  // records it in device_alignment_. The upper layer (StorageDeviceBase.SectorSize) is told the
  // SAME value at construction time by NativeStorageDevice via NativeDevice_ProbeAlignment, so
  // every buffer / offset / length flowing into ReadAsync / WriteAsync is already pre-aligned
  // by the time it gets here.
  //
  // Probe order:
  //   (1) Linux 6.1+ : statx(STATX_DIOALIGN) — reports the file's actual required DIO offset
  //       and memory alignment. Authoritative when non-zero.
  //   (2) Default : 512 — historical Garnet assumption.
  //
  // On pre-6.1 kernels (or filesystems that don't yet fill statx_dio_*_align in), we assume 512
  // and rely on the kernel to surface EINVAL via the completion callback if the actual disk
  // requires more — the same behavior every other Garnet Linux device has had for years. We
  // deliberately do NOT fall back to statvfs.f_frsize as a hint: f_frsize is the filesystem's
  // allocation unit (often 4096 on ext4) which has no necessary relationship to the DIO
  // alignment requirement (512 on a 512n disk regardless of f_frsize), and using it as a hint
  // over-reports alignment and causes the cross-check in C# Initialize to throw spuriously on
  // perfectly good 512n hardware.
  //
  // Hot-path cost: zero. This runs once per file open (cold path).
  //
  // If the upper-layer probe disagrees with what we discover here (e.g., upper layer said 512
  // but the kernel demands 4096), the C# NativeStorageDevice.Initialize cross-checks and
  // throws with a clear message. We never silently accept a mismatch — that would surface as
  // cryptic per-IO EINVAL after the cluster is serving traffic, or worse, as silent
  // corruption if the kernel coerced offsets.
  device_alignment_ = 512;

#if defined(__linux__) && defined(STATX_DIOALIGN)
  struct statx stx{};
  if (::statx(fd_, "", AT_EMPTY_PATH, STATX_DIOALIGN, &stx) == 0) {
    uint32_t required = std::max(stx.stx_dio_offset_align, stx.stx_dio_mem_align);
    if (required != 0) {
      // Round up to a power of two — the upper layer's bit-mask arithmetic assumes pow2.
      // In practice the kernel always reports a power of two, but be defensive.
      uint32_t pow2 = 512;
      while (pow2 < required) pow2 <<= 1;
      device_alignment_ = std::max<size_t>(device_alignment_, pow2);
    }
  }
#endif

  return Status::Ok;
}

int File::GetCreateDisposition(FileCreateDisposition create_disposition) {
  switch(create_disposition) {
  case FileCreateDisposition::CreateOrTruncate:
    return O_CREAT | O_TRUNC;
  case FileCreateDisposition::OpenOrCreate:
    return O_CREAT;
  case FileCreateDisposition::OpenExisting:
    return 0;
  default:
    assert(false);
    return 0; // not reached
  }
}

void QueueIoHandler::IoCompletionCallback(io_context_t ctx, struct iocb* iocb, long res,
    long res2) {
  auto callback_context = core::make_context_unique_ptr<IoCallbackContext>(
                            reinterpret_cast<IoCallbackContext*>(iocb));
  size_t bytes_transferred;
  Status return_status;
  if(res < 0) {
    return_status = Status::IOError;
    bytes_transferred = 0;
  } else {
    return_status = Status::Ok;
    bytes_transferred = res;
  }
  callback_context->callback(callback_context->caller_context, return_status, bytes_transferred);
}

bool QueueIoHandler::TryComplete() {
  struct timespec timeout;
  std::memset(&timeout, 0, sizeof(timeout));
  struct io_event events[1];
  int result = ::io_getevents(io_object_, 1, 1, events, &timeout);
  if(result == 1) {
    io_callback_t callback = reinterpret_cast<io_callback_t>(events[0].data);
    callback(io_object_, events[0].obj, events[0].res, events[0].res2);
    return true;
  } else {
    return false;
  }
}

#define IO_BATCH_EVENTS	8		/* number of events to batch up */

int QueueIoHandler::QueueRun(int timeout_secs) {
    struct timespec timeout;
    timeout.tv_sec = timeout_secs;
    timeout.tv_nsec = 0;
    struct io_event events[IO_BATCH_EVENTS];
    struct io_event* ep;

    int ret = 0;		/* total number of events processed */
    int n;

    /*
     * Process io events and call the callbacks.
     * Try to batch the events up to IO_BATCH_EVENTS at a time.
     * Loop until we have read all the available events and called the callbacks.
     */
    do {
        int i;
        if ((n = ::io_getevents(io_object_, 1, IO_BATCH_EVENTS, events, &timeout)) <= 0)
            break;
        ret += n;
        for (ep = events, i = n; i-- > 0; ep++) {
            io_callback_t callback = reinterpret_cast<io_callback_t>(ep->data);
            callback(io_object_, ep->obj, ep->res, ep->res2);
        }
    } while (n == IO_BATCH_EVENTS);

    return ret ? ret : n;
}

Status QueueFile::Open(FileCreateDisposition create_disposition, const FileOptions& options,
                       QueueIoHandler* handler, bool* exists) {
  int flags = 0;
  if(options.unbuffered) {
    flags |= O_DIRECT;
  }
  RETURN_NOT_OK(File::Open(flags, create_disposition, exists));
  if(exists && !*exists) {
    return Status::Ok;
  }

  io_object_ = handler->io_object();
  return Status::Ok;
}

Status QueueFile::Read(size_t offset, uint32_t length, uint8_t* buffer,
                       IAsyncContext& context, AsyncIOCallback callback) const {
  DCHECK_ALIGNMENT(offset, length, buffer);
#ifdef IO_STATISTICS
  ++read_count_;
  bytes_read_ += length;
#endif
  return const_cast<QueueFile*>(this)->ScheduleOperation(FileOperationType::Read, buffer,
         offset, length, context, callback);
}

Status QueueFile::Write(size_t offset, uint32_t length, const uint8_t* buffer,
                        IAsyncContext& context, AsyncIOCallback callback) {
  DCHECK_ALIGNMENT(offset, length, buffer);
#ifdef IO_STATISTICS
  bytes_written_ += length;
#endif
  return ScheduleOperation(FileOperationType::Write, const_cast<uint8_t*>(buffer), offset, length,
                           context, callback);
}

Status QueueFile::ScheduleOperation(FileOperationType operationType, uint8_t* buffer,
                                    size_t offset, uint32_t length, IAsyncContext& context,
                                    AsyncIOCallback callback) {
  auto io_context = core::alloc_context<QueueIoHandler::IoCallbackContext>(sizeof(
                      QueueIoHandler::IoCallbackContext));
  if(!io_context.get()) return Status::OutOfMemory;

  IAsyncContext* caller_context_copy;
  RETURN_NOT_OK(context.DeepCopy(caller_context_copy));
  // RAII guard for the deep-copied caller context. Until ownership transfers to the iocb that
  // the kernel has accepted (i.e., io_submit returns 1), we must free this LSS slot on every
  // failure path or it leaks. Cold-path-friendly: stack-allocated, zero-size deleter (EBO),
  // single conditional null-check at scope exit. The deleter invokes IAsyncContext's virtual
  // dtor (correctly dispatching to the concrete derived type) before returning the LSS slot.
  auto caller_copy_guard = core::make_context_unique_ptr<IAsyncContext>(caller_context_copy);

  new(io_context.get()) QueueIoHandler::IoCallbackContext(operationType, fd_, offset, length,
      buffer, caller_context_copy, callback);

  struct iocb* iocbs[1];
  iocbs[0] = reinterpret_cast<struct iocb*>(io_context.get());

  // INVARIANT: We prepare exactly ONE iocb per call. libaio's io_submit returns the number
  // of iocbs successfully queued; for N_prepared = 1 the return is either 1 (kernel accepted —
  // will produce a completion event we own) or any other value (kernel did NOT accept — we
  // still own io_context and caller_context_copy, the RAII guards free both). Do NOT batch
  // multiple iocbs without revisiting ownership: partial submission with N_prepared > 1
  // creates a UAF window (some accepted, some not, but only the failing one needs cleanup).
  int result = ::io_submit(io_object_, 1, iocbs);
  if(result != 1) {
    // io_context's deleter calls ~IoCallbackContext() which is trivial, then frees the LSS
    // slot. caller_copy_guard releases the caller's deep-copy. No leak.
    return Status::IOError;
  }

  // Ownership transferred to the kernel; release both RAII guards so they don't double-free
  // when the completion path destroys them.
  caller_copy_guard.release();
  io_context.release();
  return Status::Ok;
}

#ifdef FASTER_URING

namespace {

// Report a completed CQE to the caller. Negative CQE results are surfaced as
// IOError (matching the libaio path in QueueIoHandler::IoCompletionCallback);
// non-negative results carry the bytes-transferred count. Permanent errors
// (-EINVAL/-EBADF/-EIO/-ENOSPC) MUST NOT be retried here — the C# wrapper
// tracks each submitted op via numPending and relies on every submission
// producing exactly one callback to balance that counter.
inline void DispatchUringCqe(int io_res, UringIoHandler::IoCallbackContext* context) {
    core::Status return_status;
    size_t bytes_transferred;
    if (io_res < 0) {
        return_status = core::Status::IOError;
        bytes_transferred = 0;
    } else {
        return_status = core::Status::Ok;
        bytes_transferred = static_cast<size_t>(io_res);
    }
    context->callback(context->caller_context, return_status, bytes_transferred);
    lss_allocator.Free(context);
}

} // anonymous namespace

bool UringIoHandler::TryComplete() {
  // Back-compat: scan all rings.
  bool any = false;
  for (int i = 0; i < num_contexts(); ++i) {
    if (TryCompleteFor(i)) any = true;
  }
  return any;
}

bool UringIoHandler::TryCompleteFor(int idx) {
  if (idx < 0 || idx >= static_cast<int>(rings_.size())) return false;
  struct io_uring* ring = rings_[idx];
  if (ring == nullptr) return false;
  // Per-ring CQ lock: required because the legacy TryComplete() / QueueRun() back-compat
  // paths scan all rings and can race with this dedicated drainer. Without the lock both
  // sides peek the same CQE, both call io_uring_cqe_seen, both dispatch the callback →
  // double-free.
  SpinLock* cq_lock = cq_locks_[idx];
  struct io_uring_cqe* cqe = nullptr;
  cq_lock->Acquire();
  int res = io_uring_peek_cqe(ring, &cqe);
  if (res == 0 && cqe) {
    int io_res = cqe->res;
    auto* context = reinterpret_cast<UringIoHandler::IoCallbackContext*>(io_uring_cqe_get_data(cqe));
    io_uring_cqe_seen(ring, cqe);
    cq_lock->Release();
    DispatchUringCqe(io_res, context);
    return true;
  }
  cq_lock->Release();
  return false;
}

int UringIoHandler::QueueRun(int timeout_secs) {
  // Back-compat: drain all rings. Single-shard (N=1) callers see identical behaviour.
  // First ring gets the full timeout; subsequent rings poll with timeout=0.
  if (rings_.empty()) return 0;
  int total = 0;
  int first = QueueRunFor(0, timeout_secs);
  if (first > 0) total += first;
  for (int i = 1; i < static_cast<int>(rings_.size()); ++i) {
    int n = QueueRunFor(i, 0);
    if (n > 0) total += n;
  }
  return total > 0 ? total : first;
}

int UringIoHandler::QueueRunFor(int idx, int timeout_secs) {
    // Blocking drain for one ring. The wait phase is held without locks (multiple threads
    // can block in io_uring_wait_cqe_timeout concurrently — kernel-side they all wake on
    // any CQE), and the peek+seen pair is serialised by cq_lock to prevent the
    // double-dispatch race described above TryCompleteFor.
    if (idx < 0 || idx >= static_cast<int>(rings_.size())) return -1;
    struct io_uring* ring = rings_[idx];
    if (ring == nullptr) return -1;
    SpinLock* cq_lock = cq_locks_[idx];

    int ret = 0;

    // Phase 1: block waiting for at least one CQE, or until timeout. Don't consume.
    if (timeout_secs > 0) {
        struct __kernel_timespec ts;
        ts.tv_sec = timeout_secs;
        ts.tv_nsec = 0;
        struct io_uring_cqe* wait_cqe = nullptr;
        (void)io_uring_wait_cqe_timeout(ring, &wait_cqe, &ts);
    }

    // Phase 2: drain everything currently available, serialised on cq_lock so legacy
    // TryComplete / QueueRun back-compat scans don't race with us.
    for (;;) {
        cq_lock->Acquire();
        struct io_uring_cqe* cqe = nullptr;
        int rc = io_uring_peek_cqe(ring, &cqe);
        if (rc != 0 || cqe == nullptr) {
            cq_lock->Release();
            break;
        }
        int io_res = cqe->res;
        auto* context = reinterpret_cast<UringIoHandler::IoCallbackContext*>(io_uring_cqe_get_data(cqe));
        io_uring_cqe_seen(ring, cqe);
        cq_lock->Release();

        DispatchUringCqe(io_res, context);
        ++ret;
    }

    return ret;
}

Status UringFile::Open(FileCreateDisposition create_disposition, const FileOptions& options,
                       UringIoHandler* handler, bool* exists) {
  int flags = 0;
  if(options.unbuffered) {
    flags |= O_DIRECT;
  }
  RETURN_NOT_OK(File::Open(flags, create_disposition, exists));
  if(exists && !*exists) {
    return Status::Ok;
  }

  handler_ = handler;
  return Status::Ok;
}

Status UringFile::Read(size_t offset, uint32_t length, uint8_t* buffer,
                       IAsyncContext& context, AsyncIOCallback callback) const {
  DCHECK_ALIGNMENT(offset, length, buffer);
#ifdef IO_STATISTICS
  ++read_count_;
  bytes_read_ += length;
#endif
  return const_cast<UringFile*>(this)->ScheduleOperation(FileOperationType::Read, buffer,
         offset, length, context, callback);
}

Status UringFile::Write(size_t offset, uint32_t length, const uint8_t* buffer,
                        IAsyncContext& context, AsyncIOCallback callback) {
  DCHECK_ALIGNMENT(offset, length, buffer);
#ifdef IO_STATISTICS
  bytes_written_ += length;
#endif
  return ScheduleOperation(FileOperationType::Write, const_cast<uint8_t*>(buffer), offset, length,
                           context, callback);
}

Status UringFile::ScheduleOperation(FileOperationType operationType, uint8_t* buffer,
                                    size_t offset, uint32_t length, IAsyncContext& context,
                                    AsyncIOCallback callback) {
  auto io_context = alloc_context<UringIoHandler::IoCallbackContext>(sizeof(UringIoHandler::IoCallbackContext));
  if (!io_context.get()) return Status::OutOfMemory;

  IAsyncContext* caller_context_copy;
  RETURN_NOT_OK(context.DeepCopy(caller_context_copy));
  // RAII guard: see equivalent comment in QueueFile::ScheduleOperation. Frees caller_context_copy
  // on every failure path between here and a successful io_uring_submit().
  auto caller_copy_guard = core::make_context_unique_ptr<IAsyncContext>(caller_context_copy);

  bool is_read = operationType == FileOperationType::Read;
  new(io_context.get()) UringIoHandler::IoCallbackContext(is_read, fd_, buffer, length, offset, caller_context_copy, callback);

  // Pick a (ring, sq_lock) pair for this submission. Under N=1, identical to the prior
  // single-ring path. Under N>1, each ScheduleOperation may land on a different ring.
  struct io_uring* ring = nullptr;
  SpinLock* sq_lock = nullptr;
  handler_->pick_ring(ring, sq_lock);

  sq_lock->Acquire();
  struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
  if (sqe == nullptr) {
    // SQ ring is full; io_uring_get_sqe returned nullptr without producing a CQE. The previous
    // code asserted here, which silently no-ops in Release and leaves the SQ ring untouched but
    // never invokes the user callback — desynchronizing the C# numPending counter and ultimately
    // hanging Dispose's drain loop. Fail the submission cleanly; the RAII guards above will
    // release io_context (LSS slot) and caller_context_copy (deep-copy + LSS slot).
    sq_lock->Release();
    return Status::IOError;
  }

  if (is_read) {
    io_uring_prep_readv(sqe, fd_, &io_context->vec_, 1, offset);
    //io_uring_prep_read(sqe, fd_, buffer, length, offset);
  } else {
    io_uring_prep_writev(sqe, fd_, &io_context->vec_, 1, offset);
    //io_uring_prep_write(sqe, fd_, buffer, length, offset);
  }
  io_uring_sqe_set_data(sqe, io_context.get());

  // INVARIANT (single-SQE submission): We hold sq_lock across io_uring_get_sqe + io_uring_submit
  // and prepare exactly ONE SQE per call. liburing's io_uring_submit returns the number of SQEs
  // it submitted to the kernel; with N_prepared = 1 the return value is either:
  //   res == 1  → kernel accepted the SQE; it will eventually produce exactly one CQE we own.
  //   res != 1  → kernel did NOT accept the SQE (or accepted it without queueing); no CQE will
  //               be produced. We still own io_context and caller_context_copy; both RAII guards
  //               will release them.
  // Do NOT switch to batched submission (N_prepared > 1) without revisiting the ownership model:
  // partial submission would require per-SQE accounting to know which contexts the kernel kept
  // and which we must free.
  int res = io_uring_submit(ring);
  sq_lock->Release();
  if (res != 1) {
    return Status::IOError;
  }

  // Kernel now owns both io_context and caller_context_copy; release the guards so completion
  // path destroys them (via DispatchUringCqe → lss_allocator.Free, and the inner
  // ~IoCallbackContext eventually).
  caller_copy_guard.release();
  io_context.release();
  return Status::Ok;
}

#endif

#undef DCHECK_ALIGNMENT

}
} // namespace FASTER::environment
