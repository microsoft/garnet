// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include <algorithm>
#include <cstring>
#include <type_traits>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <linux/fs.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <sched.h>
#include <stdio.h>
#include <time.h>
#include "file_linux.h"
#include "native_device_error.h"

namespace FASTER {
namespace environment {

namespace {
/// Maximum sched_yield() retries on transient kernel back-pressure during submission
/// (libaio io_submit == 0, uring io_uring_get_sqe == nullptr or io_uring_submit -EAGAIN/-EBUSY).
/// Permanent submission errors are not retried.
constexpr int kMaxSubmitRetries = 8;
} // anonymous namespace

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

  // Probe file existence BEFORE open(); errno is unspecified after a successful open().
  // TOCTOU between stat() and open() is acceptable here: `exists` is informational only.
  bool file_existed_before_open = false;
  if (exists != nullptr) {
    struct stat st;
    file_existed_before_open = (::stat(filename_.c_str(), &st) == 0);
  }

  // OpenExisting on a missing file is a non-error: report via *exists=false + Status::Ok
  // (matches the Windows path).
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
  // Probe the kernel's required direct-I/O alignment for this file and record it in
  // device_alignment_. Uses statx(STATX_DIOALIGN) on Linux 6.1+ when available; falls back
  // to 512 (also the default for pre-6.1 kernels and filesystems that do not populate the
  // statx alignment fields). Mismatches with the upper layer's pre-computed sector size are
  // caught by the C# Initialize cross-check.
  device_alignment_ = 512;

#if defined(__linux__) && defined(STATX_DIOALIGN)
  struct statx stx{};
  if (::statx(fd_, "", AT_EMPTY_PATH, STATX_DIOALIGN, &stx) == 0) {
    uint32_t required = std::max(stx.stx_dio_offset_align, stx.stx_dio_mem_align);
    if (required != 0) {
      // Round up to a power of two (the upper-layer bit-mask arithmetic assumes pow2).
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

namespace {

// No-op io_callback_t used by QueueIoHandler::Wake — frees the heap-allocated iocb that
// was submitted purely to unblock a sleeping io_getevents waiter.
void QueueWakeCompletionCallback(io_context_t, struct iocb* iocb, long /*res*/, long /*res2*/) {
    delete iocb;
}

} // namespace

int QueueIoHandler::Wake(int idx) {
    if (idx != 0) return -1;
    if (io_object_ == 0) return -1;
    if (wake_fd_ < 0) return -1;
    // Submit a 0-byte read on /dev/null. The kernel completes it immediately (reads on
    // /dev/null always return 0 bytes), io_getevents wakes up, dispatches the callback
    // which frees the iocb. We allocate a fresh iocb each call because Wake() runs at
    // most once per Dispose() — the per-allocation cost is negligible vs the ~1s stall
    // it eliminates.
    static thread_local char dummy_buf[8] alignas(8) = {};
    struct iocb* wake_iocb = new struct iocb();
    ::io_prep_pread(wake_iocb, wake_fd_, dummy_buf, 0, 0);
    ::io_set_callback(wake_iocb, &QueueWakeCompletionCallback);
    struct iocb* iocbs[1] = { wake_iocb };
    int res = ::io_submit(io_object_, 1, iocbs);
    if (res != 1) {
        delete wake_iocb;
        return -1;
    }
    return 0;
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
  // Guards own io_context and caller_context_copy until io_submit returns 1; on every
  // failure path the destructors release both.
  auto caller_copy_guard = core::make_context_unique_ptr<IAsyncContext>(caller_context_copy);

  new(io_context.get()) QueueIoHandler::IoCallbackContext(operationType, fd_, offset, length,
      buffer, caller_context_copy, callback);

  struct iocb* iocbs[1];
  iocbs[0] = reinterpret_cast<struct iocb*>(io_context.get());

  // Exactly one iocb is prepared. io_submit return values for N_prepared == 1:
  //   1  : kernel accepted; one completion will fire.
  //   0  : transient kernel ring full; retry with sched_yield up to kMaxSubmitRetries.
  //        The iocb is not queued; we still own it.
  //   <0 : permanent error (EINVAL/EBADF/EIO); surface immediately.
  // Batching is unsupported here; partial submission would require per-iocb ownership tracking.
  int retries = 0;
  int result;
  while (true) {
    result = ::io_submit(io_object_, 1, iocbs);
    if (result == 1) break;
    if (result < 0) return Status::IOError;
    if (++retries > kMaxSubmitRetries) return Status::IOError;
    ::sched_yield();
  }

  // Ownership transferred to the kernel.
  caller_copy_guard.release();
  io_context.release();
  return Status::Ok;
}

#ifdef FASTER_URING

namespace {

// Dispatches one completion CQE to the user callback. Negative `io_res` becomes
// Status::IOError; non-negative carries the bytes-transferred count. Every submission
// must produce exactly one callback to balance the C# numPending counter.
inline void DispatchUringCqe(int io_res, UringIoHandler::IoCallbackContext* context) {
    static_assert(std::is_trivially_destructible<UringIoHandler::IoCallbackContext>::value,
                  "DispatchUringCqe relies on trivial destruction; route through "
                  "make_context_unique_ptr if a non-trivial member is added.");
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
  // Drain one CQE from any ring (compat: scans all rings).
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
  // cq_lock serialises peek + cqe_seen against the all-rings compat scanner so the same
  // CQE cannot be dispatched twice.
  SpinLock* cq_lock = cq_locks_[idx];
  struct io_uring_cqe* cqe = nullptr;
  cq_lock->Acquire();
  int res = io_uring_peek_cqe(ring, &cqe);
  if (res == 0 && cqe) {
    int io_res = cqe->res;
    auto* context = reinterpret_cast<UringIoHandler::IoCallbackContext*>(io_uring_cqe_get_data(cqe));
    io_uring_cqe_seen(ring, cqe);
    cq_lock->Release();
    // user_data == nullptr is the sentinel for wake-up SQEs (UringIoHandler::Wake) and
    // rewritten-after-failed-submit SQEs (ScheduleOperation error path). Must NOT
    // dispatch — there's no caller context to deliver to. Counts as a successful drain
    // so TryComplete()'s any-flag flips, matching the QueueRunFor semantics.
    if (context == nullptr) {
      return true;
    }
    DispatchUringCqe(io_res, context);
    return true;
  }
  cq_lock->Release();
  return false;
}

int UringIoHandler::QueueRun(int timeout_secs) {
  // Compat: drain across all rings. First ring uses the full timeout; subsequent rings poll.
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
    // Blocking drain for one ring. The wait phase is lock-free (kernel wakes every blocked
    // thread on a CQE); peek+cqe_seen is serialised by cq_lock against the compat scanner.
    if (idx < 0 || idx >= static_cast<int>(rings_.size())) return -1;
    struct io_uring* ring = rings_[idx];
    if (ring == nullptr) return -1;
    SpinLock* cq_lock = cq_locks_[idx];

    int ret = 0;

    // Phase 1: wait up to `timeout_secs` for at least one CQE; do not consume.
    if (timeout_secs > 0) {
        struct __kernel_timespec ts;
        ts.tv_sec = timeout_secs;
        ts.tv_nsec = 0;
        struct io_uring_cqe* wait_cqe = nullptr;
        (void)io_uring_wait_cqe_timeout(ring, &wait_cqe, &ts);
    }

    // Phase 2: drain everything currently available.
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

        // Wake-up SQEs are submitted with user_data = nullptr (see UringIoHandler::Wake)
        // and only exist to unblock io_uring_wait_cqe_timeout. They carry no caller context
        // and must NOT be dispatched.
        if (context == nullptr) {
            ++ret;
            continue;
        }
        DispatchUringCqe(io_res, context);
        ++ret;
    }

    return ret;
}

int UringIoHandler::Wake(int idx) {
    if (idx < 0 || idx >= static_cast<int>(rings_.size())) return -1;
    struct io_uring* ring = rings_[idx];
    if (ring == nullptr) return -1;
    SpinLock* sq_lock = sq_locks_[idx];

    sq_lock->Acquire();
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (sqe == nullptr) {
        sq_lock->Release();
        return -1;
    }
    io_uring_prep_nop(sqe);
    // user_data = nullptr is the sentinel for "wake-up; do not dispatch a callback"
    // recognised by the QueueRunFor drain loop.
    io_uring_sqe_set_data(sqe, nullptr);
    int res = io_uring_submit(ring);
    sq_lock->Release();
    return res == 1 ? 0 : -1;
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
  // Guard owns caller_context_copy until io_uring_submit returns 1.
  auto caller_copy_guard = core::make_context_unique_ptr<IAsyncContext>(caller_context_copy);

  bool is_read = operationType == FileOperationType::Read;
  new(io_context.get()) UringIoHandler::IoCallbackContext(is_read, fd_, buffer, length, offset, caller_context_copy, callback);

  // pick_ring distributes submissions across rings via atomic round-robin (single ring under N=1).
  struct io_uring* ring = nullptr;
  SpinLock* sq_lock = nullptr;
  handler_->pick_ring(ring, sq_lock);

  // Acquire an SQE. io_uring_get_sqe returns nullptr when the SQ ring is full; retry with
  // sched_yield up to kMaxSubmitRetries. sq_lock is released around sched_yield so other
  // submitters on this ring are not blocked across the syscall.
  struct io_uring_sqe* sqe = nullptr;
  int retries = 0;
  while (true) {
    sq_lock->Acquire();
    sqe = io_uring_get_sqe(ring);
    if (sqe != nullptr) break;
    sq_lock->Release();
    if (++retries > kMaxSubmitRetries) return Status::IOError;
    ::sched_yield();
  }
  // sq_lock is held; sqe is non-null.

  if (is_read) {
    io_uring_prep_readv(sqe, fd_, &io_context->vec_, 1, offset);
  } else {
    io_uring_prep_writev(sqe, fd_, &io_context->vec_, 1, offset);
  }
  io_uring_sqe_set_data(sqe, io_context.get());

  // Submit. Exactly one SQE was prepared and is committed to the user-side SQ ring; we MUST
  // keep re-submitting on transient negatives (-EAGAIN/-EBUSY) or the slot leaks permanently.
  // Other negatives are surfaced as IOError. A positive return other than 1 should not occur
  // for a single prepared SQE on a non-SQPOLL ring; treated as IOError defensively.
  // Batching multiple SQEs is unsupported by this caller.
  int res;
  int submit_retries = 0;
  while (true) {
    res = io_uring_submit(ring);
    if (res == 1) break;
    if (res != -EAGAIN && res != -EBUSY) break;
    if (++submit_retries > kMaxSubmitRetries) break;
    sq_lock->Release();
    ::sched_yield();
    sq_lock->Acquire();
  }
  if (res != 1) {
    // Submit failed but io_uring_get_sqe() already advanced the user-side sqe_tail, so the
    // prepared SQE is still sitting in the SQ ring pointing at io_context. The next successful
    // submit on this ring would consume it and dispatch a callback against the io_context we
    // are about to free here, which is a use-after-free.
    //
    // Rewrite the slot in place as a no-op with the wake-up sentinel (user_data = nullptr).
    // The QueueRunFor drain loop (file_linux.cc:429-432) explicitly skips nullptr user_data
    // without dispatching a callback, so when a later submit flushes this nop the CQE is
    // drained harmlessly.
    //
    // Safe to mutate `sqe` here: we still hold sq_lock and no concurrent submitter can have
    // observed this SQE yet (the kernel only learns about it on the next io_uring_submit).
    io_uring_prep_nop(sqe);
    io_uring_sqe_set_data(sqe, nullptr);
  }
  sq_lock->Release();
  if (res != 1) {
    return Status::IOError;
  }

  // Ownership transferred to the kernel.
  caller_copy_guard.release();
  io_context.release();
  return Status::Ok;
}

#endif

#undef DCHECK_ALIGNMENT

}
} // namespace FASTER::environment
