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
/// In-epoch submission yield budget. ScheduleOperation runs inside NativeDeviceImpl's epoch
/// protection; on transient kernel back-pressure during submission (libaio io_submit == 0,
/// uring io_uring_get_sqe == nullptr or io_uring_submit -EAGAIN/-EBUSY) we yield this many times
/// to absorb the common microsecond-scale drain latency, then UNWIND (return Status::Pending) so
/// SubmitWithEpoch can wait WITHOUT holding the epoch/thread-id slot. Kept small: with the ring
/// sized to the throttle limit a sustained-full ring is exceptional, so we favor releasing the
/// epoch quickly over avoiding the per-retry io_context rebuild. Permanent submission errors are
/// surfaced immediately and not retried.
constexpr int kSubmitYieldBudget = 16;
} // anonymous namespace

namespace {
// --- Batched libaio submit (opt-in via GARNET_SUBMIT_BATCH env var) ---
// Per-submitter-thread accumulation of prepared READ iocbs. Reads are appended here and
// submitted in bulk via io_submit(ctx, N, ...) when the batch reaches the threshold or an
// explicit FlushSubmits() is requested, cutting io_submit syscalls (and the per-call kernel
// aio-context mutex acquisition) by ~N. Writes are never batched: the memory-bounded log
// flush path needs prompt write submission. All iocbs in a thread's batch target that
// thread's pick_context() ctx (stable per thread for a given handler), so io_submit(ctx, N)
// is valid. Ownership: an appended iocb's per-op io_context + caller-context copy have been
// released from their RAII guards, so the batch owns them until the bulk io_submit transfers
// them to the kernel (success) or FlushLibaioBatch delivers an error + frees them.
struct LibaioSubmitBatch {
  io_context_t ctx = nullptr;
  std::vector<struct iocb*> iocbs;
};
thread_local LibaioSubmitBatch t_libaio_batch;

// Number of reads to accumulate before an auto-flush. 1 (default, env unset) == no batching:
// submit immediately, byte-for-byte legacy behaviour. Read once from GARNET_SUBMIT_BATCH.
inline size_t libaio_batch_threshold() {
  static const size_t v = [] {
    const char* s = ::getenv("GARNET_SUBMIT_BATCH");
    long n = s ? ::atol(s) : 1;
    if (n < 1) n = 1;
    if (n > 1024) n = 1024;
    return static_cast<size_t>(n);
  }();
  return v;
}

// Max completions to reap per io_getevents in the opportunistic TryComplete()/TryCompleteFor()
// poll. Reaping >1 event per syscall amortises the io_getevents call + its kernel aio-context
// ring-lock across many completions when several are ready (bursty / lower-QD completion), and is
// harmless at the saturated peak (the poll simply returns fewer than the max). Always on with a
// small default of 8 (matching IO_BATCH_EVENTS, the dedicated-drainer reap batch); tunable via
// GARNET_TRYCOMPLETE_BATCH (clamped 1..kTryCompleteMaxEvents; set 1 for the legacy 1-event poll).
constexpr int kTryCompleteMaxEvents = 128;
inline int trycomplete_batch_events() {
  static const int v = [] {
    const char* s = ::getenv("GARNET_TRYCOMPLETE_BATCH");
    long n = s ? ::atol(s) : 8;
    if (n < 1) n = 1;
    if (n > kTryCompleteMaxEvents) n = kTryCompleteMaxEvents;
    return static_cast<int>(n);
  }();
  return v;
}

// Flush the calling thread's accumulated read batch: submit all queued iocbs to their ctx via
// io_submit(ctx, N). Handles partial submits (io_submit may accept fewer than N) and transient
// ring-full (return 0 / -EAGAIN) by yielding and retrying the remaining. A permanent per-iocb
// error delivers IOError to that read's callback (mirroring IoCompletionCallback) and drops it.
// Returns the count submitted to the kernel. Runs on the same thread that accumulated (the batch
// is thread_local), so no cross-thread synchronization is needed.
int FlushLibaioBatch() {
  auto& b = t_libaio_batch;
  const size_t n = b.iocbs.size();
  if (n == 0) return 0;
  struct iocb** base = b.iocbs.data();
  size_t done = 0;
  int submitted = 0;
  while (done < n) {
    int r = ::io_submit(b.ctx, static_cast<long>(n - done), base + done);
    if (r > 0) { done += static_cast<size_t>(r); submitted += r; continue; }
    if (r == 0 || r == -EAGAIN) {
      // Ring transiently full; the throttle bounds in-flight below ring depth so this is rare.
      ::sched_yield();
      continue;
    }
    // Permanent error on base[done]: deliver IOError to its callback and free it, then skip.
    QueueIoHandler::IoCompletionCallback(b.ctx, base[done], -EIO, 0);
    ++done;
  }
  b.iocbs.clear();
  return submitted;
}
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
  // Compat scanner: walk all shards. Matches UringIoHandler::TryComplete() so
  // callers that don't know about sharding (e.g., AllocatorBase's throttle-wait
  // loop calling device.TryComplete() opportunistically) still observe
  // completions on shards >0.
  bool any = false;
  for (int i = 0; i < static_cast<int>(io_objects_.size()); ++i) {
    if (TryCompleteFor(i)) any = true;
  }
  return any;
}

bool QueueIoHandler::TryCompleteFor(int idx) {
  if (idx < 0 || idx >= static_cast<int>(io_objects_.size())) return false;
  io_context_t ctx = io_objects_[idx];
  if (ctx == 0) return false;
  struct timespec timeout;
  std::memset(&timeout, 0, sizeof(timeout));
  struct io_event events[kTryCompleteMaxEvents];
  // Reap up to a batch of ready completions in a single (non-blocking, timeout=0) io_getevents,
  // amortising the syscall + kernel aio-context ring-lock over many events. min_nr stays 1 so a
  // zeroed timeout makes this a pure poll (returns immediately with 0..max ready events).
  int result = ::io_getevents(ctx, 1, trycomplete_batch_events(), events, &timeout);
  if (result <= 0) return false;
  for (int i = 0; i < result; ++i) {
    io_callback_t callback = reinterpret_cast<io_callback_t>(events[i].data);
    callback(ctx, events[i].obj, events[i].res, events[i].res2);
  }
  return true;
}

#define IO_BATCH_EVENTS	8		/* number of events to batch up */

int QueueIoHandler::QueueRun(int timeout_secs) {
  // Compat: drain across all contexts. First context uses the full timeout; subsequent
  // contexts poll (timeout=0). This matches the legacy single-context behaviour for
  // num_contexts==1 (one ::io_getevents with the full timeout, batched up to
  // IO_BATCH_EVENTS).
  if (io_objects_.empty()) return 0;
  int total = 0;
  int first = QueueRunFor(0, timeout_secs);
  if (first > 0) total += first;
  for (int i = 1; i < static_cast<int>(io_objects_.size()); ++i) {
    int n = QueueRunFor(i, 0);
    if (n > 0) total += n;
  }
  return total > 0 ? total : first;
}

int QueueIoHandler::QueueRunFor(int idx, int timeout_secs) {
    if (idx < 0 || idx >= static_cast<int>(io_objects_.size())) return -1;
    io_context_t ctx = io_objects_[idx];
    if (ctx == 0) return -1;
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
        if ((n = ::io_getevents(ctx, 1, IO_BATCH_EVENTS, events, &timeout)) <= 0)
            break;
        ret += n;
        for (ep = events, i = n; i-- > 0; ep++) {
            io_callback_t callback = reinterpret_cast<io_callback_t>(ep->data);
            callback(ctx, ep->obj, ep->res, ep->res2);
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
    if (idx < 0 || idx >= static_cast<int>(io_objects_.size())) return -1;
    io_context_t ctx = io_objects_[idx];
    if (ctx == 0) return -1;
    int wake_fd = wake_fds_[idx];
    if (wake_fd < 0) return -1;
    // Submit a 0-byte read on /dev/null. The kernel completes it immediately (reads on
    // /dev/null always return 0 bytes), io_getevents wakes up, dispatches the callback
    // which frees the iocb. We allocate a fresh iocb each call because Wake() runs at
    // most once per Dispose() per context — the per-allocation cost is negligible vs the
    // ~1s stall it eliminates.
    static thread_local char dummy_buf[8] alignas(8) = {};
    struct iocb* wake_iocb = new struct iocb();
    ::io_prep_pread(wake_iocb, wake_fd, dummy_buf, 0, 0);
    ::io_set_callback(wake_iocb, &QueueWakeCompletionCallback);
    struct iocb* iocbs[1] = { wake_iocb };
    int res = ::io_submit(ctx, 1, iocbs);
    if (res != 1) {
        delete wake_iocb;
        return -1;
    }
    return 0;
}

// Flush the calling thread's accumulated read batch (see FlushLibaioBatch). Called on the
// submitting thread from the managed layer (explicit tail flush + throttle-spin safety net).
int QueueIoHandler::FlushSubmits() {
    return FlushLibaioBatch();
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

  handler_ = handler;
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
  // Defense-in-depth: refuse to submit to io_submit with an invalid fd. The
  // FileSystemSegmentBundle/OpenSegment fix in file_system_disk.h prevents a partially-
  // opened bundle from being committed to files_, so in well-behaved flows fd_ is always
  // valid here. This guard catches any future regression that re-introduces fd_=-1 on
  // the submit path — empirically, io_submit with aio_fildes=-1 has been observed to
  // hang inside libaio on some kernels instead of returning -EBADF synchronously, which
  // crashes the calling process.
  if (fd_ < 0) {
    return Status::IOError;
  }

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

  // Pick a per-thread sharded io_context so each submitter primarily lands on its own
  // kernel io_context_t mutex. With num_contexts >= num_submitters, io_submit becomes
  // effectively contention-free at the kernel side.
  io_context_t ctx = handler_->pick_context();

  // Batched submit (opt-in via GARNET_SUBMIT_BATCH>1): accumulate READ iocbs per-thread and
  // submit them in bulk to cut io_submit syscalls (and per-call kernel aio-ctx mutex hits).
  // Writes are never batched (log flush needs prompt submission). The iocb + its caller-context
  // copy are handed to the thread-local batch, which owns them until the bulk io_submit
  // transfers them to the kernel (or FlushLibaioBatch errors + frees them). The batch always
  // targets this thread's stable ctx; a defensive flush handles any ctx change.
  const size_t batchThreshold = libaio_batch_threshold();
  if (batchThreshold > 1 && operationType == FileOperationType::Read) {
    auto& b = t_libaio_batch;
    if (!b.iocbs.empty() && b.ctx != ctx) {
      FlushLibaioBatch();
    }
    b.ctx = ctx;
    b.iocbs.push_back(iocbs[0]);
    caller_copy_guard.release();
    io_context.release();
    if (b.iocbs.size() >= batchThreshold) {
      FlushLibaioBatch();
    }
    return Status::Ok;
  }


  //   1            : kernel accepted; one completion will fire.
  //   0 or -EAGAIN : transient kernel ring full; brief in-epoch yield, then unwind.
  //                  The iocb is not queued; we still own it.
  //   other <0     : permanent error (EINVAL/EBADF/EIO); surface immediately.
  //
  // We MUST NOT surface transient EAGAIN to the caller as an error: the engine interprets
  // numBytes=0 as a short read, recursively retries via AsyncGetFromDiskCallback, and we
  // spiral into a positive feedback loop that backs up the ThreadPool and never drains.
  //
  // Two-stage backoff: first a short in-epoch yield budget (kSubmitYieldBudget sched_yield's) that
  // absorbs the common case where the kernel drainer frees a slot within microseconds — cheap,
  // and avoids tearing down/rebuilding the per-op io_context. If the ring is STILL full after
  // that budget (sustained saturation — only reachable when in-flight exceeds the ring depth,
  // which the throttle-sized ring normally prevents), we return Status::Pending. Nothing was
  // submitted, so the RAII guards below free io_context/caller_context_copy cleanly.
  // NativeDeviceImpl::SubmitWithEpoch catches Pending, RELEASES the epoch (and its thread-id
  // slot) so other submitters and the drainer make progress, backs off, and retries the whole
  // op — re-resolving the segment bundle under fresh epoch protection. This preserves the
  // "submit either succeeds or returns a permanent error" contract toward the managed layer
  // (Pending never escapes NativeDeviceImpl) while never holding the epoch across a long wait.
  int retries = 0;
  int result;
  while (true) {
    result = ::io_submit(ctx, 1, iocbs);
    if (result == 1) break;
    if (result < 0 && result != -EAGAIN) return Status::IOError;
    // result == 0 (ring full) or result == -EAGAIN (kernel saying "try later")
    if (retries >= kSubmitYieldBudget) {
      // Unwind to NativeDeviceImpl::SubmitWithEpoch to wait without holding the epoch.
      return Status::Pending;
    }
    ::sched_yield();
    ++retries;
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

// ---------------------------------------------------------------------------------------------
// io_uring batched submit (opt-in via GARNET_SUBMIT_BATCH; default 1 == submit per-op == legacy).
// Mirrors the libaio batch (FlushLibaioBatch): a submitter thread accumulates prepared READ SQEs
// and defers io_uring_submit until a threshold, coalescing many reads into ONE submit syscall.
// Only a thread that solely owns its ring (UringIoHandler::try_own_ring) defers; writes always
// submit immediately (log-flush latency). Every deferred SQE already carries its io_context as
// user_data, so once ANY later submit on the ring flushes it, exactly one completion is
// dispatched. The managed completion/throttle path (NativeStorageDevice.TryComplete/
// TryCompleteMine/FlushSubmits -> NativeDevice_FlushSubmits -> UringIoHandler::FlushSubmits)
// flushes this thread's batch before it waits on in-flight completions, so a sub-threshold batch
// can never stall the AllocatorBase.AsyncGetFromDisk throttle (no lost-flush deadlock).
// ---------------------------------------------------------------------------------------------

// UringIoHandler::uring_thread_id() (defined inline in the header) is the single source of the
// stable per-thread id used to CAS ring ownership, so pick_ring_index_le() and the submit path's
// try_own_ring() below agree on the owner.

// Batch size threshold read once from GARNET_SUBMIT_BATCH (shared with the libaio backend).
// 1 (default/unset) disables batching -> byte-for-byte legacy per-op submit.
inline size_t uring_batch_threshold() {
    static const size_t threshold = [] {
        const char* s = ::getenv("GARNET_SUBMIT_BATCH");
        long n = (s != nullptr) ? ::atol(s) : 1;
        if (n < 1) n = 1;
        if (n > 1024) n = 1024;
        return static_cast<size_t>(n);
    }();
    return threshold;
}

// Per-thread deferred-submit state. A thread holds deferred SQEs on AT MOST one ring at a time
// (switching rings/handlers flushes the prior batch first), so a single slot suffices.
struct UringSubmitBatch {
    const UringIoHandler* handler = nullptr;  // handler whose ring holds the deferred SQEs
    struct io_uring* ring = nullptr;          // ring the SQEs were prepared on
    SpinLock* sq_lock = nullptr;              // that ring's SQ lock
    int ring_idx = -1;                        // that ring's index (for drain-assist)
    int pending = 0;                          // count of deferred, not-yet-submitted SQEs
};
thread_local UringSubmitBatch t_uring_batch;

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

// Non-blocking batch drain of ONE ring (the caller's affine ring), reaping up to kCqeBatch
// completions in a single cq_lock section with dispatch moved outside the lock. This is the
// io_uring analogue of libaio's batched TryCompleteMine (io_getevents up to
// trycomplete_batch_events): the inline submitter-thread completion path (Tsavorite
// CompletePending / AsyncGetFromDisk throttle-wait) reaps its own ring a batch at a time
// instead of one io_uring_peek_cqe per call, cutting per-completion cq_lock + peek overhead ~Nx.
// Mirrors QueueRunFor's phase-2 (snapshot-before-advance, dispatch-after-release) but is a single
// non-blocking pass (no wait, no drain-until-empty loop) so it stays a bounded poll.
bool UringIoHandler::TryCompleteMineBatch(int idx) {
  if (idx < 0 || idx >= static_cast<int>(rings_.size())) return false;
  struct io_uring* ring = rings_[idx];
  if (ring == nullptr) return false;
  SpinLock* cq_lock = cq_locks_[idx];

  constexpr unsigned kCqeBatch = 64;
  struct io_uring_cqe* cqes[kCqeBatch];
  struct DrainSlot {
    int io_res;
    UringIoHandler::IoCallbackContext* context;
  } snapshot[kCqeBatch];

  cq_lock->Acquire();
  unsigned n = io_uring_peek_batch_cqe(ring, cqes, kCqeBatch);
  if (n == 0) {
    cq_lock->Release();
    return false;
  }
  for (unsigned i = 0; i < n; ++i) {
    snapshot[i].io_res = cqes[i]->res;
    snapshot[i].context = reinterpret_cast<UringIoHandler::IoCallbackContext*>(
        io_uring_cqe_get_data(cqes[i]));
  }
  io_uring_cq_advance(ring, n);
  cq_lock->Release();

  // Dispatch outside the lock. null user_data marks wake-up / rewritten-after-failed-submit SQEs
  // (no caller context); skip them, exactly as TryCompleteFor / QueueRunFor do.
  for (unsigned i = 0; i < n; ++i) {
    if (snapshot[i].context == nullptr) continue;
    DispatchUringCqe(snapshot[i].io_res, snapshot[i].context);
  }
  return true;
}

// Opt-out gate for the TryCompleteMine batch-reap (default ON). GARNET_URING_BATCH_REAP=0 falls
// back to the legacy single-CQE reap (TryCompleteFor) so the batch-reap's throughput impact can be
// measured without a revert-build. Read once.
bool UringIoHandler::batch_reap_enabled() {
  static const bool v = [] {
    const char* s = ::getenv("GARNET_URING_BATCH_REAP");
    long n = s ? ::atol(s) : 1;
    return n != 0;
  }();
  return v;
}

bool UringIoHandler::le_affinity_enabled() {
  static const bool v = [] {
    const char* s = ::getenv("GARNET_RING_LE_AFFINITY");
    long n = s ? ::atol(s) : 0;
    if (n != 0) {
      ::fprintf(stderr, "[ring-le-affinity] enabled: LightEpoch-style per-thread ring affinity "
                        "(warm preferred ring + probe-replace + batch-boundary release)\n");
    }
    return n != 0;
  }();
  return v;
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
    // thread on a CQE); peek+advance is serialised by cq_lock against the compat scanner.
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

    // Phase 2: batch-drain. The current scheme amortizes one cq_lock acquire/release across
    // up to kCqeBatch CQEs (libaio's io_getevents pulls up to 128 per syscall; this is the
    // io_uring equivalent). Just as important, the snapshot-then-advance-then-release pattern
    // moves the user callback dispatch OUT of the locked section so submitters that need the
    // ring aren't blocked by callback latency. Without batching, a single drainer caps the
    // ring at ~340K IOPS on this hardware even though libaio at ct=1 saturates at ~750K;
    // with batching the per-CQE lock cost goes away and the gap closes substantially.
    //
    // Snapshot BEFORE io_uring_cq_advance: once advanced the kernel may reuse the CQ slots,
    // so the cqe pointers (which point into the CQ ring) become dangling.
    constexpr unsigned kCqeBatch = 64;
    struct io_uring_cqe* cqes[kCqeBatch];
    struct DrainSlot {
        int io_res;
        UringIoHandler::IoCallbackContext* context;
    } snapshot[kCqeBatch];

    for (;;) {
        cq_lock->Acquire();
        unsigned n = io_uring_peek_batch_cqe(ring, cqes, kCqeBatch);
        if (n == 0) {
            cq_lock->Release();
            break;
        }
        for (unsigned i = 0; i < n; ++i) {
            snapshot[i].io_res = cqes[i]->res;
            snapshot[i].context = reinterpret_cast<UringIoHandler::IoCallbackContext*>(
                io_uring_cqe_get_data(cqes[i]));
        }
        io_uring_cq_advance(ring, n);
        cq_lock->Release();

        // Dispatch outside the lock. user_data == nullptr marks the wake-up SQE
        // (UringIoHandler::Wake) and rewritten-after-failed-submit SQEs — both carry no
        // caller context and must be skipped, never dispatched.
        for (unsigned i = 0; i < n; ++i) {
            if (snapshot[i].context == nullptr) {
                ++ret;
                continue;
            }
            DispatchUringCqe(snapshot[i].io_res, snapshot[i].context);
            ++ret;
        }
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
    // io_uring_submit flushes all pending SQEs and returns the count; any res >= 1 means our
    // wake no-op reached the kernel (it may also have flushed stale no-ops in front of it).
    return res >= 1 ? 0 : -1;
}

// Flush this thread's deferred io_uring READ batch for THIS handler, if any. Invoked by the
// managed completion/throttle path before it waits on in-flight completions, guaranteeing a
// sub-threshold batch is always submitted (no lost-flush stall). Returns the number of SQEs the
// final submit reported flushing (0 if nothing was pending or a peer already flushed them).
int UringIoHandler::FlushSubmits() {
    UringSubmitBatch& b = t_uring_batch;
    if (b.pending <= 0 || b.handler != this) return 0;

    int flushed = 0;
    int retries = 0;
    b.sq_lock->Acquire();
    while (true) {
        int res = io_uring_submit(b.ring);
        if (res >= 1) { flushed = res; break; }     // our deferred SQEs reached the kernel
        if (res == 0) break;                         // nothing to flush (a peer already did)
        if (res == -EAGAIN || res == -EBUSY) {
            // CQ ring full: help drain THIS ring's completions to free space, then retry so the
            // deferred reads are never stranded behind a full CQ (would deadlock the throttle).
            b.sq_lock->Release();
            TryCompleteFor(b.ring_idx);
            if (retries++ >= kSubmitYieldBudget) ::sched_yield();
            b.sq_lock->Acquire();
            continue;
        }
        break;                                       // permanent error: SQEs keep their user_data
                                                     // and flush on the next submit/Wake.
    }
    b.sq_lock->Release();
    // In all cases the deferred SQEs are now either in the kernel or still queued in the ring with
    // valid user_data (flushed by the next submit); either way this thread no longer owes a flush.
    b.pending = 0;
    return flushed;
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
  // Guard owns caller_context_copy until io_uring_submit confirms our SQE was flushed (res >= 1).
  auto caller_copy_guard = core::make_context_unique_ptr<IAsyncContext>(caller_context_copy);

  bool is_read = operationType == FileOperationType::Read;
  new(io_context.get()) UringIoHandler::IoCallbackContext(is_read, fd_, buffer, length, offset, caller_context_copy, callback);

  // pick_ring distributes submissions across rings via atomic round-robin (single ring under N=1).
  struct io_uring* ring = nullptr;
  SpinLock* sq_lock = nullptr;
  int ring_idx = -1;
  handler_->pick_ring(ring, sq_lock, ring_idx);

  // Acquire an SQE. io_uring_get_sqe returns nullptr when the SQ ring is full; brief in-lock-free
  // yield budget, then unwind — same rationale and contract as the libaio path
  // (QueueFile::ScheduleOperation): a short yield burst absorbs the common transient, and a
  // sustained-full ring returns Status::Pending so NativeDeviceImpl::SubmitWithEpoch can wait
  // and retry WITHOUT holding the epoch. Nothing has been committed to the ring at this point
  // (no SQE obtained), so the RAII guards free io_context/caller_context_copy on the Pending
  // return. sq_lock is released before yielding/unwinding so other submitters are not blocked.
  struct io_uring_sqe* sqe = nullptr;
  int retries = 0;
  while (true) {
    sq_lock->Acquire();
    sqe = io_uring_get_sqe(ring);
    if (sqe != nullptr) break;
    // SQ ring is full. If this thread has deferred (batched) SQEs sitting in THIS ring, submit
    // them now to let the kernel consume the SQ and free a slot for the retry. This also upholds
    // the invariant that we never unwind to a wait (Status::Pending) with un-submitted SQEs.
    {
      UringSubmitBatch& fb = t_uring_batch;
      if (fb.pending > 0 && fb.handler == handler_ && fb.ring == ring) {
        if (io_uring_submit(ring) >= 1) fb.pending = 0;  // still holding sq_lock
      }
    }
    sq_lock->Release();
    if (retries >= kSubmitYieldBudget) {
      // Flush any residual deferred SQEs (FlushSubmits is a no-op if none) so nothing is left
      // un-submitted across the out-of-epoch wait, then unwind to SubmitWithEpoch.
      handler_->FlushSubmits();
      return Status::Pending;
    }
    ::sched_yield();
    ++retries;
  }
  // sq_lock is held; sqe is non-null.

  if (is_read) {
    io_uring_prep_readv(sqe, fd_, &io_context->vec_, 1, offset);
  } else {
    io_uring_prep_writev(sqe, fd_, &io_context->vec_, 1, offset);
  }
  io_uring_sqe_set_data(sqe, io_context.get());

  // ---- Batched submit fast path (opt-in; default threshold 1 == disabled == legacy) ----
  // If batching is enabled, this is a READ, and this thread solely owns this ring, DEFER
  // io_uring_submit: leave the prepared SQE (already carrying its io_context as user_data) in the
  // SQ and return Ok without a submit syscall, coalescing many reads into one later submit. Writes
  // always submit immediately below. `batched_owner` tells the tail to clear the batch counter
  // once the accumulated SQEs are actually flushed.
  bool batched_owner = false;
  {
    const size_t batch_threshold = uring_batch_threshold();
    if (batch_threshold > 1 && is_read) {
      UringSubmitBatch& b = t_uring_batch;
      // A thread holds deferred SQEs on at most one ring; if it is switching to a different
      // handler/ring, flush the prior batch first so those reads are never stranded (the managed
      // FlushSubmits for that handler would no longer see them once we repurpose this slot).
      if (b.pending > 0 && (b.handler != handler_ || b.ring != ring)) {
        b.sq_lock->Acquire();
        io_uring_submit(b.ring);
        b.sq_lock->Release();
        b.pending = 0;
      }
      if (handler_->try_own_ring(ring_idx, UringIoHandler::uring_thread_id())) {
        batched_owner = true;
        b.handler = handler_;
        b.ring = ring;
        b.sq_lock = sq_lock;
        b.ring_idx = ring_idx;
        ++b.pending;
        if (static_cast<size_t>(b.pending) < batch_threshold) {
          // Under threshold: hand io_context ownership to the queued SQE and return without a
          // submit syscall. The SQE flushes when the batch fills, on the next op, or when the
          // managed layer calls FlushSubmits before waiting.
          sq_lock->Release();
          caller_copy_guard.release();
          io_context.release();
          return Status::Ok;
        }
        // Threshold reached: fall through to submit the whole accumulated batch now.
      }
      // Not the ring owner (shared ring): fall through to immediate per-op submit.
    }
  }

  // Submit. io_uring_submit() flushes ALL SQEs pending in this ring's SQ ring (everything between
  // the kernel-consumed head and our just-prepared SQE at the tail) and returns the COUNT flushed
  // — not "1 for this op". So any res >= 1 means OUR SQE (the last prepared) reached the kernel;
  // there may also be a stale no-op SQE in front of it (left by a prior failed-submit/unwind or by
  // Wake()), which the kernel completes harmlessly (null user_data, skipped by the drainer).
  // Treating res >= 1 as success is REQUIRED for correctness: a res == 2 (stale nop + our op) taken
  // as failure would rewrite/free an io_context whose op is already in flight -> use-after-free on
  // completion.
  //
  // On transient -EAGAIN/-EBUSY (CQ ring full / kernel busy) we yield a bounded in-epoch budget,
  // then UNWIND (Status::Pending) exactly like the get_sqe / libaio paths so we never spin on
  // submit while holding the epoch and thread-id slot. Both the unwind path and a permanent submit
  // error rewrite our prepared-but-unsubmitted SQE to a no-op with null user_data (so a later
  // submit cannot dispatch a completion against the io_context we are about to free); the next
  // successful submit flushes that no-op harmlessly. No-ops are bounded by the SQ ring depth and
  // self-heal as soon as any submit succeeds.
  int res;
  int submit_retries = 0;
  bool unwind = false;
  while (true) {
    res = io_uring_submit(ring);
    if (res >= 1) break;                            // our SQE (the last prepared) was flushed
    if (res != -EAGAIN && res != -EBUSY) break;     // permanent error
    if (submit_retries >= kSubmitYieldBudget) { unwind = true; break; }
    sq_lock->Release();
    ::sched_yield();
    ++submit_retries;
    sq_lock->Acquire();
  }
  if (res < 1) {
    // Permanent submit error, or we are unwinding after a sustained transient. The SQE is prepared
    // in the SQ ring pointing at io_context; rewrite it to a no-op (the QueueRunFor drain loop
    // skips nullptr user_data without dispatching) so a later submit cannot reference the io_context
    // we free here. Safe to mutate `sqe`: we still hold sq_lock and the kernel only observes it on
    // the next io_uring_submit.
    io_uring_prep_nop(sqe);
    io_uring_sqe_set_data(sqe, nullptr);
  }
  sq_lock->Release();
  if (res < 1) {
    // RAII frees io_context/caller_context_copy on return. Unwind -> SubmitWithEpoch retries the
    // whole op outside the epoch; a permanent error surfaces to the caller. Any prior deferred
    // SQEs remain live in the ring (valid user_data) and flush on the retry's/next submit, so
    // clear our batch counter to avoid re-counting them.
    if (batched_owner) t_uring_batch.pending = 0;
    return unwind ? Status::Pending : Status::IOError;
  }

  // res >= 1: ownership transferred to the kernel. A batched submit flushed the whole accumulated
  // run (io_uring_submit drains every queued SQE), so the batch is now empty.
  if (batched_owner) t_uring_batch.pending = 0;
  caller_copy_guard.release();
  io_context.release();
  return Status::Ok;
}

#endif

#undef DCHECK_ALIGNMENT

}
} // namespace FASTER::environment
