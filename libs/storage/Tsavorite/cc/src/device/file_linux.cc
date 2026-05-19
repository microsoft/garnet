// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include <cstring>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <stdio.h>
#include <time.h>
#include "file_linux.h"

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

  /// Always unbuffered (O_DIRECT).
  fd_ = ::open(filename_.c_str(), flags | O_RDWR | create_flags, S_IRUSR | S_IWUSR);

  if(exists) {
    // Let the caller know whether the file we tried to open or create (already) exists.
    if(create_disposition == FileCreateDisposition::CreateOrTruncate ||
        create_disposition == FileCreateDisposition::OpenOrCreate) {
      *exists = (errno == EEXIST);
    } else if(create_disposition == FileCreateDisposition::OpenExisting) {
      *exists = (errno != ENOENT);
      if(!*exists) {
        // The file doesn't exist. Don't return an error, since the caller is expecting this case.
        return Status::Ok;
      }
    }
  }
  if(fd_ == -1) {
    int error = errno;
    return Status::IOError;
  }

  Status result = GetDeviceAlignment();
  if(result != Status::Ok) {
    Close();
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
  // For now, just hardcode 512-byte alignment.
  device_alignment_ = 512;
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

  new(io_context.get()) QueueIoHandler::IoCallbackContext(operationType, fd_, offset, length,
      buffer, caller_context_copy, callback);

  struct iocb* iocbs[1];
  iocbs[0] = reinterpret_cast<struct iocb*>(io_context.get());

  int result = ::io_submit(io_object_, 1, iocbs);
  if(result != 1) {
    return Status::IOError;
  }

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
  struct io_uring_cqe* cqe = nullptr;
  cq_lock_.Acquire();
  int res = io_uring_peek_cqe(ring_, &cqe);
  if(res == 0 && cqe) {
    int io_res = cqe->res;
    auto *context = reinterpret_cast<UringIoHandler::IoCallbackContext*>(io_uring_cqe_get_data(cqe));
    io_uring_cqe_seen(ring_, cqe);
    cq_lock_.Release();
    DispatchUringCqe(io_res, context);
    return true;
  } else {
    cq_lock_.Release();
    return false;
  }
}

int UringIoHandler::QueueRun(int timeout_secs) {
    // Blocking drain of completed events, intended for a long-running completion
    // thread. Mirrors libaio QueueIoHandler::QueueRun semantics: wait up to
    // timeout_secs for at least one event, then drain everything currently
    // available. Designed to be safe with N concurrent completion threads:
    // the blocking wait is performed WITHOUT holding cq_lock_ (multiple threads
    // may wait concurrently on the kernel side via io_uring_enter), and the
    // user-space ring dequeue is serialized via cq_lock_ around peek+seen.
    int ret = 0;

    // Phase 1: block (without holding any lock) waiting for at least one CQE,
    // or until the timeout expires. Don't consume (seen) the CQE here — let
    // the peek path under cq_lock_ claim it. If multiple threads are blocked
    // here, they all wake when an event arrives; the first one to acquire the
    // lock claims the CQE, the others fall through with an empty peek and exit.
    if (timeout_secs > 0) {
        struct __kernel_timespec ts;
        ts.tv_sec = timeout_secs;
        ts.tv_nsec = 0;
        struct io_uring_cqe* wait_cqe = nullptr;
        // Return value is intentionally discarded: -ETIME means no event arrived
        // within the timeout, other errors (e.g. -EINTR) are equally handled by
        // falling through to phase 2 which drains anything already present.
        (void)io_uring_wait_cqe_timeout(ring_, &wait_cqe, &ts);
    }

    // Phase 2: drain everything currently available, serialized via cq_lock_.
    // Each completion (success or failure) is reported to the caller via the
    // user callback; we do NOT retry failures here — the io_uring CQE result
    // already reflects the kernel's final outcome for that submission, and
    // retrying permanent errors such as -EINVAL/-EBADF/-EIO would loop forever
    // and leak the in-flight count tracked by the C# layer.
    for (;;) {
        cq_lock_.Acquire();
        struct io_uring_cqe* cqe = nullptr;
        int rc = io_uring_peek_cqe(ring_, &cqe);
        if (rc != 0 || cqe == nullptr) {
            cq_lock_.Release();
            break;
        }
        int io_res = cqe->res;
        auto* context = reinterpret_cast<UringIoHandler::IoCallbackContext*>(io_uring_cqe_get_data(cqe));
        io_uring_cqe_seen(ring_, cqe);
        cq_lock_.Release();

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

  ring_ = handler->io_uring();
  sq_lock_ = handler->sq_lock();
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

  bool is_read = operationType == FileOperationType::Read;
  new(io_context.get()) UringIoHandler::IoCallbackContext(is_read, fd_, buffer, length, offset, caller_context_copy, callback);

  sq_lock_->Acquire();
  struct io_uring_sqe *sqe = io_uring_get_sqe(ring_);
  assert(sqe != 0);

  if (is_read) {
    io_uring_prep_readv(sqe, fd_, &io_context->vec_, 1, offset);
    //io_uring_prep_read(sqe, fd_, buffer, length, offset);
  } else {
    io_uring_prep_writev(sqe, fd_, &io_context->vec_, 1, offset);
    //io_uring_prep_write(sqe, fd_, buffer, length, offset);
  }
  io_uring_sqe_set_data(sqe, io_context.get());

  int res = io_uring_submit(ring_);
  sq_lock_->Release();
  if (res != 1) {
    return Status::IOError;
  }
  
  io_context.release();
  return Status::Ok;
}

#endif

#undef DCHECK_ALIGNMENT

}
} // namespace FASTER::environment
