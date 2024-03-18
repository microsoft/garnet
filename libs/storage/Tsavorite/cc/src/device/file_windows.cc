// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include <cassert>
#include <iomanip>
#include <sstream>
#include "file.h"

using namespace FASTER::core;

namespace FASTER {
namespace environment {
std::string FormatWin32AndHRESULT(DWORD win32_result) {
  std::stringstream ss;
  ss << "Win32(" << win32_result << ") HRESULT("
     << std::showbase << std::uppercase << std::setfill('0') << std::hex
     << HRESULT_FROM_WIN32(win32_result) << ")";
  return ss.str();
}

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

Status File::Open(DWORD flags, FileCreateDisposition create_disposition, bool* exists) {
  assert(!filename_.empty());
  if(exists) {
    *exists = false;
  }

  file_handle_ = ::CreateFileA(filename_.c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr,
                               GetCreateDisposition(create_disposition), flags, nullptr);
  if(exists) {
    // Let the caller know whether the file we tried to open or create (already) exists.
    if(create_disposition == FileCreateDisposition::CreateOrTruncate ||
        create_disposition == FileCreateDisposition::OpenOrCreate) {
      *exists = (::GetLastError() == ERROR_ALREADY_EXISTS);
    } else if(create_disposition == FileCreateDisposition::OpenExisting) {
      *exists = (::GetLastError() != ERROR_FILE_NOT_FOUND);
      if(!*exists) {
        // The file doesn't exist. Don't return an error, since the caller is expecting this case.
        return Status::Ok;
      }
    }
  }
  if(file_handle_ == INVALID_HANDLE_VALUE) {
    auto error = ::GetLastError();
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
  if(file_handle_ != INVALID_HANDLE_VALUE) {
    bool success = ::CloseHandle(file_handle_);
    file_handle_ = INVALID_HANDLE_VALUE;
    if(!success) {
      auto error = ::GetLastError();
      return Status::IOError;
    }
  }
  owner_ = false;
  return Status::Ok;
}

Status File::Delete() {
  bool success = ::DeleteFileA(filename_.c_str());
  if(!success) {
    auto error = ::GetLastError();
    return Status::IOError;
  }
  return Status::Ok;
}

Status File::GetDeviceAlignment() {
  FILE_STORAGE_INFO info;
  bool result = ::GetFileInformationByHandleEx(file_handle_,
                FILE_INFO_BY_HANDLE_CLASS::FileStorageInfo, &info, sizeof(info));
  if(!result) {
    auto error = ::GetLastError();
    return Status::IOError;
  }

  device_alignment_ = info.LogicalBytesPerSector;
  return Status::Ok;
}

DWORD File::GetCreateDisposition(FileCreateDisposition create_disposition) {
  switch(create_disposition) {
  case FileCreateDisposition::CreateOrTruncate:
    return CREATE_ALWAYS;
  case FileCreateDisposition::OpenOrCreate:
    return OPEN_ALWAYS;
  case FileCreateDisposition::OpenExisting:
    return OPEN_EXISTING;
  default:
    assert(false);
    return INVALID_FILE_ATTRIBUTES; // not reached
  }
}

void CALLBACK ThreadPoolIoHandler::IoCompletionCallback(PTP_CALLBACK_INSTANCE instance,
    PVOID context, PVOID overlapped, ULONG ioResult, ULONG_PTR bytesTransferred, PTP_IO io) {
  // context is always nullptr; state is threaded via the OVERLAPPED
  auto callback_context = make_context_unique_ptr<IoCallbackContext>(
                            reinterpret_cast<IoCallbackContext*>(overlapped));

  HRESULT hr = HRESULT_FROM_WIN32(ioResult);
  Status return_status;
  if(FAILED(hr)) {
    return_status = Status::IOError;
  } else {
    return_status = Status::Ok;
  }
  callback_context->callback(callback_context->caller_context, return_status,
                             static_cast<size_t>(bytesTransferred));
}

WindowsPtpThreadPool::WindowsPtpThreadPool(size_t max_threads)
  : pool_{ nullptr }
  , callback_environment_{ nullptr }
  , cleanup_group_{ nullptr }
  , max_threads_{ max_threads } {
  pool_ = ::CreateThreadpool(nullptr);
  ::SetThreadpoolThreadMaximum(pool_, static_cast<DWORD>(max_threads));
  bool ret = ::SetThreadpoolThreadMinimum(pool_, 1);
  if(!ret) {
    throw std::runtime_error{ "Cannot set threadpool thread minimum to 1" };
  }
  cleanup_group_ = ::CreateThreadpoolCleanupGroup();
  if(!cleanup_group_) {
    throw std::runtime_error{ "Cannot create threadpool cleanup group" };
  }

  callback_environment_ = new TP_CALLBACK_ENVIRON{};

  ::InitializeThreadpoolEnvironment(callback_environment_);
  ::SetThreadpoolCallbackPool(callback_environment_, pool_);
  ::SetThreadpoolCallbackPriority(callback_environment_, TP_CALLBACK_PRIORITY_LOW);
  ::SetThreadpoolCallbackCleanupGroup(callback_environment_, cleanup_group_, nullptr);
}

WindowsPtpThreadPool::~WindowsPtpThreadPool() {
  if(!cleanup_group_) return;

  // Wait until all callbacks have finished.
  ::CloseThreadpoolCleanupGroupMembers(cleanup_group_, FALSE, nullptr);

  ::DestroyThreadpoolEnvironment(callback_environment_);

  ::CloseThreadpoolCleanupGroup(cleanup_group_);
  ::CloseThreadpool(pool_);

  delete callback_environment_;
}

Status WindowsPtpThreadPool::Schedule(Task task, void* task_parameters) {
  auto info = alloc_context<TaskInfo>(sizeof(TaskInfo));
  if(!info.get()) return Status::OutOfMemory;
  new(info.get()) TaskInfo();

  info->task = task;
  info->task_parameters = task_parameters;

  PTP_WORK_CALLBACK ptp_callback = TaskStartSpringboard;
  PTP_WORK work = CreateThreadpoolWork(ptp_callback, info.get(), callback_environment_);
  if(!work) {
    std::stringstream ss;
    ss << "Failed to schedule work: " << FormatWin32AndHRESULT(::GetLastError());
    fprintf(stderr, "%s\n", ss.str().c_str());
    return Status::Aborted;
  }
  SubmitThreadpoolWork(work);
  info.release();

  return Status::Ok;
}

void CALLBACK WindowsPtpThreadPool::TaskStartSpringboard(PTP_CALLBACK_INSTANCE instance,
    PVOID parameter, PTP_WORK work) {
  auto info = make_context_unique_ptr<TaskInfo>(reinterpret_cast<TaskInfo*>(parameter));
  info->task(info->task_parameters);
  CloseThreadpoolWork(work);
}

Status ThreadPoolFile::Open(FileCreateDisposition create_disposition, const FileOptions& options,
                            ThreadPoolIoHandler* handler, bool* exists) {
  DWORD flags = FILE_FLAG_RANDOM_ACCESS | FILE_FLAG_OVERLAPPED;
  if(options.unbuffered) {
    flags |= FILE_FLAG_NO_BUFFERING;
  }
  RETURN_NOT_OK(File::Open(flags, create_disposition, exists));
  if(exists && !*exists) {
    return Status::Ok;
  }

  io_object_ = ::CreateThreadpoolIo(file_handle_, handler->IoCompletionCallback, nullptr,
                                    handler->callback_environment());
  if(!io_object_) {
    Close();
    return Status::IOError;
  }
  return Status::Ok;
}

Status ThreadPoolFile::Read(size_t offset, uint32_t length, uint8_t* buffer,
                            IAsyncContext& context, AsyncIOCallback callback) const {
  DCHECK_ALIGNMENT(offset, length, buffer);
#ifdef IO_STATISTICS
  ++read_count_;
  bytes_read_ += length;
#endif
  return const_cast<ThreadPoolFile*>(this)->ScheduleOperation(FileOperationType::Read, buffer,
         offset, length, context, callback);
}

Status ThreadPoolFile::Write(size_t offset, uint32_t length, const uint8_t* buffer,
                             IAsyncContext& context, AsyncIOCallback callback) {
  DCHECK_ALIGNMENT(offset, length, buffer);
#ifdef IO_STATISTICS
  bytes_written_ += length;
#endif
  return ScheduleOperation(FileOperationType::Write, const_cast<uint8_t*>(buffer), offset, length,
                           context, callback);
}

Status ThreadPoolFile::ScheduleOperation(FileOperationType operationType, uint8_t* buffer,
    size_t offset, uint32_t length, IAsyncContext& context, AsyncIOCallback callback) {
  auto io_context = alloc_context<ThreadPoolIoHandler::IoCallbackContext>(sizeof(
                      ThreadPoolIoHandler::IoCallbackContext));
  if(!io_context.get()) return Status::OutOfMemory;

  IAsyncContext* caller_context_copy;
  RETURN_NOT_OK(context.DeepCopy(caller_context_copy));

  new(io_context.get()) ThreadPoolIoHandler::IoCallbackContext(offset, caller_context_copy,
      callback);

  ::StartThreadpoolIo(io_object_);

  bool success = FALSE;
  if(FileOperationType::Read == operationType) {
    success = ::ReadFile(file_handle_, buffer, length, nullptr, &io_context->parent_overlapped);
  } else {
    success = ::WriteFile(file_handle_, buffer, length, nullptr, &io_context->parent_overlapped);
  }
  if(!success) {
    DWORD win32_result = ::GetLastError();
    // Any error other than ERROR_IO_PENDING means the IO failed. Otherwise it will finish
    // asynchronously on the threadpool
    if(ERROR_IO_PENDING != win32_result) {
      ::CancelThreadpoolIo(io_object_);
      std::stringstream ss;
      ss << "Failed to schedule async IO: " << FormatWin32AndHRESULT(win32_result);
      fprintf(stderr, "%s\n", ss.str().c_str());
      return Status::IOError;
    }
  }
  io_context.release();
  return Status::Ok;
}

bool QueueIoHandler::TryComplete() {
  DWORD bytes_transferred;
  ULONG_PTR completion_key;
  LPOVERLAPPED overlapped = NULL;
  bool succeeded = ::GetQueuedCompletionStatus(io_completion_port_, &bytes_transferred,
                   &completion_key, &overlapped, 0);
  if(overlapped) {
    Status return_status;
    if(!succeeded) {
      return_status = Status::IOError;
    } else {
      return_status = Status::Ok;
    }
    auto callback_context = make_context_unique_ptr<IoCallbackContext>(
                              reinterpret_cast<IoCallbackContext*>(overlapped));
    callback_context->callback(callback_context->caller_context, return_status, bytes_transferred);
    return true;
  } else {
    return false;
  }
}

int QueueIoHandler::QueueRun(int timeout_secs) {
    DWORD bytes_transferred;
    ULONG_PTR completion_key;
    LPOVERLAPPED overlapped = NULL;
    bool succeeded = ::GetQueuedCompletionStatus(io_completion_port_, &bytes_transferred,
        &completion_key, &overlapped, timeout_secs * 1000);
    if (overlapped) {
        Status return_status;
        if (!succeeded) {
            return_status = Status::IOError;
        }
        else {
            return_status = Status::Ok;
        }
        auto callback_context = make_context_unique_ptr<IoCallbackContext>(
            reinterpret_cast<IoCallbackContext*>(overlapped));
        callback_context->callback(callback_context->caller_context, return_status, bytes_transferred);
        return 1;
    }
    else {
        return 0;
    }
}

Status QueueFile::Open(FileCreateDisposition create_disposition, const FileOptions& options,
                       QueueIoHandler* handler, bool* exists) {
  DWORD flags = FILE_FLAG_RANDOM_ACCESS | FILE_FLAG_OVERLAPPED;
  if(options.unbuffered) {
    flags |= FILE_FLAG_NO_BUFFERING;
  }
  RETURN_NOT_OK(File::Open(flags, create_disposition, exists));
  if(exists && !*exists) {
    return Status::Ok;
  }

  handler->AssociateFile(file_handle_);
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
  auto io_context = alloc_context<QueueIoHandler::IoCallbackContext>(sizeof(
                      QueueIoHandler::IoCallbackContext));
  if(!io_context.get()) return Status::OutOfMemory;

  IAsyncContext* caller_context_copy;
  RETURN_NOT_OK(context.DeepCopy(caller_context_copy));

  new(io_context.get()) QueueIoHandler::IoCallbackContext(offset, caller_context_copy,
      callback);

  bool success = FALSE;
  if(FileOperationType::Read == operationType) {
    success = ::ReadFile(file_handle_, buffer, length, nullptr, &io_context->parent_overlapped);
  } else {
    success = ::WriteFile(file_handle_, buffer, length, nullptr, &io_context->parent_overlapped);
  }
  if(!success) {
    DWORD win32_result = ::GetLastError();
    // Any error other than ERROR_IO_PENDING means the IO failed. Otherwise it will finish
    // asynchronously on the threadpool
    if(ERROR_IO_PENDING != win32_result) {
      std::stringstream ss;
      ss << "Failed to schedule async IO: " << FormatWin32AndHRESULT(win32_result) <<
         ", handle " << std::to_string((uint64_t)file_handle_);
      fprintf(stderr, "%s\n", ss.str().c_str());
      return Status::IOError;
    }
  }
  io_context.release();
  return Status::Ok;
}

#undef DCHECK_ALIGNMENT

}
} // namespace FASTER::environment