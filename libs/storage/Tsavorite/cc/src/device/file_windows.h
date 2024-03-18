// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#ifdef _WIN32
#define NOMINMAX
#define _WINSOCKAPI_
#include <Windows.h>
#endif

#include <atomic>
#include <cstdint>
#include <string>

#include "async.h"
#include "status.h"
#include "file_common.h"

/// Windows file routines.

namespace FASTER {
namespace environment {
constexpr const char* kPathSeparator = "\\";

/// The File class encapsulates the OS file handle.
class File {
 protected:
  File()
    : file_handle_{ INVALID_HANDLE_VALUE }
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
    : file_handle_{ INVALID_HANDLE_VALUE }
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
    : file_handle_{ other.file_handle_ }
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
    file_handle_ = other.file_handle_;
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
  core::Status Open(DWORD flags, FileCreateDisposition create_disposition, bool* exists = nullptr);

 public:
  core::Status Close();
  core::Status Delete();

  uint64_t size() const {
    LARGE_INTEGER file_size;
    auto result = ::GetFileSizeEx(file_handle_, &file_size);
    return result ? file_size.QuadPart : 0;
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
  static DWORD GetCreateDisposition(FileCreateDisposition create_disposition);

 protected:
  HANDLE file_handle_;

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

class WindowsPtpThreadPool {
 public:
  typedef void(*Task)(void* arguments);

  WindowsPtpThreadPool()
    : pool_{ nullptr }
    , callback_environment_{ nullptr }
    , cleanup_group_{ nullptr }
    , max_threads_{ 0 } {
  }

  WindowsPtpThreadPool(size_t max_threads);

  /// Move constructor
  WindowsPtpThreadPool(WindowsPtpThreadPool&& other)
    : pool_{ other.pool_ }
    , callback_environment_{ other.callback_environment_ }
    , cleanup_group_{ other.cleanup_group_ }
    , max_threads_{ other.max_threads_ } {
    other.pool_ = nullptr;
    other.callback_environment_ = nullptr;
    other.cleanup_group_ = nullptr;
    other.max_threads_ = 0;
  }

  ~WindowsPtpThreadPool();

  core::Status Schedule(Task task, void* task_argument);

  PTP_CALLBACK_ENVIRON callback_environment() {
    return callback_environment_;
  }

 private:
  /// Describes a task that should be invoked. Created and enqueued in ScheduleTask(); dispatched
  /// and freed in TaskStartSpringboard().
  struct TaskInfo {
    TaskInfo()
      : task{}
      , task_parameters{} {
    }

    /// The task to be invoked when the work item is issued by the pool.
    Task task;

    /// Argument passed into #m_task when it is called.
    void* task_parameters;
  };

  /// Called asynchronously by a thread from #m_pool whenever the thread pool starts to execute a
  /// task scheduled via ScheduleTask(). Just determines which routine was requested for execution
  /// and calls it.
  static void CALLBACK TaskStartSpringboard(PTP_CALLBACK_INSTANCE instance, PVOID parameter,
      PTP_WORK work);

  /// A Window Thread Pool object that is used to run asynchronous IO
  /// operations (and callbacks) and other tasks  (scheduled via
  /// ScheduleTask()).
  PTP_POOL pool_;

  /// An environment that associates Windows Thread Pool IO and Task objects
  /// to #m_pool. AsyncIOFileWrappers and scheduled tasks are associated
  /// with this environments to schedule them for execution.
  PTP_CALLBACK_ENVIRON callback_environment_;

  /// The cleanup group associated with all environments and the thread pool.
  PTP_CLEANUP_GROUP cleanup_group_;

  /// Maximum number of threads the thread pool should allocate.
  uint64_t max_threads_;
};

class ThreadPoolFile;
class QueueFile;

/// The ThreadPoolIoHandler class encapsulates completions for async file I/O, scheduled on a
/// thread pool.
class ThreadPoolIoHandler {
 public:
  typedef ThreadPoolFile async_file_t;

  ThreadPoolIoHandler()
    : threadpool_{} {
  }

  ThreadPoolIoHandler(size_t max_threads)
    : threadpool_{ max_threads } {
  }

  /// Move constructor.
  ThreadPoolIoHandler(ThreadPoolIoHandler&& other)
    : threadpool_{ std::move(other.threadpool_) } {
  }

  /// Invoked whenever an asynchronous IO completes; needed because Windows asynchronous IOs are
  /// tied to a specific TP_IO object. As a result, we allocate pointers for a per-operation
  /// callback along with its OVERLAPPED structure. This allows us to call a specific function in
  /// response to each IO, without having to create a TP_IO for each of them.
  static void CALLBACK IoCompletionCallback(PTP_CALLBACK_INSTANCE instance, PVOID context,
      PVOID overlapped, ULONG ioResult, ULONG_PTR bytesTransferred, PTP_IO io);

  PTP_CALLBACK_ENVIRON callback_environment() {
    return threadpool_.callback_environment();
  }

  struct IoCallbackContext {
    IoCallbackContext(size_t offset, core::IAsyncContext* context_, core::AsyncIOCallback callback_)
      : caller_context{ context_ }
      , callback{ callback_ } {
      ::memset(&parent_overlapped, 0, sizeof(parent_overlapped));
      parent_overlapped.Offset = offset & 0xffffffffllu;
      parent_overlapped.OffsetHigh = offset >> 32;
    }

    // WARNING: parent_overlapped must be the first field in IOCallbackContext. This class is a
    // C-style subclass of "OVERLAPPED".

    /// The overlapped structure for Windows IO
    OVERLAPPED parent_overlapped;
    /// Caller callback context.
    core::IAsyncContext* caller_context;
    /// The caller's asynchronous callback function
    core::AsyncIOCallback callback;
  };

  inline static constexpr bool TryComplete() {
    return false;
  }
    
  inline static constexpr int QueueRun(int timeout_secs) {
      return -1; // disable queue IO
  }

 private:
  /// The parent threadpool.
  WindowsPtpThreadPool threadpool_;
};

/// The QueueIoHandler class encapsulates completions for async file I/O, where the completions
/// are put on a completion port's queue.
class QueueIoHandler {
 public:
  typedef QueueFile async_file_t;

  QueueIoHandler()
    : io_completion_port_{ INVALID_HANDLE_VALUE } {
  }
  QueueIoHandler(size_t max_threads)
    : io_completion_port_{ 0 } {
    io_completion_port_ = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0,
                          (DWORD)core::Thread::kMaxNumThreads);
  }

  /// Move constructor
  QueueIoHandler(QueueIoHandler&& other)
    : io_completion_port_{ other.io_completion_port_ } {
    other.io_completion_port_ = INVALID_HANDLE_VALUE;
  }

  ~QueueIoHandler() {
    if(io_completion_port_ != INVALID_HANDLE_VALUE) {
      ::CloseHandle(io_completion_port_);
    }
  }

  inline void AssociateFile(HANDLE file_handle) {
    assert(io_completion_port_ != 0);
    ::CreateIoCompletionPort(file_handle, io_completion_port_,
                             reinterpret_cast<uint64_t>(file_handle), 0);
  }

  struct IoCallbackContext {
    IoCallbackContext(size_t offset, core::IAsyncContext* context_, core::AsyncIOCallback callback_)
      : caller_context{ context_ }
      , callback{ callback_ } {
      ::memset(&parent_overlapped, 0, sizeof(parent_overlapped));
      parent_overlapped.Offset = offset & 0xffffffffllu;
      parent_overlapped.OffsetHigh = offset >> 32;
    }

    // WARNING: parent_overlapped must be the first field in IOCallbackContext. This class is a
    // C-style subclass of "OVERLAPPED".

    /// The overlapped structure for Windows IO
    OVERLAPPED parent_overlapped;
    /// Caller callback context.
    core::IAsyncContext* caller_context;
    /// The caller's asynchronous callback function
    core::AsyncIOCallback callback;
  };

  bool TryComplete();
  int QueueRun(int timeout_secs);

 private:
  /// The completion port to whose queue completions are added.
  HANDLE io_completion_port_;
};

/// The ThreadPoolFile class encapsulates asynchronous reads and writes, where the OS schedules the
/// IO completion on a thread pool.
class ThreadPoolFile : public File {
 public:
  ThreadPoolFile()
    : File()
    , io_object_{ nullptr } {
  }

  ThreadPoolFile(const std::string& filename)
    : File(filename)
    , io_object_{ nullptr } {
  }

  /// Move constructor
  ThreadPoolFile(ThreadPoolFile&& other)
    : File(std::move(other))
    , io_object_{ other.io_object_} {
  }

  /// Move assignment operator.
  ThreadPoolFile& operator=(ThreadPoolFile&& other) {
    File::operator=(std::move(other));
    io_object_ = other.io_object_;
    return *this;
  }

  core::Status Open(FileCreateDisposition create_disposition, const FileOptions& options,
              ThreadPoolIoHandler* handler, bool* exists = nullptr);

  core::Status Read(size_t offset, uint32_t length, uint8_t* buffer,
              core::IAsyncContext& context, core::AsyncIOCallback callback) const;
  core::Status Write(size_t offset, uint32_t length, const uint8_t* buffer,
               core::IAsyncContext& context, core::AsyncIOCallback callback);

 private:
  core::Status ScheduleOperation(FileOperationType operationType, uint8_t* buffer, size_t offset,
                           uint32_t length, core::IAsyncContext& context, core::AsyncIOCallback callback);

  PTP_IO io_object_;
};

/// The QueueFile class encapsulates asynchronous reads and writes, where the IO completions are
/// placed on the completion port's queue.
class QueueFile : public File {
 public:
  QueueFile()
    : File() {
  }
  QueueFile(const std::string& filename)
    : File(filename) {
  }
  /// Move constructor
  QueueFile(QueueFile&& other)
    : File(std::move(other)) {
  }

  /// Move assignment operator.
  QueueFile& operator=(QueueFile&& other) {
    File::operator=(std::move(other));
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
};

}
} // namespace FASTER::environment