// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include <cstdint>
#include <experimental/filesystem>
#include <mutex>
#include <string>

#include "gc_state.h"
#include "guid.h"
#include "light_epoch.h"
#include "utility.h"
#include "file.h"

/// Wrapper that exposes files to FASTER. Encapsulates segmented files, etc.

namespace FASTER {
namespace device {

template <class H, uint64_t S>
class FileSystemDisk;

template <class H>
class FileSystemFile {
 public:
  typedef H handler_t;
  typedef typename handler_t::async_file_t file_t;

  /// Default constructor
  FileSystemFile()
    : file_{}
    , file_options_{} {
  }

  FileSystemFile(const std::string& filename, const environment::FileOptions& file_options)
    : file_{ filename }
    , file_options_{ file_options } {
  }

  /// Move constructor.
  FileSystemFile(FileSystemFile&& other)
    : file_{ std::move(other.file_) }
    , file_options_{ other.file_options_ } {
  }

  /// Move assignment operator.
  FileSystemFile& operator=(FileSystemFile&& other) {
    file_ = std::move(other.file_);
    file_options_ = other.file_options_;
    return *this;
  }

  core::Status Open(handler_t* handler) {
    return file_.Open(FASTER::environment::FileCreateDisposition::OpenOrCreate, file_options_,
                      handler, nullptr);
  }
  core::Status Close() {
    return file_.Close();
  }
  core::Status Delete() {
    return file_.Delete();
  }

  uint64_t size() const {
	return file_.size();
  }

  void Truncate(uint64_t new_begin_offset, core::GcState::truncate_callback_t callback) {
    // Truncation is a no-op.
    if(callback) {
      callback(new_begin_offset);
    }
  }

  core::Status ReadAsync(uint64_t source, void* dest, uint32_t length,
                   core::AsyncIOCallback callback, core::IAsyncContext& context) const {
    return file_.Read(source, length, reinterpret_cast<uint8_t*>(dest), context, callback);
  }
  core::Status WriteAsync(const void* source, uint64_t dest, uint32_t length,
                    core::AsyncIOCallback callback, core::IAsyncContext& context) {
    return file_.Write(dest, length, reinterpret_cast<const uint8_t*>(source), context, callback);
  }

  size_t alignment() const {
    return file_.device_alignment();
  }

 private:
  file_t file_;
  environment::FileOptions file_options_;
};

// Similar to std::lock_guard, but allows manual early unlock
//
class ReleasableLockGuard
{
public:
    ReleasableLockGuard(std::mutex* mutex)
    {
        m_mutex = mutex;
        m_mutex->lock();
        m_released = false;
    }

    ~ReleasableLockGuard()
    {
        if (!m_released)
        {
            m_mutex->unlock();
        }
    }

    void Unlock()
    {
        assert(!m_released);
        m_mutex->unlock();
        m_released = true;
    }

private:
    std::mutex* m_mutex;
    bool m_released;
};
 
/// Manages a bundle of segment files.
template <class H>
class FileSystemSegmentBundle {
 public:
  typedef H handler_t;
  typedef FileSystemFile<handler_t> file_t;
  typedef FileSystemSegmentBundle<handler_t> bundle_t;

  FileSystemSegmentBundle(const std::string& filename,
                          const environment::FileOptions& file_options, handler_t* handler,
                          uint64_t begin_segment_, uint64_t end_segment_)
    : filename_{ filename }
    , file_options_{ file_options }
    , begin_segment{ begin_segment_ }
    , end_segment{ end_segment_ }
    , owner_{ true } {
    for(uint64_t idx = begin_segment; idx < end_segment; ++idx) {
      new(files() + (idx - begin_segment)) file_t{ filename_ + "." + std::to_string(idx),
          file_options_ };
      core::Status result = file(idx).Open(handler);
      assert(result == core::Status::Ok);
    }
  }

  FileSystemSegmentBundle(handler_t* handler, uint64_t begin_segment_, uint64_t end_segment_,
                          bundle_t& other)
    : filename_{ std::move(other.filename_) }
    , file_options_{ other.file_options_ }
    , begin_segment{ begin_segment_ }
    , end_segment{ end_segment_ }
    , owner_{ true } {
    assert(end_segment >= other.end_segment);

    uint64_t begin_new = begin_segment;
    uint64_t begin_copy = std::max(begin_segment, other.begin_segment);
    uint64_t end_copy = std::min(end_segment, other.end_segment);
    uint64_t end_new = end_segment;

    for(uint64_t idx = begin_segment; idx < begin_copy; ++idx) {
      new(files() + (idx - begin_segment)) file_t{ filename_ + "." + std::to_string(idx),
          file_options_ };
      core::Status result = file(idx).Open(handler);
      assert(result == core::Status::Ok);
    }
    for(uint64_t idx = begin_copy; idx < end_copy; ++idx) {
      // Move file handles for segments already opened.
      new(files() + (idx - begin_segment)) file_t{ std::move(other.file(idx)) };
    }
    for(uint64_t idx = end_copy; idx < end_new; ++idx) {
      new(files() + (idx - begin_segment)) file_t{ filename_ + "." + std::to_string(idx),
          file_options_ };
      core::Status result = file(idx).Open(handler);
      assert(result == core::Status::Ok);
    }

    other.owner_ = false;
  }

  ~FileSystemSegmentBundle() {
    if(owner_) {
      for(uint64_t idx = begin_segment; idx < end_segment; ++idx) {
        file(idx).~file_t();
      }
    }
  }

  core::Status Close() {
    assert(owner_);
    core::Status result = core::Status::Ok;
    for(uint64_t idx = begin_segment; idx < end_segment; ++idx) {
      core::Status r = file(idx).Close();
      if(r != core::Status::Ok) {
        // We'll report the last error.
        result = r;
      }
    }
    return result;
  }

  core::Status Delete() {
    assert(owner_);
    core::Status result = core::Status::Ok;
    for(uint64_t idx = begin_segment; idx < end_segment; ++idx) {
      core::Status r = file(idx).Delete();
      if(r != core::Status::Ok) {
        // We'll report the last error.
        result = r;
      }
    }
    return result;
  }

  file_t* files() {
    return reinterpret_cast<file_t*>(this + 1);
  }
  file_t& file(uint64_t segment) {
    assert(segment >= begin_segment);
    return files()[segment - begin_segment];
  }
  bool exists(uint64_t segment) const {
    return segment >= begin_segment && segment < end_segment;
  }

  static constexpr uint64_t size(uint64_t num_segments) {
    return sizeof(bundle_t) + num_segments * sizeof(file_t);
  }

 public:
  const uint64_t begin_segment;
  const uint64_t end_segment;
 private:
  std::string filename_;
  environment::FileOptions file_options_;
  bool owner_;
};

template <class H, uint64_t S>
class FileSystemSegmentedFile {
 public:
  typedef H handler_t;
  typedef FileSystemFile<H> file_t;
  typedef FileSystemSegmentBundle<handler_t> bundle_t;

  static constexpr uint64_t kSegmentSize = S;
  static_assert(core::Utility::IsPowerOfTwo(S), "template parameter S is not a power of two!");

  FileSystemSegmentedFile(const std::string& filename,
                          const environment::FileOptions& file_options, core::LightEpoch* epoch)
    : begin_segment_{ 0 }
    , files_{ nullptr }
    , handler_{ nullptr }
    , filename_{ filename }
    , file_options_{ file_options }
    , epoch_{ epoch } {
  }

  ~FileSystemSegmentedFile() {
    bundle_t* files = files_.load();
    if(files) {
      files->~bundle_t();
      std::free(files);
    }
  }

  core::Status Open(handler_t* handler) {
    handler_ = handler;
    return core::Status::Ok;
  }
  core::Status Close() {
      CloseAllSegments();
      return core::Status::Ok;
  }
  core::Status Delete() {
      bundle_t* files = files_.load();
      if (!files) return core::Status::Ok;
      return files->Delete();
  }
  void RemoveSegment(uint64_t segment) {
      auto truncate_callback = [](uint64_t offset) {
          };
      TruncateSegments(segment + 1, truncate_callback);
  }
  void Truncate(uint64_t new_begin_offset, core::GcState::truncate_callback_t callback) {
    uint64_t new_begin_segment = new_begin_offset / kSegmentSize;
    begin_segment_ = new_begin_segment;
    TruncateSegments(new_begin_segment, callback);
  }

  core::Status ReadAsync(uint64_t source, void* dest, uint32_t length, core::AsyncIOCallback callback,
                   core::IAsyncContext& context) const {
    uint64_t segment = source / kSegmentSize;
    assert(source % kSegmentSize + length <= kSegmentSize);

    bundle_t* files = files_.load();

    if(!files || !files->exists(segment)) {
      core::Status result = const_cast<FileSystemSegmentedFile<H, S>*>(this)->OpenSegment(segment);
      if(result != core::Status::Ok) {
        return result;
      }
      files = files_.load();
    }
    return files->file(segment).ReadAsync(source % kSegmentSize, dest, length, callback, context);
  }

  core::Status WriteAsync(const void* source, uint64_t dest, uint32_t length,
                    core::AsyncIOCallback callback, core::IAsyncContext& context) {
    uint64_t segment = dest / kSegmentSize;
    assert(dest % kSegmentSize + length <= kSegmentSize);

    bundle_t* files = files_.load();

    if(!files || !files->exists(segment)) {
      core::Status result = OpenSegment(segment);
      if(result != core::Status::Ok) {
        return result;
      }
      files = files_.load();
    }
    return files->file(segment).WriteAsync(source, dest % kSegmentSize, length, callback, context);
  }

  size_t alignment() const {
    return 512; // For now, assume all disks have 512-bytes alignment.
  }

  uint64_t size(uint64_t segment) {
      bundle_t* files = files_.load();

      if (!files || !files->exists(segment)) {
          core::Status result = OpenSegment(segment);
          if (result != core::Status::Ok) {
              return 0;
          }
          files = files_.load();
      }
      return files->file(segment).size();
  }

 private:
  core::Status OpenSegment(uint64_t segment) {
    class Context : public core::IAsyncContext {
     public:
      Context(void* files_)
        : files{ files_ } {
      }
      /// The deep-copy constructor.
      Context(const Context& other)
        : files{ other.files} {
      }
     protected:
      core::Status DeepCopy_Internal(core::IAsyncContext*& context_copy) final {
        return core::IAsyncContext::DeepCopy_Internal(*this, context_copy);
      }
     public:
      void* files;
    };

    auto callback = [](core::IAsyncContext* ctxt) {
      core::CallbackContext<Context> context{ ctxt };
      std::free(context->files);
    };

    // Only one thread can modify the list of files at a given time.
    ReleasableLockGuard lock{ &mutex_ };
    bundle_t* files = files_.load();

    if(segment < begin_segment_) {
      // The requested segment has been truncated.
      return core::Status::IOError;
    }
    if(files && files->exists(segment)) {
      // Some other thread already opened this segment for us.
      return core::Status::Ok;
    }

    if(!files) {
      // First segment opened.
      void* buffer = std::malloc(bundle_t::size(1));
      bundle_t* new_files = new(buffer) bundle_t{ filename_, file_options_, handler_,
          segment, segment + 1 };
      files_.store(new_files);
      return core::Status::Ok;
    }

    // Expand the list of files_.
    uint64_t new_begin_segment = std::min(files->begin_segment, segment);
    uint64_t new_end_segment = std::max(files->end_segment, segment + 1);
    void* buffer = std::malloc(bundle_t::size(new_end_segment - new_begin_segment));
    bundle_t* new_files = new(buffer) bundle_t{ handler_, new_begin_segment, new_end_segment,
        *files };
    files_.store(new_files);
    // Delete the old list only after all threads have finished looking at it.
    Context context{ files };
    core::IAsyncContext* context_copy;
    core::Status result = context.DeepCopy(context_copy);
    assert(result == core::Status::Ok);
    // unlock the lock before calling BumpCurrentEpoch(),
    // which may call completion callbacks which call this function again,
    // resulting in self-deadlock.
    //
    lock.Unlock();
    epoch_->BumpCurrentEpoch(callback, context_copy);
    return core::Status::Ok;
  }

  void TruncateSegments(uint64_t new_begin_segment, core::GcState::truncate_callback_t caller_callback) {
    class Context : public core::IAsyncContext {
     public:
      Context(bundle_t* files_, uint64_t new_begin_segment_,
              core::GcState::truncate_callback_t caller_callback_)
        : files{ files_ }
        , new_begin_segment{ new_begin_segment_ }
        , caller_callback{ caller_callback_ } {
      }
      /// The deep-copy constructor.
      Context(const Context& other)
        : files{ other.files }
        , new_begin_segment{ other.new_begin_segment }
        , caller_callback{ other.caller_callback } {
      }
     protected:
      core::Status DeepCopy_Internal(core::IAsyncContext*& context_copy) final {
        return core::IAsyncContext::DeepCopy_Internal(*this, context_copy);
      }
     public:
      bundle_t* files;
      uint64_t new_begin_segment;
      core::GcState::truncate_callback_t caller_callback;
    };

    auto callback = [](core::IAsyncContext* ctxt) {
      core::CallbackContext<Context> context{ ctxt };
      for(uint64_t idx = context->files->begin_segment; idx < context->new_begin_segment; ++idx) {
        file_t& file = context->files->file(idx);
        file.Close();
        file.Delete();
      }
      std::free(context->files);
      if(context->caller_callback) {
        context->caller_callback(context->new_begin_segment * kSegmentSize);
      }
    };

    // Only one thread can modify the list of files at a given time.
    ReleasableLockGuard lock{ &mutex_ };
    bundle_t* files = files_.load();
    assert(files);
    if(files->begin_segment >= new_begin_segment) {
      // Segments have already been truncated.
      if(caller_callback) {
        caller_callback(files->begin_segment * kSegmentSize);
      }
      return;
    }

    // Make a copy of the list, excluding the files to be truncated.
    void* buffer = std::malloc(bundle_t::size(files->end_segment - new_begin_segment));
    bundle_t* new_files = new(buffer) bundle_t{ handler_, new_begin_segment, files->end_segment,
        *files };
    files_.store(new_files);
    // Delete the old list only after all threads have finished looking at it.
    Context context{ files, new_begin_segment, caller_callback };
    core::IAsyncContext* context_copy;
    core::Status result = context.DeepCopy(context_copy);
    assert(result == core::Status::Ok);
    // unlock the lock before calling BumpCurrentEpoch(),
    // which may call completion callbacks which call this function again,
    // resulting in self-deadlock.
    //
    lock.Unlock();
    epoch_->BumpCurrentEpoch(callback, context_copy);
  }

  void CloseAllSegments() {
      class Context : public core::IAsyncContext {
      public:
          Context(bundle_t* files_)
              : files{ files_ } {
          }
          /// The deep-copy constructor.
          Context(const Context& other)
              : files{ other.files } {
          }
      protected:
          core::Status DeepCopy_Internal(core::IAsyncContext*& context_copy) final {
              return core::IAsyncContext::DeepCopy_Internal(*this, context_copy);
          }
      public:
          bundle_t* files;
      };

      auto callback = [](core::IAsyncContext* ctxt) {
          core::CallbackContext<Context> context{ ctxt };
          for (uint64_t idx = context->files->begin_segment; idx < context->files->end_segment; ++idx) {
              file_t& file = context->files->file(idx);
              file.Close();
          }
          std::free(context->files);
          };

      // Only one thread can modify the list of files at a given time.
      ReleasableLockGuard lock{ &mutex_ };
      bundle_t* files = files_.load();
      if (!files) {
          return;
      }

      // Store nullptr in files_
      files_.store(nullptr);
      // Delete the old list only after all threads have finished looking at it.
      Context context{ files };
      core::IAsyncContext* context_copy;
      core::Status result = context.DeepCopy(context_copy);
      assert(result == core::Status::Ok);
      // unlock the lock before calling BumpCurrentEpoch(),
      // which may call completion callbacks which call this function again,
      // resulting in self-deadlock.
      //
      lock.Unlock();
      epoch_->BumpCurrentEpoch(callback, context_copy);
  }

  std::atomic<uint64_t> begin_segment_;
  std::atomic<bundle_t*> files_;
  handler_t* handler_;
  std::string filename_;
  environment::FileOptions file_options_;
  core::LightEpoch* epoch_;
  std::mutex mutex_;
};

template <class H, uint64_t S>
class FileSystemDisk {
 public:
  typedef H handler_t;
  typedef FileSystemFile<handler_t> file_t;
  typedef FileSystemSegmentedFile<handler_t, S> log_file_t;

 private:
  static std::string NormalizePath(std::string root_path) {
    if(root_path.empty() || root_path.back() != FASTER::environment::kPathSeparator[0]) {
      root_path += FASTER::environment::kPathSeparator;
    }
    return root_path;
  }

 public:
  FileSystemDisk(const std::string& root_path, core::LightEpoch& epoch,
                 const std::string& config = "",
                 bool enablePrivileges = false, bool unbuffered = true,
                 bool delete_on_close = false)
    : root_path_{ NormalizePath(root_path) }
    , handler_{ 16 /*max threads*/ }
    , default_file_options_{ unbuffered, delete_on_close }
    , log_{ root_path_ + "log.log", default_file_options_, &epoch} {
    core::Status result = log_.Open(&handler_);
    assert(result == core::Status::Ok);
  }

  /// Methods required by the (implicit) disk interface.
  uint32_t sector_size() const {
    return static_cast<uint32_t>(log_.alignment());
  }

  const log_file_t& log() const {
    return log_;
  }
  log_file_t& log() {
    return log_;
  }

  std::string relative_index_checkpoint_path(const core::Guid& token) const {
    std::string retval = "index-checkpoints";
    retval += FASTER::environment::kPathSeparator;
    retval += token.ToString();
    retval += FASTER::environment::kPathSeparator;
    return retval;
  }
  std::string index_checkpoint_path(const core::Guid& token) const {
    return root_path_ + relative_index_checkpoint_path(token);
  }

  std::string relative_cpr_checkpoint_path(const core::Guid& token) const {
    std::string retval = "cpr-checkpoints";
    retval += FASTER::environment::kPathSeparator;
    retval += token.ToString();
    retval += FASTER::environment::kPathSeparator;
    return retval;
  }
  std::string cpr_checkpoint_path(const core::Guid& token) const {
    return root_path_ + relative_cpr_checkpoint_path(token);
  }

  void CreateIndexCheckpointDirectory(const core::Guid& token) {
    std::string index_dir = index_checkpoint_path(token);
    std::experimental::filesystem::path path{ index_dir };
    try {
      std::experimental::filesystem::remove_all(path);
    } catch(std::experimental::filesystem::filesystem_error&) {
      // Ignore; throws when path doesn't exist yet.
    }
    std::experimental::filesystem::create_directories(path);
  }

  void CreateCprCheckpointDirectory(const core::Guid& token) {
    std::string cpr_dir = cpr_checkpoint_path(token);
    std::experimental::filesystem::path path{ cpr_dir };
    try {
      std::experimental::filesystem::remove_all(path);
    } catch(std::experimental::filesystem::filesystem_error&) {
      // Ignore; throws when path doesn't exist yet.
    }
    std::experimental::filesystem::create_directories(path);
  }

  file_t NewFile(const std::string& relative_path) {
    return file_t{ root_path_ + relative_path, default_file_options_ };
  }

  /// Implementation-specific accessor.
  handler_t& handler() {
    return handler_;
  }

  bool TryComplete() {
    return handler_.TryComplete();
  }

 private:
  std::string root_path_;
  handler_t handler_;

  environment::FileOptions default_file_options_;

  /// Store the log (contains all records).
  log_file_t log_;
};

}
} // namespace FASTER::device
