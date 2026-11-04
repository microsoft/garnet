// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include <cstdint>
#include "filesystem_compat.h"
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
  core::Status Empty() {
    return file_.Empty();
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
                          uint64_t begin_segment_, uint64_t end_segment_,
                          bool omit_segment_id)
    : filename_{ filename }
    , file_options_{ file_options }
    , begin_segment{ begin_segment_ }
    , end_segment{ end_segment_ }
    , omit_segment_id_{ omit_segment_id }
    , owner_{ true }
    , open_status_{ core::Status::Ok } {
    for(uint64_t idx = begin_segment; idx < end_segment; ++idx) {
      new(files() + (idx - begin_segment)) file_t{ segment_path(idx),
          file_options_ };
      core::Status result = file(idx).Open(handler);
      assert(result == core::Status::Ok);
      // The assert is compiled out in Release, so capture the first non-Ok status so the
      // caller (FileSystemSegmentedFile::OpenSegment) can detect a partially-opened
      // bundle and refuse to commit it to files_. Without this guard a chmod-0 parent
      // directory produced a bundle with fd_=-1, which the subsequent WriteAsync would
      // submit to io_submit with aio_fildes=-1 — empirically hanging inside libaio on
      // some kernels instead of returning -EBADF synchronously.
      if (result != core::Status::Ok && open_status_ == core::Status::Ok) {
        open_status_ = result;
      }
    }
  }

  FileSystemSegmentBundle(handler_t* handler, uint64_t begin_segment_, uint64_t end_segment_,
                          bundle_t& other)
    // Copy (do not move) filename_ from `other`. If a new-segment Open() fails below the
    // bundle is rejected by OpenSegment, but `files_` still points to `other`, so `other`
    // must remain fully intact — including its filename_ which subsequent OpenSegment
    // calls read via segment_path().
    : filename_{ other.filename_ }
    , file_options_{ other.file_options_ }
    , begin_segment{ begin_segment_ }
    , end_segment{ end_segment_ }
    , omit_segment_id_{ other.omit_segment_id_ }
    , owner_{ true }
    , open_status_{ core::Status::Ok } {
    assert(end_segment >= other.end_segment);

    uint64_t begin_copy = std::max(begin_segment, other.begin_segment);
    uint64_t end_copy = std::min(end_segment, other.end_segment);
    uint64_t end_new = end_segment;

    // Phase 1: open NEW segments only (the leading range [begin_segment, begin_copy)
    // and the trailing range [end_copy, end_new)). `other` is not touched here, so
    // if any Open() fails we can safely abort the construction without corrupting the
    // previously-published bundle that `files_` still references. Track which slots we
    // have constructed so the failure path destroys only those (not the still-empty
    // middle slots [begin_copy, end_copy) reserved for the move below).
    uint64_t leading_constructed_end = begin_segment;
    uint64_t trailing_constructed_end = end_copy;

    for(uint64_t idx = begin_segment; idx < begin_copy; ++idx) {
      new(files() + (idx - begin_segment)) file_t{ segment_path(idx),
          file_options_ };
      leading_constructed_end = idx + 1;
      core::Status result = file(idx).Open(handler);
      if (result != core::Status::Ok) {
        open_status_ = result;
        break;
      }
    }

    if (open_status_ == core::Status::Ok) {
      for(uint64_t idx = end_copy; idx < end_new; ++idx) {
        new(files() + (idx - begin_segment)) file_t{ segment_path(idx),
            file_options_ };
        trailing_constructed_end = idx + 1;
        core::Status result = file(idx).Open(handler);
        if (result != core::Status::Ok) {
          open_status_ = result;
          break;
        }
      }
    }

    if (open_status_ != core::Status::Ok) {
      // Roll back: destroy the file_t objects we just constructed (this Close()s any
      // fds that opened successfully via file_t's destructor) and disable the bundle
      // destructor so it does not iterate over uninitialised middle slots. `other` is
      // left completely intact — owner_ was never transferred away from it.
      for(uint64_t idx = begin_segment; idx < leading_constructed_end; ++idx) {
        file(idx).~file_t();
      }
      for(uint64_t idx = end_copy; idx < trailing_constructed_end; ++idx) {
        file(idx).~file_t();
      }
      owner_ = false;
      return;
    }

    // Phase 2: all new opens succeeded — now consume `other`. Move file handles for the
    // overlapping range and transfer bundle ownership. If we crash between these steps
    // (impossible for the move and assignment below — they are noexcept), the rollback
    // is unnecessary.
    for(uint64_t idx = begin_copy; idx < end_copy; ++idx) {
      new(files() + (idx - begin_segment)) file_t{ std::move(other.file(idx)) };
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

  /// Returns the first non-Ok status captured during bundle construction (i.e., the first
  /// per-segment file.Open() that returned an error). Status::Ok means every segment
  /// opened successfully and the bundle is safe to commit to files_. OpenSegment must
  /// check this before publishing a freshly-constructed bundle: committing a bundle that
  /// contains an unopened (fd_=-1) file would later route an io_submit to an invalid
  /// descriptor.
  core::Status open_status() const {
    return open_status_;
  }

  /// Build the on-disk filename for `segment`. With `omit_segment_id_` false (the default),
  /// segments are named `<base>.<segmentId>` so multiple segments live side-by-side. With
  /// `omit_segment_id_` true, all writes go to the bare `<base>` filename — only meaningful
  /// in single-segment / unbounded mode (the upstream wrapper enforces that constraint).
  std::string segment_path(uint64_t idx) const {
    return segment_path(filename_, omit_segment_id_, idx);
  }

  /// The on-disk filename for segment `idx` of the file `filename`; see the overload above.
  static std::string segment_path(const std::string& filename, bool omit_segment_id, uint64_t idx) {
    return omit_segment_id ? filename : (filename + "." + std::to_string(idx));
  }

 public:
  const uint64_t begin_segment;
  const uint64_t end_segment;
 private:
  std::string filename_;
  environment::FileOptions file_options_;
  bool omit_segment_id_;
  bool owner_;
  /// First non-Ok status returned by any per-segment file.Open() during ctor. Status::Ok
  /// while every Open succeeded; sticky after the first failure so the caller can refuse
  /// to commit a partially-opened bundle to files_.
  core::Status open_status_;
};

template <class H>
class FileSystemSegmentedFile {
 public:
  typedef H handler_t;
  typedef FileSystemFile<H> file_t;
  typedef FileSystemSegmentBundle<handler_t> bundle_t;

  /// Runtime segment size. Set once at construction and never mutated thereafter.
  uint64_t segment_size() const { return segment_size_; }

  FileSystemSegmentedFile(const std::string& filename,
                          const environment::FileOptions& file_options,
                          core::LightEpoch* epoch,
                          uint64_t segment_size,
                          bool omit_segment_id)
    : begin_segment_{ 0 }
    , files_{ nullptr }
    , handler_{ nullptr }
    , filename_{ filename }
    , file_options_{ file_options }
    , epoch_{ epoch }
    , segment_size_{ segment_size }
    , segment_mask_{ segment_size - 1 }
    , segment_shift_{ ValidatedShift(segment_size) }
    , omit_segment_id_{ omit_segment_id } {
    // ValidatedShift threw if segment_size is non-positive or non-power-of-2 BEFORE the
    // mask/shift constants were observable. Once we get here, the constants are sane and
    // the hot-path uses `>> segment_shift_` / `& segment_mask_` (single register loads).
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
  /// Removes `segment` alone; the segments around it are unchanged. `*done` is set once its file is deleted or emptied:
  /// - If this does not hold it open, as when no IO has reached it since this was created or reset, its file is deleted now.
  /// - If it is the lowest segment this holds open, it is dropped from the open segments, and its file is closed and deleted
  ///   once no thread can still be using it, from the epoch drain list. `*done` is set there, possibly after this returns.
  /// - If it is open above the lowest, its file is emptied now, through its handle. The open segments must stay contiguous,
  ///   so the handle and the empty file stay until truncation passes the segment.
  void RemoveSegment(uint64_t segment, std::atomic<bool>* done) {
    // Holding the mutex keeps OpenSegment from opening the segment, and TruncateSegments from closing it, while it is removed
    ReleasableLockGuard lock{ &mutex_ };
    bundle_t* files = files_.load();
    if(!files || !files->exists(segment)) {
      // There is no handle to close. The file may not exist; any failure is ignored, as when an open segment is deleted.
      (void)file_t{ bundle_t::segment_path(filename_, omit_segment_id_, segment), file_options_ }.Delete();
      done->store(true);
      return;
    }
    if(segment > files->begin_segment) {
      // Any failure is ignored, as when a file is deleted
      (void)files->file(segment).Empty();
      done->store(true);
      return;
    }
    // The lowest open segment: truncating below segment + 1 drops it alone. It is open, so no unopened file needs deleting, and
    // the range of those, [lower, new begin), is empty.
    TruncateSegmentsLocked(lock, segment + 1, segment + 1, nullptr, done);
  }
  /// Removes every segment below `new_begin_segment`. The segments this holds open below it are dropped from the open
  /// segments, and their files are closed and deleted once no thread can still be using them, from the epoch drain list;
  /// `*done` is set after that, possibly after this returns. The files of the segments in [lower_segment, new_begin_segment)
  /// that this does not hold open are deleted now.
  void TruncateSegments(uint64_t lower_segment, uint64_t new_begin_segment, std::atomic<bool>* done) {
    ReleasableLockGuard lock{ &mutex_ };
    TruncateSegmentsLocked(lock, lower_segment, new_begin_segment, nullptr, done);
  }
  void Truncate(uint64_t new_begin_offset, core::GcState::truncate_callback_t callback) {
    uint64_t new_begin_segment = new_begin_offset >> segment_shift_;
    begin_segment_ = new_begin_segment;
    // Truncate removes only the segments this holds open, so the range of unopened files to delete, [lower, new begin), is
    // empty. NativeDeviceImpl does not call it.
    ReleasableLockGuard lock{ &mutex_ };
    TruncateSegmentsLocked(lock, new_begin_segment, new_begin_segment, callback, nullptr);
  }

  core::Status ReadAsync(uint64_t source, void* dest, uint32_t length, core::AsyncIOCallback callback,
                   core::IAsyncContext& context) const {
    // Hot path. The shift+mask resolves to two register-loads from this object (`segment_shift_`
    // and `segment_mask_` are const-after-construction and on the same cache line as other
    // members touched in this call) plus a `shr` + `and`. Equivalent codegen to the previous
    // `/ S` / `% S` because compile-time-constant S was already strength-reduced; the only
    // difference is the load of the runtime constants from `this` instead of an immediate.
    uint64_t segment = source >> segment_shift_;
    if ((source & segment_mask_) + length > segment_size_) {
      // Spans segment boundary — caller bug. Promoted from debug assert because hitting this in
      // production would silently mis-route the IO. Cold branch; predicted-not-taken.
      return core::Status::IOError;
    }

    bundle_t* files = files_.load();

    if(!files || !files->exists(segment)) {
      core::Status result = const_cast<FileSystemSegmentedFile<H>*>(this)->OpenSegment(segment);
      if(result != core::Status::Ok) {
        return result;
      }
      files = files_.load();
    }
    return files->file(segment).ReadAsync(source & segment_mask_, dest, length, callback, context);
  }

  core::Status WriteAsync(const void* source, uint64_t dest, uint32_t length,
                    core::AsyncIOCallback callback, core::IAsyncContext& context) {
    uint64_t segment = dest >> segment_shift_;
    if ((dest & segment_mask_) + length > segment_size_) {
      return core::Status::IOError;
    }

    bundle_t* files = files_.load();

    if(!files || !files->exists(segment)) {
      core::Status result = OpenSegment(segment);
      if(result != core::Status::Ok) {
        return result;
      }
      files = files_.load();
    }
    return files->file(segment).WriteAsync(source, dest & segment_mask_, length, callback, context);
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

  /// Reports the segments whose files this holds open, [begin, end); both are 0 when it holds none. The file of a segment
  /// outside that range can be deleted without affecting IO through this object, provided no IO to that segment is issued
  /// meanwhile.
  void open_segments(uint64_t& begin, uint64_t& end) {
    // files_ is replaced only under mutex_, and a bundle is freed only after it has been replaced, so the bundle read here
    // stays valid while mutex_ is held.
    std::lock_guard<std::mutex> lock{ mutex_ };
    bundle_t* files = files_.load();
    begin = files ? files->begin_segment : 0;
    end = files ? files->end_segment : 0;
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
          segment, segment + 1, omit_segment_id_ };
      // If any per-segment file.Open() inside the bundle ctor failed (e.g. EACCES on a
      // chmod-0 parent directory) the bundle holds at least one file with fd_=-1.
      // Submitting an io_submit with aio_fildes=-1 historically hangs inside libaio on
      // some kernels rather than returning -EBADF synchronously, which crashes the test
      // host and surfaces in production as a silently-stuck WriteAsync. Refuse to commit
      // the broken bundle to files_; the caller's WriteAsync returns Status::IOError
      // immediately, which routes through the AsyncIOCallback contract.
      if (new_files->open_status() != core::Status::Ok) {
        core::Status open_err = new_files->open_status();
        new_files->~bundle_t();
        std::free(buffer);
        return open_err;
      }
      files_.store(new_files);
      return core::Status::Ok;
    }

    // Expand the list of files_.
    uint64_t new_begin_segment = std::min(files->begin_segment, segment);
    uint64_t new_end_segment = std::max(files->end_segment, segment + 1);
    void* buffer = std::malloc(bundle_t::size(new_end_segment - new_begin_segment));
    bundle_t* new_files = new(buffer) bundle_t{ handler_, new_begin_segment, new_end_segment,
        *files };
    // The expand-bundle ctor opens new segments BEFORE moving handles out of `*files`,
    // so on failure `*files` is fully intact and still referenced by files_. Discard
    // the rejected new bundle and surface the open error to the caller.
    if (new_files->open_status() != core::Status::Ok) {
      core::Status open_err = new_files->open_status();
      new_files->~bundle_t();
      std::free(buffer);
      return open_err;
    }
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

  /// TruncateSegments, under mutex_, which `lock` holds; this releases it. `caller_callback`, if given, is called with the new
  /// begin address once the open segments' files are deleted, before `*done`, if given, is set.
  void TruncateSegmentsLocked(ReleasableLockGuard& lock, uint64_t lower_segment, uint64_t new_begin_segment,
                              core::GcState::truncate_callback_t caller_callback, std::atomic<bool>* done) {
    class Context : public core::IAsyncContext {
     public:
      Context(bundle_t* files_, uint64_t new_begin_segment_,
              core::GcState::truncate_callback_t caller_callback_,
              uint64_t segment_size_bytes_, std::atomic<bool>* done_)
        : files{ files_ }
        , new_begin_segment{ new_begin_segment_ }
        , caller_callback{ caller_callback_ }
        , segment_size_bytes{ segment_size_bytes_ }
        , done{ done_ } {
      }
      /// The deep-copy constructor.
      Context(const Context& other)
        : files{ other.files }
        , new_begin_segment{ other.new_begin_segment }
        , caller_callback{ other.caller_callback }
        , segment_size_bytes{ other.segment_size_bytes }
        , done{ other.done } {
      }
     protected:
      core::Status DeepCopy_Internal(core::IAsyncContext*& context_copy) final {
        return core::IAsyncContext::DeepCopy_Internal(*this, context_copy);
      }
     public:
      bundle_t* files;
      uint64_t new_begin_segment;
      core::GcState::truncate_callback_t caller_callback;
      // Captured at TruncateSegments() entry so the deferred drain-list callback (which fires
      // on an arbitrary thread after BumpCurrentEpoch) doesn't need to dereference `this`.
      uint64_t segment_size_bytes;
      std::atomic<bool>* done;
    };

    auto callback = [](core::IAsyncContext* ctxt) {
      core::CallbackContext<Context> context{ ctxt };
      // A new begin past the bundle's end truncates every segment in it
      uint64_t truncate_end = std::min(context->new_begin_segment, context->files->end_segment);
      for(uint64_t idx = context->files->begin_segment; idx < truncate_end; ++idx) {
        file_t& file = context->files->file(idx);
        file.Close();
        file.Delete();
      }
      std::free(context->files);
      if(context->caller_callback) {
        context->caller_callback(context->new_begin_segment * context->segment_size_bytes);
      }
      if(context->done) {
        // Last: the waiter can return as soon as this is set, ending the flag's lifetime
        context->done->store(true);
      }
    };

    bundle_t* files = files_.load();

    // Delete the files of the segments to remove that this does not hold open. Holding the mutex keeps OpenSegment from
    // opening one between the check and the deletion. The file may not exist; any failure is ignored, as when an open
    // segment is deleted.
    for(uint64_t idx = lower_segment; idx < new_begin_segment; ++idx) {
      if(!files || !files->exists(idx)) {
        (void)file_t{ bundle_t::segment_path(filename_, omit_segment_id_, idx), file_options_ }.Delete();
      }
    }

    if(!files || files->begin_segment >= new_begin_segment) {
      // No open segment lies below the new begin: segments have already been truncated, or none is open.
      uint64_t begin_address = (files ? files->begin_segment : new_begin_segment) * segment_size_;
      // Signal after releasing mutex_, as the deferred path below does, so the callback can call into this object: mutex_ is
      // not recursive
      lock.Unlock();
      if(caller_callback) {
        caller_callback(begin_address);
      }
      if(done) {
        done->store(true);
      }
      return;
    }

    // Make a copy of the list, excluding the files to be truncated. If none stays open, the next segment opened starts a new
    // list, rather than extending this one and reopening every segment between them.
    bundle_t* new_files = nullptr;
    if(new_begin_segment < files->end_segment) {
      void* buffer = std::malloc(bundle_t::size(files->end_segment - new_begin_segment));
      new_files = new(buffer) bundle_t{ handler_, new_begin_segment, files->end_segment,
          *files };
    }
    files_.store(new_files);
    // Delete the old list only after all threads have finished looking at it.
    Context context{ files, new_begin_segment, caller_callback, segment_size_, done };
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

  /// Runtime segment-size geometry. All three are set in the constructor and never mutated
  /// thereafter — declared const so the optimizer can keep them in registers across the hot
  /// path. segment_mask_ = segment_size_ - 1 and segment_shift_ = ctzll(segment_size_) so that
  /// `addr % segment_size == addr & segment_mask_` and `addr / segment_size == addr >> segment_shift_`.
  /// ValidatedShift below ensures segment_size_ is a positive power of 2 before computing
  /// segment_shift_ (`__builtin_ctzll(0)` is UB).
  const uint64_t segment_size_;
  const uint64_t segment_mask_;
  const uint32_t segment_shift_;

  /// When true, segment files are named just `<filename_>` with no `.<segmentId>` suffix.
  /// Only meaningful when there is a single segment in play (the upstream wrapper requires
  /// unbounded segment-size mode for this to be safe — otherwise multiple segments would all
  /// resolve to the same path and clobber each other).
  const bool omit_segment_id_;

  /// Validates that segment_size is a non-zero power of 2 and returns its ctzll. Throws
  /// std::invalid_argument otherwise — Release-mode active (the previous `static_assert` is
  /// gone because S is no longer a compile-time constant). Called from the member-initializer
  /// list; if it throws, the partially-constructed FileSystemSegmentedFile is never seen by
  /// the caller, which is the desired fail-fast behavior.
  static uint32_t ValidatedShift(uint64_t segment_size) {
    if (!core::Utility::IsPowerOfTwo(segment_size)) {
      throw std::invalid_argument(
        "FileSystemSegmentedFile: segment_size must be a positive power of two");
    }
    return core::Utility::CountTrailingZeros64(segment_size);
  }
};

template <class H, uint64_t S>
class FileSystemDisk {
 public:
  typedef H handler_t;
  typedef FileSystemFile<handler_t> file_t;
  typedef FileSystemSegmentedFile<handler_t> log_file_t;

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
    , log_{ root_path_ + "log.log", default_file_options_, &epoch, S, false } {
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
    tsv_fs::path path{ index_dir };
    try {
      tsv_fs::remove_all(path);
    } catch(tsv_fs::filesystem_error&) {
      // Ignore; throws when path doesn't exist yet.
    }
    tsv_fs::create_directories(path);
  }

  void CreateCprCheckpointDirectory(const core::Guid& token) {
    std::string cpr_dir = cpr_checkpoint_path(token);
    tsv_fs::path path{ cpr_dir };
    try {
      tsv_fs::remove_all(path);
    } catch(tsv_fs::filesystem_error&) {
      // Ignore; throws when path doesn't exist yet.
    }
    tsv_fs::create_directories(path);
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
