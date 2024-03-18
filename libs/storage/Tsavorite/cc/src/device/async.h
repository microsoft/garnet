// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <mutex>
#include <thread>
#include <condition_variable>

#include "auto_ptr.h"
#include "status.h"

namespace FASTER {
namespace core {

#define RETURN_NOT_OK(s) do { \
    Status _s = (s); \
    if (_s != Status::Ok) return _s; \
  } while (0)

class IAsyncContext;

/// Signature of the async callback for I/Os.
typedef void(*AsyncIOCallback)(IAsyncContext* context, Status result, size_t bytes_transferred);

/// Standard interface for contexts used by async callbacks.
class IAsyncContext {
 public:
  IAsyncContext()
    : from_deep_copy_{ false } {
  }

  virtual ~IAsyncContext() { }

  /// Contexts are initially allocated (as local variables) on the stack. When an operation goes
  /// async, it deep copies its context to a new heap allocation; this context must also deep copy
  /// its parent context, if any. Once a context has been deep copied, subsequent DeepCopy() calls
  /// just return the original, heap-allocated copy.
  Status DeepCopy(IAsyncContext*& context_copy) {
    if(from_deep_copy_) {
      // Already on the heap: nothing to do.
      context_copy = this;
      return Status::Ok;
    } else {
      RETURN_NOT_OK(DeepCopy_Internal(context_copy));
      context_copy->from_deep_copy_ = true;
      return Status::Ok;
    }
  }

  /// Whether the internal state for the async context has been copied to a heap-allocated memory
  /// block.
  bool from_deep_copy() const {
    return from_deep_copy_;
  }

 protected:
  /// Override this method to make a deep, persistent copy of your context. A context should:
  ///   1. Allocate memory for its copy. If the allocation fails, return Status::OutOfMemory.
  ///   2. If it has a parent/caller context, call DeepCopy() on that context. If the call fails,
  ///      free the memory it just allocated and return the call's error code.
  ///   3. Initialize its copy and return Status::Ok..
  virtual Status DeepCopy_Internal(IAsyncContext*& context_copy) = 0;

  /// A common pattern: deep copy, when context has no parent/caller context.
  template <class C>
  inline static Status DeepCopy_Internal(C& context, IAsyncContext*& context_copy) {
    context_copy = nullptr;
    auto ctxt = alloc_context<C>(sizeof(C));
    if(!ctxt.get()) return Status::OutOfMemory;
    new(ctxt.get()) C{ context };
    context_copy = ctxt.release();
    return Status::Ok;
  }
  /// Another common pattern: deep copy, when context has a parent/caller context.
  template <class C>
  inline static Status DeepCopy_Internal(C& context, IAsyncContext* caller_context,
                                         IAsyncContext*& context_copy) {
    context_copy = nullptr;
    auto ctxt = alloc_context<C>(sizeof(C));
    if(!ctxt.get()) return Status::OutOfMemory;
    IAsyncContext* caller_context_copy;
    RETURN_NOT_OK(caller_context->DeepCopy(caller_context_copy));
    new(ctxt.get()) C{ context, caller_context_copy };
    context_copy = ctxt.release();
    return Status::Ok;
  }

 private:
  /// Whether the internal state for the async context has been copied to a heap-allocated memory
  /// block.
  bool from_deep_copy_;
};

/// User-defined callbacks for async FASTER operations. Async callback equivalent of:
///   Status some_function(context* arg).
typedef void(*AsyncCallback)(IAsyncContext* ctxt, Status result);

/// Helper class, for use inside a continuation callback, that ensures the context will be freed
/// when the callback exits.
template <class C>
class CallbackContext {
 public:
  CallbackContext(IAsyncContext* context)
    : async{ false } {
    context_ = make_context_unique_ptr(static_cast<C*>(context));
  }

  ~CallbackContext() {
    if(async || !context_->from_deep_copy()) {
      // The callback went async again, or it never went async. The next callback or the caller is
      // responsible for freeing the context.
      context_.release();
    }
  }

  C* get() const {
    return context_.get();
  }
  C* operator->() const {
    return context_.get();
  }

 public:
  bool async;
 protected:
  context_unique_ptr_t<C> context_;
};

}
} // namespace FASTER::core