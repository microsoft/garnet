// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Lockable context functions. Useful when doing generic locking across diverse 
    /// <see cref="LockableUnsafeContext{Key, Value, Input, Output, Context, Functions, StoreFunctions, Allocator}"/> and 
    /// <see cref="LockableContext{Key, Value, Input, Output, Context, Functions, StoreFunctions, Allocator}"/> specializations.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    public interface ILockableContext<TKey>
    {
        /// <summary>
        /// Begins a series of lock operations on possibly multiple keys; call before any locks are taken.
        /// </summary>
        void BeginTransaction();

        /// <summary>
        /// Ends a series of lock operations on possibly multiple keys; call after all locks are released.
        /// </summary>
        void EndTransaction();
    }
}