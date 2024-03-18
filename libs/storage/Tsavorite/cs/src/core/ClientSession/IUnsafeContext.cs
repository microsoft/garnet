// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Manual epoch control functions. Useful when doing generic operations across diverse <see cref="LockableUnsafeContext{Key, Value, Input, Output, Context, Functions}"/> 
    /// and <see cref="UnsafeContext{Key, Value, Input, Output, Context, Functions}"/> specializations.
    /// </summary>
    public interface IUnsafeContext
    {
        /// <summary>
        /// Resume session on current thread. IMPORTANT: Call <see cref="EndUnsafe"/> before any async op.
        /// </summary>
        void BeginUnsafe();

        /// <summary>
        /// Suspend session on current thread
        /// </summary>
        void EndUnsafe();
    }
}