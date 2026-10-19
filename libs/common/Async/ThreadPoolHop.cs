// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// Awaitable that continues on the thread pool, for work that may wait on its thread and has to leave one that
    /// must not wait.
    /// </summary>
    /// <remarks>
    /// Work may wait on a thread pool thread: the pool adds threads while its own are held. Any other thread serves
    /// more than the work in hand. A socket event thread that runs completions inline receives for every connection
    /// assigned to it, and a storage device's completion thread completes the reads of every session.
    /// </remarks>
    public readonly struct ThreadPoolHop : ICriticalNotifyCompletion
    {
        /// <summary>
        /// Whether work that may wait on its thread has to leave the calling thread, which is not a thread pool thread.
        /// </summary>
        public static bool Required
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => !Thread.CurrentThread.IsThreadPoolThread;
        }

        /// <summary>Gets the awaiter, which is the hop itself.</summary>
        public ThreadPoolHop GetAwaiter() => this;

        /// <summary>Always false: the continuation is queued.</summary>
        public bool IsCompleted => false;

        /// <summary>Ends the await.</summary>
        public void GetResult() { }

        /// <inheritdoc />
        public void OnCompleted(Action continuation)
            => ThreadPool.QueueUserWorkItem(static continuation => continuation(), continuation, preferLocal: false);

        /// <inheritdoc />
        public void UnsafeOnCompleted(Action continuation)
            => ThreadPool.UnsafeQueueUserWorkItem(static continuation => continuation(), continuation, preferLocal: false);
    }
}