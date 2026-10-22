// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// Decides whether work that may wait on its thread has to move to the thread pool.
    /// </summary>
    /// <remarks>
    /// Work may wait on a thread pool thread: the pool adds threads while its own are held. Any other thread serves
    /// more than the work in hand. A socket event thread that runs completions inline receives for every connection
    /// assigned to it, and a storage device's completion thread completes the reads of every session.
    /// </remarks>
    public static class ThreadPoolHop
    {
        /// <summary>
        /// Whether work that may wait on its thread has to leave the calling thread, which is not a thread pool thread.
        /// </summary>
        public static bool Required
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => !Thread.CurrentThread.IsThreadPoolThread;
        }
    }
}