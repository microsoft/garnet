// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable VSTHRD002 // The whole point of this class is to segregate problematic blocking

using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Garnet.common
{
    /// <summary>
    /// Utility class for dealing with <see cref="Task"/>, <see cref="Task{TResult}"/>, <see cref="ValueTask"/>, and <see cref="ValueTask{TResult}"/>.
    /// </summary>
    /// <remarks>
    /// Blocking is legitimate off the network completion thread: startup, recovery, background workers,
    /// cluster maintenance and tests each own the thread they block, and a wait there costs nothing but that
    /// thread. It is not legitimate on the network completion thread, where it holds a thread pool thread for
    /// the duration of the wait and so caps concurrent waiting sessions at the size of the pool. A session
    /// that has to wait parks instead, through <c>RespServerSession.BeginAsyncCommand</c>, which returns the
    /// completion thread to the pool and resumes the session when the wait is satisfied.
    /// </remarks>
    public static class AsyncUtils
    {
        /// <summary>
        /// Block on a given <see cref="Task"/>.
        /// 
        /// If at all possible, you should instead await your tasks, but for cases where this is not possible use this helper.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void BlockingWait(Task t)
        => t.GetAwaiter().GetResult();

        /// <summary>
        /// Block on a given <see cref="Task"/> for at most <paramref name="millisecondsTimeout"/> milliseconds.
        /// 
        /// If at all possible, you should instead await your tasks, but for cases where this is not possible use this helper.
        /// </summary>
        /// <param name="t">The task to wait for.</param>
        /// <param name="millisecondsTimeout">The maximum time to wait, or <see cref="System.Threading.Timeout.Infinite"/>.</param>
        /// <returns>True if the task completed within the timeout; otherwise false, and the task keeps running.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool BlockingWait(Task t, int millisecondsTimeout)
        => t.Wait(millisecondsTimeout);

        /// <summary>
        /// Reads the result of a value task the caller has already proven complete via
        /// <c>IsCompletedSuccessfully</c>. This is the synchronous arm of the dual-path idiom, where no
        /// blocking can occur; the name distinguishes it from a genuine blocking wait.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void GetResultGuarded(this ValueTaskAwaiter awaiter) => awaiter.GetResult();

        /// <inheritdoc cref="GetResultGuarded(ValueTaskAwaiter)"/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T GetResultGuarded<T>(this ValueTaskAwaiter<T> awaiter) => awaiter.GetResult();

        /// <summary>
        /// Block on a given <see cref="Task{TResult}"/>.
        /// 
        /// If at all possible, you should instead await your tasks, but for cases where this is not possible use this helper.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T BlockingWait<T>(Task<T> t)
        => t.GetAwaiter().GetResult();

        /// <summary>
        /// Block on a given <see cref="ValueTask"/>.
        /// 
        /// If at all possible, you should instead await your tasks, but for cases where this is not possible use this helper.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void BlockingWait(ValueTask t)
        => t.GetAwaiter().GetResult();

        /// <summary>
        /// Block on a given <see cref="ValueTask{TResult}"/>.
        /// 
        /// If at all possible, you should instead await your tasks, but for cases where this is not possible use this helper.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T BlockingWait<T>(ValueTask<T> t)
        => t.GetAwaiter().GetResult();
    }
}