// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Garnet.common
{
    /// <summary>
    /// Utility class for dealing with <see cref="Task"/>, <see cref="Task{TResult}"/>, <see cref="ValueTask"/>, and <see cref="ValueTask{TResult}"/>.
    /// </summary>
    public static class AsyncUtils
    {
        /// <summary>
        /// Block on a a given <see cref="Task"/>.
        /// 
        /// If at all possible, you should instead await your tasks, but for cases where this is not possible use this helper.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void BlockingWait(Task t)
        => t.GetAwaiter().GetResult();

        /// <summary>
        /// Block on a a given <see cref="Task{TResult}"/>.
        /// 
        /// If at all possible, you should instead await your tasks, but for cases where this is not possible use this helper.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T BlockingWait<T>(Task<T> t)
        => t.GetAwaiter().GetResult();

        /// <summary>
        /// Block on a a given <see cref="ValueTask"/>.
        /// 
        /// If at all possible, you should instead await your tasks, but for cases where this is not possible use this helper.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void BlockingWait(ValueTask t)
        => t.GetAwaiter().GetResult();

        /// <summary>
        /// Block on a a given <see cref="ValueTask{TResult}"/>..
        /// 
        /// If at all possible, you should instead await your tasks, but for cases where this is not possible use this helper.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T BlockingWait<T>(ValueTask<T> t)
        => t.GetAwaiter().GetResult();
    }
}