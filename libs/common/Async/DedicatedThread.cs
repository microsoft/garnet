// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.common
{
    /// <summary>
    /// Runs work that holds its thread for long stretches, and may wait there on work the thread pool runs, on a thread
    /// of its own.
    /// </summary>
    /// <remarks>
    /// A thread pool thread that waits for other pool work needs another pool thread to be free, which a pool capped by
    /// its maximum thread count does not always have.
    /// </remarks>
    public static class DedicatedThread
    {
        /// <summary>
        /// Starts <paramref name="work"/> on a new background thread.
        /// </summary>
        /// <typeparam name="T">Type of the result</typeparam>
        /// <param name="name">Name of the thread</param>
        /// <param name="work">Work to run</param>
        /// <returns>
        /// A task that completes with the result of <paramref name="work"/>, or with the exception it throws. Its
        /// continuations do not run on the thread.
        /// </returns>
        public static Task<T> RunAsync<T>(string name, Func<T> work)
        {
            var done = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
            new Thread(() =>
            {
                try
                {
                    done.SetResult(work());
                }
                catch (Exception e)
                {
                    done.SetException(e);
                }
            })
            { IsBackground = true, Name = name }.Start();
            return done.Task;
        }
    }
}