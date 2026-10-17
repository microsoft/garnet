// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// Threads, apart from the thread pool's, for work that may hold its thread on synchronous blocking IO.
    /// </summary>
    public interface IOffloadExecutor
    {
        /// <summary>
        /// Whether work on the calling thread runs where it is rather than being posted: the thread is one of the
        /// executor's, or the executor has stopped and would hand the work back to the thread pool.
        /// </summary>
        bool RunsInPlace { get; }

        /// <summary>
        /// Runs <paramref name="item"/> on one of the executor's threads.
        /// </summary>
        /// <param name="item">Item to run.</param>
        void Post(IThreadPoolWorkItem item);
    }

    /// <summary>
    /// Awaitable that continues on a thread of an <see cref="IOffloadExecutor"/>.
    /// </summary>
    internal readonly struct OffloadHop(IOffloadExecutor executor) : ICriticalNotifyCompletion
    {
        public OffloadHop GetAwaiter() => this;

        public bool IsCompleted => false;

        public void GetResult() { }

        public void OnCompleted(Action continuation)
            => executor.Post(new Continuation(continuation, ExecutionContext.Capture()));

        public void UnsafeOnCompleted(Action continuation)
            => executor.Post(new Continuation(continuation, null));

        sealed class Continuation(Action continuation, ExecutionContext context) : IThreadPoolWorkItem
        {
            public void Execute()
            {
                if (context == null)
                    continuation();
                else
                    ExecutionContext.Run(context, static state => ((Action)state)(), continuation);
            }
        }
    }
}