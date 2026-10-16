// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.test
{
    /// <summary>
    /// Records the thread a task's completion continuation runs on. The continuation runs inline on the thread
    /// that completes the task, so the recorded thread is the one that completed it.
    /// </summary>
    sealed class ResumeProbe
    {
        readonly ManualResetEventSlim resumed = new();
        readonly TimeSpan bound;

        internal int ThreadId { get; private set; } = -1;

        internal ResumeProbe(Task task, TimeSpan bound)
        {
            this.bound = bound;
            _ = task.ContinueWith(_ =>
            {
                ThreadId = Environment.CurrentManagedThreadId;
                resumed.Set();
            }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        }

        internal bool Wait() => resumed.Wait(bound);
    }
}