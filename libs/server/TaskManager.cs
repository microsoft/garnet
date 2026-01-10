// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    public enum TaskType
    {
        AofSizeLimitTask,
        CommitTask,
        CompactionTask,
        ObjectCollectTask,
        ExpiredKeyDeletionTask,
        IndexAutoGrowTask,
    }

    /// <summary>
    /// Create a new TaskManager instance
    /// </summary>
    /// <param name="logger"></param>
    public sealed class TaskManager(ILogger logger = null) : IDisposable
    {
        readonly CancellationTokenSource cts = new();
        readonly ConcurrentDictionary<TaskType, (CancellationTokenSource, Task)> tasks = new ();
        readonly ILogger logger = logger;
        SingleWriterMultiReaderLock dispose = new();

        /// <summary>
        /// Dispose TaskManager instance
        /// </summary>
        public void Dispose()
        {
            // Finalize lock to avoid double dispose
            if (dispose.TryWriteLock())
                return;

            // Cancel clobal task
            cts.Cancel();
            foreach(var taskType in tasks.Keys)
                TryCancelTask(taskType).Wait();
            cts.Dispose();
        }

        /// <summary>
        /// Register and start new task using the provider taskType and taskFactory
        /// </summary>
        /// <param name="taskType"></param>
        /// <param name="taskFactory"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public void Register(TaskType taskType, Func<CancellationToken, Task> taskFactory)
        {
            if (!dispose.TryReadLock())
                return;

            try
            {
                if (tasks.ContainsKey(taskType))
                {
                    logger?.LogError("Failed to initialize task because it already exists!");
                    return;
                }

                // Create a linked cts
                var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
                
                // Try to add task to registry
                if (!tasks.TryAdd(taskType, (linkedCts, null)))
                {
                    linkedCts.Cancel();
                    linkedCts.Dispose();
                    throw new Exception("Failed to register task!");
                }

                // Finally run the task and update the registry
                var task = Task.Run(async () => await taskFactory(linkedCts.Token), linkedCts.Token);
                if(!tasks.TryUpdate(taskType, (linkedCts, task), (linkedCts, null)))
                {
                    task.Wait();
                    throw new Exception("Failed to register task!");
                }
            }
            finally
            {
                dispose.ReadUnlock();
            }
        }

        /// <summary>
        /// Cancel task
        /// </summary>
        /// <param name="taskType"></param>
        /// <returns></returns>
        public async Task TryCancelTask(TaskType taskType)
        {
            // TryRemove task from registry and cancel it
            if (tasks.TryRemove(taskType, out var taskInfo))
            {
                taskInfo.Item1.Cancel();
                try
                {
                    await taskInfo.Item2;
                }
                catch (OperationCanceledException)
                {
                    // Expected when cancelling the task
                }
                taskInfo.Item1.Dispose();
            }
        }
    }
}
