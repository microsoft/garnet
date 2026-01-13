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

    public enum TaskCategory
    {
        All,
        Generic,
        PrimaryOnly,
    }

    public struct TaskInfo
    {
        public TaskCategory taskCategory;
        public CancellationTokenSource cts;
        public Task task;
    }

    /// <summary>
    /// Create a new TaskManager instance
    /// </summary>
    /// <param name="logger"></param>
    public sealed class TaskManager(ILogger logger = null) : IDisposable
    {
        readonly CancellationTokenSource cts = new();
        readonly ConcurrentDictionary<TaskType, TaskInfo> tasks = new();
        readonly ILogger logger = logger;
        SingleWriterMultiReaderLock dispose = new();

        /// <summary>
        /// Dispose TaskManager instance
        /// </summary>
        public void Dispose()
        {
            // Finalize lock to avoid double dispose
            if (!dispose.TryWriteLock())
                return;

            // Cancel global cts
            cts.Cancel();
            TryCancelAllTasks().Wait();
            cts.Dispose();
        }

        /// <summary>
        /// Register and start new task using the provider taskType and taskFactory
        /// </summary>
        /// <param name="taskType"></param>
        /// <param name="taskFactory"></param>
        /// <param name="taskCategory"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public void Register(TaskType taskType, TaskCategory taskCategory, Func<CancellationToken, Task> taskFactory)
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
                if (!tasks.TryAdd(taskType, new TaskInfo() { cts = linkedCts, taskCategory = taskCategory, task = null }))
                {
                    linkedCts.Cancel();
                    linkedCts.Dispose();
                    throw new Exception("Failed to add task to registry!");
                }

                // Finally run the task and update the registry
                var task = Task.Run(async () => await taskFactory(linkedCts.Token), linkedCts.Token);
                if (!tasks.TryUpdate(taskType, new TaskInfo() { cts = linkedCts, taskCategory = taskCategory, task = task }, new TaskInfo() { cts = linkedCts, taskCategory = taskCategory, task = null }))
                {
                    linkedCts.Cancel();
                    task.Wait();
                    linkedCts.Dispose();
                    throw new Exception("Failed to update registry with running task!");
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
        /// <param name="taskCategory"></param>
        /// <returns></returns>
        public async Task TryCancelTask(TaskType taskType, TaskCategory taskCategory)
        {
            // TryGetValue task from registry
            if (tasks.TryGetValue(taskType, out var taskInfo) && (taskInfo.taskCategory == taskCategory || taskCategory == TaskCategory.All) && tasks.TryRemove(taskType, out taskInfo))
            {
                taskInfo.cts.Cancel();
                try
                {
                    if (taskInfo.task != null)
                        await taskInfo.task;
                }
                catch (OperationCanceledException)
                {
                    // Expected when cancelling the task
                }
                taskInfo.cts.Dispose();
            }
        }

        /// <summary>
        /// Try cancel all background tasks
        /// </summary>
        /// <returns></returns>
        /// <param name="taskCategory"></param>
        public async Task TryCancelAllTasks(TaskCategory taskCategory = TaskCategory.All)
        {
            foreach (var taskType in tasks.Keys)
                await TryCancelTask(taskType, taskCategory);
        }
    }
}