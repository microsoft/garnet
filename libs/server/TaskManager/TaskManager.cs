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
    /// <summary>
    /// Create a new TaskManager instance
    /// </summary>
    /// <param name="logger"></param>
    public sealed class TaskManager(ILogger logger = null) : IDisposable
    {
        readonly CancellationTokenSource cts = new();
        readonly ConcurrentDictionary<TaskType, TaskMetadata> registry = new();
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
            CancelTask(TaskPlacementCategory.All).Wait();
            cts.Dispose();
        }

        /// <summary>
        /// Register and start new task using the provider taskType and taskFactory
        /// </summary>
        /// <param name="taskType"></param>
        /// <param name="taskFactory"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public bool RegisterAndRun(TaskType taskType, Func<CancellationToken, Task> taskFactory)
        {
            if (!dispose.TryReadLock())
                return false;

            var failed = false;
            TaskMetadata taskInfo = null;
            try
            {
                // Create registry entry
                taskInfo = new TaskMetadata() { cts = null, task = null };

                // Try to add new task entry for provided taskType
                if (!registry.TryAdd(taskType, taskInfo))
                {
                    logger?.LogError("{taskType} already registered!", taskType);
                    return false;
                }

                // Update entry with linked token and run task
                taskInfo.cts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
                taskInfo.task = taskFactory(taskInfo.cts.Token);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Failed starting task {taskType} with {method}", taskType, nameof(RegisterAndRun));
                failed = true;
            }
            finally
            {
                if (failed)
                {
                    try
                    {
                        taskInfo?.cts.Cancel();
                        taskInfo?.task.Wait();
                    }
                    finally
                    {
                        taskInfo?.cts.Dispose();
                    }
                }
                dispose.ReadUnlock();
            }
            return true;
        }

        /// <summary>
        /// Cancel task associated with the provided TaskType
        /// </summary>
        /// <param name="taskType"></param>
        /// <returns></returns>
        public async Task CancelTask(TaskType taskType)
        {
            if (registry.TryRemove(taskType, out var taskInfo))
            {
                try
                {
                    await taskInfo?.cts.CancelAsync();
                    await taskInfo?.task;
                }
                finally
                {
                    taskInfo?.cts.Dispose();
                }
            }
        }

        /// <summary>
        /// Cancel task
        /// </summary>
        /// <param name="taskPlacementCategory"></param>
        /// <returns></returns>
        public async Task CancelTask(TaskPlacementCategory taskPlacementCategory)
        {
            foreach (var taskType in TaskTypeExtensions.GetTaskTypes(taskPlacementCategory))
                await CancelTask(taskType);
        }
    }
}