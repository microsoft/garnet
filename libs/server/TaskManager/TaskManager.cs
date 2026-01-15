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
        bool disposed = false;

        /// <summary>
        /// Check if task associated with provided TaskType is running
        /// </summary>
        /// <param name="taskType"></param>
        /// <returns></returns>
        public bool IsRunning(TaskType taskType)
            => registry.TryGetValue(taskType, out var taskInfo) && taskInfo.task != null && !taskInfo.task.IsCompleted;

        /// <summary>
        /// Dispose TaskManager instance
        /// </summary>
        public void Dispose()
        {
            try
            {
                dispose.WriteLock();
                if (disposed)
                    return;
                disposed = true;
            }
            finally
            {
                dispose.WriteUnlock();
            }

            cts.Cancel();
            try
            {
                CancelTasks(TaskPlacementCategory.All).Wait();
            }
            finally
            {
                cts.Dispose();
            }
        }

        /// <summary>
        /// Register and start new task using the provider taskType and taskFactory
        /// </summary>
        /// <param name="taskType"></param>
        /// <param name="taskFactory"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public void RegisterAndRun(TaskType taskType, Func<CancellationToken, Task> taskFactory)
        {
            if (!dispose.TryReadLock())
                return;

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
                    return;
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
                        taskInfo?.task.Wait(disposed ? default : cts.Token);
                    }
                    catch (Exception ex)
                    {
                        logger?.LogCritical(ex, "Unknown exception received for {RegisterAndRun} at finally.", nameof(RegisterAndRun));
                    }
                    finally
                    {
                        taskInfo?.cts.Dispose();
                    }
                }
                dispose.ReadUnlock();
            }
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
                    await taskInfo?.task.WaitAsync(disposed ? default : cts.Token);
                }
                catch (Exception ex)
                {
                    logger?.LogCritical(ex, "Unknown exception received for {CancelTask}.", nameof(CancelTask));
                }
                finally
                {
                    taskInfo?.cts.Dispose();
                }
            }
        }

        /// <summary>
        /// Cancel tasks associated with the provided TaskPlacementCategory
        /// </summary>
        /// <param name="taskPlacementCategory"></param>
        /// <returns></returns>
        public async Task CancelTasks(TaskPlacementCategory taskPlacementCategory)
        {
            foreach (var taskType in TaskTypeExtensions.GetTaskTypes(taskPlacementCategory))
                await CancelTask(taskType);
        }

        /// <summary>
        /// Wait for task associated with the provided TaskType to complete.
        /// </summary>
        /// <param name="taskType"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public bool Wait(TaskType taskType, CancellationToken token = default)
        {
            if (registry.TryGetValue(taskType, out var taskInfo))
            {
                taskInfo.task.Wait(token);
                return true;
            }
            return false;
        }

        /// <summary>
        /// WaitAsync for task associated with the provided TaskType to complete.
        /// </summary>
        /// <param name="taskType"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<bool> WaitAsync(TaskType taskType, CancellationToken token = default)
        {
            if (registry.TryGetValue(taskType, out var taskInfo))
            {
                await taskInfo.task.WaitAsync(token);
                return true;
            }
            return false;
        }
    }
}