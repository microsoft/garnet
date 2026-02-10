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
            => registry.TryGetValue(taskType, out var taskInfo) && taskInfo.Task != null && !taskInfo.Task.IsCompleted;

        /// <summary>
        /// Check if task is still registered
        /// </summary>
        /// <param name="taskType"></param>
        /// <returns></returns>
        public bool IsRegistered(TaskType taskType)
            => registry.ContainsKey(taskType);

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
                Cancel(TaskPlacementCategory.All).Wait();
            }
            finally
            {
                cts.Dispose();
            }
        }

        /// <summary>
        /// Register and start new task using the provider taskType and taskFactory
        /// </summary>
        /// <param name="taskType">Task type</param>
        /// <param name="taskFactory">Task factory</param>
        /// <param name="cleanupOnCompletion">Whether to remove task from task manager registry on completion.</param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public bool RegisterAndRun(TaskType taskType, Func<CancellationToken, Task> taskFactory, bool cleanupOnCompletion = false)
        {
            try
            {
                // Acquire lock
                dispose.ReadLock();
                // Return early if instance is disposed
                if (disposed)
                    return false;

                // Try to add new task entry for provided taskType
                var taskMetadata = new TaskMetadata() { Cts = null, Task = null };
                if (!registry.TryAdd(taskType, taskMetadata))
                {
                    logger?.LogWarning("{taskType} already registered!", taskType);
                    return false;
                }

                // Create linked token
                taskMetadata.Cts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);

                // Execute task factory
                if (cleanupOnCompletion)
                    taskMetadata.Task = taskFactory(taskMetadata.Cts.Token).ContinueWith(async _ => await Cancel(taskType)).Unwrap();
                else
                    taskMetadata.Task = taskFactory(taskMetadata.Cts.Token);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Failed starting task {taskType} with {method}", taskType, nameof(RegisterAndRun));
                // Remove and cleanup registered entry when exception gets triggered
                Cancel(taskType).Wait();
                return false;
            }
            finally
            {
                dispose.ReadUnlock();
            }

            return true;
        }

        /// <summary>
        /// Cancel task associated with the provided TaskType
        /// </summary>
        /// <param name="taskType"></param>
        /// <returns></returns>
        public async Task Cancel(TaskType taskType)
        {
            if (registry.TryRemove(taskType, out var taskMetadata))
            {
                try
                {
                    using (taskMetadata.Cts)
                    {
                        taskMetadata.Cts.Cancel();
                        if (taskMetadata.Task != null)
                            await taskMetadata.Task.WaitAsync(disposed ? default : cts.Token);
                    }
                }
                catch (Exception ex)
                {
                    logger?.LogCritical(ex, "Unknown exception received for {Cancel} when awaiting for {taskType}.", nameof(Cancel), taskType);
                }
            }
        }

        /// <summary>
        /// Cancel tasks associated with the provided TaskPlacementCategory
        /// </summary>
        /// <param name="taskPlacementCategory"></param>
        /// <returns></returns>
        public async Task Cancel(TaskPlacementCategory taskPlacementCategory)
        {
            foreach (var taskType in TaskTypeExtensions.GetTaskTypes(taskPlacementCategory))
                await Cancel(taskType);
        }

        /// <summary>
        /// Wait for task associated with the provided TaskType to complete.
        /// </summary>
        /// <param name="taskType"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public bool Wait(TaskType taskType, CancellationToken token = default)
            => WaitAsync(taskType, token).Result;

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
                await taskInfo.Task.WaitAsync(token);
                return true;
            }
            return false;
        }
    }
}