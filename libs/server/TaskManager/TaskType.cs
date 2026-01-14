// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Defines the types of background maintenance tasks that can be managed by the TaskManager.
    /// These tasks represent various server operations that run asynchronously to maintain
    /// system health, performance, and data integrity.
    /// </summary>
    public enum TaskType
    {
        /// <summary>
        /// Monitors AOF size and triggers checkpoints when size limit is exceeded.
        /// <para>See <see cref="StoreWrapper.AutoCheckpointBasedOnAofSizeLimit"/> for implementation.</para>
        /// </summary>
        AofSizeLimitTask,

        /// <summary>
        /// Periodically commits AOF data to ensure durability.
        /// <para>See <see cref="StoreWrapper.CommitTask"/> for implementation.</para>
        /// </summary>
        CommitTask,

        /// <summary>
        /// Performs log compaction to reclaim space from deleted records.
        /// <para>See <see cref="StoreWrapper.CompactionTask"/> for implementation.</para>
        /// </summary>
        CompactionTask,

        /// <summary>
        /// Collects expired members from object store collections.
        /// <para>See <see cref="StoreWrapper.ObjectCollectTask"/> for implementation.</para>
        /// </summary>
        ObjectCollectTask,

        /// <summary>
        /// Scans and removes expired keys from main and object stores.
        /// <para>See <see cref="StoreWrapper.ExpiredKeyDeletionScanTask"/> for implementation.</para>
        /// </summary>
        ExpiredKeyDeletionTask,

        /// <summary>
        /// Automatically grows hash table indexes when overflow thresholds are met.
        /// <para>See <see cref="StoreWrapper.IndexAutoGrowTask"/> for implementation.</para>
        /// </summary>
        IndexAutoGrowTask,
    }

    /// <summary>
    /// Provides extensions and utilities for TaskType to TaskPlacementCategory mapping.
    /// </summary>
    public static class TaskTypeExtensions
    {
        /// <summary>
        /// Dictionary mapping placement categories to arrays of task types that belong to each category.
        /// This provides efficient lookup of all tasks for a specific placement category.
        /// </summary>
        private static readonly Dictionary<TaskPlacementCategory, TaskType[]> CategoryToTasksMapping = new()
        {
            [TaskPlacementCategory.Primary] = [
                TaskType.AofSizeLimitTask,
                TaskType.CommitTask,
                TaskType.CompactionTask,
                TaskType.ObjectCollectTask,
                TaskType.ExpiredKeyDeletionTask
            ],
            [TaskPlacementCategory.Replica] = []
        };

        /// <summary>
        /// Contains all possible values of the TaskType enumeration.
        /// </summary>
        private static readonly TaskType[] AllTaskTypes = [.. Enum.GetValues<TaskType>()];

        /// <summary>
        /// Retrieves the collection of TaskType values associated with the specified task placement category.
        /// </summary>
        /// <param name="taskPlacementCategory">The category of task placement to retrieve task types for.</param>
        /// <returns>An enumerable collection of TaskType values for the given category.</returns>
        /// <exception cref="GarnetException">Thrown when the specified task placement category is invalid.</exception>
        public static IEnumerable<TaskType> GetTaskTypes(TaskPlacementCategory taskPlacementCategory)
        {
            if (taskPlacementCategory == TaskPlacementCategory.All)
            {
                foreach (var taskType in AllTaskTypes)
                    yield return taskType;
            }
            else
            {
                if (CategoryToTasksMapping.TryGetValue(taskPlacementCategory, out var taskTypes))
                {
                    foreach (var taskType in taskTypes)
                        yield return taskType;
                }
                else
                {
                    throw new GarnetException($"Invalid TaskPlacementCategory {taskPlacementCategory}");
                }

            }
        }
    }
}