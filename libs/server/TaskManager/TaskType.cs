// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

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
        /// Array mapping task types to their placement categories by enum index.
        /// </summary>
        private static readonly TaskPlacementCategory[] TaskPlacementMapping =
        [
            TaskPlacementCategory.Primary,  // AofSizeLimitTask
            TaskPlacementCategory.Primary,  // CommitTask
            TaskPlacementCategory.Primary,  // CompactionTask
            TaskPlacementCategory.Primary,  // ObjectCollectTask
            TaskPlacementCategory.Primary,  // ExpiredKeyDeletionTask
            TaskPlacementCategory.All,      // IndexAutoGrowTask
        ];

        /// <summary>
        /// Retrieves task types for a placement category.
        /// </summary>
        /// <param name="lookupPlacementCategory">The placement category to filter by.</param>
        /// <returns>An enumerable of task types that match the placement category.</returns>
        public static IEnumerable<TaskType> GetTaskTypes(TaskPlacementCategory lookupPlacementCategory)
        {
            // Iterate through all task types in order (no allocations)
            for (var i = 0; i < TaskPlacementMapping.Length; i++)
            {
                var taskType = (TaskType)i;
                var taskPlacement = TaskPlacementMapping[i];

                // Check if this task matches the requested placement
                if (MatchPlacementCategory(taskPlacement, lookupPlacementCategory))
                {
                    yield return taskType;
                }
            }
        }

        /// <summary>
        /// Determines if a task placement matches the lookup placement category.
        /// </summary>
        /// <param name="taskPlacementCategory">The task's assigned placement.</param>
        /// <param name="lookupPlacementCategory">The lookup placement category.</param>
        /// <returns>True if the task can run on the lookup placement.</returns>
        private static bool MatchPlacementCategory(TaskPlacementCategory taskPlacementCategory, TaskPlacementCategory lookupPlacementCategory)
        {
            // Tasks marked as "All" can run anywhere
            if (taskPlacementCategory == TaskPlacementCategory.All || lookupPlacementCategory == TaskPlacementCategory.All)
                return true;

            // Check if the lookup placement includes this task's placement
            return (lookupPlacementCategory & taskPlacementCategory) == taskPlacementCategory;
        }
    }
}