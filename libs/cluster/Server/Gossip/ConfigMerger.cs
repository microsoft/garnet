// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Garnet.server;

namespace Garnet.cluster
{
    /// <summary>
    /// Manages asynchronous config merging with a dual-queue architecture.
    /// One queue is actively being appended to while the other is being processed.
    /// Uses SingleWriterMultiReaderLock to allow parallel appends while maintaining lock-free queue swaps.
    /// </summary>
    public sealed class ConfigMerger
    {
        /// <summary>
        /// Represents a queue with associated entries and completion source.
        /// All entries in a queue share a single TCS that is set when processing completes.
        /// Uses ConcurrentQueue for thread-safe FIFO accumulation without needing external synchronization.
        /// </summary>
        private class QueueBundle
        {
            public ConcurrentQueue<ClusterConfig> Entries = new();
            public TaskCompletionSource<bool> Tcs = new();
        }

        // Configuration
        private readonly int processingIntervalMs;

        // Dual queue architecture
        private QueueBundle activeQueueBundle;
        private QueueBundle processingQueueBundle;
        private SingleWriterMultiReaderLock appendLock = new();

        // Suspension state - managed via lock acquisition
        private SingleWriterMultiReaderLock suspensionLock = new();

        private readonly ClusterProvider clusterProvider;
        private readonly ILogger logger;

        /// <summary>
        /// Suspends merge processing. Acquires the write lock to block background processing.
        /// </summary>
        public void SuspendMergeProcessing()
        {
            suspensionLock.WriteLock();
        }

        /// <summary>
        /// Resumes merge processing. Releases the write lock to allow background processing.
        /// </summary>
        public void ResumeMergeProcessing()
        {
            suspensionLock.WriteUnlock();
        }

        /// <summary>
        /// Initializes a new instance of ConfigManager.
        /// </summary>
        public ConfigMerger(
            ClusterProvider clusterProvider,
            int processingIntervalMs = 50,
            ILogger logger = null)
        {
            this.clusterProvider = clusterProvider;
            this.logger = logger;
            this.processingIntervalMs = processingIntervalMs;

            _ = clusterProvider.storeWrapper.TaskManager.RegisterAndRun(TaskType.ConfigMergerTask, (token) => ProcessMergeQueueAsync(token));

            // Initialize dual queue bundles
            activeQueueBundle = new QueueBundle();
            processingQueueBundle = new QueueBundle();
        }

        /// <summary>
        /// Queues a merge request for asynchronous processing.
        /// The config will be accumulated and merged with others in the active queue.
        /// Multiple callers can queue in parallel using reader locks.
        /// Returns a task associated with the active queue's TCS.
        /// When the queue is processed, all entries complete with the same result.
        /// </summary>
        internal Task<bool> EnqueueMergeRequestAsync(ClusterConfig senderConfig)
        {
            try
            {
                // Acquire reader lock to allow parallel appends
                appendLock.ReadLock();

                activeQueueBundle.Entries.Enqueue(senderConfig);
                return activeQueueBundle.Tcs.Task;
            }
            finally
            {
                appendLock.ReadUnlock();
            }
        }

        /// <summary>
        /// Main background task that processes accumulated merge batches.
        /// Atomically swaps queues using writer lock, allowing appends to continue in active queue.
        /// Merges all configs from processing queue and completes associated TaskCompletionSources.
        /// Compatible with TaskManager factory pattern.
        /// </summary>
        private async Task ProcessMergeQueueAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(processingIntervalMs, cancellationToken);

                    // Try to acquire suspension lock - if suspended, lock is held by suspension context
                    if (!suspensionLock.TryWriteLock())
                        continue;

                    try
                    {
                        // Atomically swap queues using writer lock
                        appendLock.WriteLock();
                        try
                        {
                            // If active queue is empty, nothing to process
                            if (activeQueueBundle.Entries.IsEmpty)
                                continue;

                            // Swap bundles: move active to processing, reuse old processing as new active
                            (activeQueueBundle, processingQueueBundle) = (processingQueueBundle, activeQueueBundle);
                        }
                        finally
                        {
                            appendLock.WriteUnlock();
                        }
                    }
                    finally
                    {
                        suspensionLock.WriteUnlock();
                    }

                    // Process the swapped queue outside the lock
                    ProcessQueueEntries();
                }
            }
            catch (OperationCanceledException)
            {
                logger?.LogInformation("ConfigMerger merge processor cancelled");
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "ConfigMerger merge processor failed");
            }
        }

        /// <summary>
        /// Processes all entries in the processing queue, merging configs and completing the queue's TCS.
        /// </summary>
        private void ProcessQueueEntries()
        {
            if (processingQueueBundle.Entries.IsEmpty)
                return;

            var success = false;
            try
            {
                // Dequeue first config from the queue
                if (!processingQueueBundle.Entries.TryDequeue(out var mergedConfig))
                    return;

                // Consolidate all config merge requests
                while (processingQueueBundle.Entries.TryDequeue(out var config))
                {
                    mergedConfig = mergedConfig
                        .Merge(config, clusterProvider.clusterManager.WorkerBanList, logger)
                        .HandleConfigEpochCollision(config, logger);
                }

                // Finally merge to local config
                _ = clusterProvider.clusterManager.TryMerge(mergedConfig);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error processing merge queue batch");
                success = false;
            }
            finally
            {
                // Complete all entries in this batch with the same result via the queue's TCS
                processingQueueBundle.Tcs.SetResult(success);
                // Queue should be empty after all items have been dequeued
                Debug.Assert(processingQueueBundle.Entries.IsEmpty, "Processing queue should be empty after dequeuing all entries");
                // Create new TCS for next batch
                processingQueueBundle.Tcs = new TaskCompletionSource<bool>();
            }
        }

        /// <summary>
        /// Gracefully shuts down the ConfigManager and background processor.
        /// </summary>
        public void Dispose()
        {
            // Process any remaining items in the active queue before shutdown
            try
            {
                clusterProvider.storeWrapper.TaskManager.Cancel(TaskType.ConfigMergerTask).GetAwaiter().GetResult();
                suspensionLock.WriteLock();
                appendLock.WriteLock();
                try
                {
                    if (!activeQueueBundle.Entries.IsEmpty)
                    {
                        // Move any pending items to processing queue
                        (activeQueueBundle, processingQueueBundle) = (processingQueueBundle, activeQueueBundle);
                    }
                }
                finally
                {
                    appendLock.WriteUnlock();
                }

                // Process remaining items
                if (!processingQueueBundle.Entries.IsEmpty)
                    ProcessQueueEntries();
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error during ConfigManager shutdown");
            }
        }
    }
}