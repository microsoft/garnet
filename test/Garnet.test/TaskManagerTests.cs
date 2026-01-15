// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [TestFixture]
    internal class TaskManagerTests
    {
        [Test]
        public void TestBasicRegisterAndRun()
        {
            using var taskManager = new TaskManager();
            var taskStarted = new SemaphoreSlim(0);
            var taskCanComplete = new SemaphoreSlim(0);

            taskManager.RegisterAndRun(TaskType.IndexAutoGrowTask, async token =>
            {
                _ = taskStarted.Release();
                await taskCanComplete.WaitAsync(token);
            });

            ClassicAssert.IsTrue(taskStarted.Wait(TimeSpan.FromSeconds(5)), "Task should start within timeout");
            _ = taskCanComplete.Release();
            ClassicAssert.IsTrue(taskManager.Wait(TaskType.IndexAutoGrowTask));
        }

        [Test]
        public void TestDoubleRegistration()
        {
            using var taskManager = new TaskManager();
            var taskStarted = new SemaphoreSlim(0);
            var taskCanComplete = new SemaphoreSlim(0);
            var startedCounter = 0;

            Task taskFactory(CancellationToken token) => DummyTask(token);

            taskManager.RegisterAndRun(TaskType.IndexAutoGrowTask, taskFactory);
            taskManager.RegisterAndRun(TaskType.IndexAutoGrowTask, taskFactory);

            ClassicAssert.IsTrue(taskStarted.Wait(TimeSpan.FromSeconds(5)), "Task should start within timeout");
            _ = taskCanComplete.Release();

            ClassicAssert.IsTrue(taskManager.Wait(TaskType.IndexAutoGrowTask));
            ClassicAssert.AreEqual(1, startedCounter);

            async Task DummyTask(CancellationToken token)
            {
                _ = Interlocked.Increment(ref startedCounter);
                _ = taskStarted.Release();
                await taskCanComplete.WaitAsync(token);
            }
        }

        [Test]
        [TestCase(TaskPlacementCategory.Primary)]
        [TestCase(TaskPlacementCategory.All)]
        [TestCase(TaskPlacementCategory.Replica)]
        public async Task TestTaskPlacementCategoryCancellation(TaskPlacementCategory cancelTaskPlacementCategory)
        {
            using var taskManager = new TaskManager();
            var primaryTaskStarted = new SemaphoreSlim(0);
            var allTaskStarted = new SemaphoreSlim(0);

            Task primaryCategoryTaskFactory(CancellationToken token) => PrimaryCategoryTask(token);
            Task allCategoryTaskFactory(CancellationToken token) => AllCategoryTask(token);

            taskManager.RegisterAndRun(TaskType.IndexAutoGrowTask, primaryCategoryTaskFactory);
            taskManager.RegisterAndRun(TaskType.AofSizeLimitTask, allCategoryTaskFactory);


            ClassicAssert.IsTrue(primaryTaskStarted.Wait(TimeSpan.FromSeconds(5)), "Primary task should start");
            ClassicAssert.IsTrue(allTaskStarted.Wait(TimeSpan.FromSeconds(5)), "All task should start");

            if (cancelTaskPlacementCategory == TaskPlacementCategory.Replica)
            {
                // Cancel tasks based on placement category
                await taskManager.CancelTasks(cancelTaskPlacementCategory);

                // Tasks not of replica category so they should keep running
                ClassicAssert.IsTrue(taskManager.IsRunning(TaskType.IndexAutoGrowTask));
                ClassicAssert.IsTrue(taskManager.IsRunning(TaskType.AofSizeLimitTask));

                // Cancel all tasks
                await taskManager.CancelTasks(TaskPlacementCategory.All);
            }
            else
            {
                // Cancel tasks based on placement category
                await taskManager.CancelTasks(cancelTaskPlacementCategory);
            }

            // Both tasks should complete and be removed since they are in primary category
            ClassicAssert.IsFalse(taskManager.Wait(TaskType.AofSizeLimitTask));
            ClassicAssert.IsFalse(taskManager.Wait(TaskType.IndexAutoGrowTask));

            async Task PrimaryCategoryTask(CancellationToken token)
            {
                _ = primaryTaskStarted.Release();
                await Task.Delay(Timeout.Infinite, token);
            }

            async Task AllCategoryTask(CancellationToken token)
            {
                _ = allTaskStarted.Release();
                await Task.Delay(Timeout.Infinite, token);
            }
        }
    }
}