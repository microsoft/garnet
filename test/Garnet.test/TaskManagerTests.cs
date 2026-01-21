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

            taskManager.RegisterAndRun(TaskType.AofSizeLimitTask, primaryCategoryTaskFactory);
            taskManager.RegisterAndRun(TaskType.IndexAutoGrowTask, allCategoryTaskFactory);


            ClassicAssert.IsTrue(primaryTaskStarted.Wait(TimeSpan.FromSeconds(5)), "Primary task should start");
            ClassicAssert.IsTrue(allTaskStarted.Wait(TimeSpan.FromSeconds(5)), "All task should start");

            if (cancelTaskPlacementCategory == TaskPlacementCategory.Replica)
            {
                // Cancel tasks based on replica placement category
                await taskManager.Cancel(cancelTaskPlacementCategory);

                // Tasks not of replica category so they should keep running
                ClassicAssert.IsTrue(taskManager.IsRunning(TaskType.AofSizeLimitTask));

                // Cancel all tasks
                await taskManager.Cancel(TaskPlacementCategory.All);
            }
            else
            {
                // Cancel tasks based on placement category
                await taskManager.Cancel(cancelTaskPlacementCategory);
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

        [Test]
        public void TestTaskRegisterAfterDispose()
        {
            var taskManager = new TaskManager();
            taskManager.Dispose();

            var registered = taskManager.RegisterAndRun(TaskType.IndexAutoGrowTask, (token) => Task.CompletedTask);
            ClassicAssert.IsFalse(registered);
        }

        [Test]
        public void TestTaskFactoryException([Values] bool cleanupOnCompletion)
        {
            using var taskManager = new TaskManager();

            // Create a task factory that throws an exception
            Task ThrowingTaskFactory(CancellationToken token)
            {
                throw new InvalidOperationException("Test exception from task factory");
            }

            // RegisterAndRun should return false when the task factory throws an exception
            var registered = taskManager.RegisterAndRun(TaskType.IndexAutoGrowTask, ThrowingTaskFactory, cleanupOnCompletion);
            ClassicAssert.IsFalse(registered, "RegisterAndRun should return false when task factory throws an exception");

            // Verify that the task is not running after the exception
            ClassicAssert.IsFalse(taskManager.IsRunning(TaskType.IndexAutoGrowTask), "Task should not be running after exception");
        }

        [Test]
        public void TestCleanupOnCompletion()
        {
            using var taskManager = new TaskManager();

            var registered = taskManager.RegisterAndRun(TaskType.IndexAutoGrowTask, (token) => Task.CompletedTask, cleanupOnCompletion: true);
            ClassicAssert.IsTrue(registered);

            while (taskManager.IsRegistered(TaskType.IndexAutoGrowTask)) { }
            ClassicAssert.IsFalse(taskManager.IsRegistered(TaskType.IndexAutoGrowTask));
        }

        [Test]
        public void TestCleanupWithException()
        {
            using var taskManager = new TaskManager();
            var taskCanRun = new SemaphoreSlim(0);

            Task taskFactory(CancellationToken token) => ThrowingTaskFactory(token);

            // Create a task factory that throws an exception
            async Task ThrowingTaskFactory(CancellationToken token)
            {
                await taskCanRun.WaitAsync(token);
                throw new InvalidOperationException("Test exception from task factory");
            }

            var registered = taskManager.RegisterAndRun(TaskType.IndexAutoGrowTask, taskFactory, cleanupOnCompletion: true);
            ClassicAssert.IsTrue(registered, "RegisterAndRun should return true");

            taskCanRun.Release();

            while (taskManager.IsRegistered(TaskType.IndexAutoGrowTask)) { }
            ClassicAssert.IsFalse(taskManager.IsRegistered(TaskType.IndexAutoGrowTask));
        }
    }
}