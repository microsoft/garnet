// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    [TestFixture]
    [NonParallelizable]
    internal class LogCommitTaskObservationTests : TsavoriteLogTestBase
    {
        public enum DisposalState
        {
            Empty,
            Committed,
            Recovered
        }

        [SetUp]
        public void Setup() => BaseSetup(false);

        [TearDown]
        public void TearDown() => BaseTearDown();

        [Test]
        [Category("TsavoriteLog")]
        public void DisposeDoesNotPublishUnobservedCommitTask([Values] DisposalState state)
            => AssertNoUnobservedCommitTasks(() => CreateDisposedLog(state));

        [Test]
        [Category("TsavoriteLog")]
        public void DeviceFailureDoesNotPublishUnobservedCommitTask([Values] bool tolerateDeviceFailure)
            => AssertNoUnobservedCommitTasks(() => CreateFailedLog(tolerateDeviceFailure));

        [Test]
        [Category("TsavoriteLog")]
        [CancelAfter(10000)]
        public void DisposeFailsPendingCommitWaiter(CancellationToken cancellationToken)
        {
            device = Devices.CreateLogDevice(Path.Join(MethodTestDir, "waiter.log"), deleteOnClose: true);
            log = new TsavoriteLog(CreateSettings(device));
            _ = log.Enqueue(entry);
            var waiter = log.WaitForCommitAsync(token: cancellationToken).AsTask();
            Assert.That(waiter.IsCompleted, Is.False);

            log.Dispose();
            log = null;

            var exception = Assert.ThrowsAsync<ObjectDisposedException>(async () => await waiter.ConfigureAwait(false));
            Assert.That(exception.ObjectName, Is.EqualTo("TsavoriteLog has been disposed"));
        }

        [Test]
        [Category("TsavoriteLog")]
        [CancelAfter(10000)]
        public async Task DisposeEndsCommittedIteratorWait(CancellationToken cancellationToken)
        {
            device = Devices.CreateLogDevice(Path.Join(MethodTestDir, "iterator.log"), deleteOnClose: true);
            log = new TsavoriteLog(CreateSettings(device));
            using var iterator = log.Scan(log.BeginAddress, long.MaxValue);
            var waiter = iterator.WaitAsync(cancellationToken).AsTask();
            Assert.That(waiter.IsCompleted, Is.False);

            iterator.Dispose();
            log.Dispose();
            log = null;

            Assert.That(await waiter.ConfigureAwait(false), Is.False);
        }

        [Test]
        [Category("TsavoriteLog")]
        [CancelAfter(10000)]
        public async Task DeviceFailureStillReachesWaiters([Values] bool tolerateDeviceFailure, CancellationToken cancellationToken)
        {
            var failing = new RecordingFailingDevice(
                Devices.CreateLogDevice(Path.Join(MethodTestDir, "waiterFailure.log"), deleteOnClose: true));
            device = failing;
            var settings = CreateSettings(device);
            settings.TolerateDeviceFailure = tolerateDeviceFailure;
            log = new TsavoriteLog(settings);
            _ = log.Enqueue(entry);
            var waiter = log.WaitForCommitAsync(token: cancellationToken).AsTask();
            failing.FailWrites = true;

            var commitException = Assert.ThrowsAsync<CommitFailureException>(
                async () => await log.CommitAsync(token: cancellationToken).ConfigureAwait(false));
            var waiterException = Assert.ThrowsAsync<CommitFailureException>(
                async () => await waiter.ConfigureAwait(false));
            Assert.That(commitException.InnerException, Is.SameAs(failing.InjectedError));
            Assert.That(waiterException, Is.SameAs(commitException));

            if (tolerateDeviceFailure)
            {
                failing.FailWrites = false;
                _ = log.Enqueue(entry);
                await log.CommitAsync(token: cancellationToken).ConfigureAwait(false);
                Assert.That(log.CommittedUntilAddress, Is.EqualTo(log.TailAddress));
            }
            else
            {
                var syncException = Assert.Throws<CommitFailureException>(() => log.Commit());
                Assert.That(syncException, Is.SameAs(commitException));
                var futureException = Assert.ThrowsAsync<CommitFailureException>(
                    async () => await log.WaitForCommitAsync(token: cancellationToken).ConfigureAwait(false));
                Assert.That(futureException, Is.SameAs(commitException));
            }
        }

        private TsavoriteLogSettings CreateSettings(IDevice logDevice) => new()
        {
            LogDevice = logDevice,
            LogCommitManager = manager,
            PageSizeBits = 12,
            MemorySizeBits = 16,
            SegmentSizeBits = 20,
            TryRecoverLatest = false
        };

        private readonly record struct TaskProbe(int Id, WeakReference Reference)
        {
            internal TaskProbe(Task task) : this(task.Id, new WeakReference(task)) { }
        }

        private static void AssertNoUnobservedCommitTasks(Func<List<TaskProbe>> createTargets)
        {
            var unobserved = new ConcurrentDictionary<int, AggregateException>();
            void OnUnobserved(object sender, UnobservedTaskExceptionEventArgs args)
                => unobserved.TryAdd(((Task)sender).Id, args.Exception);

            TaskScheduler.UnobservedTaskException += OnUnobserved;
            try
            {
                var control = CreateUnobservedControl();
                var targets = createTargets();
                Assert.That(() =>
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    return !control.Reference.IsAlive && targets.All(target => !target.Reference.IsAlive);
                }, Is.True.After(10000, 100), "Commit-notification tasks must be collected to exercise finalization.");

                Assert.That(unobserved.ContainsKey(control.Id), Is.True,
                    "The deliberately unobserved control must reach the event handler.");
                foreach (var target in targets)
                {
                    Assert.That(unobserved.TryGetValue(target.Id, out var exception), Is.False,
                        $"An internally owned commit task raised UnobservedTaskException: {exception}");
                }
            }
            finally
            {
                TaskScheduler.UnobservedTaskException -= OnUnobserved;
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static TaskProbe CreateUnobservedControl()
            => new(Task.FromException(new InvalidOperationException("unobserved-task test control")));

        [MethodImpl(MethodImplOptions.NoInlining)]
        private List<TaskProbe> CreateDisposedLog(DisposalState state)
        {
            using var localDevice = Devices.CreateLogDevice(Path.Join(MethodTestDir, "dispose.log"), deleteOnClose: true);
            var settings = CreateSettings(localDevice);
            settings.FastCommitMode = true;
            var targets = new List<TaskProbe>();
            long committedTail;
            using (var localLog = new TsavoriteLog(settings))
            {
                if (state != DisposalState.Empty)
                {
                    var completedTask = localLog.CommitTask;
                    _ = localLog.Enqueue(entry);
                    localLog.CommitStrongly(out _, out _, spinWait: true);
                    Assert.That(() => completedTask.IsCompletedSuccessfully, Is.True.After(10000, 10));
                }
                committedTail = localLog.TailAddress;
                targets.Add(new TaskProbe(localLog.CommitTask));
            }

            if (state == DisposalState.Recovered)
            {
                settings.TryRecoverLatest = true;
                using var recovered = new TsavoriteLog(settings);
                Assert.That(recovered.TailAddress, Is.EqualTo(committedTail));
                using (var scan = recovered.Scan(recovered.BeginAddress, committedTail))
                {
                    Assert.That(scan.GetNext(out byte[] recoveredEntry, out _, out _), Is.True);
                    Assert.That(recoveredEntry, Is.EqualTo(entry));
                }
                targets.Add(new TaskProbe(recovered.CommitTask));
            }
            return targets;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private List<TaskProbe> CreateFailedLog(bool tolerateDeviceFailure)
        {
            using var failing = new RecordingFailingDevice(
                Devices.CreateLogDevice(Path.Join(MethodTestDir, "failure.log"), deleteOnClose: true));
            var settings = CreateSettings(failing);
            settings.TolerateDeviceFailure = tolerateDeviceFailure;
            using var localLog = new TsavoriteLog(settings);
            _ = localLog.Enqueue(entry);
            var failedTask = localLog.CommitTask;
            var targets = new List<TaskProbe> { new(failedTask) };
            failing.FailWrites = true;

            if (tolerateDeviceFailure)
                localLog.Commit();
            else
            {
                var exception = Assert.Throws<CommitFailureException>(() => localLog.Commit(spinWait: true));
                Assert.That(exception.InnerException, Is.SameAs(failing.InjectedError));
            }
            Assert.That(() => failedTask.IsFaulted, Is.True.After(10000, 10));

            if (tolerateDeviceFailure)
                targets.Add(new TaskProbe(localLog.CommitTask));
            return targets;
        }
    }
}