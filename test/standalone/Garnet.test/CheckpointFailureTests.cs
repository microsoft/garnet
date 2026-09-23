// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// A checkpoint that fails must never be reported to clients as a successful save. LASTSAVE must not advance,
    /// SAVE must return an error, and INFO PERSISTENCE must report <c>rdb_last_bgsave_status:err</c>. Otherwise a
    /// client following the documented "BGSAVE then poll LASTSAVE" pattern treats data as durable that was never
    /// written, and the loss only surfaces as an empty store after the next restart.
    /// </summary>
    /// <remarks>
    /// The failure is injected through <see cref="FailingCheckpointDeviceFactoryCreator"/> rather than by making the
    /// filesystem itself fail, so the tests do not depend on path length, file permissions or platform.
    /// </remarks>
    [TestFixture]
    public class CheckpointFailureTests : TestBase
    {
        const string StatusPrefix = "rdb_last_bgsave_status:";
        const string TestKey = "CheckpointFailureTestKey";
        const string TestValue = "CheckpointFailureTestValue";

        static readonly long EpochTicks = DateTimeOffset.FromUnixTimeSeconds(0).Ticks;
        static readonly TimeSpan CheckpointTimeout = TimeSpan.FromSeconds(30);

        // Taken from the naming scheme rather than hard-coded, so renaming a checkpoint file cannot silently turn a
        // deferred-write test into one that defers nothing.
        static readonly string HashTableFileName = new DefaultCheckpointNamingScheme(string.Empty).HashTable(Guid.Empty).fileName;
        static readonly string LogSnapshotFileName = new DefaultCheckpointNamingScheme(string.Empty).LogSnapshot(Guid.Empty).fileName;

        // Only has to outlast an abort that releases its devices without waiting for the writes still targeting
        // them; it does not bound an abort that waits, which finishes as soon as this elapses.
        static readonly TimeSpan DeferredWriteReleaseDelay = TimeSpan.FromMilliseconds(500);

        GarnetServer server;
        GarnetServerOptions options;
        FailingCheckpointDeviceFactoryCreator deviceFactoryCreator;
        DeferringCheckpointDeviceFactoryCreator deferringDeviceFactoryCreator;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = CreateServer(tryRecover: false);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown();
        }

        GarnetServer CreateServer(bool tryRecover) => CreateServer(tryRecover, enableAof: false);

        GarnetServer CreateServer(bool tryRecover, bool enableAof)
        {
            // Built through GetGarnetServerOptions rather than CreateGarnetServer because only the options object
            // exposes DeviceFactoryCreator, which is how the checkpoint failure is injected.
            options = TestUtils.GetGarnetServerOptions(
                checkpointDir: TestUtils.MethodTestDir,
                logDir: TestUtils.MethodTestDir,
                endpoint: TestUtils.EndPoint,
                enableCluster: false,
                enableAOF: enableAof,
                tryRecover: tryRecover);

            deviceFactoryCreator = new FailingCheckpointDeviceFactoryCreator(options.StoreCheckpointBaseDirectory);
            options.DeviceFactoryCreator = deviceFactoryCreator;

            return new GarnetServer(options);
        }

        /// <summary>
        /// Replaces the running server with one whose writes to <paramref name="checkpointFileName"/> only report
        /// completion when the test releases them, so a checkpoint can be aborted while its writes are in flight.
        /// </summary>
        void RestartServerDeferringWritesTo(string checkpointFileName)
        {
            server.Dispose();

            options = TestUtils.GetGarnetServerOptions(
                checkpointDir: TestUtils.MethodTestDir,
                logDir: TestUtils.MethodTestDir,
                endpoint: TestUtils.EndPoint,
                enableCluster: false,
                enableAOF: false,
                tryRecover: false);

            deferringDeviceFactoryCreator = new DeferringCheckpointDeviceFactoryCreator(options.StoreCheckpointBaseDirectory, checkpointFileName);
            options.DeviceFactoryCreator = deferringDeviceFactoryCreator;

            server = new GarnetServer(options);
            server.Start();
        }

        [Test]
        public void BackgroundSaveFailureDoesNotAdvanceLastSave()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var redisServer = redis.GetServer(TestUtils.EndPoint);

            db.StringSet(TestKey, TestValue);
            ClassicAssert.AreEqual(EpochTicks, redisServer.LastSave().Ticks, "LASTSAVE should start at the epoch");

            deviceFactoryCreator.FailureMode = CheckpointDeviceFailure.All;
            redisServer.Save(SaveType.BackgroundSave);

            // BGSAVE replies before the checkpoint runs, so wait for the outcome to be recorded rather than sleeping
            // for an arbitrary interval.
            WaitForLastSaveStatus(db, "err");

            ClassicAssert.AreEqual(EpochTicks, redisServer.LastSave().Ticks,
                "LASTSAVE advanced for a checkpoint that failed, so a client polling it would treat unwritten data as durable");
            ClassicAssert.AreEqual(0, CountCheckpointFiles(dbId: 0), "A failed checkpoint should not leave checkpoint files behind");
        }

        [Test]
        public void SaveFailureReturnsErrorToClient()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var redisServer = redis.GetServer(TestUtils.EndPoint);

            db.StringSet(TestKey, TestValue);

            deviceFactoryCreator.FailureMode = CheckpointDeviceFailure.All;
            var ex = Assert.Throws<RedisServerException>(() => db.Execute("SAVE"));
            ClassicAssert.IsTrue(ex.Message.StartsWith("ERR checkpoint failed", StringComparison.Ordinal),
                $"SAVE should report the failure to the client, but replied: {ex.Message}");

            ClassicAssert.AreEqual(EpochTicks, redisServer.LastSave().Ticks, "LASTSAVE advanced for a failed SAVE");
            ClassicAssert.AreEqual("err", GetLastSaveStatus(db));
            ClassicAssert.AreEqual(0, CountCheckpointFiles(dbId: 0));
        }

        [Test]
        public void SaveSucceedsAfterFailureAndDataSurvivesRestart()
        {
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                var redisServer = redis.GetServer(TestUtils.EndPoint);

                db.StringSet(TestKey, TestValue);

                deviceFactoryCreator.FailureMode = CheckpointDeviceFailure.All;
                _ = Assert.Throws<RedisServerException>(() => db.Execute("SAVE"));
                ClassicAssert.AreEqual(EpochTicks, redisServer.LastSave().Ticks);

                // A failed checkpoint must leave the server able to take the next one.
                deviceFactoryCreator.FailureMode = CheckpointDeviceFailure.None;
                ClassicAssert.AreEqual("OK", db.Execute("SAVE").ToString());

                ClassicAssert.AreNotEqual(EpochTicks, redisServer.LastSave().Ticks, "LASTSAVE should advance for a successful save");
                ClassicAssert.AreEqual("ok", GetLastSaveStatus(db));
                ClassicAssert.Greater(CountCheckpointFiles(dbId: 0), 0);
            }

            server.Dispose(false);
            server = CreateServer(tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual(TestValue, db.StringGet(TestKey).ToString(),
                    "The successful save should have been recoverable");
            }
        }

        [Test]
        public void BackgroundSaveFailureDoesNotAdvanceLastSaveForAnyDatabase()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db0 = redis.GetDatabase(0);

            // Touching a non-zero database promotes the server to the multi-database manager, which records the
            // last save time through its own code path.
            var db1 = redis.GetDatabase(1);

            db0.StringSet(TestKey, TestValue);
            db1.StringSet(TestKey, TestValue);

            deviceFactoryCreator.FailureMode = CheckpointDeviceFailure.All;
            ClassicAssert.AreEqual("Background saving started", db0.Execute("BGSAVE").ToString());

            WaitForLastSaveStatus(db0, "err");
            WaitForLastSaveStatus(db1, "err");

            ClassicAssert.AreEqual(0, (long)db0.Execute("LASTSAVE"), "LASTSAVE advanced for DB 0 after a failed checkpoint");
            ClassicAssert.AreEqual(0, (long)db1.Execute("LASTSAVE"), "LASTSAVE advanced for DB 1 after a failed checkpoint");
            ClassicAssert.AreEqual(0, CountCheckpointFiles(dbId: 0));
            ClassicAssert.AreEqual(0, CountCheckpointFiles(dbId: 1));
        }

        [Test]
        public void SnapshotDeviceFailureDoesNotAdvanceLastSaveAndLeavesStoreCheckpointable()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var redisServer = redis.GetServer(TestUtils.EndPoint);

            db.StringSet(TestKey, TestValue);

            // The snapshot devices are requested at WAIT_FLUSH, once both the index checkpoint and the hybrid-log
            // checkpoint have been initialized, so this aborts with state from both to release. The all-devices mode
            // instead fails in the index checkpoint, before the hybrid-log checkpoint exists.
            deviceFactoryCreator.FailureMode = CheckpointDeviceFailure.SnapshotLogDevicesOnly;

            var ex = Assert.Throws<RedisServerException>(() => db.Execute("SAVE"));
            ClassicAssert.IsTrue(ex.Message.StartsWith("ERR checkpoint failed", StringComparison.Ordinal),
                $"SAVE should report the failure to the client, but replied: {ex.Message}");
            ClassicAssert.AreEqual(EpochTicks, redisServer.LastSave().Ticks, "LASTSAVE advanced for a failed snapshot");
            ClassicAssert.AreEqual("err", GetLastSaveStatus(db));

            // Assert the absence of leaks directly, not just that the next checkpoint works: a leaked device would
            // still let the next checkpoint pass while leaking a file handle on every failure.
            AssertNoCheckpointStateLeaked();

            // Only possible if the abort released the hybrid-log checkpoint; otherwise the next checkpoint fails too.
            deviceFactoryCreator.FailureMode = CheckpointDeviceFailure.None;
            ClassicAssert.AreEqual("OK", db.Execute("SAVE").ToString());

            ClassicAssert.AreNotEqual(EpochTicks, redisServer.LastSave().Ticks, "LASTSAVE should advance for a successful save");
            ClassicAssert.AreEqual("ok", GetLastSaveStatus(db));
            ClassicAssert.Greater(CountCheckpointFiles(dbId: 0), 0);
        }

        [Test]
        public void CheckpointBlockedByAnotherStateMachineFailsWithoutTruncatingAof()
        {
            // The AOF is what makes the truncation this branch prevents observable.
            server.Dispose();
            server = CreateServer(tryRecover: false, enableAof: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var redisServer = redis.GetServer(TestUtils.EndPoint);

            for (var i = 0; i < 64; i++)
                db.StringSet($"{TestKey}{i}", TestValue);

            var beginAddressBeforeSave = GetPersistenceField(db, "BeginAddress:");

            // Occupy the database's state machine driver, so the checkpoint's RunAsync returns false instead of
            // throwing. No checkpoint is taken in that case, so the AOF must not be truncated and the save must not
            // be reported as successful.
            var driver = server.Provider.StoreWrapper.DefaultDatabase.StateMachineDriver;
            using var release = new ManualResetEventSlim(false);
            var blocking = new BlockingStateMachine(release);
            ClassicAssert.IsTrue(driver.Register(blocking), "Could not occupy the state machine driver");

            try
            {
                ClassicAssert.IsTrue(blocking.Entered.Wait(CheckpointTimeout), "Blocking state machine never started");

                var ex = Assert.Throws<RedisServerException>(() => db.Execute("SAVE"));
                ClassicAssert.IsTrue(ex.Message.StartsWith("ERR checkpoint failed", StringComparison.Ordinal),
                    $"SAVE should report the failure to the client, but replied: {ex.Message}");

                ClassicAssert.AreEqual(EpochTicks, redisServer.LastSave().Ticks,
                    "LASTSAVE advanced for a checkpoint that never ran");
                ClassicAssert.AreEqual("err", GetLastSaveStatus(db));
                ClassicAssert.AreEqual(beginAddressBeforeSave, GetPersistenceField(db, "BeginAddress:"),
                    "The AOF was truncated for a checkpoint that never ran");
                ClassicAssert.AreEqual(0, CountCheckpointFiles(dbId: 0));
            }
            finally
            {
                release.Set();
            }

            ClassicAssert.IsTrue(blocking.Completed.Wait(CheckpointTimeout), "Blocking state machine never finished");
        }

        [Test]
        public void FailedCheckpointFaultsCheckpointWaitersAndPublishesANewTask()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            db.StringSet(TestKey, TestValue);

            var store = server.Provider.StoreWrapper.store;
            var taskBeforeSave = store.CheckpointTask;
            ClassicAssert.IsFalse(taskBeforeSave.IsCompleted, "The checkpoint task should start out pending");

            deviceFactoryCreator.FailureMode = CheckpointDeviceFailure.SnapshotLogDevicesOnly;
            _ = Assert.Throws<RedisServerException>(() => db.Execute("SAVE"));

            // Waiters such as ClientSession.WaitForCommitAsync park on this task, which only the REST phase would
            // have completed. Leaving it pending after an abort hangs them forever.
            ClassicAssert.IsTrue(taskBeforeSave.IsCompleted, "The checkpoint task was left pending after a failed checkpoint");
            ClassicAssert.IsTrue(taskBeforeSave.IsFaulted, "The checkpoint task should be faulted for a failed checkpoint");
            _ = taskBeforeSave.Exception;

            // The replacement must already be published, so a waiter that re-reads the task after being woken picks
            // up the source for the next checkpoint rather than the one it just watched fail.
            var taskAfterSave = store.CheckpointTask;
            ClassicAssert.AreNotSame(taskBeforeSave, taskAfterSave, "A new checkpoint task should have been published");
            ClassicAssert.IsFalse(taskAfterSave.IsCompleted, "The replacement checkpoint task should be pending");
        }

        [Test]
        public void CheckpointAbortedAtVersionShiftLeaksNothingAndLeavesStoreCheckpointable()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);
            var redisServer = redis.GetServer(TestUtils.EndPoint);

            db.StringSet(TestKey, TestValue);

            // A device failure can only abort where a device is created, which is either before the version shift
            // (index device, in PREPARE) or after FlushBegin has already cleared the range-index barrier (snapshot
            // devices, in WAIT_FLUSH). Aborting at IN_PROGRESS is therefore the only way to reach the window where
            // the barrier is set but not yet cleared - the window CheckpointTrigger.CheckpointFailed exists for.
            var driver = server.Provider.StoreWrapper.DefaultDatabase.StateMachineDriver;
            var failOnce = new ThrowOnceAtPhase(Phase.IN_PROGRESS);
            driver.UnsafeRegisterCallback(failOnce);

            var ex = Assert.Throws<RedisServerException>(() => db.Execute("SAVE"));
            ClassicAssert.IsTrue(ex.Message.StartsWith("ERR checkpoint failed", StringComparison.Ordinal),
                $"SAVE should report the failure to the client, but replied: {ex.Message}");
            ClassicAssert.IsTrue(failOnce.Fired, "The abort was never injected");

            ClassicAssert.AreEqual(EpochTicks, redisServer.LastSave().Ticks, "LASTSAVE advanced for an aborted checkpoint");
            ClassicAssert.AreEqual("err", GetLastSaveStatus(db));
            AssertNoCheckpointStateLeaked();

            // The callback only throws once, so the next checkpoint exercises recovery from the abort.
            ClassicAssert.AreEqual("OK", db.Execute("SAVE").ToString());
            ClassicAssert.AreNotEqual(EpochTicks, redisServer.LastSave().Ticks, "LASTSAVE should advance for a successful save");
            ClassicAssert.AreEqual("ok", GetLastSaveStatus(db));
            ClassicAssert.Greater(CountCheckpointFiles(dbId: 0), 0);
        }

        [Test]
        public void AbortedCheckpointWaitsForTheIndexCheckpointFlushItIssued()
        {
            RestartServerDeferringWritesTo(HashTableFileName);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            db.StringSet(TestKey, TestValue);

            // The fuzzy index checkpoint is issued in PREPARE and only put on the driver's waiting list in
            // WAIT_INDEX_CHECKPOINT, so aborting at the version shift in between is the window where nothing else
            // waits for it.
            var driver = server.Provider.StoreWrapper.DefaultDatabase.StateMachineDriver;
            var failOnce = new ThrowOnceAtPhase(Phase.IN_PROGRESS);
            driver.UnsafeRegisterCallback(failOnce);

            deferringDeviceFactoryCreator.Deferring = true;
            var releaser = Task.Run(() =>
            {
                Thread.Sleep(DeferredWriteReleaseDelay);
                deferringDeviceFactoryCreator.ReleaseAll();
            });

            try
            {
                var ex = Assert.Throws<RedisServerException>(() => db.Execute("SAVE"));
                ClassicAssert.IsTrue(ex.Message.StartsWith("ERR checkpoint failed", StringComparison.Ordinal),
                    $"SAVE should report the failure to the client, but replied: {ex.Message}");

                // The abort disposes the index checkpoint device these writes target. One still in flight here fails
                // because of that, and it reports through flush state that is shared with the next checkpoint, so the
                // failure of this checkpoint would be charged to that one.
                ClassicAssert.AreEqual(0, deferringDeviceFactoryCreator.Outstanding,
                    "SAVE reported the aborted checkpoint while index checkpoint writes it issued were still in flight");
            }
            finally
            {
                deferringDeviceFactoryCreator.ReleaseAll();
                ClassicAssert.IsTrue(releaser.Wait(CheckpointTimeout), "Deferred writes were never released");
            }

            ClassicAssert.IsTrue(failOnce.Fired, "The abort was never injected");
            ClassicAssert.IsTrue(deferringDeviceFactoryCreator.AnyDeferred, "No index checkpoint write was held back");

            AssertNoCheckpointStateLeaked();
            ClassicAssert.AreEqual("OK", db.Execute("SAVE").ToString(), "The checkpoint after the abort should succeed");
        }

        [Test]
        public void AbortedCheckpointWaitsForTheSnapshotFlushItIssued()
        {
            RestartServerDeferringWritesTo(LogSnapshotFileName);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            // Enough records that the snapshot has pages to flush; with an empty log there is no flush to wait for.
            for (var i = 0; i < 1024; i++)
                db.StringSet($"{TestKey}{i}", TestValue);

            // A state machine's own hooks run before the callbacks registered here, so by the time this fires,
            // WAIT_FLUSH has already issued the snapshot flush to the device the abort goes on to dispose.
            var driver = server.Provider.StoreWrapper.DefaultDatabase.StateMachineDriver;
            var failOnce = new ThrowOnceAtPhase(Phase.WAIT_FLUSH);
            driver.UnsafeRegisterCallback(failOnce);

            deferringDeviceFactoryCreator.Deferring = true;
            var releaser = Task.Run(() =>
            {
                Thread.Sleep(DeferredWriteReleaseDelay);
                deferringDeviceFactoryCreator.ReleaseAll();
            });

            try
            {
                var ex = Assert.Throws<RedisServerException>(() => db.Execute("SAVE"));
                ClassicAssert.IsTrue(ex.Message.StartsWith("ERR checkpoint failed", StringComparison.Ordinal),
                    $"SAVE should report the failure to the client, but replied: {ex.Message}");

                ClassicAssert.AreEqual(0, deferringDeviceFactoryCreator.Outstanding,
                    "SAVE reported the aborted checkpoint while the snapshot flush it issued was still in flight");
            }
            finally
            {
                deferringDeviceFactoryCreator.ReleaseAll();
                ClassicAssert.IsTrue(releaser.Wait(CheckpointTimeout), "Deferred writes were never released");
            }

            ClassicAssert.IsTrue(failOnce.Fired, "The abort was never injected");
            ClassicAssert.IsTrue(deferringDeviceFactoryCreator.AnyDeferred, "No snapshot write was held back");

            AssertNoCheckpointStateLeaked();
            ClassicAssert.AreEqual("OK", db.Execute("SAVE").ToString(), "The checkpoint after the abort should succeed");
        }

        /// <summary>
        /// Asserts that an aborted checkpoint released every device and buffer it had taken. Checking the fields
        /// directly catches a leak that a "the next checkpoint still works" assertion would miss.
        /// </summary>
        void AssertNoCheckpointStateLeaked()
        {
            var store = server.Provider.StoreWrapper.store;

            Assert.Multiple(() =>
            {
                Assert.That(store._hybridLogCheckpoint.snapshotFileDevice, Is.Null, "Snapshot log device was leaked");
                Assert.That(store._hybridLogCheckpoint.snapshotFileObjectLogDevice, Is.Null, "Snapshot object log device was leaked");
                Assert.That(store._hybridLogCheckpoint.objectLogFlushBuffers, Is.Null, "Object log flush buffers were leaked");
                Assert.That(store._indexCheckpoint.IsDefault, Is.True, "Index checkpoint was not reset");
                Assert.That(store._indexCheckpoint.main_ht_device, Is.Null, "Index hash table device was leaked");
            });
        }

        /// <summary>
        /// Number of files written under the given database's log checkpoint directory.
        /// </summary>
        int CountCheckpointFiles(int dbId)
        {
            var namingScheme = new DefaultCheckpointNamingScheme(options.GetStoreCheckpointDirectory(dbId));
            var checkpointDir = Path.Combine(namingScheme.BaseName, namingScheme.LogCheckpointBasePath);

            return Directory.Exists(checkpointDir)
                ? Directory.GetFiles(checkpointDir, "*", SearchOption.AllDirectories).Length
                : 0;
        }

        static string GetLastSaveStatus(IDatabase db) => GetPersistenceField(db, StatusPrefix);

        /// <summary>
        /// Value of a single <c>field:value</c> line in the INFO PERSISTENCE section.
        /// </summary>
        static string GetPersistenceField(IDatabase db, string prefix)
        {
            var info = db.Execute("INFO", "PERSISTENCE").ToString();
            var line = info.Split("\r\n").FirstOrDefault(x => x.StartsWith(prefix, StringComparison.Ordinal));
            ClassicAssert.IsNotNull(line, $"INFO PERSISTENCE did not report {prefix}");

            return line[prefix.Length..];
        }

        static void WaitForLastSaveStatus(IDatabase db, string expected)
        {
            var deadline = DateTime.UtcNow + CheckpointTimeout;
            while (GetLastSaveStatus(db) != expected && DateTime.UtcNow < deadline)
                Thread.Sleep(10);

            ClassicAssert.AreEqual(expected, GetLastSaveStatus(db),
                $"{StatusPrefix} did not become '{expected}' within {CheckpointTimeout.TotalSeconds} seconds");
        }

        /// <summary>
        /// Occupies a <see cref="StateMachineDriver"/> until released, so that a checkpoint issued meanwhile finds the
        /// driver already running a state machine and does not run at all.
        /// </summary>
        private sealed class BlockingStateMachine : IStateMachine
        {
            readonly ManualResetEventSlim release;

            /// <summary>Set once the state machine is occupying the driver.</summary>
            internal readonly ManualResetEventSlim Entered = new(false);

            /// <summary>Set once the state machine has been released and is returning to REST.</summary>
            internal readonly ManualResetEventSlim Completed = new(false);

            internal BlockingStateMachine(ManualResetEventSlim release) => this.release = release;

            /// <inheritdoc/>
            public SystemState NextState(SystemState start)
            {
                var next = SystemState.Copy(ref start);
                next.Phase = start.Phase == Phase.REST ? Phase.PREPARE_GROW : Phase.REST;
                return next;
            }

            /// <inheritdoc/>
            public void GlobalBeforeEnteringState(SystemState nextState, StateMachineDriver stateMachineDriver)
            {
                // Blocks before the driver takes epoch protection, so parking here holds no epoch.
                if (nextState.Phase == Phase.PREPARE_GROW)
                {
                    Entered.Set();
                    release.Wait();
                }
                else if (nextState.Phase == Phase.REST)
                {
                    Completed.Set();
                }
            }

            /// <inheritdoc/>
            public void GlobalAfterEnteringState(SystemState nextState, StateMachineDriver stateMachineDriver) { }
        }

        /// <summary>
        /// Aborts the state machine once, on first entry to a chosen phase. Device-agnostic, so it reaches abort
        /// points that no device failure can - notably <see cref="Phase.IN_PROGRESS"/>, which is after the version
        /// shift has published its lifecycle notification but before any snapshot device exists.
        /// </summary>
        /// <remarks>
        /// The driver has no way to unregister a callback, so this stays attached for the store's lifetime and must
        /// throw only once; later checkpoints have to be able to succeed.
        /// </remarks>
        private sealed class ThrowOnceAtPhase : IStateMachineCallback
        {
            readonly Phase phase;
            int fired;

            internal ThrowOnceAtPhase(Phase phase) => this.phase = phase;

            /// <summary>True once the abort has been injected.</summary>
            internal bool Fired => Volatile.Read(ref fired) != 0;

            /// <inheritdoc/>
            public void BeforeEnteringState(SystemState nextState)
            {
                if (nextState.Phase == phase && Interlocked.CompareExchange(ref fired, 1, 0) == 0)
                    throw new IOException($"Simulated checkpoint abort at {phase}");
            }
        }
    }
}