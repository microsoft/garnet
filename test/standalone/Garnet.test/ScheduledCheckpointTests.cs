// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    [TestFixture]
    public class ScheduledCheckpointTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            server = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown();
        }

        void StartServer(int frequencySecs, bool recover = false, bool enableAof = false)
        {
            var options = TestUtils.GetGarnetServerOptions(
                checkpointDir: TestUtils.MethodTestDir,
                logDir: TestUtils.MethodTestDir,
                endpoint: TestUtils.EndPoint,
                enableCluster: false,
                enableAOF: enableAof,
                tryRecover: recover);
            options.CheckpointFrequencySecs = frequencySecs;
            server = new GarnetServer(options);
            server.Start();
        }

        DateTimeOffset LastSave => server.Provider.StoreWrapper.DefaultDatabase.LastSaveTime;

        void WaitForCheckpointAfter(DateTimeOffset prior)
        {
            var deadline = DateTime.UtcNow.AddSeconds(15);
            while (LastSave <= prior && DateTime.UtcNow < deadline)
                Thread.Sleep(25);
            Assert.That(LastSave, Is.GreaterThan(prior));
        }

        // Disabling the scheduler does not interrupt a checkpoint that already started, so let it finish before sampling LASTSAVE.
        void WaitForRunningCheckpoint()
        {
            var store = server.Provider.StoreWrapper;
            var deadline = DateTime.UtcNow.AddSeconds(15);
            while (!store.TryPauseCheckpoints())
            {
                Assert.That(DateTime.UtcNow, Is.LessThan(deadline), "Checkpoint did not finish");
                Thread.Sleep(25);
            }
            store.ResumeCheckpoints();
        }

        [Test]
        public void ScheduledCheckpointsRepeatWithoutWrites()
        {
            StartServer(1);
            WaitForCheckpointAfter(DateTimeOffset.FromUnixTimeSeconds(0));
            WaitForCheckpointAfter(LastSave);
        }

        [Test]
        public void DisabledSchedulerTakesNoCheckpoints()
        {
            StartServer(0);
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase();
            db.StringSet("counter", 0);
            db.StringIncrement("counter");
            db.HashSet("hash", "field", "value");
            db.KeyDelete("hash");
            Thread.Sleep(2500);
            Assert.That(LastSave, Is.EqualTo(DateTimeOffset.FromUnixTimeSeconds(0)));
        }

        [Test]
        public void EnablingCheckpointsAtRuntimeSavesExistingData()
        {
            StartServer(0);
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase();
                db.StringSet("counter", 0);
                Assert.That(db.Execute("SAVE").ToString(), Is.EqualTo("OK"));
                var prior = LastSave;
                db.StringIncrement("counter");
                redis.GetDatabase(1).HashSet("hash", "field", "value");

                Assert.That(db.Execute("CONFIG", "SET", "checkpoint-freq", "1").ToString(), Is.EqualTo("OK"));
                WaitForCheckpointAfter(prior);
                Assert.That(db.Execute("CONFIG", "SET", "checkpoint-freq", "0").ToString(), Is.EqualTo("OK"));
            }

            server.Dispose(false);
            server = null;
            StartServer(0, recover: true);
            using var recovered = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            Assert.That((long)recovered.GetDatabase().StringGet("counter"), Is.EqualTo(1));
            Assert.That(recovered.GetDatabase(1).HashGet("hash", "field").ToString(), Is.EqualTo("value"));
        }

        [TestCase(-1, false)]
        [TestCase(0, true)]
        [TestCase(1, true)]
        [TestCase(int.MaxValue, true)]
        public void CheckpointFrequencyOptionValidatesRange(int frequencySecs, bool expectedSuccess)
        {
            var success = ServerSettingsManager.TryParseCommandLineArguments(
                ["--checkpoint-freq", frequencySecs.ToString(System.Globalization.CultureInfo.InvariantCulture)],
                out var options, out _, out _, out _, silentMode: true);
            Assert.That(success, Is.EqualTo(expectedSuccess));
            if (success)
                Assert.That(options.GetServerOptions().CheckpointFrequencySecs, Is.EqualTo(frequencySecs));
        }

        [Test]
        public void ScheduledCheckpointCoversAllActiveDatabases()
        {
            StartServer(1);
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db0 = redis.GetDatabase(0);
            var db1 = redis.GetDatabase(1);
            db1.StringSet("key", "value");

            var deadline = DateTime.UtcNow.AddSeconds(15);
            while (((long)db0.Execute("LASTSAVE", 0) == 0 || (long)db1.Execute("LASTSAVE", 1) == 0) && DateTime.UtcNow < deadline)
                Thread.Sleep(25);
            Assert.That((long)db0.Execute("LASTSAVE", 0), Is.GreaterThan(0));
            Assert.That((long)db1.Execute("LASTSAVE", 1), Is.GreaterThan(0));
        }

        [Test]
        public void FailedScheduledCheckpointRetries()
        {
            var options = TestUtils.GetGarnetServerOptions(
                checkpointDir: TestUtils.MethodTestDir,
                logDir: TestUtils.MethodTestDir,
                endpoint: TestUtils.EndPoint,
                enableCluster: false,
                enableAOF: false);
            options.CheckpointFrequencySecs = 1;
            var deviceFactory = new FailingCheckpointDeviceFactoryCreator(options.StoreCheckpointBaseDirectory)
            {
                FailureMode = CheckpointDeviceFailure.All
            };
            options.DeviceFactoryCreator = deviceFactory;
            server = new GarnetServer(options);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            redis.GetDatabase().StringSet("key", "value");

            var deadline = DateTime.UtcNow.AddSeconds(15);
            while (server.Provider.StoreWrapper.DefaultDatabase.LastSaveSucceeded && DateTime.UtcNow < deadline)
                Thread.Sleep(25);
            Assert.That(server.Provider.StoreWrapper.DefaultDatabase.LastSaveSucceeded, Is.False);
            Assert.That(LastSave, Is.EqualTo(DateTimeOffset.FromUnixTimeSeconds(0)));

            deviceFactory.FailureMode = CheckpointDeviceFailure.None;
            WaitForCheckpointAfter(DateTimeOffset.FromUnixTimeSeconds(0));
        }

        [Test]
        public void ConfigSetStartsAndStopsScheduledCheckpoints()
        {
            StartServer(0);
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase();
            db.StringSet("counter", 0);

            Assert.That(((RedisResult[])db.Execute("CONFIG", "GET", "checkpoint-freq"))[1].ToString(), Is.EqualTo("0"));
            Assert.That(db.Execute("CONFIG", "SET", "checkpoint-freq", "1").ToString(), Is.EqualTo("OK"));
            Assert.That(((RedisResult[])db.Execute("CONFIG", "GET", "checkpoint-freq"))[1].ToString(), Is.EqualTo("1"));
            WaitForCheckpointAfter(DateTimeOffset.FromUnixTimeSeconds(0));

            Assert.That(db.Execute("CONFIG", "SET", "checkpoint-freq", "0").ToString(), Is.EqualTo("OK"));
            WaitForRunningCheckpoint();
            var prior = LastSave;
            db.StringIncrement("counter");
            Thread.Sleep(2500);
            Assert.That(LastSave, Is.EqualTo(prior));

            Assert.Throws<RedisServerException>(() => db.Execute("CONFIG", "SET", "checkpoint-freq", "-1"));
            Assert.That(db.Execute("CONFIG", "SET", "checkpoint-freq", int.MaxValue).ToString(), Is.EqualTo("OK"));
            Assert.That(db.Execute("CONFIG", "SET", "checkpoint-freq", "0").ToString(), Is.EqualTo("OK"));
        }

        [Test]
        public async Task ConfigSetStartsSchedulerThatIsNotRunningAsync()
        {
            StartServer(0);
            var store = server.Provider.StoreWrapper;
            await store.SuspendPrimaryOnlyTasksAsync().ConfigureAwait(false);
            Assert.That(store.TaskManager.IsRunning(TaskType.ScheduledCheckpointTask), Is.False);

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase();
            Assert.That(db.Execute("CONFIG", "SET", "checkpoint-freq", "1").ToString(), Is.EqualTo("OK"));
            Assert.That(store.TaskManager.IsRunning(TaskType.ScheduledCheckpointTask), Is.True);
            WaitForCheckpointAfter(DateTimeOffset.FromUnixTimeSeconds(0));
        }

        [Test]
        public void SettingTheSameIntervalDoesNotPostponeTheCheckpoint()
        {
            StartServer(3);
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase();

            // Re-applying the value more often than the interval would starve the scheduler if each call restarted the wait.
            var deadline = DateTime.UtcNow.AddSeconds(15);
            while (LastSave == DateTimeOffset.FromUnixTimeSeconds(0) && DateTime.UtcNow < deadline)
            {
                Assert.That(db.Execute("CONFIG", "SET", "checkpoint-freq", "3").ToString(), Is.EqualTo("OK"));
                Thread.Sleep(250);
            }

            Assert.That(LastSave, Is.GreaterThan(DateTimeOffset.FromUnixTimeSeconds(0)));
        }

        [TestCase(false)]
        [TestCase(true)]
        public void ScheduledCheckpointRecoversStringAndObjectUpdates(bool enableAof)
        {
            StartServer(1, enableAof: enableAof);
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase();
                db.StringSet("counter", 0);
                db.HashSet("hash", "field", "original");
                db.ListLeftPush("list", "only-member");
                db.SortedSetAdd("sorted-set", "only-member", 1);
                db.StringSet("expiring", "value");
                WaitForCheckpointAfter(DateTimeOffset.FromUnixTimeSeconds(0));

                db.StringIncrement("counter");
                db.HashSet("hash", "field", "updated");
                Assert.That(db.ListLeftPop("list").ToString(), Is.EqualTo("only-member"));
                Assert.That(db.SortedSetRemove("sorted-set", "only-member"), Is.True);
                Assert.That(db.KeyExpire("expiring", TimeSpan.FromMinutes(10)), Is.True);

                // A checkpoint may already be running during the updates, so only the one after it is guaranteed to include them all.
                WaitForCheckpointAfter(LastSave);
                WaitForCheckpointAfter(LastSave);
                Assert.That(db.Execute("CONFIG", "SET", "checkpoint-freq", "0").ToString(), Is.EqualTo("OK"));
            }

            server.Dispose(false);
            server = null;
            StartServer(0, recover: true, enableAof: enableAof);
            using var recovered = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var recoveredDb = recovered.GetDatabase();
            Assert.That((long)recoveredDb.StringGet("counter"), Is.EqualTo(1));
            Assert.That(recoveredDb.HashGet("hash", "field").ToString(), Is.EqualTo("updated"));
            Assert.That(recoveredDb.KeyExists("list"), Is.False);
            Assert.That(recoveredDb.KeyExists("sorted-set"), Is.False);
            Assert.That(recoveredDb.KeyTimeToLive("expiring"), Is.GreaterThan(TimeSpan.Zero));
        }

        [Test]
        public async Task ConfigChangesDuringCheckpointDoNotWaitAndConcurrentWritesAreSavedAsync()
        {
            StartServer(0);
            using var entered = new ManualResetEventSlim();
            using var release = new ManualResetEventSlim();
            var driver = server.Provider.StoreWrapper.DefaultDatabase.StateMachineDriver;
            driver.UnsafeRegisterCallback(new PauseCheckpointOnce(entered, release));

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase();
                db.StringSet("seed", "value");
                var prior = LastSave;
                Assert.That(db.Execute("CONFIG", "SET", "checkpoint-freq", "1").ToString(), Is.EqualTo("OK"));

                try
                {
                    Assert.That(entered.Wait(TimeSpan.FromSeconds(15)), Is.True);
                    var transaction = db.CreateTransaction();
                    var disable = transaction.ExecuteAsync("CONFIG", ["SET", "checkpoint-freq", "0"]);
                    var retune = transaction.ExecuteAsync("CONFIG", ["SET", "checkpoint-freq", "2"]);
                    var disableAgain = transaction.ExecuteAsync("CONFIG", ["SET", "checkpoint-freq", "0"]);
                    var write = transaction.StringSetAsync("during-checkpoint", "committed");

                    Assert.That(await transaction.ExecuteAsync().WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false), Is.True);
                    foreach (var reply in new[] { disable, retune, disableAgain })
                        Assert.That((await reply.ConfigureAwait(false)).ToString(), Is.EqualTo("OK"));
                    Assert.That(await write.ConfigureAwait(false), Is.True);
                    Assert.That(LastSave, Is.EqualTo(prior));
                    Assert.That(((RedisResult[])db.Execute("CONFIG", "GET", "checkpoint-freq"))[1].ToString(), Is.EqualTo("0"));
                }
                finally
                {
                    release.Set();
                    await driver.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
                }

                WaitForCheckpointAfter(prior);
                prior = LastSave;
                Thread.Sleep(1500);
                Assert.That(LastSave, Is.EqualTo(prior), "Disabling must leave the scheduler idle after the current checkpoint");
                Assert.That(db.Execute("CONFIG", "SET", "checkpoint-freq", "1").ToString(), Is.EqualTo("OK"));
                WaitForCheckpointAfter(prior);
                Assert.That(db.Execute("CONFIG", "SET", "checkpoint-freq", "0").ToString(), Is.EqualTo("OK"));
            }

            server.Dispose(false);
            server = null;
            StartServer(0, recover: true);
            using var recovered = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            Assert.That(recovered.GetDatabase().StringGet("during-checkpoint").ToString(), Is.EqualTo("committed"));
        }

        [Test]
        public async Task ShutdownCompletesAnActiveScheduledCheckpointAsync()
        {
            StartServer(0);
            using var entered = new ManualResetEventSlim();
            using var release = new ManualResetEventSlim();
            var driver = server.Provider.StoreWrapper.DefaultDatabase.StateMachineDriver;
            driver.UnsafeRegisterCallback(new PauseCheckpointOnce(entered, release));
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase();
                db.StringSet("key", "value");
                Assert.That(db.Execute("CONFIG", "SET", "checkpoint-freq", "1").ToString(), Is.EqualTo("OK"));
            }

            Task shutdown = null;
            try
            {
                Assert.That(entered.Wait(TimeSpan.FromSeconds(15)), Is.True);
                var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                shutdown = Task.Run(() =>
                {
                    started.SetResult();
                    server.Dispose(false);
                });
                await started.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
                Assert.That(await Task.WhenAny(shutdown, Task.Delay(200)).ConfigureAwait(false), Is.Not.SameAs(shutdown));
            }
            finally
            {
                release.Set();
                if (shutdown != null)
                {
                    await shutdown.WaitAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
                    server = null;
                }
                else
                {
                    await driver.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
                }
            }

            StartServer(0, recover: true);
            using var recovered = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            Assert.That(recovered.GetDatabase().StringGet("key").ToString(), Is.EqualTo("value"));
        }

        sealed class PauseCheckpointOnce(ManualResetEventSlim entered, ManualResetEventSlim release) : IStateMachineCallback
        {
            int paused;

            public void BeforeEnteringState(SystemState next)
            {
                if (next.Phase != Phase.WAIT_FLUSH || Interlocked.Exchange(ref paused, 1) != 0)
                    return;
                entered.Set();
                if (!release.Wait(TimeSpan.FromSeconds(30)))
                    throw new TimeoutException("Checkpoint was not released by the test");
            }
        }
    }
}