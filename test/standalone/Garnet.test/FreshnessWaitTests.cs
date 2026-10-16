// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Tests of what lets a session on a replica park, rather than block a thread, when a read has to wait for replay to
    /// catch up with what the session has already read: the await source that holds the session until replay has, and
    /// the probes that decide whether a read has to wait.
    /// </summary>
    /// <remarks>
    /// Replay is played by hand through <see cref="ReadConsistencyManager"/>. Two physical sublogs make two virtual ones.
    /// A parked wait completes a task whose continuation runs on the thread pool, so that a wait has not ended is
    /// asserted on whether it is still listed, which changes synchronously, and that it has ended by waiting for it.
    /// </remarks>
    [TestFixture]
    public class FreshnessWaitTests : TestBase
    {
        static readonly TimeSpan Bound = TimeSpan.FromSeconds(15);

        // The sequence number a session has read up to
        const long Target = 100;

        GarnetServerOptions options;
        GarnetAppendOnlyFile appendOnlyFile;
        ReadConsistencyManager manager;

        static GarnetServerOptions CreateOptions(int replayDriftThreshold = -1) => new()
        {
            EnableAOF = true,
            UseAofNullDevice = true,
            CommitFrequencyMs = -1,
            FastAofTruncate = true,
            EnableCluster = true,
            AofReplayMaxLagBytes = 0,
            AofPhysicalSublogCount = 2,
            AofReplayDriftThreshold = replayDriftThreshold,
        };

        static GarnetAppendOnlyFile CreateAppendOnlyFile(GarnetServerOptions options)
        {
            options.GetAofSettings(0, out var logSettings);
            return new GarnetAppendOnlyFile(options, logSettings);
        }

        [SetUp]
        public void Setup()
        {
            options = CreateOptions();
            appendOnlyFile = CreateAppendOnlyFile(options);
            manager = appendOnlyFile.readConsistencyManager;
        }

        [TearDown]
        public void TearDown()
        {
            appendOnlyFile?.Dispose();
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// Parks a wait on a virtual sublog the way a session does.
        /// </summary>
        /// <returns>The task that completes when the wait ends.</returns>
        static Task Park(FreshnessWait wait, ReadConsistencyManager consistencyManager, int virtualSublogIdx, long sequenceNumber)
        {
            wait.Prepare(consistencyManager, virtualSublogIdx, sequenceNumber);
            wait.Arm();
            ClassicAssert.IsTrue(wait.TryRegister(), "The wait should have been listed");
            return wait.WaitAsync().AsTask();
        }

        /// <summary>
        /// Makes a key that lives on a virtual sublog.
        /// </summary>
        (byte[] Key, long Hash) KeyOn(int virtualSublogIdx)
        {
            for (var i = 0; ; i++)
            {
                var bytes = Encoding.ASCII.GetBytes($"key{i}");
                var key = GC.AllocateArray<byte>(bytes.Length, pinned: true);
                bytes.CopyTo(key, 0);

                var hash = GarnetLog.HASH(key) & long.MaxValue;
                if (appendOnlyFile.Log.GetVirtualSublogIdx(hash) == virtualSublogIdx)
                    return (key, hash);
            }
        }

        /// <summary>
        /// Makes a session that has read a key whose latest write replay applied at <see cref="Target"/>.
        /// </summary>
        ReadSessionState SessionThatHasRead((byte[] Key, long Hash) key, int virtualSublogIdx)
        {
            manager.UpdateVirtualSublogKeySequenceNumber(virtualSublogIdx, key.Hash, Target);
            var session = new ReadSessionState(appendOnlyFile, options);
            session.PreSingleKeyConsistentRead(key.Hash);
            session.PostSingleKeyConsistentReadCallback();
            return session;
        }

        /// <summary>
        /// Asks whether a command that names the keys would have to wait for replay, as the dispatch loop does.
        /// </summary>
        bool MustWait(ReadSessionState session, RespCommand cmd, out int virtualSublogIdx, out long sequenceNumber, params byte[][] keys)
        {
            ClassicAssert.IsTrue(RespCommandsInfo.TryGetSimpleRespCommandInfo(cmd, out var info));

            var parseState = new SessionParseState();
            parseState.Initialize(keys.Length);
            for (var i = 0; i < keys.Length; i++)
                parseState.SetArgument(i, PinnedSpanByte.FromPinnedSpan(keys[i]));

            var mustWait = session.MustWaitForReplay(ref parseState, info.KeySpecs, isSubCommand: false, out var probed, out virtualSublogIdx, out sequenceNumber);
            ClassicAssert.AreSame(manager, probed, "The probe should run against the current manager");
            return mustWait;
        }

        [Test]
        public void AWaitEndsOnlyOnceReplayHasPassedItsSequenceNumber()
        {
            var wait = new FreshnessWait(Timeout.InfiniteTimeSpan);
            var parked = Park(wait, manager, virtualSublogIdx: 0, Target);
            ClassicAssert.AreEqual(1, manager.ParkedFreshnessWaiterCount());

            // Replay that has reached the sequence number has not passed it
            manager.UpdateVirtualSublogMaxSequenceNumber(0, Target);
            ClassicAssert.AreEqual(1, manager.ParkedFreshnessWaiterCount());

            // Nor does replay of the other sublog matter
            manager.UpdateVirtualSublogMaxSequenceNumber(1, Target + 1);
            ClassicAssert.AreEqual(1, manager.ParkedFreshnessWaiterCount());

            manager.UpdateVirtualSublogMaxSequenceNumber(0, Target + 1);
            ClassicAssert.AreEqual(0, manager.ParkedFreshnessWaiterCount());
            ClassicAssert.IsTrue(parked.Wait(Bound));
            Assert.DoesNotThrow(wait.Finish);
        }

        [Test]
        public void ANewWaitIsListedOnlyIfReplayHasNotPassedItsSequenceNumber()
        {
            // Replay that has reached the sequence number has not passed it: this is when a read has to wait
            manager.UpdateVirtualSublogMaxSequenceNumber(0, Target);
            _ = Park(new FreshnessWait(Timeout.InfiniteTimeSpan), manager, 0, Target);
            ClassicAssert.AreEqual(1, manager.ParkedFreshnessWaiterCount());

            // Replay that has passed it leaves nothing to wait for
            manager.UpdateVirtualSublogMaxSequenceNumber(0, Target + 1);
            ClassicAssert.AreEqual(0, manager.ParkedFreshnessWaiterCount());

            var wait = new FreshnessWait(Timeout.InfiniteTimeSpan);
            wait.Prepare(manager, 0, Target);
            wait.Arm();
            ClassicAssert.IsFalse(wait.TryRegister());
            ClassicAssert.AreEqual(0, manager.ParkedFreshnessWaiterCount());
        }

        [Test]
        public void ReplayWakesTheWaitsItHasPassedAndCancelledWaitsLeaveTheListIntact()
        {
            // Listed out of order of their sequence numbers, so that a wait is inserted at the head, in the middle and at the
            // end, and so that the last one listed is not the lowest
            var sequenceNumbers = new[] { 30, 10, 40, 20, 50 };
            var waits = new FreshnessWait[sequenceNumbers.Length];
            var parked = new Task[sequenceNumbers.Length];
            for (var i = 0; i < waits.Length; i++)
            {
                waits[i] = new FreshnessWait(Timeout.InfiniteTimeSpan);
                parked[i] = Park(waits[i], manager, 0, Target + sequenceNumbers[i]);
            }
            ClassicAssert.AreEqual(5, manager.ParkedFreshnessWaiterCount());

            // Two neighbours, neither of them at either end of the list, leave it
            waits[0].Cancel();
            ClassicAssert.AreEqual(4, manager.ParkedFreshnessWaiterCount());
            waits[2].Cancel();
            ClassicAssert.AreEqual(3, manager.ParkedFreshnessWaiterCount());

            // Replay passes the first two of what is left, and not the last
            manager.UpdateVirtualSublogMaxSequenceNumber(0, Target + 25);
            ClassicAssert.AreEqual(1, manager.ParkedFreshnessWaiterCount());
            ClassicAssert.IsTrue(parked[1].Wait(Bound));
            ClassicAssert.IsTrue(parked[3].Wait(Bound));

            // A wait replay has woken is no longer listed, and cancelling it leaves the list alone
            waits[1].Cancel();
            ClassicAssert.AreEqual(1, manager.ParkedFreshnessWaiterCount());

            // The one left is now the whole list
            waits[4].Cancel();
            ClassicAssert.AreEqual(0, manager.ParkedFreshnessWaiterCount());

            foreach (var i in new[] { 0, 2, 4 })
                _ = Assert.CatchAsync<OperationCanceledException>(async () => await parked[i].WaitAsync(Bound));
        }

        [Test]
        public void AWaitThatReplayDoesNotEndInTimeFailsTheRead()
        {
            var wait = new FreshnessWait(TimeSpan.FromMilliseconds(100));
            var parked = Park(wait, manager, 0, Target);

            ClassicAssert.IsTrue(parked.Wait(Bound), "The timeout should end the wait");
            ClassicAssert.AreEqual(0, manager.ParkedFreshnessWaiterCount());
            _ = Assert.Throws<TimeoutException>(wait.Finish);
        }

        [Test]
        public void ATimeoutThatLosesToReplayDoesNotFailTheRead()
        {
            var wait = new FreshnessWait(TimeSpan.FromMilliseconds(100));
            var parked = Park(wait, manager, 0, Target);
            manager.UpdateVirtualSublogMaxSequenceNumber(0, Target + 1);
            ClassicAssert.IsTrue(parked.Wait(Bound));

            // The timeout elapses before the session gets to finish the wait
            Thread.Sleep(TimeSpan.FromMilliseconds(400));
            Assert.DoesNotThrow(wait.Finish);
        }

        [Test]
        public void AWaitThatTimedOutDoesNotFailTheNextOne()
        {
            var wait = new FreshnessWait(TimeSpan.FromMilliseconds(100));
            ClassicAssert.IsTrue(Park(wait, manager, 0, Target).Wait(Bound));
            _ = Assert.Throws<TimeoutException>(wait.Finish);

            var parked = Park(wait, manager, 0, Target);
            manager.UpdateVirtualSublogMaxSequenceNumber(0, Target + 1);
            ClassicAssert.IsTrue(parked.Wait(Bound));
            Assert.DoesNotThrow(wait.Finish);
        }

        [Test]
        public void ATimerLeftOverFromAnEarlierWaitDoesNotEndALaterOne()
        {
            var wait = new FreshnessWait(TimeSpan.FromHours(1));
            var parked = Park(wait, manager, 0, Target);

            // The timer of an earlier wait fires during this one, well before its deadline
            wait.OnTimer();
            ClassicAssert.AreEqual(1, manager.ParkedFreshnessWaiterCount());

            manager.UpdateVirtualSublogMaxSequenceNumber(0, Target + 1);
            ClassicAssert.IsTrue(parked.Wait(Bound));
            Assert.DoesNotThrow(wait.Finish);
        }

        [Test]
        public void CancelingAParkedWaitTakesItOffTheListAndFaultsIt()
        {
            var wait = new FreshnessWait(Timeout.InfiniteTimeSpan);
            var parked = Park(wait, manager, 0, Target);

            wait.Cancel();
            ClassicAssert.AreEqual(0, manager.ParkedFreshnessWaiterCount());
            _ = Assert.CatchAsync<OperationCanceledException>(async () => await parked.WaitAsync(Bound));
        }

        [Test]
        public void ACancellationBeforeAWaitStartsFaultsTheWaitAtOnce()
        {
            // Teardown can run before the session has ever waited, or between its registering and its awaiting
            var wait = new FreshnessWait(Timeout.InfiniteTimeSpan);
            wait.Cancel();

            wait.Prepare(manager, 0, Target);
            wait.Arm();
            ClassicAssert.IsTrue(wait.TryRegister());
            var parked = wait.WaitAsync().AsTask();

            _ = Assert.CatchAsync<OperationCanceledException>(async () => await parked.WaitAsync(Bound));
            ClassicAssert.AreEqual(0, manager.ParkedFreshnessWaiterCount());
        }

        [Test]
        public void RetiringTheManagerWakesParkedWaitsAndRefusesNewOnes()
        {
            var wait = new FreshnessWait(Timeout.InfiniteTimeSpan);
            var parked = Park(wait, manager, 0, Target);

            manager.RetireFreshnessWaiters();
            ClassicAssert.IsTrue(parked.Wait(Bound), "Replay no longer feeds a retired manager, so nothing else would wake the wait");
            ClassicAssert.AreEqual(0, manager.ParkedFreshnessWaiterCount());
            Assert.DoesNotThrow(wait.Finish);

            // The session decides again against the manager that replaced this one, which this wait cannot be listed with
            wait.Prepare(manager, 0, Target);
            wait.Arm();
            ClassicAssert.IsFalse(wait.TryRegister());
            ClassicAssert.AreEqual(0, manager.ParkedFreshnessWaiterCount());
        }

        [Test]
        public void RetiringTheManagerLeavesReadersThatBlockAThreadWaiting()
        {
            var keyA = KeyOn(0);
            var keyB = KeyOn(1);
            using var session = SessionThatHasRead(keyA, virtualSublogIdx: 0);

            // Reading B has to wait for replay of B's sublog to pass what the session has read
            var reader = Task.Run(() => session.PreSingleKeyConsistentRead(keyB.Hash));
            ClassicAssert.IsTrue(SpinWait.SpinUntil(() => manager.BlockedFreshnessWaiterCount() == 1, Bound));

            // A blocked reader returns only once replay has passed what it waits for: it does not decide again
            manager.RetireFreshnessWaiters();
            ClassicAssert.AreEqual(1, manager.BlockedFreshnessWaiterCount());
            ClassicAssert.IsFalse(reader.IsCompleted);

            manager.UpdateVirtualSublogMaxSequenceNumber(1, Target + 1);
            ClassicAssert.IsTrue(reader.Wait(Bound));
        }

        [Test]
        public void ReplacingTheManagerWakesWaitsParkedOnTheOneItReplaces()
        {
            var wait = new FreshnessWait(Timeout.InfiniteTimeSpan);
            var parked = Park(wait, manager, 0, Target);

            appendOnlyFile.CreateOrUpdateKeySequenceManager();
            ClassicAssert.AreNotSame(manager, appendOnlyFile.readConsistencyManager);

            ClassicAssert.IsTrue(parked.Wait(Bound));
            ClassicAssert.AreEqual(0, manager.ParkedFreshnessWaiterCount());
            Assert.DoesNotThrow(wait.Finish);
        }

        [Test]
        public void ParkingAWaitBoundsReplayDriftAsBlockingDoes()
        {
            var driftOptions = CreateOptions(replayDriftThreshold: 10);
            var driftAppendOnlyFile = CreateAppendOnlyFile(driftOptions);
            try
            {
                var driftManager = driftAppendOnlyFile.readConsistencyManager;

                // Replay of one sublog is far ahead of the other's
                driftManager.UpdateVirtualSublogMaxSequenceNumber(0, 1000);
                ClassicAssert.IsFalse(driftManager.replayBarrier.InProgress);

                var wait = new FreshnessWait(Timeout.InfiniteTimeSpan);
                _ = Park(wait, driftManager, virtualSublogIdx: 1, 500);
                ClassicAssert.IsTrue(driftManager.replayBarrier.InProgress, "The replay of the sublog that lags should be given a chance to catch up");

                wait.Cancel();
            }
            finally
            {
                driftAppendOnlyFile.Dispose();
            }
        }

        [Test]
        public void AWaitListedWhileReplayPassesItsSequenceNumberIsNeverLeftParked()
        {
            const int Rounds = 100_000;
            var started = 0;
            var finished = 0;
            var stop = 0;

            var replay = Task.Factory.StartNew(() =>
            {
                var jitter = new Random(Rounds);
                for (var round = 1; round <= Rounds; round++)
                {
                    while (Volatile.Read(ref started) != round)
                    {
                        if (Volatile.Read(ref stop) != 0)
                            return;
                    }

                    Thread.SpinWait(jitter.Next(0, 40));
                    manager.UpdateVirtualSublogMaxSequenceNumber(0, (round * 10L) + 1);
                    Volatile.Write(ref finished, round);
                }
            }, TaskCreationOptions.LongRunning);

            try
            {
                // On a machine with few cores the two threads take turns, and a round takes far longer than the race
                // being looked for lasts, so the number of rounds is capped by time as well
                var deadline = Environment.TickCount64 + 5_000;
                var jitter = new Random(Rounds + 1);
                var wait = new FreshnessWait(Timeout.InfiniteTimeSpan);
                for (var round = 1; round <= Rounds && Environment.TickCount64 < deadline; round++)
                {
                    wait.Prepare(manager, 0, round * 10L);
                    wait.Arm();
                    Volatile.Write(ref started, round);

                    // Both threads wait a random while after the start of the round, so the update lands anywhere from
                    // before the registration to after it
                    Thread.SpinWait(jitter.Next(0, 40));
                    if (wait.TryRegister())
                    {
                        var parked = wait.WaitAsync();
                        if (!SpinWait.SpinUntil(() => parked.IsCompleted, Bound))
                            Assert.Fail($"The wait of round {round} was listed as replay passed its sequence number, and never woken");
                        parked.GetAwaiter().GetResult();
                    }

                    ClassicAssert.IsTrue(SpinWait.SpinUntil(() => Volatile.Read(ref finished) == round, Bound));
                }
            }
            finally
            {
                Volatile.Write(ref stop, 1);
                _ = replay.Wait(Bound);
            }
        }

        [Test]
        public void AProbeNamesWhatAReadWouldWaitForAndLeavesTheSessionAsItWas()
        {
            var keyA = KeyOn(0);
            var keyB = KeyOn(1);
            using var session = SessionThatHasRead(keyA, virtualSublogIdx: 0);

            // B's sublog has not been replayed as far as the session has read
            ClassicAssert.IsTrue(MustWait(session, RespCommand.GET, out var virtualSublogIdx, out var sequenceNumber, keyB.Key));
            ClassicAssert.AreEqual(1, virtualSublogIdx);
            ClassicAssert.AreEqual(Target, sequenceNumber);

            // Asking again gives the same answer, since the probe does not read
            ClassicAssert.IsTrue(MustWait(session, RespCommand.GET, out virtualSublogIdx, out sequenceNumber, keyB.Key));
            ClassicAssert.AreEqual(1, virtualSublogIdx);
            ClassicAssert.AreEqual(Target, sequenceNumber);

            // The sublog of the key the session has read never waits
            ClassicAssert.IsFalse(MustWait(session, RespCommand.GET, out _, out _, keyA.Key));

            // Replay that has only reached what the session has read is still behind it
            manager.UpdateVirtualSublogMaxSequenceNumber(1, Target);
            ClassicAssert.IsTrue(MustWait(session, RespCommand.GET, out _, out _, keyB.Key));

            manager.UpdateVirtualSublogMaxSequenceNumber(1, Target + 1);
            ClassicAssert.IsFalse(MustWait(session, RespCommand.GET, out _, out _, keyB.Key));
        }

        [Test]
        public void AProbeThatDoesNotWaitLeavesTheSessionsReadContextAsItWas()
        {
            var keyA = KeyOn(0);
            var keyB = KeyOn(1);
            using var session = SessionThatHasRead(keyA, virtualSublogIdx: 0);

            // Replay has brought B's sublog past what the session has read, to a sequence number that A's sublog has not reached
            manager.UpdateVirtualSublogKeySequenceNumber(1, keyB.Hash, Target + 5);
            ClassicAssert.IsFalse(MustWait(session, RespCommand.GET, out _, out _, keyB.Key));

            // The session has not read B, so it is still on A's sublog and A never waits. Had the probe moved the
            // session to B, reading A would wait for replay of A's sublog to pass B's sequence number.
            ClassicAssert.IsFalse(MustWait(session, RespCommand.GET, out _, out _, keyA.Key));
        }

        [Test]
        public void AProbeOfSeveralKeysAdvancesThroughThemInOrder()
        {
            var keyA = KeyOn(0);
            var keyB = KeyOn(1);
            manager.UpdateVirtualSublogKeySequenceNumber(0, keyA.Hash, Target);
            using var session = new ReadSessionState(appendOnlyFile, options);

            // A session that has read nothing yet can read any one key, but once the command has read A, B is a switch
            // to a sublog replay has not brought up to A
            ClassicAssert.IsFalse(MustWait(session, RespCommand.MGET, out _, out _, keyA.Key));
            ClassicAssert.IsTrue(MustWait(session, RespCommand.MGET, out var virtualSublogIdx, out var sequenceNumber, keyA.Key, keyB.Key));
            ClassicAssert.AreEqual(1, virtualSublogIdx);
            ClassicAssert.AreEqual(Target, sequenceNumber);
        }

        [Test]
        public void ProbesAgainstAManagerThatReplacedTheOneASessionReadThroughStartTheSessionAfresh()
        {
            var keyA = KeyOn(0);
            var keyB = KeyOn(1);
            using var session = SessionThatHasRead(keyA, virtualSublogIdx: 0);
            ClassicAssert.IsTrue(MustWait(session, RespCommand.GET, out _, out _, keyB.Key));

            // What the session has read through a manager says nothing about the one that replaced it
            appendOnlyFile.CreateOrUpdateKeySequenceManager();
            manager = appendOnlyFile.readConsistencyManager;

            ClassicAssert.IsFalse(MustWait(session, RespCommand.GET, out _, out _, keyB.Key));

            ClassicAssert.IsTrue(session.BeginScanProbe());
            ClassicAssert.IsFalse(session.ProbeScanKey(keyB.Hash));
            ClassicAssert.IsFalse(session.EndScanProbe(completed: false, out _, out _, out _));
        }

        [Test]
        public void AScanStopsAtTheFirstRecordItWouldWaitFor()
        {
            var keyA = KeyOn(0);
            var keyB = KeyOn(1);
            using var session = SessionThatHasRead(keyA, virtualSublogIdx: 0);

            ClassicAssert.IsTrue(session.BeginScanProbe());
            ClassicAssert.IsTrue(session.ScanProbing);

            ClassicAssert.IsFalse(session.ProbeScanKey(keyA.Hash));
            session.PostScanKey();
            ClassicAssert.IsFalse(session.ScanMustWait);

            ClassicAssert.IsTrue(session.ProbeScanKey(keyB.Hash));
            ClassicAssert.IsTrue(session.ScanMustWait);

            ClassicAssert.IsTrue(session.EndScanProbe(completed: true, out var probed, out var virtualSublogIdx, out var sequenceNumber));
            ClassicAssert.AreSame(manager, probed);
            ClassicAssert.AreEqual(1, virtualSublogIdx);
            ClassicAssert.AreEqual(Target, sequenceNumber);
            ClassicAssert.IsFalse(session.ScanProbing);
            ClassicAssert.IsFalse(session.ScanMustWait);
        }

        [Test]
        public void OnlyAScanThatRunsToTheEndAdvancesTheSession()
        {
            var keyA = KeyOn(0);
            var keyB = KeyOn(1);
            using var completes = SessionThatHasRead(keyA, virtualSublogIdx: 0);
            using var throws = SessionThatHasRead(keyA, virtualSublogIdx: 0);

            // Replay has brought B's sublog past what the sessions have read, to a sequence number A's sublog has not reached
            manager.UpdateVirtualSublogKeySequenceNumber(1, keyB.Hash, Target + 5);

            foreach (var (session, completed) in new[] { (completes, true), (throws, false) })
            {
                ClassicAssert.IsTrue(session.BeginScanProbe());
                ClassicAssert.IsFalse(session.ProbeScanKey(keyB.Hash));

                // Replay writes B again between the scan probing it and reading it
                manager.UpdateVirtualSublogKeySequenceNumber(1, keyB.Hash, Target + 9);
                session.PostScanKey();
                ClassicAssert.IsFalse(session.EndScanProbe(completed, out _, out _, out _));
            }

            // Reading A is a switch back from B only for the session that read B, as of the second write
            ClassicAssert.IsTrue(MustWait(completes, RespCommand.GET, out var virtualSublogIdx, out var sequenceNumber, keyA.Key));
            ClassicAssert.AreEqual(0, virtualSublogIdx);
            ClassicAssert.AreEqual(Target + 9, sequenceNumber);
            ClassicAssert.IsFalse(MustWait(throws, RespCommand.GET, out _, out _, keyA.Key));
        }

        [Test]
        public void AScanThatKeepsHavingToWaitIsGivenTheBlockingPath()
        {
            var keyA = KeyOn(0);
            var keyB = KeyOn(1);
            using var session = SessionThatHasRead(keyA, virtualSublogIdx: 0);

            void ScanThatWaits()
            {
                ClassicAssert.IsTrue(session.BeginScanProbe());
                ClassicAssert.IsTrue(session.ProbeScanKey(keyB.Hash));
                ClassicAssert.IsTrue(session.EndScanProbe(completed: true, out _, out _, out _));
            }

            // A scan that gets to the end without waiting starts the count again
            for (var i = 0; i < ReadSessionState.MaxScanParks - 1; i++)
                ScanThatWaits();
            ClassicAssert.IsTrue(session.BeginScanProbe());
            ClassicAssert.IsFalse(session.ProbeScanKey(keyA.Hash));
            ClassicAssert.IsFalse(session.EndScanProbe(completed: true, out _, out _, out _));

            for (var i = 0; i < ReadSessionState.MaxScanParks; i++)
                ScanThatWaits();

            // The next one waits where it stands, which always gets it to the end, and the one after may park again
            ClassicAssert.IsFalse(session.BeginScanProbe());
            ClassicAssert.IsTrue(session.BeginScanProbe());
        }
    }
}