// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Garnet.cluster;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Tests of <see cref="MigrationProgress"/>, the registry that wakes sessions parked on a key being migrated.
    /// A session reads <see cref="MigrationProgress.Version"/>, checks its key, and registers against the version it
    /// read; a migration step changes the key's state and then signals. These tests play both roles, so what they
    /// pin down is that no interleaving of the two leaves a registered session without a notification.
    /// </summary>
    [TestFixture]
    internal class MigrationProgressTests : TestBase
    {
        sealed class CountingWaiter : IMigrationWaiter
        {
            readonly ManualResetEventSlim notified = new(false);
            int notifications;

            internal int Notifications => Volatile.Read(ref notifications);

            internal bool Wait(int millisecondsTimeout) => notified.Wait(millisecondsTimeout);

            internal void Reset() => notified.Reset();

            public void OnMigrationProgress()
            {
                _ = Interlocked.Increment(ref notifications);
                notified.Set();
            }
        }

        /// <summary>A waiter that registers again from inside its notification, as a session resuming from its wait does.</summary>
        sealed class ReRegisteringWaiter(MigrationProgress progress) : IMigrationWaiter
        {
            int notifications;
            int registered;

            internal int Notifications => Volatile.Read(ref notifications);

            internal int Registered => Volatile.Read(ref registered);

            public void OnMigrationProgress()
            {
                _ = Interlocked.Increment(ref notifications);
                if (progress.TryRegister(this, progress.Version))
                    _ = Interlocked.Increment(ref registered);
            }
        }

        [Test]
        public void ARegistrationMadeAfterTheVersionMovedOnIsRefused()
        {
            var progress = new MigrationProgress();
            var waiter = new CountingWaiter();

            // The version is read, the key is found blocked, and a step lands before the session registers: the
            // step has already notified everyone registered, so the session must check its key again.
            var stale = progress.Version;
            progress.Signal();

            ClassicAssert.IsFalse(progress.TryRegister(waiter, stale));
            ClassicAssert.IsTrue(progress.TryRegister(waiter, progress.Version));

            progress.Signal();
            ClassicAssert.AreEqual(1, waiter.Notifications);
        }

        [Test]
        public void SignalNotifiesEachRegisteredWaiterOnceAndThenForgetsIt()
        {
            var progress = new MigrationProgress();
            var first = new CountingWaiter();
            var second = new CountingWaiter();

            ClassicAssert.IsTrue(progress.TryRegister(first, progress.Version));
            ClassicAssert.IsTrue(progress.TryRegister(second, progress.Version));

            progress.Signal();
            ClassicAssert.AreEqual(1, first.Notifications);
            ClassicAssert.AreEqual(1, second.Notifications);

            // A step with nobody registered notifies nobody, and the earlier registrations are spent
            progress.Signal();
            ClassicAssert.AreEqual(1, first.Notifications);
            ClassicAssert.AreEqual(1, second.Notifications);
        }

        [Test]
        public void AWaiterThatRegistersAgainFromItsNotificationIsNotifiedByTheNextStep()
        {
            var progress = new MigrationProgress();
            var waiter = new ReRegisteringWaiter(progress);

            ClassicAssert.IsTrue(progress.TryRegister(waiter, progress.Version));

            // The registration made during the notification belongs to the next step, not to this one
            progress.Signal();
            ClassicAssert.AreEqual(1, waiter.Notifications);
            ClassicAssert.AreEqual(1, waiter.Registered);

            progress.Signal();
            ClassicAssert.AreEqual(2, waiter.Notifications);
            ClassicAssert.AreEqual(2, waiter.Registered);
        }

        [Test, CancelAfter(120_000)]
        public void NoRegisteredWaiterIsLeftWithoutANotificationWhenAStepRacesRegistrations()
        {
            const int Rounds = 20_000;
            const int Waiters = 4;
            var barrierTimeout = TimeSpan.FromSeconds(30);

            var progress = new MigrationProgress();
            var lost = 0;
            var hung = 0;
            var blocked = 1;
            using var start = new Barrier(Waiters + 1);
            using var finish = new Barrier(Waiters + 1);

            bool Arrive(Barrier barrier)
            {
                if (barrier.SignalAndWait(barrierTimeout))
                    return true;

                _ = Interlocked.Increment(ref hung);
                return false;
            }

            var threads = new List<Thread>();
            for (var i = 0; i < Waiters; i++)
            {
                var waiter = new CountingWaiter();
                var seed = i + 1;
                threads.Add(new Thread(() =>
                {
                    var rng = new Random(seed);
                    for (var round = 0; round < Rounds; round++)
                    {
                        if (!Arrive(start))
                            return;

                        // Wait the way a parked session does: read the version, find the key blocked, and register
                        // some time later, which is the window a step can land in
                        while (true)
                        {
                            var version = progress.Version;
                            if (Volatile.Read(ref blocked) == 0)
                                break;

                            Thread.SpinWait(rng.Next(0, 200));

                            waiter.Reset();
                            if (!progress.TryRegister(waiter, version))
                                continue;

                            if (!waiter.Wait(5_000))
                            {
                                _ = Interlocked.Increment(ref lost);
                                break;
                            }
                        }

                        if (!Arrive(finish))
                            return;

                        // Every thread reads the same value here, because a loss was counted before its
                        // thread arrived at the barrier, so they all stop together
                        if (Volatile.Read(ref lost) != 0)
                            return;
                    }
                })
                { IsBackground = true });
            }

            threads.Add(new Thread(() =>
            {
                var rng = new Random(1234);
                for (var round = 0; round < Rounds; round++)
                {
                    Volatile.Write(ref blocked, 1);
                    if (!Arrive(start))
                        return;

                    Thread.SpinWait(rng.Next(0, 400));

                    // The step: the state changes, and only then is the step signalled
                    Volatile.Write(ref blocked, 0);
                    progress.Signal();

                    if (!Arrive(finish))
                        return;

                    if (Volatile.Read(ref lost) != 0)
                        return;
                }
            })
            { IsBackground = true });

            foreach (var thread in threads)
                thread.Start();
            foreach (var thread in threads)
                ClassicAssert.IsTrue(thread.Join(TimeSpan.FromMinutes(1)), "a thread did not finish");

            ClassicAssert.AreEqual(0, hung, "the threads fell out of step");
            ClassicAssert.AreEqual(0, lost, "a registered waiter was never notified");
        }

        [Test, CancelAfter(120_000)]
        public async Task ConcurrentStepsNotifyEveryRegistrationExactlyOnce()
        {
            const int Registrars = 4;
            const int PerRegistrar = 5_000;
            const int Lost = -1;
            const int NotifiedTwice = -2;

            var progress = new MigrationProgress();
            var stop = false;

            var signaller = Task.Run(() =>
            {
                while (!Volatile.Read(ref stop))
                    progress.Signal();
            });

            var registrars = new Task<int>[Registrars];
            for (var i = 0; i < Registrars; i++)
            {
                registrars[i] = Task.Run(() =>
                {
                    var waiter = new CountingWaiter();
                    var delivered = 0;
                    for (var n = 0; n < PerRegistrar; n++)
                    {
                        waiter.Reset();
                        var before = waiter.Notifications;
                        if (!progress.TryRegister(waiter, progress.Version))
                            continue;

                        // Steps keep arriving, so a registration that is not notified within the bound was lost
                        if (!waiter.Wait(5_000))
                            return Lost;

                        // One registration is one notification: a second would mean a list was notified twice
                        if (waiter.Notifications != before + 1)
                            return NotifiedTwice;

                        delivered++;
                    }
                    return delivered;
                });
            }

            var results = await Task.WhenAll(registrars);
            Volatile.Write(ref stop, true);
            await signaller;

            ClassicAssert.IsTrue(results.All(r => r >= 0), $"a registration was lost ({Lost}) or notified twice ({NotifiedTwice}): [{string.Join(", ", results)}]");
            ClassicAssert.Greater(results.Sum(), 0, "no registration was ever accepted, so nothing was notified");
        }

        // A migration unblocks the keys it holds through one of these three steps, depending on the variant:
        // slots mode clears the sketch after each batch, keys mode moves it to MIGRATED, and a store that is
        // disposed lets every key through. A request cannot be woken by a step that does not signal, and an
        // end-to-end migration would reach the next step soon enough to hide the missing one, so each is pinned here.

        [Test]
        public void ClearingASketchNotifiesTheRequestsWaitingOnIt()
        {
            var progress = new MigrationProgress();
            var sketch = new Sketch(1 << 10) { Progress = progress };
            var waiter = new CountingWaiter();

            sketch.SetStatus(SketchStatus.DELETING);
            var version = progress.Version;
            ClassicAssert.IsTrue(progress.TryRegister(waiter, version));

            sketch.Clear();

            ClassicAssert.AreEqual(1, waiter.Notifications);
            ClassicAssert.AreNotEqual(version, progress.Version);
            ClassicAssert.AreEqual(SketchStatus.INITIALIZING, sketch.Status);
        }

        [Test]
        public void ChangingASketchStatusNotifiesTheRequestsWaitingOnIt()
        {
            var progress = new MigrationProgress();
            var sketch = new Sketch(1 << 10) { Progress = progress };
            var waiter = new CountingWaiter();

            ClassicAssert.IsTrue(progress.TryRegister(waiter, progress.Version));

            sketch.SetStatus(SketchStatus.MIGRATED);

            ClassicAssert.AreEqual(1, waiter.Notifications);
            ClassicAssert.AreEqual(SketchStatus.MIGRATED, sketch.Status);
        }

        [Test]
        public void DisposingTheTaskStoreNotifiesTheRequestsWaitingOnIt()
        {
            var progress = new MigrationProgress();
            var store = new MigrateSessionTaskStore(progress);
            var waiter = new CountingWaiter();

            ClassicAssert.IsTrue(progress.TryRegister(waiter, progress.Version));

            store.Dispose();

            ClassicAssert.AreEqual(1, waiter.Notifications);
        }
    }
}