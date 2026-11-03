// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Reflection;
#if EPOCH_RELEASE_WORKLOAD_TESTS
using System;
using System.Linq;
using System.Threading;
#endif
#if GARNET_CLIENT_EPOCH_BENCHMARK
using Garnet.client;

namespace BDN.benchmark.Client
#else
using Tsavorite.core;

namespace BenchmarkDotNetTests
#endif
{
    public enum EpochReleasePressure
    {
        Stable,
        Drain,
        Waiter
    }

    sealed class EpochReleaseWorkload : IDisposable
    {
        internal const int Operations = 65_536;
        const int PendingActions = 4;
        static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
        static readonly FieldInfo WaiterCount = GetField("waiterCount");
        static readonly FieldInfo DrainCount = GetField("drainCount");

        readonly LightEpoch epoch;
        readonly ManualResetEventSlim releaseHolders = new();
        readonly ManualResetEventSlim releasePrimed = new();
        readonly ManualResetEventSlim continueDrain = new();
        readonly AutoResetEvent acquireDrain;
        readonly AutoResetEvent drainAcquired;
        readonly AutoResetEvent releaseDrain;
        readonly AutoResetEvent drainReleased;
        readonly int[] actions = new int[PendingActions];
        readonly Action[] callbacks = new Action[PendingActions];
        readonly int threads;
        readonly int jitter;
        readonly EpochReleasePressure pressure;
        readonly Exception[] errors;
        readonly Thread[] workers;
        readonly Barrier start;
        readonly Barrier done;
        readonly CountdownEvent ready;
        readonly CountdownEvent halfway;
        Thread[] holders = [];
        Exception[] holderErrors = [];
        CountdownEvent holdersReady;
        CountdownEvent primed;
        volatile bool stopping;
        bool priming;
        bool disposed;
        int completedRounds;

        internal EpochReleaseWorkload(int threads, int jitter, EpochReleasePressure pressure)
        {
            if (threads < 2 || threads > Operations || Operations % threads != 0 || threads >= LightEpoch.EntryCount)
                throw new ArgumentOutOfRangeException(nameof(threads), "Use at least two workers, fewer than the table size, dividing the operation count.");
            if (jitter < 0 || jitter > 64)
                throw new ArgumentOutOfRangeException(nameof(jitter), "Jitter must be between zero and 64 spin iterations.");
            epoch = new LightEpoch();
            acquireDrain = new AutoResetEvent(false);
            drainAcquired = new AutoResetEvent(false);
            releaseDrain = new AutoResetEvent(false);
            drainReleased = new AutoResetEvent(false);
            this.threads = threads;
            this.jitter = jitter;
            this.pressure = pressure;
            workers = new Thread[threads];
            errors = new Exception[threads];
            ready = new CountdownEvent(threads);
            halfway = new CountdownEvent(threads);
            start = new Barrier(threads + 1);
            done = new Barrier(threads + 1);
        }

        internal void Initialize()
        {
            for (var i = 0; i < threads; i++)
            {
                var index = i;
                workers[i] = new Thread(() => Worker(index)) { IsBackground = true, Name = $"EpochRelease-{i}" };
                workers[i].Start();
            }
            Wait(ready, "Worker initialization");
            ThrowErrors();

            var holderCount = pressure switch
            {
                EpochReleasePressure.Drain => 1,
                EpochReleasePressure.Waiter => LightEpoch.EntryCount - threads / 2,
                _ => 0
            };
            holders = new Thread[holderCount];
            holderErrors = new Exception[holderCount];
            holdersReady = new CountdownEvent(holderCount);
            for (var i = 0; i < holderCount; i++)
            {
                var index = i;
                holders[i] = new Thread(() => Hold(index)) { IsBackground = true, Name = $"EpochRelease-holder-{i}" };
                holders[i].Start();
            }
            Wait(holdersReady, "Slot holders");
            ThrowErrors();

            if (pressure == EpochReleasePressure.Drain)
            {
                for (var i = 0; i < PendingActions; i++)
                {
                    var index = i;
                    callbacks[i] = () => Interlocked.Increment(ref actions[index]);
                }
                BeginDrain();
                QueueActions();
                if (Read(DrainCount) != PendingActions || actions.Any(value => value != 0))
                    throw new InvalidOperationException("The old-epoch holder did not retain every drain action.");
                EndDrain();
                completedRounds++;
            }
            else if (pressure == EpochReleasePressure.Waiter)
            {
                primed = new CountdownEvent(threads / 2);
                priming = true;
                Rendezvous(start);
                try
                {
                    Wait(primed, "Priming the available slots");
                    var watch = Stopwatch.StartNew();
                    while (Read(WaiterCount) < threads - threads / 2)
                    {
                        if (watch.Elapsed > Timeout)
                            throw new TimeoutException("Contenders did not register as epoch waiters.");
                        Thread.Sleep(1);
                    }
                }
                finally
                {
                    releasePrimed.Set();
                    Rendezvous(done);
                    priming = false;
                }
                ThrowErrors();
            }
            Validate();
        }

        void Hold(int index)
        {
            if (pressure == EpochReleasePressure.Drain)
            {
                HoldDrain(index);
                return;
            }
            var acquired = false;
            try
            {
                try
                {
                    epoch.Resume();
                    acquired = true;
                }
                finally
                {
                    holdersReady.Signal();
                }
                if (!releaseHolders.Wait(TimeSpan.FromMinutes(15)))
                    throw new TimeoutException("Slot holder was not released.");
            }
            catch (Exception exception)
            {
                holderErrors[index] = exception;
            }
            finally
            {
                if (acquired)
                {
                    try
                    {
                        epoch.Suspend();
                    }
                    catch (Exception exception)
                    {
                        holderErrors[index] = exception;
                    }
                }
            }
        }

        void HoldDrain(int index)
        {
            try
            {
                holdersReady.Signal();
                while (true)
                {
                    if (!acquireDrain.WaitOne(TimeSpan.FromMinutes(15)))
                        throw new TimeoutException("Drain holder was not requested.");
                    if (stopping)
                        return;
                    epoch.Resume();
                    try
                    {
                        drainAcquired.Set();
                        if (!releaseDrain.WaitOne(Timeout))
                            throw new TimeoutException("Drain holder was not released.");
                    }
                    finally
                    {
                        epoch.Suspend();
                        drainReleased.Set();
                    }
                }
            }
            catch (Exception exception)
            {
                holderErrors[index] = exception;
            }
        }

        void Worker(int index)
        {
            EpochBenchmarkEnvironment.Affinity affinity = null;
            try
            {
                var delays = CreateDelays(index, jitter);
                try
                {
                    affinity = EpochBenchmarkEnvironment.Pin(index, threads);
                    for (var i = 0; i < 1_000; i++)
                    {
                        epoch.Resume();
                        epoch.Suspend();
                    }
                }
                catch (Exception exception)
                {
                    errors[index] = exception;
                }
                finally
                {
                    ready.Signal();
                }
                while (true)
                {
                    Rendezvous(start);
                    if (stopping)
                        return;
                    try
                    {
                        if (errors[index] != null)
                            continue;
                        if (priming)
                        {
                            epoch.Resume();
                            try
                            {
                                if (primed.CurrentCount > 0)
                                    primed.Signal();
                                if (!releasePrimed.Wait(Timeout))
                                    throw new TimeoutException("Primed worker was not released.");
                            }
                            finally
                            {
                                epoch.Suspend();
                            }
                        }
                        else
                        {
                            var count = Operations / threads;
                            if (pressure == EpochReleasePressure.Drain)
                            {
                                Cycles(epoch, delays, count / 2);
                                halfway.Signal();
                                if (!continueDrain.Wait(Timeout))
                                    throw new TimeoutException("Drain transition was not released.");
                                Cycles(epoch, delays, count / 2, count / 2);
                            }
                            else
                            {
                                Cycles(epoch, delays, count);
                            }
                        }
                    }
                    catch (Exception exception)
                    {
                        errors[index] = exception;
                    }
                    finally
                    {
                        Rendezvous(done);
                    }
                }
            }
            catch (Exception exception)
            {
                errors[index] = exception;
            }
            finally
            {
                affinity?.Dispose();
            }
        }

        internal void Run()
        {
            if (pressure == EpochReleasePressure.Drain)
            {
                halfway.Reset(threads);
                continueDrain.Reset();
                BeginDrain();
            }
            Rendezvous(start);
            try
            {
                if (pressure == EpochReleasePressure.Drain)
                {
                    QueueActions();
                    Wait(halfway, "Protected drain phase");
                }
            }
            finally
            {
                if (pressure == EpochReleasePressure.Drain)
                {
                    releaseDrain.Set();
                    continueDrain.Set();
                }
                Rendezvous(done);
            }
            if (pressure == EpochReleasePressure.Drain)
            {
                Wait(drainReleased, "Drain holder release");
                completedRounds++;
            }
            ThrowErrors();
        }

        void BeginDrain()
        {
            acquireDrain.Set();
            Wait(drainAcquired, "Drain holder acquisition");
        }

        void QueueActions()
        {
            epoch.Resume();
            try
            {
                foreach (var callback in callbacks)
                    epoch.BumpCurrentEpoch(callback);
            }
            finally
            {
                epoch.Suspend();
            }
        }

        void EndDrain()
        {
            releaseDrain.Set();
            Wait(drainReleased, "Drain holder release");
        }

        internal void Validate()
        {
            ThrowErrors();
            if (Read(DrainCount) != 0 || actions.Any(value => value != completedRounds))
                throw new InvalidOperationException("Pending actions remain or an action did not execute exactly once per round.");
            if (Read(WaiterCount) != 0)
                throw new InvalidOperationException("A worker is still waiting after its batch completed.");
        }

        internal static int[] CreateDelays(int index, int maximum)
        {
            var result = new int[4_096];
            var state = 0x9E3779B9u * (uint)(index + 1);
            for (var i = 0; i < result.Length; i++)
            {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                result[i] = (int)(state % (uint)(maximum + 1));
            }
            return result;
        }

        static void Cycles(LightEpoch epoch, int[] delays, int count, int offset = 0)
        {
            for (var i = 0; i < count; i++)
            {
                epoch.Resume();
                try
                {
                    // Bounded CPU-local work models short, variable protected holds without scheduler yielding.
                    var delay = delays[(2 * (i + offset)) & (delays.Length - 1)];
                    if (delay != 0)
                        Thread.SpinWait(delay);
                }
                finally
                {
                    epoch.Suspend();
                }
                var outside = delays[(2 * (i + offset) + 1) & (delays.Length - 1)];
                if (outside != 0)
                    Thread.SpinWait(outside);
            }
        }

        public void Dispose()
        {
            if (disposed)
                return;
            disposed = true;
            releaseHolders.Set();
            releasePrimed.Set();
            stopping = true;
            acquireDrain.Set();
            releaseDrain.Set();
            continueDrain.Set();
            Rendezvous(start);
            foreach (var worker in workers)
            {
                if (!worker.Join(Timeout))
                    throw new TimeoutException("Epoch release worker did not stop.");
            }
            foreach (var holder in holders)
            {
                if (!holder.Join(Timeout))
                    throw new TimeoutException("Epoch slot holder did not stop.");
            }
            ThrowErrors();
            if (Read(DrainCount) != 0 || Read(WaiterCount) != 0
                || actions.Any(value => value != completedRounds))
                throw new InvalidOperationException("Epoch did not quiesce or drain actions did not execute exactly once.");
            epoch.Dispose();
            start.Dispose();
            done.Dispose();
            ready.Dispose();
            holdersReady?.Dispose();
            primed?.Dispose();
            releaseHolders.Dispose();
            releasePrimed.Dispose();
            continueDrain.Dispose();
            acquireDrain.Dispose();
            drainAcquired.Dispose();
            releaseDrain.Dispose();
            drainReleased.Dispose();
            halfway.Dispose();
        }

        void ThrowErrors()
        {
            var failures = errors.Concat(holderErrors).Where(error => error != null).ToArray();
            if (failures.Length != 0)
                throw new AggregateException("Epoch release workload failed.", failures);
        }

        int Read(FieldInfo field) => (int)field.GetValue(epoch);

        static FieldInfo GetField(string name) => typeof(LightEpoch).GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new MissingFieldException(typeof(LightEpoch).FullName, name);

        static void Rendezvous(Barrier barrier)
        {
            if (!barrier.SignalAndWait(Timeout))
                throw new TimeoutException("Epoch release workers did not rendezvous.");
        }

        static void Wait(CountdownEvent signal, string operation)
        {
            if (!signal.Wait(Timeout))
                throw new TimeoutException($"{operation} timed out.");
        }

        static void Wait(WaitHandle signal, string operation)
        {
            if (!signal.WaitOne(Timeout))
                throw new TimeoutException($"{operation} timed out.");
        }
    }
}