// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.devices
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Lease timing requires better reliability that what we get from asynchronous Task.Delay, 
    /// so we implement a timer wheel.
    /// </summary>
    class LeaseTimer
    {
        const int MaxDelay = 60;
        const int TicksPerSecond = 4;

        static readonly Lazy<LeaseTimer> instance = new Lazy<LeaseTimer>(() => new LeaseTimer());

        readonly Timer timer;
        readonly Entry[] schedule = new Entry[MaxDelay * TicksPerSecond];
        public readonly object reentrancyLock = new object();

        readonly Stopwatch stopwatch = new Stopwatch();
        int performedSteps;

        int position = 0;

        public static LeaseTimer Instance => instance.Value;

        public Action<int> DelayWarning { get; set; }

        class Entry
        {
            public TaskCompletionSource<bool> Tcs;
            public CancellationTokenRegistration Registration;
            public Func<Task> Callback;
            public Entry Next;

            public async Task Run()
            {
                try
                {
                    await Callback().ConfigureAwait(false);
                    Tcs.TrySetResult(true);

                }
                catch (Exception exception) when (!Utils.IsFatal(exception))
                {
                    Tcs.TrySetException(exception);
                }
                Registration.Dispose();
            }

            public void Cancel()
            {
                Tcs.TrySetCanceled();
                Registration.Dispose();
            }

            public void RunAll()
            {
                var _ = Run();
                if (Next != null)
                {
                    Next.RunAll();
                }
            }
        }

        LeaseTimer()
        {
            timer = new Timer(Run, null, 0, 1000 / TicksPerSecond);
            stopwatch.Start();
        }

        public void Run(object _)
        {
            if (Monitor.TryEnter(reentrancyLock))
            {
                try
                {
                    var stepsToDo = (stopwatch.ElapsedMilliseconds * TicksPerSecond / 1000) - performedSteps;

                    if (stepsToDo > 5 * TicksPerSecond)
                    {
                        DelayWarning?.Invoke((int)stepsToDo / TicksPerSecond);
                    }

                    for (int i = 0; i < stepsToDo; i++)
                    {
                        AdvancePosition();
                    }
                }
                finally
                {
                    Monitor.Exit(reentrancyLock);
                }
            }
        }

        void AdvancePosition()
        {
            int position = this.position;
            this.position = (position + 1) % (MaxDelay * TicksPerSecond);
            performedSteps++;

            Entry current;
            while (true)
            {
                current = schedule[position];
                if (current == null || Interlocked.CompareExchange<Entry>(ref schedule[position], null, current) == current)
                {
                    break;
                }
            }

            current?.RunAll();
        }

        public Task Schedule(int delay, Func<Task> callback, CancellationToken token)
        {
            if ((delay / 1000) >= MaxDelay || delay < 0)
            {
                throw new ArgumentException(nameof(delay));
            }

            var entry = new Entry()
            {
                Tcs = new TaskCompletionSource<bool>(),
                Callback = callback,
            };

            entry.Registration = token.Register(entry.Cancel);

            while (true)
            {
                int targetPosition = (position + (delay * TicksPerSecond) / 1000) % (MaxDelay * TicksPerSecond);

                if (targetPosition == position)
                {
                    return callback();
                }
                else
                {
                    Entry current = schedule[targetPosition];
                    entry.Next = current;
                    if (Interlocked.CompareExchange<Entry>(ref schedule[targetPosition], entry, current) == current)
                    {
                        return entry.Tcs.Task;
                    }
                }
            }
        }
    }
}