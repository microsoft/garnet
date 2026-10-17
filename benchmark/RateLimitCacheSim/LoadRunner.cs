// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;

namespace RateLimitCacheSim
{
    /// <summary>
    /// Result of a load run.
    /// </summary>
    /// <param name="Latencies">Latencies over the measurement window.</param>
    /// <param name="Throughput">Completed operations per second over the measurement window.</param>
    /// <param name="MaxInFlight">Peak number of operations outstanding at the client.</param>
    /// <param name="Shed">Operations the client refused to issue because its in-flight cap was reached.</param>
    public readonly record struct LoadResult(LatencySet Latencies, double Throughput, int MaxInFlight, long Shed);

    /// <summary>
    /// Drives the modelled operation mix either open loop at a target rate, which is how the
    /// production fleet is loaded, or closed loop at a fixed in-flight depth to find the capacity knee.
    /// </summary>
    public sealed class LoadRunner
    {
        /// <summary>Client-side cap on outstanding operations, to bound memory if the server stalls.</summary>
        const int MaxInFlightCap = 200_000;

        readonly Options opts;
        readonly RateLimitKeyspace keyspace;
        readonly ICacheClient[] clients;

        volatile bool measuring;
        int inFlight;
        int maxInFlight;
        long shed;

        /// <summary>
        /// Creates the runner.
        /// </summary>
        /// <param name="opts">Run options.</param>
        /// <param name="keyspace">Key space and operation mix.</param>
        /// <param name="clients">One connected client per connection.</param>
        public LoadRunner(Options opts, RateLimitKeyspace keyspace, ICacheClient[] clients)
        {
            this.opts = opts;
            this.keyspace = keyspace;
            this.clients = clients;
        }

        /// <summary>
        /// Runs warmup followed by the measurement window.
        /// </summary>
        /// <returns>The merged result over the measurement window.</returns>
        public async Task<LoadResult> RunAsync()
            => opts.Mode == RunMode.Saturate ? await RunSaturateAsync().ConfigureAwait(false) : await RunRateAsync().ConfigureAwait(false);

        async Task<LoadResult> RunRateAsync()
        {
            var sets = new LatencySet[clients.Length];
            for (var i = 0; i < sets.Length; i++) sets[i] = new LatencySet();

            using var cts = new CancellationTokenSource();
            var ratePerConnection = (double)opts.Ops / clients.Length;
            var intervalTicks = Stopwatch.Frequency / ratePerConnection;

            // Pacers run on dedicated threads. On the thread pool they would compete with the
            // completion callbacks they are generating, which turns a client-side scheduling
            // shortfall into what looks like server latency.
            var pacers = new Thread[clients.Length];
            for (var i = 0; i < clients.Length; i++)
            {
                var index = i;
                pacers[i] = new Thread(() => Pace(clients[index], sets[index], intervalTicks, index, cts.Token))
                { IsBackground = true, Name = $"rl-pacer-{index}" };
                pacers[i].Start();
            }

            var elapsed = await MeasureWindowAsync(sets).ConfigureAwait(false);
            cts.Cancel();
            foreach (var pacer in pacers) pacer.Join(TimeSpan.FromSeconds(10));

            return Summarize(sets, elapsed);
        }

        /// <summary>
        /// Emits operations at their ideal arrival times. The thread wakes on a coarse timer and emits
        /// every operation that has come due, each stamped with its exact ideal time rather than the
        /// time the thread happened to wake, so a late wake shows up as schedule delay instead of
        /// silently disappearing.
        /// </summary>
        void Pace(ICacheClient client, LatencySet set, double intervalTicks, int seed, CancellationToken token)
        {
            var rng = new Random(seed + 1);
            var origin = Clock.Now() + (long)(rng.NextDouble() * intervalTicks);
            var issued = 0L;
            var nextDue = origin;

            while (!token.IsCancellationRequested)
            {
                var now = Clock.Now();

                while (nextDue <= now && !token.IsCancellationRequested)
                {
                    Fire(client, set, nextDue, rng);
                    issued++;
                    nextDue = origin + (long)(issued * intervalTicks);
                }

                var wait = nextDue - Clock.Now();
                if (wait <= 0) continue;

                if (wait > Clock.TicksPerMs)
                    _ = token.WaitHandle.WaitOne((int)(wait / Clock.TicksPerMs));
                else
                    Thread.Yield();
            }
        }

        void Fire(ICacheClient client, LatencySet set, long due, Random rng)
        {
            var current = Interlocked.Increment(ref inFlight);
            if (current > MaxInFlightCap)
            {
                Interlocked.Decrement(ref inFlight);
                Interlocked.Increment(ref shed);
                return;
            }

            var observedMax = maxInFlight;
            if (current > observedMax) Interlocked.CompareExchange(ref maxInFlight, current, observedMax);

            var shape = keyspace.ShapeFor(rng);
            var key = keyspace.KeyFor(JobKind.EndpointSync, shape, rng);
            var field = keyspace.FieldFor(rng);
            var send = Clock.Now();

            _ = CompleteAsync(client, set, shape, key, field, due, send);
        }

        async Task CompleteAsync(ICacheClient client, LatencySet set, OpShape shape, string key, string field, long due, long send)
        {
            try
            {
                await client.ExecuteAsync(shape, key, field).ConfigureAwait(false);
                var done = Clock.Now();
                if (measuring) lock (set) set.Record(due, send, done);
            }
            catch (Exception ex)
            {
                if (measuring) lock (set) set.RecordError(ex);
            }
            finally
            {
                Interlocked.Decrement(ref inFlight);
            }
        }

        async Task<LoadResult> RunSaturateAsync()
        {
            var sets = new LatencySet[clients.Length * opts.InFlightPerConnection];
            for (var i = 0; i < sets.Length; i++) sets[i] = new LatencySet();

            using var cts = new CancellationTokenSource();
            var workers = new Task[sets.Length];
            for (var i = 0; i < sets.Length; i++)
            {
                var index = i;
                var client = clients[i / opts.InFlightPerConnection];
                workers[i] = Task.Run(() => SaturateAsync(client, sets[index], index, cts.Token), CancellationToken.None);
            }

            var elapsed = await MeasureWindowAsync(sets).ConfigureAwait(false);
            cts.Cancel();
            await Task.WhenAll(workers).WaitAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false);

            return Summarize(sets, elapsed);
        }

        async Task SaturateAsync(ICacheClient client, LatencySet set, int seed, CancellationToken token)
        {
            var rng = new Random(seed + 1);
            while (!token.IsCancellationRequested)
            {
                var shape = keyspace.ShapeFor(rng);
                var key = keyspace.KeyFor(JobKind.EndpointSync, shape, rng);
                var field = keyspace.FieldFor(rng);
                var send = Clock.Now();
                try
                {
                    await client.ExecuteAsync(shape, key, field).ConfigureAwait(false);
                    // Closed loop: the operation becomes due the moment the previous one finished,
                    // so schedule delay is zero by construction and service time is the whole story.
                    if (measuring) set.Record(send, send, Clock.Now());
                }
                catch (Exception ex) when (!token.IsCancellationRequested)
                {
                    if (measuring) set.RecordError(ex);
                }
            }
        }

        async Task<double> MeasureWindowAsync(LatencySet[] sets)
        {
            if (opts.Warmup > 0)
                await Task.Delay(TimeSpan.FromSeconds(opts.Warmup)).ConfigureAwait(false);

            foreach (var set in sets) lock (set) set.Reset();
            maxInFlight = 0;
            shed = 0;

            var start = Clock.Now();
            measuring = true;
            await Task.Delay(TimeSpan.FromSeconds(opts.Runtime)).ConfigureAwait(false);
            measuring = false;
            return (double)(Clock.Now() - start) / Stopwatch.Frequency;
        }

        LoadResult Summarize(LatencySet[] sets, double elapsedSeconds)
        {
            var merged = new LatencySet();
            foreach (var set in sets) lock (set) merged.Merge(set);
            return new LoadResult(merged, merged.Completed / elapsedSeconds, maxInFlight, shed);
        }
    }
}