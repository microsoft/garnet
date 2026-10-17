// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace RateLimitCacheSim
{
    /// <summary>
    /// Reproduces the two-VM experiment: N mock service instances, each holding its own
    /// connection and running three periodic background jobs against the cache.
    /// </summary>
    /// <remarks>
    /// The offered load here is fixed by the job periods, not by the server. With the documented periods
    /// each instance offers 1/2 + 1/0.25 + 1/5 = 4.7 ops/sec, so 50 instances offer only ~235 ops/sec.
    /// Because the runner separates schedule delay from service time, a result dominated by the client
    /// rather than the server is visible directly in the output.
    /// </remarks>
    public sealed class PeriodicRunner
    {
        readonly Options opts;
        readonly RateLimitKeyspace keyspace;
        readonly ICacheClient[] clients;

        volatile bool measuring;

        /// <summary>
        /// Creates the runner.
        /// </summary>
        /// <param name="opts">Run options.</param>
        /// <param name="keyspace">Key space and operation mix.</param>
        /// <param name="clients">One connected client per mock service instance.</param>
        public PeriodicRunner(Options opts, RateLimitKeyspace keyspace, ICacheClient[] clients)
        {
            this.opts = opts;
            this.keyspace = keyspace;
            this.clients = clients;
        }

        /// <summary>
        /// Runs warmup followed by the measurement window.
        /// </summary>
        /// <returns>The merged latency set over the measurement window.</returns>
        public async Task<LatencySet> RunAsync()
        {
            var jobs = BuildJobs();
            using var cts = new CancellationTokenSource();

            var tasks = new List<Task>(jobs.Count);
            foreach (var job in jobs)
            {
                if (opts.DedicatedThreads)
                {
                    var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    var thread = new Thread(() =>
                    {
                        try { RunJobBlocking(job, cts.Token); }
                        finally { tcs.TrySetResult(); }
                    })
                    { IsBackground = true, Name = $"rl-{job.Instance}-{job.Kind}" };
                    thread.Start();
                    tasks.Add(tcs.Task);
                }
                else if (opts.Sync)
                {
                    tasks.Add(Task.Run(() => RunJobBlocking(job, cts.Token), CancellationToken.None));
                }
                else
                {
                    tasks.Add(RunJobAsync(job, cts.Token));
                }
            }

            if (opts.Warmup > 0)
                await Task.Delay(TimeSpan.FromSeconds(opts.Warmup)).ConfigureAwait(false);

            foreach (var job in jobs) job.Latencies.Reset();
            measuring = true;

            await Task.Delay(TimeSpan.FromSeconds(opts.Runtime)).ConfigureAwait(false);
            measuring = false;
            cts.Cancel();

            await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false);

            var merged = new LatencySet();
            foreach (var job in jobs) merged.Merge(job.Latencies);
            return merged;
        }

        List<Job> BuildJobs()
        {
            var jobs = new List<Job>(clients.Length * 3);
            var seed = 0;
            for (var i = 0; i < clients.Length; i++)
            {
                jobs.Add(new Job(i, JobKind.Probe, opts.ProbePeriodMs, clients[i], new Random(++seed)));
                jobs.Add(new Job(i, JobKind.EndpointSync, opts.EndpointPeriodMs, clients[i], new Random(++seed)));
                jobs.Add(new Job(i, JobKind.TenantSync, opts.TenantPeriodMs, clients[i], new Random(++seed)));
            }
            return jobs;
        }

        async Task RunJobAsync(Job job, CancellationToken token)
        {
            var periodTicks = (long)job.PeriodMs * Clock.TicksPerMs;
            var due = Clock.Now() + (long)(job.Rng.NextDouble() * periodTicks);

            while (!token.IsCancellationRequested)
            {
                await Clock.WaitUntilAsync(due, token).ConfigureAwait(false);
                if (token.IsCancellationRequested) break;

                var shape = keyspace.ShapeFor(job.Rng);
                var key = keyspace.KeyFor(job.Kind, shape, job.Rng);
                var field = keyspace.FieldFor(job.Rng);

                var send = Clock.Now();
                try
                {
                    await job.Client.ExecuteAsync(shape, key, field).ConfigureAwait(false);
                    if (measuring) job.Latencies.Record(due, send, Clock.Now());
                }
                catch (Exception ex) when (!token.IsCancellationRequested)
                {
                    if (measuring) job.Latencies.RecordError(ex);
                }

                due += periodTicks;
            }
        }

        void RunJobBlocking(Job job, CancellationToken token)
        {
            var periodTicks = (long)job.PeriodMs * Clock.TicksPerMs;
            var due = Clock.Now() + (long)(job.Rng.NextDouble() * periodTicks);

            while (!token.IsCancellationRequested)
            {
                Clock.WaitUntil(due, token);
                if (token.IsCancellationRequested) break;

                var shape = keyspace.ShapeFor(job.Rng);
                var key = keyspace.KeyFor(job.Kind, shape, job.Rng);
                var field = keyspace.FieldFor(job.Rng);

                var send = Clock.Now();
                try
                {
                    job.Client.Execute(shape, key, field);
                    if (measuring) job.Latencies.Record(due, send, Clock.Now());
                }
                catch (Exception ex) when (!token.IsCancellationRequested)
                {
                    if (measuring) job.Latencies.RecordError(ex);
                }

                due += periodTicks;
            }
        }

        sealed class Job(int instance, JobKind kind, int periodMs, ICacheClient client, Random rng)
        {
            public int Instance { get; } = instance;

            public JobKind Kind { get; } = kind;

            public int PeriodMs { get; } = periodMs;

            public ICacheClient Client { get; } = client;

            public Random Rng { get; } = rng;

            public LatencySet Latencies { get; } = new();
        }
    }
}