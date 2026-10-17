// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using StackExchange.Redis;

namespace RateLimitCacheSim
{
    /// <summary>
    /// The three periodic background jobs each mock service instance runs against the cache.
    /// </summary>
    public enum JobKind
    {
        /// <summary>Liveness probe, every 2 s by default.</summary>
        Probe,

        /// <summary>Endpoint token-bucket sync, every 250 ms by default. Dominates the offered load.</summary>
        EndpointSync,

        /// <summary>Tenant token-bucket sync, every 5 s by default.</summary>
        TenantSync,
    }

    /// <summary>
    /// Key space and operation mix for the modelled workload.
    /// </summary>
    public sealed class RateLimitKeyspace
    {
        readonly Options opts;

        /// <summary>The value written back by the read-write script.</summary>
        public string BucketValue { get; }

        /// <summary>
        /// Creates the key space.
        /// </summary>
        /// <param name="opts">Run options.</param>
        public RateLimitKeyspace(Options opts)
        {
            this.opts = opts;
            BucketValue = new string('v', opts.ValueLength);
        }

        /// <summary>Endpoint bucket key for an index.</summary>
        /// <param name="i">Endpoint index.</param>
        /// <returns>The key.</returns>
        public static string EndpointKey(int i) => $"rl:ep:{i}";

        /// <summary>Tenant bucket key for an index.</summary>
        /// <param name="i">Tenant index.</param>
        /// <returns>The key.</returns>
        public static string TenantKey(int i) => $"rl:tn:{i}";

        /// <summary>String key used by the non-scripted share of the traffic.</summary>
        /// <param name="i">String key index.</param>
        /// <returns>The key.</returns>
        public static string StringKey(int i) => $"rl:s:{i}";

        /// <summary>Bucket field name for an index.</summary>
        /// <param name="i">Field index.</param>
        /// <returns>The field name.</returns>
        public static string Field(int i) => $"f{i}";

        /// <summary>
        /// Picks the key touched by a job.
        /// </summary>
        /// <param name="kind">Job kind.</param>
        /// <param name="shape">Operation shape, which decides whether a hash or a string key is needed.</param>
        /// <param name="rng">Random source.</param>
        /// <returns>The key.</returns>
        public string KeyFor(JobKind kind, OpShape shape, Random rng)
        {
            if (shape == OpShape.PlainGet) return StringKey(rng.Next(opts.Endpoints));
            return kind == JobKind.TenantSync ? TenantKey(rng.Next(opts.Tenants)) : EndpointKey(rng.Next(opts.Endpoints));
        }

        /// <summary>
        /// Picks a bucket field.
        /// </summary>
        /// <param name="rng">Random source.</param>
        /// <returns>The field name.</returns>
        public string FieldFor(Random rng) => Field(rng.Next(opts.Fields));

        /// <summary>
        /// Picks an operation shape using the configured mix: <c>--eval-ratio</c> of operations are
        /// scripted, and <c>--rw-ratio</c> of those are the HGET + HSET + EXPIRE variant.
        /// </summary>
        /// <param name="rng">Random source.</param>
        /// <returns>The operation shape.</returns>
        public OpShape ShapeFor(Random rng)
        {
            if (rng.NextDouble() >= opts.EvalRatio) return OpShape.PlainGet;
            return rng.NextDouble() < opts.ReadWriteRatio ? OpShape.ScriptReadWrite : OpShape.ScriptRead;
        }

        /// <summary>
        /// Populates the bucket hashes so that every HGET hits.
        /// </summary>
        /// <param name="opts">Run options.</param>
        /// <returns>A task that completes when the key space is loaded.</returns>
        public async Task LoadAsync(Options opts)
        {
            var config = SeRedisConfig.Build(opts, 120000);

            using var mux = await ConnectionMultiplexer.ConnectAsync(config).ConfigureAwait(false);
            var db = mux.GetDatabase();

            var entries = new HashEntry[opts.Fields];
            for (var f = 0; f < opts.Fields; f++)
                entries[f] = new HashEntry(Field(f), BucketValue);

            var pending = new List<Task>(1024);
            for (var i = 0; i < opts.Endpoints; i++)
            {
                pending.Add(db.HashSetAsync(EndpointKey(i), entries));
                pending.Add(db.StringSetAsync(StringKey(i), BucketValue));
                if (pending.Count < 1024) continue;
                await Task.WhenAll(pending).ConfigureAwait(false);
                pending.Clear();
            }

            for (var i = 0; i < opts.Tenants; i++)
            {
                pending.Add(db.HashSetAsync(TenantKey(i), entries));
                if (pending.Count < 1024) continue;
                await Task.WhenAll(pending).ConfigureAwait(false);
                pending.Clear();
            }

            await Task.WhenAll(pending).ConfigureAwait(false);
        }
    }
}