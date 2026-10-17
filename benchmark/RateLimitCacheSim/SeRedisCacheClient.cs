// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using StackExchange.Redis;

namespace RateLimitCacheSim
{
    /// <summary>
    /// Drives the workload with StackExchange.Redis, the client a .NET service would use.
    /// Each instance owns its own multiplexer so that "one connection per mock service instance" holds.
    /// </summary>
    public sealed class SeRedisCacheClient : ICacheClient
    {
        readonly Options opts;
        readonly RedisValue bucketValue;
        readonly RedisValue ttlValue;

        ConnectionMultiplexer mux;
        IDatabase db;
        byte[] readSha;
        byte[] readWriteSha;

        /// <summary>
        /// Creates a client.
        /// </summary>
        /// <param name="opts">Run options.</param>
        /// <param name="bucketValue">Value written by the read-write script.</param>
        public SeRedisCacheClient(Options opts, string bucketValue)
        {
            this.opts = opts;
            this.bucketValue = bucketValue;
            ttlValue = opts.Ttl;
        }

        /// <inheritdoc/>
        public async Task ConnectAsync()
        {
            var config = SeRedisConfig.Build(opts, 60000);

            mux = await ConnectionMultiplexer.ConnectAsync(config).ConfigureAwait(false);
            db = mux.GetDatabase();

            var server = mux.GetServer(mux.GetEndPoints()[0]);
            readSha = (byte[])await server.ScriptLoadAsync(RateLimitScripts.Read).ConfigureAwait(false);
            readWriteSha = (byte[])await server.ScriptLoadAsync(RateLimitScripts.ReadWrite).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public Task ExecuteAsync(OpShape shape, string key, string field)
        {
            switch (shape)
            {
                case OpShape.PlainGet:
                    return db.StringGetAsync(key);

                case OpShape.ScriptRead:
                    return opts.EvalFull
                        ? db.ScriptEvaluateAsync(RateLimitScripts.Read, [key], [field])
                        : db.ScriptEvaluateAsync(readSha, [key], [field]);

                case OpShape.ScriptReadWrite:
                    return opts.EvalFull
                        ? db.ScriptEvaluateAsync(RateLimitScripts.ReadWrite, [key], [field, bucketValue, ttlValue])
                        : db.ScriptEvaluateAsync(readWriteSha, [key], [field, bucketValue, ttlValue]);

                default:
                    throw new ArgumentOutOfRangeException(nameof(shape));
            }
        }

        /// <inheritdoc/>
        public void Execute(OpShape shape, string key, string field)
        {
            switch (shape)
            {
                case OpShape.PlainGet:
                    _ = db.StringGet(key);
                    break;

                case OpShape.ScriptRead:
                    _ = opts.EvalFull
                        ? db.ScriptEvaluate(RateLimitScripts.Read, [key], [field])
                        : db.ScriptEvaluate(readSha, [key], [field]);
                    break;

                case OpShape.ScriptReadWrite:
                    _ = opts.EvalFull
                        ? db.ScriptEvaluate(RateLimitScripts.ReadWrite, [key], [field, bucketValue, ttlValue])
                        : db.ScriptEvaluate(readWriteSha, [key], [field, bucketValue, ttlValue]);
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(shape));
            }
        }

        /// <inheritdoc/>
        public void Dispose() => mux?.Dispose();
    }
}