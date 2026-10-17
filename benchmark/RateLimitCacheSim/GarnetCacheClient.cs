// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using System.Net.Security;
using Garnet.client;

namespace RateLimitCacheSim
{
    /// <summary>
    /// Drives the workload with Garnet's own async client. Used to separate the server's cost from
    /// the client library's cost: if SERedis and GarnetClient disagree, the gap is the client.
    /// </summary>
    public sealed class GarnetCacheClient : ICacheClient
    {
        readonly Options opts;
        readonly string bucketValue;
        readonly string ttlValue;
        readonly GarnetClient client;

        string readSha;
        string readWriteSha;

        /// <summary>
        /// Creates a client.
        /// </summary>
        /// <param name="opts">Run options.</param>
        /// <param name="bucketValue">Value written by the read-write script.</param>
        public GarnetCacheClient(Options opts, string bucketValue)
        {
            this.opts = opts;
            this.bucketValue = bucketValue;
            ttlValue = opts.Ttl.ToString();

            SslClientAuthenticationOptions sslOptions = null;
            if (opts.Tls)
            {
                sslOptions = new SslClientAuthenticationOptions
                {
                    TargetHost = opts.TlsHost,
                    AllowRenegotiation = false,
                    RemoteCertificateValidationCallback = static (_, _, _, _) => true,
                };
            }

            client = new GarnetClient(new IPEndPoint(IPAddress.Parse(opts.Host), opts.Port), sslOptions);
        }

        /// <inheritdoc/>
        public async Task ConnectAsync()
        {
            await client.ConnectAsync().ConfigureAwait(false);
            readSha = await client.ExecuteForStringResultAsync("SCRIPT", ["LOAD", RateLimitScripts.Read]).ConfigureAwait(false);
            readWriteSha = await client.ExecuteForStringResultAsync("SCRIPT", ["LOAD", RateLimitScripts.ReadWrite]).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public Task ExecuteAsync(OpShape shape, string key, string field) => shape switch
        {
            OpShape.PlainGet => client.StringGetAsync(key),
            OpShape.ScriptRead => opts.EvalFull
                ? client.ExecuteForStringResultAsync("EVAL", [RateLimitScripts.Read, "1", key, field])
                : client.ExecuteForStringResultAsync("EVALSHA", [readSha, "1", key, field]),
            OpShape.ScriptReadWrite => opts.EvalFull
                ? client.ExecuteForStringResultAsync("EVAL", [RateLimitScripts.ReadWrite, "1", key, field, bucketValue, ttlValue])
                : client.ExecuteForStringResultAsync("EVALSHA", [readWriteSha, "1", key, field, bucketValue, ttlValue]),
            _ => throw new ArgumentOutOfRangeException(nameof(shape)),
        };

        /// <inheritdoc/>
        public void Execute(OpShape shape, string key, string field)
            => ExecuteAsync(shape, key, field).GetAwaiter().GetResult();

        /// <inheritdoc/>
        public void Dispose() => client?.Dispose();
    }
}