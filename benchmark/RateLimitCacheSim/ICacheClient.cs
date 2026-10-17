// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace RateLimitCacheSim
{
    /// <summary>
    /// One connection to the cache, able to issue the modelled operation shapes either
    /// asynchronously or with a blocking call.
    /// </summary>
    public interface ICacheClient : IDisposable
    {
        /// <summary>Opens the connection and registers the scripts.</summary>
        /// <returns>A task that completes once the connection is usable.</returns>
        Task ConnectAsync();

        /// <summary>Issues one operation and awaits its reply.</summary>
        /// <param name="shape">Operation shape.</param>
        /// <param name="key">Bucket key.</param>
        /// <param name="field">Bucket field.</param>
        /// <returns>A task that completes when the reply has been parsed.</returns>
        Task ExecuteAsync(OpShape shape, string key, string field);

        /// <summary>Issues one operation with a blocking call.</summary>
        /// <param name="shape">Operation shape.</param>
        /// <param name="key">Bucket key.</param>
        /// <param name="field">Bucket field.</param>
        void Execute(OpShape shape, string key, string field);
    }
}