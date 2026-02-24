// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using StackExchange.Redis;

namespace VectorSearchBench;

/// <summary>
/// StackExchange.Redis-backed client for both Garnet and Redis endpoints.
/// </summary>
public sealed class RedisTargetClient : ITargetClient
{
    private readonly ConnectionMultiplexer _mux;
    private readonly IDatabase _db;

    public string Name { get; }

    private RedisTargetClient(string name, ConnectionMultiplexer mux)
    {
        Name = name;
        _mux = mux;
        _db = mux.GetDatabase();
    }

    public static async Task<RedisTargetClient> ConnectAsync(string name, string host, int port)
    {
        var cfg = new ConfigurationOptions
        {
            EndPoints = { { host, port } },
            ConnectTimeout = 5_000,
            SyncTimeout = 30_000,
            AsyncTimeout = 30_000,
            AbortOnConnectFail = false,
        };
        var mux = await ConnectionMultiplexer.ConnectAsync(cfg);
        return new RedisTargetClient(name, mux);
    }

    public Task<RedisResult> ExecuteAsync(string command, params object[] args)
    {
        var redisArgs = args.Select(a => a is byte[] ? a : (object)(a?.ToString() ?? "")).ToArray();
        return _db.ExecuteAsync(command, redisArgs);
    }

    public async Task PingAsync() => await _db.PingAsync();

    public async ValueTask DisposeAsync() => await _mux.CloseAsync();
}
