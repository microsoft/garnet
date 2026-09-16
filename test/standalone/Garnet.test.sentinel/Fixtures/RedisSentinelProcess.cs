// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;

namespace Garnet.test.sentinel.Fixtures
{
    /// <summary>
    /// Spawns a stock redis-sentinel subprocess (same binary as redis-server, run in
    /// sentinel mode) configured to monitor a given primary.
    /// </summary>
    internal sealed class RedisSentinelProcess : ProcessWrapper
    {
        public const string DefaultCacheDir = "/home/skyline/.cache/redis-bin/7.4.11";

        private readonly string _sentinelConfigPath;
        private readonly string _primaryName;
        private readonly string _primaryHost;
        private readonly int _primaryPort;
        private readonly int _quorum;

        public RedisSentinelProcess(
            int port,
            string primaryName,
            string primaryHost,
            int primaryPort,
            int quorum = 2,
            int downAfterMilliseconds = 5000)
            : base(port)
        {
            _primaryName = primaryName;
            _primaryHost = primaryHost;
            _primaryPort = primaryPort;
            _quorum = quorum;

            var dir = Path.Combine(Path.GetTempPath(), $"garnet-test-sentinel-{port}-{Guid.NewGuid():N}");
            Directory.CreateDirectory(dir);
            _sentinelConfigPath = Path.Combine(dir, "sentinel.conf");

            using var w = new StreamWriter(_sentinelConfigPath);
            w.WriteLine($"port {Port}");
            w.WriteLine("bind 127.0.0.1");
            w.WriteLine("daemonize no");
            w.WriteLine("protected-mode no");
            w.WriteLine("logfile \"\"");
            w.WriteLine("loglevel notice");
            w.WriteLine($"sentinel monitor {_primaryName} {_primaryHost} {_primaryPort} {_quorum}");
            w.WriteLine($"sentinel down-after-milliseconds {_primaryName} {downAfterMilliseconds}");
            w.WriteLine($"sentinel failover-timeout {_primaryName} {downAfterMilliseconds * 2}");
            w.WriteLine($"sentinel parallel-syncs {_primaryName} 1");
        }

        public string ConfigPath => _sentinelConfigPath;

        protected override string ExecutablePath =>
            Environment.GetEnvironmentVariable("GARNET_TEST_REDIS_SENTINEL")
            ?? Path.Combine(DefaultCacheDir, "redis-sentinel");

        protected override IReadOnlyList<string> BuildArguments() =>
            [_sentinelConfigPath, "--sentinel"];

        // Sentinel also prints "Ready to accept connections" once it's listening.
        protected override string ReadyLineMarker => "Ready to accept connections";

        protected override string? PoliteShutdownCommand => null;
    }
}
