// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;

namespace Garnet.test.sentinel.Fixtures
{
    /// <summary>
    /// Spawns a stock redis-server subprocess (a binary the developer builds once, e.g.
    /// into ~/.cache/redis-bin/7.4.11/redis-server). Discovers the binary via the
    /// GARNET_TEST_REDIS_SERVER env var, falling back to the conventional cache path
    /// under the current user's home directory.
    ///
    /// Ready line is "Ready to accept connections", which redis-server emits on stdout.
    /// </summary>
    internal sealed class RedisServerProcess : ProcessWrapper
    {
        /// <summary>
        /// Conventional binary location, resolved against the current user's home
        /// directory so the tests are not tied to one developer's machine. Override with
        /// the GARNET_TEST_REDIS_SERVER env var when Redis lives elsewhere.
        /// </summary>
        public static string DefaultCacheDir =>
            Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".cache", "redis-bin", "7.4.11");

        private readonly string _configPath;
        private readonly bool _asReplica;
        private readonly string? _primaryHost;
        private readonly int? _primaryPort;

        public RedisServerProcess(
            int port,
            string? workingDir = null,
            bool asReplica = false,
            string? primaryHost = null,
            int? primaryPort = null)
            : base(port)
        {
            // Persist a minimal config file in a per-process temp dir so that
            // we don't need to know the test-runner's CWD.
            var dir = workingDir ?? Path.Combine(Path.GetTempPath(), $"garnet-test-redis-{port}-{Guid.NewGuid():N}");
            Directory.CreateDirectory(dir);
            _configPath = Path.Combine(dir, "redis.conf");
            _asReplica = asReplica;
            _primaryHost = primaryHost;
            _primaryPort = primaryPort;
            WriteConfig();
        }

        private void WriteConfig()
        {
            // Minimal config: bind loopback, no daemon, no RDB/AOF for test isolation,
            // a writable tmpdir per process, and an explicit logfile so we can
            // inspect failures.
            var tmp = Path.Combine(Path.GetTempPath(), $"garnet-test-redis-data-{Port}-{Guid.NewGuid():N}");
            Directory.CreateDirectory(tmp);

            using var w = new StreamWriter(_configPath);
            w.WriteLine($"port {Port}");
            w.WriteLine("bind 127.0.0.1");
            w.WriteLine("protected-mode no");
            w.WriteLine("daemonize no");
            w.WriteLine("save \"\"");
            w.WriteLine("appendonly no");
            w.WriteLine("dir \"" + tmp.Replace("\"", "\\\"") + "\"");
            w.WriteLine("logfile \"\"");          // empty -> log to stdout (which we capture)
            w.WriteLine("loglevel notice");
            w.WriteLine("repl-diskless-sync yes");
            w.WriteLine("repl-backlog-size 1mb");

            if (_asReplica && _primaryHost != null && _primaryPort != null)
            {
                w.WriteLine($"replicaof {_primaryHost} {_primaryPort.Value}");
            }
        }

        public string ConfigPath => _configPath;

        protected override string ExecutablePath =>
            Environment.GetEnvironmentVariable("GARNET_TEST_REDIS_SERVER")
            ?? Path.Combine(DefaultCacheDir, "redis-server");

        protected override IReadOnlyList<string> BuildArguments() =>
            [_configPath];

        protected override string ReadyLineMarker => "Ready to accept connections";

        // redis-server handles SIGTERM cleanly; we don't send a RESP command.
        protected override string? PoliteShutdownCommand => null;
    }
}
