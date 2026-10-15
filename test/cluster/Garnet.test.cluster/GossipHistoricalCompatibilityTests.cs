// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Garnet.common;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable, Explicit("Requires GARNET_LEGACY_SERVER to identify a historical server executable or assembly.")]
    internal class GossipHistoricalCompatibilityTests : TestBase
    {
        private ClusterTestContext context;
        private Process legacy;
        private Task<string> output;
        private Task<string> errors;
        private ConnectionMultiplexer client;
        private const string Key = "{gossip-negotiation}key";
        private IPEndPoint LegacyEndpoint => new(IPAddress.Loopback, ClusterTestContext.Port + 1);

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([], testTimeoutSeconds: 120);
            context.CreateInstances(1, enableAOF: true, clusterReplicationReestablishmentTimeout: 1);
        }

        [TearDown]
        public void TearDown()
        {
            client?.Dispose();
            if (legacy != null)
            {
                if (!legacy.HasExited)
                    legacy.Kill(entireProcessTree: true);
                Assert.That(legacy.WaitForExit(10000), Is.True);
                TestContext.Progress.WriteLine(output.GetAwaiter().GetResult());
                TestContext.Progress.WriteLine(errors.GetAwaiter().GetResult());
                legacy.Dispose();
            }
            context.TearDown();
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task HistoricalPeerMaintainsMembershipReplicationAndFailover(bool legacyInitiatesMeet)
        {
            await StartLegacyAsync();
            client = await ConnectionMultiplexer.ConnectAsync(TestUtils.GetConfig(
                [context.endpoints[0].ToIPEndPoint(), LegacyEndpoint], allowAdmin: true));
            string modernId = Execute(false, "CLUSTER", "MYID").ToString();
            string legacyId = Execute(true, "CLUSTER", "MYID").ToString();
            Assert.That(Execute(false, "CLUSTER", "SET-CONFIG-EPOCH", 1).ToString(), Is.EqualTo("OK"));
            Assert.That(Execute(true, "CLUSTER", "SET-CONFIG-EPOCH", 2).ToString(), Is.EqualTo("OK"));
            IPEndPoint target = legacyInitiatesMeet ? context.endpoints[0].ToIPEndPoint() : LegacyEndpoint;
            Assert.That(Execute(legacyInitiatesMeet, "CLUSTER", "MEET", target.Address.ToString(), target.Port).ToString(), Is.EqualTo("OK"));
            await WaitUntilAsync(() => client.GetServer(LegacyEndpoint).ClusterNodes().Nodes.Count() == 2
                && client.GetServer(context.endpoints[0].ToIPEndPoint()).ClusterNodes().Nodes.Count() == 2);

            int slot = HashSlotUtils.HashSlot(Encoding.ASCII.GetBytes(Key));
            Assert.That(Execute(true, "CLUSTER", "ADDSLOTS", slot).ToString(), Is.EqualTo("OK"));
            await WaitUntilAsync(() => client.GetServer(context.endpoints[0].ToIPEndPoint()).ClusterNodes().GetBySlot(slot)?.NodeId == legacyId);
            Assert.That(Execute(true, "SET", Key, "before").ToString(), Is.EqualTo("OK"));
            Assert.That(Execute(false, "CLUSTER", "REPLICATE", legacyId).ToString(), Is.EqualTo("OK"));
            await WaitUntilAsync(() => ((RedisResult[])Execute(false, "ROLE"))[0].ToString() == "slave"
                && Execute(false, "INFO", "REPLICATION").ToString().Contains("master_sync_in_progress:False", StringComparison.Ordinal));
            Assert.That(Execute(false, "READONLY").ToString(), Is.EqualTo("OK"));
            await WaitUntilAsync(() => Execute(false, "GET", Key).ToString() == "before");

            Assert.That(Execute(false, "CLUSTER", "FAILOVER").ToString(), Is.EqualTo("OK"));
            await WaitUntilAsync(() => Execute(false, "INFO", "REPLICATION").ToString()
                .Contains("last_failover_state:failover-completed", StringComparison.Ordinal));
            await WaitUntilAsync(() => ((RedisResult[])Execute(true, "ROLE"))[0].ToString() == "slave");
            await WaitUntilAsync(() => client.GetServer(LegacyEndpoint).ClusterNodes().GetBySlot(slot)?.NodeId == modernId);
            Assert.That(Execute(false, "GET", Key).ToString(), Is.EqualTo("before"));
            Assert.That(Execute(false, "SET", Key, "after").ToString(), Is.EqualTo("OK"));
            Assert.That(Execute(true, "READONLY").ToString(), Is.EqualTo("OK"));
            await WaitUntilAsync(() => Execute(true, "GET", Key).ToString() == "after");
            Assert.That(Execute(false, "CLUSTER", "MYID").ToString(), Is.EqualTo(modernId));
            Assert.That(Execute(true, "CLUSTER", "MYID").ToString(), Is.EqualTo(legacyId));
        }

        private async Task StartLegacyAsync()
        {
            string executable = Environment.GetEnvironmentVariable("GARNET_LEGACY_SERVER");
            Assert.That(File.Exists(executable), Is.True, "GARNET_LEGACY_SERVER must identify a historical server executable or assembly.");
            bool isAssembly = Path.GetExtension(executable).Equals(".dll", StringComparison.OrdinalIgnoreCase);
            ProcessStartInfo start = new(isAssembly ? "dotnet" : executable)
            {
                WorkingDirectory = Path.GetDirectoryName(executable),
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            };
            if (isAssembly)
                start.ArgumentList.Add(executable);
            string directory = Path.Combine(context.TestFolder, "legacy");
            Directory.CreateDirectory(directory);
            string[] arguments = ["--bind", "127.0.0.1", "--port", LegacyEndpoint.Port.ToString(),
                "--cluster", "--aof", "--storage-tier", "--memory", "64m", "--page", "32k", "--index", "1m",
                "--aof-memory", "4m", "--aof-page-size", "64k", "--logdir", directory, "--checkpointdir", directory,
                "--logger-level", "Error", "--no-pubsub"];
            foreach (string argument in arguments)
                start.ArgumentList.Add(argument);
            legacy = new() { StartInfo = start };
            Assert.That(legacy.Start(), Is.True);
            output = legacy.StandardOutput.ReadToEndAsync();
            errors = legacy.StandardError.ReadToEndAsync();
            await WaitUntilAsync(() =>
            {
                Assert.That(legacy.HasExited, Is.False, "Historical server exited before accepting connections.");
                using Socket socket = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                try
                {
                    socket.Connect(LegacyEndpoint);
                    return true;
                }
                catch (SocketException exception) when (exception.SocketErrorCode == SocketError.ConnectionRefused)
                {
                    return false;
                }
            });
        }

        private async Task WaitUntilAsync(Func<bool> predicate)
        {
            while (!predicate())
                await Task.Delay(20, context.cts.Token);
        }

        private RedisResult Execute(bool onLegacy, string command, params object[] arguments)
            => client.GetServer(onLegacy ? LegacyEndpoint : context.endpoints[0].ToIPEndPoint())
                .Execute(0, command, arguments, CommandFlags.NoRedirect);
    }
}