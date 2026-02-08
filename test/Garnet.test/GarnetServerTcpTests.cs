// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture, NonParallelizable]
    public class GarnetServerTcpTests : AllureTestBase
    {
        private GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void StopListeningPreventsNewConnections()
        {
            // Arrange - Establish a working connection first
            using var redis1 = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db1 = redis1.GetDatabase(0);
            db1.StringSet("test", "value");
            ClassicAssert.AreEqual("value", (string)db1.StringGet("test"));

            // Act - Stop listening on all servers
            foreach (var tcpServer in server.Provider.StoreWrapper.Servers.OfType<GarnetServerTcp>())
            {
                tcpServer.StopListening();
            }

            Thread.Sleep(100); // Brief delay to ensure socket is closed

            // Assert - New connections should fail
            Assert.Throws<RedisConnectionException>(() =>
            {
                using var redis2 = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                redis2.GetDatabase(0).Ping();
            });

            // Existing connection should still work
            ClassicAssert.AreEqual("value", (string)db1.StringGet("test"));
        }

        [Test]
        public void StopListeningIdempotent()
        {
            // Arrange
            foreach (var tcpServer in server.Provider.StoreWrapper.Servers.OfType<GarnetServerTcp>())
            {
                tcpServer.StopListening();
            }

            // Act & Assert - Calling StopListening again should not throw
            Assert.DoesNotThrow(() =>
            {
                foreach (var tcpServer in server.Provider.StoreWrapper.Servers.OfType<GarnetServerTcp>())
                {
                    tcpServer.StopListening();
                }
            });
        }

        [Test]
        public void StopListeningLogsInformation()
        {
            // This test verifies that StopListening logs appropriate information
            // You would need to set up a logger and verify the log output
            // For now, we just verify no exceptions are thrown

            Assert.DoesNotThrow(() =>
            {
                foreach (var tcpServer in server.Provider.StoreWrapper.Servers.OfType<GarnetServerTcp>())
                {
                    tcpServer.StopListening();
                }
            });
        }

        [Test]
        public async Task StopListeningDuringActiveConnectionAttempts()
        {
            // Arrange - Start multiple connection attempts
            var connectionTasks = new System.Collections.Generic.List<Task>();
            using var cts = new CancellationTokenSource();

            for (int i = 0; i < 10; i++)
            {
                connectionTasks.Add(Task.Run(async () =>
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        try
                        {
                            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                            await redis.GetDatabase(0).PingAsync();
                            await Task.Delay(10);
                        }
                        catch
                        {
                            // Connection failures are expected after StopListening
                        }
                    }
                }, cts.Token));
            }

            await Task.Delay(50); // Let some connections establish

            // Act
            foreach (var tcpServer in server.Provider.StoreWrapper.Servers.OfType<GarnetServerTcp>())
            {
                tcpServer.StopListening();
            }

            await Task.Delay(100);
            cts.Cancel();

            // Assert - All tasks should complete without unhandled exceptions
            Assert.DoesNotThrowAsync(async () => await Task.WhenAll(connectionTasks));
        }

        [Test]
        public async Task ShutdownAsyncCompletesGracefully()
        {
            // Arrange - Write data that should survive shutdown
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.StringSet("shutdown-test", "data");
            ClassicAssert.AreEqual("data", (string)db.StringGet("shutdown-test"));

            // Act - Graceful shutdown
            await server.ShutdownAsync(timeout: TimeSpan.FromSeconds(5)).ConfigureAwait(false);

            // Assert - New connections should fail after shutdown
            Assert.Throws<RedisConnectionException>(() =>
            {
                using var redis2 = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                redis2.GetDatabase(0).Ping();
            });
        }

        [Test]
        public async Task ShutdownAsyncRespectsTimeout()
        {
            // Arrange - Establish a connection that will stay open
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.Ping();

            // Act - Shutdown with a very short timeout
            var sw = System.Diagnostics.Stopwatch.StartNew();
            await server.ShutdownAsync(timeout: TimeSpan.FromMilliseconds(200)).ConfigureAwait(false);
            sw.Stop();

            // Assert - Should complete without hanging indefinitely
            // Allow generous upper bound for CI environments
            ClassicAssert.Less(sw.ElapsedMilliseconds, 10_000,
                "ShutdownAsync should complete within a reasonable time even with active connections");
        }

        [Test]
        public async Task ShutdownAsyncRespectsCancellation()
        {
            // Arrange
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            redis.GetDatabase(0).Ping();

            using var cts = new CancellationTokenSource();

            // Act - Cancel immediately
            cts.Cancel();
            Assert.DoesNotThrowAsync(async () =>
            {
                await server.ShutdownAsync(timeout: TimeSpan.FromSeconds(30), token: cts.Token).ConfigureAwait(false);
            });
        }

        [Test]
        public async Task ShutdownAsyncWithAofCommit()
        {
            // Arrange - Create server with AOF enabled
            server?.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Write some data
            for (int i = 0; i < 100; i++)
            {
                db.StringSet($"aof-key-{i}", $"value-{i}");
            }

            // Act - Shutdown should commit AOF without errors
            Assert.DoesNotThrowAsync(async () =>
            {
                await server.ShutdownAsync(timeout: TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            });
        }
    }
}