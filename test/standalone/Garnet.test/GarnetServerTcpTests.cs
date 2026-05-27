// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture, NonParallelizable]
    public class GarnetServerTcpTests : TestBase
    {
        private GarnetServer server;

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// Starts a full Garnet server on <see cref="TestUtils.TestPort"/>.
        /// Called explicitly by tests that need a live server; the listener-reuse tests below
        /// bind raw <see cref="GarnetServerTcp"/> instances to the same port and require it to be free.
        /// </summary>
        private void StartServer(bool enableAOF = false)
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: enableAOF);
            server.Start();
        }

        #region Listener reuse semantics

        static IPEndPoint GetEndPoint(bool ipv6)
        {
            if (OperatingSystem.IsWindows())
                Assert.Ignore("Tests Unix listener reuse semantics.");
            if (ipv6 && !Socket.OSSupportsIPv6)
                Assert.Ignore("IPv6 is unavailable.");

            return new IPEndPoint(ipv6 ? IPAddress.IPv6Loopback : IPAddress.Loopback, TestUtils.TestPort);
        }

        [TestCase(false)]
        [TestCase(true)]
        public void StartRejectsAnAlreadyListeningEndpoint(bool ipv6)
        {
            var endpoint = GetEndPoint(ipv6);
            using var first = new GarnetServerTcp(endpoint);
            using var second = new GarnetServerTcp(endpoint);
            first.Start();

            var exception = Assert.Throws<SocketException>(() => second.Start());
            Assert.That(exception.SocketErrorCode, Is.EqualTo(SocketError.AddressAlreadyInUse));
        }

        [TestCase(false)]
        [TestCase(true)]
        public void StartReusesEndpointAfterAnAcceptedConnectionCloses(bool ipv6)
        {
            var endpoint = GetEndPoint(ipv6);
            using (var listener = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp))
            {
                listener.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
                listener.Bind(endpoint);
                listener.Listen(1);
                using var client = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                client.ReceiveTimeout = 5000;
                client.Connect(endpoint);
                using var accepted = listener.Accept();
                accepted.ReceiveTimeout = 5000;

                // The server side closes first, leaving its accepted connection in TIME_WAIT.
                accepted.Shutdown(SocketShutdown.Send);
                Assert.That(client.Receive(new byte[1]), Is.Zero);
                client.Shutdown(SocketShutdown.Send);
                Assert.That(accepted.Receive(new byte[1]), Is.Zero);
            }

            using var server = new GarnetServerTcp(endpoint);
            Assert.DoesNotThrow(() => server.Start());
        }

        [TestCase(false)]
        [TestCase(true)]
        public void StartReusesEndpointAfterListenerCloses(bool ipv6)
        {
            var endpoint = GetEndPoint(ipv6);
            using var first = new GarnetServerTcp(endpoint);
            using var second = new GarnetServerTcp(endpoint);
            first.Start();
            first.Close();

            Assert.DoesNotThrow(() => second.Start());
        }

        #endregion

        #region Graceful shutdown

        [Test]
        public void StopListeningPreventsNewConnections()
        {
            StartServer();

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
            StartServer();

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
        public async Task StopListeningDuringActiveConnectionAttempts()
        {
            StartServer();

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
                        catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
                        {
                            break;
                        }
                        catch
                        {
                            // Connection failures are expected after StopListening
                        }
                    }
                }));
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
            StartServer();

            // Arrange - Write data and then close the connection
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                db.StringSet("shutdown-test", "data");
                ClassicAssert.AreEqual("data", (string)db.StringGet("shutdown-test"));
            }

            // Act - Graceful shutdown (no active connections)
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
            StartServer();

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
            StartServer();

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
            StartServer(enableAOF: true);

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

        #endregion
    }
}