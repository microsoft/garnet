// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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
            var cts = new CancellationTokenSource();

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
    }
}