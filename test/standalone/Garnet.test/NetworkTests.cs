// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [TestFixture]
    public class NetworkTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: false, enableTLS: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        public void NetworkExceptions([ValuesPrefix("Network")] ExceptionInjectionType exception)
        {
            ExceptionInjectionHelper.EnableException(exception);
            try
            {
                for (int i = 0; i < 3; i++)
                {
                    using var db1 = TestUtils.GetGarnetClient(useTLS: true);
                    try
                    {
                        db1.Connect();
                    }
                    catch
                    {
                        // Ignore connection exceptions
                    }

                    // Wait for connection to fail due to server-side exception
                    while (db1.IsConnected)
                    {
                        Thread.Sleep(100);
                    }
                    ClassicAssert.IsFalse(db1.IsConnected);
                }
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(exception);
            }

            // Finally, we should connect successfully
            using var db2 = TestUtils.GetGarnetClient(useTLS: true);
            db2.Connect();

            string origValue = "abcdefg";
            ManualResetEventSlim e = new();
            db2.StringSet("mykey", origValue, (c, retValue) =>
            {
                ClassicAssert.AreEqual("OK", retValue);
                e.Set();
            });

            e.Wait();
            e.Reset();

            db2.StringGet("mykey", (c, retValue) =>
            {
                ClassicAssert.AreEqual(origValue, retValue);
                e.Set();
            });
            e.Wait();
        }

        /// <summary>
        /// Verifies that when a TLS client abruptly disconnects, the server properly
        /// cleans up the handler and removes it from activeHandlers.
        /// </summary>
        [Test]
        public void TlsClientDisconnectCleansUpHandler()
        {
            var garnetServerTcp = (GarnetServerBase)server.Provider.StoreWrapper.Servers[0];
            var disposedBefore = garnetServerTcp.TotalConnectionsDisposed;

            // Connect multiple clients and verify they register as active handlers
            const int clientCount = 5;
            var clients = new Garnet.client.GarnetClient[clientCount];
            try
            {
                for (int i = 0; i < clientCount; i++)
                {
                    clients[i] = TestUtils.GetGarnetClient(useTLS: true);
                    clients[i].Connect();

                    // Verify the connection works
                    ManualResetEventSlim done = new();
                    string result = null;
                    clients[i].StringSet($"key{i}", $"value{i}", (c, r) => { result = r; done.Set(); });
                    ClassicAssert.IsTrue(done.Wait(5000), $"Timed out waiting for StringSet callback on client {i}");
                    ClassicAssert.AreEqual("OK", result);
                }

                // Wait for all connections to be registered
                var deadline = System.Environment.TickCount64 + 5000;
                while (garnetServerTcp.get_conn_active() < clientCount && System.Environment.TickCount64 < deadline)
                    Thread.Sleep(250);
                ClassicAssert.GreaterOrEqual(garnetServerTcp.get_conn_active(), clientCount,
                    "Expected all clients to be registered as active handlers");
            }
            finally
            {
                // Abruptly dispose all clients (sends FIN, simulating remote peer disconnect)
                for (int i = 0; i < clientCount; i++)
                {
                    clients[i]?.Dispose();
                }
            }

            // Wait for the server to detect the disconnections and clean up handlers
            var deadline2 = System.Environment.TickCount64 + 10000;
            while (garnetServerTcp.get_conn_active() > 0 && System.Environment.TickCount64 < deadline2)
                Thread.Sleep(250);

            ClassicAssert.AreEqual(0, garnetServerTcp.get_conn_active(),
                "Server still has active handlers after all clients disconnected — handlers were not properly cleaned up (CLOSE-WAIT leak)");
            ClassicAssert.GreaterOrEqual(garnetServerTcp.TotalConnectionsDisposed - disposedBefore, clientCount,
                "Expected TotalConnectionsDisposed to increment by at least the number of disconnected clients");

            // Verify the server still accepts new connections after cleanup
            using var db = TestUtils.GetGarnetClient(useTLS: true);
            db.Connect();
            ManualResetEventSlim e = new();
            string val = null;
            db.StringSet("after_cleanup", "works", (c, r) => { val = r; e.Set(); });
            ClassicAssert.IsTrue(e.Wait(5000), "Timed out waiting for post-cleanup StringSet callback");
            ClassicAssert.AreEqual("OK", val);
        }

        /// <summary>
        /// Verifies that Dispose() properly calls DisposeImpl() to remove the handler from
        /// activeHandlers when no SAEA receive loop is running. This directly tests the
        /// CLOSE-WAIT fix: the exception fires after the handler is registered but before
        /// Start() is called, so there is no SAEA backup cleanup path. Without the fix,
        /// public Dispose() does not call DisposeImpl() and the handler leaks permanently,
        /// which also causes DisposeActiveHandlers() to spin forever during server shutdown.
        ///
        /// This test uses its own server instance (not the shared one from SetUp/TearDown)
        /// because the bug causes server.Dispose() to hang forever on leaked handlers.
        /// </summary>
        [Test]
        public void DisposeCallsDisposeImplWithoutSaeaBackup()
        {
            // Use a separate server on a different port so we don't interfere
            // with the shared server from SetUp, and so TearDown doesn't hang.
            var testDir = TestUtils.MethodTestDir + "_injection";
            TestUtils.DeleteDirectory(testDir, wait: true);
            var endpoint = new IPEndPoint(IPAddress.Loopback, TestUtils.TestPort + 1000);
            var testServer = TestUtils.CreateGarnetServer(testDir, enableTLS: true,
                endpoints: [endpoint]);
            testServer.Start();

            try
            {
                var garnetServerTcp = (GarnetServerBase)testServer.Provider.StoreWrapper.Servers[0];

                // Verify no active connections initially
                ClassicAssert.AreEqual(0, garnetServerTcp.get_conn_active());

                ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Dispose_After_Handler_Registered_Before_Start);
                try
                {
                    // Use raw TCP sockets to trigger the server's accept loop.
                    // We can't use GarnetClient here because TLS auth would hang —
                    // the exception fires before Start(), so the server never begins
                    // TLS negotiation, and the client would wait forever.
                    const int injectionCount = 3;
                    var receivedBefore = garnetServerTcp.TotalConnectionsReceived;

                    for (int i = 0; i < injectionCount; i++)
                    {
                        using var rawSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                        try
                        {
                            rawSocket.Connect(endpoint);
                        }
                        catch
                        {
                            // Connection may fail if server closed socket fast enough
                        }

                        // Wait for the server to receive and process this connection
                        var connDeadline = System.Environment.TickCount64 + 5000;
                        while (garnetServerTcp.TotalConnectionsReceived < receivedBefore + i + 1
                               && System.Environment.TickCount64 < connDeadline)
                            Thread.Sleep(250);
                    }
                }
                finally
                {
                    ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Dispose_After_Handler_Registered_Before_Start);
                }

                // Wait for all injected connections to be fully disposed
                var disposeDeadline = System.Environment.TickCount64 + 5000;
                while (garnetServerTcp.get_conn_active() > 0 && System.Environment.TickCount64 < disposeDeadline)
                    Thread.Sleep(250);

                long activeCount = garnetServerTcp.get_conn_active();

                if (activeCount > 0)
                {
                    // Bug confirmed: handlers leaked. Attempt best-effort cleanup
                    // on a background thread with a timeout so we don't leave a live
                    // listener running for the rest of the test suite.
                    var disposeTask = Task.Run(() =>
                    {
                        try { testServer.Dispose(); }
                        catch { /* best effort */ }
                    });
                    disposeTask.Wait(TimeSpan.FromSeconds(5));

                    ClassicAssert.Fail(
                        $"Leaked {activeCount} handler(s): Dispose() did not call DisposeImpl() to remove handler from activeHandlers.");
                }

                // If we get here, the fix is working — handlers were cleaned up properly.
                // Verify the server still accepts connections after cleanup.
                using var db = TestUtils.GetGarnetClient(endpoint, useTLS: true);
                db.Connect();
                ManualResetEventSlim e = new();
                string val = null;
                db.StringSet("after_injection", "works", (c, r) => { val = r; e.Set(); });
                ClassicAssert.IsTrue(e.Wait(5000), "Timed out waiting for post-injection StringSet callback");
                ClassicAssert.AreEqual("OK", val);

                // Safe to dispose — no leaked handlers
                testServer.Dispose();
            }
            catch
            {
                // Best-effort cleanup on failure — attempt disposal with a timeout
                // to avoid leaving a live listener for the rest of the test run.
                var disposeTask = Task.Run(() =>
                {
                    try { testServer.Dispose(); }
                    catch { /* best effort */ }
                });
                disposeTask.Wait(TimeSpan.FromSeconds(5));
                throw;
            }
            finally
            {
                TestUtils.DeleteDirectory(testDir, wait: true);
            }
        }
    }
}
#endif