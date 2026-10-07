// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers what <c>GarnetClient</c> owes its callers when the connection or the client goes away with
    /// requests still outstanding: every caller is completed, each exactly once, and nothing waits forever.
    ///
    /// A request that is never completed does not fail a test, it wedges the host until the hang detector
    /// kills the run, so each test here is bounded and reports how many requests were left pending.
    /// </summary>
    /// <remarks>
    /// These run against a listener that accepts and then stays silent rather than against a Garnet server.
    /// That is what makes them deterministic: no reply can arrive, so every completion comes from teardown,
    /// which is the path under test. It is also the shape of the failure that motivated it -- a server that
    /// goes mute with a request in flight.
    /// </remarks>
    [TestFixture]
    public class GarnetClientDisposalTests : TestBase
    {
        const int RequestCount = 32;

        /// <summary>
        /// Ceiling on how long a correctly behaving client may take to complete its callers. Generous,
        /// because exceeding it fails the test rather than retrying; the bug it guards against exceeds any
        /// bound.
        /// </summary>
        static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        [Test]
        public void DisposeWithRequestsInFlightCompletesEveryCaller()
        {
            using var silent = new SilentServer();
            var client = TestUtils.GetGarnetClient(silent.EndPoint);
            client.Connect();

            var issued = new List<Task>();
            try
            {
                for (var i = 0; i < RequestCount; i++)
                {
                    issued.Add(client.PingAsync());
                    // Covers the Task<long> path as well, which teardown did not complete at all.
                    issued.Add(client.ExecuteForLongResultAsync("INCR", [$"key{i}"]));
                }
            }
            catch (Exception)
            {
                // A request refused outright is a completed outcome, not a pending one.
            }

            client.Dispose();

            ClassicAssert.IsTrue(Task.WhenAll(issued.Select(Settled)).Wait(Bound),
                $"{issued.Count(t => !t.IsCompleted)} of {issued.Count} requests were still pending after the " +
                "client was disposed. A caller abandoned like this waits forever and wedges the test host.");
        }

        [Test]
        public void ConcurrentTeardownCompletesEachRequestExactlyOnce()
        {
            using var silent = new SilentServer();
            var client = TestUtils.GetGarnetClient(silent.EndPoint);
            client.Connect();

            var invocations = 0;
            var issuedCount = 0;
            try
            {
                for (var i = 0; i < RequestCount; i++)
                {
                    client.Ping((_, _) => Interlocked.Increment(ref invocations));
                    issuedCount++;
                }
            }
            catch (Exception)
            {
            }

            // Drops the connection and disposes the client at the same time. Connection teardown and client
            // disposal drain independently, and neither waits for the other, so this is the window in which
            // one request could be completed by both.
            using var bothStarted = new Barrier(2);
            var dropper = Task.Run(() =>
            {
                bothStarted.SignalAndWait();
                silent.DropConnections();
            });

            bothStarted.SignalAndWait();
            client.Dispose();
            ClassicAssert.IsTrue(dropper.Wait(Bound), "Dropping the connection did not finish.");

            ClassicAssert.IsTrue(SpinWait.SpinUntil(() => Volatile.Read(ref invocations) >= issuedCount, Bound),
                $"Only {Volatile.Read(ref invocations)} of {issuedCount} callbacks ran, so at least one request " +
                "was left outstanding by a concurrent teardown.");

            // Settle, then confirm nothing was completed a second time.
            Thread.Sleep(500);
            ClassicAssert.AreEqual(issuedCount, Volatile.Read(ref invocations),
                "A request was completed more than once, so two teardown paths retired the same slot.");
        }

        [Test]
        public void ThrowingCallbackDoesNotStrandOtherRequests()
        {
            using var silent = new SilentServer();
            var client = TestUtils.GetGarnetClient(silent.EndPoint);
            client.Connect();

            var awaited = new List<Task>();
            try
            {
                for (var i = 0; i < RequestCount; i++)
                {
                    // Every callback here runs from teardown, because the server never replies. One that
                    // throws must not abandon the drain and strand the requests queued behind it.
                    client.Ping((_, _) => throw new InvalidOperationException("callback failure"));
                    awaited.Add(client.PingAsync());
                }
            }
            catch (Exception)
            {
            }

            client.Dispose();

            ClassicAssert.IsTrue(Task.WhenAll(awaited.Select(Settled)).Wait(Bound),
                $"{awaited.Count(t => !t.IsCompleted)} of {awaited.Count} requests were left unresolved after a " +
                "callback threw during disposal.");
            ClassicAssert.IsTrue(client.Disposed, "Disposal did not finish after a callback threw.");
        }

        /// <summary>
        /// Awaits a task for its completion only. Being faulted by disposal is the expected outcome here;
        /// the property under test is that the task completes at all.
        /// </summary>
        static async Task Settled(Task task)
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
        }

        /// <summary>
        /// Accepts connections and then says nothing, so a request can never be answered and every completion
        /// has to come from teardown.
        /// </summary>
        private sealed class SilentServer : IDisposable
        {
            readonly TcpListener listener;
            readonly List<Socket> accepted = [];
            volatile bool stopped;

            public SilentServer()
            {
                // Port 0: the OS hands out a port it is holding for us, so this needs no coordination with
                // the test port allocator, which exists to place Garnet servers.
                listener = new TcpListener(IPAddress.Loopback, 0);
                listener.Start();
                _ = Task.Run(AcceptLoopAsync);
            }

            public IPEndPoint EndPoint => (IPEndPoint)listener.LocalEndpoint;

            async Task AcceptLoopAsync()
            {
                while (!stopped)
                {
                    Socket socket;
                    try
                    {
                        socket = await listener.AcceptSocketAsync().ConfigureAwait(false);
                    }
                    catch (Exception)
                    {
                        return;
                    }

                    lock (accepted)
                        accepted.Add(socket);
                }
            }

            /// <summary>Drops every accepted connection, so the client's handler tears down.</summary>
            public void DropConnections()
            {
                lock (accepted)
                {
                    foreach (var socket in accepted)
                    {
                        try
                        {
                            socket.Close();
                        }
                        catch (Exception)
                        {
                        }
                    }

                    accepted.Clear();
                }
            }

            public void Dispose()
            {
                stopped = true;
                DropConnections();
                try
                {
                    listener.Stop();
                }
                catch (Exception)
                {
                }
            }
        }
    }
}