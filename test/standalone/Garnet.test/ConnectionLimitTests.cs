// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers connection admission control: what a refused client is told, the rejected_connections
    /// metric that makes a refusal observable, the process-wide ceiling shared across listeners,
    /// and CONFIG SET maxclients.
    ///
    /// The assertions read the bytes the server sends rather than merely observing that connecting
    /// failed. A test that only asserted "connecting fails" would pass against the silent drop this
    /// replaces, which is the defect these tests exist to catch.
    /// </summary>
    [TestFixture]
    public class ConnectionLimitTests : TestBase
    {
        /// <summary>
        /// Small enough to fill quickly. One slot is held by the probe connection the INFO and
        /// CONFIG assertions run over, so tests fill <see cref="LimitLessProbe"/> further sockets
        /// to reach the ceiling.
        /// </summary>
        const int Limit = 4;

        const int LimitLessProbe = Limit - 1;

        /// <summary>The wire text Redis uses, so existing client error handling applies unchanged.</summary>
        const string MaxClientsError = "-ERR max number of clients reached\r\n";

        /// <summary>
        /// rejected_connections is published by the monitor's sampling loop rather than read live,
        /// and the loop only exists when a metrics frequency is configured. Without this every read
        /// would return "0" and every assertion below would be vacuous.
        /// </summary>
        const int MetricsSamplingFreq = 1;

        GarnetServer server;

        /// <summary>
        /// A connection held open for the whole test, so INFO and CONFIG can be issued after the
        /// server is full. A probe that connected on demand would itself be refused.
        /// </summary>
        Socket probe;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                networkConnectionLimit: Limit, metricsSamplingFreq: MetricsSamplingFreq);
            server.Start();

            probe = OpenAcceptedConnection();
        }

        [TearDown]
        public void TearDown()
        {
            probe?.Dispose();
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        #region connection helpers

        /// <summary>
        /// Opens a socket and completes one PING, so the connection is established before the
        /// caller opens the next. The limit counts live handlers, so a racing accept would make the
        /// fill count unreliable.
        /// </summary>
        static Socket OpenAcceptedConnection(EndPoint endPoint = null)
        {
            var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(endPoint ?? TestUtils.EndPoint);
            socket.ReceiveTimeout = 10_000;
            socket.Send("PING\r\n"u8.ToArray());

            var buffer = new byte[64];
            var read = socket.Receive(buffer);
            ClassicAssert.AreEqual("+PONG\r\n", Encoding.ASCII.GetString(buffer, 0, read),
                "an accepted connection should have answered PING");
            return socket;
        }

        static List<Socket> FillRemainingSlots()
        {
            var accepted = new List<Socket>();
            for (var i = 0; i < LimitLessProbe; i++)
                accepted.Add(OpenAcceptedConnection());
            return accepted;
        }

        static void DisposeAll(List<Socket> sockets)
        {
            foreach (var socket in sockets)
                socket.Dispose();
        }

        /// <summary>
        /// Connects and reads until the peer closes, returning everything received. Reading to the
        /// close rather than taking the first packet means a truncated or missing tail cannot pass.
        /// </summary>
        static string ConnectAndReadToClose(EndPoint endPoint = null)
        {
            using var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(endPoint ?? TestUtils.EndPoint);
            socket.ReceiveTimeout = 10_000;

            var received = new List<byte>();
            var buffer = new byte[256];
            while (true)
            {
                int read;
                try
                {
                    read = socket.Receive(buffer);
                }
                catch (SocketException)
                {
                    // Peer reset rather than closing gracefully; report whatever arrived first.
                    break;
                }

                if (read == 0) break;
                received.AddRange(new ArraySegment<byte>(buffer, 0, read));
            }

            return Encoding.ASCII.GetString(received.ToArray());
        }

        #endregion

        #region probe exchange

        /// <summary>
        /// Issues one command on the persistent probe and returns its reply.
        ///
        /// A PING is pipelined behind the command and the read runs until "+PONG\r\n" arrives.
        /// RESP replies are ordered, so the sentinel cannot arrive before the preceding reply is
        /// complete -- which makes this correct for a bulk string of any length without the probe
        /// having to parse RESP framing itself.
        ///
        /// Commands are sent as RESP arrays: Garnet's inline parsing handles single-token commands
        /// but not multi-token ones, so an inline "CONFIG GET maxclients" produces no reply at all.
        /// </summary>
        string Exchange(params string[] command)
        {
            var request = new StringBuilder();
            request.Append('*').Append(command.Length).Append("\r\n");
            foreach (var token in command)
                request.Append('$').Append(token.Length).Append("\r\n").Append(token).Append("\r\n");
            request.Append("*1\r\n$4\r\nPING\r\n");

            probe.Send(Encoding.ASCII.GetBytes(request.ToString()));

            const string Sentinel = "+PONG\r\n";
            var received = new List<byte>();
            var buffer = new byte[8192];

            while (true)
            {
                int read;
                try
                {
                    read = probe.Receive(buffer);
                }
                catch (SocketException e)
                {
                    Assert.Fail($"the probe connection failed while reading the reply to '{string.Join(' ', command)}': {e.Message}");
                    return null;
                }

                ClassicAssert.Greater(read, 0, $"the server closed the probe connection during '{string.Join(' ', command)}'");
                received.AddRange(new ArraySegment<byte>(buffer, 0, read));

                var text = Encoding.ASCII.GetString(received.ToArray());
                if (text.EndsWith(Sentinel, StringComparison.Ordinal))
                    return text[..^Sentinel.Length];
            }
        }

        long ReadRejectedConnections()
        {
            var info = Exchange("INFO", "STATS");
            foreach (var line in info.Split("\r\n", StringSplitOptions.RemoveEmptyEntries))
            {
                if (!line.StartsWith("rejected_connections:", StringComparison.Ordinal)) continue;
                return long.Parse(line["rejected_connections:".Length..]);
            }

            Assert.Fail("INFO STATS did not report rejected_connections");
            return -1;
        }

        /// <summary>
        /// Polls until rejected_connections reaches <paramref name="expected"/>, because the
        /// monitor publishes it on a sampling interval rather than at the moment of the refusal.
        ///
        /// The failure message names the expected count and the last value seen, so a build that
        /// stops counting rejections fails with a diagnosis rather than with "expected 0, was 0".
        /// </summary>
        void WaitForRejectedConnections(long expected)
        {
            var sw = Stopwatch.StartNew();
            long last = -1;
            while (sw.Elapsed < TimeSpan.FromSeconds(30))
            {
                last = ReadRejectedConnections();
                if (last == expected) return;
                Thread.Sleep(100);
            }

            Assert.Fail($"rejected_connections never reached {expected}; last read {last}");
        }

        #endregion

        /// <summary>
        /// The load-bearing assertion: a refused client is told why. Asserting only that the
        /// connection closed would pass against a silent drop.
        /// </summary>
        [Test]
        public void RejectedConnectionReceivesAnErrorRatherThanASilentClose()
        {
            var accepted = FillRemainingSlots();
            try
            {
                ClassicAssert.AreEqual(MaxClientsError, ConnectAndReadToClose(),
                    "a connection refused by the limit must receive the RESP error before the close, " +
                    "not an unexplained reset");
            }
            finally
            {
                DisposeAll(accepted);
            }
        }

        /// <summary>
        /// The error must arrive unsolicited. A refused connection never gets a session, so there
        /// is no command for the server to reply to -- the write has to happen at accept time.
        /// </summary>
        [Test]
        public void TheErrorArrivesWithoutTheClientSendingAnything()
        {
            var accepted = FillRemainingSlots();
            try
            {
                using var refused = new Socket(SocketType.Stream, ProtocolType.Tcp);
                refused.Connect(TestUtils.EndPoint);
                refused.ReceiveTimeout = 10_000;

                var buffer = new byte[128];
                var read = refused.Receive(buffer);

                ClassicAssert.AreEqual(MaxClientsError, Encoding.ASCII.GetString(buffer, 0, read));
            }
            finally
            {
                DisposeAll(accepted);
            }
        }

        /// <summary>
        /// Pins the counter, and pins that it counts rejections specifically rather than accepts:
        /// the connections that filled the server must leave it at zero, and a later accepted
        /// connection must not advance it.
        /// </summary>
        [Test]
        public void RejectedConnectionsAreCountedInInfoStats()
        {
            var accepted = FillRemainingSlots();
            try
            {
                const int Refusals = 3;
                for (var i = 0; i < Refusals; i++)
                    _ = ConnectAndReadToClose();

                WaitForRejectedConnections(Refusals);

                // Free a slot and use it, to show the counter tracks refusals and not churn.
                accepted[0].Dispose();
                accepted.RemoveAt(0);
                WaitForAcceptance(out var refusedWhileWaiting).Dispose();

                // Two sampling intervals, so a build that miscounted accepts has had time to show it.
                Thread.Sleep(MetricsSamplingFreq * 2000);
                ClassicAssert.AreEqual(Refusals + refusedWhileWaiting, ReadRejectedConnections(),
                    "accepted connections must not be counted as rejected");
            }
            finally
            {
                DisposeAll(accepted);
            }
        }

        /// <summary>
        /// Lowering maxclients below the live population refuses the next connection, and raising
        /// it admits again. This is the only assertion that pins the CONFIG SET update action
        /// reaching the accept path -- a startup-configured limit would pass without it.
        /// </summary>
        [Test]
        public void MaxClientsSetAtRuntimeTakesEffectOnTheAcceptPath()
        {
            ClassicAssert.AreEqual($"*2\r\n$10\r\nmaxclients\r\n${Limit.ToString().Length}\r\n{Limit}\r\n",
                Exchange("CONFIG", "GET", "maxclients"),
                "CONFIG GET should report the startup limit before anything changes it");

            // The probe plus one more, then lower the ceiling onto exactly that population.
            using var held = OpenAcceptedConnection();

            ClassicAssert.AreEqual("+OK\r\n", Exchange("CONFIG", "SET", "maxclients", "2"));
            ClassicAssert.AreEqual("*2\r\n$10\r\nmaxclients\r\n$1\r\n2\r\n", Exchange("CONFIG", "GET", "maxclients"));

            ClassicAssert.AreEqual(MaxClientsError, ConnectAndReadToClose(),
                "lowering maxclients onto the live population must refuse the next connection");

            // Existing connections are untouched by the lowering, as in Redis. ECHO rather than
            // PING, because Exchange's sentinel is a PING reply and a command whose own reply is
            // also "+PONG\r\n" would let the read stop one reply early.
            ClassicAssert.AreEqual("$5\r\nalive\r\n", Exchange("ECHO", "alive"),
                "lowering maxclients must not disconnect established clients");

            ClassicAssert.AreEqual("+OK\r\n", Exchange("CONFIG", "SET", "maxclients", "10"));
            WaitForAcceptance().Dispose();
        }

        /// <summary>
        /// -1 restores unlimited admission at runtime. Asserts several connections past the old
        /// ceiling, since admitting exactly one more is also what raising the limit by one would do.
        /// </summary>
        [Test]
        public void MaxClientsCanBeSetToUnlimitedAtRuntime()
        {
            var accepted = FillRemainingSlots();
            try
            {
                ClassicAssert.AreEqual(MaxClientsError, ConnectAndReadToClose());

                ClassicAssert.AreEqual("+OK\r\n", Exchange("CONFIG", "SET", "maxclients", "-1"));

                for (var i = 0; i < Limit + 2; i++)
                    accepted.Add(OpenAcceptedConnection());
            }
            finally
            {
                DisposeAll(accepted);
            }
        }

        /// <summary>
        /// Capacity returns when the refused-then-drained server observes the closes, and the
        /// counter does not unwind with it -- it is a cumulative total, not a gauge.
        /// </summary>
        [Test]
        public void TheServerRecoversWhenConnectionsDrainAndTheCounterDoesNotUnwind()
        {
            var accepted = FillRemainingSlots();
            _ = ConnectAndReadToClose();
            WaitForRejectedConnections(1);

            DisposeAll(accepted);

            using var reconnected = WaitForAcceptance(out var refusedWhileWaiting);

            Thread.Sleep(MetricsSamplingFreq * 2000);
            ClassicAssert.AreEqual(1 + refusedWhileWaiting, ReadRejectedConnections(),
                "rejected_connections is cumulative, so draining and reconnecting must not reduce it");
        }

        static Socket WaitForAcceptance() => WaitForAcceptance(out _);

        /// <summary>
        /// Polls until the server has capacity, returning the established socket. Capacity returns
        /// when the server observes the closes, which is not synchronous with the client's dispose,
        /// so an attempt made before it does is refused for real and does advance
        /// <c>rejected_connections</c>.
        ///
        /// <paramref name="refused"/> reports how many attempts were turned away, so a caller
        /// asserting on that counter can stay exact instead of racing a teardown it does not
        /// control. Only a genuine max-clients reply is counted, so an unrelated socket error
        /// cannot inflate the allowance and mask a miscount.
        /// </summary>
        static Socket WaitForAcceptance(out int refused)
        {
            refused = 0;
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed < TimeSpan.FromSeconds(30))
            {
                var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
                try
                {
                    socket.Connect(TestUtils.EndPoint);
                    socket.ReceiveTimeout = 1000;
                    socket.Send("PING\r\n"u8.ToArray());

                    var buffer = new byte[64];
                    var read = socket.Receive(buffer);
                    var reply = read > 0 ? Encoding.ASCII.GetString(buffer, 0, read) : string.Empty;
                    if (reply == "+PONG\r\n")
                        return socket;
                    if (reply == MaxClientsError)
                        refused++;
                }
                catch (SocketException) { }

                socket.Dispose();
                Thread.Sleep(50);
            }

            Assert.Fail("the server never regained capacity");
            return null;
        }
    }

    /// <summary>
    /// The ceiling is one process-wide budget, not one per listener. Its own fixture because it
    /// needs a second endpoint.
    /// </summary>
    [TestFixture]
    public class ConnectionLimitAcrossListenersTests : TestBase
    {
        const int Limit = 3;

        const string MaxClientsError = "-ERR max number of clients reached\r\n";

        GarnetServer server;
        EndPoint first;
        EndPoint second;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            first = TestUtils.EndPoint;
            second = new IPEndPoint(IPAddress.Loopback, TestUtils.TestPort + 1);

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                endpoints: [first, second], networkConnectionLimit: Limit);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// Filling one listener to the ceiling must refuse the other listener too. With a
        /// per-listener limit each endpoint would have its own ceiling and this would be accepted,
        /// giving a process that admits N times the configured maximum.
        ///
        /// The second listener is made to accept one connection first. Without that, a refusal
        /// there is equally consistent with the second listener having reached a ceiling of its
        /// own, and the test would pass against the defect it exists to catch.
        /// </summary>
        [Test]
        public void TheCeilingIsSharedAcrossListenersRatherThanGrantedToEachOne()
        {
            var accepted = new List<Socket>
            {
                // Establishes that the second listener admits connections in its own right, so the
                // refusal below can only be the shared sum.
                OpenAcceptedConnection(second)
            };

            try
            {
                for (var i = 0; i < Limit - 1; i++)
                    accepted.Add(OpenAcceptedConnection(first));

                using var refused = new Socket(SocketType.Stream, ProtocolType.Tcp);
                refused.Connect(second);
                refused.ReceiveTimeout = 10_000;

                var reply = new byte[128];
                var got = refused.Receive(reply);

                ClassicAssert.AreEqual(MaxClientsError, Encoding.ASCII.GetString(reply, 0, got),
                    $"the second listener admitted a connection while the process already held {Limit}, " +
                    "so the ceiling is being granted per listener rather than shared");
            }
            finally
            {
                foreach (var socket in accepted) socket.Dispose();
            }
        }

        /// <summary>
        /// <c>CONFIG SET maxclients</c> must reach the shared limit in a host that has more than one
        /// listener. The update walks the server's listeners, so a host whose listener collection is
        /// empty -- or whose entries fail the cast -- would return +OK and change nothing, which no
        /// single-endpoint test can distinguish from working.
        ///
        /// Raising rather than lowering, so the assertion is an admission that was previously
        /// refused. A lowering test could be satisfied by the connections already being at the
        /// ceiling.
        /// </summary>
        [Test]
        public void MaxClientsReachesTheSharedLimitFromAMultiListenerHost()
        {
            var accepted = new List<Socket>();

            try
            {
                // Held first, so it survives the server reaching its ceiling below. A connection
                // opened once the ceiling is reached would itself be refused.
                var admin = OpenAcceptedConnection(first);
                accepted.Add(admin);

                for (var i = 0; i < Limit - 1; i++)
                    accepted.Add(OpenAcceptedConnection(i % 2 == 0 ? second : first));

                using (var refused = new Socket(SocketType.Stream, ProtocolType.Tcp))
                {
                    refused.Connect(second);
                    refused.ReceiveTimeout = 10_000;

                    var reply = new byte[128];
                    var got = refused.Receive(reply);
                    ClassicAssert.AreEqual(MaxClientsError, Encoding.ASCII.GetString(reply, 0, got),
                        "the process should be at its ceiling before the limit is raised");
                }

                ClassicAssert.AreEqual("+OK\r\n", ConfigSetMaxClients(admin, Limit + 4));

                // Both listeners must honour the raised value, since they share one limit object.
                // A CONFIG SET that reached no listener would leave these refused.
                accepted.Add(OpenAcceptedConnection(second));
                accepted.Add(OpenAcceptedConnection(first));
            }
            finally
            {
                foreach (var socket in accepted) socket.Dispose();
            }
        }

        static string ConfigSetMaxClients(Socket socket, int value)
        {
            var text = value.ToString();
            var command = $"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$10\r\nmaxclients\r\n${text.Length}\r\n{text}\r\n";
            socket.Send(Encoding.ASCII.GetBytes(command));

            var buffer = new byte[256];
            var read = socket.Receive(buffer);
            return Encoding.ASCII.GetString(buffer, 0, read);
        }

        static Socket OpenAcceptedConnection(EndPoint endPoint)
        {
            var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(endPoint);
            socket.ReceiveTimeout = 10_000;
            socket.Send("PING\r\n"u8.ToArray());

            var buffer = new byte[64];
            var read = socket.Receive(buffer);
            ClassicAssert.AreEqual("+PONG\r\n", Encoding.ASCII.GetString(buffer, 0, read),
                $"the listener at {endPoint} should have accepted this connection");
            return socket;
        }
    }

    /// <summary>
    /// Under TLS a refused client cannot be told why -- the peer has sent a ClientHello and is
    /// waiting for a ServerHello, so a RESP error there would be a protocol violation surfacing as a
    /// handshake failure and pointing the operator at certificates rather than at capacity. So for
    /// TLS the rejected_connections counter is the entire signal, and the incident that motivated
    /// this work was thousands of TLS connections.
    ///
    /// Rejection happens at accept time, before any handshake, which is what makes this testable
    /// with bare sockets against a TLS listener.
    /// </summary>
    [TestFixture]
    public class ConnectionLimitTlsTests : TestBase
    {
        const int Limit = 4;
        const int MetricsSamplingFreq = 1;

        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: true,
                networkConnectionLimit: Limit, metricsSamplingFreq: MetricsSamplingFreq);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        [Test]
        public async Task TlsRejectionsAreCountedEvenThoughTheClientCannotBeTold()
        {
            var clients = new List<GarnetClient>();
            try
            {
                // Fillers are real TLS clients rather than bare sockets so that the test asserts on
                // admitted connections. Bare silent sockets would also occupy slots -- the handshake
                // has no timeout, so they hold them indefinitely -- but nothing would distinguish an
                // occupied slot from a connection still waiting to be accepted.
                for (var i = 0; i < Limit; i++)
                {
                    var client = TestUtils.GetGarnetClient(useTLS: true);
                    await client.ConnectAsync();
                    _ = await client.PingAsync();
                    clients.Add(client);
                }

                using (var refused = new Socket(SocketType.Stream, ProtocolType.Tcp))
                {
                    refused.Connect(TestUtils.EndPoint);
                    refused.ReceiveTimeout = 30_000;

                    var buffer = new byte[128];
                    var read = refused.Receive(buffer);
                    ClassicAssert.AreEqual(0, read,
                        "a TLS listener must close a refused connection without writing a RESP error, " +
                        "which would be a protocol violation mid-handshake");
                }

                await WaitForRejectedConnections(clients[0], 1);
            }
            finally
            {
                foreach (var client in clients) client.Dispose();
            }
        }

        static async Task WaitForRejectedConnections(GarnetClient client, long expected)
        {
            var deadline = Stopwatch.StartNew();
            long last = -1;

            while (deadline.Elapsed < TimeSpan.FromSeconds(30))
            {
                var stats = await client.ExecuteForStringResultAsync("INFO", ["STATS"]);
                foreach (var line in stats.Split('\n'))
                {
                    if (!line.StartsWith("rejected_connections:", StringComparison.Ordinal)) continue;
                    last = long.Parse(line["rejected_connections:".Length..].Trim());
                    break;
                }

                if (last >= expected) return;
                await Task.Delay(200);
            }

            Assert.Fail($"rejected_connections never reached {expected}; last read {last}");
        }
    }
}