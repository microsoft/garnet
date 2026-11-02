// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers clients that stop reading their responses. A session executes commands on a thread-pool thread
    /// and blocks that thread once its outstanding sends reach the per-session throttle, so a client that never
    /// drains its socket holds the thread for as long as its sends cannot complete.
    /// </summary>
    /// <remarks>
    /// Each client requests <see cref="GetsPerClient"/> copies of a <see cref="ValueSize"/> value: several times what
    /// the default throttle of eight 128 KB sends and the kernel's socket buffers hold between them.
    /// </remarks>
    [TestFixture]
    public class NetworkSendStallTests : TestBase
    {
        const int ValueSize = 2 * 1024 * 1024;
        const int GetsPerClient = 3;
        const string Key = "bigkey";
        const ExceptionInjectionType PauseBeforeSlotRelease = ExceptionInjectionType.Sender_Pause_Before_Slot_Release;

        // A throttle no response reaches, so every send is in flight when QUIT ends the session and the only wait
        // is the one for them to drain
        const int UnreachedThrottle = 1024;

        // A slow reader takes about 1 MB a second. Sends complete in batches as the socket buffers drain, about a
        // second apart, within its send timeout, while all of its responses take several timeouts to deliver.
        const int SlowReadChunk = 64 * 1024;
        static readonly TimeSpan SlowReadPause = TimeSpan.FromMilliseconds(60);
        static readonly TimeSpan SlowReaderSendTimeout = TimeSpan.FromSeconds(2);

        static readonly TimeSpan SendTimeout = TimeSpan.FromSeconds(2);

        // A stalled connection closes one send timeout after it stalls; a second timeout would exceed this
        static readonly TimeSpan StalledCloseBound = 1.5 * SendTimeout;

        static readonly TimeSpan Deadline = TimeSpan.FromSeconds(30);

        GarnetServer server;

        [SetUp]
        public void Setup()
            => TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

        [TearDown]
        public void TearDown()
        {
            ExceptionInjectionHelper.DisableException(PauseBeforeSlotRelease);
            server?.Dispose();
            server = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        void StartServer(TimeSpan networkSendTimeout, int? networkSendThrottleMax = null, TextWriter logTo = null, bool enableTLS = false)
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: enableTLS,
                networkSendTimeout: networkSendTimeout, networkSendThrottleMax: networkSendThrottleMax, logTo: logTo);
            server.Start();

            using Stream setup = enableTLS ? ConnectTls() : new NetworkStream(Connect(), ownsSocket: true);
            var value = new byte[ValueSize];
            new Random(0).NextBytes(value);
            setup.Write(Command("SET"u8.ToArray(), Encoding.ASCII.GetBytes(Key), value));
            ExpectReply(setup, "+OK\r\n");
        }

        /// <summary>
        /// A client that stops reading is disconnected once no send completes for the send timeout, and other
        /// clients are served throughout.
        /// </summary>
        [Test]
        public void ClientsThatStopReadingAreDisconnectedAfterTheSendTimeout()
        {
            const int StalledClients = 4;

            var log = new StringWriter();
            StartServer(SendTimeout, logTo: TextWriter.Synchronized(log));
            using var healthy = Connect();

            var stalled = new List<Socket>();
            try
            {
                var elapsed = Stopwatch.StartNew();
                for (var i = 0; i < StalledClients; i++)
                    stalled.Add(ConnectAndStopReading());

                // The setup connection is the one other disposal
                WaitForDisposedConnections(1 + StalledClients,
                    "a client that accepts no data for the send timeout must be disconnected");
                Assert.That(elapsed.Elapsed, Is.LessThan(StalledCloseBound),
                    "a send that times out must close its connection, rather than wait a further timeout for the connection's other sends to drain");

                Ping(healthy);
                foreach (var socket in stalled)
                    AssertClosedBeforeFullResponse(socket);

                var logged = log.ToString();
                Assert.That(logged, Does.Not.Contain(".(errr)").And.Not.Contain(".(crit)"),
                    "a client that stops reading is an expected condition, not a server error");
            }
            finally
            {
                foreach (var socket in stalled) socket.Dispose();
            }
        }

        /// <summary>
        /// Over TLS a session's responses are encrypted onto the same sender, so a TLS client that stops reading
        /// is disconnected in the same way, and its session ends without error.
        /// </summary>
        [Test]
        public void TlsClientsThatStopReadingAreDisconnectedAfterTheSendTimeout()
        {
            var log = new StringWriter();
            StartServer(SendTimeout, logTo: TextWriter.Synchronized(log), enableTLS: true);
            using var healthy = ConnectTls();

            using var stalled = ConnectTls(receiveBufferSize: 4096);
            var elapsed = Stopwatch.StartNew();
            stalled.Write(Gets());

            WaitForDisposedConnections(2, "a TLS client that accepts no data for the send timeout must be disconnected");
            Assert.That(elapsed.Elapsed, Is.LessThan(StalledCloseBound),
                "a send that times out must close its connection, rather than wait a further timeout for the connection's other sends to drain");

            Ping(healthy);
            AssertClosedBeforeFullResponse(stalled);

            var logged = log.ToString();
            Assert.That(logged, Does.Not.Contain(".(errr)").And.Not.Contain(".(crit)"),
                "a TLS client that stops reading is an expected condition, not a server error");
            Assert.That(logged, Does.Not.Contain(nameof(IOException)),
                "a send timeout must end the session without writing again to the connection it closed");
        }

        /// <summary>
        /// The send timeout bounds a period without progress, not a whole response, so a client that reads
        /// steadily but slowly receives all of a response that takes several timeouts to deliver.
        /// </summary>
        [Test]
        public void AClientThatReadsSlowlyIsNotDisconnected()
        {
            StartServer(SlowReaderSendTimeout);
            using var slow = Connect(receiveBufferSize: SlowReadChunk);
            slow.Send(Gets());

            ReadSlowly(slow, GetsPerClient * BulkStringLength(ValueSize));
            Ping(slow);
        }

        /// <summary>
        /// A client that quits while its earlier responses are undelivered, and then stops reading, is
        /// disconnected once those responses stop draining, rather than holding the session's thread until it
        /// reads them.
        /// </summary>
        [Test]
        public void AClientThatQuitsWithoutReadingIsDisconnected()
        {
            StartServer(SendTimeout, networkSendThrottleMax: UnreachedThrottle);
            using var healthy = Connect();

            using var quitting = Connect(receiveBufferSize: 4096);
            var elapsed = Stopwatch.StartNew();
            quitting.Send([.. Gets(), .. Command("QUIT")]);

            WaitForDisposedConnections(2,
                "a session that ends with undelivered responses must close once they stop draining");
            Assert.That(elapsed.Elapsed, Is.LessThan(StalledCloseBound),
                "a session whose responses stop draining must close once the send timeout elapses");
            Ping(healthy);
            AssertClosedBeforeFullResponse(quitting);
        }

        /// <summary>
        /// Like a send, the wait for a quitting session's responses to drain is bounded by a period without
        /// progress, so a client that quits and then reads slowly receives every response before the close.
        /// </summary>
        [Test]
        public void AClientThatQuitsAndReadsSlowlyReceivesEveryResponse()
        {
            // Windows holds back the completion of a send made while the socket's send buffer is full until the data
            // buffered ahead of it fits that buffer again. Sends in flight at QUIT then complete together near the
            // end of delivery, so a drain observes progress only where sends complete as the buffers drain.
            if (OperatingSystem.IsWindows())
                Assert.Ignore("Windows completes the sends a drain waits for together, at the end of their delivery.");

            StartServer(SlowReaderSendTimeout, networkSendThrottleMax: UnreachedThrottle);
            using var quitting = Connect(receiveBufferSize: SlowReadChunk);
            quitting.Send([.. Gets(), .. Command("QUIT")]);

            ReadSlowly(quitting, GetsPerClient * BulkStringLength(ValueSize) + "+OK\r\n".Length);
            ClassicAssert.AreEqual(0, quitting.Receive(new byte[1]), "QUIT must close the connection after its reply");
        }

        /// <summary>
        /// A stalled client holds a server thread until its sends complete or time out, whether the thread is
        /// waiting to send more of a response or, after QUIT, for the responses already sent to drain. That thread
        /// is lent back to the thread pool while it waits, so other connections are still served even when stalled
        /// clients outnumber the threads the pool would otherwise run.
        /// </summary>
        [Test]
        public void ClientsThatStopReadingDoNotDelayOtherConnections([Values] bool quit)
        {
            // The pool replaces a thread it is not told is blocked about once a second, so a request queued behind
            // this many stalled ones would wait many seconds
            const int ThreadGoal = 8;
            const int StalledClients = 4 * ThreadGoal;
            var probeDeadline = TimeSpan.FromSeconds(5);

            // No timeout, so only the thread pool can make room for other connections
            StartServer(Timeout.InfiniteTimeSpan, networkSendThrottleMax: quit ? UnreachedThrottle : null);
            using var healthy = Connect();
            Ping(healthy);

            var stalled = new List<Socket>();
            using var threadGoal = ThreadPoolGoal.Lower(ThreadGoal);
            try
            {
                // Establish every session first, so that each is waiting on a receive and their requests reach the
                // server as socket completions, queued ahead of the healthy connection's request
                for (var i = 0; i < StalledClients; i++)
                {
                    stalled.Add(Connect(receiveBufferSize: 4096));
                    Ping(stalled[i]);
                }
                foreach (var socket in stalled)
                    socket.Send(quit ? [.. Gets(), .. Command("QUIT")] : Gets());

                // Let the stalled sessions take their threads
                Thread.Sleep(1000);

                var elapsed = Stopwatch.StartNew();
                Ping(healthy);
                Assert.That(elapsed.Elapsed, Is.LessThan(probeDeadline),
                    "clients that stop reading must not delay other connections");
            }
            finally
            {
                foreach (var socket in stalled) socket.Dispose();
            }
        }

        /// <summary>
        /// A session subscribed to a channel it publishes to delivers the message into its own response. A send of
        /// that message that times out ends the session without error, rather than letting the PUBLISH reply be
        /// written into the response buffer the failed send released, and the message still reaches the
        /// subscribers after it. A session subscribed both to the channel and to a pattern matching it receives
        /// the message twice, and writes neither delivery after the first fails.
        /// </summary>
        [Test]
        public async Task AClientThatStopsReadingItsOwnPublishedMessageIsDisconnected(
            [Values("SUBSCRIBE", "PSUBSCRIBE", "SUBSCRIBE PSUBSCRIBE")] string subscriptions)
        {
            const string Channel = "channel";
            var subscribes = subscriptions.Split(' ');
            var subscribe = subscribes[^1];
            var kind = subscribe.ToLowerInvariant();

            var log = new StringWriter();
            StartServer(SendTimeout, logTo: TextWriter.Synchronized(log));
            using var healthy = Connect();

            // Subscribers receive a message in the order they subscribed, so the stalled client subscribes first
            using var stalled = Connect(receiveBufferSize: 4096);
            using (var stream = new NetworkStream(stalled, ownsSocket: false))
            {
                stream.Write([.. Command("HELLO", "3"), .. subscribes.SelectMany(command => Command(command, Channel))]);
                ReadUntil(stream, $"{kind}\r\n${Channel.Length}\r\n{Channel}\r\n:{subscribes.Length}\r\n");
            }

            using var listener = new NetworkStream(Connect(), ownsSocket: true);
            listener.Write(Command(subscribe, Channel));
            ExpectReply(listener, $"*3\r\n${kind.Length}\r\n{kind}\r\n${Channel.Length}\r\n{Channel}\r\n:1\r\n");

            var message = new byte[GetsPerClient * ValueSize];
            var delivery = subscribe == "SUBSCRIBE" ? "*3\r\n$7\r\nmessage\r\n" : $"*4\r\n$8\r\npmessage\r\n${Channel.Length}\r\n{Channel}\r\n";
            var delivered = Task.Run(() => ExpectReply(listener,
                $"{delivery}${Channel.Length}\r\n{Channel}\r\n${message.Length}\r\n{new string('\0', message.Length)}\r\n"));
            stalled.Send(Command("PUBLISH"u8.ToArray(), Encoding.ASCII.GetBytes(Channel), message));

            WaitForDisposedConnections(2, "a client that accepts no data for the send timeout must be disconnected");
            Ping(healthy);
            AssertClosedBeforeFullResponse(stalled);
            await delivered.WaitAsync(Deadline);

            var logged = log.ToString();
            Assert.That(logged, Does.Not.Contain(".(errr)").And.Not.Contain(".(crit)"),
                "a client that stops reading its own published message is an expected condition, not a server error");
        }

        /// <summary>
        /// A send that times out has its connection closed, but must not let the connection be disposed while the
        /// completion that freed its slot has yet to release it, since that release would then land on the
        /// disposed sender. The completion is held between freeing the slot and releasing it until the send has
        /// timed out.
        /// </summary>
        [Test]
        public async Task ASendCompletionHeldPastTheSendTimeoutDoesNotOutliveItsConnection()
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            var log = new StringWriter();
            StartServer(SendTimeout, networkSendThrottleMax: 1, logTo: TextWriter.Synchronized(log));
            using var healthy = Connect();

            using var stalled = Connect(receiveBufferSize: 4096);
            ExceptionInjectionHelper.EnableException(PauseBeforeSlotRelease);
            stalled.Send(Command("GET", Key));

            // Let the session fill the socket and wait for a slot, then read, so a send completes and frees the
            // slot while the session waits for it. Reading on to the close leaves no other send in flight.
            Thread.Sleep(200);
            var drained = Task.Run(() => AssertClosedBeforeFullResponse(stalled, expected: BulkStringLength(ValueSize)));
            await ExceptionInjectionHelper.WaitOnClearAsync(PauseBeforeSlotRelease).WaitAsync(Deadline);
            await drained.WaitAsync(Deadline);

            Thread.Sleep(500);
            ClassicAssert.AreEqual(1, ((GarnetServerBase)server.Provider.StoreWrapper.Servers[0]).TotalConnectionsDisposed,
                "a connection must not be disposed while a completion has yet to release its timed-out send's slot");

            ExceptionInjectionHelper.EnableException(PauseBeforeSlotRelease);
            WaitForDisposedConnections(2, "a send that times out must dispose its connection once its slot is released");
            Ping(healthy);

            var logged = log.ToString();
            Assert.That(logged, Does.Not.Contain(".(errr)").And.Not.Contain(".(crit)"),
                "a send completion released after its connection timed out must not fail");
        }

        static SslStream ConnectTls(int receiveBufferSize = 0)
        {
            var ssl = new SslStream(new NetworkStream(Connect(receiveBufferSize), ownsSocket: true), leaveInnerStreamOpen: false,
                TestUtils.ValidateServerCertificate);
            ssl.AuthenticateAsClient(new SslClientAuthenticationOptions
            {
                ClientCertificates = [TestUtils.GetClientCertificate()],
                TargetHost = "GarnetTest",
                AllowRenegotiation = false,
            });
            return ssl;
        }

        static Socket Connect(int receiveBufferSize = 0)
        {
            var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true, ReceiveTimeout = (int)Deadline.TotalMilliseconds };

            // Set before connecting, so the window advertised in the handshake is small too
            if (receiveBufferSize > 0)
                socket.ReceiveBufferSize = receiveBufferSize;
            socket.Connect(TestUtils.EndPoint);
            return socket;
        }

        /// <summary>
        /// Connects, requests the large value several times, and never reads.
        /// </summary>
        static Socket ConnectAndStopReading()
        {
            var socket = Connect(receiveBufferSize: 4096);
            socket.Send(Gets());
            return socket;
        }

        /// <summary>
        /// Pipelined GETs of the large value, <see cref="GetsPerClient"/> of them.
        /// </summary>
        static byte[] Gets()
            => [.. Enumerable.Range(0, GetsPerClient).SelectMany(_ => Command("GET", Key))];

        /// <summary>
        /// Reads <paramref name="expected"/> bytes, a chunk at a time with a pause before each, and requires the
        /// reading to outlast several send timeouts, so that delivery depends on the timeout tracking progress.
        /// </summary>
        static void ReadSlowly(Socket socket, long expected)
        {
            var buffer = new byte[SlowReadChunk];
            long received = 0;
            var elapsed = Stopwatch.StartNew();
            while (received < expected)
            {
                Thread.Sleep(SlowReadPause);
                var chunk = (int)Math.Min(SlowReadChunk, expected - received);
                for (var filled = 0; filled < chunk;)
                {
                    var read = socket.Receive(buffer, filled, chunk - filled, SocketFlags.None);
                    ClassicAssert.Greater(read, 0, $"the server closed a connection that was still reading, after {received + filled} bytes");
                    filled += read;
                }
                received += chunk;
            }

            Assert.That(elapsed.Elapsed, Is.GreaterThan(2 * SlowReaderSendTimeout),
                "the responses must take several send timeouts to deliver for this test to cover progress");
        }

        static void Ping(Socket socket)
        {
            using var stream = new NetworkStream(socket, ownsSocket: false);
            Ping(stream);
        }

        static void Ping(Stream stream)
        {
            stream.Write(Command("PING"));
            ExpectReply(stream, "+PONG\r\n");
        }

        /// <summary>
        /// Reads replies up to and including <paramref name="terminator"/>.
        /// </summary>
        static void ReadUntil(Stream stream, string terminator)
        {
            var received = new StringBuilder();
            while (!received.ToString().EndsWith(terminator, StringComparison.Ordinal))
            {
                var read = stream.ReadByte();
                ClassicAssert.GreaterOrEqual(read, 0, "connection closed before the reply");
                _ = received.Append((char)read);
            }
        }

        static void ExpectReply(Stream stream, string expected)
        {
            var buffer = new byte[expected.Length];
            for (var received = 0; received < buffer.Length;)
            {
                var read = stream.Read(buffer, received, buffer.Length - received);
                ClassicAssert.Greater(read, 0, "connection closed before the reply");
                received += read;
            }
            ClassicAssert.AreEqual(expected, Encoding.ASCII.GetString(buffer));
        }

        /// <summary>
        /// Reads what the server delivered before it closed the connection, which must be less than the full
        /// responses. A connection the server did not close delivers everything once read, then times out.
        /// </summary>
        static void AssertClosedBeforeFullResponse(Socket socket, long expected = GetsPerClient * (1 + 7 + 2 + ValueSize + 2L))
        {
            using var stream = new NetworkStream(socket, ownsSocket: false);
            AssertClosedBeforeFullResponse(stream, expected);
        }

        static void AssertClosedBeforeFullResponse(Stream stream, long expected = GetsPerClient * (1 + 7 + 2 + ValueSize + 2L))
        {
            var buffer = new byte[64 * 1024];
            long received = 0;
            try
            {
                int read;
                while ((read = stream.Read(buffer)) > 0)
                    received += read;
            }
            catch (IOException ex) when (ex.InnerException is not SocketException { SocketErrorCode: SocketError.TimedOut })
            {
            }

            ClassicAssert.Less(received, expected,
                "the server must close a stalled connection without delivering its outstanding responses");
        }

        /// <summary>
        /// Waits until the server has disposed at least <paramref name="expected"/> connections. The server starts
        /// with none disposed, so the count covers only connections the test closed or expects the server to close.
        /// </summary>
        void WaitForDisposedConnections(long expected, string because)
        {
            var tcpServer = (GarnetServerBase)server.Provider.StoreWrapper.Servers[0];
            var elapsed = Stopwatch.StartNew();
            while (tcpServer.TotalConnectionsDisposed < expected && elapsed.Elapsed < Deadline)
                Thread.Sleep(10);

            var disposed = tcpServer.TotalConnectionsDisposed;
            if (disposed < expected)
                Assert.Fail($"{disposed} of {expected} connections disposed within {Deadline.TotalSeconds:0} seconds: {because}");
        }

        static int BulkStringLength(int size)
            => 1 + size.ToString().Length + 2 + size + 2;

        static byte[] Command(params string[] args)
            => Command([.. args.Select(Encoding.ASCII.GetBytes)]);

        static byte[] Command(params byte[][] args)
        {
            var command = new List<byte>(Encoding.ASCII.GetBytes($"*{args.Length}\r\n"));
            foreach (var arg in args)
            {
                command.AddRange(Encoding.ASCII.GetBytes($"${arg.Length}\r\n"));
                command.AddRange(arg);
                command.AddRange("\r\n"u8);
            }
            return [.. command];
        }

        /// <summary>
        /// Lowers the thread pool's thread goal for the life of the scope, leaving its maximum unchanged. Threads
        /// the pool is told are blocked are then replaced promptly, up to that maximum, while threads blocked
        /// without notice are replaced only as fast as the pool's starvation detection adds them.
        /// </summary>
        sealed class ThreadPoolGoal : IDisposable
        {
            readonly int minWorkers, minIo;

            ThreadPoolGoal(int minWorkers, int minIo) => (this.minWorkers, this.minIo) = (minWorkers, minIo);

            internal static ThreadPoolGoal Lower(int workers)
            {
                ThreadPool.GetMinThreads(out var minWorkers, out var minIo);
                ThreadPool.GetMaxThreads(out var maxWorkers, out var maxIo);
                var scope = new ThreadPoolGoal(minWorkers, minIo);

                // Capping the maximum pulls the goal down to the cap, and restoring the maximum leaves it there
                var lowered = ThreadPool.SetMinThreads(workers, minIo) && ThreadPool.SetMaxThreads(workers, maxIo);
                var restored = ThreadPool.SetMaxThreads(maxWorkers, maxIo);
                if (!lowered || !restored)
                {
                    scope.Dispose();
                    Assert.Ignore("Could not lower the thread pool's thread goal, so a held thread cannot be made to matter.");
                }

                return scope;
            }

            public void Dispose() => _ = ThreadPool.SetMinThreads(minWorkers, minIo);
        }
    }
}