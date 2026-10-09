// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers which suspensions keep the cluster epoch and which release it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A session advertises an operation in flight by holding the cluster epoch. A configuration change
    /// flips the slot state and then waits for every session to leave the older epoch before it moves any
    /// data, so a session holding the epoch cannot act on a key that has already been migrated away.
    /// </para>
    /// <para>
    /// A suspension therefore has to choose. A body waiting on an operation keeps the epoch, so the key it
    /// is operating on is still there when the operation completes. A body waiting while idle releases it,
    /// so a wait with no bound on its length cannot stall a cluster transition. These tests pin both.
    /// <c>CLUSTER SETSLOT ... STABLE</c> stands in for the transition: it always reaches the epoch barrier
    /// and, unlike the migrating and importing states, needs no second node to name.
    /// </para>
    /// </remarks>
    [TestFixture]
    public class RespClusterEpochSuspensionTests : TestBase
    {
        /// <summary>How long a parked command waits. Long enough that the barrier cannot cross it by luck.</summary>
        const double ParkSeconds = 2.0;

        /// <summary>Time allowed for the parked command to reach its wait before the barrier is started.</summary>
        static readonly TimeSpan Settle = TimeSpan.FromMilliseconds(500);

        /// <summary>
        /// Shortest barrier that counts as having waited for the park. Comfortably above <see cref="Settle"/>
        /// and comfortably below the time left on the park when the barrier starts.
        /// </summary>
        static readonly TimeSpan HeldThreshold = TimeSpan.FromMilliseconds(750);

        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableCluster: true);
            server.Start();

            // Own every slot, so ordinary key commands are served and SETSLOT has a slot to act on.
            using var admin = new RawConnection();
            ClassicAssert.AreEqual("+OK", admin.Execute("CLUSTER", "ADDSLOTSRANGE", "0", "16383"));
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// A command that suspends with an operation in flight keeps the epoch, so a configuration change
        /// started while it waits does not complete until it has resumed and finished with its key.
        /// </summary>
        [Test]
        public void ASuspensionWithAnOperationInFlightHoldsTheClusterEpoch()
        {
            using var writer = new RawConnection();
            ClassicAssert.AreEqual("+OK", writer.Execute("SET", "epochkey", "value"));

            using var parked = new RawConnection();
            parked.Send("DEBUG", "BLOCKIO", ParkSeconds.ToString("0.0"), "epochkey");
            Thread.Sleep(Settle);

            var elapsed = TimeTransition(writer);

            // The barrier could not retire the older epoch while the command was waiting on its operation.
            ClassicAssert.GreaterOrEqual(elapsed, HeldThreshold,
                $"CLUSTER SETSLOT returned in {elapsed.TotalMilliseconds:F0} ms, so the suspended command " +
                "had already released its epoch and a migration could have moved its key.");

            // The key the command resumed onto is the one it was parked on.
            ClassicAssert.AreEqual("value", parked.ReadReply());
        }

        /// <summary>
        /// A command that suspends while idle releases the epoch, so a configuration change started while it
        /// waits completes immediately rather than being held up for the length of the wait.
        /// </summary>
        [Test]
        public void AnIdleSuspensionReleasesTheClusterEpoch()
        {
            using var writer = new RawConnection();
            ClassicAssert.AreEqual("+OK", writer.Execute("SET", "epochkey", "value"));

            using var parked = new RawConnection();
            parked.Send("DEBUG", "BLOCK", ParkSeconds.ToString("0.0"), "epochkey");
            Thread.Sleep(Settle);

            var elapsed = TimeTransition(writer);

            ClassicAssert.Less(elapsed, HeldThreshold,
                $"CLUSTER SETSLOT took {elapsed.TotalMilliseconds:F0} ms, so an idle wait is holding its " +
                "epoch and can stall a cluster transition for as long as it waits.");

            ClassicAssert.AreEqual("value", parked.ReadReply());
        }

        /// <summary>
        /// Runs a configuration change, which bumps the epoch and waits for every session to leave the older
        /// one, and returns how long that took.
        /// </summary>
        static TimeSpan TimeTransition(RawConnection connection)
        {
            var start = Stopwatch.GetTimestamp();
            var reply = connection.Execute("CLUSTER", "SETSLOT", "0", "STABLE");
            var elapsed = Stopwatch.GetElapsedTime(start);

            ClassicAssert.AreEqual("+OK", reply);
            return elapsed;
        }

        /// <summary>
        /// Minimal RESP connection. Sending and reading are separate so a command can be left parked while
        /// another connection is used.
        /// </summary>
        sealed class RawConnection : IDisposable
        {
            readonly Socket socket;
            readonly byte[] buffer = new byte[8 * 1024];
            int start, end;

            internal RawConnection()
            {
                var endpoint = TestUtils.EndPoint;
                socket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
                {
                    NoDelay = true,
                    ReceiveTimeout = (int)TimeSpan.FromSeconds(60).TotalMilliseconds,
                };
                socket.Connect(endpoint);
            }

            internal string Execute(params string[] args)
            {
                Send(args);
                return ReadReply();
            }

            internal void Send(params string[] args)
            {
                var frame = new StringBuilder().Append('*').Append(args.Length).Append("\r\n");
                foreach (var arg in args)
                    frame.Append('$').Append(Encoding.UTF8.GetByteCount(arg)).Append("\r\n").Append(arg).Append("\r\n");

                var bytes = Encoding.UTF8.GetBytes(frame.ToString());
                var sent = 0;
                while (sent < bytes.Length)
                    sent += socket.Send(bytes, sent, bytes.Length - sent, SocketFlags.None);
            }

            /// <summary>Reads one reply, returning a bulk string's payload and any other reply's whole line.</summary>
            internal string ReadReply()
            {
                var line = ReadLine();
                if (line.Length == 0 || line[0] != '$')
                    return line;

                var length = int.Parse(line.AsSpan(1));
                if (length < 0)
                    return null;

                while (end - start < length + 2)
                    Fill();

                var payload = Encoding.UTF8.GetString(buffer, start, length);
                start += length + 2;
                return payload;
            }

            string ReadLine()
            {
                while (true)
                {
                    for (var i = start; i + 1 < end; i++)
                    {
                        if (buffer[i] != '\r' || buffer[i + 1] != '\n')
                            continue;

                        var line = Encoding.UTF8.GetString(buffer, start, i - start);
                        start = i + 2;
                        return line;
                    }
                    Fill();
                }
            }

            void Fill()
            {
                if (start > 0)
                {
                    Buffer.BlockCopy(buffer, start, buffer, 0, end - start);
                    end -= start;
                    start = 0;
                }

                var read = socket.Receive(buffer, end, buffer.Length - end, SocketFlags.None);
                if (read <= 0)
                    Assert.Fail("The server closed the connection before replying.");

                end += read;
            }

            public void Dispose() => socket.Dispose();
        }
    }
}