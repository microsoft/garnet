// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Garnet.networking;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// A session parked until its sender has room re-reads the sender after every wake rather than trusting the
    /// signal, so a wake that finds the sender full again parks the session a second time and runs nothing.
    /// </summary>
    /// <remarks>
    /// The session is driven directly, against a sender whose fullness the test decides. Behind a socket that
    /// order is set by the network: a wake finds the sender full again only when a send lands in the few
    /// instructions between a waiter registering and the session unwinding, which a test through a server
    /// reaches only by luck.
    /// </remarks>
    [TestFixture]
    public class RespSendCapacityParkTests : TestBase
    {
        static readonly TimeSpan Bound = TimeSpan.FromSeconds(15);

        static readonly FieldInfo SuspendedField = typeof(RespServerSession).GetField("asyncSuspended",
            BindingFlags.Instance | BindingFlags.NonPublic);

        GarnetServer server;

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
            server = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        [Test]
        public async Task AWakeThatFindsTheSenderFullAgainParksTheSessionAgain()
        {
            ClassicAssert.IsNotNull(SuspendedField, "RespServerSession should still track its suspension in 'asyncSuspended'");

            // Two replies that cannot share the sender's buffer, so the second command sends the first reply
            // and the sender reports itself full before the third command is read.
            const int PayloadLength = 700;
            var echo = Encoding.ASCII.GetBytes($"*2\r\n$4\r\nECHO\r\n${PayloadLength}\r\n{new string('x', PayloadLength)}\r\n");
            var echoReply = $"${PayloadLength}\r\n{new string('x', PayloadLength)}\r\n";
            var ping = "*1\r\n$4\r\nPING\r\n"u8.ToArray();

            var input = GC.AllocateArray<byte>(echo.Length * 2 + ping.Length, pinned: true);
            echo.CopyTo(input, 0);
            echo.CopyTo(input, echo.Length);
            ping.CopyTo(input, echo.Length * 2);

            var sender = new ControlledSender(bufferSize: 1024) { Full = true };
            var session = (RespServerSession)server.Provider.GetSession(WireFormat.ASCII, sender);
            try
            {
                var consume = Consume(session, input);

                // Parked ahead of the PING, with both echoes already handed to the sender
                ClassicAssert.IsFalse(consume.IsCompleted, "the session ran the PING although the sender was full");
                ClassicAssert.AreEqual(1, sender.Registrations);
                ClassicAssert.AreEqual(echoReply + echoReply, sender.Sent);

                // The wake arrives, but the sender is full again
                sender.SignalCapacity();
                await WaitUntilAsync(() => (sender.Registrations == 2 && IsSuspended(session)) || sender.Closed || consume.IsCompleted,
                    "the session parked a second time");

                ClassicAssert.IsFalse(sender.Closed, "the session was torn down by the second wait");
                ClassicAssert.IsFalse(consume.IsCompleted, "the session ran on although the sender was still full");
                ClassicAssert.AreEqual(2, sender.Registrations);
                ClassicAssert.AreEqual(echoReply + echoReply, sender.Sent);

                // Room at last
                sender.Full = false;
                sender.SignalCapacity();

                var consumed = await consume.AsTask().WaitAsync(Bound);
                ClassicAssert.AreEqual(input.Length, consumed);
                ClassicAssert.AreEqual(echoReply + echoReply + "+PONG\r\n", sender.Sent);
            }
            finally
            {
                session.Dispose();
            }
        }

        /// <summary>
        /// A transaction holds its key locks until it ends, so a sender that fills while it runs does not park the
        /// session inside it: the transaction runs to its end, and the session parks before the next command.
        /// </summary>
        [Test]
        public async Task ATransactionRunsToItsEndBeforeTheSessionParksForSendCapacity()
        {
            // Two of these replies cannot share the sender's buffer, so the transaction's second GET sends the
            // first and leaves the sender full with the SET still to run.
            const int PayloadLength = 700;
            var payload = new string('x', PayloadLength);
            var getReply = $"${PayloadLength}\r\n{payload}\r\n";

            var sender = new ControlledSender(bufferSize: 1024);
            var session = (RespServerSession)server.Provider.GetSession(WireFormat.ASCII, sender);
            try
            {
                var set = RespPublishSuspensionTests.Pinned(RespPublishSuspensionTests.Frame("SET", "big", payload));
                ClassicAssert.AreEqual(set.Length, await Consume(session, set));
                ClassicAssert.AreEqual("+OK\r\n", sender.Sent);

                sender.Full = true;
                var input = RespPublishSuspensionTests.Pinned(
                    RespPublishSuspensionTests.Frame("MULTI"),
                    RespPublishSuspensionTests.Frame("GET", "big"),
                    RespPublishSuspensionTests.Frame("GET", "big"),
                    RespPublishSuspensionTests.Frame("SET", "k", "v"),
                    RespPublishSuspensionTests.Frame("EXEC"),
                    RespPublishSuspensionTests.Frame("PING"));
                var consume = Consume(session, input);

                ClassicAssert.IsFalse(consume.IsCompleted, "the session ran the PING although the sender was full");
                ClassicAssert.IsTrue(IsSuspended(session));
                ClassicAssert.AreEqual(TxnState.None, session.txnManager.state, "the session parked inside its transaction");
                ClassicAssert.AreEqual(1, sender.Registrations);

                sender.Full = false;
                sender.SignalCapacity();

                ClassicAssert.AreEqual(input.Length, await consume.AsTask().WaitAsync(Bound));
                ClassicAssert.AreEqual("+OK\r\n+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n*3\r\n" + getReply + getReply + "+OK\r\n+PONG\r\n",
                    sender.Sent);
            }
            finally
            {
                session.Dispose();
            }
        }

        static unsafe ValueTask<int> Consume(RespServerSession session, byte[] pinnedInput)
            => session.TryConsumeMessagesAsync((byte*)Unsafe.AsPointer(ref pinnedInput[0]), pinnedInput.Length);

        static bool IsSuspended(RespServerSession session) => (bool)SuspendedField.GetValue(session);

        static async Task WaitUntilAsync(Func<bool> condition, string what)
        {
            var stopwatch = Stopwatch.StartNew();
            while (!condition())
            {
                if (stopwatch.Elapsed > Bound)
                    Assert.Fail($"Timed out waiting until {what}.");
                await Task.Delay(1);
            }
        }
    }
}