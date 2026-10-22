// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers the send side of a connection as the thread that handles its network callbacks uses it: sends that
    /// do not wait for capacity, a close that does not wait for the peer, and the throttle that blocking senders
    /// still depend on.
    /// </summary>
    /// <remarks>
    /// Each test runs a real loopback connection whose peer does not read until the test says so. Both ends have
    /// small kernel buffers and every send is larger than they can absorb, so a send stays in flight for as long
    /// as the peer leaves it unread, which makes capacity something the test controls.
    /// </remarks>
    [TestFixture]
    public class GarnetTcpNetworkSenderTests : TestBase
    {
        /// <summary>Size of one send. It exceeds what the kernel buffers of either end can absorb.</summary>
        const int Chunk = 1 << 16;

        /// <summary>Kernel buffer size requested on both ends of the connection.</summary>
        const int KernelBuffer = 4 * 1024;

        /// <summary>How long a test waits for something that must happen, and for the peer to receive.</summary>
        static readonly TimeSpan Bound = TimeSpan.FromSeconds(15);

        SenderRig rig;

        [TearDown]
        public void TearDown()
        {
            rig?.Dispose();
            rig = null;
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// A send that does not wait for capacity returns however far past the limit the sends outstanding are,
        /// which is what lets the thread that issued it go back to serving other connections.
        /// </summary>
        [Test]
        public void SendsThatDoNotWaitReturnWhileThePeerIsNotReading()
        {
            const int Sends = 32;
            rig = new SenderRig(throttleMax: 2);

            var flood = rig.Track(Task.Run(() =>
            {
                for (var i = 0; i < Sends; i++)
                    _ = rig.Send(Chunk, noWait: true);
            }));

            ClassicAssert.IsTrue(flood.Wait(Bound),
                $"{Sends} sends against a limit of 2 did not return while the peer was not reading, so a send is waiting for capacity");
            ClassicAssert.IsTrue(rig.Sender.IsSendSaturated);

            rig.ReadAndVerify((long)Sends * Chunk);
            ClassicAssert.IsTrue(SpinWait.SpinUntil(() => !rig.Sender.IsSendSaturated, Bound),
                "capacity did not return after the peer read everything");
        }

        /// <summary>
        /// A sender with fewer sends in flight than the limit has capacity. Reporting saturation early would park
        /// a session that could have kept going.
        /// </summary>
        [Test]
        public void SaturationNeedsTheFullLimitInFlight()
        {
            const int Limit = 4;
            rig = new SenderRig(throttleMax: Limit);

            ClassicAssert.GreaterOrEqual(rig.Saturate(), (long)Limit * Chunk,
                "the sender reported saturation with fewer sends in flight than its limit");
        }

        /// <summary>
        /// A waiter registered while the sender is saturated is called when capacity returns, and only once, since
        /// a session that is woken twice would resume twice.
        /// </summary>
        [Test]
        public void AWaiterIsCalledOnceWhenCapacityReturns()
        {
            rig = new SenderRig(throttleMax: 2);
            var sent = rig.Saturate();
            using var waiter = new CountingWaiter();

            ClassicAssert.IsTrue(rig.Sender.TryRegisterSendCapacityWaiter(waiter),
                "a saturated sender did not take the waiter");
            ClassicAssert.AreEqual(0, waiter.Calls, "the waiter was called before any capacity returned");

            rig.ReadAndVerify(sent);
            ClassicAssert.IsTrue(waiter.Wait(Bound), "the waiter was not told that capacity returned");

            // A graceful close finishes only once every send has completed, so the end of the stream means each
            // completion has already had its chance to call the waiter
            rig.GracefulClose();
            rig.ExpectEndOfStream();
            ClassicAssert.AreEqual(1, waiter.Calls, "the waiter was called more than once");
        }

        /// <summary>
        /// A registration that finds capacity already available is withdrawn, so a session that carries on
        /// is not woken later for a wait it never started.
        /// </summary>
        [Test]
        public void ARegistrationThatFindsCapacityIsWithdrawn()
        {
            rig = new SenderRig(throttleMax: 2);
            using var waiter = new CountingWaiter();

            ClassicAssert.IsFalse(rig.Sender.TryRegisterSendCapacityWaiter(waiter),
                "an idle sender has capacity, so the waiter must not be registered");

            var sent = rig.Saturate();
            rig.ReadAndVerify(sent);
            rig.GracefulClose();
            rig.ExpectEndOfStream();

            ClassicAssert.AreEqual(0, waiter.Calls, "a waiter that was told not to wait was called when capacity returned");
        }

        /// <summary>
        /// A connection serves more than one waiter: the session that owns it, and each publisher waiting to
        /// deliver to it. The return of capacity calls every one of them, once.
        /// </summary>
        [Test]
        public void EveryWaiterIsCalledOnceWhenCapacityReturns()
        {
            // More waiters than sends in flight, so that waking one waiter per completion leaves some parked
            const int Waiters = 5;
            rig = new SenderRig(throttleMax: 2);
            var sent = rig.Saturate();
            var waiters = new CountingWaiter[Waiters];
            try
            {
                for (var i = 0; i < Waiters; i++)
                {
                    waiters[i] = new CountingWaiter();
                    ClassicAssert.IsTrue(rig.Sender.TryRegisterSendCapacityWaiter(waiters[i]),
                        $"a saturated sender did not take waiter {i}");
                }

                rig.ReadAndVerify(sent);
                for (var i = 0; i < Waiters; i++)
                    ClassicAssert.IsTrue(waiters[i].Wait(Bound), $"waiter {i} was not told that capacity returned");

                rig.GracefulClose();
                rig.ExpectEndOfStream();
                for (var i = 0; i < Waiters; i++)
                    ClassicAssert.AreEqual(1, waiters[i].Calls, $"waiter {i} was not called exactly once");
            }
            finally
            {
                foreach (var waiter in waiters)
                    waiter?.Dispose();
            }
        }

        /// <summary>
        /// A waiter that withdraws is not called, and withdrawing says whether the waiter was still registered:
        /// a publisher that stops waiting learns from it that a call is already on its way.
        /// </summary>
        [Test]
        public void AWithdrawnWaiterIsNotCalled()
        {
            rig = new SenderRig(throttleMax: 2);
            var sent = rig.Saturate();
            using var withdrawn = new CountingWaiter();
            using var kept = new CountingWaiter();
            ClassicAssert.IsTrue(rig.Sender.TryRegisterSendCapacityWaiter(withdrawn));
            ClassicAssert.IsTrue(rig.Sender.TryRegisterSendCapacityWaiter(kept));

            ClassicAssert.IsTrue(rig.Sender.WithdrawSendCapacityWaiter(withdrawn), "a registered waiter could not be withdrawn");
            ClassicAssert.IsFalse(rig.Sender.WithdrawSendCapacityWaiter(withdrawn), "a waiter was withdrawn twice");

            rig.ReadAndVerify(sent);
            ClassicAssert.IsTrue(kept.Wait(Bound), "the waiter that stayed registered was not called");
            ClassicAssert.IsFalse(rig.Sender.WithdrawSendCapacityWaiter(kept),
                "a waiter that had been called was still registered, so withdrawing it would hide the call that is on its way");

            rig.GracefulClose();
            rig.ExpectEndOfStream();
            ClassicAssert.AreEqual(0, withdrawn.Calls, "a withdrawn waiter was called");
            ClassicAssert.AreEqual(1, kept.Calls);
        }

        /// <summary>
        /// A registration made as the last send completes is told to wait and then called, or told that
        /// capacity is available and then never called. Neither stays parked, and neither is both.
        /// </summary>
        [Test]
        public void ARegistrationRacingTheReturnOfCapacityIsCalledOnceOrNotAtAll()
        {
            const int Rounds = 160;
            var told = 0;
            for (var round = 0; round < Rounds; round++)
            {
                using var connection = new SenderRig(throttleMax: 2);
                var sent = connection.Saturate();
                using var waiter = new CountingWaiter();

                var reader = Task.Run(() => connection.ReadAndVerify(sent));

                // Lands the registration at a different point of the read in each round
                Thread.SpinWait(4 << (round % 16));
                var mustWait = connection.Sender.TryRegisterSendCapacityWaiter(waiter);
                reader.Wait();

                // A graceful close finishes only once every send has completed, so by the end of the stream each
                // completion has had its chance to call the waiter
                connection.GracefulClose();
                connection.ExpectEndOfStream();

                if (mustWait)
                {
                    told++;
                    ClassicAssert.IsTrue(waiter.Wait(Bound), $"round {round}: a waiter told to wait was never called");
                }

                ClassicAssert.AreEqual(mustWait ? 1 : 0, waiter.Calls,
                    $"round {round}: a waiter that was {(mustWait ? "told to wait" : "told that capacity was available")} was called {waiter.Calls} times");
            }

            Assert.That(told, Is.GreaterThan(0).And.LessThan(Rounds),
                "the registrations all landed on one side of the return of capacity, so the race was not exercised");
        }


        /// <summary>
        /// Sends fail when the connection does, and each failure still gives its slot back and calls the waiter,
        /// since a session parked on a connection that is gone has no other way to find out.
        /// </summary>
        [Test]
        public void ADeadPeerStillReturnsCapacity()
        {
            rig = new SenderRig(throttleMax: 2);
            _ = rig.Saturate();
            using var waiter = new CountingWaiter();
            ClassicAssert.IsTrue(rig.Sender.TryRegisterSendCapacityWaiter(waiter));

            rig.ResetPeer();

            ClassicAssert.IsTrue(waiter.Wait(Bound),
                "the sends failed with the connection but the waiter was never told, so its session would stay parked");
        }

        /// <summary>
        /// The sends that held the capacity end after this side has closed the sender, and the waiter is still
        /// called, since a session parked on a sender that is closing has no other way to find out.
        /// </summary>
        [TestCase(true, Description = "graceful close")]
        [TestCase(false, Description = "hard close")]
        public void ACloseStillReturnsCapacity(bool graceful)
        {
            rig = new SenderRig(throttleMax: 2);
            var sent = rig.Saturate();
            using var waiter = new CountingWaiter();
            ClassicAssert.IsTrue(rig.Sender.TryRegisterSendCapacityWaiter(waiter));

            if (graceful)
            {
                // The sends in flight finish as the peer reads them
                rig.GracefulClose();
                rig.ReadAndVerify(sent);
                rig.ExpectEndOfStream();
            }
            else
            {
                rig.Sender.Dispose();
            }

            ClassicAssert.IsTrue(waiter.Wait(Bound),
                "the sends ended after the close but the waiter was never told, so its session would stay parked");
        }

        /// <summary>
        /// A graceful close is called from the thread that handles the connection's callbacks, so it returns
        /// at once and the close finishes when the last send in flight does.
        /// </summary>
        [Test]
        public void ClosingNeverWaitsForAPeerThatIsNotReading()
        {
            rig = new SenderRig(throttleMax: 2);
            var sent = rig.Saturate();

            rig.GracefulClose();

            // What was already in flight is still delivered, and the connection ends once it has been
            rig.ReadAndVerify(sent);
            rig.ExpectEndOfStream();
        }

        /// <summary>
        /// A graceful close shuts down the send side as it finishes, and it finishes on a send completion, where an
        /// exception ends the process. A socket that the network handler has already disposed must not stop it.
        /// </summary>
        [Test]
        public void AGracefulCloseFinishesWhenTheHandlerHasDisposedTheSocket()
        {
            rig = new SenderRig(throttleMax: 2);
            rig.DisposeSocket();

            Assert.DoesNotThrow(() => rig.Sender.DisposeNetworkSender(true));
            ClassicAssert.IsTrue(rig.Sender.Closed, "the sender did not finish closing");
            rig.AssertEveryBufferReturned();
        }

        /// <summary>
        /// A shutdown that fails, here because the socket is not connected, must not stop the close either
        /// </summary>
        [Test]
        public void AGracefulCloseFinishesWhenTheShutdownFails()
        {
            using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            var settings = new NetworkBufferSettings(sendBufferSize: Chunk, initialReceiveBufferSize: Chunk);
            var pool = settings.CreateBufferPool(ownerType: PoolOwnerType.ServerNetwork);
            var sender = new ProbedSender(socket, settings, pool, throttleMax: 2);

            Assert.DoesNotThrow(() => sender.DisposeNetworkSender(true));
            ClassicAssert.IsTrue(sender.Closed, "the sender did not finish closing");
            _ = Assert.Throws<ObjectDisposedException>(() => socket.Shutdown(SocketShutdown.Both), "the close left the socket open");
            ClassicAssert.IsTrue(Task.Run(pool.Dispose).Wait(Bound), "the sender did not return its buffers to the pool");
        }

        /// <summary>
        /// A send that starts after the close has begun is dropped. It must not reach the peer, and it must give
        /// back the slot and the buffer it took, or the close never finishes.
        /// </summary>
        [TestCase(true, Description = "send that does not wait")]
        [TestCase(false, Description = "blocking send")]
        public void SendsAfterTheCloseBeginsAreDropped(bool noWait)
        {
            rig = new SenderRig(throttleMax: 2);
            var sent = rig.Saturate();

            rig.GracefulClose();
            var late = rig.Track(Task.Run(() => rig.Send(Chunk, noWait)));
            ClassicAssert.IsTrue(late.Wait(Bound), "A send after the close began did not return");

            rig.ReadAndVerify(sent);
            rig.ExpectEndOfStream();
            rig.AssertEveryBufferReturned();
        }

        /// <summary>
        /// Senders queued on the throttle when a close begins are each released and each give their slot back,
        /// so none is left waiting and the close finishes.
        /// </summary>
        [TestCase(true, Description = "graceful close")]
        [TestCase(false, Description = "hard close")]
        public void ACloseBehindBlockedSendersStrandsNone(bool graceful)
        {
            rig = new SenderRig(throttleMax: 1);

            // The first send to stay in flight takes the only slot
            var sent = rig.SendUntilOneStaysInFlight();

            // One blocked sender takes the sender lock, as a session's send does, and the other is a caller
            // of Throttle, as the replication senders are
            var blockedSend = rig.Track(Task.Run(() => rig.Send(Chunk, noWait: false)));
            var blockedThrottle = rig.Track(Task.Run(() => rig.Sender.Throttle()));
            ClassicAssert.IsTrue(SpinWait.SpinUntil(() => rig.Sender.BlockingInFlight == 3, Bound),
                "the second and third senders did not queue on the throttle");

            if (graceful)
                rig.GracefulClose();
            else
                rig.Sender.Dispose();

            if (graceful)
            {
                // Only the sends that went out before the close reach the peer: the queued ones are dropped when they wake
                rig.ReadAndVerify(sent);
                rig.ExpectEndOfStream();
            }

            ClassicAssert.IsTrue(Task.WaitAll([blockedSend, blockedThrottle], Bound),
                "a sender queued on the throttle was never released after the close");
            ClassicAssert.AreEqual(0, rig.Sender.BlockingInFlight);
            rig.AssertEveryBufferReturned();
        }

        /// <summary>
        /// A throttle of zero is a valid setting. It must not leave a blocking send waiting on a limit nothing
        /// can ever satisfy, or report an idle sender as saturated, which would park a session for good.
        /// </summary>
        [Test]
        public void AThrottleOfZeroStillSends()
        {
            rig = new SenderRig(throttleMax: 0);

            ClassicAssert.IsFalse(rig.Sender.IsSendSaturated, "a sender with nothing in flight is saturated");

            var send = rig.Track(Task.Run(() => rig.Send(1024, noWait: false)));
            ClassicAssert.IsTrue(send.Wait(Bound), "a blocking send waited on a throttle that has no capacity");
            rig.ReadAndVerify(1024);
        }

        /// <summary>
        /// Sends that do not wait are counted apart from the blocking ones. Releasing the throttle for them as
        /// well would leave permits that no waiter asked for, and blocking senders would stop being throttled.
        /// </summary>
        [Test]
        public void SendsThatDoNotWaitNeverMintAThrottlePermit()
        {
            const int Sends = 32;
            rig = new SenderRig(throttleMax: 2);

            var flood = rig.Track(Task.Run(() =>
            {
                for (var i = 0; i < Sends; i++)
                    _ = rig.Send(Chunk, noWait: true);
            }));
            ClassicAssert.IsTrue(flood.Wait(Bound), "A send that does not wait did not return while the peer was not reading");

            rig.ReadAndVerify((long)Sends * Chunk);
            rig.GracefulClose();
            rig.ExpectEndOfStream();

            ClassicAssert.AreEqual(0, rig.Sender.Permits, "completing sends that did not wait released the throttle");
            ClassicAssert.AreEqual(0, rig.Sender.BlockingInFlight);
        }

        /// <summary>
        /// Blocking senders, which replication and the clients depend on, still stop once the sends in flight
        /// reach the limit and carry on when the peer reads.
        /// </summary>
        [Test]
        public void BlockingSendsStillStopAtTheThrottle()
        {
            const int Sends = 16;
            rig = new SenderRig(throttleMax: 2);

            var flood = rig.Track(Task.Run(() =>
            {
                for (var i = 0; i < Sends; i++)
                    _ = rig.Send(Chunk, noWait: false);
            }));

            // The third send in flight holds a slot beyond the limit, so it is waiting for one
            ClassicAssert.IsTrue(SpinWait.SpinUntil(() => rig.Sender.BlockingInFlight >= 3, Bound),
                "the sends never reached the limit, so the peer did not leave them in flight");
            Thread.Sleep(250);
            ClassicAssert.IsFalse(flood.IsCompleted, "blocking sends did not stop at the throttle while the peer was not reading");

            rig.ReadAndVerify((long)Sends * Chunk);
            ClassicAssert.IsTrue(flood.Wait(Bound), "the blocked sends did not carry on once the peer read");
        }

        sealed class CountingWaiter : ISendCapacityWaiter, IDisposable
        {
            readonly ManualResetEventSlim called = new(false);
            int calls;

            internal int Calls => Volatile.Read(ref calls);

            internal bool Wait(TimeSpan timeout) => called.Wait(timeout);

            public void OnSendCapacity()
            {
                _ = Interlocked.Increment(ref calls);
                called.Set();
            }

            public void Dispose() => called.Dispose();
        }

        /// <summary>
        /// Exposes the throttle the tests account against.
        /// </summary>
        sealed class ProbedSender : GarnetTcpNetworkSender
        {
            internal ProbedSender(Socket socket, NetworkBufferSettings settings, LimitedFixedBufferPool pool, int throttleMax)
                : base(socket, settings, pool, throttleMax)
            {
            }

            /// <summary>Permits released on the throttle that no blocked sender has taken</summary>
            internal int Permits => throttle.CurrentCount;

            /// <summary>Blocking sends, and callers of <c>Throttle</c>, that hold a slot or wait for one</summary>
            internal int BlockingInFlight => Volatile.Read(ref throttleCount);

            /// <summary>Whether the sender has finished closing</summary>
            internal bool Closed => IsClosed;
        }

        /// <summary>
        /// A connected pair of sockets with a sender on one end and the test as the reader on the other. What the
        /// sender writes is a known sequence, so the reader can tell reordered, repeated and missing bytes apart.
        /// </summary>
        sealed unsafe class SenderRig : IDisposable
        {
            /// <summary>Sends issued before concluding that the platform absorbs them instead of holding them in flight</summary>
            const int MaxSaturatingSends = 256;

            /// <summary>How long a send in flight has to stay there to count as held, rather than on its way to completing</summary>
            static readonly TimeSpan InFlightSettle = TimeSpan.FromMilliseconds(100);

            readonly NetworkBufferSettings settings;
            readonly LimitedFixedBufferPool pool;
            readonly Socket accepted;
            readonly Socket peer;
            readonly byte[] readBuffer = new byte[Chunk];
            readonly List<Task> tasks = [];
            long produced;
            long consumed;

            internal ProbedSender Sender { get; }

            internal SenderRig(int throttleMax)
            {
                using var listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
                listener.Listen(1);

                // The receive buffer is set before connecting so that the window the peer advertises is the small one
                peer = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
                {
                    ReceiveBufferSize = KernelBuffer,
                    ReceiveTimeout = (int)Bound.TotalMilliseconds,
                };
                peer.Connect(listener.LocalEndPoint);

                accepted = listener.Accept();
                accepted.SendBufferSize = KernelBuffer;
                accepted.NoDelay = true;

                settings = new NetworkBufferSettings(sendBufferSize: Chunk, initialReceiveBufferSize: Chunk);
                pool = settings.CreateBufferPool(ownerType: PoolOwnerType.ServerNetwork);
                Sender = new ProbedSender(accepted, settings, pool, throttleMax);

                // A connection always has a receive pending, and closing a socket under one resets the connection
                // unless the send side was shut down first
                _ = Track(accepted.ReceiveAsync(new Memory<byte>(new byte[1]), SocketFlags.None).AsTask());
            }

            static byte PatternAt(long position) => (byte)(position % 251);

            internal Task Track(Task task)
            {
                lock (tasks)
                    tasks.Add(task);
                return task;
            }

            /// <summary>
            /// Sends the next <paramref name="size"/> bytes of the sequence, the way a session does: under the
            /// sender lock, with the lock held across a send that waits.
            /// </summary>
            /// <returns>What the send reports, which is whether the sender is saturated for a send that does not wait</returns>
            internal bool Send(int size, bool noWait)
            {
                Sender.EnterAndGetResponseObject(out var head, out _);
                try
                {
                    var span = new Span<byte>(head, size);
                    for (var i = 0; i < size; i++)
                        span[i] = PatternAt(produced + i);
                    produced += size;

                    return noWait ? Sender.SendResponseNoWait(0, size) : Sender.SendResponse(0, size);
                }
                finally
                {
                    Sender.ExitAndReturnResponseObject();
                }
            }

            /// <summary>
            /// Sends without waiting until the sender reports that it is saturated
            /// </summary>
            /// <returns>The number of bytes sent</returns>
            internal long Saturate()
            {
                var saturating = Task.Run(() =>
                {
                    var before = produced;
                    for (var i = 0; i < MaxSaturatingSends; i++)
                    {
                        if (Send(Chunk, noWait: true))
                            return produced - before;
                    }

                    return -1L;
                });
                _ = Track(saturating);

                ClassicAssert.IsTrue(saturating.Wait(Bound), "A send that does not wait did not return while the peer was not reading");

                var sent = saturating.Result;
                if (sent < 0)
                    Assert.Ignore("The platform absorbs sends instead of holding them in flight, so capacity cannot be made to run out.");
                return sent;
            }

            /// <summary>
            /// Issues blocking sends until one stays in flight. A platform can take a whole send into its buffer while
            /// the buffer has room, so the first sends can complete at once.
            /// </summary>
            /// <returns>The number of bytes sent</returns>
            internal long SendUntilOneStaysInFlight()
            {
                var before = produced;
                for (var i = 0; i < MaxSaturatingSends; i++)
                {
                    _ = Send(Chunk, noWait: false);
                    if (!SpinWait.SpinUntil(() => Sender.BlockingInFlight == 0, InFlightSettle))
                        return produced - before;
                }

                Assert.Ignore("The platform absorbs sends instead of holding them in flight, so capacity cannot be made to run out.");
                return -1;
            }

            /// <summary>
            /// Reads <paramref name="count"/> bytes at the peer and checks that they are the next ones of the sequence
            /// </summary>
            internal void ReadAndVerify(long count)
            {
                while (count > 0)
                {
                    int n;
                    try
                    {
                        n = peer.Receive(readBuffer, 0, (int)Math.Min(readBuffer.Length, count), SocketFlags.None);
                    }
                    catch (SocketException e) when (e.SocketErrorCode == SocketError.TimedOut)
                    {
                        Assert.Fail($"The peer timed out with {count} bytes still to read after {consumed} had arrived");
                        return;
                    }
                    catch (SocketException e) when (e.SocketErrorCode == SocketError.ConnectionReset)
                    {
                        Assert.Fail($"The connection was reset with {count} bytes still to read after {consumed} had arrived");
                        return;
                    }

                    ClassicAssert.Greater(n, 0, $"The connection ended with {count} bytes still to read after {consumed} had arrived");
                    for (var i = 0; i < n; i++)
                    {
                        if (readBuffer[i] != PatternAt(consumed + i))
                            Assert.Fail($"Byte {consumed + i} of the stream is {readBuffer[i]}, expected {PatternAt(consumed + i)}");
                    }

                    consumed += n;
                    count -= n;
                }
            }

            /// <summary>
            /// Expects the connection to end with nothing further to read
            /// </summary>
            internal void ExpectEndOfStream()
            {
                int n;
                try
                {
                    n = peer.Receive(readBuffer);
                }
                catch (SocketException e) when (e.SocketErrorCode == SocketError.TimedOut)
                {
                    Assert.Fail("The connection did not end, so the sender never finished closing");
                    return;
                }
                catch (SocketException e) when (e.SocketErrorCode == SocketError.ConnectionReset)
                {
                    Assert.Fail("The connection was reset instead of ending");
                    return;
                }

                ClassicAssert.AreEqual(0, n, "The peer received bytes that were not sent before the close");
            }

            /// <summary>
            /// Disposes the sender's socket the way the network handler does when its receive fails
            /// </summary>
            internal void DisposeSocket() => accepted.Dispose();

            /// <summary>
            /// Begins a graceful close from another thread, so that a close which waits for the peer fails the
            /// test instead of hanging it
            /// </summary>
            internal void GracefulClose()
            {
                var close = Track(Task.Run(() => Sender.DisposeNetworkSender(true)));
                ClassicAssert.IsTrue(close.Wait(Bound),
                    "A graceful close waited for the peer, which holds the thread that called it");
            }

            /// <summary>
            /// Resets the connection from the peer, which fails what the sender has in flight
            /// </summary>
            internal void ResetPeer()
            {
                peer.LingerState = new LingerOption(true, 0);
                peer.Dispose();
            }

            /// <summary>
            /// Closes the sender and waits for every buffer it took from the pool to come back. The pool waits
            /// for them indefinitely, so a buffer that is not returned shows up as a wait that does not end.
            /// </summary>
            internal void AssertEveryBufferReturned()
            {
                Sender.Dispose();
                ClassicAssert.IsTrue(Task.Run(pool.Dispose).Wait(Bound), "A send did not return its buffer to the pool");
            }

            public void Dispose()
            {
                // Failing what is in flight also releases any thread still waiting on the throttle
                Sender.Dispose();
                peer.Dispose();

                Task[] pending;
                lock (tasks)
                    pending = [.. tasks];

                try
                {
                    _ = Task.WaitAll(pending, Bound);
                }
                catch (AggregateException)
                {
                    // Closing the sender fails the sends of a test that already failed
                }
            }
        }
    }
}