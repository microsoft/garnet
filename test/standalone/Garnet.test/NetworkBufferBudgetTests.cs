// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers the process-wide network buffer budget: the sizing formula, the hysteresis band that keeps it
    /// from oscillating between size classes, the conservation of the live buffer count that the formula
    /// divides by, and the fact that the published target is inert at default settings.
    /// </summary>
    /// <remarks>
    /// The sizing tests pin the signal on its own, so that a drift bug in the live buffer count is
    /// observable separately from a clamping bug at an allocation site. The end-to-end tests then pin what
    /// the allocation sites and the shrink policy actually do with it.
    /// </remarks>
    [TestFixture]
    public class NetworkBufferBudgetTests : TestBase
    {
        const int Ceiling = 1 << 17;      // 128 KB, the shipped --network-buffer-size
        const int ReceiveFloor = 1 << 14; // 16 KB
        const int SendFloor = 1 << 16;    // 64 KB

        GarnetServer server;

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.OnTearDown();
        }

        static NetworkBufferBudget Budget(long budgetBytes = 1L << 30)
            => new(budgetBytes, Ceiling, ReceiveFloor, SendFloor);

        #region sizing formula

        /// <summary>
        /// The whole point of the design: while the quotient is at or above the configured size, the target
        /// is the configured size and adaptation is arithmetically inert.
        /// </summary>
        [Test]
        public void TargetPinsAtCeilingWhileBudgetIsSlack()
        {
            var budget = Budget();

            // 1 GB / 128 KB = 8,192 buffers before the quotient reaches the configured size.
            ClassicAssert.AreEqual(Ceiling, budget.TargetForCount(1));
            ClassicAssert.AreEqual(Ceiling, budget.TargetForCount(100));
            ClassicAssert.AreEqual(Ceiling, budget.TargetForCount(8192));
        }

        /// <summary>
        /// Past the threshold the target steps down through pool size classes, never to something the pool
        /// could not recycle.
        /// </summary>
        [Test]
        public void TargetStepsDownThroughPowersOfTwo()
        {
            var budget = Budget();

            ClassicAssert.AreEqual(1 << 16, budget.TargetForCount(8193));   // just past the threshold
            ClassicAssert.AreEqual(1 << 16, budget.TargetForCount(16384));
            ClassicAssert.AreEqual(1 << 15, budget.TargetForCount(32768));
            ClassicAssert.AreEqual(1 << 14, budget.TargetForCount(65536));
        }

        /// <summary>
        /// The incident shape. 9,212 live buffers against the default budget must land one step below the
        /// configured size, which is the halving the design predicts.
        /// </summary>
        [Test]
        public void IncidentShapeLandsOneStepBelowConfiguredSize()
        {
            ClassicAssert.AreEqual(1 << 16, Budget().TargetForCount(9212));
        }

        /// <summary>
        /// The target must never fall below the floor no matter how extreme the count, because a buffer
        /// below the pool's smallest size class can never be recycled.
        /// </summary>
        [Test]
        public void TargetNeverFallsBelowTheFloor()
        {
            var budget = Budget();
            ClassicAssert.AreEqual(ReceiveFloor, budget.TargetForCount(long.MaxValue));
            ClassicAssert.AreEqual(ReceiveFloor, budget.TargetForCount(1_000_000_000));
        }

        /// <summary>
        /// A zero budget is the production escape hatch and must restore the configured size unconditionally.
        /// </summary>
        [Test]
        public void DisabledBudgetAlwaysReportsTheConfiguredSize()
        {
            var budget = Budget(budgetBytes: 0);
            ClassicAssert.IsFalse(budget.IsEnabled);
            ClassicAssert.AreEqual(Ceiling, budget.TargetForCount(1));
            ClassicAssert.AreEqual(Ceiling, budget.TargetForCount(1_000_000));
            ClassicAssert.AreEqual(Ceiling, budget.TargetBufferSize);
        }

        /// <summary>
        /// Send buffers carry a higher floor than receive buffers, and neither floor may raise the base size
        /// above what was configured.
        /// </summary>
        [Test]
        public void SendAndReceiveFloorsAreSeparateAndNeverExceedTheCeiling()
        {
            var budget = Budget();
            for (var i = 0; i < 1_000_000; i++)
                budget.OnBufferAcquired();
            budget.Recompute();

            ClassicAssert.AreEqual(ReceiveFloor, budget.TargetReceiveBufferSize);
            ClassicAssert.AreEqual(SendFloor, budget.TargetSendBufferSize);

            // A floor configured above the buffer size must not raise the base size; adaptation only lowers.
            var tight = new NetworkBufferBudget(1L << 30, 1 << 14, 1 << 16, 1 << 18);
            ClassicAssert.AreEqual(1 << 14, tight.TargetReceiveBufferSize);
            ClassicAssert.AreEqual(1 << 14, tight.TargetSendBufferSize);
        }

        #endregion

        #region hysteresis

        /// <summary>
        /// The actuation step is a factor of two, so the deadband must be at least a factor of two or the
        /// target limit-cycles between adjacent size classes. Walking the count up one buffer at a time must
        /// produce a monotonically non-increasing target with no re-expansion.
        /// </summary>
        [Test]
        public void TargetNeverOscillatesWhileTheCountGrows()
        {
            var budget = Budget();
            var previous = budget.TargetBufferSize;
            var increases = 0;

            for (var i = 0; i < 200_000; i++)
            {
                budget.OnBufferAcquired();
                budget.Recompute();
                var current = budget.TargetBufferSize;
                if (current > previous) increases++;
                previous = current;
            }

            ClassicAssert.AreEqual(0, increases, "target grew while the live buffer count was only rising");
            ClassicAssert.AreEqual(ReceiveFloor, previous);
        }

        /// <summary>
        /// Growth requires the quotient to reach twice the published target, not merely to exceed it. Inside
        /// that band the target must be pinned, which is what stops a workload sitting near a size-class
        /// boundary from flapping.
        /// </summary>
        [Test]
        public void GrowthRequiresTwiceThePublishedTarget()
        {
            // 64 buffers of budget each, so the quotient starts at the ceiling and the target with it.
            var budget = new NetworkBufferBudget(Ceiling * 64L, Ceiling, ReceiveFloor, SendFloor);
            for (var i = 0; i < 256; i++)
                budget.OnBufferAcquired();
            budget.Recompute();

            // 64 * 128 KB / 256 = 32 KB.
            ClassicAssert.AreEqual(1 << 15, budget.TargetBufferSize);

            // Release back to 129 buffers, a quotient of ~63.5 KB: above the published 32 KB, but short of
            // twice it, so the target must hold.
            for (var i = 0; i < 127; i++)
                budget.OnBufferReleased();
            budget.Recompute();
            ClassicAssert.AreEqual(1 << 15, budget.TargetBufferSize, "target grew inside the deadband");

            // One more release takes the quotient to exactly 64 KB, twice the target, which permits a step.
            budget.OnBufferReleased();
            budget.Recompute();
            ClassicAssert.AreEqual(1 << 16, budget.TargetBufferSize);
        }

        /// <summary>
        /// Draining every buffer must restore the configured size exactly, not leave the target stuck low.
        /// </summary>
        [Test]
        public void TargetReturnsToTheCeilingWhenConnectionsDrain()
        {
            var budget = Budget();
            for (var i = 0; i < 100_000; i++)
                budget.OnBufferAcquired();
            budget.Recompute();
            ClassicAssert.AreEqual(ReceiveFloor, budget.TargetBufferSize);

            for (var i = 0; i < 100_000; i++)
                budget.OnBufferReleased();
            budget.Recompute();

            ClassicAssert.AreEqual(0, budget.LiveBufferCount);
            ClassicAssert.AreEqual(Ceiling, budget.TargetBufferSize);
        }

        #endregion

        #region conservation of the live buffer count

        /// <summary>
        /// The target is budget divided by this count, so a count that ratchets up does not degrade
        /// gracefully -- it collapses the target to the floor permanently. Every path out of the pool must
        /// therefore balance, including the one where a returned buffer is dropped rather than pooled.
        /// </summary>
        [Test]
        public void PoolGetAndReturnConserveTheLiveBufferCount()
        {
            var budget = Budget();
            var settings = new NetworkBufferSettings(Ceiling, Ceiling, 1 << 20);
            // A tiny idle cap, so most returns take the over-budget drop path rather than being pooled.
            using var pool = settings.CreateBufferPool(ownerType: PoolOwnerType.ServerNetwork, maxPooledBytes: Ceiling, budget: budget);

            var entries = new List<PoolEntry>();
            for (var i = 0; i < 64; i++)
                entries.Add(pool.Get(Ceiling));
            ClassicAssert.AreEqual(64, budget.LiveBufferCount);

            foreach (var entry in entries)
                entry.Dispose();
            ClassicAssert.AreEqual(0, budget.LiveBufferCount, "a return path skipped the live buffer count");

            // Out-of-bound sizes bypass the size classes entirely and must still balance.
            var oversized = pool.Get(1 << 22);
            ClassicAssert.AreEqual(1, budget.LiveBufferCount);
            oversized.Dispose();
            ClassicAssert.AreEqual(0, budget.LiveBufferCount);
        }

        /// <summary>
        /// Pools that are not connection-scaled must not touch the budget at all, or replication and
        /// migration traffic would drive live client connections toward the floor.
        /// </summary>
        [Test]
        public void PoolsWithoutABudgetDoNotAffectIt()
        {
            var budget = Budget();
            var settings = new NetworkBufferSettings(Ceiling, Ceiling, 1 << 20);
            using var unbudgeted = settings.CreateBufferPool(ownerType: PoolOwnerType.Replication);

            var entry = unbudgeted.Get(Ceiling);
            ClassicAssert.AreEqual(0, budget.LiveBufferCount);
            ClassicAssert.AreEqual(Ceiling, budget.TargetBufferSize);
            entry.Dispose();
        }

        /// <summary>
        /// The falsifying test for the clamp itself. Everything else pins the published signal; this pins that
        /// an allocation site consumes it, by measuring the bytes the server says its live connections hold.
        /// </summary>
        [Test]
        public void SmallBudgetShrinksTheBytesHeldByLiveConnections()
        {
            // Disabled budget first, so the control is today's behaviour on the same build.
            var unbudgeted = MeasureLiveBytesPerConnection("0");
            var budgeted = MeasureLiveBytesPerConnection("512k");

            // Receive buffers drop from 128 KB to their 16 KB floor and send buffers from 128 KB to their
            // higher 64 KB floor, so the per-connection total falls to roughly a third.
            ClassicAssert.Less(budgeted, unbudgeted * 0.5,
                $"live bytes per connection did not fall under budget pressure: {unbudgeted} -> {budgeted}");
        }

        /// <summary>
        /// Pressure must be transient. When a connection spike drains, the target has to climb back to the
        /// ceiling under its own steam -- nothing outside the budget calls into it. If it does not, the server
        /// stays permanently degraded: new connections start at the floor, receive buffers shrink after 8
        /// receives instead of 256, and the pool drops every over-target entry instead of pooling it.
        /// </summary>
        [Test]
        public void TargetRecoversWithoutHelpAfterASpikeDrains()
        {
            StartServer(networkBufferMemoryBudget: "512k");

            var spike = new List<Socket>();
            try
            {
                for (var i = 0; i < 60; i++)
                    spike.Add(Ping(Connect()));
                ClassicAssert.AreEqual(ReceiveFloor, StatBytes("targetBufferSize"), "the spike did not create pressure");
            }
            finally
            {
                foreach (var s in spike) s.Dispose();
            }

            // Reconnect at low concurrency. These are served from the free list the spike left behind, so a
            // recovery that only runs on the pool's allocate-miss path never happens.
            using var survivor = Connect();
            var recovered = 0L;
            for (var attempt = 0; attempt < 100; attempt++)
            {
                Ping(Connect()).Dispose();
                _ = Ping(survivor);
                recovered = StatBytes("targetBufferSize");
                if (recovered == Ceiling) break;
                Thread.Sleep(20);
            }

            TestContext.Out.WriteLine($"targetBufferSize after drain={recovered}, liveBufferCount={StatValue("liveBufferCount")}");
            ClassicAssert.AreEqual(Ceiling, recovered,
                "the target stayed at the floor after the spike drained, leaving the server permanently degraded");
        }

        /// <summary>
        /// Send and receive adapt to separate floors, so the drop rule has to compare an entry against the
        /// target for its own direction. Comparing every entry against the receive target would treat a
        /// correctly sized send buffer as over-sized and make send buffers un-poolable for as long as the
        /// budget binds -- a fresh pinned allocation for every connection, under exactly the churn the budget
        /// exists to survive.
        /// </summary>
        [Test]
        public void CorrectlySizedSendBuffersStayPoolableWhileTheBudgetIsBinding()
        {
            StartServer(networkBufferMemoryBudget: "1m");

            var pressure = new List<Socket>();
            try
            {
                for (var i = 0; i < 60; i++)
                    pressure.Add(Ping(Connect()));
                ClassicAssert.AreEqual(ReceiveFloor, StatBytes("targetBufferSize"));
                ClassicAssert.AreEqual(SendFloor, StatBytes("targetSendBufferSize"));

                var before = SocketStatBytes("pooledBytes");

                // Churn connections that are correctly sized from birth: nothing they hold is over-target.
                // Each one reuses what the last returned, so the free list settles at one connection's worth
                // rather than accumulating -- which is the point, and is also what makes the size readable.
                const int Churn = 100;
                for (var i = 0; i < Churn; i++)
                    Ping(Connect()).Dispose();

                var after = WaitForPooledBytes(SendFloor);
                TestContext.Out.WriteLine($"pooledBytes before={before}, after={after}");

                // A receive buffer at the 16 KB floor cannot reach this on its own, so the free list can only
                // hold a send-sized entry if send buffers are still being pooled.
                ClassicAssert.GreaterOrEqual(after, (long)SendFloor,
                    $"correctly sized send buffers were dropped rather than pooled under pressure " +
                    $"(before={before}, after={after})");
            }
            finally
            {
                foreach (var s in pressure) s.Dispose();
            }
        }

        /// <summary>
        /// Connection teardown is asynchronous, so the buffers a closed connection releases arrive on the free
        /// list shortly after the socket is closed.
        /// </summary>
        static long WaitForPooledBytes(long atLeast)
        {
            var pooled = 0L;
            for (var attempt = 0; attempt < 100; attempt++)
            {
                pooled = SocketStatBytes("pooledBytes");
                if (pooled >= atLeast) return pooled;
                Thread.Sleep(50);
            }
            return pooled;
        }

        /// <summary>
        /// Shrinking a connection's buffer only moves the bytes from <c>liveBytes</c> to the idle free list --
        /// still pinned, still counted against the process. While the budget is binding those over-target
        /// buffers must be dropped rather than pooled. Unpressured they are pooled as before, which the control
        /// arm pins.
        /// </summary>
        [Test]
        public void OverTargetBuffersAreNotPooledWhileTheBudgetIsBinding()
        {
            // The two arms release through different hysteresis paths, so each is given enough rounds to get
            // there: the budgeted arm converges in PressureShrinkHysteresis receives, the control arm has to
            // wait out the full idle ShrinkHysteresis. Without that the control would only be showing the
            // growth ladder's returns and the comparison would not be about the drop rule at all.
            var budgeted = MeasurePooledBytesAfterBurst("1m", rounds: 20);
            var control = MeasurePooledBytesAfterBurst("0", rounds: 300);

            TestContext.Out.WriteLine($"budgeted pooledBytes={budgeted}, control pooledBytes={control}");

            ClassicAssert.Greater(control, 8L * 1024 * 1024,
                $"the control arm did not pool the buffers it released, so there is nothing to compare against " +
                $"(pooledBytes={control})");
            ClassicAssert.Less(budgeted, control / 4,
                $"over-target buffers were still pooled while the budget was binding " +
                $"(budgeted={budgeted}, control={control})");
        }

        long MeasurePooledBytesAfterBurst(string budget, int rounds)
        {
            server?.Dispose();
            server = null;
            StartServer(networkBufferMemoryBudget: budget);

            const int Connections = 24;
            const int PayloadLength = 400 * 1024;
            var payload = new string('p', PayloadLength);

            var sockets = new List<Socket>();
            try
            {
                for (var i = 0; i < Connections; i++)
                    sockets.Add(Ping(Connect()));

                foreach (var s in sockets)
                {
                    Send(s, $"*3\r\n$3\r\nSET\r\n$3\r\nbig\r\n${PayloadLength}\r\n{payload}\r\n");
                    ClassicAssert.AreEqual("+OK\r\n", ReadExactly(s, 5));
                }

                for (var round = 0; round < rounds; round++)
                    foreach (var s in sockets)
                        _ = Ping(s);

                return SocketStatBytes("pooledBytes");
            }
            finally
            {
                foreach (var s in sockets) s.Dispose();
            }
        }

        /// <summary>
        /// Clamping the allocation size only governs <em>new</em> buffers. A connection established before the
        /// budget started binding keeps its full-size send buffers on its own stack and hands them straight back
        /// out, so without a pressure-gated return it would never converge. Drives exactly that shape: a first
        /// group that connects while the budget is slack, then enough connections to make it bind.
        /// </summary>
        [Test]
        public void LiveConnectionsConvergeAfterPressureArrives()
        {
            StartServer(networkBufferMemoryBudget: "8m", networkSendBufferMinSize: "16k");

            const int Early = 10;
            const int Late = 200;

            var early = new List<Socket>();
            var late = new List<Socket>();
            try
            {
                for (var i = 0; i < Early; i++)
                    early.Add(Ping(Connect()));

                // These connections must have been given the full configured size, or there is nothing to converge.
                ClassicAssert.AreEqual(Ceiling, StatBytes("targetBufferSize"));
                foreach (var s in early)
                    for (var j = 0; j < 5; j++)
                        _ = Ping(s);

                for (var i = 0; i < Late; i++)
                    late.Add(Ping(Connect()));

                ClassicAssert.AreEqual(ReceiveFloor, StatBytes("targetBufferSize"), "the budget must be binding now");
                var underPressure = SocketStatBytes("liveBytes");

                // Only the early connections do any work from here, so any change is attributable to them.
                foreach (var s in early)
                    for (var j = 0; j < 20; j++)
                        _ = Ping(s);

                var converged = SocketStatBytes("liveBytes");
                TestContext.Out.WriteLine($"underPressure={underPressure}, converged={converged}, " +
                    $"delta per early connection={(underPressure - converged) / Early}");

                // Each early connection was holding 128 KB buffers in both directions and should end near 16 KB.
                // The receive side alone accounts for about half of this; the threshold sits above that, so the
                // assertion cannot be satisfied without the send-side stack converging too.
                ClassicAssert.Greater(underPressure - converged, Early * 150L * 1024,
                    $"live connections did not converge after pressure arrived ({underPressure} -> {converged})");
            }
            finally
            {
                foreach (var s in early) s.Dispose();
                foreach (var s in late) s.Dispose();
            }
        }

        /// <summary>
        /// The incident is thousands of TLS connections, and a TLS connection shrinks a second, independent
        /// buffer -- the decrypted transport buffer -- through a separate code path. Without this the pressure
        /// branch that matters most in production would be unexercised.
        /// </summary>
        [Test]
        public async Task PressureShrinksTheTlsTransportBufferToo()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: true,
                networkBufferMemoryBudget: "1m");
            server.Start();

            const int Connections = 16;
            const int PayloadLength = 400 * 1024;
            var payload = new string('p', PayloadLength);

            var clients = new List<GarnetClient>();
            try
            {
                for (var i = 0; i < Connections; i++)
                {
                    var c = TestUtils.GetGarnetClient(useTLS: true);
                    await c.ConnectAsync();
                    _ = await c.PingAsync();
                    clients.Add(c);
                }

                var baseline = SocketStatBytes("liveBytes", clients[0]);
                var before = StatValue("pressureShrinks", clients[0]);

                foreach (var c in clients)
                    _ = await c.StringSetAsync("big", payload);
                var burst = SocketStatBytes("liveBytes", clients[0]);

                // Far short of the 256-receive idle hysteresis.
                for (var round = 0; round < 20; round++)
                    foreach (var c in clients)
                        _ = await c.PingAsync();

                var settled = SocketStatBytes("liveBytes", clients[0]);
                var shrinks = StatValue("pressureShrinks", clients[0]) - before;
                TestContext.Out.WriteLine($"tls baseline={baseline}, burst={burst}, settled={settled}, pressureShrinks={shrinks}");

                ClassicAssert.Greater(burst, baseline, "the oversized payload should have grown TLS buffers");
                ClassicAssert.Greater(shrinks, 0, "no pressure shrink was recorded on the TLS path");
                ClassicAssert.LessOrEqual(settled, baseline * 1.3,
                    $"grown TLS buffers were not released under pressure (settled={settled}, baseline={baseline})");
            }
            finally
            {
                foreach (var c in clients) c.Dispose();
            }
        }

        /// <summary>
        /// A session aborted by a protocol error is torn down through a different path than a clean
        /// disconnect. Because the target is <c>budget / liveBufferCount</c>, a count that ratchets up does
        /// not degrade gracefully -- it collapses every connection to the floor permanently -- so the abort
        /// path has to return every pool reference it took.
        /// </summary>
        /// <remarks>
        /// Verified to bind: skipping the receive buffer release in <c>NetworkHandler.DisposeImpl</c> makes
        /// this hang on teardown rather than pass. Note that the response buffer specifically is already
        /// returned by the batch's <c>finally</c> before any teardown path runs, so it is the receive-side
        /// references this pins.
        /// </remarks>
        [Test]
        public void AbortedSessionsReleaseTheirPoolReferences()
        {
            StartServer();

            const int Rounds = 200;

            // Baseline with one live connection, so the comparison is against a steady state rather than zero.
            using var keepalive = Ping(Connect());
            var baseline = StatValue("liveBufferCount");

            for (var i = 0; i < Rounds; i++)
            {
                var s = Connect();
                _ = Ping(s);
                // Malformed bulk length: the parser throws, the session writes an error and tears itself down
                // through DisposeNetworkSender rather than the clean path.
                Send(s, "*1\r\n$x\r\nPING\r\n");
                try { _ = s.Receive(new byte[256]); } catch (SocketException) { }
                s.Dispose();
            }

            // Teardown is asynchronous with respect to the client's close, so allow it to drain.
            var settled = baseline;
            for (var i = 0; i < 100; i++)
            {
                settled = StatValue("liveBufferCount");
                if (settled <= baseline) break;
                Thread.Sleep(50);
            }

            TestContext.Out.WriteLine($"liveBufferCount baseline={baseline}, after {Rounds} aborted sessions={settled}");
            ClassicAssert.LessOrEqual(settled, baseline,
                $"aborted sessions leaked pool references: {settled} live buffers against a baseline of {baseline}");
        }

        /// <summary>
        /// Without TLS the transport buffer is an alias of the network buffer, refreshed at the top of every
        /// receive. A resize therefore leaves the alias pointing at the entry that was just handed back to the
        /// pool, and for a connection that goes quiet straight after a shrink that stale field roots the old
        /// pinned array indefinitely -- the accounting shows the release, the memory never happens. That is
        /// exactly the incident's shape: thousands of connections that grew once and then idled.
        /// </summary>
        /// <remarks>
        /// The probe is a GET of a huge non-existent key, so the receive buffer has to grow to hold the command
        /// while nothing is stored -- a SET would contaminate the measurement with the value itself. Connections
        /// are driven in lockstep and stopped on the receive that shrinks them, so each one is idle at the
        /// moment its alias goes stale.
        /// </remarks>
        [Test]
        public void ShrunkReceiveBuffersAreNotRootedByTheStaleTransportAlias()
        {
            StartServer(networkBufferMemoryBudget: "1m");

            const int Connections = 64;
            const int KeyLength = 512 * 1024;

            var sockets = new List<Socket>();
            for (var i = 0; i < Connections; i++)
                sockets.Add(Ping(Connect()));

            var baseline = SettledMemory();

            var bigKey = new string('k', KeyLength);
            foreach (var s in sockets)
            {
                Send(s, $"*2\r\n$3\r\nGET\r\n${KeyLength}\r\n{bigKey}\r\n");
                DrainReplies(s, 1);
            }

            var grown = StatValue("pressureShrinks");

            // Stop on the round that shrinks them, so every connection is idle with a stale alias.
            for (var round = 0; round < 64; round++)
            {
                foreach (var s in sockets)
                {
                    Send(s, "*1\r\n$4\r\nPING\r\n");
                    DrainReplies(s, 1);
                }
                if (StatValue("pressureShrinks") >= grown + Connections) break;
            }

            var shrinks = StatValue("pressureShrinks") - grown;
            ClassicAssert.GreaterOrEqual(shrinks, Connections,
                "the connections never shrank, so the stale alias was never created and this measured nothing");

            var settled = SettledMemory();
            var retained = settled - baseline;

            TestContext.Out.WriteLine(
                $"retained {retained / 1024} KB over {Connections} connections => {retained / 1024.0 / Connections:F1} KB/conn");
            TestContext.Out.WriteLine(BpStats());

            // One stale root per connection would be the pre-shrink buffer, at least 512 KB. Half of that is
            // comfortably above the noise of the rest of the server and far below the leak.
            ClassicAssert.Less(retained, (long)Connections * 256 * 1024,
                $"shrunk receive buffers are still rooted: {retained / 1024} KB retained over {Connections} connections");

            foreach (var s in sockets) s.Dispose();
        }

        static long SettledMemory()
        {
            for (var i = 0; i < 3; i++)
            {
                GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
                GC.WaitForPendingFinalizers();
            }
            return GC.GetTotalMemory(forceFullCollection: true);
        }

        /// <summary>
        /// A TLS connection carries a fourth buffer the plain path does not: the plaintext send buffer, which is
        /// allocated once at construction with no grow path. Leaving it at the configured size would make every
        /// TLS connection over-target under pressure, so the pool would drop it on return and pin a fresh one on
        /// the next connect -- no send-side budget benefit at all on precisely the transport the incident used.
        /// </summary>
        /// <remarks>
        /// Asserts on the aggregate rather than a per-connection delta because <c>liveBytes</c> is reported
        /// through <c>Format.MemoryBytes</c>, which rounds up to a whole megabyte. The connection count is
        /// chosen so the 64 KB per connection at stake is several times that quantum.
        /// </remarks>
        [Test]
        public async Task PressureSizesTheTlsPlaintextSendBufferToo()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: true,
                networkBufferMemoryBudget: "1m");
            server.Start();

            const int Connections = 64;

            var clients = new List<GarnetClient>();
            try
            {
                for (var i = 0; i < Connections; i++)
                {
                    var c = TestUtils.GetGarnetClient(useTLS: true);
                    await c.ConnectAsync();
                    _ = await c.PingAsync();
                    clients.Add(c);
                }

                var probe = clients[0];
                ClassicAssert.AreEqual((long)SendFloor, StatBytes("targetSendBufferSize", probe),
                    "the send target must have settled at its floor for this test");

                var live = SocketStatBytes("liveBytes", probe);
                var buffers = StatValue("liveBufferCount", probe);
                TestContext.Out.WriteLine($"tls liveBytes={live} over {buffers} buffers for {Connections} connections");

                // Four buffers per connection: a 16 KB network receive buffer, a 16 KB plaintext receive
                // buffer, a 64 KB plaintext send buffer and a 64 KB socket send buffer. Leaving the plaintext
                // send buffer at the 128 KB ceiling adds 64 KB to each, or 4 MB across this many connections,
                // which is far outside the megabyte rounding of the reported figure.
                ClassicAssert.AreEqual(4L * Connections, buffers, "unexpected buffer count per TLS connection");
                ClassicAssert.LessOrEqual(live, (long)Connections * (2 * ReceiveFloor + 2 * SendFloor + SendFloor / 2),
                    "the TLS plaintext send buffer did not adapt to the send target");
            }
            finally
            {
                foreach (var c in clients) c.Dispose();
            }
        }

        /// <summary>
        /// Pressure is sticky: the target stays below the ceiling for as long as the connections are live. So
        /// an immediate shrink under pressure would reallocate a pinned buffer on every large request in an
        /// alternating large/small workload -- per-request churn precisely when the server is most loaded.
        /// Counts shrink events rather than bytes, since bytes settle either way.
        /// </summary>
        [Test]
        public void AlternatingPayloadsDoNotChurnUnderSustainedPressure()
        {
            StartServer(networkBufferMemoryBudget: "1m");

            const int Connections = 16;
            const int Rounds = 40;
            const int PayloadLength = 400 * 1024;
            var payload = new string('p', PayloadLength);

            var sockets = new List<Socket>();
            try
            {
                for (var i = 0; i < Connections; i++)
                    sockets.Add(Ping(Connect()));

                ClassicAssert.Less(StatBytes("targetBufferSize"), Ceiling, "the budget must be binding for this test");
                var before = StatValue("pressureShrinks");

                for (var round = 0; round < Rounds; round++)
                {
                    foreach (var s in sockets)
                    {
                        Send(s, $"*3\r\n$3\r\nSET\r\n$3\r\nbig\r\n${PayloadLength}\r\n{payload}\r\n");
                        ClassicAssert.AreEqual("+OK\r\n", ReadExactly(s, 5));
                        _ = Ping(s);
                    }
                }

                var shrinks = StatValue("pressureShrinks") - before;
                TestContext.Out.WriteLine($"pressure shrinks over {Rounds * Connections} large/small pairs: {shrinks}");

                // A large receive must reset the countdown, so strict alternation should never trip it at all.
                // The threshold is one shrink per connection rather than zero only to tolerate the initial
                // settling as the buffers find their size; a rate that scales with the number of rounds means
                // the shrink decision is reading the post-processing residual instead of the demand.
                ClassicAssert.LessOrEqual(shrinks, (long)Connections,
                    $"receive buffers churned under sustained pressure: {shrinks} shrinks over {Rounds * Connections} pairs");
            }
            finally
            {
                foreach (var s in sockets) s.Dispose();
            }
        }

        /// <summary>
        /// Under pressure a grown buffer must come back without waiting out the long idle hysteresis, or the
        /// aggregate cannot converge in time to matter. The control arm -- identical workload, budget disabled
        /// -- must still be holding its grown buffers after the same handful of small receives, which is what
        /// proves the pressure gate rather than the countdown did the work.
        /// </summary>
        [Test]
        public void PressureShrinksGrownBuffersWithoutWaitingOutTheIdleHysteresis()
        {
            var (budgetedSettled, budgetedBaseline, pressureShrinks) = MeasureShrinkAfterBurst("1m");
            var (controlSettled, controlBaseline, controlPressureShrinks) = MeasureShrinkAfterBurst("0");

            ClassicAssert.Greater(pressureShrinks, 0, "no pressure shrink was recorded under a small budget");
            ClassicAssert.AreEqual(0, controlPressureShrinks, "an unbudgeted server must never record a pressure shrink");

            // Far fewer than ShrinkHysteresis receives happened, so only the pressure path can have released these.
            ClassicAssert.LessOrEqual(budgetedSettled, budgetedBaseline * 1.2,
                $"grown buffers were not released under pressure (settled={budgetedSettled}, baseline={budgetedBaseline})");
            ClassicAssert.Greater(controlSettled, controlBaseline * 1.5,
                $"the control arm released its buffers too, so the pressure gate is not what is being measured " +
                $"(settled={controlSettled}, baseline={controlBaseline})");
        }

        /// <summary>
        /// Connects, grows every connection's receive buffer with one large request, then does a handful of
        /// small round trips -- far short of the idle hysteresis. Returns settled and baseline live bytes
        /// together with the pressure-shrink count.
        /// </summary>
        (long Settled, long Baseline, long PressureShrinks) MeasureShrinkAfterBurst(string budget)
        {
            server?.Dispose();
            server = null;
            StartServer(networkBufferMemoryBudget: budget);

            const int Connections = 24;
            const int PayloadLength = 400 * 1024;
            var payload = new string('p', PayloadLength);

            var sockets = new List<Socket>();
            try
            {
                for (var i = 0; i < Connections; i++)
                    sockets.Add(Ping(Connect()));
                var baseline = SocketStatBytes("liveBytes");

                foreach (var s in sockets)
                {
                    Send(s, $"*3\r\n$3\r\nSET\r\n$3\r\nbig\r\n${PayloadLength}\r\n{payload}\r\n");
                    ClassicAssert.AreEqual("+OK\r\n", ReadExactly(s, 5));
                }

                // Ten rounds, against a 256-receive idle hysteresis.
                for (var round = 0; round < 10; round++)
                    foreach (var s in sockets)
                        _ = Ping(s);

                return (SocketStatBytes("liveBytes"), baseline, StatValue("pressureShrinks"));
            }
            finally
            {
                foreach (var s in sockets) s.Dispose();
            }
        }

        /// <summary>
        /// Adaptation governs the base size only. A response far larger than the adapted send buffer, and a
        /// request far larger than the adapted receive buffer, must still succeed -- the send side by chunking
        /// through the buffer it was given, the receive side by growing on demand.
        /// </summary>
        [Test]
        public void LargePayloadsStillSucceedWhileTheBudgetIsAtItsFloor()
        {
            StartServer(networkBufferMemoryBudget: "512k");

            var connections = new List<Socket>();
            try
            {
                // Drive the target to the floor before the payload connection is opened.
                for (var i = 0; i < 60; i++)
                    connections.Add(Ping(Connect()));
                ClassicAssert.AreEqual(ReceiveFloor, StatBytes("targetBufferSize"));

                using var s = Connect();

                // A 1 MB value is well beyond both floors in both directions.
                const int ValueLength = 1 << 20;
                var value = new string('v', ValueLength);
                Send(s, $"*3\r\n$3\r\nSET\r\n$3\r\nbig\r\n${ValueLength}\r\n{value}\r\n");
                ClassicAssert.AreEqual("+OK\r\n", ReadExactly(s, 5));

                Send(s, "*2\r\n$3\r\nGET\r\n$3\r\nbig\r\n");
                var header = $"${ValueLength}\r\n";
                var reply = ReadExactly(s, header.Length + ValueLength + 2);
                ClassicAssert.IsTrue(reply.StartsWith(header, StringComparison.Ordinal), "unexpected bulk string header");
                ClassicAssert.IsTrue(reply.EndsWith("\r\n", StringComparison.Ordinal));
                ClassicAssert.AreEqual(ValueLength, reply.Length - header.Length - 2);

                // A key longer than the send buffer exercises the chunked write path rather than the growing one.
                var longKey = new string('k', 200 * 1024);
                Send(s, $"*3\r\n$3\r\nSET\r\n${longKey.Length}\r\n{longKey}\r\n$1\r\nx\r\n");
                ClassicAssert.AreEqual("+OK\r\n", ReadExactly(s, 5));
            }
            finally
            {
                foreach (var c in connections) c.Dispose();
            }
        }

        double MeasureLiveBytesPerConnection(string networkBufferMemoryBudget)
        {
            server?.Dispose();
            server = null;
            StartServer(networkBufferMemoryBudget: networkBufferMemoryBudget);

            const int Count = 50;
            var connections = new List<Socket>();
            try
            {
                for (var i = 0; i < Count; i++)
                    connections.Add(Ping(Connect()));

                return (double)SocketStatBytes("liveBytes") / Count;
            }
            finally
            {
                foreach (var c in connections) c.Dispose();
            }
        }

        #endregion

        #region end to end

        /// <summary>
        /// The signal must be observable from outside the process, and must read as inert at the shipped
        /// defaults with an ordinary number of connections.
        /// </summary>
        [Test]
        public void DefaultConfigPublishesTheConfiguredSizeToInfo()
        {
            StartServer();

            var connections = new List<Socket>();
            try
            {
                for (var i = 0; i < 20; i++)
                    connections.Add(Ping(Connect()));

                ClassicAssert.AreEqual(1L << 30, StatBytes("budgetBytes"));
                ClassicAssert.AreEqual(Ceiling, StatBytes("targetBufferSize"));
                ClassicAssert.Greater(StatValue("liveBufferCount"), 0);
            }
            finally
            {
                foreach (var c in connections) c.Dispose();
            }
        }

        /// <summary>
        /// The falsifying half: with a budget small enough for the connections in the test to exhaust, the
        /// published target must actually fall. Without this the whole suite would pass against a target
        /// hardcoded to the configured size.
        /// </summary>
        [Test]
        public void SmallBudgetDrivesThePublishedTargetDown()
        {
            // 512 KB of budget, so a handful of connections is already past the threshold.
            StartServer(networkBufferMemoryBudget: "512k");

            var connections = new List<Socket>();
            try
            {
                for (var i = 0; i < 40; i++)
                    connections.Add(Ping(Connect()));

                var target = StatBytes("targetBufferSize");
                ClassicAssert.Less(target, Ceiling, "target did not adapt under a budget it cannot satisfy");
                ClassicAssert.GreaterOrEqual(target, ReceiveFloor, "target fell below the receive floor");
            }
            finally
            {
                foreach (var c in connections) c.Dispose();
            }
        }

        /// <summary>
        /// Zero restores today's behaviour exactly, which is the escape hatch operators are told to use.
        /// </summary>
        [Test]
        public void ZeroBudgetDisablesAdaptationEndToEnd()
        {
            StartServer(networkBufferMemoryBudget: "0");

            var connections = new List<Socket>();
            try
            {
                for (var i = 0; i < 40; i++)
                    connections.Add(Ping(Connect()));

                ClassicAssert.AreEqual(0, StatBytes("budgetBytes"));
                ClassicAssert.AreEqual(Ceiling, StatBytes("targetBufferSize"));
            }
            finally
            {
                foreach (var c in connections) c.Dispose();
            }
        }

        #endregion

        #region unpressured common case

        /// <summary>
        /// The central promise of the design: with few connections the budget is slack, so a connection that
        /// grows its receive buffer must keep it for as long as it stays active, exactly as it does today. The
        /// control arm is the same workload with the budget disabled entirely; the two must hold the same
        /// bytes, and the budgeted arm must record no pressure shrink at all.
        /// </summary>
        [Test]
        public void UnpressuredGrownBuffersAreRetainedExactlyAsWithNoBudget()
        {
            var budgeted = MeasureUnpressuredGrowth(budget: null);
            var control = MeasureUnpressuredGrowth(budget: "0");

            ClassicAssert.AreEqual(0, budgeted.PressureShrinks,
                "the default budget must be slack at this connection count, so no pressure shrink may occur");
            ClassicAssert.AreEqual(Ceiling, budgeted.Target,
                "the published target must pin at the configured size while the budget is slack");

            // Grown, and still grown after the small traffic that follows.
            ClassicAssert.Greater(budgeted.Grown, budgeted.Baseline * 1.5,
                $"the receive buffers did not grow (baseline={budgeted.Baseline}, grown={budgeted.Grown})");
            ClassicAssert.Greater(budgeted.Settled, budgeted.Baseline * 1.5,
                $"a grown buffer was released while the budget was slack (baseline={budgeted.Baseline}, settled={budgeted.Settled})");

            // And byte-for-byte what an unbudgeted server holds, which is the no-regression claim.
            ClassicAssert.AreEqual(control.Grown, budgeted.Grown,
                $"demand-driven growth differed from an unbudgeted server (control={control.Grown}, budgeted={budgeted.Grown})");
            ClassicAssert.AreEqual(control.Settled, budgeted.Settled,
                $"retention differed from an unbudgeted server (control={control.Settled}, budgeted={budgeted.Settled})");
        }

        /// <summary>
        /// Connects a handful of clients, grows each one's receive buffer with a single large request, then
        /// does a short run of small round trips -- far short of the idle hysteresis.
        /// </summary>
        (long Baseline, long Grown, long Settled, long PressureShrinks, long Target) MeasureUnpressuredGrowth(string budget)
        {
            server?.Dispose();
            server = null;
            StartServer(networkBufferMemoryBudget: budget);

            const int Connections = 8;
            const int PayloadLength = 400 * 1024;
            var payload = new string('p', PayloadLength);

            var sockets = new List<Socket>();
            try
            {
                for (var i = 0; i < Connections; i++)
                    sockets.Add(Ping(Connect()));
                var baseline = SocketStatBytes("liveBytes");

                foreach (var s in sockets)
                {
                    Send(s, $"*3\r\n$3\r\nSET\r\n$3\r\nbig\r\n${PayloadLength}\r\n{payload}\r\n");
                    ClassicAssert.AreEqual("+OK\r\n", ReadExactly(s, 5));
                }
                var grown = SocketStatBytes("liveBytes");

                for (var round = 0; round < 20; round++)
                    foreach (var s in sockets)
                        _ = Ping(s);

                return (baseline, grown, SocketStatBytes("liveBytes"), StatValue("pressureShrinks"), StatBytes("targetBufferSize"));
            }
            finally
            {
                foreach (var s in sockets) s.Dispose();
            }
        }

        /// <summary>
        /// Guards correction C3. Dropping an over-target entry on return must be gated on pressure: while the
        /// budget is slack, the 256 KB and 512 KB buffers that receive-growth cycles through are legitimate
        /// pool levels, and discarding them would turn connection churn into repeated pinned allocation of
        /// arrays that are recyclable today.
        /// </summary>
        [Test]
        public void UnpressuredConnectionChurnKeepsRecyclingGrownBuffers()
        {
            StartServer();

            const int Rounds = 20;
            const int PayloadLength = 300 * 1024;
            var payload = new string('p', PayloadLength);

            for (var round = 0; round < Rounds; round++)
            {
                using var s = Ping(Connect());
                Send(s, $"*3\r\n$3\r\nSET\r\n$3\r\nbig\r\n${PayloadLength}\r\n{payload}\r\n");
                ClassicAssert.AreEqual("+OK\r\n", ReadExactly(s, 5));
            }

            ClassicAssert.AreEqual(0, StatValue("pressureShrinks"), "the default budget must be slack at this connection count");
            ClassicAssert.AreEqual(0, SocketStatBytes("totalOutOfBoundAllocations"),
                "every buffer in this workload must stay inside the pool's size classes for the count to be complete");

            // The grown buffers were handed back to the pool rather than dropped, so they are available to the
            // next connection. Anything at or below the baseline free list would mean they were discarded.
            var pooled = SocketStatBytes("pooledBytes");
            TestContext.Out.WriteLine($"pooled bytes after {Rounds} churned connections: {pooled}");
            ClassicAssert.GreaterOrEqual(pooled, PayloadLength,
                $"grown buffers were not recycled while the budget was slack (pooledBytes={pooled})");
        }

        #endregion

        /// <summary>
        /// The published target must agree with the live count it was derived from once churn stops.
        /// The population is parked at 65 buffers and churned across the 64/65 boundary, which is exactly
        /// where the derived target changes class: 65 buffers imply 64 KB, 64 imply 128 KB.
        ///
        /// This pins the invariant, not the retry loop in Recompute. That loop closes a window between
        /// sampling the count and winning the exchange, and the window was not reachable here: the mutant
        /// with the retry removed passes this test at 256 oversubscribed threads over 20 repeats, because
        /// every thread's last operation recomputes from the settled count. It is recorded as a defensive
        /// fix rather than a measured one. What this test does catch is any future path that moves the live
        /// count without republishing the target, which would leave the two permanently disagreeing.
        /// </summary>
        [Test]
        [Repeat(5)]
        public void ConcurrentChurnAcrossASizeClassLeavesAnAgreeingTarget()
        {
            const int Threads = 32;
            const int Pairs = 4_000;
            const int Parked = 65;

            // 64 * 128 KB of budget: 65 live buffers give a quotient just under 128 KB, so the correct
            // target is 64 KB, while a stale sample of 64 gives exactly 128 KB.
            var budget = new NetworkBufferBudget(Ceiling * 64L, Ceiling, ReceiveFloor, SendFloor);
            for (var i = 0; i < Parked; i++)
                budget.OnBufferAcquired();

            ClassicAssert.AreEqual(1 << 16, budget.TargetBufferSize, "the parked population did not set up the boundary");

            var start = new ManualResetEventSlim(false);
            var workers = new Thread[Threads];
            for (var t = 0; t < Threads; t++)
            {
                workers[t] = new Thread(() =>
                {
                    start.Wait();
                    for (var i = 0; i < Pairs; i++)
                    {
                        // Dips the count to 64 and back, so both sides of the boundary are sampled
                        // concurrently by different threads.
                        budget.OnBufferReleased();
                        budget.OnBufferAcquired();
                    }
                })
                { IsBackground = true };
                workers[t].Start();
            }

            start.Set();
            foreach (var w in workers)
                ClassicAssert.IsTrue(w.Join(TimeSpan.FromSeconds(60)), "budget churn worker did not finish");

            ClassicAssert.AreEqual(Parked, budget.LiveBufferCount, "the churn did not conserve the live count");

            // Asserted without a repairing Recompute(): in production nothing calls it except acquire and
            // release, so the quiescent state has to be right on its own.
            ClassicAssert.AreEqual(1 << 16, budget.TargetBufferSize,
                "the target did not agree with the live count once churn stopped");
            ClassicAssert.IsTrue(budget.IsUnderPressure, "pressure is not reported at a below-ceiling target");
        }

        #region helpers

        void StartServer(string networkBufferMemoryBudget = null, string networkSendBufferMinSize = null)
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                networkBufferMemoryBudget: networkBufferMemoryBudget,
                networkSendBufferMinSize: networkSendBufferMinSize);
            server.Start();
        }

        static Socket Connect()
        {
            var s = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
            s.Connect(TestUtils.EndPoint);
            return s;
        }

        /// <summary>
        /// Drive one round trip so the connection's buffers are actually allocated before the count is read.
        /// </summary>
        static Socket Ping(Socket s)
        {
            s.Send(Encoding.ASCII.GetBytes("*1\r\n$4\r\nPING\r\n"));
            var buf = new byte[64];
            _ = s.Receive(buf);
            return s;
        }

        /// <summary>Reads until <paramref name="expected"/> complete replies have arrived.</summary>
        static void DrainReplies(Socket s, int expected)
        {
            var buf = new byte[64 * 1024];
            var seen = 0;
            while (seen < expected)
            {
                var n = s.Receive(buf);
                if (n == 0) throw new Exception("connection closed");
                for (var i = 0; i < n; i++)
                    if (buf[i] == (byte)'\n') seen++;
            }
        }

        static void Send(Socket s, string command)
        {
            var bytes = Encoding.ASCII.GetBytes(command);
            var sent = 0;
            while (sent < bytes.Length)
                sent += s.Send(bytes, sent, bytes.Length - sent, SocketFlags.None);
        }

        /// <summary>
        /// Reads exactly <paramref name="count"/> bytes, since a large reply arrives in many chunks.
        /// </summary>
        static string ReadExactly(Socket s, int count)
        {
            var buf = new byte[count];
            var read = 0;
            while (read < count)
            {
                var n = s.Receive(buf, read, count - read, SocketFlags.None);
                if (n == 0) throw new Exception("connection closed");
                read += n;
            }
            return Encoding.ASCII.GetString(buf, 0, count);
        }

        static string BpStats()
        {
            using var s = Connect();
            s.Send(Encoding.ASCII.GetBytes("*2\r\n$4\r\nINFO\r\n$7\r\nBPSTATS\r\n"));
            var buf = new byte[32 * 1024];
            var n = s.Receive(buf);
            return Encoding.ASCII.GetString(buf, 0, n);
        }

        static string StatRaw(string name) => StatRaw(name, BpStats());

        static string StatRaw(string name, string stats)
        {
            // Read from the shared budget section, not from a per-socket pool section.
            var section = stats.IndexOf("network_buffer_budget", StringComparison.Ordinal);
            ClassicAssert.GreaterOrEqual(section, 0, $"budget section not found in BPSTATS: {stats}");

            var marker = name + "=";
            var idx = stats.IndexOf(marker, section, StringComparison.Ordinal);
            ClassicAssert.GreaterOrEqual(idx, 0, $"'{name}' not found in BPSTATS: {stats}");
            idx += marker.Length;
            var end = stats.IndexOfAny([',', '\r', '\n'], idx);
            return stats[idx..end];
        }

        static long StatValue(string name) => long.Parse(StatRaw(name));

        /// <summary>
        /// Same as <see cref="StatValue(string)"/>, but over an existing client. Needed on TLS, where opening a
        /// throwaway plain socket for INFO would not work.
        /// </summary>
        static long StatValue(string name, GarnetClient client)
            => long.Parse(StatRaw(name, BpStats(client)));

        static string BpStats(GarnetClient client)
            => client.ExecuteForStringResultAsync("INFO", ["BPSTATS"]).GetAwaiter().GetResult();

        /// <summary>
        /// Reads a stat from the listener's own pool section rather than the shared budget section.
        /// </summary>
        static long SocketStatBytes(string name) => SocketStatBytes(name, BpStats());

        static long SocketStatBytes(string name, GarnetClient client) => SocketStatBytes(name, BpStats(client));

        static long SocketStatBytes(string name, string stats)
        {
            var section = stats.IndexOf("server_socket_0", StringComparison.Ordinal);
            ClassicAssert.GreaterOrEqual(section, 0, $"socket section not found in BPSTATS: {stats}");

            var marker = name + "=";
            var idx = stats.IndexOf(marker, section, StringComparison.Ordinal);
            ClassicAssert.GreaterOrEqual(idx, 0, $"'{name}' not found in BPSTATS: {stats}");
            idx += marker.Length;
            var end = stats.IndexOfAny([',', '\r', '\n'], idx);
            return ParseMemoryBytes(stats[idx..end]);
        }

        static long StatBytes(string name)
        {
            var value = StatRaw(name);
            return ParseMemoryBytes(value);
        }

        /// <summary>
        /// Same as <see cref="StatBytes(string)"/>, but over an existing client, for the TLS fixtures.
        /// </summary>
        static long StatBytes(string name, GarnetClient client)
            => ParseMemoryBytes(StatRaw(name, BpStats(client)));

        static long ParseMemoryBytes(string value)
        {
            // Format.MemoryBytes emits e.g. "123KB", "1.50MB", "2.00GB".
            var (suffix, scale) = value.EndsWith("KB", StringComparison.Ordinal) ? ("KB", 1L << 10)
                : value.EndsWith("MB", StringComparison.Ordinal) ? ("MB", 1L << 20)
                : value.EndsWith("GB", StringComparison.Ordinal) ? ("GB", 1L << 30)
                : ("", 1L);
            var number = suffix.Length == 0 ? value : value[..^suffix.Length];
            return (long)(double.Parse(number, System.Globalization.CultureInfo.InvariantCulture) * scale);
        }

        #endregion
    }
}