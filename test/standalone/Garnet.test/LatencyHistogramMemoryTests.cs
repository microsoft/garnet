// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using Garnet.common;
using Garnet.server;
using HdrHistogram;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// The latency monitor gives every session its own set of HdrHistograms so the record path stays
    /// lock-free. They are allocated on the first recorded value rather than when the connection is
    /// accepted, so an idle or single-purpose session does not pay for the latency types it never uses,
    /// and their size is set by the configured precision.
    ///
    /// Memory here is measured in-process with a forced compacting collection, because the server's own
    /// <c>gc_heap_bytes</c> is <c>GC.GetTotalMemory(false)</c> and cannot show a release.
    /// </summary>
    [TestFixture]
    public class LatencyHistogramMemoryTests : TestBase
    {
        const int Connections = 100;

        GarnetServer server;

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.OnTearDown();
        }

        void StartServer(bool latencyMonitor, int precision = GarnetServerOptions.DefaultLatencyMonitorPrecision,
            int samplingFreq = 1)
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                latencyMonitor: latencyMonitor,
                metricsSamplingFreq: latencyMonitor ? samplingFreq : -1,
                latencyMonitorPrecision: precision);
            server.Start();
        }

        static Socket Connect()
        {
            var s = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true, ReceiveTimeout = 15_000 };
            s.Connect(TestUtils.EndPoint);
            return s;
        }

        static void Ping(Socket s)
        {
            s.Send(Encoding.ASCII.GetBytes("*1\r\n$4\r\nPING\r\n"));
            var buf = new byte[64];
            var n = s.Receive(buf);
            ClassicAssert.AreEqual("+PONG\r\n", Encoding.ASCII.GetString(buf, 0, n));
        }

        /// <summary>
        /// Brings a RespServerSession into existence without completing a command, so the session and its
        /// per-session state are allocated but nothing has recorded a latency value yet.
        /// </summary>
        /// <remarks>
        /// A connection that sends nothing at all never reaches TryCreateMessageConsumer, which needs four
        /// bytes to pick a wire format, so no session is constructed and the measurement says nothing about
        /// what a session costs. Sending an incomplete command crosses that threshold while leaving the
        /// command unparsed.
        /// </remarks>
        static void OpenSessionWithoutRecording(Socket s)
            => s.Send(Encoding.ASCII.GetBytes("*1\r\n$4\r\nPIN"));

        /// <summary>
        /// Number of <c>RespServerSession</c> instances the server currently has.
        /// </summary>
        /// <remarks>
        /// Counted by enumerating the sessions directly rather than over the wire. It must be sessions and
        /// not accepted sockets -- <c>connected_clients</c> counts network handlers, so a socket that never
        /// sends four bytes is counted there but has no session and carries no histograms.
        /// <para>
        /// This previously issued <c>CLIENT LIST</c> over a raw socket and hand-parsed the RESP bulk string.
        /// That parser produced a review finding in four consecutive rounds -- an unreachable terminator
        /// check, a null-length case, and twice an overflow in its bounds arithmetic -- none of which were
        /// about the property under test. The session count is an instrument here, not the subject, so it
        /// does not need to exercise RESP framing. Enumerating also opens no probe connection of its own,
        /// removing the discount the wire probe had to apply to avoid counting itself.
        /// </para>
        /// </remarks>
        int RespSessionCount() => ActiveSessions().Count;

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
        /// Bytes retained per connection, optionally after the connection has issued a command. The
        /// difference between the two is the part of the session that only exists once it records.
        /// </summary>
        long MeasurePerConnection(bool ping)
        {
            // A discarded round of idle connections first: the threads, sockets and network buffers a
            // batch of connections brings up are one-time costs, and charging them to the measured round
            // makes the result depend on ordering. Deliberately idle, so this does not pre-populate the
            // shared array pool the histograms rent from and hide their cost.
            var warmup = new List<Socket>();
            for (var i = 0; i < Connections; i++) warmup.Add(Connect());
            foreach (var w in warmup) { try { w.Dispose(); } catch { } }

            var sockets = new List<Socket>();
            try
            {
                sockets.Add(Connect());
                if (ping) Ping(sockets[0]); else OpenSessionWithoutRecording(sockets[0]);

                var baseline = SettledMemory();

                for (var i = 1; i < Connections; i++)
                {
                    var s = Connect();
                    if (ping) Ping(s); else OpenSessionWithoutRecording(s);
                    sockets.Add(s);
                }

                // Every connection must have produced a session, or the measurement is of sockets rather
                // than of sessions and the arm proves nothing about per-session allocation.
                var deadline = DateTime.UtcNow.AddSeconds(15);
                int seen;
                while ((seen = RespSessionCount()) < Connections && DateTime.UtcNow < deadline)
                    Thread.Sleep(50);
                ClassicAssert.GreaterOrEqual(seen, Connections,
                    $"only {seen} of {Connections} sessions were created, so no per-session cost was measured");

                return Math.Max(0, SettledMemory() - baseline) / (Connections - 1);
            }
            finally
            {
                foreach (var s in sockets) { try { s.Dispose(); } catch { } }
            }
        }

        static long EagerCostPerSession()
            => 2L * Enum.GetValues<LatencyMetricsType>().Length
                  * new LongHistogram(1, TimeStamp.Seconds(100), GarnetServerOptions.DefaultLatencyMonitorPrecision)
                        .GetEstimatedFootprintInBytes();

        /// <summary>
        /// A session that exists but has never recorded must not carry the histograms. The assertion is on
        /// the absolute per-session cost against what all twelve histograms would add: a session also pays
        /// for its parse state, scratch buffers and network buffers, so the bar is "well under the eager
        /// cost", not "near zero". Restoring eager allocation adds the full eager figure on top of that
        /// baseline and puts it over the bar.
        /// </summary>
        [Test]
        public void IdleConnectionsDoNotPayForHistograms()
        {
            StartServer(latencyMonitor: true);
            var idle = MeasurePerConnection(ping: false);
            var eager = EagerCostPerSession();

            TestContext.Progress.WriteLine(
                $"per-session retained without recording: {idle / 1024}KB (eager allocation would add {eager / 1024}KB)");

            ClassicAssert.Less(idle, eager,
                "a session that never records should not be charged for all twelve histograms");
        }

        /// <summary>
        /// The flip side, asserted as a magnitude rather than a direction: the gap between a session that
        /// records and one that does not has to be a real share of the histogram cost. Under eager
        /// allocation both arms pay the same and the gap collapses, which is what this catches. A bare
        /// "recording > idle" would not -- the two measurements differ by noise either way.
        /// </summary>
        [Test]
        public void RecordingConnectionsDoAllocateHistograms()
        {
            StartServer(latencyMonitor: true);
            var idle = MeasurePerConnection(ping: false);
            var recording = MeasurePerConnection(ping: true);
            var eager = EagerCostPerSession();

            TestContext.Progress.WriteLine(
                $"per-session retained: without recording={idle / 1024}KB recording={recording / 1024}KB " +
                $"(all twelve histograms would be {eager / 1024}KB)");

            ClassicAssert.Greater(recording - idle, eager / 4,
                "recording a latency value should allocate the histograms it records into");
        }

        /// <summary>
        /// The histogram array is sized from the significant-digit count, so the knob is what actually
        /// moves the memory. Asserted against the histogram itself rather than a heap delta, because the
        /// sizing is exact and a heap delta is not.
        /// </summary>
        [Test]
        public void LowerPrecisionAllocatesSmallerHistograms()
        {
            var range = TimeStamp.Seconds(100);
            var at2 = new LongHistogram(1, range, 2).GetEstimatedFootprintInBytes();
            var at1 = new LongHistogram(1, range, 1).GetEstimatedFootprintInBytes();
            var at0 = new LongHistogram(1, range, 0).GetEstimatedFootprintInBytes();

            TestContext.Progress.WriteLine(
                $"histogram footprint: precision=2 {at2 / 1024}KB precision=1 {at1 / 1024}KB precision=0 {at0 / 1024}KB");

            ClassicAssert.Less(at1, at2, "one fewer significant digit must shrink the histogram");
            ClassicAssert.Less(at0, at1);
            ClassicAssert.Less(at1 * 3, at2, "the saving should be several-fold, not marginal");
        }

        /// <summary>
        /// Allocating on first record must not change what the monitor reports, so a session that has
        /// recorded values still shows up in <c>LATENCY HISTOGRAM</c> at a reduced precision.
        /// </summary>
        [Test]
        public void LatencyIsStillReportedAtReducedPrecision()
        {
            StartServer(latencyMonitor: true, precision: 1);
            using var s = Connect();

            // Session metrics reach the global histograms only when the monitor next samples.
            var deadline = DateTime.UtcNow.AddSeconds(30);
            string reply;
            do
            {
                for (var i = 0; i < 50; i++) Ping(s);

                s.Send(Encoding.ASCII.GetBytes("*2\r\n$7\r\nLATENCY\r\n$9\r\nHISTOGRAM\r\n"));
                var buf = new byte[64 * 1024];
                var n = s.Receive(buf);
                reply = Encoding.ASCII.GetString(buf, 0, n);

                StringAssert.DoesNotContain("-ERR", reply);
                if (reply.Contains("NET_RS_LAT")) return;
            }
            while (DateTime.UtcNow < deadline);

            Assert.Fail($"latency metrics were never reported: {reply}");
        }

        /// <summary>
        /// The histograms are the memory, and they must not exist before the session records into them.
        /// </summary>
        [Test]
        public void HistogramsAreAllocatedOnFirstRecord()
        {
            var entry = new LatencyMetricsEntrySession(GarnetServerOptions.DefaultLatencyMonitorPrecision);
            ClassicAssert.IsNull(entry.latency, "nothing should be allocated before a value is recorded");

            // A measurement that was never started records nothing, so it must not allocate either.
            entry.RecordValue(0);
            ClassicAssert.IsNull(entry.latency);

            entry.RecordValue(0, 0);
            ClassicAssert.IsNull(entry.latency);

            entry.RecordValue(0, 1234);
            ClassicAssert.IsNotNull(entry.latency, "recording a value should allocate the histograms");
            ClassicAssert.AreEqual(2, entry.latency.Length, "the histograms are double buffered");
        }

        /// <summary>
        /// The configured precision has to reach the histograms that are actually allocated, not just the
        /// ones a test constructs by hand. <c>LATENCY HISTOGRAM</c> reports each histogram's footprint, so
        /// it pins the whole path from the command line through to the allocation.
        /// </summary>
        [Test]
        public void ConfiguredPrecisionSizesTheHistogramsThatAreAllocated(
            [Values(GarnetServerOptions.DefaultLatencyMonitorPrecision, 1, 0)] int precision)
        {
            StartServer(latencyMonitor: true, precision: precision);
            using var s = Connect();

            var expected = new LongHistogram(1, TimeStamp.Seconds(100), precision).GetEstimatedFootprintInBytes();

            var deadline = DateTime.UtcNow.AddSeconds(30);
            string reply;
            do
            {
                for (var i = 0; i < 50; i++) Ping(s);

                s.Send(Encoding.ASCII.GetBytes("*2\r\n$7\r\nLATENCY\r\n$9\r\nHISTOGRAM\r\n"));
                var buf = new byte[256 * 1024];
                var n = s.Receive(buf);
                reply = Encoding.ASCII.GetString(buf, 0, n);

                StringAssert.DoesNotContain("-ERR", reply);
                if (reply.Contains("NET_RS_LAT"))
                {
                    var reported = ReportedHistogramSizes(reply);
                    CollectionAssert.IsNotEmpty(reported, "LATENCY HISTOGRAM should report a size per type");
                    foreach (var size in reported)
                        ClassicAssert.AreEqual(expected, size,
                            $"histograms should be sized for precision {precision}");
                    return;
                }
            }
            while (DateTime.UtcNow < deadline);

            Assert.Fail($"latency metrics were never reported: {reply}");
        }

        /// <summary>
        /// The sizes reported under each "size" field of a LATENCY HISTOGRAM reply.
        /// </summary>
        static List<int> ReportedHistogramSizes(string reply)
        {
            var sizes = new List<int>();
            var marker = "\r\n$4\r\nsize\r\n:";
            for (var i = reply.IndexOf(marker, StringComparison.Ordinal); i >= 0;
                 i = reply.IndexOf(marker, i + 1, StringComparison.Ordinal))
            {
                var from = i + marker.Length;
                var to = reply.IndexOf("\r\n", from, StringComparison.Ordinal);
                if (to > from && int.TryParse(reply[from..to], out var size))
                    sizes.Add(size);
            }
            return sizes;
        }

        /// <summary>
        /// The reply above reports the monitor's global histograms. The per-session histograms are what
        /// scale with connection count, so assert their size directly as well.
        /// </summary>
        [Test]
        public void ConfiguredPrecisionSizesTheSessionHistograms(
            [Values(GarnetServerOptions.DefaultLatencyMonitorPrecision, 1)] int precision)
        {
            StartServer(latencyMonitor: true, precision: precision);
            using var s = Connect();

            var expected = new LongHistogram(1, TimeStamp.Seconds(100), precision).GetEstimatedFootprintInBytes();

            // The session records its latency after the reply has been written, so receiving the reply does
            // not mean the histogram exists yet.
            var deadline = DateTime.UtcNow.AddSeconds(30);
            var asserted = 0;
            do
            {
                Ping(s);

                var sessions = ActiveSessions();
                CollectionAssert.IsNotEmpty(sessions, "the pinging connection should have a live session");

                foreach (var session in sessions)
                {
                    var metrics = session.LatencyMetrics?.metrics;
                    if (metrics == null) continue;

                    foreach (var entry in metrics)
                    {
                        if (entry.latency == null) continue;
                        foreach (var histogram in entry.latency)
                        {
                            ClassicAssert.AreEqual(expected, histogram.GetEstimatedFootprintInBytes(),
                                $"session histograms should be sized for precision {precision}");
                            asserted++;
                        }
                    }
                }
            }
            while (asserted == 0 && DateTime.UtcNow < deadline);

            ClassicAssert.Greater(asserted, 0, "a pinging session should have allocated at least one histogram");
        }

        /// <summary>
        /// The live RESP sessions of the server under test. Reached by reflection because the listeners are
        /// private to the host, and only the sessions carry the per-connection histograms.
        /// </summary>
        List<RespServerSession> ActiveSessions()
        {
            var field = typeof(GarnetServer).GetField("servers",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            ClassicAssert.IsNotNull(field, "GarnetServer should still hold its listeners in a 'servers' field");

            var listeners = (IGarnetServer[])field.GetValue(server);
            var sessions = new List<RespServerSession>();
            foreach (var listener in listeners)
                foreach (var consumer in ((GarnetServerBase)listener).ActiveConsumers())
                    if (consumer is RespServerSession resp)
                        sessions.Add(resp);
            return sessions;
        }

        /// <summary>
        /// Sessions are torn down while the monitor may still be reaching them, so releasing the pooled
        /// arrays has to tolerate being called more than once and on an entry that never allocated.
        /// </summary>
        [Test]
        public void ReturnIsSafeWhenNothingWasAllocated()
        {
            var unused = new LatencyMetricsEntrySession(GarnetServerOptions.DefaultLatencyMonitorPrecision);
            Assert.DoesNotThrow(() => unused.Return());
            Assert.DoesNotThrow(() => unused.Return());

            var used = new LatencyMetricsEntrySession(GarnetServerOptions.DefaultLatencyMonitorPrecision);
            used.RecordValue(0, 1234);
            Assert.DoesNotThrow(() => used.Return());
            Assert.DoesNotThrow(() => used.Return());
        }

        /// <summary>
        /// The histogram arrays go back to a shared pool on dispose, so disconnecting mid-command must not
        /// leave the record path writing into an array another session has since rented. Churns connections
        /// against a sampling monitor and requires the server to stay healthy throughout.
        /// </summary>
        [Test]
        public void DisconnectingUnderLoadDoesNotCorruptTheMonitor()
        {
            StartServer(latencyMonitor: true);

            for (var round = 0; round < 200; round++)
            {
                var s = Connect();
                try
                {
                    // Leave commands in flight, then drop the connection without reading the replies.
                    for (var i = 0; i < 16; i++)
                        s.Send(Encoding.ASCII.GetBytes("*1\r\n$4\r\nPING\r\n"));
                }
                catch (SocketException) { }
                finally
                {
                    s.Dispose();
                }
            }

            using var survivor = Connect();
            for (var i = 0; i < 50; i++) Ping(survivor);

            survivor.Send(Encoding.ASCII.GetBytes("*2\r\n$7\r\nLATENCY\r\n$9\r\nHISTOGRAM\r\n"));
            var buf = new byte[64 * 1024];
            var n = survivor.Receive(buf);
            StringAssert.DoesNotContain("-ERR", Encoding.ASCII.GetString(buf, 0, n));
        }

        /// <summary>
        /// Latency types across all live sessions that currently hold histograms.
        /// </summary>
        /// <remarks>
        /// Asserted on directly rather than inferred from process memory. Whether a histogram is held is
        /// the property under test, and reading it from the sessions cannot be satisfied by unrelated
        /// allocation noise the way a memory delta can.
        /// </remarks>
        int AllocatedHistogramTypes()
        {
            var count = 0;
            foreach (var session in ActiveSessions())
            {
                var m = session.LatencyMetrics?.metrics;
                if (m == null) continue;
                foreach (var cmd in Enum.GetValues<LatencyMetricsType>())
                    if (m[(int)cmd].latency != null) count++;
            }
            return count;
        }

        /// <summary>
        /// Waits for <paramref name="predicate"/> to hold over the allocated histogram count, returning the
        /// last value observed.
        /// </summary>
        int WaitForHistograms(Func<int, bool> predicate, int seconds = 40)
        {
            var deadline = DateTime.UtcNow.AddSeconds(seconds);
            int seen;
            while (!predicate(seen = AllocatedHistogramTypes()) && DateTime.UtcNow < deadline)
                Thread.Sleep(100);
            return seen;
        }

        /// <summary>
        /// A connection that was active and then goes quiet must give its histograms back. This is the
        /// incident's shape -- thousands of connections that do some work and then idle while staying open
        /// -- and lazy allocation alone does not cover it, because it only defers the first allocation.
        /// </summary>
        [Test]
        public void QuiescedSessionsReleaseTheirHistograms()
        {
            StartServer(latencyMonitor: true);

            var sockets = new List<Socket>();
            try
            {
                for (var i = 0; i < Connections; i++)
                {
                    var s = Connect();
                    Ping(s);
                    sockets.Add(s);
                }

                // Recording must have happened, or the release has nothing to release and the test would
                // pass against an implementation that never reclaims anything.
                var allocated = WaitForHistograms(n => n > 0, seconds: 15);
                ClassicAssert.GreaterOrEqual(allocated, Connections,
                    $"only {allocated} histogram types were allocated across {Connections} pinging sessions, " +
                    "so there was nothing to reclaim");

                var before = SettledMemory();

                // The connections stay open and simply stop sending.
                var remaining = WaitForHistograms(n => n == 0);
                ClassicAssert.AreEqual(0, remaining,
                    $"{remaining} histogram types were still held after the sessions went quiet, so a " +
                    "connection that stops sending keeps paying for histograms it is not recording into");

                // The structural release above is the discriminating assertion; this one confirms the
                // memory actually comes back rather than the reference merely being cleared.
                var after = SettledMemory();
                ClassicAssert.Less(after, before,
                    $"histograms were released but retained memory did not fall ({before} -> {after})");
            }
            finally
            {
                foreach (var s in sockets) { try { s.Dispose(); } catch { } }
            }
        }

        /// <summary>
        /// The control for <see cref="QuiescedSessionsReleaseTheirHistograms"/>: a session that keeps
        /// recording must keep its histograms across many windows, or the reclaim is stealing them from
        /// active connections and charging every window an allocation.
        /// </summary>
        [Test]
        public void ActiveSessionsKeepTheirHistograms()
        {
            StartServer(latencyMonitor: true);

            using var socket = Connect();
            Ping(socket);

            var allocated = WaitForHistograms(n => n > 0, seconds: 15);
            ClassicAssert.Greater(allocated, 0, "a pinging session should have allocated histograms");

            // Well past the release threshold, but never quiet for a whole window.
            var deadline = DateTime.UtcNow.AddSeconds(3 * GarnetServerMonitor.QuiescedWindowsBeforeRelease);
            while (DateTime.UtcNow < deadline)
            {
                Ping(socket);
                ClassicAssert.Greater(AllocatedHistogramTypes(), 0,
                    "a session that is still recording had its histograms reclaimed");
                Thread.Sleep(100);
            }
        }

        [Test]
        public void SlowButLiveSessionsAreNotReclaimedBetweenRequests()
        {
            // The hysteresis exists so that a session whose request rate is slower than the
            // monitor's sweep does not lose and re-allocate its histograms between requests.
            // Its margin is what this pins: a request every three seconds against a two-second
            // sweep leaves isolated empty windows but never a run of them.
            //
            // This test is wall-clock sensitive. Releasing needs four consecutive empty windows,
            // so on a loaded machine a stall of more than about five seconds beyond the request
            // interval will fail it. A failure here is far more likely to be scheduling than a
            // reclaim defect; the sampling frequency is deliberately slower than the rest of the
            // fixture's to widen that tolerance.
            StartServer(latencyMonitor: true, samplingFreq: 2);

            using var socket = Connect();
            Ping(socket);

            var allocated = WaitForHistograms(n => n > 0, seconds: 15);
            ClassicAssert.Greater(allocated, 0, "a pinging session should have allocated histograms");

            var deadline = DateTime.UtcNow.AddSeconds(24);
            while (DateTime.UtcNow < deadline)
            {
                Thread.Sleep(3000);
                ClassicAssert.Greater(AllocatedHistogramTypes(), 0,
                    "a session that is still issuing requests, only more slowly than the monitor sweeps, "
                    + "had its histograms reclaimed -- the release threshold leaves no hysteresis margin");
                Ping(socket);
            }
        }

        /// <summary>
        /// A session that goes quiet and then resumes must record normally again, since the release drops
        /// the histograms rather than marking the type unusable.
        /// </summary>
        [Test]
        public void AResumedSessionRecordsAgainAfterRelease()
        {
            StartServer(latencyMonitor: true);

            using var socket = Connect();
            Ping(socket);

            ClassicAssert.AreEqual(0, WaitForHistograms(n => n == 0),
                "the quiet session did not release its histograms, so the resume is untested");

            Ping(socket);

            var reallocated = WaitForHistograms(n => n > 0, seconds: 15);
            ClassicAssert.Greater(reallocated, 0,
                "a session that resumed after releasing its histograms did not record again");

            socket.Send(Encoding.ASCII.GetBytes("*2\r\n$7\r\nLATENCY\r\n$9\r\nHISTOGRAM\r\n"));
            var buf = new byte[64 * 1024];
            var n = socket.Receive(buf);
            StringAssert.DoesNotContain("-ERR", Encoding.ASCII.GetString(buf, 0, n));
        }

        /// <summary>
        /// The release waits for consecutive empty windows, so a session whose traffic straddles a window
        /// boundary is not released and re-allocated repeatedly. Driven directly, because inducing an exact
        /// sequence of empty and non-empty windows through a live server is timing-dependent.
        /// </summary>
        [Test]
        public void ReclaimWaitsForConsecutiveEmptyWindows()
        {
            const int Threshold = 4;
            var entry = new LatencyMetricsEntrySession(GarnetServerOptions.DefaultLatencyMonitorPrecision);

            entry.RecordValue(0, 1234);
            ClassicAssert.IsNotNull(entry.latency, "recording a value should have allocated the histograms");

            for (var i = 0; i < Threshold - 1; i++)
                ClassicAssert.IsFalse(entry.ReclaimIfQuiesced(Threshold),
                    $"the histograms were released after only {i + 1} empty windows");

            // A recorded value resets the run, so the next empty window starts counting from zero again.
            entry.RecordValue(0, 1234);
            ClassicAssert.IsFalse(entry.ReclaimIfQuiesced(Threshold),
                "a window that recorded a value should have reset the empty-window run");

            entry.latency[0].Reset();
            entry.latency[1].Reset();

            for (var i = 0; i < Threshold - 1; i++)
                ClassicAssert.IsFalse(entry.ReclaimIfQuiesced(Threshold),
                    "the run was not restarted by the recorded value");

            ClassicAssert.IsTrue(entry.ReclaimIfQuiesced(Threshold),
                $"the histograms were not released after {Threshold} consecutive empty windows");
            ClassicAssert.IsNull(entry.latency, "the released histograms should no longer be referenced");
        }

        /// <summary>
        /// A released histogram must not be handed back to the shared pool. The record path is
        /// unsynchronised, so a caller can still hold the array; returning it would let another session
        /// rent the array that caller is about to write into.
        /// </summary>
        [Test]
        public void ReleasingAQuiescedHistogramDoesNotReturnItToThePool()
        {
            var entry = new LatencyMetricsEntrySession(GarnetServerOptions.DefaultLatencyMonitorPrecision);
            entry.RecordValue(0, 1234);

            // The reference a racing writer would already be holding.
            var held = entry.latency;

            // Clear the recorded value so the windows that follow are empty ones.
            held[0].Reset();
            held[1].Reset();

            for (var i = 0; i < 4; i++) entry.ReclaimIfQuiesced(4);
            ClassicAssert.IsNull(entry.latency, "the histograms should have been released");

            ClassicAssert.IsFalse(held[0].IsReturned,
                "the released histogram was returned to the shared pool, so a concurrent recorder could " +
                "write into an array another session has since rented");
            ClassicAssert.IsFalse(held[1].IsReturned,
                "the released histogram was returned to the shared pool, so a concurrent recorder could " +
                "write into an array another session has since rented");

            // The write a racing recorder would make lands in a graph nobody else can reach.
            Assert.DoesNotThrow(() => held[0].RecordValue(1234));
        }
    }
}