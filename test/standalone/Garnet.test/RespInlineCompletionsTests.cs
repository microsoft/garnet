// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers <see cref="GarnetServerOptions.NetworkInlineCompletions"/>, under which a client connection's input
    /// is processed on the socket event thread that observed it. Clients here are <see cref="RawRespSession"/>s,
    /// whose synchronous sockets never join an event thread, so the server's connections are the only ones that do.
    /// </summary>
    [TestFixture(false)]
    [TestFixture(true)]
    public class RespInlineCompletionsTests : TestBase
    {
        const string Ok = "+OK\r\n";
        const string Pong = "+PONG\r\n";

        readonly bool useTls;
        GarnetServer server;

        public RespInlineCompletionsTests(bool useTls) => this.useTls = useTls;

        /// <summary>
        /// Replies with the kind of thread that runs it.
        /// </summary>
        sealed class ThreadKind : CustomProcedure
        {
            public override bool Execute<TGarnetApi>(TGarnetApi garnetApi, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
            {
                WriteSimpleString(ref output, Thread.CurrentThread.IsThreadPoolThread ? "POOL" : "EVENT");
                return true;
            }
        }

        [SetUp]
        public void Setup()
        {
            if (!SocketCompletions.InlineCompletionsSupported)
                Assert.Ignore("Sockets cannot run their completions inline on this platform.");

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        void StartServer(bool inlineCompletions)
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: useTls, networkInlineCompletions: inlineCompletions);
            server.Register.NewProcedure("THREADKIND", () => new ThreadKind());
            server.Start();
        }

        /// <summary>
        /// Connects and waits out the first receive, which can complete on the thread that accepted the connection
        /// if the client's input is already there, so every later request reaches the server through its event thread.
        /// </summary>
        RawRespSession Connect()
        {
            var client = new RawRespSession(useTls);
            ClassicAssert.AreEqual(Pong, client.Execute("PING"));
            Thread.Sleep(50);
            return client;
        }

        [Test]
        public void RequestsRunWhereTheSwitchSays([Values] bool inlineCompletions)
        {
            if (!inlineCompletions && TestUtils.InlineCompletionsForcedByEnvironment)
                Assert.Ignore("The environment runs every socket's completions inline, with the switch off too");

            StartServer(inlineCompletions);
            using var client = Connect();

            ClassicAssert.AreEqual(inlineCompletions ? "+EVENT\r\n" : "+POOL\r\n", client.Execute("THREADKIND"));
        }

        [Test]
        public void ParkedSessionsLeaveTheEventThreadsToOtherConnections()
        {
            StartServer(inlineCompletions: true);

            // Connections join the event threads in turn, so this many parked sessions sit on every thread, twice
            var eventThreads = SocketCompletions.EventThreadCount;
            ClassicAssert.Greater(eventThreads, 0);
            var blockers = new RawRespSession[(2 * eventThreads) + 1];
            try
            {
                for (var i = 0; i < blockers.Length; i++)
                    blockers[i] = Connect();

                var blocked = Stopwatch.StartNew();
                foreach (var blocker in blockers)
                    blocker.Send("DEBUG", "BLOCK", "5");
                Thread.Sleep(200);

                using (var client = Connect())
                    ClassicAssert.AreEqual(Pong, client.Execute("PING"));
                var answered = blocked.Elapsed;

                ClassicAssert.Less(answered.TotalSeconds, 2.5,
                    "A new connection waited for the blocked sessions, so they held the event threads rather than parking.");

                foreach (var blocker in blockers)
                    ClassicAssert.AreEqual(Ok, blocker.ReadReply());
                ClassicAssert.GreaterOrEqual(blocked.Elapsed.TotalSeconds, 4.5);
            }
            finally
            {
                foreach (var blocker in blockers)
                    blocker?.Dispose();
            }
        }

        [Test]
        public void LoadingACertificateLeavesTheEventThreadsToOtherConnections()
        {
            if (!useTls)
                Assert.Ignore("Only a TLS server loads a certificate.");
            if (!OperatingSystem.IsLinux())
                Assert.Ignore("The certificate file is a FIFO, made with mkfifo.");

            StartServer(inlineCompletions: true);

            // The certificate file is a FIFO, whose open waits for a writer, and every event thread has a load of it, twice
            var file = Path.Combine(TestUtils.MethodTestDir, "cert.pfx");
            using (var mkfifo = Process.Start("mkfifo", file))
            {
                mkfifo.WaitForExit();
                ClassicAssert.AreEqual(0, mkfifo.ExitCode, $"mkfifo {file} failed.");
            }

            var eventThreads = SocketCompletions.EventThreadCount;
            ClassicAssert.Greater(eventThreads, 0);
            var loaders = new RawRespSession[(2 * eventThreads) + 1];
            try
            {
                for (var i = 0; i < loaders.Length; i++)
                    loaders[i] = Connect();

                Task<string> probe;
                bool answeredWhileLoading;
                try
                {
                    foreach (var loader in loaders)
                        loader.Send("CONFIG", "SET", "cert-file-name", file, "cert-password", TestUtils.certPassword);
                    Thread.Sleep(200);

                    probe = Task.Run(() =>
                    {
                        using var client = Connect();
                        return client.Execute("PING");
                    });
                    answeredWhileLoading = probe.Wait(TimeSpan.FromSeconds(5));
                }
                finally
                {
                    ReleaseLoads(file);
                }

                ClassicAssert.IsTrue(answeredWhileLoading,
                    "A new connection waited for the certificate loads, so they held the event threads rather than leaving them.");
                ClassicAssert.AreEqual(Pong, probe.Result);
                foreach (var loader in loaders)
                    ClassicAssert.AreEqual(Ok, loader.ReadReply());
            }
            finally
            {
                foreach (var loader in loaders)
                    loader?.Dispose();
            }
        }

        /// <summary>
        /// Lets the loads of <paramref name="fifo"/> through to the test certificate. Opening the FIFO lets every load
        /// waiting to open it go on; the certificate then takes the FIFO's name, and closing the FIFO ends their first
        /// reads empty, so each load, finding no PEM marker, opens the name again as PKCS#12 and gets the certificate.
        /// A load that opens the name after it has been taken gets the certificate at once.
        /// </summary>
        static void ReleaseLoads(string fifo)
        {
            var certificate = fifo + ".tmp";
            File.Copy(TestUtils.certFile, certificate);

            var releaser = new Thread(() =>
            {
                using var writer = new FileStream(fifo, FileMode.Open, FileAccess.Write, FileShare.ReadWrite);
                File.Move(certificate, fifo, overwrite: true);
            })
            { IsBackground = true };
            releaser.Start();

            ClassicAssert.IsTrue(releaser.Join(TimeSpan.FromSeconds(30)), "No certificate load opened the file.");
        }
    }
}