// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Where a server handshake validates a client certificate, on a server that processes client input on the socket
    /// event threads. <see cref="SslStream"/> builds the certificate chain, which may download issuer certificates and
    /// revocation lists, and then calls the validation callback, both synchronously on the thread that completed the
    /// handshake's read. A callback that waits therefore stands in for a download that has not arrived. Clients here are
    /// <see cref="RawRespSession"/>s, whose synchronous sockets never join an event thread.
    /// </summary>
    [TestFixture]
    public class TlsClientCertificateValidationTests : TestBase
    {
        const string Pong = "+PONG\r\n";
        static readonly TimeSpan Bound = TimeSpan.FromSeconds(15);

        GarnetServer server;
        ManualResetEventSlim release;
        int stallCount;
        int validations;
        int validationsOnThreadPool;

        [SetUp]
        public void Setup()
        {
            if (!SocketCompletions.InlineCompletionsSupported)
                Assert.Ignore("Sockets cannot run their completions inline on this platform.");

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            release = new ManualResetEventSlim(false);
        }

        [TearDown]
        public void TearDown()
        {
            release?.Set();
            server?.Dispose();
            server = null;
            release?.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        void StartServer(int stallCount)
        {
            this.stallCount = stallCount;
            var options = new SslServerAuthenticationOptions
            {
                ServerCertificate = TestUtils.GetClientCertificate(),
                ClientCertificateRequired = true,
                CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
                RemoteCertificateValidationCallback = Validate,
            };
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: true, networkInlineCompletions: true,
                tlsServerOptionsOverride: options);
            server.Start();
        }

        bool Validate(object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors errors)
        {
            if (Thread.CurrentThread.IsThreadPoolThread)
                _ = Interlocked.Increment(ref validationsOnThreadPool);
            if (Interlocked.Increment(ref validations) <= stallCount)
                release.Wait();
            return true;
        }

        [Test]
        public void ClientCertificatesAreValidatedOnTheThreadPool()
        {
            StartServer(stallCount: 0);

            using (var client = new RawRespSession(useTls: true))
                ClassicAssert.AreEqual(Pong, client.Execute("PING"));

            ClassicAssert.AreEqual(1, Volatile.Read(ref validations));
            ClassicAssert.AreEqual(1, Volatile.Read(ref validationsOnThreadPool),
                "A client certificate was validated on the socket event thread that completed the handshake's read.");
        }

        /// <summary>
        /// Twice as many handshakes wait in validation as there are socket event threads, and a further client is still
        /// admitted and served.
        /// </summary>
        [Test]
        public void MoreHandshakesWaitInValidationThanThereAreEventThreads()
        {
            // Connections join the event threads in turn, so this many stalled handshakes sit on every thread, twice
            var eventThreads = SocketCompletions.EventThreadCount;
            ClassicAssert.Greater(eventThreads, 0);
            var stalled = (2 * eventThreads) + 1;

            StartServer(stalled);

            // Each stalled handshake holds a pool thread on either side, which must not wait for the pool to grow
            ThreadPool.GetMinThreads(out var minWorkers, out var minIo);
            _ = ThreadPool.SetMinThreads(Math.Max(minWorkers, (2 * stalled) + 16), minIo);

            var handshakes = new Task<RawRespSession>[stalled];
            try
            {
                for (var i = 0; i < stalled; i++)
                    handshakes[i] = Task.Run(() => new RawRespSession(useTls: true));

                ClassicAssert.IsTrue(SpinWait.SpinUntil(() => Volatile.Read(ref validations) >= stalled, Bound),
                    $"Only {Volatile.Read(ref validations)} of {stalled} handshakes reached validation within {Bound.TotalSeconds:0} seconds.");

                var probe = Task.Run(() =>
                {
                    using var client = new RawRespSession(useTls: true);
                    return client.Execute("PING");
                });
                ClassicAssert.IsTrue(probe.Wait(Bound),
                    "A new connection was not served while handshakes waited in validation, so they held the event threads.");
                ClassicAssert.AreEqual(Pong, probe.Result);
                ClassicAssert.AreEqual(Volatile.Read(ref validations), Volatile.Read(ref validationsOnThreadPool),
                    "A client certificate was validated on a socket event thread.");

                release.Set();
                foreach (var handshake in handshakes)
                {
                    ClassicAssert.IsTrue(handshake.Wait(Bound), "A handshake released from validation did not complete.");
                    ClassicAssert.AreEqual(Pong, handshake.Result.Execute("PING"));
                }
            }
            finally
            {
                release.Set();
                _ = ThreadPool.SetMinThreads(minWorkers, minIo);
                foreach (var handshake in handshakes)
                {
                    if (handshake is { IsCompletedSuccessfully: true })
                        handshake.Result.Dispose();
                }
            }
        }
    }
}