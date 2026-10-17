// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Where a server handshake validates a client certificate. <see cref="SslStream"/> builds the certificate chain,
    /// which may download issuer certificates and revocation lists, and then calls the validation callback, both
    /// synchronously on one thread. A callback that waits therefore stands in for a download that has not arrived.
    /// </summary>
    [TestFixture]
    public class TlsClientCertificateValidationTests : TestBase
    {
        static readonly TimeSpan Bound = TimeSpan.FromSeconds(15);

        GarnetServer server;
        ManualResetEventSlim release;
        int stallCount;
        int validations;
        int validationsOnThreadPool;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            release = new ManualResetEventSlim(false);
        }

        [TearDown]
        public void TearDown()
        {
            release.Set();
            server?.Dispose();
            release.Dispose();
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
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: true, tlsServerOptionsOverride: options);
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
        public async Task ClientCertificatesAreValidatedOffTheThreadPool()
        {
            StartServer(stallCount: 0);

            await AssertPingsPromptly();

            ClassicAssert.AreEqual(1, Volatile.Read(ref validations));
            ClassicAssert.AreEqual(0, Volatile.Read(ref validationsOnThreadPool));
        }

        /// <summary>
        /// Twice as many handshakes wait in validation as the thread pool has threads, and a further client is still
        /// admitted and served.
        /// </summary>
        [Test]
        public async Task MoreHandshakesWaitInValidationThanTheThreadPoolHasThreads()
        {
            const int CappedWorkers = 16;
            const int Stalled = 2 * CappedWorkers;

            StartServer(Stalled);

            ThreadPool.GetMinThreads(out var minWorkers, out var minIo);
            ThreadPool.GetMaxThreads(out var maxWorkers, out var maxIo);

            var clients = new List<GarnetClient>();
            try
            {
                if (!ThreadPool.SetMinThreads(Math.Min(minWorkers, CappedWorkers), minIo) ||
                    !ThreadPool.SetMaxThreads(CappedWorkers, maxIo))
                    Assert.Ignore("Could not cap the thread pool, so the handshakes cannot be made to exceed it.");

                for (var i = 0; i < Stalled; i++)
                {
                    var client = TestUtils.GetGarnetClient(useTLS: true);
                    clients.Add(client);
                    _ = client.ConnectAsync();
                }

                var deadline = Stopwatch.StartNew();
                while (Volatile.Read(ref validations) < Stalled)
                {
                    if (deadline.Elapsed > Bound)
                        Assert.Fail($"Only {Volatile.Read(ref validations)} of {Stalled} handshakes reached validation within {Bound.TotalSeconds:0} seconds.");
                    Thread.Sleep(10);
                }

                await AssertPingsPromptly();

                ClassicAssert.AreEqual(0, Volatile.Read(ref validationsOnThreadPool));
            }
            finally
            {
                // Restored before the handshakes are released: a validation that was not moved is holding a pool thread
                _ = ThreadPool.SetMaxThreads(maxWorkers, maxIo);
                _ = ThreadPool.SetMinThreads(minWorkers, minIo);
                release.Set();

                foreach (var client in clients)
                    client.Dispose();
            }
        }

        static async Task AssertPingsPromptly()
        {
            using var client = TestUtils.GetGarnetClient(useTLS: true);
            var exchange = Task.Run(async () =>
            {
                await client.ConnectAsync();
                return await client.PingAsync();
            });

            if (await Task.WhenAny(exchange, Task.Delay(Bound)) != exchange)
                Assert.Fail($"A TLS client was not served within {Bound.TotalSeconds:0} seconds.");
            ClassicAssert.AreEqual("PONG", await exchange);
        }
    }
}