// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test
{
    [TestFixture]
    internal class SimpleVersionSchemeTest
    {
        [Test]
        [Category("TsavoriteLog")]
        public void SimpleTest()
        {
            var tested = new EpochProtectedVersionScheme(new LightEpoch());
            var protectedVal = 0;
            var v = tested.Enter();

            Assert.AreEqual(1, v.Version);
            tested.TryAdvanceVersionWithCriticalSection((_, _) => protectedVal = 1);
            Thread.Sleep(10);
            // because of ongoing protection, nothing should happen yet
            tested.Leave();
            // As soon as protection is dropped, action should be done.
            Assert.AreEqual(1, protectedVal);

            // Next thread sees new version
            v = tested.Enter();
            Assert.AreEqual(v.Version, 2);
            tested.Leave();
        }

        [Test]
        [Category("TsavoriteLog")]
        public void SingleThreadTest()
        {
            var tested = new EpochProtectedVersionScheme(new LightEpoch());
            var protectedVal = 0;

            var v = tested.Enter();
            Assert.AreEqual(1, v.Version);
            tested.Leave();

            tested.TryAdvanceVersionWithCriticalSection((_, _) => protectedVal = 1);
            Assert.AreEqual(1, protectedVal);

            tested.TryAdvanceVersionWithCriticalSection((_, _) => protectedVal = 2, 4);
            Assert.AreEqual(2, protectedVal);

            v = tested.Enter();
            Assert.AreEqual(4, v.Version);
            tested.Leave();
        }

        [Test]
        [Category("TsavoriteLog")]
        public void LargeConcurrentTest()
        {
            var tested = new EpochProtectedVersionScheme(new LightEpoch());
            var protectedVal = 1L;
            var termination = new ManualResetEventSlim();

            var workerThreads = new List<Thread>();
            int numThreads = Math.Min(8, Environment.ProcessorCount / 2);
            // Force lots of interleavings by having many threads
            for (var i = 0; i < numThreads; i++)
            {
                var t = new Thread(() =>
                {
                    while (!termination.IsSet)
                    {
                        var v = tested.Enter();
                        Assert.AreEqual(v.Version, Interlocked.Read(ref protectedVal));
                        tested.Leave();
                    }
                });
                workerThreads.Add(t);
                t.Start();
            }

            for (var i = 0; i < 1000; i++)
            {
                tested.TryAdvanceVersionWithCriticalSection((vOld, vNew) =>
                {
                    Assert.AreEqual(vOld, Interlocked.Read(ref protectedVal));
                    // Flip sign to simulate critical section processing
                    protectedVal = -vOld;
                    Thread.Yield();
                    protectedVal = vNew;
                });
            }
            termination.Set();

            foreach (var t in workerThreads)
                t.Join();
        }
    }
}