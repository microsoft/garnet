// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Tsavorite.test
{
    [AllureNUnit]
    [TestFixture]
    internal class SimpleVersionSchemeTest : AllureTestBase
    {
        [Test]
        [Category("TsavoriteLog")]
        public void SimpleTest()
        {
            using var tested = new EpochProtectedVersionScheme();
            var protectedVal = 0;
            var v = tested.Enter();

            ClassicAssert.AreEqual(1, v.Version);
            tested.TryAdvanceVersionWithCriticalSection((_, _) => protectedVal = 1);
            Thread.Sleep(10);
            // because of ongoing protection, nothing should happen yet
            tested.Leave();
            // As soon as protection is dropped, action should be done.
            ClassicAssert.AreEqual(1, protectedVal);

            // Next thread sees new version
            v = tested.Enter();
            ClassicAssert.AreEqual(v.Version, 2);
            tested.Leave();
        }

        [Test]
        [Category("TsavoriteLog")]
        public void SingleThreadTest()
        {
            using var tested = new EpochProtectedVersionScheme();
            var protectedVal = 0;

            var v = tested.Enter();
            ClassicAssert.AreEqual(1, v.Version);
            tested.Leave();

            tested.TryAdvanceVersionWithCriticalSection((_, _) => protectedVal = 1);
            ClassicAssert.AreEqual(1, protectedVal);

            tested.TryAdvanceVersionWithCriticalSection((_, _) => protectedVal = 2, 4);
            ClassicAssert.AreEqual(2, protectedVal);

            v = tested.Enter();
            ClassicAssert.AreEqual(4, v.Version);
            tested.Leave();
        }

        [Test]
        [Category("TsavoriteLog")]
        public void LargeConcurrentTest()
        {
            using var tested = new EpochProtectedVersionScheme();
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
                        ClassicAssert.AreEqual(v.Version, Interlocked.Read(ref protectedVal));
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
                    ClassicAssert.AreEqual(vOld, Interlocked.Read(ref protectedVal));
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