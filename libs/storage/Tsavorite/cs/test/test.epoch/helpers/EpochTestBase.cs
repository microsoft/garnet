// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Threading;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.epoch
{
    /// <summary>
    /// Base for every fixture here: the running-test tracking from <see cref="Garnet.test.TestBase"/>,
    /// a fresh <see cref="LightEpoch"/> per test, and the join helper they share.
    /// </summary>
    public abstract class EpochTestBase : Garnet.test.TestBase
    {
        protected LightEpoch epoch;

        [SetUp]
        public virtual void CreateEpoch() => epoch = new LightEpoch();

        [TearDown]
        public virtual void DisposeEpoch()
        {
            epoch?.Dispose();
            epoch = null;
        }

        /// <summary>Wait for every thread.</summary>
        protected static void JoinAll(IEnumerable<Thread> threads)
        {
            foreach (var thread in threads)
                thread.Join();
        }
    }
}