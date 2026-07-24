// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.cluster;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Regression tests for the gossip-connection client timeout computation
    /// (<see cref="GarnetServerNode.GetClientTimeoutMilliseconds(int)"/>), which previously used
    /// <c>TimeSpan.Milliseconds</c> (sub-second component) instead of the full millisecond total.
    /// </summary>
    [TestFixture]
    public class GarnetServerNodeTimeoutTests : TestBase
    {
        [Test]
        [TestCase(1, 1_000)]
        [TestCase(5, 5_000)]
        [TestCase(60, 60_000)]     // production default; buggy code yielded 0 here
        [TestCase(3600, 3_600_000)]
        public void ClientTimeoutUsesTotalMilliseconds(int clusterTimeoutSeconds, int expectedMilliseconds)
        {
            var actual = GarnetServerNode.GetClientTimeoutMilliseconds(clusterTimeoutSeconds);
            ClassicAssert.AreEqual(expectedMilliseconds, actual);
        }

        [Test]
        [TestCase(0)]
        [TestCase(-1)]
        public void NonPositiveClusterTimeoutDisablesClientTimeout(int clusterTimeoutSeconds)
        {
            var actual = GarnetServerNode.GetClientTimeoutMilliseconds(clusterTimeoutSeconds);
            ClassicAssert.AreEqual(0, actual);
        }

        [Test]
        [TestCase(2_147_484)]      // *1000 overflows int; naive cast would wrap negative and disable the timeout
        [TestCase(int.MaxValue)]
        public void LargeClusterTimeoutClampsToIntMaxValue(int clusterTimeoutSeconds)
        {
            var actual = GarnetServerNode.GetClientTimeoutMilliseconds(clusterTimeoutSeconds);
            ClassicAssert.AreEqual(int.MaxValue, actual);
        }
    }
}