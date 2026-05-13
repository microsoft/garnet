// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;

namespace Garnet.test.cluster
{
    [TestFixture]
    [NonParallelizable]
    [Ignore("Skip to reduce CI duration.")]
    public class ClusterReplicationAsyncReplay : ClusterReplicationBaseTests
    {
        [SetUp]
        public override void Setup()
        {
            asyncReplay = true;
            base.Setup();
        }

        [TearDown]
        public override void TearDown()
        {
            base.TearDown();
        }
    }
}