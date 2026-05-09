// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Allure.NUnit;
using NUnit.Framework;

namespace Garnet.test.cluster
{
    [TestFixture]
    [AllureNUnit]
    [NonParallelizable]
    public class ClusterReplicationTLS : ClusterReplicationBaseTests
    {
        [SetUp]
        public override void Setup()
        {
            useTLS = true;
            base.Setup();
        }

        [TearDown]
        public override void TearDown()
        {
            base.TearDown();
        }
    }
}