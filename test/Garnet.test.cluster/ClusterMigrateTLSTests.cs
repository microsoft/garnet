// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;
using NUnit.Framework;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class ClusterTLSMT
    {
        ClusterMigrateTests tests;

        [SetUp]
        public void Setup()
        {
            tests = new ClusterMigrateTests(UseTLS: true);
            tests.Setup();
        }

        [TearDown]
        public void TearDown()
        {
            tests.TearDown();
            tests = null;
        }

        [Test, Order(1)]
        [Category("CLUSTER")]
        public async Task ClusterTLSInitialize()
            => await tests.ClusterSimpleInitialize();

        [Test, Order(2)]
        [Category("CLUSTER")]
        public async Task ClusterTLSSlotInfo()
            => await tests.ClusterSimpleSlotInfo();

        [Test, Order(3)]
        [Category("CLUSTER")]
        public async Task ClusterTLSAddDelSlots()
            => await tests.ClusterAddDelSlots();

        [Test, Order(4)]
        [Category("CLUSTER")]
        public async Task ClusterTLSSlotChangeStatus()
            => await tests.ClusterSlotChangeStatus();

        [Test, Order(5)]
        [Category("CLUSTER")]
        public async Task ClusterTLSRedirectMessage()
            => await tests.ClusterRedirectMessage();

        [Test, Order(6)]
        [Category("CLUSTER")]
        public async Task ClusterTLSMigrateSlots()
            => await tests.ClusterSimpleMigrateSlots();

        [Test, Order(7)]
        [Category("CLUSTER")]
        public async Task ClusterTLSMigrateSlotsExpiry()
            => await tests.ClusterSimpleMigrateSlotsExpiry();

        [Test, Order(8)]
        [Category("CLUSTER")]
        public async Task ClusterTLSMigrateSlotsWithObjects()
            => await tests.ClusterSimpleMigrateSlotsWithObjects();

        [Test, Order(9)]
        [Category("CLUSTER")]
        public async Task ClusterTLSMigrateKeys()
            => await tests.ClusterSimpleMigrateKeys();

        [Test, Order(10)]
        [Category("CLUSTER")]
        public async Task ClusterTLSMigrateKeysWithObjects()
            => await tests.ClusterSimpleMigrateKeysWithObjects();

        [Test, Order(11)]
        [Category("CLUSTER")]
        public async Task ClusterTLSMigratetWithReadWrite()
            => await tests.ClusterSimpleMigrateWithReadWrite();
    }
}