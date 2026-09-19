// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [TestFixture]
    internal sealed class ReplicationLeaseTests : TestBase
    {
        [Test]
        public void CheckpointLeaseReleasesOnce()
        {
            var releaseCount = 0;
            var value = new object();
            var lease = new CheckpointLease<object>(value, () => releaseCount++);

            ClassicAssert.AreSame(value, lease.Value);
            lease.Dispose();
            lease.Dispose();

            ClassicAssert.AreEqual(1, releaseCount);
        }

        [Test]
        public void FixedAofRetentionLeaseBoundsTruncation()
        {
            using var manager = new AofRetentionManager(1);
            var retainedAddress = AofAddress.Create(1, 128);
            ClassicAssert.IsTrue(manager.TryAcquire(retainedAddress, allowDataLoss: false, out var lease));

            var requestedAddress = AofAddress.Create(1, 256);
            var truncationLimit = manager.GetTruncationLimit(requestedAddress);
            ClassicAssert.AreEqual(128, truncationLimit[0]);

            lease.Dispose();
            truncationLimit = manager.GetTruncationLimit(requestedAddress);
            ClassicAssert.AreEqual(256, truncationLimit[0]);
        }

        [Test]
        public void MovingAofRetentionLeaseAdvancesTruncation()
        {
            using var manager = new AofRetentionManager(1);
            var currentAddress = 128L;
            var startAddress = AofAddress.Create(1, currentAddress);
            ClassicAssert.IsTrue(manager.TryAcquire(startAddress, _ => currentAddress, allowDataLoss: false, out var lease));

            var requestedAddress = AofAddress.Create(1, 512);
            ClassicAssert.AreEqual(128, manager.GetTruncationLimit(requestedAddress)[0]);

            currentAddress = 384;
            ClassicAssert.AreEqual(384, manager.GetTruncationLimit(requestedAddress)[0]);
            lease.Dispose();
        }

        [Test]
        public void AofRetentionLeaseRejectsTruncatedAddress()
        {
            using var manager = new AofRetentionManager(1);
            var truncatedAddress = AofAddress.Create(1, 256);
            manager.UpdateTruncatedUntil(truncatedAddress);

            var unavailableAddress = AofAddress.Create(1, 128);
            ClassicAssert.IsFalse(manager.TryAcquire(unavailableAddress, allowDataLoss: false, out var lease));
            ClassicAssert.IsNull(lease);
            ClassicAssert.IsTrue(manager.TryAcquire(unavailableAddress, allowDataLoss: true, out lease));
            lease.Dispose();
        }
    }
}