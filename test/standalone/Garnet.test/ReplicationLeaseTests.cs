// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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

        [Test]
        public void StandaloneReplicationFrameHeaderRoundTrips()
        {
            var token = Guid.NewGuid();
            Span<byte> buffer = stackalloc byte[StandaloneReplicationWireFormat.HeaderLength];

            StandaloneReplicationWireFormat.WriteHeader(
                buffer,
                StandaloneReplicationFrameType.CheckpointFile,
                CheckpointFileType.STORE_SNAPSHOT,
                payloadLength: 4096,
                token,
                address: 8192);

            ClassicAssert.IsTrue(StandaloneReplicationWireFormat.TryReadHeader(buffer, out var header));
            ClassicAssert.AreEqual(StandaloneReplicationFrameType.CheckpointFile, header.Type);
            ClassicAssert.AreEqual(CheckpointFileType.STORE_SNAPSHOT, header.CheckpointFileType);
            ClassicAssert.AreEqual(4096, header.PayloadLength);
            ClassicAssert.AreEqual(token, header.Token);
            ClassicAssert.AreEqual(8192, header.Address);
        }

        [Test]
        public void StandaloneReplicationFrameHeaderRejectsInvalidShape()
        {
            Span<byte> buffer = stackalloc byte[StandaloneReplicationWireFormat.HeaderLength];
            StandaloneReplicationWireFormat.WriteHeader(
                buffer,
                StandaloneReplicationFrameType.AofRecord,
                CheckpointFileType.STORE_INDEX,
                payloadLength: 32,
                Guid.NewGuid(),
                address: 64);

            ClassicAssert.IsFalse(StandaloneReplicationWireFormat.TryReadHeader(buffer, out _));
        }
    }
}