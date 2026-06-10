// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Unit tests for <see cref="StreamObject"/> as an <see cref="IGarnetObject"/>, focused on the
    /// object-store serialization round-trip that makes consumer-group state durable.
    /// </summary>
    [TestFixture]
    public unsafe class StreamObjectTests
    {
        const long PageSize = 4096;
        const long MemorySize = 1L << 24;

        [SetUp]
        public void Setup()
        {
            // In-memory streams (NullDevice). Configure the ambient stream settings used by the
            // object-store deserialization factory.
            StreamObjectConfig.Configure(streamsRootDir: null, PageSize, MemorySize);
        }

        /// <summary>
        /// Consumer groups, their PEL, consumers, last-delivered-id, and entries-read counters must
        /// survive an object-store serialize/deserialize round-trip — the core goal of modeling a
        /// stream as a persistable <see cref="IGarnetObject"/>.
        /// </summary>
        [Test]
        public void ConsumerGroupStateSurvivesSerializationRoundTrip()
        {
            var stream = new StreamObject(streamsRootDir: null, streamDirName: null, PageSize, MemorySize);

            var lastDelivered = new StreamID(5, 0);
            ClassicAssert.IsTrue(stream.CreateGroup("g1", lastDelivered, entriesRead: 3));
            ClassicAssert.IsTrue(stream.TryGetConsumerGroupForTest("g1", out var group));

            // Populate a consumer and a pending entry directly via the public consumer-group API.
            var pendingId = new StreamID(2, 0);
            var consumer = group.GetOrCreateConsumer("c1", nowMs: 12345);
            group.AddPendingEntry(pendingId, consumer, nowMs: 67890);

            // Serialize through the Garnet object serializer (writes the type byte then the object body).
            GarnetObjectSerializer.Serialize(stream, out var bytes);

            // Deserialize: the factory reads the type byte, then StreamObject(BinaryReader) reconstructs.
            using var ms = new MemoryStream(bytes);
            using var reader = new BinaryReader(ms);
            var type = (GarnetObjectType)reader.ReadByte();
            ClassicAssert.AreEqual(GarnetObjectType.Stream, type);

            var restored = new StreamObject(reader);

            // Group and its scalar cursors survived.
            ClassicAssert.AreEqual(1, restored.ConsumerGroupCount);
            ClassicAssert.IsTrue(restored.TryGetConsumerGroupForTest("g1", out var restoredGroup));
            ClassicAssert.AreEqual(3, restoredGroup.EntriesRead);
            ClassicAssert.AreEqual(0, restoredGroup.LastDeliveredId.CompareTo(lastDelivered));

            // PEL survived with its delivery metadata.
            ClassicAssert.AreEqual(1, restoredGroup.PEL.Count);
            ClassicAssert.IsTrue(restoredGroup.PEL.ContainsKey(pendingId));
            var restoredPending = restoredGroup.PEL[pendingId];
            ClassicAssert.AreEqual("c1", restoredPending.ConsumerName);
            ClassicAssert.AreEqual(1, restoredPending.DeliveryCount);
            ClassicAssert.AreEqual(67890, restoredPending.DeliveryTime);

            // Consumer survived and its PendingIds index was rebuilt from the PEL.
            ClassicAssert.AreEqual(1, restoredGroup.Consumers.Count);
            ClassicAssert.IsTrue(restoredGroup.Consumers.ContainsKey("c1"));
            ClassicAssert.IsTrue(restoredGroup.Consumers["c1"].PendingIds.Contains(pendingId));

            stream.Dispose();
            restored.Dispose();
        }

        /// <summary>
        /// A stream with multiple groups (including an empty one) round-trips faithfully.
        /// </summary>
        [Test]
        public void MultipleConsumerGroupsSurviveSerializationRoundTrip()
        {
            var stream = new StreamObject(streamsRootDir: null, streamDirName: null, PageSize, MemorySize);

            ClassicAssert.IsTrue(stream.CreateGroup("g1", new StreamID(10, 0), entriesRead: 1));
            ClassicAssert.IsTrue(stream.CreateGroup("g2", new StreamID(20, 5), entriesRead: 7));

            GarnetObjectSerializer.Serialize(stream, out var bytes);

            using var ms = new MemoryStream(bytes);
            using var reader = new BinaryReader(ms);
            _ = reader.ReadByte(); // type byte
            var restored = new StreamObject(reader);

            ClassicAssert.AreEqual(2, restored.ConsumerGroupCount);
            ClassicAssert.IsTrue(restored.TryGetConsumerGroupForTest("g1", out var g1));
            ClassicAssert.IsTrue(restored.TryGetConsumerGroupForTest("g2", out var g2));
            ClassicAssert.AreEqual(0, g1.LastDeliveredId.CompareTo(new StreamID(10, 0)));
            ClassicAssert.AreEqual(1, g1.EntriesRead);
            ClassicAssert.AreEqual(0, g2.LastDeliveredId.CompareTo(new StreamID(20, 5)));
            ClassicAssert.AreEqual(7, g2.EntriesRead);

            stream.Dispose();
            restored.Dispose();
        }
    }
}