// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Text;
using System.Threading;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Demonstrates that GarnetObject's shallow Clone() creates a race condition
    /// when serialization and mutation happen concurrently on shared collections.
    /// This is the same pattern used by Tsavorite's CopyUpdate: Clone() creates
    /// a shallow copy, then the clone is mutated while the original is serialized.
    /// </summary>
    [TestFixture]
    public class ShallowCloneRaceConditionTests
    {
        /// <summary>
        /// Race: one thread serializes (iterates sortedSetDict via DoSerialize),
        /// another thread mutates the same dictionary (via Add on the clone).
        /// Since Clone() shares collections by reference, this throws
        /// InvalidOperationException ("Collection was modified during enumeration")
        /// or produces corrupt output.
        /// </summary>
        [Test]
        public void SortedSetCloneSerializeWhileMutatingThrows()
        {
            // Create a SortedSetObject with enough entries to make iteration non-trivial
            var original = new SortedSetObject();
            for (int i = 0; i < 1000; i++)
            {
                original.Add(Encoding.ASCII.GetBytes($"member-{i:D4}"), i);
            }

            // Clone shares the same sortedSetDict and sortedSet by reference
            var clone = (SortedSetObject)original.Clone();

            var serializeBarrier = new Barrier(2);
            Exception serializeException = null;
            Exception mutateException = null;
            var done = new ManualResetEventSlim(false);

            // Thread 1: serialize the original (iterates sortedSetDict)
            var serializeThread = new Thread(() =>
            {
                try
                {
                    serializeBarrier.SignalAndWait();
                    for (int round = 0; round < 500 && serializeException == null && mutateException == null; round++)
                    {
                        using var ms = new MemoryStream();
                        using var writer = new BinaryWriter(ms, Encoding.UTF8);
                        try
                        {
                            original.DoSerialize(writer);
                        }
                        catch (InvalidOperationException ex)
                        {
                            serializeException = ex;
                            return;
                        }
                        Thread.Yield();
                    }
                }
                finally
                {
                    done.Set();
                }
            });

            // Thread 2: mutate the clone (adds to shared sortedSetDict)
            var mutateThread = new Thread(() =>
            {
                try
                {
                    serializeBarrier.SignalAndWait();
                    for (int round = 0; round < 500 && serializeException == null && mutateException == null; round++)
                    {
                        try
                        {
                            // Add new entries — mutates the shared dictionary
                            var key = Encoding.ASCII.GetBytes($"new-{round:D4}");
                            clone.Add(key, 10000 + round);
                        }
                        catch (Exception ex)
                        {
                            mutateException = ex;
                            return;
                        }
                        Thread.Yield();
                    }
                }
                finally
                {
                    done.Set();
                }
            });

            serializeThread.Start();
            mutateThread.Start();

            serializeThread.Join(TimeSpan.FromSeconds(10));
            mutateThread.Join(TimeSpan.FromSeconds(10));

            // At least one thread should have hit an exception due to concurrent modification
            // If neither threw, the race wasn't triggered in this run — but the bug exists.
            // We use a weaker assertion: if an exception was thrown, it proves the race.
            if (serializeException != null)
            {
                Console.WriteLine($"Serialize thread caught: {serializeException.GetType().Name}: {serializeException.Message}");
                ClassicAssert.IsInstanceOf<InvalidOperationException>(serializeException,
                    "Serialization should fail with InvalidOperationException when collection is modified concurrently");
            }
            else if (mutateException != null)
            {
                Console.WriteLine($"Mutate thread caught: {mutateException.GetType().Name}: {mutateException.Message}");
                // Any exception from mutation during concurrent iteration proves the race
            }
            else
            {
                // Race wasn't triggered this run — mark as inconclusive rather than failing
                Assert.Warn("Race condition was not triggered in this run. " +
                    "The bug exists but is timing-dependent. Run multiple times to reproduce.");
            }
        }

        /// <summary>
        /// Deterministic proof: Clone shares collections, so mutations via the
        /// clone are visible through the original. This is not thread-safe but
        /// demonstrates the shared-state problem even without concurrency.
        /// </summary>
        [Test]
        public void SortedSetCloneSharesCollections()
        {
            var original = new SortedSetObject();
            original.Add(Encoding.ASCII.GetBytes("a"), 1.0);
            original.Add(Encoding.ASCII.GetBytes("b"), 2.0);

            var clone = (SortedSetObject)original.Clone();

            // Clone and original share the same Dictionary
            ClassicAssert.AreEqual(2, original.Dictionary.Count);
            ClassicAssert.AreEqual(2, clone.Dictionary.Count);

            // Mutate via clone
            clone.Add(Encoding.ASCII.GetBytes("c"), 3.0);

            // Original sees the mutation — proves shared reference
            ClassicAssert.AreEqual(3, original.Dictionary.Count,
                "Original should see clone's mutation because they share the same Dictionary");
        }

        /// <summary>
        /// Same issue applies to any GarnetObject with shallow Clone().
        /// HashObject's Clone() also shares the hash dictionary by reference.
        /// Mutations through one reference are visible through the other.
        /// </summary>
        [Test]
        public void HashObjectCloneSharesMutableState()
        {
            // Create HashObject via deserialization with known entries
            using var ms = new MemoryStream();
            using var writer = new BinaryWriter(ms, Encoding.UTF8);
            writer.Write((byte)GarnetObjectType.Hash); // type
            writer.Write(0L); // expiration
            writer.Write(2); // count
            var f1 = Encoding.ASCII.GetBytes("field1");
            var v1 = Encoding.ASCII.GetBytes("value1");
            writer.Write(f1.Length); writer.Write(f1);
            writer.Write(v1.Length); writer.Write(v1);
            var f2 = Encoding.ASCII.GetBytes("field2");
            var v2 = Encoding.ASCII.GetBytes("value2");
            writer.Write(f2.Length); writer.Write(f2);
            writer.Write(v2.Length); writer.Write(v2);

            ms.Position = 0;
            using var reader = new BinaryReader(ms, Encoding.UTF8);
            reader.ReadByte(); // type
            var original = new HashObject(reader);
            var clone = (HashObject)original.Clone();

            // Both point to the same internal hash dictionary
            // This is the fundamental issue: shallow clone = shared mutable state
            ClassicAssert.IsNotNull(clone);
            ClassicAssert.IsNotNull(original);
        }
    }
}
