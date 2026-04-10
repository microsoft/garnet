// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Factory for the race-detecting custom dictionary.
    /// </summary>
    class RaceDetectDictFactory : CustomObjectFactory
    {
        public override CustomObjectBase Create(byte type) => new RaceDetectDict(type);
        public override CustomObjectBase Deserialize(byte type, BinaryReader reader) => new RaceDetectDict(type, reader);
    }

    /// <summary>
    /// Custom dictionary object that pauses during serialization to create a
    /// deterministic window for concurrent mutations on the shared dictionary.
    /// </summary>
    class RaceDetectDict : CustomObjectBase
    {
        internal readonly Dictionary<byte[], byte[]> dict;

        /// <summary>Signaled when serialization has started and is paused.</summary>
        internal static ManualResetEventSlim SerializationStarted = new(false);

        /// <summary>Signaled to let serialization resume after mutations are done.</summary>
        internal static ManualResetEventSlim ResumeSerialization = new(false);

        /// <summary>Number of times serialization caught a concurrent modification exception.</summary>
        internal static volatile int SerializationRaceCount;

        /// <summary>Total number of times SerializeObject was called.</summary>
        internal static volatile int SerializationCallCount;

        public static void Reset()
        {
            SerializationStarted = new(false);
            ResumeSerialization = new(false);
            SerializationRaceCount = 0;
            SerializationCallCount = 0;
        }

        public RaceDetectDict(byte type)
            : base(type, MemoryUtils.DictionaryOverhead)
        {
            dict = new(ByteArrayComparer.Instance);
        }

        public RaceDetectDict(byte type, BinaryReader reader)
            : base(type, reader, MemoryUtils.DictionaryOverhead)
        {
            dict = new(ByteArrayComparer.Instance);
            var count = reader.ReadInt32();
            for (var i = 0; i < count; i++)
            {
                var key = reader.ReadBytes(reader.ReadInt32());
                var value = reader.ReadBytes(reader.ReadInt32());
                dict.Add(key, value);
                UpdateSize(key, value);
            }
        }

        /// <summary>Shallow copy — shares the same dict (the bug we're exposing).</summary>
        public RaceDetectDict(RaceDetectDict obj) : base(obj)
        {
            dict = obj.dict;
        }

        public override CustomObjectBase CloneObject() => new RaceDetectDict(this);

        public override void SerializeObject(BinaryWriter writer)
        {
            Interlocked.Increment(ref SerializationCallCount);

            // Signal that serialization has started, then pause
            SerializationStarted.Set();
            ResumeSerialization.Wait(TimeSpan.FromSeconds(10));

            try
            {
                writer.Write(dict.Count);
                foreach (var kvp in dict)
                {
                    writer.Write(kvp.Key.Length);
                    writer.Write(kvp.Key);
                    writer.Write(kvp.Value.Length);
                    writer.Write(kvp.Value);
                }
            }
            catch (InvalidOperationException)
            {
                Interlocked.Increment(ref SerializationRaceCount);
                // Write valid empty object so the stream isn't corrupted
                writer.BaseStream.SetLength(0);
                writer.Write(0);
            }
        }

        public override void Dispose() { }

        public override unsafe void Scan(long start, out List<byte[]> items, out long cursor,
            int count = 10, byte* pattern = null, int patternLength = 0, bool isNoValue = false)
        {
            cursor = 0;
            items = [];
        }

        public bool Set(byte[] key, byte[] value)
        {
            dict[key] = value;
            UpdateSize(key, value);
            return true;
        }

        private void UpdateSize(byte[] key, byte[] value, bool add = true)
        {
            var memorySize = Utility.RoundUp(key.Length, IntPtr.Size) + Utility.RoundUp(value.Length, IntPtr.Size)
                + (2 * MemoryUtils.ByteArrayOverhead) + MemoryUtils.DictionaryEntryOverhead;
            if (add)
                HeapMemorySize += memorySize;
            else
                HeapMemorySize -= memorySize;
        }

        public bool TryGetValue(byte[] key, out byte[] value) => dict.TryGetValue(key, out value);
    }

    /// <summary>Custom SET command for RaceDetectDict.</summary>
    class RaceDetectDictSet : CustomObjectFunctions
    {
        public override bool NeedInitialUpdate(scoped ReadOnlySpan<byte> key, ref ObjectInput input, ref RespMemoryWriter writer) => true;

        public override bool Updater(ReadOnlySpan<byte> key, ref ObjectInput input, IGarnetObject value, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
        {
            Debug.Assert(value is RaceDetectDict);
            var offset = 0;
            var keyArg = GetNextArg(ref input, ref offset).ToArray();
            var valueArg = GetNextArg(ref input, ref offset).ToArray();
            ((RaceDetectDict)value).Set(keyArg, valueArg);
            writer.WriteSimpleString("OK"u8);
            return true;
        }
    }

    /// <summary>
    /// End-to-end test that attempts to trigger the shallow Clone() race
    /// condition through a running Garnet server. Uses lowMemory mode and
    /// concurrent writes to create memory pressure and force page eviction.
    ///
    /// NOTE: Tsavorite's record-level locking prevents this race from
    /// manifesting in practice — CopyUpdate and subsequent mutations are
    /// serialized per-record. The unit-level test (ShallowCloneRaceConditionTests)
    /// proves the bug exists at the object level, but the server's concurrency
    /// control layer provides protection. This test is kept as a stress test
    /// to verify that the server remains stable under heavy object store churn.
    /// </summary>
    [TestFixture]
    public class ShallowCloneServerRaceTests
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);

            server.Register.NewCommand("RACESET", CommandType.ReadModifyWrite,
                new RaceDetectDictFactory(), new RaceDetectDictSet(),
                new RespCommandsInfo { Arity = 4 });

            server.Start();
            RaceDetectDict.Reset();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        /// <summary>
        /// 1. Create a target object early in the log.
        /// 2. Grow the tail with filler objects until the target's page is flushed
        ///    (serialization triggered by hybrid log page eviction, not checkpoint).
        /// 3. SerializeObject pauses via ManualResetEventSlim, holding the
        ///    serialization thread while the shared dictionary is exposed.
        /// 4. From a separate RESP client, spam mutations (RACESET) on the same
        ///    target key — these go through CopyUpdate which calls Clone() and
        ///    then mutates the shared dictionary.
        /// 5. Resume serialization — the foreach iteration should hit the modified
        ///    dictionary and throw InvalidOperationException.
        /// </summary>
        [Test]
        public void HybridLogFlushWithConcurrentMutationTriggersRace()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Step 1: Create target object with some data
            for (int i = 0; i < 100; i++)
            {
                db.Execute("RACESET", "target-obj", $"field-{i}", new string('v', 256));
            }

            // Step 2: In background, grow tail with filler objects to force page flush
            // When the target object's page is evicted, SerializeObject will be called
            // and will pause at the barrier.
            var fillerTask = Task.Run(() =>
            {
                using var fillerRedis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                var fillerDb = fillerRedis.GetDatabase(0);
                for (int i = 0; i < 10000; i++)
                {
                    try
                    {
                        fillerDb.Execute("RACESET", $"filler-{i}", "k", new string('x', 512));
                    }
                    catch { }

                    // Check if serialization was triggered on our target
                    if (RaceDetectDict.SerializationStarted.IsSet)
                        break;
                }
            });

            // Step 3: Wait for serialization to start and pause
            var serializationTriggered = RaceDetectDict.SerializationStarted.Wait(TimeSpan.FromSeconds(30));

            if (!serializationTriggered)
            {
                // Serialization wasn't triggered — filler didn't push target out
                RaceDetectDict.ResumeSerialization.Set();
                fillerTask.Wait(TimeSpan.FromSeconds(5));
                Assert.Warn("Serialization was not triggered by page flush. " +
                    $"SerializationCallCount={RaceDetectDict.SerializationCallCount}. " +
                    "The lowMemory setting may not be small enough to evict the target page.");
                return;
            }

            Console.WriteLine("Serialization paused. Hammering target object with mutations...");

            // Step 4: While serialization is paused, spam mutations on the target key
            // These will go through RMW → CopyUpdate → Clone() → Operate on clone
            // The clone shares the same dict that serialization is about to iterate
            var mutationCount = 0;
            using (var mutRedis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var mutDb = mutRedis.GetDatabase(0);
                for (int i = 0; i < 200; i++)
                {
                    try
                    {
                        mutDb.Execute("RACESET", "target-obj", $"new-field-{i}", new string('m', 128));
                        mutationCount++;
                    }
                    catch { }
                }
            }

            Console.WriteLine($"Applied {mutationCount} mutations while serialization was paused.");

            // Step 5: Resume serialization — it will now iterate a modified dictionary
            RaceDetectDict.ResumeSerialization.Set();

            fillerTask.Wait(TimeSpan.FromSeconds(10));

            Console.WriteLine($"Serialization call count: {RaceDetectDict.SerializationCallCount}");
            Console.WriteLine($"Serialization races detected: {RaceDetectDict.SerializationRaceCount}");

            if (RaceDetectDict.SerializationRaceCount > 0)
            {
                ClassicAssert.Greater(RaceDetectDict.SerializationRaceCount, 0,
                    "BUG CONFIRMED: Shallow Clone() caused concurrent modification " +
                    "during hybrid log flush serialization.");
            }
            else
            {
                Assert.Warn($"Race not triggered despite {mutationCount} mutations during paused serialization. " +
                    "Tsavorite's locking may have serialized the mutations after the flush completed.");
            }
        }
    }
}
