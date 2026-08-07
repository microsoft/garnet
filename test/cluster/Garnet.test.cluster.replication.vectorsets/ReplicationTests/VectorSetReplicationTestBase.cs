// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Shared harness for Vector Set replication tests.
    /// </summary>
    public abstract class VectorSetReplicationTestBase : TestBase
    {
        /// <summary>Length in bytes of an XB8 vector.</summary>
        protected const int VectorDimensions = 64;

        protected ClusterTestContext context;

        protected readonly int timeout = (int)TimeSpan.FromSeconds(15).TotalSeconds;

        /// <summary>Elements written through PopulateVectorSet, used for source-vs-target checks.</summary>
        private readonly Dictionary<string, List<byte[]>> writtenElements = [];

        /// <summary>Raise this in a fixture to get more detail while debugging a replication failure.</summary>
        protected virtual LogLevel MonitorLogLevel => LogLevel.Error;

        [SetUp]
        public virtual void Setup()
        {
            writtenElements.Clear();

            context = new ClusterTestContext();
            context.Setup(new Dictionary<string, LogLevel> { [TestContext.CurrentContext.Test.MethodName] = MonitorLogLevel });
        }

        [TearDown]
        public virtual void TearDown()
        {
            context?.TearDown();
        }

        #region cluster formation

        /// <summary>
        /// Waits until nodeIndex serves key and has drained any Vector Set replay queued behind it.
        /// </summary>
        protected void WaitUntilServes(int nodeIndex, string key)
        {
            context.clusterTestUtils.WaitUntilServes(nodeIndex, key, context.logger);
            WaitForVectorReplay(nodeIndex);
        }

        #endregion

        #region vector set operations

        /// <summary>
        /// Adds deterministic XB8 elements and records them so targets can be compared with the
        /// actual writes.
        /// </summary>
        protected void PopulateVectorSet(int nodeIndex, string key, int count, int seed)
        {
            var endpoint = context.clusterTestUtils.GetEndPoint(nodeIndex);
            var r = new Random(seed);

            if (!writtenElements.TryGetValue(key, out var elements))
            {
                writtenElements[key] = elements = [];
            }

            for (var i = 0; i < count; i++)
            {
                var vector = new byte[VectorDimensions];
                r.NextBytes(vector);

                var element = new byte[4];
                BinaryPrimitives.WriteInt32LittleEndian(element, elements.Count);

                var added = (int)context.clusterTestUtils.Execute(endpoint, "VADD", [key, "XB8", vector, element, "XPREQ8"], skipLogging: true);
                ClassicAssert.AreEqual(1, added, $"VADD of element {i} into '{key}' should have inserted a new element");

                elements.Add(element);
            }
        }

        private IReadOnlyList<byte[]> ElementsWrittenTo(string key) => writtenElements.TryGetValue(key, out var e) ? e : [];

        /// <summary>Reads VINFO size.</summary>
        protected long VectorSetSize(int nodeIndex, string key)
        {
            var reply = context.clusterTestUtils.Execute(context.clusterTestUtils.GetEndPoint(nodeIndex), "VINFO", [key], logger: context.logger);

            // Execute returns failures as bulk strings, so non-array replies mean the read failed.
            ClassicAssert.AreEqual(ResultType.Array, reply.Resp2Type, $"VINFO on '{key}' at node {nodeIndex} did not return an array: {reply}");

            var fields = (RedisValue[])reply;
            ClassicAssert.IsNotNull(fields, $"VINFO on '{key}' at node {nodeIndex} returned a nil array, so that node does not hold the Vector Set at all");

            for (var i = 0; i + 1 < fields.Length; i += 2)
            {
                if (((string)fields[i]).Equals("size", StringComparison.OrdinalIgnoreCase))
                    return (long)fields[i + 1];
            }

            Assert.Fail($"VINFO reply for '{key}' had no 'size' field: [{string.Join(", ", fields.Select(static f => (string)f))}]");
            return -1;
        }

        private long VectorSetDimensions(int nodeIndex, string key)
        {
            var reply = context.clusterTestUtils.Execute(context.clusterTestUtils.GetEndPoint(nodeIndex), "VDIM", [key], logger: context.logger);
            ClassicAssert.AreEqual(ResultType.Integer, reply.Resp2Type, $"VDIM on '{key}' at node {nodeIndex} did not return an integer: {reply}");

            return (long)reply;
        }

        /// <summary>Returns the stored embedding for an element, or an empty array when absent.</summary>
        private string[] ElementEmbedding(int nodeIndex, string key, byte[] element)
        {
            var reply = context.clusterTestUtils.Execute(context.clusterTestUtils.GetEndPoint(nodeIndex), "VEMB", [key, element], skipLogging: true);
            ClassicAssert.AreEqual(ResultType.Array, reply.Resp2Type, $"VEMB on '{key}' at node {nodeIndex} did not return an array: {reply}");

            return (string[])reply;
        }

        #endregion

        #region assertions

        /// <summary>
        /// Compares cardinality, dimensions, and per-element embeddings against the elements
        /// actually written; the embedding sweep catches correct counts with wrong vectors.
        /// </summary>
        private void AssertVectorSetsMatch(int sourceIndex, int targetIndex, string key)
        {
            var expected = ElementsWrittenTo(key);
            ClassicAssert.Greater(expected.Count, 0, $"no elements were recorded for '{key}'; the test is asserting nothing");

            WaitForReplication(sourceIndex, targetIndex);

            var sourceSize = VectorSetSize(sourceIndex, key);
            var targetSize = VectorSetSize(targetIndex, key);

            ClassicAssert.AreEqual(expected.Count, sourceSize, $"node {sourceIndex} lost elements of '{key}'");
            ClassicAssert.AreEqual(sourceSize, targetSize, $"node {targetIndex} disagrees with node {sourceIndex} on the cardinality of '{key}'");

            ClassicAssert.AreEqual(VectorSetDimensions(sourceIndex, key), VectorSetDimensions(targetIndex, key), $"node {targetIndex} disagrees with node {sourceIndex} on the dimensionality of '{key}'");

            var missing = new List<int>();
            var mismatched = new List<int>();

            for (var i = 0; i < expected.Count; i++)
            {
                var element = expected[i];
                var targetEmbedding = ElementEmbedding(targetIndex, key, element);

                if (targetEmbedding.Length == 0)
                {
                    missing.Add(i);
                    continue;
                }

                var sourceEmbedding = ElementEmbedding(sourceIndex, key, element);
                if (!sourceEmbedding.SequenceEqual(targetEmbedding))
                    mismatched.Add(i);
            }

            ClassicAssert.IsEmpty(missing, $"{missing.Count} of {expected.Count} elements VADDed into '{key}' on node {sourceIndex} are missing on node {targetIndex} (first few: {string.Join(", ", missing.Take(10))})");
            ClassicAssert.IsEmpty(mismatched, $"{mismatched.Count} of {expected.Count} elements of '{key}' have a different embedding on node {targetIndex} than on node {sourceIndex} (first few: {string.Join(", ", mismatched.Take(10))})");
        }

        /// <summary>
        /// AOF offsets are insufficient on their own: replicas queue VADDs onto VectorManager's replay
        /// channel, so offset sync can run ahead of index state.
        /// </summary>
        private void WaitForVectorReplay(int nodeIndex)
            => GetStoreWrapper(context.nodes[nodeIndex]).DefaultDatabase.VectorManager.WaitForVectorOperationsToComplete();

        /// <summary>
        /// Waits until targetIndex has caught up with sourceIndex, both in AOF offset and in Vector Set
        /// replay, so callers cannot observe one without the other.
        /// </summary>
        protected void WaitForReplication(int sourceIndex, int targetIndex)
        {
            context.clusterTestUtils.WaitForReplicaAofSync(sourceIndex, targetIndex, logger: context.logger);
            WaitForVectorReplay(sourceIndex);
            WaitForVectorReplay(targetIndex);
        }

        /// <summary>
        /// Reads the persisted DiskANN handle via Read_MainStore
        /// </summary>
        protected nint ReadPersistedIndexPtr(int nodeIndex, string key)
        {
            ReadPersistedIndexFields(nodeIndex, key, out _, out var indexPtr);
            return indexPtr;
        }

        /// <summary>Reads the persisted context id for a Vector Set.</summary>
        protected ulong ReadPersistedContext(int nodeIndex, string key)
        {
            ReadPersistedIndexFields(nodeIndex, key, out var ctx, out _);
            return ctx;
        }

        private void ReadPersistedIndexFields(int nodeIndex, string key, out ulong ctx, out nint indexPtr)
        {
            var storeWrapper = GetStoreWrapper(context.nodes[nodeIndex]);
            var db = storeWrapper.DefaultDatabase;

            using var storageSession = new StorageSession(storeWrapper, new ScratchBufferBuilder(), new ScratchBufferAllocator(), null, null, db.Id, readSessionState: null, db.VectorManager, null);

            Span<byte> keyBytes = stackalloc byte[Encoding.ASCII.GetByteCount(key)];
            _ = Encoding.ASCII.GetBytes(key, keyBytes);

            StringInput input = new(RespCommand.VINFO);
            input.parseState.Initialize(1);
            input.parseState.SetArgument(0, PinnedSpanByte.FromPinnedSpan(keyBytes));

            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSize];
            StringOutput output = new(SpanByteAndMemory.FromPinnedSpan(indexSpan));

            // Read without copying, so inspecting the record does not move it to the tail of the log.
            ReadOptions readOptions = new() { CopyOptions = ReadCopyOptions.None };
            var status = storageSession.stringBasicContext.Read((FixedSpanByteKey)keyBytes, ref input, ref output, ref readOptions);

            if (status.IsPending)
            {
                storageSession.stringBasicContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                ClassicAssert.IsTrue(completedOutputs.Next(), $"pending read of '{key}' on node {nodeIndex} produced no output");
                status = completedOutputs.Current.Status;
                output = completedOutputs.Current.Output;
                completedOutputs.Dispose();
            }

            ClassicAssert.IsTrue(status.Found, $"could not read the index record for '{key}' on node {nodeIndex}");
            ClassicAssert.IsTrue(output.SpanByteAndMemory.IsSpanByte, $"index record for '{key}' did not come back inline");
            ClassicAssert.AreEqual(VectorManager.IndexSize, output.SpanByteAndMemory.Length, $"value under '{key}' is not a Vector Set index record");

            VectorManager.ReadIndex(output.SpanByteAndMemory.Span, out ctx, out _, out _, out _, out _, out _, out _, out _, out indexPtr);
        }

        /// <summary>
        /// Counts live records by namespace, using the same snapshot iterator as the Vector Set
        /// cleanup task.
        /// </summary>
        private sealed class NamespaceCensus : IScanIteratorFunctions
        {
            public readonly Dictionary<ulong, int> RecordsByNamespace = [];

            public void OnException(Exception exception, long numberOfRecords) { }
            public bool OnStart(long beginAddress, long endAddress) => true;
            public void OnStop(bool completed, long numberOfRecords) { }

            /// <inheritdoc/>
            public bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                cursorRecordResult = CursorRecordResult.Accept;

                if (!logRecord.HasNamespace)
                    return true;

                var namespaceBytes = logRecord.NamespaceBytes;
                if (namespaceBytes.Length is not (sizeof(byte) or sizeof(uint)))
                    return true;

                var ns = VectorManager.ExtractContextFromNamespaces(namespaceBytes);
                RecordsByNamespace[ns] = RecordsByNamespace.TryGetValue(ns, out var seen) ? seen + 1 : 1;

                return true;
            }
        }

        /// <summary>
        /// Live namespaced records on a node, keyed by namespace. Uses the same snapshot iterator the
        /// Vector Set cleanup task uses, so it observes exactly the records cleanup would be able to see.
        /// </summary>
        protected Dictionary<ulong, int> CensusNamespacedRecords(int nodeIndex)
        {
            var storeWrapper = GetStoreWrapper(context.nodes[nodeIndex]);
            var db = storeWrapper.DefaultDatabase;

            db.VectorManager?.WaitForVectorOperationsToComplete();

            using var storageSession = new StorageSession(storeWrapper, new ScratchBufferBuilder(), new ScratchBufferAllocator(), null, null, db.Id, readSessionState: null, db.VectorManager, null);

            NamespaceCensus census = new();
            _ = storageSession.stringBasicContext.Session.IterateLookupSnapshot(ref census);

            return census.RecordsByNamespace;
        }

        /// <summary>
        /// Records held by a node under any namespace belonging to <paramref name="context"/>. A Vector Set
        /// owns <see cref="VectorManager.ContextStep"/> consecutive namespaces, so records are matched by
        /// masking down to the base context exactly as PostDropCleanupFunctions does.
        /// </summary>
        protected int RecordsHeldForContext(int nodeIndex, ulong context)
            => CensusNamespacedRecords(nodeIndex).Where(kv => (kv.Key & ~(VectorManager.ContextStep - 1)) == context).Sum(static kv => kv.Value);

        /// <summary>
        /// Spins until a node holds no records for a context, e.g. after a DEL whose data removal is
        /// completed asynchronously by the background cleanup task.
        /// </summary>
        protected void WaitForContextDrained(int nodeIndex, ulong context, string because)
        {
            for (var attempt = 0; attempt < 200; attempt++)
            {
                if (RecordsHeldForContext(nodeIndex, context) == 0)
                    return;

                Thread.Sleep(100);
            }

            Assert.Fail($"node {nodeIndex} still holds records for context {context} after 20s; {because}");
        }

        /// <summary>
        /// Asserts a node does not hold another node's DiskANN handle. A zero handle is valid
        /// after sanitization until lazy rebuild; only foreign non-zero pointers fail.
        /// </summary>
        protected void AssertOwnsItsIndex(int nodeIndex, int otherNodeIndex, string key)
        {
            var otherPtr = ReadPersistedIndexPtr(otherNodeIndex, key);
            var nodePtr = ReadPersistedIndexPtr(nodeIndex, key);

            if (nodePtr == nint.Zero || otherPtr == nint.Zero)
                return;

            ClassicAssert.AreNotEqual(otherPtr, nodePtr, $"node {nodeIndex} holds node {otherNodeIndex}'s DiskANN handle for '{key}' (0x{otherPtr:x}); it is pointing into another node's heap and will fault once the two are separate processes");
        }

        /// <summary>
        /// Asserts data equality before ownership because reads trigger lazy rebuild; checking
        /// ownership first can see the expected zero sanitized pointer.
        /// </summary>
        protected void AssertFullyReplicated(int sourceIndex, int targetIndex, string key)
        {
            AssertVectorSetsMatch(sourceIndex, targetIndex, key);

            var sourcePtr = ReadPersistedIndexPtr(sourceIndex, key);
            var targetPtr = ReadPersistedIndexPtr(targetIndex, key);

            ClassicAssert.AreNotEqual(nint.Zero, sourcePtr, $"node {sourceIndex} should have a live index for '{key}' after being read");
            ClassicAssert.AreNotEqual(nint.Zero, targetPtr, $"node {targetIndex} should have built its own index for '{key}' after being read");

            ClassicAssert.AreNotEqual(sourcePtr, targetPtr, $"node {targetIndex} holds node {sourceIndex}'s DiskANN handle for '{key}' (0x{sourcePtr:x}); it is pointing into another node's heap and will fault once the two are separate processes");
        }

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "storeWrapper")]
        private static extern ref StoreWrapper GetStoreWrapper(GarnetServer server);

        #endregion
    }
}