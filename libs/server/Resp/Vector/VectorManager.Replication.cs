// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Methods for managing the replication of Vector Sets from primaries to other replicas.
    /// 
    /// This is very bespoke because Vector Set operations are phrased as reads for most things, which
    /// bypasses Garnet's usual replication logic.
    /// </summary>
    public sealed partial class VectorManager
    {
        /// <summary>
        /// Represents a copy of a VADD being replayed during replication.
        /// </summary>
        private readonly record struct VADDReplicationState(Memory<byte> Key, uint Dims, uint ReduceDims, VectorValueType ValueType, Memory<byte> Values, Memory<byte> Element, VectorQuantType Quantizer, uint BuildExplorationFactor, Memory<byte> Attributes, uint NumLinks, VectorDistanceMetricType DistanceMetric)
        {
        }

        private int replicationReplayStarted;
        private CountingEventSlim replicationBlockEvent;
        private readonly Channel<VADDReplicationState> replicationReplayChannel;
        private readonly Task[] replicationReplayTasks;

        private CancellationToken replicationReplayCancellation;

        /// <summary>
        /// For testing purposes, are the replication replay tasks active.
        /// </summary>
        public bool AreReplicationTasksActive
        => replicationReplayCancellation.CanBeCanceled && replicationReplayTasks.Any(static r => !r.IsCompleted);

        /// <summary>
        /// Hook for <see cref="TaskManager"/> to request replication tasks start.
        /// 
        /// The underlying tasks may not be spun up until later, but the provided <see cref="CancellationToken"/> will be used
        /// if the yare.
        /// </summary>
        public async Task StartReplicationTasksAsync(CancellationToken cancellationToken)
        {
            try
            {
                await Task.Yield();

                replicationReplayCancellation = cancellationToken;

                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                }
                catch { }

                var abandoned = ResetReplayTasks();
                logger?.LogInformation("VectorManager replication cancellation abandoned {abandoned} VADDs", abandoned);
            }
            finally
            {
                replicationReplayCancellation = default;
            }
        }

        /// <summary>
        /// For replication purposes, we need a write against the main log.
        /// 
        /// But we don't actually want to do the (expensive) vector ops as part of a write.
        /// 
        /// So this fakes up a modify operation that we can then intercept as part of replication.
        /// 
        /// This the Primary part, on a Replica <see cref="HandleVectorSetAddReplication"/> runs.
        /// </summary>
        internal void ReplicateVectorSetAdd(ReadOnlySpan<byte> key, ref StringInput input, ref StringBasicContext context)
        {
            Debug.Assert(input.header.cmd == RespCommand.VADD, "Shouldn't be called with anything but VADD inputs");

            var inputCopy = input;
            inputCopy.arg1 = VADDAppendLogArg;

            var res = context.RMW((FixedSpanByteKey)key, ref inputCopy);

            if (res.IsPending)
            {
                CompletePending(ref res, ref context);
            }

            if (!res.IsCompletedSuccessfully)
            {
                logger?.LogCritical("Failed to inject replication write for VADD into log, result was {res}", res);
                throw new GarnetException("Couldn't synthesize Vector Set add operation for replication, data loss will occur");
            }
        }

        /// <summary>
        /// For replication purposes, we need a write against the main log.
        /// 
        /// But we don't actually want to do the (expensive) vector ops as part of a write.
        /// 
        /// So this fakes up a modify operation that we can then intercept as part of replication.
        /// 
        /// This the Primary part, on a Replica <see cref="HandleVectorSetRemoveReplication"/> runs.
        /// </summary>
        internal void ReplicateVectorSetRemove(ReadOnlySpan<byte> key, ReadOnlySpan<byte> element, ref StringInput input, ref StringBasicContext context)
        {
            Debug.Assert(input.header.cmd == RespCommand.VREM, "Shouldn't be called with anything but VREM inputs");

            var inputCopy = input;
            inputCopy.arg1 = VREMAppendLogArg;

            inputCopy.parseState.InitializeWithArgument(PinnedSpanByte.FromPinnedSpan(element));

            var res = context.RMW((FixedSpanByteKey)key, ref inputCopy);

            if (res.IsPending)
            {
                CompletePending(ref res, ref context);
            }

            if (!res.IsCompletedSuccessfully)
            {
                logger?.LogCritical("Failed to inject replication write for VREM into log, result was {res}", res);
                throw new GarnetException("Couldn't synthesize Vector Set remove operation for replication, data loss will occur");
            }
        }

        /// <summary>
        /// Vector Set adds are phrased as reads (once the index is created), so they require special handling.
        /// 
        /// Operations that are faked up by <see cref="ReplicateVectorSetAdd"/> running on the Primary get diverted here on a Replica.
        /// </summary>
        internal void HandleVectorSetAddReplication(
            StorageSession currentSession,
            Func<RespServerSession> obtainServerSession, ReadOnlySpan<byte> key, ref StringInput input)
        {
            if (input.arg1 == MigrateElementKeyLogArg)
            {
                // These are special, injecting by a PRIMARY applying migration operations
                // These get replayed on REPLICAs typically, though role changes might still cause these
                // to get replayed on now-primary nodes

                // Serialized len + ns + len + key in ReplicateMigratedElementKey
                var elementNamespaceAndKey = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;

                var elementNsLen = BinaryPrimitives.ReadInt32LittleEndian(elementNamespaceAndKey);
                var elementNsBytes = elementNamespaceAndKey.Slice(sizeof(int), elementNsLen);
                var elementKeyLen = BinaryPrimitives.ReadInt32LittleEndian(elementNamespaceAndKey[(sizeof(int) + elementNsLen)..]);
                var elementKeyBytes = elementNamespaceAndKey.Slice(sizeof(int) + elementNsLen + sizeof(int), elementKeyLen);

                var value = input.parseState.GetArgSliceByRef(1);

                Debug.Assert(elementNsBytes.Length == 1, "Longer length namespaces not supported");

                var ns = (ulong)elementNsBytes[0];

                // REPLICAs wouldn't have seen a reservation message, so allocate this on demand
                var ctx = ns & ~(ContextStep - 1);
                if (!contextMetadata.IsMigrating(ctx))
                {
                    var needsUpdate = false;

                    lock (this)
                    {
                        if (!contextMetadata.IsMigrating(ctx))
                        {
                            contextMetadata.MarkInUse(ctx, ushort.MaxValue);
                            contextMetadata.MarkMigrating(ctx);

                            needsUpdate = true;
                        }
                    }

                    if (needsUpdate)
                    {
                        UpdateContextMetadata(ref currentSession.vectorBasicContext);
                    }
                }

                HandleMigratedElementKey(ref currentSession.stringBasicContext, ref currentSession.vectorBasicContext, elementNsBytes, elementKeyBytes, value);
                return;
            }
            else if (input.arg1 == MigrateIndexKeyLogArg)
            {
                // These also injected by a PRIMARY applying migration operations

                var indexKey = input.parseState.GetArgSliceByRef(0);
                var value = input.parseState.GetArgSliceByRef(1);
                var context = MemoryMarshal.Cast<byte, ulong>(input.parseState.GetArgSliceByRef(2).Span)[0];

                // Most of the time a replica will have seen an element moving before now
                // but if you a migrate an EMPTY Vector Set that is not necessarily true
                //
                // So force reservation now
                if (!contextMetadata.IsMigrating(context))
                {
                    var needsUpdate = false;

                    lock (this)
                    {
                        if (!contextMetadata.IsMigrating(context))
                        {
                            contextMetadata.MarkInUse(context, ushort.MaxValue);
                            contextMetadata.MarkMigrating(context);

                            needsUpdate = true;
                        }
                    }

                    if (needsUpdate)
                    {
                        UpdateContextMetadata(ref currentSession.vectorBasicContext);
                    }
                }

                ActiveThreadSession = currentSession;
                try
                {
                    HandleMigratedIndexKey(null, null, indexKey, value);
                }
                finally
                {
                    ActiveThreadSession = null;
                }
                return;
            }

            Debug.Assert(input.arg1 == VADDAppendLogArg, "Unexpected operation during replication");

            // Undo mangling that got replication going
            var inputCopy = input;
            inputCopy.arg1 = default;

            // Copy key onto 
            var keyBytesArr = ArrayPool<byte>.Shared.Rent(key.Length);
            var keyBytes = keyBytesArr.AsMemory()[..key.Length];

            key.CopyTo(keyBytes.Span);

            var dims = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(0).Span);
            var reduceDims = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(1).Span);
            var valueType = MemoryMarshal.Read<VectorValueType>(input.parseState.GetArgSliceByRef(2).Span);
            var values = input.parseState.GetArgSliceByRef(3).Span;
            var element = input.parseState.GetArgSliceByRef(4).Span;
            var quantizer = MemoryMarshal.Read<VectorQuantType>(input.parseState.GetArgSliceByRef(5).Span);
            var buildExplorationFactor = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(6).Span);
            var attributes = input.parseState.GetArgSliceByRef(7).Span;
            var numLinks = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(8).Span);
            var distanceMetric = MemoryMarshal.Read<VectorDistanceMetricType>(input.parseState.GetArgSliceByRef(9).Span);

            // We have to make copies (and they need to be on the heap) to pass to background tasks
            var valuesBytes = ArrayPool<byte>.Shared.Rent(values.Length).AsMemory()[..values.Length];
            values.CopyTo(valuesBytes.Span);

            var elementBytes = ArrayPool<byte>.Shared.Rent(element.Length).AsMemory()[..element.Length];
            element.CopyTo(elementBytes.Span);

            var attributesBytes = ArrayPool<byte>.Shared.Rent(attributes.Length).AsMemory()[..attributes.Length];
            attributes.CopyTo(attributesBytes.Span);

            // Spin up replication replay tasks on first use
            if (replicationReplayStarted == 0)
            {
                if (Interlocked.CompareExchange(ref replicationReplayStarted, 1, 0) == 0)
                {
                    StartReplicationReplayTasks(this, obtainServerSession);
                }
            }

            // We need a running count of pending VADDs so WaitForVectorOperationsToComplete can work

            replicationBlockEvent.Increment();
            var queued = replicationReplayChannel.Writer.TryWrite(new(keyBytes, dims, reduceDims, valueType, valuesBytes, elementBytes, quantizer, buildExplorationFactor, attributesBytes, numLinks, distanceMetric));
            if (!queued)
            {
                replicationBlockEvent.Decrement();
            }

            static void StartReplicationReplayTasks(VectorManager self, Func<RespServerSession> obtainServerSession)
            {
                if (self.dbId != 0)
                {
                    throw new GarnetException($"Unexpected DB ({self.dbId}) in cluster mode, expected 0");
                }

                self.logger?.LogInformation("Starting {numTasks} replication tasks for VADDs", self.replicationReplayTasks.Length);

                for (var i = 0; i < self.replicationReplayTasks.Length; i++)
                {
                    self.replicationReplayTasks[i] = Task.Factory.StartNew(
                        async () =>
                        {
                            try
                            {
                                var reader = self.replicationReplayChannel.Reader;

                                SessionParseState reusableParseState = default;
                                reusableParseState.Initialize(11);

                                while (await reader.WaitToReadAsync(self.replicationReplayCancellation))
                                {
                                    // Allocate session for current batch, now so we stay on same managed thread
                                    using var allocatedSession = obtainServerSession();
                                    if (allocatedSession.activeDbId != self.dbId && !allocatedSession.TrySwitchActiveDatabaseSession(self.dbId))
                                    {
                                        throw new GarnetException($"Could not switch replication replay session to {self.dbId}, replication will fail");
                                    }

                                    while (reader.TryRead(out var entry))
                                    {
                                        try
                                        {
                                            try
                                            {
                                                ApplyVectorSetAdd(self, allocatedSession.storageSession, entry, ref reusableParseState);
                                            }
                                            finally
                                            {
                                                self.replicationBlockEvent.Decrement();
                                            }
                                        }
                                        catch
                                        {
                                            self.logger?.LogCritical(
                                                "Faulting ApplyVectorSetAdd ({key}, {dims}, {reducedDims}, {valueType}, 0x{values}, 0x{element}, {quantizer}, {bef}, {attributes}, {numLinks}",
                                                Encoding.UTF8.GetString(entry.Key.Span),
                                                entry.Dims,
                                                entry.ReduceDims,
                                                entry.ValueType,
                                                Convert.ToBase64String(entry.Values.Span),
                                                Convert.ToBase64String(entry.Values.Span),
                                                entry.Quantizer,
                                                entry.BuildExplorationFactor,
                                                Encoding.UTF8.GetString(entry.Attributes.Span),
                                                entry.NumLinks
                                            );

                                            throw;
                                        }
                                    }
                                }
                            }
                            catch (OperationCanceledException cancelEx)
                            {
                                self.logger?.LogInformation(cancelEx, "ReplicationReplayTask cancelled");
                            }
                            catch (Exception e)
                            {
                                self.logger?.LogCritical(e, "Unexpected abort of replication replay task");
                                throw;
                            }
                        }
                    )
                    .Unwrap();
                }
            }

            // Actually apply a replicated VADD
            static unsafe void ApplyVectorSetAdd(VectorManager self, StorageSession storageSession, VADDReplicationState state, ref SessionParseState reusableParseState)
            {
                var (keyBytes, dims, reduceDims, valueType, valuesBytes, elementBytes, quantizer, buildExplorationFactor, attributesBytes, numLinks, distanceMetric) = state;
                try
                {
                    Span<byte> indexSpan = stackalloc byte[IndexSizeBytes];

                    fixed (byte* keyPtr = keyBytes.Span)
                    fixed (byte* valuesPtr = valuesBytes.Span)
                    fixed (byte* elementPtr = elementBytes.Span)
                    fixed (byte* attributesPtr = attributesBytes.Span)
                    {
                        var key = SpanByte.FromPinnedPointer(keyPtr, keyBytes.Length);
                        var values = SpanByte.FromPinnedPointer(valuesPtr, valuesBytes.Length);
                        var element = SpanByte.FromPinnedPointer(elementPtr, elementBytes.Length);
                        var attributes = SpanByte.FromPinnedPointer(attributesPtr, attributesBytes.Length);

                        var indexBytes = stackalloc byte[IndexSizeBytes];

                        var dimsArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref dims, 1)));
                        var reduceDimsArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref reduceDims, 1)));
                        var valueTypeArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<VectorValueType, byte>(MemoryMarshal.CreateSpan(ref valueType, 1)));
                        var valuesArg = PinnedSpanByte.FromPinnedSpan(values);
                        var elementArg = PinnedSpanByte.FromPinnedSpan(element);
                        var quantizerArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<VectorQuantType, byte>(MemoryMarshal.CreateSpan(ref quantizer, 1)));
                        var buildExplorationFactorArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref buildExplorationFactor, 1)));
                        var attributesArg = PinnedSpanByte.FromPinnedSpan(attributes);
                        var numLinksArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref numLinks, 1)));
                        var distanceMetricArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<VectorDistanceMetricType, byte>(MemoryMarshal.CreateSpan(ref distanceMetric, 1)));

                        reusableParseState.InitializeWithArguments([dimsArg, reduceDimsArg, valueTypeArg, valuesArg, elementArg, quantizerArg, buildExplorationFactorArg, attributesArg, numLinksArg, distanceMetricArg]);

                        StringInput input = new(RespCommand.VADD, ref reusableParseState);

                        // Equivalent to VectorStoreOps.VectorSetAdd
                        //
                        // We still need locking here because the replays may proceed in parallel

                        using (self.ReadOrCreateVectorIndex(storageSession, key, ref input, indexSpan, out var status))
                        {
                            Debug.Assert(status == GarnetStatus.OK, "Replication should only occur when an add is successful, so index must exist");

                            var addRes = self.TryAdd(indexSpan, element, valueType, values, attributes, reduceDims, quantizer, buildExplorationFactor, numLinks, distanceMetric, out _);

                            if (addRes != VectorManagerResult.OK)
                            {
                                throw new GarnetException("Failed to add to vector set index during AOF sync, this should never happen but will cause data loss if it does");
                            }
                        }
                    }
                }
                finally
                {
                    if (MemoryMarshal.TryGetArray<byte>(keyBytes, out var toFree))
                    {
                        ArrayPool<byte>.Shared.Return(toFree.Array);
                    }

                    if (MemoryMarshal.TryGetArray(valuesBytes, out toFree))
                    {
                        ArrayPool<byte>.Shared.Return(toFree.Array);
                    }

                    if (MemoryMarshal.TryGetArray(elementBytes, out toFree))
                    {
                        ArrayPool<byte>.Shared.Return(toFree.Array);
                    }

                    if (MemoryMarshal.TryGetArray(attributesBytes, out toFree))
                    {
                        ArrayPool<byte>.Shared.Return(toFree.Array);
                    }
                }
            }
        }

        /// <summary>
        /// Cancels replication tasks, resetting enough state that they can be resumed by a future call to <see cref="HandleVectorSetAddReplication"/>.
        /// 
        /// Returns the number of abanded VADDs.
        /// </summary>
        private int ResetReplayTasks()
        {
            Task.WaitAll(replicationReplayTasks);
            Array.Fill(replicationReplayTasks, Task.CompletedTask);

            _ = Interlocked.Exchange(ref replicationReplayStarted, 0);

            var abandoned = 0;
            while (replicationReplayChannel.Reader.TryRead(out _))
            {
                replicationBlockEvent.Decrement();
                abandoned++;
            }

            return abandoned;
        }

        /// <summary>
        /// Shuts down replication tasks in a way where they _cannot_ be resumed in the future.
        /// 
        /// Intended for general Garnet shutdown.
        /// </summary>
        public void ShutdownReplayTasks()
        {
            _ = replicationReplayChannel.Writer.TryComplete();
            Task.WaitAll(replicationReplayTasks);

            _ = Interlocked.Exchange(ref replicationReplayStarted, -1);
        }

        /// <summary>
        /// Vector Set removes are phrased as reads (once the index is created), so they require special handling.
        /// 
        /// Operations that are faked up by <see cref="ReplicateVectorSetRemove"/> running on the Primary get diverted here on a Replica.
        /// </summary>
        internal void HandleVectorSetRemoveReplication(StorageSession storageSession, ReadOnlySpan<byte> key, ref StringInput input)
        {
            Span<byte> indexSpan = stackalloc byte[IndexSizeBytes];
            var element = input.parseState.GetArgSliceByRef(0);

            var inputCopy = input;
            inputCopy.arg1 = default;

            using (ReadVectorIndex(storageSession, key, ref inputCopy, indexSpan, out var status))
            {
                Debug.Assert(status == GarnetStatus.OK, "Replication should only occur when a remove is successful, so index must exist");

                var addRes = TryRemove(indexSpan, element.ReadOnlySpan);

                if (addRes != VectorManagerResult.OK)
                {
                    throw new GarnetException("Failed to remove from vector set index during AOF sync, this should never happen but will cause data loss if it does");
                }
            }
        }

        /// <summary>
        /// Wait until all ops passed to <see cref="HandleVectorSetAddReplication"/> have completed.
        /// </summary>
        public void WaitForVectorOperationsToComplete()
        {
            try
            {
                _ = replicationBlockEvent.Wait();
            }
            catch (ObjectDisposedException)
            {
                // This is possible during dispose
                //
                // Dispose already takes pains to drain everything before disposing, so this is safe to ignore
            }
        }
        // Helper to complete read/writes during vector set synthetic op goes async
        private static void CompletePending<TContext>(ref Status status, ref VectorBasicContext context)
        {
            _ = context.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            var more = completedOutputs.Next();
            Debug.Assert(more);
            status = completedOutputs.Current.Status;
            more = completedOutputs.Next();
            Debug.Assert(!more);
            completedOutputs.Dispose();
        }
    }
}