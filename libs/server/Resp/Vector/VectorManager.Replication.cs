// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.ExceptionServices;
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
        /// Represents a copied VADD or XVIMPORT term being replayed during replication.
        /// </summary>
        private readonly record struct VADDReplicationState(Memory<byte> Key, uint Dims, uint ReduceDims, VectorValueType ValueType, Memory<byte> Values, Memory<byte> Element, VectorQuantType Quantizer, uint BuildExplorationFactor, Memory<byte> Attributes, uint NumLinks, VectorDistanceMetricType DistanceMetric, uint? ImportTermType = null)
        {
        }

        private int replicationReplayStarted;
        private CountingEventSlim replicationBlockEvent;
        private readonly Channel<VADDReplicationState> replicationReplayChannel;
        private readonly Task[] replicationReplayTasks;
        private readonly object replicationReplayGate = new();
        private RespCommand replicationBatchCommand;
        private ExceptionDispatchInfo replicationReplayFailure;
        private int importReplayRequestsProcessed;
        internal int ImportReplayRequestsProcessed => Volatile.Read(ref importReplayRequestsProcessed);

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
                replicationReplayCancellation = cancellationToken;

                await Task.Yield();

                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
                }
                catch { }

                var abandoned = await ResetReplayTasksAsync().ConfigureAwait(false);
                logger?.LogInformation("VectorManager replication cancellation abandoned {abandoned} vector operations", abandoned);
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

            ExceptionInjectionHelper.ResetAndWait(ExceptionInjectionType.VectorSet_Pause_Before_Synthetic_Replication_Rmw);

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

            ExceptionInjectionHelper.ResetAndWait(ExceptionInjectionType.VectorSet_Pause_Before_Synthetic_Replication_Rmw);

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

        internal void ReplicateVectorSetSetAttribute(ReadOnlySpan<byte> key, ReadOnlySpan<byte> element, ReadOnlySpan<byte> attribute, ref StringInput input, ref StringBasicContext context)
        {
            Debug.Assert(input.header.cmd == RespCommand.VSETATTR, "Shouldn't be called with anything but VSETATTR inputs");

            var inputCopy = input;
            inputCopy.arg1 = VSETATTRAppendLogArg;

            inputCopy.parseState.InitializeWithArguments(PinnedSpanByte.FromPinnedSpan(element), PinnedSpanByte.FromPinnedSpan(attribute));

            ExceptionInjectionHelper.ResetAndWait(ExceptionInjectionType.VectorSet_Pause_Before_Synthetic_Replication_Rmw);

            var res = context.RMW((FixedSpanByteKey)key, ref inputCopy);

            if (res.IsPending)
            {
                CompletePending(ref res, ref context);
            }

            if (!res.IsCompletedSuccessfully)
            {
                logger?.LogCritical("Failed to inject replication write for VSETATTR into log, result was {res}", res);
                throw new GarnetException("Couldn't synthesize Vector Set attribute set operation for replication, data loss will occur");
            }
        }

        private static void ReplicateImportOperation(ReadOnlySpan<byte> key, ref StringInput input)
        {
            ref var context = ref ActiveThreadSession.stringBasicContext;
            var status = context.RMW((FixedSpanByteKey)key, ref input);
            if (status.IsPending)
            {
                CompletePending(ref status, ref context);
            }
            if (!status.IsCompletedSuccessfully)
            {
                throw new GarnetException("Could not log Vector Set import operation");
            }
        }

        private static void ReplicateVectorSetCreate(ReadOnlySpan<byte> key, uint dimensions, uint reduceDims,
            VectorQuantType quantizer, uint buildExplorationFactor, uint numLinks, VectorDistanceMetricType distanceMetric,
            bool hasQuantState, ReadOnlySpan<byte> quantState)
        {
#pragma warning disable IDE0302
            Span<uint> configuration = stackalloc uint[] { dimensions, reduceDims, (uint)quantizer, buildExplorationFactor, numLinks, (uint)distanceMetric };
#pragma warning restore IDE0302
            var input = new StringInput(RespCommand.XVCREATE);
            var configurationArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.AsBytes(configuration));
            if (hasQuantState)
            {
                input.parseState.InitializeWithArguments(configurationArg, PinnedSpanByte.FromPinnedSpan(quantState));
            }
            else
            {
                input.parseState.InitializeWithArgument(configurationArg);
            }
            ReplicateImportOperation(key, ref input);
        }

        private static void ReplicateVectorSetImport(ReadOnlySpan<byte> key, uint termType, ReadOnlySpan<byte> id, ReadOnlySpan<byte> value)
        {
            var input = new StringInput(RespCommand.XVIMPORT);
            input.parseState.InitializeWithArguments(
                PinnedSpanByte.FromPinnedSpan(MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref termType, 1))),
                PinnedSpanByte.FromPinnedSpan(id), PinnedSpanByte.FromPinnedSpan(value));
            ReplicateImportOperation(key, ref input);
        }

        private static void ReplicateVectorSetFinishImport(ReadOnlySpan<byte> key)
        {
            var input = new StringInput(RespCommand.XVIMPORT);
            ReplicateImportOperation(key, ref input);
        }

        internal void HandleVectorSetImportReplication(StorageSession session, Func<RespServerSession> obtainServerSession, ReadOnlySpan<byte> key, ref StringInput input)
        {
            if (!IsEnabled)
            {
                throw new GarnetException("Vector Set preview is disabled during import replay");
            }

            Debug.Assert(input.header.cmd is RespCommand.XVCREATE or RespCommand.XVIMPORT, "Only XVCREATE and XVIMPORT should be redirected here");

            WaitForImportFinalization(key);

            GarnetStatus status;
            VectorManagerResult result;
            ReadOnlySpan<byte> error;
            if (input.header.cmd == RespCommand.XVCREATE)
            {
                if (input.parseState.Count is not (1 or 2) || input.parseState.GetArgSliceByRef(0).Length != 6 * sizeof(uint))
                {
                    throw new GarnetException("Invalid XVCREATE AOF payload");
                }
                var configuration = MemoryMarshal.Cast<byte, uint>(input.parseState.GetArgSliceByRef(0).ReadOnlySpan);
                status = session.VectorSetCreate(PinnedSpanByte.FromPinnedSpan(key), (int)configuration[0], (int)configuration[1],
                    (VectorQuantType)configuration[2], (int)configuration[3], (int)configuration[4], (VectorDistanceMetricType)configuration[5],
                    input.parseState.Count == 2 ? input.parseState.GetArgSliceByRef(1) : null, out result, out error);
            }
            else if (input.parseState.Count == 0)
            {
                if (input.arg1 != 0)
                {
                    throw new GarnetException("Invalid XVIMPORT FINISH AOF payload");
                }
                status = session.VectorSetFinishImport(PinnedSpanByte.FromPinnedSpan(key), out result, out error);
            }
            else if (input.parseState.Count == 3 && input.parseState.GetArgSliceByRef(0).Length == sizeof(uint))
            {
                var termType = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(0).ReadOnlySpan);
                var id = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;
                var value = input.parseState.GetArgSliceByRef(2).ReadOnlySpan;
                var keyCopy = ArrayPool<byte>.Shared.Rent(key.Length).AsMemory(0, key.Length);
                var idCopy = ArrayPool<byte>.Shared.Rent(id.Length).AsMemory(0, id.Length);
                var valueCopy = ArrayPool<byte>.Shared.Rent(value.Length).AsMemory(0, value.Length);
                key.CopyTo(keyCopy.Span);
                id.CopyTo(idCopy.Span);
                value.CopyTo(valueCopy.Span);
                QueueVectorReplay(new(keyCopy, 0, 0, default, valueCopy, idCopy, default, 0, default, 0, default, termType), obtainServerSession);
                return;
            }
            else
            {
                throw new GarnetException("Invalid XVIMPORT AOF payload");
            }
            if (status != GarnetStatus.OK || result != VectorManagerResult.OK)
            {
                throw new GarnetException($"Vector Set import replay failed: {status}, {result}, {Encoding.UTF8.GetString(error)}");
            }
        }

        /// <summary>
        /// Maps a context chosen by a PRIMARY for an in-flight migration onto the context this node actually
        /// stores that migrated Vector Set under.
        /// 
        /// Guarded by <c>lock (this)</c>, like the rest of the context metadata.
        /// </summary>
        private readonly Dictionary<ulong, ulong> migratedContextRemap = new();

        /// <summary>
        /// The local contexts already claimed as the target of a remapping in <see cref="migratedContextRemap"/>.
        /// 
        /// A claimed context is marked migrating, and a later incoming context whose own value happens to equal
        /// that target would otherwise read that mark as its own interrupted migration and adopt the context
        /// directly, leaving two migrated Vector Sets sharing one context.
        /// 
        /// Guarded by <c>lock (this)</c>, like the rest of the context metadata.
        /// </summary>
        private readonly HashSet<ulong> migratedContextTargets = [];

        /// <summary>
        /// Discard every in-flight migration remapping.
        /// 
        /// Callers must already hold <c>lock (this)</c>, which is what guards the map.
        /// </summary>
        private void ClearMigratedContextRemap()
        {
            Debug.Assert(Monitor.IsEntered(this), "Migration remap is guarded by lock (this)");

            migratedContextRemap.Clear();
            migratedContextTargets.Clear();
        }

        /// <summary>
        /// Determine which context migrated data should be written to on this node.
        /// 
        /// Contexts are assigned independently by each node, so a context that is free on the PRIMARY which chose it
        /// can still be occupied here - most commonly by a deleted Vector Set whose element data has not finished being
        /// cleaned up.  Storing the migrated data there anyway would leave two Vector Sets sharing a context, corrupting
        /// both, so in that case the migration is steered onto a context that is free locally.
        /// 
        /// The mapping is remembered so that every record belonging to a migration resolves identically, and is
        /// discarded once the index key for that migration arrives.
        /// </summary>
        private ulong ResolveMigratedContext(StorageSession currentSession, ulong migratedContext)
        {
            ulong localContext;

            lock (this)
            {
                if (migratedContextRemap.TryGetValue(migratedContext, out localContext))
                {
                    return localContext;
                }

                var (contextIndex, contextValue) = ContextMetadata.DecomposeContext(migratedContext);

                // A context past the end of the local metadata has never been allocated on this node,
                // so it cannot be adopted as-is
                var contextKnownLocally = contextIndex < contextMetadatas.Length;

                if (contextKnownLocally
                    && !migratedContextTargets.Contains(migratedContext)
                    && contextMetadatas[contextIndex].IsMigrating(contextIndex != 0, contextValue))
                {
                    // Already reserved for a migration, which is how a migration that was interrupted and resumed looks
                    migratedContextRemap[migratedContext] = migratedContext;
                    _ = migratedContextTargets.Add(migratedContext);

                    return migratedContext;
                }

                if (!contextKnownLocally || contextMetadatas[contextIndex].IsInUse(contextIndex != 0, contextValue))
                {
                    localContext = NextVectorSetContext(ushort.MaxValue);

                    (contextIndex, contextValue) = ContextMetadata.DecomposeContext(localContext);
                }
                else
                {
                    localContext = migratedContext;

                    contextMetadatas[contextIndex].MarkInUse(contextIndex != 0, contextValue, ushort.MaxValue);
                }

                contextMetadatas[contextIndex].MarkMigrating(contextIndex != 0, contextValue);

                migratedContextRemap[migratedContext] = localContext;
                _ = migratedContextTargets.Add(localContext);

                _ = dirtyContextMetadatas.Add(contextIndex);
            }

            UpdateContextMetadata(ref currentSession.vectorBasicContext);

            return localContext;
        }

        /// <summary>
        /// For testing purposes, resolve a context the way an incoming migrated record would.
        /// </summary>
        public ulong ResolveMigratedContextForTest(ulong migratedContext)
        {
            using var session = (RespServerSession)getTempSession();

            return ResolveMigratedContext(session.storageSession, migratedContext);
        }

        /// <summary>
        /// Vector Set adds are phrased as reads (once the index is created), so they require special handling.
        /// 
        /// Operations that are faked up by <see cref="ReplicateVectorSetAdd"/> running on the Primary get diverted here on a Replica.
        /// </summary>
        internal void HandleVectorSetAddReplication(
            StorageSession currentSession,
            Func<RespServerSession> obtainServerSession,
            ReadOnlySpan<byte> key,
            ref StringInput input
        )
        {
            WaitForImportFinalization(key);

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

                Debug.Assert(elementNsLen == 4, "Should always receive a 4-byte namespace");
                ulong ns = BinaryPrimitives.ReadUInt32LittleEndian(elementNsBytes);

                // REPLICAs wouldn't have seen a reservation message, so allocate this on demand
                var migratedContext = ns & ~(ContextStep - 1);
                var localContext = ResolveMigratedContext(currentSession, migratedContext);

                scoped var localNamespaceBytes = elementNsBytes;

                Span<byte> remappedNamespaceBytes = stackalloc byte[sizeof(uint)];
                if (localContext != migratedContext)
                {
                    // Preserve the sub-namespace within the block, only the block itself moves
                    BinaryPrimitives.WriteUInt32LittleEndian(remappedNamespaceBytes, (uint)(localContext + (ns - migratedContext)));

                    localNamespaceBytes = remappedNamespaceBytes;
                }

                HandleMigratedElementKey(ref currentSession.stringBasicContext, ref currentSession.vectorBasicContext, localNamespaceBytes, elementKeyBytes, value);
                return;
            }
            else if (input.arg1 == MigrateIndexKeyLogArg)
            {
                // These also injected by a PRIMARY applying migration operations

                var indexKey = input.parseState.GetArgSliceByRef(0);
                var value = input.parseState.GetArgSliceByRef(1);
                var expirationTicks = MemoryMarshal.Cast<byte, long>(input.parseState.GetArgSliceByRef(2).Span)[0];
                var context = MemoryMarshal.Cast<byte, ulong>(input.parseState.GetArgSliceByRef(3).Span)[0];

                // Most of the time a replica will have seen an element moving before now
                // but if you a migrate an EMPTY Vector Set that is not necessarily true
                //
                // So force reservation now
                var migratedContext = context & ~(ContextStep - 1);
                var localContext = ResolveMigratedContext(currentSession, migratedContext);

                scoped var localIndexValue = value.ReadOnlySpan;

                Span<byte> remappedIndexValue = stackalloc byte[Index.Size];
                if (localContext != migratedContext)
                {
                    // The index records its own context, so it has to be rewritten to match where the data landed
                    localIndexValue.CopyTo(remappedIndexValue);
                    SetContextForMigration(remappedIndexValue, localContext + (context - migratedContext));

                    localIndexValue = remappedIndexValue;
                }

                ActiveThreadSession = currentSession;
                try
                {
                    DateTime? expiration = expirationTicks == 0 ? null : new DateTime(expirationTicks, DateTimeKind.Utc);
                    HandleMigratedIndexKey(null, null, indexKey, localIndexValue, expiration);
                }
                finally
                {
                    ActiveThreadSession = null;
                }

                // Element records for a migration are all transmitted, applied, and acknowledged before its index
                // key is sent, so no further record can name this context until a later migration reserves it
                lock (this)
                {
                    _ = migratedContextRemap.Remove(migratedContext);
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

            QueueVectorReplay(new(keyBytes, dims, reduceDims, valueType, valuesBytes, elementBytes, quantizer, buildExplorationFactor, attributesBytes, numLinks, distanceMetric), obtainServerSession);
        }

        private void QueueVectorReplay(VADDReplicationState state, Func<RespServerSession> obtainServerSession)
        {
            lock (replicationReplayGate)
            {
                var command = state.ImportTermType.HasValue ? RespCommand.XVIMPORT : RespCommand.VADD;
                try
                {
                    if (replicationReplayCancellation.IsCancellationRequested || replicationReplayStarted < 0)
                    {
                        throw new GarnetException("Vector replay is stopped");
                    }
                    if (replicationBatchCommand != command)
                    {
                        WaitForVectorOperationsToComplete();
                        replicationBatchCommand = command;
                    }
                    replicationReplayFailure?.Throw();
                    if (replicationReplayStarted == 0)
                    {
                        replicationReplayStarted = 1;
                        StartReplicationReplayTasks(this, obtainServerSession);
                    }
                    replicationBlockEvent.Increment();
                    if (!replicationReplayChannel.Writer.TryWrite(state))
                    {
                        replicationBlockEvent.Decrement();
                        throw new GarnetException("Vector replay queue is closed");
                    }
                }
                catch
                {
                    ReleaseReplayEntry(state);
                    throw;
                }
            }

            static void StartReplicationReplayTasks(VectorManager self, Func<RespServerSession> obtainServerSession)
            {
                self.logger?.LogInformation("Starting {numTasks} vector replication tasks", self.replicationReplayTasks.Length);

                for (var i = 0; i < self.replicationReplayTasks.Length; i++)
                {
                    self.replicationReplayTasks[i] = StartReplicaTaskAsync(self, obtainServerSession);
                }

                static async Task StartReplicaTaskAsync(VectorManager self, Func<RespServerSession> obtainServerSession)
                {
                    // Force async
                    await Task.Yield();

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
                                    if (self.replicationReplayFailure == null)
                                    {
                                        ApplyVectorSetReplay(self, allocatedSession.storageSession, entry, ref reusableParseState);
                                    }
                                }
                                catch (Exception exception)
                                {
                                    _ = Interlocked.CompareExchange(ref self.replicationReplayFailure, ExceptionDispatchInfo.Capture(exception), null);
                                    self.logger?.LogCritical(exception, "Vector replay failed for {key}", Encoding.UTF8.GetString(entry.Key.Span));
                                }
                                finally
                                {
                                    ReleaseReplayEntry(entry);
                                    self.replicationBlockEvent.Decrement();
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
                        _ = Interlocked.CompareExchange(ref self.replicationReplayFailure, ExceptionDispatchInfo.Capture(e), null);
                        while (self.replicationReplayChannel.Reader.TryRead(out var abandoned))
                        {
                            ReleaseReplayEntry(abandoned);
                            self.replicationBlockEvent.Decrement();
                        }
                        self.logger?.LogCritical(e, "Unexpected abort of replication replay task");
                    }
                }
            }

            static unsafe void ApplyVectorSetReplay(VectorManager self, StorageSession storageSession, VADDReplicationState state, ref SessionParseState reusableParseState)
            {
                var (keyBytes, dims, reduceDims, valueType, valuesBytes, elementBytes, quantizer, buildExplorationFactor, attributesBytes, numLinks, distanceMetric, importTermType) = state;
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

                        if (importTermType.HasValue)
                        {
                            var importStatus = storageSession.VectorSetImport(PinnedSpanByte.FromPinnedSpan(key), importTermType.Value,
                                PinnedSpanByte.FromPinnedSpan(element), PinnedSpanByte.FromPinnedSpan(values), out var importResult, out var error);
                            if (importStatus != GarnetStatus.OK || importResult != VectorManagerResult.OK)
                            {
                                throw new GarnetException($"XVIMPORT replay failed: {importStatus}, {importResult}, {Encoding.UTF8.GetString(error)}");
                            }
                            _ = Interlocked.Increment(ref self.importReplayRequestsProcessed);
                            return;
                        }

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
                            if (status != GarnetStatus.OK)
                            {
                                throw new GarnetException($"Could not read Vector Set during VADD replay: {status}");
                            }

                            var addRes = self.TryAdd(key, indexSpan, element, valueType, values, attributes, reduceDims, quantizer, buildExplorationFactor, numLinks, distanceMetric, out _);

                            if (addRes != VectorManagerResult.OK)
                            {
                                throw new GarnetException("Failed to add to vector set index during AOF sync, this should never happen but will cause data loss if it does");
                            }
                        }
                    }
                }
            }
        }

        private static void ReleaseReplayEntry(VADDReplicationState entry)
        {
            ReturnBuffer(entry.Key);
            ReturnBuffer(entry.Values);
            ReturnBuffer(entry.Element);
            ReturnBuffer(entry.Attributes);

            static void ReturnBuffer(Memory<byte> buffer)
            {
                if (MemoryMarshal.TryGetArray<byte>(buffer, out var array) && array.Array != null && array.Array.Length != 0)
                {
                    ArrayPool<byte>.Shared.Return(array.Array);
                }
            }
        }

        /// <summary>
        /// Clears replay state after lifecycle cancellation has stopped the workers.
        /// 
        /// Returns the number of abandoned vector operations.
        /// </summary>
        private async Task<int> ResetReplayTasksAsync()
        {
            await Task.WhenAll(replicationReplayTasks).ConfigureAwait(false);
            lock (replicationReplayGate)
            {
                Array.Fill(replicationReplayTasks, Task.CompletedTask);
                var abandoned = 0;
                while (replicationReplayChannel.Reader.TryRead(out var entry))
                {
                    ReleaseReplayEntry(entry);
                    replicationBlockEvent.Decrement();
                    abandoned++;
                }

                if (replicationReplayStarted >= 0)
                {
                    replicationBatchCommand = default;
                    _ = Interlocked.Exchange(ref replicationReplayFailure, null);
                    _ = Interlocked.Exchange(ref replicationReplayStarted, 0);
                }
                return abandoned;
            }
        }

        /// <summary>
        /// Shuts down replication tasks in a way where they _cannot_ be resumed in the future.
        /// 
        /// Intended for general Garnet shutdown.
        /// </summary>
        public void ShutdownReplayTasks()
        {
            lock (replicationReplayGate)
            {
                _ = Interlocked.Exchange(ref replicationReplayStarted, -1);
                _ = replicationReplayChannel.Writer.TryComplete();
            }

            // Disposal path, has to be synchronous
            AsyncUtils.BlockingWait(Task.WhenAll(replicationReplayTasks));
        }

        /// <summary>
        /// Vector Set removes are phrased as reads (once the index is created), so they require special handling.
        /// 
        /// Operations that are faked up by <see cref="ReplicateVectorSetRemove"/> running on the Primary get diverted here on a Replica.
        /// </summary>
        internal void HandleVectorSetRemoveReplication(StorageSession storageSession, ReadOnlySpan<byte> key, ref StringInput input)
        {
            WaitForImportFinalization(key);

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
        /// Vector Set attribute sets are phrased as reads (once the index is created), so they require special handling.
        /// 
        /// Operations that are faked up by <see cref="ReplicateVectorSetSetAttribute"/> running on the Primary get diverted here on a Replica.
        /// </summary>
        internal void HandleVectorSetSetAttributeReplication(StorageSession storageSession, ReadOnlySpan<byte> key, ref StringInput input)
        {
            WaitForImportFinalization(key);

            Span<byte> indexSpan = stackalloc byte[IndexSizeBytes];
            var element = input.parseState.GetArgSliceByRef(0);
            var attribute = input.parseState.GetArgSliceByRef(1);

            var inputCopy = input;
            inputCopy.arg1 = default;

            using (ReadVectorIndex(storageSession, key, ref inputCopy, indexSpan, out var status))
            {
                Debug.Assert(status == GarnetStatus.OK, "Replication should only occur when a setattr is successful, so index must exist");

                if (!TrySetAttribute(indexSpan, element, attribute))
                {
                    throw new GarnetException("Failed to set attribute on vector set during AOF sync, this should never happen but will cause data loss if it does");
                }
            }
        }

        /// <summary>
        /// Wait until queued vector replays complete, propagating any worker failure.
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
            replicationReplayFailure?.Throw();
        }

        /// <summary>
        /// During AOF replay we need to copy one Vector Set index into another, but we can't use the bytes stored in the log since
        /// VADD replay will have produced different contexts and pointers.
        /// 
        /// So do a simple copy instead.
        /// </summary>
        internal unsafe void HandleVectorSetRenameCopy<TUnifiedContext>(StorageSession storageSession, ref TUnifiedContext unifiedContext, ReadOnlySpan<byte> oldVectorSet, ReadOnlySpan<byte> newVectorSet)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            SessionParseState parseState = default;
            parseState.InitializeWithArguments([PinnedSpanByte.FromPinnedSpan(oldVectorSet), PinnedSpanByte.FromPinnedSpan(newVectorSet)]);

            UnifiedInput input = new(RespCommand.RENAME, ref parseState);
            UnifiedOutput output = new();

            var readStatus = unifiedContext.Read((FixedSpanByteKey)oldVectorSet, ref input, ref output);
            if (readStatus.IsPending)
            {
                _ = unifiedContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                var more = completedOutputs.Next();
                Debug.Assert(more);
                readStatus = completedOutputs.Current.Status;
                output = completedOutputs.Current.Output;
                Debug.Assert(!completedOutputs.Next());
                completedOutputs.Dispose();
            }

            if (!readStatus.Found)
            {
                throw new GarnetException("Should never fail to find original key during RENAME replay");
            }

            fixed (byte* recordPtr = output.SpanByteAndMemory.ReadOnlySpan)
            {
                // We have a record in in-memory, unserialized format, with its objects (if any) resolved to the TransientObjectIdMap.
                var logRecord = new LogRecord(recordPtr, storageSession.functionsState.transientObjectIdMap);

                var upsertStatus = unifiedContext.Upsert((FixedSpanByteKey)newVectorSet, ref input, in logRecord);

                if (upsertStatus.IsPending)
                {
                    _ = unifiedContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    var more = completedOutputs.Next();
                    Debug.Assert(more);
                    upsertStatus = completedOutputs.Current.Status;
                    Debug.Assert(!completedOutputs.Next());
                    completedOutputs.Dispose();
                }
            }

            output.Dispose();
        }
    }
}