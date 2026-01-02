// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Garnet.common;
using Tsavorite.core;

#pragma warning disable CS0649
#pragma warning disable CS0169

namespace Garnet.server
{
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

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
        private readonly record struct VADDReplicationState(Memory<byte> Key, uint Dims, uint ReduceDims, VectorValueType ValueType, Memory<byte> Values, Memory<byte> Element, VectorQuantType Quantizer, uint BuildExplorationFactor, Memory<byte> Attributes, uint NumLinks)
        {
        }

        private int replicationReplayStarted;
        private CountingEventSlim replicationBlockEvent;
        private readonly Channel<VADDReplicationState> replicationReplayChannel;
        private readonly Task[] replicationReplayTasks;

        private CancellationTokenSource replicationReplayTasksCts;

        /// <summary>
        /// For testing purposes, are the replication replay tasks active.
        /// </summary>
        public bool AreReplicationTasksActive
        => replicationReplayTasksCts != null && replicationReplayTasks.Any(static r => !r.IsCompleted);

        /// <summary>
        /// For replication purposes, we need a write against the main log.
        /// 
        /// But we don't actually want to do the (expensive) vector ops as part of a write.
        /// 
        /// So this fakes up a modify operation that we can then intercept as part of replication.
        /// 
        /// This the Primary part, on a Replica <see cref="HandleVectorSetAddReplication"/> runs.
        /// </summary>
        internal void ReplicateVectorSetAdd(ref PinnedSpanByte key, ref StringInput input, ref BasicContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> context)
        {
            throw new NotImplementedException();
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
        internal void ReplicateVectorSetRemove(ref PinnedSpanByte key, ref PinnedSpanByte element, ref StringInput input, ref BasicContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> context)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// After an index is dropped, called to cleanup state injected by <see cref="ReplicateVectorSetAdd"/>
        /// 
        /// Amounts to delete a synthetic key in namespace 0.
        /// </summary>
        internal bool TryDropVectorSetReplicationKey(PinnedSpanByte key, ref BasicContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> context)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Vector Set adds are phrased as reads (once the index is created), so they require special handling.
        /// 
        /// Operations that are faked up by <see cref="ReplicateVectorSetAdd"/> running on the Primary get diverted here on a Replica.
        /// </summary>
        internal void HandleVectorSetAddReplication(StorageSession currentSession, Func<RespServerSession> obtainServerSession, ref PinnedSpanByte keyWithNamespace, ref StringInput input)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Cancels replication tasks, resetting enough state that they can be resumed by a future call to <see cref="HandleVectorSetAddReplication"/>.
        /// 
        /// Returns the number of abanded VADDs.
        /// </summary>
        internal int ResetReplayTasks()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Vector Set removes are phrased as reads (once the index is created), so they require special handling.
        /// 
        /// Operations that are faked up by <see cref="ReplicateVectorSetRemove"/> running on the Primary get diverted here on a Replica.
        /// </summary>
        internal void HandleVectorSetRemoveReplication(StorageSession storageSession, ref PinnedSpanByte key, ref StringInput input)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Wait until all ops passed to <see cref="HandleVectorSetAddReplication"/> have completed.
        /// </summary>
        public void WaitForVectorOperationsToComplete()
        {
            throw new NotImplementedException();
        }
        // Helper to complete read/writes during vector set synthetic op goes async
        private static void CompletePending(ref Status status, ref BasicContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> context)
        {
            throw new NotImplementedException();
        }
    }
}