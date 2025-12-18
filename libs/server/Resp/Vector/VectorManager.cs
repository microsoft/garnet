// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading.Channels;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    /// <summary>
    /// Methods for managing an implementation of various vector operations.
    /// </summary>
    public sealed partial class VectorManager : IDisposable
    {
        internal const int IndexSizeBytes = Index.Size;
        internal const long VADDAppendLogArg = long.MinValue;
        internal const long DeleteAfterDropArg = VADDAppendLogArg + 1;
        internal const long RecreateIndexArg = DeleteAfterDropArg + 1;
        internal const long VREMAppendLogArg = RecreateIndexArg + 1;
        internal const long MigrateElementKeyLogArg = VREMAppendLogArg + 1;
        internal const long MigrateIndexKeyLogArg = MigrateElementKeyLogArg + 1;

        /// <summary>
        /// Minimum size of an id is assumed to be at least 4 bytes + a length prefix.
        /// </summary>
        private const int MinimumSpacePerId = sizeof(int) + 4;

        /// <summary>
        /// The process wide instances of DiskANN.
        /// 
        /// We only need the one, even if we have multiple DBs, because all context is provided by DiskANN instances and Garnet storage.
        /// </summary>
        private DiskANNService Service { get; } = new DiskANNService();

        /// <summary>
        /// Whether or not Vector Set preview is enabled.
        /// 
        /// TODO: This goes away once we're stable.
        /// </summary>
        public bool IsEnabled { get; }

        /// <summary>
        /// Unique id for this <see cref="VectorManager"/>.
        /// 
        /// Is used to determine if an <see cref="Index"/> is backed by a DiskANN index that was created in this process.
        /// </summary>
        private readonly Guid processInstanceId = Guid.NewGuid();

        private readonly ILogger logger;

        private readonly int dbId;

        public VectorManager(bool enabled, int dbId, Func<IMessageConsumer> getCleanupSession, ILoggerFactory loggerFactory)
        {
            this.dbId = dbId;

            IsEnabled = enabled;

            // Include DB and id so we correlate to what's actually stored in the log
            logger = loggerFactory?.CreateLogger($"{nameof(VectorManager)}:{dbId}:{processInstanceId}");

            replicationBlockEvent = CountingEventSlim.Create();
            replicationReplayChannel = Channel.CreateUnbounded<VADDReplicationState>(new() { SingleWriter = true, SingleReader = false, AllowSynchronousContinuations = false });

            // TODO: Pull this off a config or something
            replicationReplayTasks = new Task[Environment.ProcessorCount];
            for (var i = 0; i < replicationReplayTasks.Length; i++)
            {
                replicationReplayTasks[i] = Task.CompletedTask;
            }

            // TODO: Probably configurable?
            // For now, just number of processors
            vectorSetLocks = new(Environment.ProcessorCount);

            this.getCleanupSession = getCleanupSession;
            cleanupTaskChannel = Channel.CreateUnbounded<object>(new() { SingleWriter = false, SingleReader = true, AllowSynchronousContinuations = false });
            cleanupTask = RunCleanupTaskAsync();

            logger?.LogInformation("Created VectorManager");
        }

        /// <summary>
        /// Load state necessary for VectorManager from main store.
        /// </summary>
        public void Initialize()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Restart or update any pending work that was discovered as part of recovery.
        /// </summary>
        public void ResumePostRecovery()
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            throw new NotImplementedException();
        }

        private static void CompletePending(ref Status status, ref PinnedSpanByte output, ref BasicContext<VectorInput, PinnedSpanByte, long, VectorSessionFunctions, StoreFunctions, StoreAllocator> ctx)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Add a vector to a vector set encoded by <paramref name="indexValue"/>.
        /// 
        /// Assumes that the index is locked in the Tsavorite store.
        /// </summary>
        /// <returns>Result of the operation.</returns>
        internal VectorManagerResult TryAdd(
            scoped ReadOnlySpan<byte> indexValue,
            ReadOnlySpan<byte> element,
            VectorValueType valueType,
            ReadOnlySpan<byte> values,
            ReadOnlySpan<byte> attributes,
            uint providedReduceDims,
            VectorQuantType providedQuantType,
            uint providedBuildExplorationFactor,
            uint providedNumLinks,
            out ReadOnlySpan<byte> errorMsg
        )
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Try to remove a vector (and associated attributes) from a Vector Set, as identified by element key.
        /// </summary>
        internal VectorManagerResult TryRemove(ReadOnlySpan<byte> indexValue, ReadOnlySpan<byte> element)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Deletion of a Vector Set needs special handling.
        /// 
        /// This is called by DEL and UNLINK after a naive delete fails for us to _try_ and delete a Vector Set.
        /// </summary>
        internal Status TryDeleteVectorSet(StorageSession storageSession, ref PinnedSpanByte key, out GarnetStatus status)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Perform a similarity search given a vector to compare against.
        /// </summary>
        internal VectorManagerResult ValueSimilarity(
            ReadOnlySpan<byte> indexValue,
            VectorValueType valueType,
            ReadOnlySpan<byte> values,
            int count,
            float delta,
            int searchExplorationFactor,
            ReadOnlySpan<byte> filter,
            int maxFilteringEffort,
            bool includeAttributes,
            ref SpanByteAndMemory outputIds,
            out VectorIdFormat outputIdFormat,
            ref SpanByteAndMemory outputDistances,
            ref SpanByteAndMemory outputAttributes
        )
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Perform a similarity search given a vector to compare against.
        /// </summary>
        internal VectorManagerResult ElementSimilarity(
            ReadOnlySpan<byte> indexValue,
            ReadOnlySpan<byte> element,
            int count,
            float delta,
            int searchExplorationFactor,
            ReadOnlySpan<byte> filter,
            int maxFilteringEffort,
            bool includeAttributes,
            ref SpanByteAndMemory outputIds,
            out VectorIdFormat outputIdFormat,
            ref SpanByteAndMemory outputDistances,
            ref SpanByteAndMemory outputAttributes
        )
        {
            throw new NotImplementedException();
        }


        /// <summary>
        /// Fetch attributes for a given set of element ids.
        /// 
        /// This must only be called while holding locks which prevent the Vector Set from being dropped.
        /// </summary>
        private void FetchVectorElementAttributes(ulong context, int numIds, SpanByteAndMemory ids, ref SpanByteAndMemory attributes)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Try to read the associated dimensions for an element out of a Vector Set.
        /// </summary>
        internal bool TryGetEmbedding(ReadOnlySpan<byte> indexValue, ReadOnlySpan<byte> element, ref SpanByteAndMemory outputDistances)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Determine the dimensions of a vector given its <see cref="VectorValueType"/> and its raw data.
        /// </summary>
        internal static uint CalculateValueDimensions(VectorValueType valueType, ReadOnlySpan<byte> values)
        {
            throw new NotImplementedException();
        }

        [Conditional("DEBUG")]
        private static void AssertHaveStorageSession()
        {
            Debug.Assert(ActiveThreadSession != null, "Should have StorageSession by now");
        }
    }
}