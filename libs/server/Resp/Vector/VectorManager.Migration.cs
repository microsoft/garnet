// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    /// <summary>
    /// Methods related to migrating Vector Sets between different primaries.
    /// 
    /// This is bespoke because normal migration is key based, but Vector Set migration has to move whole namespaces first.
    /// </summary>
    public sealed partial class VectorManager
    {
        // This is a V8 GUID based on 'GARNET MIGRATION' ASCII string
        // It cannot collide with processInstanceIds because it's v8
        // It's unlikely other projects will select the value, so it's unlikely to collide with other v8s
        // If it ends up in logs, it's ASCII equivalent looks suspcious enough to lead back here
        private static readonly Guid MigratedInstanceId = new("4e524147-5445-8d20-8947-524154494f4e");

        /// <summary>
        /// Called to handle a key in a namespace being received during a migration.
        /// 
        /// These keys are what DiskANN stores, that is they are "element" data.
        /// 
        /// The index is handled specially by <see cref="HandleMigratedIndexKey"/>.
        /// </summary>
        public void HandleMigratedElementKey(
            ref BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> basicCtx,
            ref BasicContext<SpanByte, SpanByte, VectorInput, SpanByte, long, VectorSessionFunctions, MainStoreFunctions, MainStoreAllocator> vectorCtx,
            ref SpanByte key,
            ref SpanByte value
        )
        {
            Debug.Assert(key.MetadataSize == 1, "Should have namespace if we're migrating a key");

#if DEBUG
            // Do some extra sanity checking in DEBUG builds
            lock (this)
            {
                var ns = key.GetNamespaceInPayload();
                var context = (ulong)(ns & ~(ContextStep - 1));
                Debug.Assert(contextMetadata.IsInUse(context), "Shouldn't be migrating to an unused context");
                Debug.Assert(contextMetadata.IsMigrating(context), "Shouldn't be migrating to context not marked for it");
                Debug.Assert(!(contextMetadata.GetNeedCleanup()?.Contains(context) ?? false), "Shouldn't be migrating into context being deleted");
            }
#endif

            VectorInput input = default;
            SpanByte outputSpan = default;

            var status = vectorCtx.Upsert(ref key, ref input, ref value, ref outputSpan);
            if (status.IsPending)
            {
                CompletePending(ref status, ref outputSpan, ref vectorCtx);
            }

            if (!status.IsCompletedSuccessfully)
            {
                throw new GarnetException("Failed to migrate key, this should fail migration");
            }

            ReplicateMigratedElementKey(ref basicCtx, ref key, ref value, logger);

            // Fake a write for post-migration replication
            static void ReplicateMigratedElementKey(ref BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> basicCtx, ref SpanByte key, ref SpanByte value, ILogger logger)
            {
                RawStringInput input = default;

                input.header.cmd = RespCommand.VADD;
                input.arg1 = MigrateElementKeyLogArg;

                input.parseState.InitializeWithArguments([ArgSlice.FromPinnedSpan(key.AsReadOnlySpanWithMetadata()), ArgSlice.FromPinnedSpan(value.AsReadOnlySpan())]);

                SpanByte dummyKey = default;
                SpanByteAndMemory dummyOutput = default;

                var res = basicCtx.RMW(ref dummyKey, ref input, ref dummyOutput);

                if (res.IsPending)
                {
                    CompletePending(ref res, ref dummyOutput, ref basicCtx);
                }

                if (!res.IsCompletedSuccessfully)
                {
                    logger?.LogCritical("Failed to inject replication write for migrated Vector Set key/value into log, result was {res}", res);
                    throw new GarnetException("Couldn't synthesize Vector Set write operation for key/value migration, data loss may occur");
                }

                // Helper to complete read/writes during vector set synthetic op goes async
                static void CompletePending(ref Status status, ref SpanByteAndMemory output, ref BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> basicCtx)
                {
                    _ = basicCtx.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    var more = completedOutputs.Next();
                    Debug.Assert(more);
                    status = completedOutputs.Current.Status;
                    output = completedOutputs.Current.Output;
                    more = completedOutputs.Next();
                    Debug.Assert(!more);
                    completedOutputs.Dispose();
                }
            }
        }

        /// <summary>
        /// Called to handle a Vector Set key being received during a migration.  These are "index" keys.
        /// 
        /// This is the metadata stuff Garnet creates, DiskANN is not involved.
        /// 
        /// Invoked after all the namespace data is moved via <see cref="HandleMigratedElementKey"/>.
        /// </summary>
        public void HandleMigratedIndexKey(
            GarnetDatabase db,
            StoreWrapper storeWrapper,
            ref SpanByte key,
            ref SpanByte value)
        {
            Debug.Assert(key.MetadataSize != 1, "Shouldn't have a namespace if we're migrating a Vector Set index");

            RawStringInput input = default;
            input.header.cmd = RespCommand.VADD;
            input.arg1 = RecreateIndexArg;

            ReadIndex(value.AsReadOnlySpan(), out var context, out var dimensions, out var reduceDims, out var quantType, out var buildExplorationFactor, out var numLinks, out _, out var processInstanceId);

            Debug.Assert(processInstanceId == MigratedInstanceId, "Shouldn't receive a real process instance id during a migration");

            // Extra validation in DEBUG
#if DEBUG
            lock (this)
            {
                Debug.Assert(contextMetadata.IsInUse(context), "Context should be assigned if we're migrating");
                Debug.Assert(contextMetadata.IsMigrating(context), "Context should be marked migrating if we're moving an index key in");
            }
#endif

            // Spin up a new Storage Session is we don't have one
            StorageSession newStorageSession;
            if (ActiveThreadSession == null)
            {
                Debug.Assert(db != null, "Must have DB if session is not already set");
                Debug.Assert(storeWrapper != null, "Must have StoreWrapper if session is not already set");

                ActiveThreadSession = newStorageSession = new StorageSession(storeWrapper, new(), null, null, db.Id, this, this.logger);
            }
            else
            {
                newStorageSession = null;
            }

            try
            {
                // Prepare as a psuedo-VADD
                var dimsArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref dimensions, 1)));
                var reduceDimsArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref reduceDims, 1)));
                ArgSlice valueTypeArg = default;
                ArgSlice valuesArg = default;
                ArgSlice elementArg = default;
                var quantizerArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<VectorQuantType, byte>(MemoryMarshal.CreateSpan(ref quantType, 1)));
                var buildExplorationFactorArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref buildExplorationFactor, 1)));
                ArgSlice attributesArg = default;
                var numLinksArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref numLinks, 1)));

                nint newlyAllocatedIndex;
                unsafe
                {
                    newlyAllocatedIndex = Service.RecreateIndex(context, dimensions, reduceDims, quantType, buildExplorationFactor, numLinks, ReadCallbackPtr, WriteCallbackPtr, DeleteCallbackPtr, ReadModifyWriteCallbackPtr);
                }

                var ctxArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<ulong, byte>(MemoryMarshal.CreateSpan(ref context, 1)));
                var indexArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<nint, byte>(MemoryMarshal.CreateSpan(ref newlyAllocatedIndex, 1)));

                input.parseState.InitializeWithArguments([dimsArg, reduceDimsArg, valueTypeArg, valuesArg, elementArg, quantizerArg, buildExplorationFactorArg, attributesArg, numLinksArg, ctxArg, indexArg]);

                Span<byte> indexSpan = stackalloc byte[Index.Size];
                var indexConfig = SpanByteAndMemory.FromPinnedSpan(indexSpan);

                // Exclusive lock to prevent other modification of this key

                using (AcquireExclusiveLocks(ActiveThreadSession, ref key))
                {
                    // Perform the write
                    var writeRes = ActiveThreadSession.RMW_MainStore(ref key, ref input, ref indexConfig, ref ActiveThreadSession.basicContext);
                    if (writeRes != GarnetStatus.OK)
                    {
                        Service.DropIndex(context, newlyAllocatedIndex);
                        throw new GarnetException("Failed to import migrated Vector Set index, aborting migration");
                    }

                    var hashSlot = HashSlotUtils.HashSlot(ref key);

                    lock (this)
                    {
                        contextMetadata.MarkMigrationComplete(context, hashSlot);
                    }

                    UpdateContextMetadata(ref ActiveThreadSession.vectorContext);

                    // For REPLICAs which are following, we need to fake up a write
                    ReplicateMigratedIndexKey(ref ActiveThreadSession.basicContext, ref key, ref value, context, logger);
                }
            }
            finally
            {
                ActiveThreadSession = null;

                // If we spun up a new storage session, dispose it
                newStorageSession?.Dispose();
            }

            // Fake a write for post-migration replication
            static void ReplicateMigratedIndexKey(
                ref BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> basicCtx,
                ref SpanByte key,
                ref SpanByte value,
                ulong context,
                ILogger logger)
            {
                RawStringInput input = default;

                input.header.cmd = RespCommand.VADD;
                input.arg1 = MigrateIndexKeyLogArg;

                var contextArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<ulong, byte>(MemoryMarshal.CreateSpan(ref context, 1)));

                input.parseState.InitializeWithArguments([ArgSlice.FromPinnedSpan(key.AsReadOnlySpanWithMetadata()), ArgSlice.FromPinnedSpan(value.AsReadOnlySpan()), contextArg]);

                SpanByte dummyKey = default;
                SpanByteAndMemory dummyOutput = default;

                var res = basicCtx.RMW(ref dummyKey, ref input, ref dummyOutput);

                if (res.IsPending)
                {
                    CompletePending(ref res, ref dummyOutput, ref basicCtx);
                }

                if (!res.IsCompletedSuccessfully)
                {
                    logger?.LogCritical("Failed to inject replication write for migrated Vector Set index into log, result was {res}", res);
                    throw new GarnetException("Couldn't synthesize Vector Set write operation for index migration, data loss may occur");
                }

                // Helper to complete read/writes during vector set synthetic op goes async
                static void CompletePending(ref Status status, ref SpanByteAndMemory output, ref BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> basicCtx)
                {
                    _ = basicCtx.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    var more = completedOutputs.Next();
                    Debug.Assert(more);
                    status = completedOutputs.Current.Status;
                    output = completedOutputs.Current.Output;
                    more = completedOutputs.Next();
                    Debug.Assert(!more);
                    completedOutputs.Dispose();
                }
            }
        }

        /// <summary>
        /// Find namespaces used by the given keys, IFF they are Vector Sets.  They may (and often will) not be.
        /// 
        /// Meant for use during migration.
        /// </summary>
        public unsafe HashSet<ulong> GetNamespacesForKeys(StoreWrapper storeWrapper, IEnumerable<byte[]> keys, Dictionary<byte[], byte[]> vectorSetKeys)
        {
            // TODO: Ideally we wouldn't make a new session for this, but it's fine for now
            using var storageSession = new StorageSession(storeWrapper, new(), null, null, storeWrapper.DefaultDatabase.Id, this, logger);

            HashSet<ulong> namespaces = null;

            Span<byte> indexSpan = stackalloc byte[Index.Size];

            foreach (var key in keys)
            {
                fixed (byte* keyPtr = key)
                {
                    var keySpan = SpanByte.FromPinnedPointer(keyPtr, key.Length);

                    // Dummy command, we just need something Vector Set-y
                    RawStringInput input = default;
                    input.header.cmd = RespCommand.VSIM;

                    using (ReadVectorIndex(storageSession, ref keySpan, ref input, indexSpan, out var status))
                    {
                        if (status != GarnetStatus.OK)
                        {
                            continue;
                        }

                        namespaces ??= [];

                        ReadIndex(indexSpan, out var context, out _, out _, out _, out _, out _, out _, out _);
                        for (var i = 0UL; i < ContextStep; i++)
                        {
                            _ = namespaces.Add(context + i);
                        }

                        vectorSetKeys[key] = indexSpan.ToArray();
                    }
                }
            }

            return namespaces;
        }
    }
}