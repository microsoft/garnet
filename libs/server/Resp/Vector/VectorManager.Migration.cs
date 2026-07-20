// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Methods related to migrating Vector Sets between different primaries.
    /// 
    /// This is bespoke because normal migration is key based, but Vector Set migration has to move whole namespaces first.
    /// </summary>
    public sealed partial class VectorManager
    {
        /// <summary>
        /// Called to handle a key in a namespace being received during a migration.
        /// 
        /// These keys are what DiskANN stores, that is they are "element" data.
        /// 
        /// The index is handled specially by <see cref="HandleMigratedIndexKey"/>.
        /// </summary>
        public void HandleMigratedElementKey(
            ref StringBasicContext basicCtx,
            ref VectorBasicContext vectorCtx,
            ReadOnlySpan<byte> namespaceBytes,
            ReadOnlySpan<byte> keyWithoutNamespace,
            ReadOnlySpan<byte> value
        )
        {
            var ns = ExtractContextFromNamespaces(namespaceBytes);

#if DEBUG
            // Do some extra sanity checking in DEBUG builds
            lock (this)
            {
                var context = ns & ~(ContextStep - 1);

                var (contextIndex, contextValue) = ContextMetadata.DecomposeContext(context);

                Debug.Assert(contextMetadatas[contextIndex].IsInUse(contextIndex != 0, contextValue), "Shouldn't be migrating to an unused context");
                Debug.Assert(contextMetadatas[contextIndex].IsMigrating(contextIndex != 0, contextValue), "Shouldn't be migrating to context not marked for it");
                Debug.Assert(!(contextMetadatas[contextIndex].GetNeedCleanup()?.Contains(contextValue) ?? false), "Shouldn't be migrating into context being deleted");
            }
#endif

            VectorInput input = default;
            input.AlignmentExpected = true;
            VectorOutput outputSpan = new(new SpanByteAndMemory());

            // When we migrate a record we expand the namespace to always occupy 4-bytes
            // in order to have space for updating to the destination namespace.
            //
            // Shrink it back down if able post-migration.
            Span<byte> nsCopy = stackalloc byte[sizeof(uint)];
            StoreContextInNamespace(ns, ref nsCopy);

            VectorElementKey key = new(nsCopy, keyWithoutNamespace);

            var status = vectorCtx.Upsert(key, ref input, value, ref outputSpan);
            if (status.IsPending)
            {
                CompletePending(ref status, ref outputSpan, ref vectorCtx);
            }

            if (!status.IsCompletedSuccessfully)
            {
                throw new GarnetException("Failed to migrate key, this should fail migration");
            }

            ReplicateMigratedElementKey(ref basicCtx, key, value, logger);

            // Fake a write for post-migration replication
            static void ReplicateMigratedElementKey(ref StringBasicContext basicCtx, VectorElementKey key, ReadOnlySpan<byte> value, ILogger logger)
            {
                StringInput input = default;

                // Serialize namespace and key data explicitly, we'll deserialize it in HandleVectorSetAddReplication
                Span<byte> serializedKeyBytes = stackalloc byte[sizeof(int) + sizeof(uint) + sizeof(int) + key.KeyBytes.Length];
                BinaryPrimitives.WriteInt32LittleEndian(serializedKeyBytes, 4);

                var ns = ExtractContextFromNamespaces(key.NamespaceBytes);
                BinaryPrimitives.WriteUInt32LittleEndian(serializedKeyBytes[sizeof(int)..], (uint)ns);

                BinaryPrimitives.WriteInt32LittleEndian(serializedKeyBytes[(sizeof(int) + sizeof(uint))..], key.KeyBytes.Length);
                key.KeyBytes.CopyTo(serializedKeyBytes[(sizeof(int) + sizeof(uint) + sizeof(int))..]);

                input.header.cmd = RespCommand.VADD;
                input.arg1 = MigrateElementKeyLogArg;

                input.parseState.InitializeWithArguments([PinnedSpanByte.FromPinnedSpan(serializedKeyBytes), PinnedSpanByte.FromPinnedSpan(value)]);

                ReadOnlySpan<byte> dummyKey = [];
                StringOutput dummyOutput = new();

                var res = basicCtx.RMW((FixedSpanByteKey)dummyKey, ref input, ref dummyOutput);

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
                static void CompletePending(ref Status status, ref StringOutput output, ref StringBasicContext basicCtx)
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
            ReadOnlySpan<byte> key,
            ReadOnlySpan<byte> value)
        {
            StringInput input = default;
            input.header.cmd = RespCommand.VADD;
            input.arg1 = CreateIndexArg;

            ReadIndex(value, out var context, out var dimensions, out var reduceDims, out var quantType, out var buildExplorationFactor, out var numLinks, out var distanceMetric, out _, out var indexPtr);

            Debug.Assert(indexPtr == 0, "Shouldn't receive an index pointer during a migration");

            // Extra validation in DEBUG
#if DEBUG
            lock (this)
            {
                var (contextIndex, contextValue) = ContextMetadata.DecomposeContext(context);

                Debug.Assert(contextMetadatas[contextIndex].IsInUse(contextIndex != 0, contextValue), "Context should be assigned if we're migrating");
                Debug.Assert(contextMetadatas[contextIndex].IsMigrating(contextIndex != 0, contextValue), "Context should be marked migrating if we're moving an index key in");
            }
#endif

            // Spin up a new Storage Session is we don't have one
            StorageSession newStorageSession;
            if (ActiveThreadSession == null)
            {
                Debug.Assert(db != null, "Must have DB if session is not already set");
                Debug.Assert(storeWrapper != null, "Must have StoreWrapper if session is not already set");

                ActiveThreadSession = newStorageSession = new StorageSession(storeWrapper, new(), new(), null, null, db.Id, null, this, this.logger);
            }
            else
            {
                newStorageSession = null;
            }

            try
            {
                // Prepare as a psuedo-VADD
                var dimsArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref dimensions, 1)));
                var reduceDimsArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref reduceDims, 1)));
                PinnedSpanByte valueTypeArg = default;
                PinnedSpanByte valuesArg = default;
                PinnedSpanByte elementArg = default;
                var quantizerArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<VectorQuantType, byte>(MemoryMarshal.CreateSpan(ref quantType, 1)));
                var buildExplorationFactorArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref buildExplorationFactor, 1)));
                PinnedSpanByte attributesArg = default;
                var numLinksArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref numLinks, 1)));
                var distanceMetricArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<VectorDistanceMetricType, byte>(MemoryMarshal.CreateSpan(ref distanceMetric, 1)));

                nint newlyAllocatedIndex;
                bool requestQuantization;
                unsafe
                {
                    newlyAllocatedIndex = Service.RecreateIndex(context, dimensions, reduceDims, quantType, buildExplorationFactor, numLinks, distanceMetric, ReadCallbackPtr, WriteCallbackPtr, DeleteCallbackPtr, ReadModifyWriteCallbackPtr, InlineFilterCallbackPtr, out requestQuantization);
                }

                var ctxArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<ulong, byte>(MemoryMarshal.CreateSpan(ref context, 1)));
                var indexArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<nint, byte>(MemoryMarshal.CreateSpan(ref newlyAllocatedIndex, 1)));

                input.parseState.InitializeWithArguments([dimsArg, reduceDimsArg, valueTypeArg, valuesArg, elementArg, quantizerArg, buildExplorationFactorArg, attributesArg, numLinksArg, distanceMetricArg, ctxArg, indexArg]);

                Span<byte> indexSpan = stackalloc byte[Index.Size];
                var indexConfig = SpanByteAndMemory.FromPinnedSpan(indexSpan);
                StringOutput indexConfigOutput = new(indexConfig);

                // Exclusive lock to prevent other modification of this key

                var needsWaitForDrop = false;
            waitForDrop:
                if (needsWaitForDrop)
                {
                    WaitForDiskANNIndexDrop(key);
                    needsWaitForDrop = false;
                }

                using (AcquireExclusiveLocks(ActiveThreadSession, key))
                {
                    // If we're racing with a drop to the same key, we need to drop the lock and wait for index drop.
                    //
                    // This should be extremely rare (it basically requires a delete followed by an immediate migrate into the same key)
                    // but if we don't handle it future inserts will corrupt the DiskANN index.
                    if (DropRequested(key))
                    {
                        needsWaitForDrop = true;
                        goto waitForDrop;
                    }

                    // Perform the write
                    var writeRes = ActiveThreadSession.RMW_MainStore(key, ref input, ref indexConfigOutput, ref ActiveThreadSession.stringBasicContext);
                    if (writeRes != GarnetStatus.OK)
                    {
                        indexConfigOutput.SpanByteAndMemory.Memory?.Dispose();

                        Service.DropIndex(context, newlyAllocatedIndex);
                        throw new GarnetException("Failed to import migrated Vector Set index, aborting migration");
                    }

                    Debug.Assert(indexConfigOutput.SpanByteAndMemory.IsSpanByte, "Should never allocate");

                    var hashSlot = HashSlotUtils.HashSlot(key);

                    var (contextIndex, contextValue) = ContextMetadata.DecomposeContext(context);

                    lock (this)
                    {
                        contextMetadatas[contextIndex].MarkMigrationComplete(contextIndex != 0, contextValue, hashSlot);

                        _ = dirtyContextMetadatas.Add(contextIndex);
                    }

                    UpdateContextMetadata(ref ActiveThreadSession.vectorBasicContext);

                    // Post recreation the index might already need quantization - if so, queue it up
                    if (requestQuantization)
                    {
                        _ = quantizationChannel.Writer.TryWrite(new(key.ToArray(), QuantizationStep.BuildQuantizationTable, 0));
                    }

                    // For REPLICAs which are following, we need to fake up a write
                    ReplicateMigratedIndexKey(ref ActiveThreadSession.stringBasicContext, key, value, context, logger);
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
                ref StringBasicContext basicCtx,
                ReadOnlySpan<byte> key,
                ReadOnlySpan<byte> value,
                ulong context,
                ILogger logger)
            {
                StringInput input = default;

                input.header.cmd = RespCommand.VADD;
                input.arg1 = MigrateIndexKeyLogArg;

                var contextArg = PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<ulong, byte>(MemoryMarshal.CreateSpan(ref context, 1)));

                input.parseState.InitializeWithArguments([PinnedSpanByte.FromPinnedSpan(key), PinnedSpanByte.FromPinnedSpan(value), contextArg]);

                var dummyKey = (FixedSpanByteKey)default(ReadOnlySpan<byte>);
                StringOutput dummyOutput = new();

                var res = basicCtx.RMW(dummyKey, ref input, ref dummyOutput);

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
                static void CompletePending(ref Status status, ref StringOutput output, ref StringBasicContext basicCtx)
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
        public HashSet<ulong> GetNamespacesForKeys(StoreWrapper storeWrapper, IEnumerable<PinnedSpanByte> keys, Dictionary<byte[], byte[]> vectorSetKeys)
        {
            // TODO: Ideally we wouldn't make a new session for this, but it's fine for now
            using var storageSession = new StorageSession(storeWrapper, new(), new(), null, null, storeWrapper.DefaultDatabase.Id, null, this, logger);

            HashSet<ulong> namespaces = null;

            Span<byte> indexSpan = stackalloc byte[Index.Size];

            foreach (var key in keys)
            {
                // Dummy command, we just need something Vector Set-y
                StringInput input = default;
                input.header.cmd = RespCommand.VSIM;

                using (ReadVectorIndex(storageSession, key.ReadOnlySpan, ref input, indexSpan, out var status))
                {
                    if (status != GarnetStatus.OK)
                    {
                        continue;
                    }

                    namespaces ??= [];

                    ReadIndex(indexSpan, out var context, out _, out _, out _, out _, out _, out _, out _, out _);
                    for (var i = 0UL; i < ContextStep; i++)
                    {
                        _ = namespaces.Add(context + i);
                    }

                    vectorSetKeys[key.ToArray()] = indexSpan.ToArray();
                }
            }

            return namespaces;
        }


        /// <summary>
        /// Update the namespaces stored in <paramref name="readOutput"/> according to <see cref="FrozenDictionary"/>.
        /// 
        /// <paramref name="readInput"/> should have been used to populate <paramref name="readOutput"/> with a Tsavorite Read prior to this call.
        /// </summary>
        public static void UpdateMigratedElementNamespaces(FrozenDictionary<ulong, ulong> oldToNewNamespaces, ref VectorInput readInput, ref VectorOutput readOutput)
        {
            Debug.Assert(readInput.IsMigrationRead, "Unexpected input");

            DeserializeMigratedElementKey(readOutput.SpanByteAndMemory.Span, out var namespaceBytes, out _, out _);

            ulong oldNs = BinaryPrimitives.ReadUInt32LittleEndian(namespaceBytes);

            if (!oldToNewNamespaces.TryGetValue(oldNs, out var newNs))
            {
                return;
            }

            Debug.Assert(newNs <= uint.MaxValue, "Shouldn't have reserved such a large context");

            BinaryPrimitives.WriteUInt32LittleEndian(namespaceBytes, (uint)newNs);
        }

        /// <summary>
        /// Calculate needed storage for migrating a Vector Set element key.
        /// </summary>
        public static int GetMigratedElementKeySerializationSize(ReadOnlySpan<byte> keyBytes, ReadOnlySpan<byte> alignedValue)
        {
            var neededSpace =
                sizeof(int) + sizeof(uint) + // Namespace is ALWAYS expanded to 4-bytes so we have space to re-write it
                sizeof(int) + keyBytes.Length +
                sizeof(int) + alignedValue.Length;

            return neededSpace;
        }

        /// <summary>
        /// Serialize a record for migrating a Vector Set element key.
        /// </summary>
        public static void SerializeMigratedElementKey(Span<byte> dataBytes, ReadOnlySpan<byte> namespaceBytes, ReadOnlySpan<byte> keyBytes, ReadOnlySpan<byte> alignedValue)
        {
            var context = ExtractContextFromNamespaces(namespaceBytes);

            var writeTo = dataBytes;

            // Namespace length, Namespace
            BinaryPrimitives.WriteInt32LittleEndian(writeTo, 4);
            writeTo = writeTo[sizeof(int)..];
            BinaryPrimitives.WriteUInt32LittleEndian(writeTo, (uint)context);
            writeTo = writeTo[sizeof(uint)..];

            // Key length, Key
            BinaryPrimitives.WriteInt32LittleEndian(writeTo, keyBytes.Length);
            writeTo = writeTo[sizeof(int)..];
            keyBytes.CopyTo(writeTo);
            writeTo = writeTo[keyBytes.Length..];

            // Value length, Value
            BinaryPrimitives.WriteInt32LittleEndian(writeTo, alignedValue.Length);
            writeTo = writeTo[sizeof(int)..];
            alignedValue.CopyTo(writeTo);
        }

        /// <summary>
        /// Reverse <see cref="SerializeMigratedElementKey"/>.
        /// </summary>
        public static void DeserializeMigratedElementKey(Span<byte> dataBytes, out Span<byte> namespaceBytes, out Span<byte> keyBytes, out Span<byte> value)
        {
            var readFrom = dataBytes;

            // Namespace length, Namespace
            var nsLength = BinaryPrimitives.ReadInt32LittleEndian(readFrom);
            Debug.Assert(nsLength == sizeof(uint), "Namespace should always be 4-bytes when deserializing");
            readFrom = readFrom[sizeof(uint)..];
            namespaceBytes = readFrom[..nsLength];
            readFrom = readFrom[nsLength..];

            // Key length, Key
            var keyLength = BinaryPrimitives.ReadInt32LittleEndian(readFrom);
            readFrom = readFrom[sizeof(int)..];
            keyBytes = readFrom[..keyLength];
            readFrom = readFrom[keyLength..];

            // Value length, Value
            var valueLength = BinaryPrimitives.ReadInt32LittleEndian(readFrom);
            readFrom = readFrom[sizeof(int)..];
            value = readFrom[..valueLength];
        }

        /// <summary>
        /// Calculate needed storage for migrating a Vector Set index key.
        /// </summary>
        public static int GetMigratedIndexKeySerializationSize(ReadOnlySpan<byte> keyBytes, ReadOnlySpan<byte> valueBytes)
        {
            var neededSpace = sizeof(int) + keyBytes.Length + sizeof(int) + valueBytes.Length;

            return neededSpace;
        }

        /// <summary>
        /// Serialize a record for migrating a Vector Set index key.
        /// </summary>
        public static void SerializeMigratedIndexKey(Span<byte> dataBytes, ReadOnlySpan<byte> keyBytes, ReadOnlySpan<byte> valueBytes)
        {
            Debug.Assert(valueBytes.Length == VectorManager.IndexSize, "Should only ever serialize index");

            var writeTo = dataBytes;

            // Key length, Key
            BinaryPrimitives.WriteInt32LittleEndian(writeTo, keyBytes.Length);
            writeTo = writeTo[sizeof(int)..];
            keyBytes.CopyTo(writeTo);
            writeTo = writeTo[keyBytes.Length..];

            // Value length, Value
            BinaryPrimitives.WriteInt32LittleEndian(writeTo, valueBytes.Length);
            writeTo = writeTo[sizeof(int)..];
            valueBytes.CopyTo(writeTo);
        }

        /// <summary>
        /// Reverse <see cref="SerializeMigratedIndexKey"/>.
        /// </summary>
        public static void DeserializeMigratedIndexKey(ReadOnlySpan<byte> dataBytes, out ReadOnlySpan<byte> keyBytes, out ReadOnlySpan<byte> valueBytes)
        {
            var readFrom = dataBytes;

            // Key length, Key
            var keyLength = BinaryPrimitives.ReadInt32LittleEndian(readFrom);
            readFrom = readFrom[sizeof(int)..];
            keyBytes = readFrom[..keyLength];
            readFrom = readFrom[keyLength..];

            // Value length, Value
            var valueLength = BinaryPrimitives.ReadInt32LittleEndian(readFrom);
            readFrom = readFrom[sizeof(int)..];
            valueBytes = readFrom[..valueLength];
        }
    }
}