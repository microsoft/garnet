// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Garnet.common;
using Garnet.server.BfTreeInterop;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Provides methods for managing range index operations, including replication and handling of AOF (Append-Only
    /// File) entries for create, set, and delete operations.
    /// </summary>
    public sealed partial class RangeIndexManager
    {
        /// <summary>
        /// Log RI.SET to AOF via direct enqueue (no synthetic RMW).
        /// Skipped when <paramref name="storedProcMode"/> is true (stored procedure logs as a unit).
        /// </summary>
        internal void ReplicateRangeIndexSet(PinnedSpanByte key, PinnedSpanByte field, PinnedSpanByte value,
            GarnetAppendOnlyFile appendOnlyFile, long version, int sessionId, bool storedProcMode)
        {
            if (appendOnlyFile == null || storedProcMode) return;

            var replicateParseState = new SessionParseState();
            replicateParseState.InitializeWithArguments(field, value);
            var input = new StringInput(RespCommand.RISET, ref replicateParseState);
            input.header.flags |= RespInputFlags.Deterministic;

            appendOnlyFile.Log.Enqueue(
                AofEntryType.StoreRMW,
                version,
                sessionId,
                key.ReadOnlySpan,
                ref input,
                out _);
        }

        /// <summary>
        /// Log RI.DEL to AOF via direct enqueue (no synthetic RMW).
        /// Skipped when <paramref name="storedProcMode"/> is true (stored procedure logs as a unit).
        /// </summary>
        internal void ReplicateRangeIndexDel(PinnedSpanByte key, PinnedSpanByte field,
            GarnetAppendOnlyFile appendOnlyFile, long version, int sessionId, bool storedProcMode)
        {
            if (appendOnlyFile == null || storedProcMode) return;

            var replicateParseState = new SessionParseState();
            replicateParseState.InitializeWithArgument(field);
            var input = new StringInput(RespCommand.RIDEL, ref replicateParseState);
            input.header.flags |= RespInputFlags.Deterministic;

            appendOnlyFile.Log.Enqueue(
                AofEntryType.StoreRMW,
                version,
                sessionId,
                key.ReadOnlySpan,
                ref input,
                out _);
        }

        /// <summary>
        /// Handle RI.CREATE replay from AOF.
        /// </summary>
        /// <remarks>
        /// The AOF entry contains the serialized stub bytes (including a stale TreeHandle
        /// from the original process). This method:
        /// <list type="number">
        /// <item>Extracts BfTree configuration from the stale stub.</item>
        /// <item>Creates a fresh BfTree instance with a new native pointer.</item>
        /// <item>Replaces the stale TreeHandle in the stub bytes with the new pointer.</item>
        /// <item>Lets the normal RMW path (InitialUpdater) create the store record.</item>
        /// </list>
        /// If the key already exists (e.g., AOF replay of a duplicate RI.CREATE after
        /// checkpoint recovery), the RMW returns <c>InPlaceUpdated</c> and the fresh
        /// BfTree is disposed.
        /// </remarks>
        /// <param name="session">The storage session for issuing the RMW.</param>
        /// <param name="key">The Garnet key being created.</param>
        /// <param name="input">The RMW input containing the stub bytes in parseState.</param>
        internal unsafe void HandleRangeIndexCreateReplay(StorageSession session, ReadOnlySpan<byte> key, ref StringInput input)
        {
            var stubSpan = input.parseState.GetArgSliceByRef(0).Span;
            if (stubSpan.Length != IndexSizeBytes)
                throw new GarnetException($"Corrupt RI.CREATE AOF entry: stub size {stubSpan.Length}, expected {IndexSizeBytes}");

            ref var stub = ref Unsafe.As<byte, RangeIndexStub>(ref MemoryMarshal.GetReference(stubSpan));

            // Create a fresh BfTree from the config in the stub
            BfTreeService bfTree;
            try
            {
                bfTree = CreateBfTree(
                    (StorageBackendType)stub.StorageBackend, key,
                    stub.CacheSize, stub.MinRecordSize, stub.MaxRecordSize,
                    stub.MaxKeyLen, stub.LeafPageSize);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Failed to recreate BfTree during AOF replay");
                return;
            }

            // Replace stale handle with fresh one in the stub bytes
            stub.TreeHandle = bfTree.NativePtr;
            stub.ResetFlags();

            // Let the normal RMW path create the record from the updated stub bytes
            var output = new StringOutput();
            var pinnedKey = PinnedSpanByte.FromPinnedSpan(key);
            var status = session.stringBasicContext.RMW((FixedSpanByteKey)pinnedKey, ref input, ref output);
            if (status.IsPending)
                StorageSession.CompletePendingForSession(ref status, ref output, ref session.stringBasicContext);

            if (status.Record.Created)
            {
                var keyHash = session.stringBasicContext.GetKeyHash((FixedSpanByteKey)pinnedKey);
                RegisterIndex(bfTree, keyHash, key);
            }
            else
            {
                bfTree.Dispose();
            }
        }

        /// <summary>
        /// Handle RI.SET replay from AOF. Acquires a shared lock, reads the stub to get
        /// the live BfTree pointer, then performs the native insert.
        /// </summary>
        /// <param name="session">The storage session for reading the stub.</param>
        /// <param name="key">The Garnet key of the RangeIndex.</param>
        /// <param name="input">The RMW input containing field and value in parseState.</param>
        internal void HandleRangeIndexSetReplay(StorageSession session, ReadOnlySpan<byte> key, ref StringInput input)
        {
            var field = input.parseState.GetArgSliceByRef(0);
            var value = input.parseState.GetArgSliceByRef(1);

            var pinnedKey = PinnedSpanByte.FromPinnedSpan(key);
            var inputCopy = input;
            inputCopy.arg1 = default;
            Span<byte> stubSpan = stackalloc byte[IndexSizeBytes];

            using (ReadRangeIndex(session, pinnedKey, ref inputCopy, stubSpan, out var status))
            {
                if (status != GarnetStatus.OK) return;
                var treePtr = ReadIndex(stubSpan).TreeHandle;
                if (treePtr == nint.Zero) return;
                BfTreeService.InsertByPtr(treePtr, field, value);
            }
        }

        /// <summary>
        /// Handle RI.DEL replay from AOF. Acquires a shared lock, reads the stub to get
        /// the live BfTree pointer, then performs the native delete.
        /// </summary>
        /// <param name="session">The storage session for reading the stub.</param>
        /// <param name="key">The Garnet key of the RangeIndex.</param>
        /// <param name="input">The RMW input containing the field in parseState.</param>
        internal void HandleRangeIndexDelReplay(StorageSession session, ReadOnlySpan<byte> key, ref StringInput input)
        {
            var field = input.parseState.GetArgSliceByRef(0);

            var pinnedKey = PinnedSpanByte.FromPinnedSpan(key);
            var inputCopy = input;
            inputCopy.arg1 = default;
            Span<byte> stubSpan = stackalloc byte[IndexSizeBytes];

            using (ReadRangeIndex(session, pinnedKey, ref inputCopy, stubSpan, out var status))
            {
                if (status != GarnetStatus.OK) return;
                var treePtr = ReadIndex(stubSpan).TreeHandle;
                if (treePtr == nint.Zero) return;
                BfTreeService.DeleteByPtr(treePtr, field);
            }
        }
    }
}