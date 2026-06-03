// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

using Garnet.common;
using Garnet.server.BfTreeInterop;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Migration support for RangeIndex keys: source-side snapshot and factory methods.
    /// </summary>
    public sealed partial class RangeIndexManager
    {
        /// <summary>
        /// Default chunk size for streaming BfTree snapshot data during migration.
        /// </summary>
        public const int DefaultMigrationChunkSize = 256 * 1024;

        /// <summary>
        /// Discover which of the given keys are RangeIndex keys by reading each via
        /// <see cref="RespCommand.RIGET"/> through <see cref="ReadRangeIndex"/> (under a shared lock).
        /// Returns the set of keys that are RangeIndex type. Stub bytes are NOT captured here
        /// to avoid TOCTOU — the authoritative stub is read later under exclusive lock by
        /// <see cref="SnapshotForMigration"/>.
        /// Mirrors <see cref="VectorManager.GetNamespacesForKeys"/> pattern.
        /// </summary>
        /// <remarks>
        /// Used by <c>CLUSTER MIGRATE ... KEYS</c> (KEYS path) only. The SLOTS path
        /// scans the slot range directly and does not need upfront discovery.
        /// </remarks>
        public unsafe HashSet<byte[]> GetRangeIndexKeysForMigration(StoreWrapper storeWrapper, IEnumerable<PinnedSpanByte> keys)
        {
            var rangeIndexKeys = new HashSet<byte[]>(ByteArrayComparer.Instance);

            using var storageSession = new StorageSession(
                storeWrapper: storeWrapper,
                scratchBufferBuilder: new(),
                scratchBufferAllocator: new(),
                sessionMetrics: null,
                LatencyMetrics: null,
                dbId: storeWrapper.DefaultDatabase.Id,
                readSessionState: null,
                vectorManager: storeWrapper.DefaultDatabase.VectorManager,
                logger: logger);

            Span<byte> stubSpan = stackalloc byte[IndexSizeBytes];

            foreach (var keyPsb in keys)
            {
                StringInput input = default;
                input.header.cmd = RespCommand.RIGET;

                using (ReadRangeIndex(storageSession, keyPsb, ref input, stubSpan, out var status))
                {
                    if (status != GarnetStatus.OK)
                        continue;

                    rangeIndexKeys.Add(keyPsb.ToArray());
                }
            }

            return rangeIndexKeys;
        }

        /// <summary>
        /// Source side: create a migration reader that snapshots the BfTree under an exclusive lock
        /// and produces chunked migration records via async file reads.
        /// </summary>
        /// <param name="localServerSession">The local server session for store access.</param>
        /// <param name="keyBytes">The key bytes of the RangeIndex to serialize.</param>
        /// <param name="chunkSize">The chunk size for streaming. Defaults to <see cref="DefaultMigrationChunkSize"/>.</param>
        public unsafe RangeIndexMigrationReader SnapshotRangeIndexAndCreateReader(LocalServerSession localServerSession, ReadOnlySpan<byte> keyBytes, int chunkSize = DefaultMigrationChunkSize)
        {
            fixed (byte* keyPtr = keyBytes)
            {
                var pinnedKey = PinnedSpanByte.FromPinnedPointer(keyPtr, keyBytes.Length);
                if (!SnapshotForMigration(localServerSession.storageSession, pinnedKey, out var snapshotPath, out var totalBytes, out var stubBytes))
                    throw new InvalidOperationException("Failed to snapshot BfTree for migration");

                var serializer = new RangeIndexChunkedSerializer(keyBytes.ToArray(), stubBytes, totalBytes);
                var fileStream = new FileStream(snapshotPath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: chunkSize);
                return new RangeIndexMigrationReader(serializer, fileStream, chunkSize);
            }
        }

        /// <summary>
        /// Derive a temporary file path for an in-progress inbound migration.
        /// Format: {riLogRoot}/migration-tmp/{guid}.bftree
        /// </summary>
        public string DeriveTempMigrationPath() => Path.Combine(migrationTempDir, $"{Guid.NewGuid():N}.bftree");

        /// <summary>
        /// Publish a migrated RangeIndex key: move the temp file to the working path,
        /// recover the native BfTree, and insert the stub into the store via RICREATE RMW.
        /// </summary>
        public unsafe bool PublishMigratedIndex(ReadOnlySpan<byte> keyBytes, ReadOnlySpan<byte> stubBytes, string tempPath, ref StringBasicContext ctx)
        {
            try
            {
                var workingPath = LogDataPathFor(keyBytes);
                Directory.CreateDirectory(Path.GetDirectoryName(workingPath)!);

                // If a data file already exists (e.g., from a previous migration of the same key
                // that was later deleted), remove it so the new snapshot can take its place.
                if (File.Exists(workingPath))
                    File.Delete(workingPath);

                File.Move(tempPath, workingPath);

                ref readonly var srcStub = ref ReadIndex(stubBytes);

                var scratchPath = LogScratchPathFor(keyBytes);
                var bfTree = BfTreeService.RecoverFromCprSnapshot(
                    workingPath,
                    scratchPath,
                    (StorageBackendType)srcStub.StorageBackend);

                Span<byte> newStubBytes = stackalloc byte[IndexSizeBytes];
                stubBytes.CopyTo(newStubBytes);
                ref var newStub = ref Unsafe.As<byte, RangeIndexStub>(
                    ref MemoryMarshal.GetReference(newStubBytes));
                newStub.TreeHandle = bfTree.NativePtr;
                newStub.ResetFlags();
                newStub.SerializationPhase = 0;

                var parseState = new SessionParseState();
                fixed (byte* stubPtr = newStubBytes, keyPtr = keyBytes)
                {
                    var stubSlice = PinnedSpanByte.FromPinnedPointer(stubPtr, IndexSizeBytes);
                    parseState.InitializeWithArgument(stubSlice);

                    var input = new StringInput(RespCommand.RICREATE, ref parseState);
                    var output = new StringOutput();
                    var pinnedKey = PinnedSpanByte.FromPinnedPointer(keyPtr, keyBytes.Length);
                    var status = ctx.RMW((FixedSpanByteKey)pinnedKey, ref input, ref output);
                    if (status.IsPending)
                        StorageSession.CompletePendingForSession(ref status, ref output, ref ctx);

                    if (status.Record.Created || status.Record.InPlaceUpdated || status.Record.CopyUpdated)
                    {
                        var keyHash = ctx.GetKeyHash((FixedSpanByteKey)pinnedKey);
                        RegisterIndex(bfTree, keyHash, keyBytes);
                    }
                    else
                    {
                        bfTree.Dispose();
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "PublishMigratedIndex: failed to recover BfTree");
                return false;
            }
        }

        /// <summary>
        /// Source side: snapshot a BfTree for migration under an exclusive lock.
        /// Acquires the exclusive lock, re-reads the stub from the store to get a fresh
        /// <c>TreeHandle</c>. If the tree is live, takes a CPR snapshot and copies
        /// the scratch file to a temporary migration file (following the same pattern as
        /// <see cref="SnapshotForFlushViaCpr"/>). If evicted, copies the working
        /// <c>data.bftree</c> file (same source as checkpoint pending entries).
        /// </summary>
        internal bool SnapshotForMigration(StorageSession session, PinnedSpanByte key, out string path, out long totalBytes, out byte[] stubBytes)
        {
            path = null;
            totalBytes = 0;
            stubBytes = null;

            var keyBytes = key.ReadOnlySpan;
            Span<byte> stubSpan = stackalloc byte[IndexSizeBytes];
            var keyHash = session.stringBasicContext.GetKeyHash((FixedSpanByteKey)key);
            var hashPrefix = HashKeyToPrefix(keyBytes);
            var migrationPath = DeriveTempMigrationPath();

            rangeIndexLocks.AcquireExclusiveLock(keyHash, out var lockToken);
            try
            {
                // Re-read the stub under exclusive lock to get a fresh TreeHandle
                StringInput input = default;
                input.header.cmd = RespCommand.RIGET;
                var output = StringOutput.FromPinnedSpan(stubSpan);

                var status = session.Read_RangeIndex(keyBytes, ref input, ref output, ref session.stringBasicContext);
                if (status != GarnetStatus.OK)
                {
                    logger?.LogWarning("SnapshotForMigration: key not found in store");
                    return false;
                }

                ref readonly var stub = ref ReadIndex(stubSpan);

                // Return the authoritative stub bytes read under exclusive lock
                stubBytes = stubSpan.ToArray();

                if (stub.StorageBackend == (byte)StorageBackendType.Memory)
                {
                    logger?.LogWarning("SnapshotForMigration: memory-only trees cannot be migrated");
                    return false;
                }

                var keyId = KeyId(keyBytes);
                if (stub.TreeHandle != nint.Zero && liveIndexes.TryGetValue(keyId, out var treeEntry) && treeEntry.Tree != null)
                {
                    // Tree is live — CPR snapshot + copy from scratch path (same pattern as SnapshotForFlushViaCpr).
                    // If a concurrent checkpoint already owns the snapshot claim, wait for it
                    // then copy the scratch file it just produced — avoids spinning under the exclusive lock.
                    var scratchPath = LogScratchPath(hashPrefix);
                    if (!treeEntry.TryClaimSnapshot())
                    {
                        treeEntry.WaitForSnapshot();
                        File.Copy(scratchPath, migrationPath, overwrite: false);
                    }
                    else
                    {
                        try
                        {
                            BfTreeService.CprSnapshotByPtr(treeEntry.Tree.NativePtr);
                            File.Copy(scratchPath, migrationPath, overwrite: false);
                        }
                        finally
                        {
                            treeEntry.ReleaseSnapshot();
                        }
                    }
                }
                else
                {
                    // Tree was evicted — copy the working data.bftree file (same source as checkpoint pending entries)
                    var dataPath = LogDataPath(hashPrefix);

                    if (!File.Exists(dataPath))
                    {
                        logger?.LogWarning("SnapshotForMigration: data.bftree not found: {Path}", dataPath);
                        return false;
                    }

                    File.Copy(dataPath, migrationPath, overwrite: false);
                }
            }
            finally
            {
                rangeIndexLocks.ReleaseLock(lockToken);
            }

            path = migrationPath;
            totalBytes = new FileInfo(migrationPath).Length;
            logger?.LogInformation("SnapshotForMigration: snapshot file {Path}, size {Size} bytes", migrationPath, totalBytes);
            return true;
        }
    }
}