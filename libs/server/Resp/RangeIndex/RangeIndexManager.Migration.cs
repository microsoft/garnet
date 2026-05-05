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
                    switch (status)
                    {
                        case GarnetStatus.OK:
                            // Key exists and is a RangeIndex.
                            rangeIndexKeys.Add(keyPsb.ToArray());
                            break;

                        case GarnetStatus.WRONGTYPE:
                        case GarnetStatus.NOTFOUND:
                            // Key exists but is not RI (WRONGTYPE), or doesn't exist (NOTFOUND).
                            // Either way: not an RI key — skip.
                            break;

                        default:
                            throw new GarnetException($"Unexpected status {status} from ReadRangeIndex while discovering RI keys for migration");
                    }
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
                return new RangeIndexMigrationReader(serializer, fileStream, snapshotPath, chunkSize, logger);
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
        /// <remarks>
        /// If ANY key (string, object, RI, vector, etc.) already exists at this key name
        /// on the destination, the migration is skipped regardless of
        /// <paramref name="replaceOption"/>:
        /// <list type="bullet">
        /// <item><paramref name="replaceOption"/> = false → <see cref="PublishMigratedIndexResult.SkippedAlreadyExists"/></item>
        /// <item><paramref name="replaceOption"/> = true  → <see cref="PublishMigratedIndexResult.SkippedReplaceNotSupported"/>
        /// (REPLACE for RI keys is not yet implemented; safe overwrite requires the staging-file
        /// + in-progress-marker design described in the TODO inside this method).</item>
        /// </list>
        /// </remarks>
        public unsafe PublishMigratedIndexResult PublishMigratedIndex(ReadOnlySpan<byte> keyBytes, ReadOnlySpan<byte> stubBytes, string tempPath, bool replaceOption, ref StringBasicContext ctx)
        {
            var keyExists = KeyExists(keyBytes, ref ctx);
            if (keyExists)
            {
                if (replaceOption)
                {
                    logger?.LogWarning("PublishMigratedIndex: a key already exists at this name and MIGRATE REPLACE was requested, but replacement is not yet supported for RangeIndex migration; skipping");
                    return PublishMigratedIndexResult.SkippedReplaceNotSupported;
                }

                logger?.LogWarning("PublishMigratedIndex: a key already exists at this name (use MIGRATE REPLACE to overwrite once supported); skipping");
                return PublishMigratedIndexResult.SkippedAlreadyExists;
            }

            try
            {
                var workingPath = LogDataPathFor(keyBytes);

                // Reaching here implies no key exists at this name in the store. A stale
                // data.bftree file may still be on disk from a previous migration of the
                // same key whose stub was later deleted (DEL triggers
                // DisposeAndDeleteFilesDeferred, but a crash between unlink-of-file and
                // tombstone-commit could leave a stale file). Remove it so File.Move below
                // can take its place.
                //
                // TODO: When MIGRATE REPLACE support is added for RI keys, the destructive
                // swap will need a staging-file + in-progress-marker design so that recovery
                // can either complete or roll back the swap if we crash mid-replacement:
                //   1. Move the migrated snapshot to a staging path (not workingPath).
                //   2. Atomically mark the existing stub as "replacement in progress"
                //      pointing at the staging path.
                //   3. Promote staging → workingPath only after the in-store stub has
                //      been updated; on recovery, replay the marker to either complete
                //      or roll back the swap.
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
                ref var newStub = ref Unsafe.As<byte, RangeIndexStub>(ref MemoryMarshal.GetReference(newStubBytes));
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

                return PublishMigratedIndexResult.Success;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "PublishMigratedIndex: failed to recover BfTree");
                return PublishMigratedIndexResult.Failed;
            }
        }

        /// <summary>
        /// Check whether ANY key (string, object, RI, vector, etc.) already exists at the
        /// given key name on this node. Returns <c>true</c> regardless of type — used as a
        /// pre-publish gate so we never overwrite an existing key with a migrated RangeIndex.
        /// </summary>
        /// <remarks>
        /// <para>Implementation: issues a plain <see cref="RespCommand.GET"/> through the
        /// string context and checks <c>status.Found || status.IsWrongType</c>:</para>
        /// <list type="bullet">
        /// <item><b>String record</b> → <c>Found = true</c></item>
        /// <item><b>Object record</b> (hash, list, set, sorted-set) → <c>IsWrongType = true</c>
        /// (<c>ReadMethods.Reader</c> rejects GET on <c>ValueIsObject</c> records).</item>
        /// <item><b>RangeIndex / Vector record</b> → <c>IsWrongType = true</c>
        /// (<c>CheckRecordTypeMismatch</c> rejects GET on typed records).</item>
        /// <item><b>Missing key</b> → both false.</item>
        /// </list>
        /// <para>Does not take the RI shared lock — destination-side publish already runs
        /// after the migration sketch gate is closed, so no concurrent RI writes can race.</para>
        /// </remarks>
        internal unsafe bool KeyExists(ReadOnlySpan<byte> keyBytes, ref StringBasicContext ctx)
        {
            Span<byte> outputSpan = stackalloc byte[1];
            var output = StringOutput.FromPinnedSpan(outputSpan);
            StringInput input = default;
            input.header.cmd = RespCommand.GET;

            fixed (byte* keyPtr = keyBytes)
            {
                var pinnedKey = PinnedSpanByte.FromPinnedPointer(keyPtr, keyBytes.Length);
                var readOptions = new ReadOptions { CopyOptions = ReadCopyOptions.None };
                var status = ctx.Read((FixedSpanByteKey)pinnedKey, ref input, ref output, ref readOptions);
                if (status.IsPending)
                    StorageSession.CompletePendingForSession(ref status, ref output, ref ctx);

                if (!output.SpanByteAndMemory.IsSpanByte)
                    output.SpanByteAndMemory.Dispose();

                return status.Found || status.IsWrongType;
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