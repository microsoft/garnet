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
        /// Outcome of <see cref="PublishMigratedIndex"/> for a single migrated RI key.
        /// </summary>
        public enum PublishMigratedIndexResult
        {
            /// <summary>The migrated RangeIndex was published successfully.</summary>
            Success,

            /// <summary>A RangeIndex already existed at this key and MIGRATE REPLACE was not specified;
            /// no destructive action was taken.</summary>
            SkippedAlreadyExists,

            /// <summary>A RangeIndex already existed at this key and MIGRATE REPLACE was specified,
            /// but RI key replacement is not yet supported; no destructive action was taken.</summary>
            SkippedReplaceNotSupported,

            /// <summary>Publish failed due to an exception or store-level error (logged).</summary>
            Failed,
        }

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
        public unsafe RangeIndexMigrationReader SnapshotRangeIndexAndCreateReader(LocalServerSession localServerSession, ReadOnlySpan<byte> keyBytes)
        {
            fixed (byte* keyPtr = keyBytes)
            {
                var pinnedKey = PinnedSpanByte.FromPinnedPointer(keyPtr, keyBytes.Length);
                if (!SnapshotForMigration(localServerSession.storageSession, pinnedKey, out var snapshotPath, out var totalBytes, out var stubBytes))
                    throw new InvalidOperationException("Failed to snapshot BfTree for migration");

                var serializer = new RangeIndexChunkedSerializer(keyBytes.ToArray(), stubBytes, totalBytes);
                var fileStream = new FileStream(snapshotPath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: RangeIndexMigrationReader.DefaultFileReadBufferSize);
                return new RangeIndexMigrationReader(serializer, fileStream, snapshotPath, logger);
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
        public unsafe PublishMigratedIndexResult PublishMigratedIndex(ReadOnlySpan<byte> keyBytes, ReadOnlySpan<byte> stubBytes, string tempPath, bool replaceOption, ref StringBasicContext ctx, GarnetAppendOnlyFile appendOnlyFile)
        {
            var keyExists = KeyExists(keyBytes, ref ctx);
            if (keyExists)
            {
                if (replaceOption)
                {
                    // TODO(RI): Implement REPLACE for RI keys
                    logger?.LogWarning("PublishMigratedIndex: a key already exists at this name and MIGRATE REPLACE was requested, but replacement is not yet supported for RangeIndex migration; skipping");
                    return PublishMigratedIndexResult.SkippedReplaceNotSupported;
                }

                logger?.LogWarning("PublishMigratedIndex: a key already exists at this name (use MIGRATE REPLACE to overwrite once supported); skipping");
                return PublishMigratedIndexResult.SkippedAlreadyExists;
            }

            try
            {
                var bftreeDataPath = LogDataPathFor(keyBytes);

                // TODO(RI): Acquire Checkpoint lock to prevent a concurrent checkpoint from deleting the
                // RangeIndexStream chunks added to the AOF before the BFTree is created

                // TODO(RI): Create RangeIndex first in Tsavorite and only then chunk and replicate through AOF

                // Replicate the migrated BfTree to secondaries by streaming the snapshot file into
                // the AOF as chunked range index stream entries. This must happen while tempPath is still
                // intact (before the move below). On replay, HandleRangeIndexStreamReplay reassembles
                // the file and re-invokes this method.
                ReplicateRangeIndexStream(keyBytes, stubBytes, tempPath, appendOnlyFile, ctx.Session.Version, ctx.Session.ID, rangeIndexAofStreamChunkSize);

                // TODO(RI): The KeyExists check above is not race-free. The destination slot is in
                // IMPORTING state, so a client can still write to this key via -ASK before we publish.
                // Such a write can land between the check and the publish below, so we may clobber it.
                // Handled in follow-up work (claim the key atomically before moving the file).

                if (File.Exists(bftreeDataPath))
                    File.Delete(bftreeDataPath);

                File.Move(tempPath, bftreeDataPath);

                ref readonly var srcStub = ref ReadIndex(stubBytes);

                var bfTree = BfTreeService.RecoverFromCprSnapshot(
                    bftreeDataPath,
                    enableSnapshots: true,
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

                    // Suppress the auto-AOF-log for this RICREATE: the range index stream enqueued above is
                    // the single AOF source of truth for this migrated key (see StreamedPublishLogArg).
                    input.arg1 = StreamedPublishLogArg;
                    var output = new StringOutput();
                    var pinnedKey = PinnedSpanByte.FromPinnedPointer(keyPtr, keyBytes.Length);
                    var status = ctx.RMW((FixedSpanByteKey)pinnedKey, ref input, ref output);
                    if (status.IsPending)
                        StorageSession.CompletePendingForSession(ref status, ref output, ref ctx);

                    // Expect a fresh Created record: the KeyExists gate above confirmed nothing
                    // existed at this key.
                    //
                    // TODO(RI): Handle the race below - claim the key using some transactional mechanism.
                    if (status.Record.Created)
                    {
                        var keyHash = ctx.GetKeyHash((FixedSpanByteKey)pinnedKey);
                        RegisterIndex(bfTree, keyHash, keyBytes);
                    }
                    else if (status.Record.InPlaceUpdated || status.Record.CopyUpdated)
                    {
                        // A record raced in between the gate and this RMW (e.g. an adversarial ASKING
                        // write while the slot is still IMPORTING) and our RICREATE has now clobbered
                        // it. Don't dispose: the updated record may now reference this bfTree.
                        logger?.LogWarning("PublishMigratedIndex: RICREATE RMW updated an existing record (status: {status}); a concurrent write raced the migration publish", status.ToString());
                    }
                    else
                    {
                        logger?.LogWarning("PublishMigratedIndex: RICREATE RMW did not create a record (status: {status}); discarding the recovered BfTree without registering it", status.ToString());
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

                // Found = string key; IsWrongType = object/RI/vector key (GET rejects typed records).
                return status.Found || status.IsWrongType;
            }
        }

        /// <summary>
        /// Source side: snapshot a BfTree for migration under an exclusive lock.
        /// Acquires the exclusive lock, re-reads the stub from the store to get a fresh
        /// <c>TreeHandle</c>. If the tree is live, takes a CPR snapshot straight to a
        /// temporary migration file (via <see cref="TreeEntry.SnapshotUnderClaim"/>). If
        /// evicted, copies the working
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
                    // Tree is live — CPR snapshot straight to the migration destination under the
                    // per-tree claim (serializes against concurrent checkpoint / flush snapshots on
                    // the same tree). The claim is independent of the exclusive rangeIndex lock and
                    // is held only for one non-blocking snapshot.
                    treeEntry.SnapshotUnderClaim(migrationPath);
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