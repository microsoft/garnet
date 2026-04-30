// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Garnet.common;
using Garnet.server.BfTreeInterop;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Migration-specific logic for shipping a RangeIndex (BfTree) key from a source primary
    /// to a destination primary in the cluster. The BfTree data lives in a separate on-disk
    /// file (<c>data.bftree</c>) and is unusable outside the owning process, so migration:
    ///
    /// <list type="number">
    ///   <item>Snapshots the live BfTree to a migration-scoped file under an exclusive lock.</item>
    ///   <item>Compresses that file with GZip to a sibling <c>.gz</c> file.</item>
    ///   <item>Streams the compressed bytes as <see cref="Garnet.client.MigrationRecordSpanType.RangeIndexSnapshotChunk"/>
    ///         payloads followed by a <see cref="Garnet.client.MigrationRecordSpanType.RangeIndexStub"/> finalizer.</item>
    ///   <item>On the destination, reassembles the <c>.gz</c> file from chunks, decompresses it to
    ///         the deterministic <c>data.bftree</c> working path, calls
    ///         <see cref="BfTreeService.RecoverFromSnapshot"/>, rewrites the stub with the fresh
    ///         native pointer, RMWs the main-store record, and registers the tree.</item>
    /// </list>
    /// </summary>
    public sealed partial class RangeIndexManager
    {
        /// <summary>
        /// Default chunk size used for streaming a compressed BfTree snapshot. Chosen to fit
        /// comfortably inside a cluster migration network buffer along with per-record framing.
        /// </summary>
        public const int DefaultMigrationChunkSize = 256 * 1024;

        /// <summary>
        /// Derive the file path used to hold a migration-time BfTree snapshot for the given key.
        /// Distinct from <see cref="DeriveFlushPath"/> / <see cref="DeriveCheckpointPath"/> so
        /// that concurrent flush/checkpoint work doesn't collide with migration.
        /// </summary>
        internal string DeriveMigratePath(ReadOnlySpan<byte> keyBytes)
            => Path.Combine(dataDir ?? string.Empty, "rangeindex", HashKeyToDirectoryName(keyBytes), "migrate.bftree");

        /// <summary>
        /// Derive the file path used to hold the GZipped migration snapshot for the given key.
        /// </summary>
        internal string DeriveMigrateCompressedPath(ReadOnlySpan<byte> keyBytes)
            => Path.Combine(dataDir ?? string.Empty, "rangeindex", HashKeyToDirectoryName(keyBytes), "migrate.bftree.gz");

        /// <summary>
        /// Derive the file path used by the destination primary to reassemble an inbound
        /// compressed BfTree snapshot during migration restore.
        /// </summary>
        internal string DeriveRestoreCompressedPath(ReadOnlySpan<byte> keyBytes)
            => Path.Combine(dataDir ?? string.Empty, "rangeindex", HashKeyToDirectoryName(keyBytes), "restore.bftree.gz");

        /// <summary>
        /// Snapshot and compress a BfTree so it can be streamed across the wire.
        /// The caller is expected to already know the key exists and that it is RangeIndex-typed
        /// (i.e. the captured stub from the cluster migrate scan).
        ///
        /// <para>Operation: acquires the RangeIndex exclusive lock so that in-flight data writes
        /// observe a point-in-time snapshot, calls the native snapshot routine to copy the
        /// backing file to <see cref="DeriveMigratePath"/>, then streams that file through
        /// <see cref="GZipStream"/> into <see cref="DeriveMigrateCompressedPath"/>. The lock is
        /// released before compression, since compression only needs the quiescent snapshot file.</para>
        /// </summary>
        /// <param name="key">The Garnet key of the BfTree being migrated.</param>
        /// <param name="stubBytes">Stub bytes captured during scan (must contain a valid TreeHandle from this process).</param>
        /// <param name="compressedPath">On success, the path to the compressed snapshot file.</param>
        /// <param name="uncompressedSize">On success, the size in bytes of the uncompressed snapshot.</param>
        /// <param name="compressedSize">On success, the size in bytes of the compressed snapshot.</param>
        /// <returns><c>true</c> on success; <c>false</c> if the tree is no longer live or backend is unsupported.</returns>
        public bool SnapshotForMigration(
            ReadOnlySpan<byte> key,
            ReadOnlySpan<byte> stubBytes,
            out string compressedPath,
            out long uncompressedSize,
            out long compressedSize)
        {
            compressedPath = null;
            uncompressedSize = 0;
            compressedSize = 0;

            if (stubBytes.Length < RangeIndexStub.Size)
            {
                logger?.LogError("SnapshotForMigration: invalid stub size {size}", stubBytes.Length);
                return false;
            }

            ref readonly var stub = ref Unsafe.As<byte, RangeIndexStub>(
                ref MemoryMarshal.GetReference(stubBytes));

            if (stub.StorageBackend == (byte)StorageBackendType.Memory)
            {
                logger?.LogError("SnapshotForMigration: memory-only BfTrees cannot be migrated (native snapshot unsupported)");
                return false;
            }

            if (stub.TreeHandle == nint.Zero)
            {
                logger?.LogError("SnapshotForMigration: stub has null TreeHandle — tree not live in this process");
                return false;
            }

            if (!liveIndexes.TryGetValue(stub.TreeHandle, out var entry))
            {
                logger?.LogError("SnapshotForMigration: TreeHandle {handle} not found in liveIndexes", stub.TreeHandle);
                return false;
            }

            var migratePath = DeriveMigratePath(key);
            var gzPath = DeriveMigrateCompressedPath(key);
            Directory.CreateDirectory(Path.GetDirectoryName(migratePath)!);

            // Take a point-in-time snapshot to migrate.bftree under the exclusive lock so
            // no data writes interleave mid-snapshot.
            rangeIndexLocks.AcquireExclusiveLock(entry.KeyHash, out var lockToken);
            try
            {
                entry.Tree.SnapshotToFile(migratePath);
            }
            finally
            {
                rangeIndexLocks.ReleaseExclusiveLock(lockToken);
            }

            uncompressedSize = new FileInfo(migratePath).Length;

            // GZip compress the snapshot. Streaming via buffered copy keeps memory usage bounded
            // regardless of tree size.
            using (var input = new FileStream(migratePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 1 << 16))
            using (var output = new FileStream(gzPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 1 << 16))
            using (var gz = new GZipStream(output, CompressionLevel.Fastest, leaveOpen: false))
            {
                input.CopyTo(gz);
            }

            compressedSize = new FileInfo(gzPath).Length;
            compressedPath = gzPath;

            logger?.LogInformation(
                "SnapshotForMigration: key len={keyLen} uncompressed={uncompressed}B compressed={compressed}B ratio={ratio:F2}",
                key.Length, uncompressedSize, compressedSize, uncompressedSize == 0 ? 0 : (double)compressedSize / uncompressedSize);

            return true;
        }

        /// <summary>
        /// Clean up migration-time temporary files for a given key. Safe to call regardless of
        /// whether the files exist (e.g. on the happy path, on error, and when re-running).
        /// </summary>
        public void CleanupMigrationArtifacts(ReadOnlySpan<byte> key)
        {
            try
            {
                var migratePath = DeriveMigratePath(key);
                if (File.Exists(migratePath))
                    File.Delete(migratePath);

                var gzPath = DeriveMigrateCompressedPath(key);
                if (File.Exists(gzPath))
                    File.Delete(gzPath);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "CleanupMigrationArtifacts: failed to delete migration temp files for RangeIndex key");
            }
        }

        // --------------------------------------------------------------------
        // Destination-side state and handlers
        // --------------------------------------------------------------------

        /// <summary>
        /// Per-key state used by the destination primary to accumulate an inbound compressed
        /// BfTree snapshot across multiple <see cref="Garnet.client.MigrationRecordSpanType.RangeIndexSnapshotChunk"/>
        /// payloads before finalization.
        /// </summary>
        internal sealed class InboundRestoreState : IDisposable
        {
            public readonly string RestoreGzPath;
            public FileStream Stream;
            public int ExpectedTotalChunks;
            public int NextExpectedChunkIndex;
            public long BytesReceived;
            public long UncompressedSnapshotSize;
            public bool Faulted;

            public InboundRestoreState(string restoreGzPath)
            {
                RestoreGzPath = restoreGzPath;
                Stream = new FileStream(restoreGzPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 1 << 16);
                ExpectedTotalChunks = -1;
                NextExpectedChunkIndex = 0;
                BytesReceived = 0;
                UncompressedSnapshotSize = 0;
                Faulted = false;
            }

            public void Dispose()
            {
                try
                {
                    Stream?.Dispose();
                }
                catch
                {
                    // Best-effort cleanup.
                }
                Stream = null;
            }
        }

        /// <summary>
        /// Per-session inbound-restore state, keyed by the Garnet key being reconstructed.
        /// A new entry is created on the first chunk and removed after the finalizer runs.
        /// </summary>
        private readonly ConcurrentDictionary<byte[], InboundRestoreState> inboundRestoreByKey
            = new(ByteArrayComparer.Instance);

        /// <summary>
        /// Handle a single inbound <see cref="Garnet.client.MigrationRecordSpanType.RangeIndexSnapshotChunk"/> payload on the
        /// destination primary. Parses the per-chunk header, validates ordering against any prior chunks
        /// received for the same key, and appends the chunk bytes to the on-disk restore file.
        /// </summary>
        /// <remarks>
        /// Payload layout:
        /// <code>[int keyLen][key bytes][int chunkIndex][int totalChunks][long uncompressedSnapshotSize][int chunkLen][chunk bytes]</code>
        /// </remarks>
        /// <param name="payload">Raw payload bytes (without the leading MigrationRecordSpanType byte).</param>
        public void HandleMigratedRangeIndexChunk(ReadOnlySpan<byte> payload)
        {
            var offset = 0;

            var keyLen = BinaryPrimitives.ReadInt32LittleEndian(payload[offset..]);
            offset += sizeof(int);
            var keyBytes = payload.Slice(offset, keyLen);
            offset += keyLen;

            var chunkIndex = BinaryPrimitives.ReadInt32LittleEndian(payload[offset..]);
            offset += sizeof(int);
            var totalChunks = BinaryPrimitives.ReadInt32LittleEndian(payload[offset..]);
            offset += sizeof(int);
            var uncompressedSnapshotSize = BinaryPrimitives.ReadInt64LittleEndian(payload[offset..]);
            offset += sizeof(long);
            var chunkLen = BinaryPrimitives.ReadInt32LittleEndian(payload[offset..]);
            offset += sizeof(int);
            var chunkBytes = payload.Slice(offset, chunkLen);

            var keyArr = keyBytes.ToArray();

            var state = inboundRestoreByKey.GetOrAdd(keyArr, k =>
            {
                var gzPath = DeriveRestoreCompressedPath(k);
                Directory.CreateDirectory(Path.GetDirectoryName(gzPath)!);
                var st = new InboundRestoreState(gzPath)
                {
                    ExpectedTotalChunks = totalChunks,
                    UncompressedSnapshotSize = uncompressedSnapshotSize,
                };
                return st;
            });

            lock (state)
            {
                if (state.Faulted)
                    return;

                if (state.ExpectedTotalChunks != totalChunks || state.UncompressedSnapshotSize != uncompressedSnapshotSize)
                {
                    logger?.LogError(
                        "RangeIndex migration chunk header mismatch for key (totalChunks expected={exp} got={got}, uncompressedSize expected={expSize} got={gotSize})",
                        state.ExpectedTotalChunks, totalChunks, state.UncompressedSnapshotSize, uncompressedSnapshotSize);
                    FaultAndRemoveInbound(keyArr, state);
                    return;
                }

                if (chunkIndex != state.NextExpectedChunkIndex)
                {
                    logger?.LogError(
                        "RangeIndex migration chunk out of order: expected={exp} got={got}",
                        state.NextExpectedChunkIndex, chunkIndex);
                    FaultAndRemoveInbound(keyArr, state);
                    return;
                }

                state.Stream.Write(chunkBytes);
                state.NextExpectedChunkIndex++;
                state.BytesReceived += chunkLen;
            }
        }

        /// <summary>
        /// Handle the finalizer <see cref="Garnet.client.MigrationRecordSpanType.RangeIndexStub"/> payload on the
        /// destination primary after all chunks have been received. Decompresses the reassembled
        /// <c>.gz</c> file to the deterministic <c>data.bftree</c> working path, recovers a fresh
        /// BfTree from it, rewrites the stub with the new local TreeHandle, writes the stub to the
        /// main store via an RICREATE RMW, and registers the tree in <see cref="liveIndexes"/>.
        /// </summary>
        /// <remarks>
        /// Payload layout: <c>[int keyLen][key bytes][stub bytes]</c> (stub is <see cref="RangeIndexManager.RangeIndexStub.Size"/> bytes).
        /// </remarks>
        /// <param name="stringBasicContext">String basic context used to issue the stub-creation RMW into the main store.</param>
        /// <param name="payload">Raw payload bytes (without the leading MigrationRecordSpanType byte).</param>
        public unsafe void HandleMigratedRangeIndexStub(ref StringBasicContext stringBasicContext, ReadOnlySpan<byte> payload)
        {
            var offset = 0;
            var keyLen = BinaryPrimitives.ReadInt32LittleEndian(payload[offset..]);
            offset += sizeof(int);
            var keyBytes = payload.Slice(offset, keyLen).ToArray();
            offset += keyLen;
            var stubBytes = payload.Slice(offset, RangeIndexStub.Size).ToArray();

            if (!inboundRestoreByKey.TryRemove(keyBytes, out var state))
            {
                logger?.LogError("RangeIndex migration finalizer received with no matching chunk state for key");
                return;
            }

            var restoreGzPath = state.RestoreGzPath;

            try
            {
                lock (state)
                {
                    if (state.Faulted)
                    {
                        logger?.LogError("RangeIndex migration finalizer on faulted key — discarding");
                        return;
                    }

                    state.Stream?.Flush();
                    state.Stream?.Dispose();
                    state.Stream = null;

                    if (state.NextExpectedChunkIndex != state.ExpectedTotalChunks)
                    {
                        logger?.LogError(
                            "RangeIndex migration finalizer with incomplete chunks: received={recv} expected={exp}",
                            state.NextExpectedChunkIndex, state.ExpectedTotalChunks);
                        return;
                    }
                }

                // Decompress the reassembled .gz to the deterministic data.bftree working path.
                var workingPath = DeriveWorkingPath(keyBytes);
                Directory.CreateDirectory(Path.GetDirectoryName(workingPath)!);

                using (var inFile = new FileStream(restoreGzPath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 1 << 16))
                using (var gz = new GZipStream(inFile, CompressionMode.Decompress, leaveOpen: false))
                using (var outFile = new FileStream(workingPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 1 << 16))
                {
                    gz.CopyTo(outFile);
                }

                // Extract config from the received stub and recover a fresh BfTree bound to the local process.
                var stubCopy = Unsafe.As<byte, RangeIndexStub>(ref stubBytes[0]);

                var bfTree = BfTreeService.RecoverFromSnapshot(
                    workingPath,
                    (StorageBackendType)stubCopy.StorageBackend,
                    stubCopy.CacheSize,
                    stubCopy.MinRecordSize,
                    stubCopy.MaxRecordSize,
                    stubCopy.MaxKeyLen,
                    stubCopy.LeafPageSize);

                // Rewrite TreeHandle + clear flags in the stub bytes before issuing the RMW.
                ref var stubRef = ref Unsafe.As<byte, RangeIndexStub>(ref stubBytes[0]);
                stubRef.TreeHandle = bfTree.NativePtr;
                stubRef.Flags = 0;
                stubRef.SerializationPhase = 0;

                bool created;
                long keyHash = 0;
                fixed (byte* keyPtr = keyBytes)
                fixed (byte* stubPtr = stubBytes)
                {
                    var pinnedKey = PinnedSpanByte.FromPinnedPointer(keyPtr, keyBytes.Length);
                    var pinnedStub = PinnedSpanByte.FromPinnedPointer(stubPtr, RangeIndexStub.Size);

                    var localParseState = new SessionParseState();
                    localParseState.InitializeWithArgument(pinnedStub);

                    var input = new StringInput(RespCommand.RICREATE, ref localParseState);
                    var output = new StringOutput();

                    var status = stringBasicContext.RMW((FixedSpanByteKey)pinnedKey, ref input, ref output);
                    if (status.IsPending)
                        StorageSession.CompletePendingForSession(ref status, ref output, ref stringBasicContext);

                    created = status.Record.Created;
                    if (created)
                        keyHash = stringBasicContext.GetKeyHash((FixedSpanByteKey)pinnedKey);
                }

                if (!created)
                {
                    logger?.LogError("RangeIndex migration finalizer: RMW for stub creation failed — disposing new tree");
                    bfTree.Dispose();
                    return;
                }

                RegisterIndex(bfTree, keyHash, keyBytes);

                logger?.LogInformation(
                    "RangeIndex migration finalized: key len={keyLen} bytesReceived={bytes} chunks={chunks}",
                    keyBytes.Length, state.BytesReceived, state.ExpectedTotalChunks);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "RangeIndex migration finalizer threw");
            }
            finally
            {
                state.Dispose();
                // Best-effort cleanup of the inbound .gz file. Leave the decompressed data.bftree in place.
                try
                {
                    if (File.Exists(restoreGzPath))
                        File.Delete(restoreGzPath);
                }
                catch
                {
                    // ignore
                }
            }
        }

        /// <summary>
        /// Mark an inbound restore state as faulted, close its stream, and remove it from the
        /// per-session map. Used when we detect a protocol violation mid-transfer.
        /// </summary>
        private void FaultAndRemoveInbound(byte[] key, InboundRestoreState state)
        {
            state.Faulted = true;
            state.Dispose();
            _ = inboundRestoreByKey.TryRemove(key, out _);
            try
            {
                if (File.Exists(state.RestoreGzPath))
                    File.Delete(state.RestoreGzPath);
            }
            catch
            {
                // ignore
            }
        }

        /// <summary>
        /// Abort all in-flight inbound restore state. Called when a migration session is torn
        /// down before finalization, e.g. on connection drop.
        /// </summary>
        internal void AbortAllInboundRestores()
        {
            foreach (var kvp in inboundRestoreByKey)
            {
                var st = kvp.Value;
                try
                {
                    st.Dispose();
                    if (File.Exists(st.RestoreGzPath))
                        File.Delete(st.RestoreGzPath);
                }
                catch
                {
                    // ignore
                }
            }
            inboundRestoreByKey.Clear();
        }

        [Conditional("DEBUG")]
        internal void DebugAssertNoInboundRestoreState() => Debug.Assert(inboundRestoreByKey.IsEmpty);
    }
}