// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
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
        /// Discover which of the given keys are RangeIndex keys by reading each via
        /// <see cref="RespCommand.RIGET"/> through the main store string context.
        /// Returns OK + stub bytes for RI keys, WRONGTYPE for non-RI keys.
        /// Mirrors <see cref="VectorManager.GetNamespacesForKeys"/> pattern.
        /// </summary>
        public unsafe Dictionary<byte[], byte[]> GetRangeIndexKeysForMigration(StoreWrapper storeWrapper, IEnumerable<byte[]> keys)
        {
            var rangeIndexKeys = new Dictionary<byte[], byte[]>(ByteArrayComparer.Instance);

            using var storageSession = new StorageSession(storeWrapper, new(), new(), null, null,
                storeWrapper.DefaultDatabase.Id, storeWrapper.DefaultDatabase.VectorManager, logger);

            Span<byte> stubSpan = stackalloc byte[IndexSizeBytes];

            foreach (var key in keys)
            {
                fixed (byte* keyPtr = key)
                {
                    var keyPsb = PinnedSpanByte.FromPinnedPointer(keyPtr, key.Length);

                    StringInput input = default;
                    input.header.cmd = RespCommand.RIGET;

                    var output = StringOutput.FromPinnedSpan(stubSpan);
                    var status = storageSession.Read_MainStore(keyPsb.ReadOnlySpan, ref input, ref output, ref storageSession.stringBasicContext);

                    if (status != GarnetStatus.OK)
                        continue;

                    var outputSpan = output.SpanByteAndMemory.IsSpanByte
                        ? output.SpanByteAndMemory.SpanByte.ReadOnlySpan
                        : output.SpanByteAndMemory.MemoryReadOnlySpan;

                    rangeIndexKeys[key] = outputSpan.ToArray();

                    if (!output.SpanByteAndMemory.IsSpanByte)
                        output.SpanByteAndMemory.Dispose();
                }
            }

            return rangeIndexKeys;
        }
        /// <summary>
        /// Default chunk size for streaming BfTree snapshot data during migration.
        /// </summary>
        public const int DefaultMigrationChunkSize = 256 * 1024;

        /// <summary>
        /// Source side: create a serializer that yields chunked migration records for a
        /// RangeIndex key. Snapshots the BfTree under an exclusive lock.
        /// </summary>
        /// <param name="localServerSession">The local server session for store access.</param>
        /// <param name="keyBytes">The key bytes of the RangeIndex to serialize.</param>
        /// <param name="stubBytes">The stub bytes for the RangeIndex.</param>
        /// <param name="chunkSize">The chunk size for streaming. Defaults to <see cref="DefaultMigrationChunkSize"/>.</param>
        public RangeIndexChunkedSerializer CreateSerializer(LocalServerSession localServerSession, ReadOnlySpan<byte> keyBytes, ReadOnlySpan<byte> stubBytes, int chunkSize = DefaultMigrationChunkSize)
        {
            if (!SnapshotForMigration(localServerSession.StorageSession, keyBytes, out var snapshotPath, out var totalBytes))
                throw new InvalidOperationException("Failed to snapshot BfTree for migration");

            var fileStream = new FileStream(snapshotPath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: chunkSize);
            return new RangeIndexChunkedSerializer(fileStream, keyBytes.ToArray(), stubBytes.ToArray(), totalBytes);
        }

        /// <summary>
        /// Derive a temporary file path for an in-progress inbound migration.
        /// Format: {dataDir}/rangeindex/.migration-tmp/{guid}.bftree
        /// </summary>
        public string DeriveTempMigrationPath()
        {
            var tmpDir = Path.Combine(dataDir ?? string.Empty, "rangeindex", ".migration-tmp");
            Directory.CreateDirectory(tmpDir);
            return Path.Combine(tmpDir, $"{Guid.NewGuid():N}.bftree");
        }

        /// <summary>
        /// Source side: snapshot a BfTree for migration under an exclusive lock.
        /// Acquires the exclusive lock, re-reads the stub from the store to get a fresh
        /// <c>TreeHandle</c>. If the tree is live, snapshots it to a temporary migration
        /// file. If evicted, copies the existing flush/checkpoint snapshot file to a
        /// temporary migration file. The temp file is safe from concurrent overwrites
        /// by <see cref="SnapshotTreeForFlush"/> or the native BfTree.
        /// </summary>
        internal bool SnapshotForMigration(StorageSession session, ReadOnlySpan<byte> keyBytes, out string path, out long totalBytes)
        {
            path = null;
            totalBytes = 0;

            Span<byte> stubSpan = stackalloc byte[IndexSizeBytes];
            var keyHash = session.stringBasicContext.GetKeyHash((FixedSpanByteKey)PinnedSpanByte.FromPinnedSpan(keyBytes));
            var migrationPath = DeriveTempMigrationPath();

            rangeIndexLocks.AcquireExclusiveLock(keyHash, out var lockToken);
            try
            {
                // Re-read the stub under exclusive lock to get a fresh TreeHandle
                StringInput input = default;
                input.header.cmd = RespCommand.RIGET;
                var output = StringOutput.FromPinnedSpan(stubSpan);

                var status = session.Read_MainStore(keyBytes, ref input, ref output, ref session.stringBasicContext);
                if (status != GarnetStatus.OK)
                {
                    logger?.LogWarning("SnapshotForMigration: key not found in store");
                    return false;
                }

                ref readonly var stub = ref ReadIndex(stubSpan);

                if (stub.StorageBackend == (byte)StorageBackendType.Memory)
                {
                    logger?.LogWarning("SnapshotForMigration: memory-only trees cannot be migrated");
                    return false;
                }

                if (stub.TreeHandle != nint.Zero && liveIndexes.TryGetValue(stub.TreeHandle, out var treeEntry))
                {
                    // Tree is live — snapshot to a temp migration file (safe from concurrent OnFlush overwrites)
                    treeEntry.Tree.SnapshotToFile(migrationPath);
                }
                else
                {
                    // Tree was evicted — copy the existing flush/checkpoint file to a temp migration file.
                    // Tsavorite guarantees OnFlush runs before OnEvict, so a snapshot file must exist.
                    var existingPath = stub.IsRecovered && recoveredCheckpointToken != Guid.Empty
                        ? DeriveCheckpointPath(keyBytes, recoveredCheckpointToken)
                        : DeriveFlushPath(keyBytes);

                    if (!File.Exists(existingPath))
                    {
                        logger?.LogWarning("SnapshotForMigration: expected snapshot file not found: {Path}", existingPath);
                        return false;
                    }

                    File.Copy(existingPath, migrationPath, overwrite: true);
                }
            }
            finally
            {
                rangeIndexLocks.ReleaseExclusiveLock(lockToken);
            }

            path = migrationPath;
            totalBytes = new FileInfo(migrationPath).Length;
            return true;
        }
    }
}