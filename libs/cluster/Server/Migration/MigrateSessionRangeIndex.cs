// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using Garnet.client;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class MigrateSession : IDisposable
    {
        /// <summary>
        /// Ship a single RangeIndex (BfTree) key from the source primary to the destination primary
        /// as a sequence of <see cref="MigrationRecordSpanType.RangeIndexSnapshotChunk"/> payloads
        /// followed by a <see cref="MigrationRecordSpanType.RangeIndexStub"/> finalizer.
        /// </summary>
        /// <remarks>
        /// <para>The BfTree on-disk file format is not directly transportable as a single network record because
        /// it may be large (GB-scale) and because the backing <c>TreeHandle</c> in the stub is a local process
        /// pointer. To transport it we:
        /// <list type="number">
        ///   <item>Take a point-in-time snapshot of the live tree under the per-key exclusive lock.</item>
        ///   <item>GZip-compress the snapshot into a sibling <c>.gz</c> file.</item>
        ///   <item>Stream the compressed file as fixed-size chunks via the existing migration transport.</item>
        ///   <item>Send the raw stub bytes last; the destination treats that record as the finalizer.</item>
        ///   <item>Clean up the temp files and delete the source key (unless <c>COPY</c> is set).</item>
        /// </list></para>
        /// </remarks>
        /// <param name="mo">The source migrate operation whose client connects to the destination.</param>
        /// <param name="keyBytes">The Garnet key of the BfTree being migrated.</param>
        /// <param name="stubBytes">The raw stub bytes captured during scan (contains config fields).</param>
        /// <returns><c>true</c> on a fully-successful transfer; <c>false</c> on any failure (migration should abort).</returns>
        private bool TransmitRangeIndex(MigrateOperation mo, byte[] keyBytes, byte[] stubBytes)
        {
            var rim = clusterProvider.storeWrapper.DefaultDatabase.RangeIndexManager;
            if (rim is null)
            {
                logger?.LogError("TransmitRangeIndex: RangeIndexManager is null on source — cannot migrate");
                return false;
            }

            if (!rim.SnapshotForMigration(keyBytes, stubBytes, out var gzPath, out var uncompressedSize, out var compressedSize))
            {
                logger?.LogError("TransmitRangeIndex: snapshot+compress failed for key len={keyLen}", keyBytes.Length);
                return false;
            }

            var chunkSize = RangeIndexManager.DefaultMigrationChunkSize;
            var totalChunks = compressedSize == 0 ? 1 : (int)((compressedSize + chunkSize - 1) / chunkSize);
            var headerSize = sizeof(int) + keyBytes.Length + sizeof(int) + sizeof(int) + sizeof(long) + sizeof(int);
            var recordSize = headerSize + chunkSize;

            var recordBuffer = ArrayPool<byte>.Shared.Rent(recordSize);
            var chunkScratch = ArrayPool<byte>.Shared.Rent(chunkSize);

            var gcs = mo.Client;

            try
            {
                using (var input = new FileStream(gzPath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 1 << 16))
                {
                    for (var chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++)
                    {
                        // Read up to chunkSize bytes. ReadBlock in .NET FileStream is emulated via loop of Read calls.
                        var read = 0;
                        while (read < chunkSize)
                        {
                            var n = input.Read(chunkScratch, read, chunkSize - read);
                            if (n <= 0)
                                break;
                            read += n;
                        }

                        // Build the chunk payload: [keyLen][key][chunkIndex][totalChunks][uncompressedSize][chunkLen][chunk bytes]
                        var offset = 0;
                        BinaryPrimitives.WriteInt32LittleEndian(recordBuffer.AsSpan(offset), keyBytes.Length);
                        offset += sizeof(int);
                        keyBytes.CopyTo(recordBuffer.AsSpan(offset));
                        offset += keyBytes.Length;
                        BinaryPrimitives.WriteInt32LittleEndian(recordBuffer.AsSpan(offset), chunkIndex);
                        offset += sizeof(int);
                        BinaryPrimitives.WriteInt32LittleEndian(recordBuffer.AsSpan(offset), totalChunks);
                        offset += sizeof(int);
                        BinaryPrimitives.WriteInt64LittleEndian(recordBuffer.AsSpan(offset), uncompressedSize);
                        offset += sizeof(long);
                        BinaryPrimitives.WriteInt32LittleEndian(recordBuffer.AsSpan(offset), read);
                        offset += sizeof(int);
                        Array.Copy(chunkScratch, 0, recordBuffer, offset, read);
                        offset += read;

                        if (gcs.NeedsInitialization)
                            gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isVectorSets: false);

                        while (!gcs.TryWriteRecordSpan(recordBuffer.AsSpan(0, offset), MigrationRecordSpanType.RangeIndexSnapshotChunk, out var task))
                        {
                            if (!HandleMigrateTaskResponse(task))
                            {
                                logger?.LogCritical("TransmitRangeIndex: flush failed while sending chunk {idx}/{total}", chunkIndex, totalChunks);
                                return false;
                            }

                            gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isVectorSets: false);
                        }
                    }
                }

                // Finalizer: [keyLen][key][stub bytes]
                {
                    var finalSize = sizeof(int) + keyBytes.Length + stubBytes.Length;
                    var finalizer = finalSize <= recordBuffer.Length ? recordBuffer : ArrayPool<byte>.Shared.Rent(finalSize);
                    try
                    {
                        var offset = 0;
                        BinaryPrimitives.WriteInt32LittleEndian(finalizer.AsSpan(offset), keyBytes.Length);
                        offset += sizeof(int);
                        keyBytes.CopyTo(finalizer.AsSpan(offset));
                        offset += keyBytes.Length;
                        stubBytes.CopyTo(finalizer.AsSpan(offset));
                        offset += stubBytes.Length;

                        if (gcs.NeedsInitialization)
                            gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isVectorSets: false);

                        while (!gcs.TryWriteRecordSpan(finalizer.AsSpan(0, offset), MigrationRecordSpanType.RangeIndexStub, out var task))
                        {
                            if (!HandleMigrateTaskResponse(task))
                            {
                                logger?.LogCritical("TransmitRangeIndex: flush failed while sending stub finalizer");
                                return false;
                            }

                            gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isVectorSets: false);
                        }
                    }
                    finally
                    {
                        if (!ReferenceEquals(finalizer, recordBuffer))
                            ArrayPool<byte>.Shared.Return(finalizer);
                    }
                }

                // Force a flush so the destination has definitely seen everything before we delete the source.
                if (!HandleMigrateTaskResponse(gcs.SendAndResetIterationBuffer()))
                {
                    logger?.LogCritical("TransmitRangeIndex: flush failed before source-side delete");
                    return false;
                }

                unsafe
                {
                    fixed (byte* keyPtr = keyBytes)
                    {
                        var pinned = PinnedSpanByte.FromPinnedPointer(keyPtr, keyBytes.Length);
                        mo.DeleteRangeIndex(pinned);
                    }
                }

                logger?.LogInformation(
                    "TransmitRangeIndex: migrated key len={keyLen} uncompressed={uncompressed}B compressed={compressed}B chunks={chunks}",
                    keyBytes.Length, uncompressedSize, compressedSize, totalChunks);
                return true;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "TransmitRangeIndex: unexpected failure");
                return false;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(recordBuffer);
                ArrayPool<byte>.Shared.Return(chunkScratch);
                rim.CleanupMigrationArtifacts(keyBytes);
            }
        }
    }
}