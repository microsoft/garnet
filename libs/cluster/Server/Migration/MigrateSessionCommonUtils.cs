// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class MigrateSession : IDisposable
    {
        private unsafe ValueTask<bool> WriteOrSendRecordAsync(GarnetClientSession gcs, LocalServerSession localServerSession, PinnedSpanByte namespaceBytes, PinnedSpanByte key, ref VectorInput input, ref VectorOutput output, out GarnetStatus status)
        {
            // Must initialize this here because we use the network buffer as output.
            if (gcs.NeedsInitialization)
                gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isVectorSets: false);

            // Read the value for the key. This will populate output with the entire serialized record.
            var storeStatus = localServerSession.VectorBasicContext.Read(new VectorElementKey(namespaceBytes.ReadOnlySpan, key.ReadOnlySpan), ref input, ref output);

            if (storeStatus.IsPending)
            {
                CompletePending(ref storeStatus, ref output, ref localServerSession.VectorBasicContext);
            }

            if (storeStatus.Found)
            {
                status = GarnetStatus.OK;
            }
            else if (storeStatus.IsWrongType)
            {
                status = GarnetStatus.WRONGTYPE;
            }
            else
            {
                status = GarnetStatus.NOTFOUND;
            }

            // Skip (but do not fail) if key NOTFOUND, WRONGTYPE, BADSTATE, etc.
            if (status != GarnetStatus.OK)
            {
                return new(true);
            }

            // Map up any namespaces as needed
            VectorManager.UpdateMigratedElementNamespaces(_namespaceMap, ref input, ref output);

            fixed (byte* ptr = output.SpanByteAndMemory.Span)
            {
                return WriteOrSendRecordSpanAsync(gcs, MigrationRecordSpanType.VectorSetElement, new(ptr, output.SpanByteAndMemory.Span.Length));
            }

            // Complete reads that go pending
            static void CompletePending(ref Status status, ref VectorOutput output, ref VectorBasicContext ctx)
            {
                _ = ctx.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                var more = completedOutputs.Next();
                Debug.Assert(more);
                status = completedOutputs.Current.Status;
                output = completedOutputs.Current.Output;
                Debug.Assert(!completedOutputs.Next());
                completedOutputs.Dispose();
            }
        }

        // Reusable per-record scratch for the accumulator send path (one record is fully sent before the next begins).
        readonly List<ReadOnlyMemory<byte>> sendPieces = [];
        byte[] sendAssembleBuffer = [];

        // Reusable 4-byte length prefixes written before each overflow key/value piece on the chunked send path: the
        // receiver reads the full length from the stream to allocate the overflow buffer up front. One record is fully sent before
        // the next begins, so these can be reused per record.
        readonly byte[] chunkedKeyLengthPrefix = new byte[sizeof(int)];
        readonly byte[] chunkedValueLengthPrefix = new byte[sizeof(int)];

        private unsafe ValueTask<bool> WriteOrSendRecordAsync(GarnetClientSession gcs, LocalServerSession localServerSession, PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output, out GarnetStatus status)
        {
            // Must initialize this here because we use the network buffer as output.
            if (gcs.NeedsInitialization)
                gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isVectorSets: false);

            // Read the key. HandleMigrate captures the record's pieces in-epoch: the inline portion into output.SpanByteAndMemory,
            // and any overflow key / overflow value / object value into output.Accumulator. We assemble and send them out of epoch
            // (migration sends asynchronously and the store epoch must not be held across an await).
            status = localServerSession.BasicGarnetApi.Read_UnifiedStore(key, ref input, ref output);

            // Skip (but do not fail) if key NOTFOUND, WRONGTYPE, BADSTATE, etc.
            if (status != GarnetStatus.OK)
                return new(true);

            var acc = output.Accumulator;

            // Fully-inline record: SpanByteAndMemory holds the whole record. Send it whole (or chunked if it is unusually large).
            if (acc.IsEmpty)
            {
                if (acc.InlineLength <= NetworkBufferSettings.MaxSendBufferContentSize)
                {
                    fixed (byte* ptr = output.SpanByteAndMemory.Span)
                        return WriteOrSendRecordSpanAsync(gcs, MigrationRecordSpanType.LogRecord, new ReadOnlySpan<byte>(ptr, acc.InlineLength));
                }
                return WriteOrSendChunkedRecordAsync(gcs, output.SpanByteAndMemory.Memory.Memory[..acc.InlineLength]);
            }

            // Non-inline: assemble [inline][overflow key][overflow value | object chunks] and send it whole (if it fits a send
            // buffer) or as ChunkedLogRecord chunks (large record / > 2 GB object).
            return WriteOrSendAccumulatedRecordAsync(gcs, output.SpanByteAndMemory.Memory.Memory[..acc.InlineLength], acc);
        }

        /// <summary>
        /// Assemble a non-inline record from its captured pieces (inline portion + overflow key + overflow value or object value
        /// chunks) and send it: whole (type <see cref="MigrationRecordSpanType.LogRecord"/>) if it fits a send buffer, else as a
        /// sequence of <see cref="MigrationRecordSpanType.ChunkedLogRecord"/> chunks (continuation flag set until the record's
        /// final byte). On the chunked path each overflow key/value piece is preceded by a 4-byte length prefix so the
        /// receiver can allocate the overflow buffer up front and populate it directly; an object value is streamed with no prefix
        /// (its length is derived from the stream). The concatenated pieces form exactly the stream the receiver reassembles.
        /// </summary>
        private async ValueTask<bool> WriteOrSendAccumulatedRecordAsync(GarnetClientSession gcs, ReadOnlyMemory<byte> inline, MigrationChunkWriterAccumulator acc)
        {
            var maxChunk = NetworkBufferSettings.MaxSendBufferContentSize;
            var total = inline.Length + acc.KeyLength + acc.ValueLength;

            // A record whose whole serialized form fits one send buffer is sent as a single LogRecord — including an object
            // value: its object bytes are the tail of the image, so the receiver derives the object length from the record span
            // (the RDH object length is left zero). A larger record is streamed as ChunkedLogRecord chunks.
            if (total <= maxChunk)
            {
                // Small enough for one send buffer: assemble contiguously and send as a single whole record.
                if (sendAssembleBuffer.Length < total)
                    sendAssembleBuffer = new byte[(int)total];
                var span = sendAssembleBuffer.AsSpan(0, (int)total);
                var off = inline.Length;
                inline.Span.CopyTo(span);
                if (acc.HasKey)
                {
                    acc.KeyMemory.Span.CopyTo(span.Slice(off));
                    off += (int)acc.KeyLength;
                }
                if (acc.HasValueOverflow)
                    acc.ValueOverflowMemory.Span.CopyTo(span.Slice(off));
                else if (acc.HasObjectValue)
                {
                    foreach (var chunk in acc.ObjectValueChunks)
                    {
                        chunk.CopyTo(span.Slice(off));
                        off += chunk.Length;
                    }
                }
                return await WriteOrSendRecordSpanAsync(gcs, MigrationRecordSpanType.LogRecord, sendAssembleBuffer.AsSpan(0, (int)total)).ConfigureAwait(false);
            }

            // Large (or > 2 GB object): send the pieces in order as ChunkedLogRecord chunks, prefixing each overflow key/value with
            // its 4-byte length. Chunk boundaries are arbitrary send-buffer cut points; the receiver reassembles the
            // stream and routes each component by the layout header and these prefixes.
            sendPieces.Clear();
            sendPieces.Add(inline);
            if (acc.HasKey)
            {
                BinaryPrimitives.WriteInt32LittleEndian(chunkedKeyLengthPrefix, (int)acc.KeyLength);
                sendPieces.Add(chunkedKeyLengthPrefix);
                sendPieces.Add(acc.KeyMemory);
            }
            if (acc.HasValueOverflow)
            {
                BinaryPrimitives.WriteInt32LittleEndian(chunkedValueLengthPrefix, (int)acc.ValueLength);
                sendPieces.Add(chunkedValueLengthPrefix);
                sendPieces.Add(acc.ValueOverflowMemory);
            }
            else if (acc.HasObjectValue)
            {
                // Object value: streamed with no length prefix (the receiver derives its length from the reassembled stream).
                foreach (var chunk in acc.ObjectValueChunks)
                    sendPieces.Add(chunk);
            }

            // Recompute the total including the length-prefix bytes so the continuation flag clears exactly on the record's last byte.
            long chunkedTotal = 0;
            foreach (var piece in sendPieces)
                chunkedTotal += piece.Length;

            long sent = 0;
            foreach (var piece in sendPieces)
            {
                var offset = 0;
                while (offset < piece.Length)
                {
                    var chunkLength = Math.Min(maxChunk, piece.Length - offset);
                    var moreChunksFollow = sent + chunkLength < chunkedTotal;

                    if (gcs.NeedsInitialization)
                        gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isVectorSets: false);

                    if (gcs.TryWriteChunkedRecordSpan(piece.Span.Slice(offset, chunkLength), moreChunksFollow, out var task))
                    {
                        offset += chunkLength;
                        sent += chunkLength;
                        continue;
                    }

                    // Client buffer is full: flush and retry the same chunk.
                    if (!await HandleMigrateTaskResponseAsync(task).ConfigureAwait(false))
                        return false;
                    gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isVectorSets: false);
                }
            }
            return true;
        }

        /// <summary>
        /// Send a serialized record larger than one send buffer as a sequence of <see cref="MigrationRecordSpanType.ChunkedLogRecord"/>
        /// chunks, flushing and retrying when the client buffer fills. The receiver reassembles the chunks and deserializes.
        /// </summary>
        private async ValueTask<bool> WriteOrSendChunkedRecordAsync(GarnetClientSession gcs, ReadOnlyMemory<byte> record)
        {
            var maxChunk = NetworkBufferSettings.MaxSendBufferContentSize;
            var offset = 0;
            while (offset < record.Length)
            {
                var chunkLength = Math.Min(maxChunk, record.Length - offset);
                var moreChunksFollow = offset + chunkLength < record.Length;

                if (gcs.NeedsInitialization)
                    gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isVectorSets: false);

                if (gcs.TryWriteChunkedRecordSpan(record.Span.Slice(offset, chunkLength), moreChunksFollow, out var task))
                {
                    offset += chunkLength;
                    continue;
                }

                // Client buffer is full: flush and retry the same chunk.
                if (!await HandleMigrateTaskResponseAsync(task).ConfigureAwait(false))
                    return false;
                gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isVectorSets: false);
            }

            return true;
        }

        /// <summary>
        /// Send a serialized record held as a list of segments (used when an object value serialized to more than 2 GB, which a
        /// single buffer cannot hold) as a sequence of <see cref="MigrationRecordSpanType.ChunkedLogRecord"/> chunks. The chunks
        /// carry the concatenated segment bytes; the continuation flag is set until the record's final byte.
        /// </summary>
        private async ValueTask<bool> WriteOrSendSegmentedRecordAsync(GarnetClientSession gcs, List<byte[]> segments)
        {
            var maxChunk = NetworkBufferSettings.MaxSendBufferContentSize;
            long total = 0;
            foreach (var segment in segments)
                total += segment.Length;

            long sent = 0;
            foreach (var segment in segments)
            {
                var offset = 0;
                while (offset < segment.Length)
                {
                    var chunkLength = (int)Math.Min(maxChunk, segment.Length - offset);
                    var moreChunksFollow = sent + chunkLength < total;

                    if (gcs.NeedsInitialization)
                        gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isVectorSets: false);

                    if (gcs.TryWriteChunkedRecordSpan(new ReadOnlySpan<byte>(segment, offset, chunkLength), moreChunksFollow, out var task))
                    {
                        offset += chunkLength;
                        sent += chunkLength;
                        continue;
                    }

                    // Client buffer is full: flush and retry the same chunk.
                    if (!await HandleMigrateTaskResponseAsync(task).ConfigureAwait(false))
                        return false;
                    gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isVectorSets: false);
                }
            }

            return true;
        }

        /// <summary>
        /// Write a serialized record directly to the client buffer; if there is not enough room, flush the buffer and retry writing.
        /// </summary>
        /// <param name="gcs">The client session</param>
        /// <param name="type"></param>
        /// <param name="span"></param>
        /// <returns>True on success, else false</returns>
        private ValueTask<bool> WriteOrSendRecordSpanAsync(GarnetClientSession gcs, MigrationRecordSpanType type, ReadOnlySpan<byte> span)
        {
            // Check if we need to initialize cluster migrate command arguments
            if (gcs.NeedsInitialization)
                gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isVectorSets: false);

            // Try to write serialized record to client buffer
            if (!gcs.TryWriteRecordSpan(span, type, out var task))
            {
                // Flush records in the buffer and retry
                var handleTask = HandleMigrateTaskResponseAsync(task);
                return new(RetryAsync(gcs, handleTask, span.ToArray()));
            }

            return new(true);

            async Task<bool> RetryAsync(GarnetClientSession gcs, Task<bool> task, byte[] span)
            {
                if (!await task.ConfigureAwait(false))
                {
                    return false;
                }

                gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isVectorSets: false);

                if (!gcs.TryWriteRecordSpan(span, type, out _))
                {
                    logger?.LogWarning($"TryWriteRecordSpan failed on retry");
                    return false;
                }

                return true;
            }
        }

        /// <summary>
        /// Handle response from migrate data task
        /// </summary>
        /// <param name="task"></param>
        /// <returns>True on successful completion of data send, otherwise false</returns>
        public async Task<bool> HandleMigrateTaskResponseAsync(Task<string> task)
        {
            if (task != null)
            {
                try
                {
                    var resp = await task.WaitAsync(_timeout, _cts.Token).ConfigureAwait(false);

                    if (!resp.Equals("OK", StringComparison.Ordinal))
                    {
                        logger?.LogError("ClusterMigrate Keys failed with error:{error}.", resp);
                        Status = MigrateState.FAIL;
                        return false;
                    }
                    return true;
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "An error has occurred");
                    Status = MigrateState.FAIL;
                    return false;
                }
            }

            return true;
        }
    }
}