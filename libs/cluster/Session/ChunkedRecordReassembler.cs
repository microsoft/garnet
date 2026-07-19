// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.cluster
{
    /// <summary>
    /// Reassembles a <see cref="Garnet.client.MigrationRecordSpanType.ChunkedLogRecord"/> from its chunk records. A record too
    /// large for one send buffer is sent as a sequence of chunks (each framed <c>[int chunkLength | continuation][chunk bytes]</c>);
    /// the receiver appends each chunk's payload here until the final chunk (its continuation flag clear), then deserializes the
    /// reassembled record. One instance is held per connection because a record's chunks may span multiple commands.
    /// </summary>
    internal sealed class ChunkedRecordReassembler
    {
        byte[] buffer = new byte[1024];
        int length;

        /// <summary>
        /// Append one chunk's payload. Returns true when the record is complete (<paramref name="moreChunksFollow"/> is false),
        /// after which <see cref="Record"/> is the reassembled record and the caller must <see cref="Reset"/> before the next record.
        /// </summary>
        public bool Append(ReadOnlySpan<byte> chunk, bool moreChunksFollow)
        {
            if (length + chunk.Length > buffer.Length)
                Array.Resize(ref buffer, Math.Max(length + chunk.Length, buffer.Length * 2));
            chunk.CopyTo(buffer.AsSpan(length));
            length += chunk.Length;
            return !moreChunksFollow;
        }

        /// <summary>The reassembled record bytes (valid once <see cref="Append"/> returns true).</summary>
        public ReadOnlySpan<byte> Record => new(buffer, 0, length);

        /// <summary>Reset for the next record (keeps the buffer for reuse).</summary>
        public void Reset() => length = 0;
    }
}