// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.IO;

namespace Garnet.common
{
    /// <summary>
    /// A forward-only, read-only <see cref="Stream"/> over a <see cref="ReadOnlySequence{T}"/> of bytes. Reads copy only into
    /// the caller's (typically small) buffers, so a multi-segment sequence can be consumed (e.g. by a <see cref="BinaryReader"/>)
    /// without first flattening it into a single contiguous allocation.
    /// </summary>
    public sealed class ReadOnlySequenceStream : Stream
    {
        readonly long length;
        ReadOnlySequence<byte> remaining;

        /// <summary>Create a read-only stream over <paramref name="sequence"/>.</summary>
        public ReadOnlySequenceStream(in ReadOnlySequence<byte> sequence)
        {
            length = sequence.Length;
            remaining = sequence;
        }

        /// <inheritdoc/>
        public override int Read(Span<byte> buffer)
        {
            var toRead = (int)Math.Min(buffer.Length, remaining.Length);
            if (toRead == 0)
                return 0;
            remaining.Slice(0, toRead).CopyTo(buffer.Slice(0, toRead));
            remaining = remaining.Slice(toRead);
            return toRead;
        }

        /// <inheritdoc/>
        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

        /// <inheritdoc/>
        public override int ReadByte()
        {
            if (remaining.Length == 0)
                return -1;
            var value = remaining.FirstSpan[0];
            remaining = remaining.Slice(1);
            return value;
        }

        /// <inheritdoc/>
        public override bool CanRead => true;
        /// <inheritdoc/>
        public override bool CanSeek => false;
        /// <inheritdoc/>
        public override bool CanWrite => false;
        /// <inheritdoc/>
        public override long Length => length;
        /// <inheritdoc/>
        public override long Position { get => length - remaining.Length; set => throw new NotSupportedException(); }
        /// <inheritdoc/>
        public override void Flush() { }
        /// <inheritdoc/>
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        /// <inheritdoc/>
        public override void SetLength(long value) => throw new NotSupportedException();
        /// <inheritdoc/>
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}