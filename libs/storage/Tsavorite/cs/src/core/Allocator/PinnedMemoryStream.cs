// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// This is a simple stream over a pinned memory buffer, such as a SectorAlignedMemory or network buffer.
    /// </summary>
    internal class PinnedMemoryStream<TStreamBuffer> : Stream
        where TStreamBuffer : IStreamBuffer
    {
        TStreamBuffer streamBuffer;

        public PinnedMemoryStream(TStreamBuffer streamBuffer)
        {
            this.streamBuffer = streamBuffer;
        }

        /// <summary>Whether the stream is opened for Read</summary>
        public override bool CanRead => !streamBuffer.IsForWrite;

        /// <summary>This stream implementation cannot Seek</summary>
        public override bool CanSeek => false;

        /// <summary>Whether the stream is opened for Write</summary>
        public override bool CanWrite => streamBuffer.IsForWrite;

        /// <inheritdoc/>
        protected override void Dispose(bool disposing)
        {
            streamBuffer.Dispose();
            base.Dispose(disposing);
        }

        /// <summary>Flush the internal buffer</summary>
        public override void Flush() => streamBuffer.FlushAndReset();

        /// <summary>Flush the internal buffer asynchronously</summary>
        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
                return Task.FromCanceled(cancellationToken);

            try
            {
                streamBuffer.FlushAndReset(cancellationToken);
                return Task.CompletedTask;
            }
            catch (Exception ex)
            {
                return Task.FromException(ex);
            }
        }

        /// <summary>The amount of data in the internal streamBuffer, either from Read() or Write()</summary>
        public override long Length => streamBuffer.Length;

        /// <summary>The current position of the stream seeking; only Reading is supported</summary>
        public override long Position
        {
            get => streamBuffer.Position;   // TODO: needs to include priorCumulativeLength
            set => throw new InvalidOperationException("Stream does not support set_Position.");
        }

        /// <summary>Copy data from the internal streamBuffer into the buffer; the streamBuffer handles Flush, Reset, and Read more 
        /// (e.g. from disk or network) as needed.</summary>
        /// <param name="buffer">Buffer to copy the bytes into.</param>
        /// <param name="offset">Index in the buffer to start copying to.</param>
        /// <param name="count">Desired number of bytes to copy to the buffer.</param>
        /// <returns>Number of bytes actually read.</returns>
        public override int Read(byte[] buffer, int offset, int count)
        {
            ValidateBufferArguments(buffer, offset, count);
            return streamBuffer.Read(new Span<byte>(buffer, offset, count));
        }

        /// <summary>Copy data from the internal streamBuffer into the destination span; the streamBuffer handles Flush, Reset, and Read more 
        /// (e.g. from disk or network) as needed.</summary>
        public override int Read(Span<byte> destinationSpan) => streamBuffer.Read(destinationSpan);

        /// <summary>Asynchronously copy data from the internal streamBuffer into the memory buffer; the streamBuffer handles Flush, Reset, and Read more 
        /// (e.g. from disk or network) as needed.</summary>
        /// <param name="buffer">Buffer to read the bytes to.</param>
        /// <param name="cancellationToken">Token that can be used to cancel this operation.</param>
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
                return ValueTask.FromCanceled<int>(cancellationToken);

            try
            {
                return new ValueTask<int>(Read(buffer.Span));
            }
            catch (Exception ex)
            {
                return ValueTask.FromException<int>(ex);
            }
        }

        /// <summary>Returns the byte at the current streamBuffer position and advances the position</summary>
        /// <returns>The byte read (as an int)</returns>
        public override unsafe int ReadByte()
        {
            Span<byte> span = stackalloc byte[1];
            return streamBuffer.Read(span) > 0 ? span[0] : -1;
        }

        /// <summary>Seeking is not supported in this stream.</summary>
        public override long Seek(long offset, SeekOrigin loc) => throw new InvalidOperationException("Stream does not support Seek.");

        /// <summary>Seeking is not supported in this stream.</summary>
        public override void SetLength(long value) => throw new InvalidOperationException("Stream does not support SetLength.");

        /// <summary>Write the buffer to the stream; the streamBuffer handles Flush, Reset, and Writing iteratively 
        /// (e.g. to disk or network) as needed.</summary>
        /// <param name="buffer">Buffer to write the bytes from.</param>
        /// <param name="offset">Index in the buffer to start writing from.</param>
        /// <param name="count">Desired number of bytes to write from the buffer.</param>
        public override void Write(byte[] buffer, int offset, int count)
        {
            ValidateBufferArguments(buffer, offset, count);
            streamBuffer.Write(new ReadOnlySpan<byte>(buffer, offset, count));
        }

        /// <summary>Write the buffer to the stream; the streamBuffer handles Flush, Reset, and Writing iteratively 
        /// (e.g. to disk or network) as needed.</summary>
        public override void Write(ReadOnlySpan<byte> destinationSpan) => streamBuffer.Write(destinationSpan);

        /// <summary>Asynchronously write the buffer to the stream; the streamBuffer handles Flush, Reset, and Writing iteratively 
        /// (e.g. to disk or network) as needed.</summary>
        /// <param name="buffer">Buffer to write the bytes from.</param>
        /// <param name="offset">Index in the buffer to start writing from.</param>
        /// <param name="count">Desired number of bytes to write from the buffer.</param>
        /// <returns>Task that can be awaited </returns>
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken = default)
        {
            ValidateBufferArguments(buffer, offset, count);
            return WriteAsync(new ReadOnlySpan<byte>(buffer, offset, count), cancellationToken).AsTask();
        }

        /// <summary>Asynchronously write the buffer to the stream; the streamBuffer handles Flush, Reset, and Writing iteratively 
        /// (e.g. to disk or network) as needed.</summary>
        /// <param name="memoryBuffer">Buffer to write the bytes from.</param>
        /// <param name="cancellationToken">Token that can be used to cancel the operation.</param>
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> memoryBuffer, CancellationToken cancellationToken = default)
            => WriteAsync(memoryBuffer.Span, cancellationToken);

        private ValueTask WriteAsync(ReadOnlySpan<byte> destinationSpan, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
                return ValueTask.FromCanceled(cancellationToken);

            try
            {
                streamBuffer.Write(destinationSpan, cancellationToken);
                return ValueTask.CompletedTask;
            }
            catch (Exception ex)
            {
                return ValueTask.FromException(ex);
            }
        }

        /// <summary>Writes a byte at the next streamBuffer position and advances the position</summary>
        public override unsafe void WriteByte(byte value)
            => streamBuffer.Write(new ReadOnlySpan<byte>(&value, 1));

        /// <summary>Clears the value length offset, which is used to "back up" and update the length of the value due to chunking.</summary>
        public void ClearValueLengthOffset() => streamBuffer.ClearSerializedValueLengthOffset();

        /// <summary>Sets the value length offset, which is used to "back up" and update the length of the value due to chunking.</summary>
        public void SetValueLengthOffset(int offset) => streamBuffer.SetSerializedValueLengthOffset(offset);
    }
}
