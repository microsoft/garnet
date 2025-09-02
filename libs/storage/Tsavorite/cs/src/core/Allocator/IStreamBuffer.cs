// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// This interface abstracts the process of writing a full buffer to storage or network, or reading (up to) a certain number of bytes.
    /// </summary>
    public interface IStreamBuffer : IDisposable
    {
        /// <summary>Indicates that the value is continued in the next chunk, after the current length.</summary>
        internal const int ValueChunkContinuationBit = 1 << 31;

        /// <summary>Indicates that the value is completed in the current chunk (there is no next chunk).</summary>
        internal const int NoValueChunkContinuationBit = 0;

        /// <summary>
        /// The size after which a key (should be rare) or value is handled as an out-of-line allocation when reading from disk.
        /// Must be less than DiskReadBufferSize / 2 and a sector multiple, so it is separate from (and less than) <see cref="LogSettings.kMaxInlineKeySize"/>.
        /// </summary>
        internal const int DiskReadForceOverflowSize = 1 * 1024 * 1024;

        /// <summary>The size of the buffer used for writing data to and reading it from the disk. Must be a sector multiple.</summary>
        internal const int PageBufferSize = 1 << LogSettings.kMinSegmentSizeBits;

        /// <summary>Initial IO size to read.</summary>
        internal static int InitialIOSize => Environment.SystemPageSize;

        /// <summary>
        /// We use these buffers for only read or only write operations, never both at the same time.
        /// </summary>
        bool IsForWrite { get; }

        /// <summary>
        /// Write a full buffer to storage or network and reset the buffer to the starting position.
        /// </summary>
        void FlushAndReset(CancellationToken cancellationToken = default);

        /// <summary>
        /// Write a <see cref="LogRecord"/> to the storage or network buffer. Actual flushing (e.g. to disk) is done as needed.
        /// </summary>
        /// <remarks>This is the serialization driver for the passed <see cref="LogRecord"/>; if the value is an object, then the 
        /// implementation calls valueObjectSerializer to serialize, which in turn calls <see cref="Write(ReadOnlySpan{byte}, CancellationToken)"/>.</remarks>
        void Write(in LogRecord logRecord, long diskTailOffset);

        /// <summary>
        /// Write span of bytes to the storage or network buffer. Actual flushing (e.g. to disk) is done as needed..
        /// </summary>
        /// <remarks>This implements the standard Stream functionality, called from the Value Serializer</remarks>
        void Write(ReadOnlySpan<byte> data, CancellationToken cancellationToken = default);

        /// <summary>
        /// Read more bytes from the disk or network, up to <paramref name="destinationSpan.Length"/>, and store in the buffer. It may not read all bytes
        /// depending on the internal buffer management.
        /// </summary>
        /// <remarks>This implements the standard Stream functionality, called from the Value Serializer</remarks>
        /// <returns>The number of bytes read into <paramref name="destinationSpan"/>, which may be less than <paramref name="destinationSpan.Length"/>.</returns>
        int Read(Span<byte> destinationSpan, CancellationToken cancellationToken = default);
    }
}
