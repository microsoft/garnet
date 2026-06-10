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

        /// <summary>The size of the buffer used for writing data to and reading it from the disk. Must be a sector multiple.</summary>
        internal const int BufferSize = 1 << LogSettings.kMinObjectLogSegmentSizeBits;

        /// <summary>Default Initial IO size to read; may be overridden by operation-level, session-level, or store-level specification.
        /// Sized to comfortably cover a typical small record (header + small key + small value)
        /// in one device-sector IO. The previous default of one OS system page (4 KB on Linux x64) caused most reads of small
        /// records to span 4 KB NAND-page boundaries on NVMe, doubling per-IO device latency (~0.92 ms vs ~0.67 ms for sector-aligned
        /// 4 KB reads). With a 128-byte speculative read, the sector-aligned IO is typically 1 sector (and up to 2 sectors when the
        /// record begins near the end of a sector), and usually captures a full small record with no re-read.
        /// Records larger than what fits in the speculative read trigger a precise re-read via VerifyRecordFromDiskCallback with the
        /// now-known recordLength, same as before — the cost is one extra IO per multi-sector record, which is a fair trade against
        /// avoiding the NAND-crossing penalty on every small-record IO.</summary>
        public const int DefaultInitialIORecordSize = 128;

        /// <summary>
        /// We use these buffers for only read or only write operations, never both at the same time.
        /// </summary>
        bool IsForWrite { get; }

        /// <summary>
        /// Write a full buffer to storage or network and reset the buffer to the starting position. Note that this may also reset the
        /// underlying buffer pointer.
        /// </summary>
        /// <param name="cancellationToken">Optional cancellation token</param>
        void FlushAndReset(CancellationToken cancellationToken = default);

        /// <summary>
        /// Write span of bytes to the storage or network buffer. Actual flushing (e.g. to disk) is done as needed..
        /// </summary>
        /// <param name="data">The data span to write to the device.</param>
        /// <param name="cancellationToken">Optional cancellation token</param>
        /// <remarks>This implements the standard Stream functionality, called from the Value Serializer</remarks>
        void Write(ReadOnlySpan<byte> data, CancellationToken cancellationToken = default);

        /// <summary>
        /// Read more bytes from the disk or network, up to <paramref name="destinationSpan.Length"/>, and store in the buffer. It may not read all bytes
        /// depending on the internal buffer management.
        /// </summary>
        /// <param name="destinationSpan">The span to receive data from the device</param>
        /// <param name="cancellationToken">Optional cancellation token</param>
        /// <remarks>This implements the standard Stream functionality, called from the Value Serializer</remarks>
        /// <returns>The number of bytes read into <paramref name="destinationSpan"/>, which may be less than <paramref name="destinationSpan.Length"/>.</returns>
        int Read(Span<byte> destinationSpan, CancellationToken cancellationToken = default);
    }
}