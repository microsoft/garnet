// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;

    /// <summary>
    /// Maintains a ring of sector-aligned buffers for sequential object-log reads. The reader consumes bytes from
    /// <see cref="currentIndex"/>, while this class submits <c>IDevice.ReadAsync</c> requests into free buffers ahead of it.
    /// </summary>
    /// <remarks>
    /// <para>Three boundaries control the read:</para>
    /// <list type="bullet">
    /// <item><see cref="baseRequiredEndAddress"/> is the fixed endpoint of the caller's initial record range.</item>
    /// <item><see cref="dynamicRequiredEndAddress"/> is replaced as framing discovers the current component or chunk. It may grow when a
    /// continuation requires another discovery window, or shrink when a parsed header supplies an earlier exact endpoint.</item>
    /// <item><see cref="hardReadEndAddress"/> is the maximum logical address framing may require when the caller knows the durable tail.</item>
    /// </list>
    /// <para><see cref="RequiredEndAddress"/> is the logical demand that must be covered. <see cref="nextFileReadPosition"/> is different:
    /// it is the sector-aligned high-water immediately after all sequential device reads submitted since the ring was initialized or last
    /// reset after a direct read. Submitted reads may still be in flight, and sector rounding may carry this high-water beyond the logical
    /// demand. Tightening dynamic demand never cancels those requests; their bytes remain valid read-ahead for a following component or
    /// record.</para>
    /// </remarks>
    public class CircularDiskReadBuffer : IDisposable
    {
        internal readonly SectorAlignedBufferPool bufferPool;
        internal readonly int bufferSize;
        internal readonly IDevice objectLogDevice;
        internal readonly ILogger logger;

        DiskReadBuffer[] buffers;
        int currentIndex;

        bool disposed;

        /// <summary>Sector-aligned address at which the next sequential <c>IDevice.ReadAsync</c> request will start. It is advanced when a
        /// request is submitted, not when that request completes or its bytes are consumed, so its current address is also the exclusive
        /// submitted-IO high-water since the ring was initialized or last reset by <see cref="RepositionAfterDirectRead"/>.</summary>
        internal ObjectLogFilePositionInfo nextFileReadPosition;

        /// <summary>Fixed exclusive endpoint of the caller's initial one-or-more-record range, clamped to
        /// <see cref="hardReadEndAddress"/>. This preserves caller-requested read-ahead while framing changes dynamic demand.</summary>
        ulong baseRequiredEndAddress;

        /// <summary>Exclusive endpoint most recently required by component framing. This may grow for a continuation/discovery window and
        /// shrink when a header reveals an exact earlier endpoint. Replacing this value is idempotent; it does not cancel submitted IO.</summary>
        ulong dynamicRequiredEndAddress;

        /// <summary>Exclusive logical tail supplied by <see cref="OnBeginReadRecords"/>. It is the main object-log durable tail when set, or
        /// <see cref="ulong.MaxValue"/> when the supplied position has no data because the main tail is not initialized yet or no tail is
        /// available in this device's address space. Speculative discovery windows clamp to this boundary; an authoritative endpoint parsed
        /// from framing may not cross it. Sector-rounded device requests may extend past it physically.</summary>
        ulong hardReadEndAddress;

        /// <summary>Offset at which consumption must begin if the current buffer is filled later. A direct overflow read resets the ring at
        /// the sector containing the payload end. If no following bytes are currently required, no device read is submitted, so this retains
        /// the payload-end offset within that sector. If later framing grows <see cref="RequiredEndAddress"/>, the first deferred read uses
        /// this offset to hide the preceding bytes in the sector from the consumer. Zero after that read is submitted.</summary>
        int deferredBufferStartPosition;

        /// <summary>Exclusive logical endpoint the ring must currently cover. The fixed initial range remains required even when current
        /// framing tightens its dynamic endpoint; framing may extend demand beyond the initial range.</summary>
        ulong RequiredEndAddress => Math.Max(baseRequiredEndAddress, dynamicRequiredEndAddress);

        internal uint SectorSize => objectLogDevice.SectorSize;

        /// <summary>Whether this ring reads from <paramref name="device"/>. The allocator uses this to apply its durable tail only to
        /// the main object-log address space; a snapshot device requires its own bound.</summary>
        internal bool UsesDevice(IDevice device) => ReferenceEquals(objectLogDevice, device);

        internal CircularDiskReadBuffer(SectorAlignedBufferPool bufferPool, int bufferSize, int numBuffers, IDevice objectLogDevice, ILogger logger)
        {
            this.bufferPool = bufferPool;
            this.bufferSize = bufferSize;
            this.objectLogDevice = objectLogDevice;
            this.logger = logger;

            buffers = new DiskReadBuffer[numBuffers];
            currentIndex = 0;
        }

        internal DiskReadBuffer GetCurrentBuffer()
        {
            if (disposed)
                throw new ObjectDisposedException(nameof(CircularDiskReadBuffer));
            return buffers[currentIndex];
        }

        int GetNextBufferIndex(int curIndex)
        {
            var index = curIndex + 1;
            return index >= buffers.Length ? 0 : index;
        }

        private DiskReadBuffer CreateBuffer(int bufferIndex)
        {
            DiskReadBuffer buffer = new(bufferPool.Get(bufferSize), objectLogDevice, logger);
            buffers[bufferIndex] = buffer;
            return buffer;
        }

        /// <summary>
        /// Prepare a free <see cref="DiskReadBuffer"/> and submit an asynchronous device read covering as much of the current logical demand
        /// as fits before the buffer or segment boundary. This is called for initial fill-ahead, dynamic demand growth, and ring backfill.
        /// </summary>
        /// <param name="bufferIndex">The index into <see cref="buffers"/> of the <see cref="DiskReadBuffer"/> that will do the reading</param>
        /// <param name="unalignedReadStartPosition">Offset within the first sector at which logical consumption begins. The device request
        /// starts at the sector boundary; bytes before this offset are alignment overlap and are not exposed to the consumer.</param>
        private void DoReadBuffer(int bufferIndex, int unalignedReadStartPosition)
        {
            Debug.Assert((uint)unalignedReadStartPosition < objectLogDevice.SectorSize,
                $"Logical read start {unalignedReadStartPosition} must be within the request's first sector");
            var buffer = buffers[bufferIndex];
            if (buffer is null)
                buffer = CreateBuffer(bufferIndex);
            else
            {
                Debug.Assert(buffer.countdownEvent.CurrentCount == 0, $"Unexpected countdownEvent.CurrentCount ({buffer.countdownEvent.CurrentCount}) when preparing to read into buffer");
                buffer.Initialize();
            }

            var alignedReadStartPosition = RoundDown(unalignedReadStartPosition, (int)objectLogDevice.SectorSize);
            var bufferStartPosition = unalignedReadStartPosition - alignedReadStartPosition;

            // Submit only enough logical bytes to reach current demand, subject to the buffer and segment boundaries. The final request is
            // rounded to a full sector below, so nextFileReadPosition may advance beyond RequiredEndAddress.
            var unalignedReadLength = bufferSize - alignedReadStartPosition;
            var submittedReadEndAddress = nextFileReadPosition.CurrentAddress;
            var requiredEnd = RequiredEndAddress;
            Debug.Assert(requiredEnd > submittedReadEndAddress, $"required endpoint {requiredEnd} must exceed submitted-read endpoint {submittedReadEndAddress}");
            var logicalRemaining = requiredEnd - submittedReadEndAddress;
            if ((ulong)unalignedReadLength > logicalRemaining)
                unalignedReadLength = (int)logicalRemaining;

            Debug.Assert(IsAligned(nextFileReadPosition.Offset, (int)objectLogDevice.SectorSize), $"filePosition.Offset ({nextFileReadPosition.Offset}) is not sector-aligned");
            var segmentIsComplete = false;
            if (nextFileReadPosition.Offset + (ulong)unalignedReadLength >= nextFileReadPosition.SegmentSize)
            {
                unalignedReadLength = (int)(nextFileReadPosition.SegmentSize - nextFileReadPosition.Offset);
                Debug.Assert(IsAligned(unalignedReadLength, (int)objectLogDevice.SectorSize), $"unalignedReadLength ({unalignedReadLength}) is not sector-aligned at segment end");
                segmentIsComplete = true;
            }

            // We may not have had a sector-aligned amount of remaining unread data.
            var alignedReadLength = RoundUp(unalignedReadLength, (int)objectLogDevice.SectorSize);

            buffer.ReadFromDevice(nextFileReadPosition, bufferStartPosition, (uint)alignedReadLength, ReadFromDeviceCallback);

            // Advance at submission time. This uses the sector-rounded request length and may pass the logical endpoint; consumption is
            // governed by DiskReadBuffer.currentPosition/endPosition rather than this submitted-IO high-water.
            // Note: If segmentIsComplete, this increment results in nextFileReadPosition.Offset == SegmentSize, which will mask off to a 0.
            nextFileReadPosition.Offset += (uint)alignedReadLength;

            Debug.Assert(nextFileReadPosition.Offset <= nextFileReadPosition.SegmentSize, $"filePosition.Offset ({nextFileReadPosition.Offset}) must be <= filePosition.SegmentSize ({nextFileReadPosition.SegmentSize})");
            if (segmentIsComplete)
                nextFileReadPosition.AdvanceToNextSegment();

        }

        /// <summary>
        /// Called when one or more records are to be read via ReadAsync.
        /// </summary>
        /// <param name="startFilePosition">The initial file position to read</param>
        /// <param name="totalLength">Initial read extent for the one-or-more-record span. It may be based on size hints; framing can later
        /// extend or tighten demand through <see cref="SetDynamicReadThrough"/>.</param>
        /// <param name="hardReadEndPosition">Exclusive durable tail in the same object-log address space as
        /// <paramref name="startFilePosition"/>. Supply a position whose word is <see cref="ObjectLogFilePositionInfo.NotSet"/> and whose
        /// segment-size bits match the reader when that address space has no known tail. An unset main tail has the same unbounded effect.
        /// Neither case substitutes a tail from another object-log device.</param>
        internal void OnBeginReadRecords(ObjectLogFilePositionInfo startFilePosition, ulong totalLength, ObjectLogFilePositionInfo hardReadEndPosition)
        {
            if (disposed)
                throw new ObjectDisposedException(nameof(CircularDiskReadBuffer));

            Debug.Assert(totalLength > 0, "TotalLength cannot be 0");
            if (startFilePosition.SegmentSizeBits != hardReadEndPosition.SegmentSizeBits)
                throw new TsavoriteException("Object-log read start and durable tail use different segment sizes");
            hardReadEndAddress = hardReadEndPosition.HasData ? hardReadEndPosition.CurrentAddress : ulong.MaxValue;
            if (startFilePosition.CurrentAddress >= hardReadEndAddress)
                throw new TsavoriteException($"Object-log read starts at {startFilePosition.CurrentAddress}, outside durable tail {hardReadEndPosition.CurrentAddress}");
            nextFileReadPosition = startFilePosition;
            var requestedEnd = startFilePosition.CurrentAddress + totalLength;
            if (requestedEnd < startFilePosition.CurrentAddress)
                requestedEnd = ulong.MaxValue;
            baseRequiredEndAddress = Math.Min(requestedEnd, hardReadEndAddress);
            dynamicRequiredEndAddress = 0;
            deferredBufferStartPosition = 0;

            // Initialize all buffers
            for (var ii = 0; ii < buffers.Length; ii++)
                buffers[ii]?.Initialize();
            currentIndex = 0;

            // Fill ahead toward the initial demand. The first device request starts at the preceding sector boundary; recordStartPosition
            // prevents the alignment-overlap bytes from being exposed to the consumer.
            var alignedReadPosition = RoundDown(nextFileReadPosition.Offset, (int)objectLogDevice.SectorSize);
            var recordStartPosition = (int)(nextFileReadPosition.Offset - alignedReadPosition);
            nextFileReadPosition.Offset -= (uint)recordStartPosition;

            // Load all the buffers as long as we have more unread data. Leave currentIndex at 0.
            for (var ii = 0; ii < buffers.Length; ii++)
            {
                if (nextFileReadPosition.CurrentAddress >= RequiredEndAddress)
                    break;
                DoReadBuffer(ii, recordStartPosition);
                recordStartPosition = 0;  // After the first read, subsequent reads start on an aligned address
            }
        }

        /// <summary>
        /// Replace the absolute exclusive endpoint required by the component currently being framed. The endpoint may increase when a
        /// continuation opens another discovery window, or decrease when framing reveals the exact end of a shorter component/chunk.
        /// <see cref="RequiredEndAddress"/> still preserves the caller's base range. Newly required range not already covered by submitted
        /// ring reads is submitted here as far as free ring slots allow; <see cref="MoveToNextBuffer"/> submits the remainder as exhausted
        /// slots are recycled. Previously submitted sector-rounded IO is retained.
        /// </summary>
        /// <param name="exclusiveEnd">New component/chunk demand endpoint in the reader's object-log address space.</param>
        /// <param name="isDiscoveryWindow">Whether this is a speculative discovery endpoint. Discovery may clamp to the known hard tail;
        /// an authoritative framing endpoint beyond that tail indicates truncation or corruption and throws.</param>
        internal void SetDynamicReadThrough(ObjectLogFilePositionInfo exclusiveEnd, bool isDiscoveryWindow = false)
        {
            Debug.Assert(exclusiveEnd.SegmentSizeBits == nextFileReadPosition.SegmentSizeBits, "Endpoint and reader must use the same segment size");
            if (exclusiveEnd.CurrentAddress > hardReadEndAddress)
            {
                if (!isDiscoveryWindow)
                    throw new TsavoriteException($"Object-log framing requires endpoint {exclusiveEnd.CurrentAddress} beyond durable tail {hardReadEndAddress}");
                dynamicRequiredEndAddress = hardReadEndAddress;
            }
            else
                dynamicRequiredEndAddress = exclusiveEnd.CurrentAddress;

            var current = buffers[currentIndex];
            if (nextFileReadPosition.CurrentAddress < RequiredEndAddress && (current is null || !current.readIssued))
            {
                DoReadBuffer(currentIndex, deferredBufferStartPosition);
                deferredBufferStartPosition = 0;
            }

            // Fill empty buffers ahead of the current one toward the new absolute endpoint. A submitted read may be in flight or complete;
            // either way, its sector-rounded coverage is already included in nextFileReadPosition.
            for (var idx = GetNextBufferIndex(currentIndex); idx != currentIndex; idx = GetNextBufferIndex(idx))
            {
                if (nextFileReadPosition.CurrentAddress >= RequiredEndAddress)
                    break;

                var buf = buffers[idx];
                if (buf is not null && buf.readIssued)
                    continue;
                DoReadBuffer(idx, unalignedReadStartPosition: 0);
            }
        }

        /// <summary>
        /// Called when one or more records with Objects have been read and via ReadAsync, e.g. being processed by AsyncReadPageWithObjectsCallback,
        /// and we have completed reading and deserializing those objects.
        /// </summary>
        internal void OnEndReadRecords()
        {
            for (var ii = 0; ii < buffers.Length; ii++)
                buffers[ii]?.WaitForReadCompletion();
        }

        /// <summary>Advance the ring's consumption cursor to a record's authoritative object-log start. A partial flush may have
        /// sector-padded the preceding record range, so this skips that padding (less than one sector) rather than assuming records are
        /// contiguous. The target may be at the start of the next ring buffer.</summary>
        /// <param name="recordFilePosition">Object-log position stored in the record being read.</param>
        /// <returns><c>false</c> only when the current ring buffer has no data at entry; otherwise <c>true</c> after attempting to advance.
        /// A subsequent stream read reports exhaustion if the target lay exactly beyond the available buffers.</returns>
        internal bool OnBeginRecord(ObjectLogFilePositionInfo recordFilePosition)
        {
            var buffer = buffers[currentIndex] ?? throw new TsavoriteException(
                $"Internal error in read buffer sequencing; empty buffer[{currentIndex}] encountered with required endpoint {RequiredEndAddress}");

            // Each partial flush sector-pads its last write. The next stored record position may therefore be ahead of the current
            // consumption cursor by that padding, but it may never be behind it.
            if (!buffer.HasData && !buffer.WaitForDataAvailable())
                return false;

            while (true)
            {
                var bufferFilePosition = buffer.GetCurrentFilePosition();
                Debug.Assert(recordFilePosition.word >= bufferFilePosition.word, $"Record file position ({recordFilePosition}) should be >= ongoing position {bufferFilePosition}");
                Debug.Assert(recordFilePosition.SegmentId == bufferFilePosition.SegmentId, $"Record file segment ({recordFilePosition.SegmentId}) should == ongoing position {bufferFilePosition.SegmentId}");
                var increment = recordFilePosition - bufferFilePosition;
                Debug.Assert(increment < objectLogDevice.SectorSize, $"Increment {increment} must be less than SectorSize ({objectLogDevice.SectorSize})");

                // We might cleanly align to the start of the next buffer, if there was a flush that ended on a buffer boundary.
                // Otherwise, we should always be within the current buffer. We should only do this "continue" once.
                if (buffer.currentPosition + (int)increment < buffer.endPosition)
                {
                    buffer.currentPosition += (int)increment;
                    break;
                }

                Debug.Assert(buffer.currentPosition + (int)increment == buffer.endPosition, $"Increment {increment} overflows buffer (curPos {buffer.currentPosition}, endPos {buffer.endPosition}) by more than alignment");
                if (!MoveToNextBuffer(out buffer))
                    break;
            }
            return true;
        }

        /// <summary>
        /// Begin the deserialization process for a single record.
        /// </summary>
        internal void OnBeginDeserialize()
        {
            // Currently nothing
        }

        /// <summary>Recycle the exhausted current slot for future sequential IO when more logical bytes are required, then advance the
        /// consumption cursor to the next ring slot and wait for its previously submitted read if necessary.</summary>
        /// <param name="nextBuffer">The next ring buffer, which may be null or empty when all required bytes have been consumed.</param>
        /// <returns>Whether <paramref name="nextBuffer"/> contains readable data.</returns>
        internal bool MoveToNextBuffer(out DiskReadBuffer nextBuffer)
        {
            // If logical demand extends beyond the submitted-IO high-water, reuse this exhausted slot to submit the next sequential read
            // before advancing the consumer to the following ring slot.
            if (nextFileReadPosition.CurrentAddress < RequiredEndAddress)
                DoReadBuffer(currentIndex, unalignedReadStartPosition: 0);
            else
                buffers[currentIndex].Initialize();

            // Move to the next buffer and wait for any in-flight read to complete. If there is no pending IO and the buffer is
            // empty, we are done with this read op.
            currentIndex = GetNextBufferIndex(currentIndex);
            nextBuffer = buffers[currentIndex];
            if (nextBuffer is not null && nextBuffer.WaitForDataAvailable())
                return true;

            Debug.Assert(nextFileReadPosition.CurrentAddress >= RequiredEndAddress,
                $"submitted-read endpoint {nextFileReadPosition.CurrentAddress} did not reach required endpoint {RequiredEndAddress}");
            return false;
        }

        /// <summary>Reset the ring's sequential-read and consumption positions to the logical end of an overflow payload that was read
        /// directly into its final array, bypassing the ring. In-flight read-ahead is drained before buffers are reused. Existing base and
        /// dynamic demand remain in force; buffers are immediately refilled only if they require bytes after
        /// <paramref name="logicalPosition"/>.</summary>
        /// <remarks>If no bytes are currently required after <paramref name="logicalPosition"/>, the method records its offset within the
        /// containing sector in <see cref="deferredBufferStartPosition"/> without submitting a read. A later dynamic-demand increase uses
        /// that offset for the first refill, so consumption resumes exactly at the payload end rather than at the sector boundary.</remarks>
        /// <param name="logicalPosition">Exclusive logical payload endpoint at which subsequent ring consumption must resume.</param>
        internal void RepositionAfterDirectRead(ObjectLogFilePositionInfo logicalPosition)
        {
            for (var ii = 0; ii < buffers.Length; ii++)
            {
                buffers[ii]?.WaitForReadCompletion();
                buffers[ii]?.Initialize();
            }

            currentIndex = 0;
            nextFileReadPosition = logicalPosition;
            var alignedOffset = RoundDown(nextFileReadPosition.Offset, (int)objectLogDevice.SectorSize);
            var startPosition = (int)(nextFileReadPosition.Offset - alignedOffset);
            nextFileReadPosition.Offset = alignedOffset;
            deferredBufferStartPosition = startPosition;

            // Do not reread the trailing sector when the logical position itself is already at the required endpoint.
            for (var ii = 0; logicalPosition.CurrentAddress < RequiredEndAddress
                && ii < buffers.Length && nextFileReadPosition.CurrentAddress < RequiredEndAddress; ii++)
            {
                DoReadBuffer(ii, startPosition);
                startPosition = 0;
                deferredBufferStartPosition = 0;
            }

        }

        /// <summary>Synchronously complete a sector-aligned range of asynchronous device requests directly into pinned memory, splitting
        /// the range at object-log segment boundaries. This bypasses the ring and does not change its submitted-IO or consumption state;
        /// the caller must subsequently use <see cref="RepositionAfterDirectRead"/>.</summary>
        /// <param name="source">Sector-aligned object-log position at which the direct read begins.</param>
        /// <param name="destination">Sector-aligned pointer into a pinned destination allocation.</param>
        /// <param name="length">Sector-aligned byte count.</param>
        internal void ReadDirect(ObjectLogFilePositionInfo source, IntPtr destination, long length)
        {
            Debug.Assert(source.Offset % objectLogDevice.SectorSize == 0, "Direct-read source must be sector aligned");
            Debug.Assert(destination.ToInt64() % objectLogDevice.SectorSize == 0, "Direct-read destination must be sector aligned");
            using var completion = new DirectReadCompletion();

            while (length > 0)
            {
                var segmentRemaining = source.SegmentSize - source.Offset;
                var readLength = (uint)Math.Min(Math.Min(length, (long)segmentRemaining), int.MaxValue);
                completion.Prepare(readLength);
                objectLogDevice.ReadAsync(source.SegmentId, source.Offset, destination, readLength, DirectReadCallback, completion);
                completion.Wait();

                source.Advance(readLength);
                destination += (int)readLength;
                length -= readLength;
            }
        }

        static void DirectReadCallback(uint errorCode, uint numBytes, object context)
            => ((DirectReadCompletion)context).Complete(errorCode, numBytes);

        sealed class DirectReadCompletion : IDisposable
        {
            readonly ManualResetEventSlim completed = new(false);
            uint expectedBytes;
            uint errorCode;
            uint numBytes;

            internal void Prepare(uint expectedBytes)
            {
                this.expectedBytes = expectedBytes;
                errorCode = 0;
                numBytes = 0;
                completed.Reset();
            }

            internal void Complete(uint errorCode, uint numBytes)
            {
                this.errorCode = errorCode;
                this.numBytes = numBytes;
                completed.Set();
            }

            internal void Wait()
            {
                completed.Wait();
                if (errorCode != 0 || numBytes != expectedBytes)
                    throw new TsavoriteException($"Direct object-log read failed: error {errorCode}, requested {expectedBytes} bytes, read {numBytes} bytes");
            }

            public void Dispose() => completed.Dispose();
        }

        internal void ReadFromDeviceCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                logger?.LogError($"{nameof(ReadFromDeviceCallback)} error: {{errorCode}}", errorCode);

            // Finish setting up the buffer
            var buffer = (DiskReadBuffer)context;

            buffer.endPosition += (int)numBytes;
            if (buffer.endPosition == 0)
                Debug.Assert(buffer.currentPosition == 0, $"buffer.currentPosition ({buffer.currentPosition}) must be 0 if buffer.endPosition ({buffer.endPosition}) is 0");
            else
                Debug.Assert(buffer.endPosition > buffer.currentPosition, $"buffer.endPosition ({buffer.endPosition}) must be >= buffer.currentPosition ({buffer.currentPosition})");

            // Signal the buffer's event to indicate the data is available.
            _ = buffer.countdownEvent.Signal();
        }

        public void Dispose()
        {
            disposed = true;

            // Atomic swap to avoid clearing twice.
            var localBuffers = Interlocked.Exchange(ref buffers, null);
            if (localBuffers == null)
                return;

            for (var ii = 0; ii < localBuffers.Length; ii++)
                localBuffers[ii]?.Dispose();

            // Restore the now-cleared buffers array.
            //buffers = localBuffers;
        }

        /// <inheritdoc/>
        public override string ToString()
            => $"currIdx {currentIndex}; bufSize {bufferSize}; filePosition {nextFileReadPosition}; SecSize {(int)objectLogDevice.SectorSize}";
    }
}