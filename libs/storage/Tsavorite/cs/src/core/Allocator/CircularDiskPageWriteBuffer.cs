// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;

    /// <summary>
    /// This class drives object-serialization writing to the disk.
    /// </summary>
    /// <remarks>This does not implement <see cref="IDisposable"/>; it calls <see cref="EndFlushComplete"/> itself when the final callback is issued.</remarks>
    public class CircularDiskPageWriteBuffer
    {
        internal readonly SectorAlignedBufferPool bufferPool;
        internal readonly int pageBufferSize;
        internal readonly IDevice device;
        internal readonly ILogger logger;

        readonly DiskPageWriteBuffer[] buffers;
        int currentIndex;

        /// <summary>Cumulative length of data written to the buffers before the current buffer</summary>
        internal long priorCumulativeLength;

        /// <summary>The amount of bytes we wrote for the entire Flush() call, which may include multiple "partial" flushes: pages and/or page fragments.
        ///     Used by the caller (e.g. <see cref="ObjectAllocatorImpl{TStoreFunctions}"/> to track actual file output length.</summary>
        /// <remarks><see cref="DiskPageWriteBuffer.currentPosition"/> is the current length, since it is *past* the last byte copied to the buffer. Subtract the intermediate chain-chunk length markers.</remarks>
        internal long TotalWrittenLength => priorCumulativeLength + buffers[currentIndex].currentPosition;

        /// <summary>Device address to write to; incremented with each buffer flush or out-of-line write by the caller; all of these should be aligned to sector size, so this address remains sector-aligned.</summary>
        internal ulong alignedDeviceAddress;

        /// <summary>If true, this is the first partial flush (the first page or page fragment in a Flush() call). If this is false, we verify buffer positions are consistent.</summary>
        internal bool isFirstPartialFlush = true;

        /// <summary>Countdown event for global count of all buffers and all direct writes. Also triggers the external callback of a partial-flush sequence.</summary>
        /// <remarks>This is passed to all disk-write operations; multiple pending flushes may be in-flight with the callback unset; when the final flush (which may be a buffer-span, a direct write, or the
        /// final sector-aligning partial-flush completion flush), it allows the final pending flush to complete to know it *is* the final one and the callback can be called.</remarks>
        internal CountdownCallbackAndContext countdownCallbackAndContext;

        internal int SectorSize => (int)device.SectorSize;

        /// <summary>The amount of data space on the disk page between the header and end of page. Does not include any lengthSpaceReserve for continuation-chunk lengths; this is
        /// handled by <see cref="DiskStreamWriter.Write(ReadOnlySpan{byte}, System.Threading.CancellationToken)"/> and <see cref="DiskStreamWriter.OnBufferComplete(int)"/>.</summary>
        internal int UsablePageSize => pageBufferSize - DiskPageHeader.Size;

        internal CircularDiskPageWriteBuffer(SectorAlignedBufferPool bufferPool, int pageBufferSize, int numPageBuffers, IDevice device, ILogger logger)
        {
            this.bufferPool = bufferPool;
            this.pageBufferSize = pageBufferSize;
            this.device = device;
            this.logger = logger;

            buffers = new DiskPageWriteBuffer[numPageBuffers];
            currentIndex = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal DiskPageWriteBuffer GetCurrentBuffer() => buffers[currentIndex];

        internal DiskPageWriteBuffer MoveToAndInitializeNextBuffer()
        {
            currentIndex = (currentIndex + 1) & (buffers.Length - 1);
            return GetAndInitializeCurrentBuffer();
        }

        internal DiskPageWriteBuffer GetAndInitializeCurrentBuffer()
        {
            var buffer = GetCurrentBuffer();
            if (buffer is null)
            {
                buffer = new DiskPageWriteBuffer(bufferPool.Get(pageBufferSize), device, logger);
                buffers[currentIndex] = buffer;
            }
            buffer.WaitUntilFreeAndInitialize(SectorSize);
            return buffer;
        }

        /// <summary>Verifies buffer position and returns the "current" buffer for use.</summary>
        /// <param name="bufferPosition">Input; the start offset in the buffer, calculated by the caller</param>
        /// <param name="flushedDiskTailOffset">Output: updated with the number of bytes if we had to skip over the <see cref="DiskPageHeader"/>.</param>
        /// <return>Whether this was the first partial flush for this instance of <see cref="CircularDiskPageWriteBuffer"/></return>
        internal bool OnBeginPartialFlush(int bufferPosition, ref long flushedDiskTailOffset)
        {
            // Partial flushes end with a sector-aligned Flush, and this is also reflected in FlushDiskTailOffset, which has already been added to bufferPosition.
            Debug.Assert(IsAligned(bufferPosition, SectorSize), $"bufferPosition {bufferPosition} is not sector-aligned at the start of the partial flush, implying FlushedDiskTailOffset was not correctly incremented");
            if (bufferPosition == 0)
            {
                bufferPosition = DiskPageHeader.Size;
                _ = MonotonicUpdate(ref flushedDiskTailOffset, flushedDiskTailOffset + DiskPageHeader.Size, out _);
            }

            // Make sure the circular buffer is consistent with the calculated bufferPosition (based on the first address in the partial flush).
            // If this is the first partial flush on the circular buffer, then just set currentPosition to bufferPosition.
            var wasFirstFlush = isFirstPartialFlush;
            if (isFirstPartialFlush)
            {
                var buffer = GetAndInitializeCurrentBuffer();
                buffer.currentPosition = bufferPosition;
                isFirstPartialFlush = false;
            }
            else
            { 
                // It's not the first partial flush, so the current buffer's currentPosition should be the same as bufferPosition, except for the case where the current
                // buffer has just completed a record at the end; in that case we need to jump to the next buffer, and will offset buffer.currentPosition for DiskPageHeader,
                // which will match the flushedDiskTailIncrement we added above.
                var buffer = GetCurrentBuffer();
                if (buffer.RemainingCapacity != 0)
                    Debug.Assert(buffer.currentPosition == bufferPosition, $"Not the first partial flush so buffer.currentPosition {buffer.currentPosition} should equal bufferPosition {bufferPosition}");
                else
                {
                    Debug.Assert(bufferPosition == 0, $"Not the first partial flush and buffer is full, so we are moving to the next buffer and bufferPosition {bufferPosition} should come in as 0");
                    buffer = MoveToAndInitializeNextBuffer();
                    Debug.Assert(buffer.currentPosition == DiskPageHeader.Size, $"Moving to next buffer; buffer.currentPosition {buffer.currentPosition} should be DiskPageHeader.Size {DiskPageHeader.Size}");
                }
            }

            countdownCallbackAndContext = new();
            return wasFirstFlush;
        }

        /// <summary>
        /// Finish all the current partial flush, including flushing any as-yet-unflushed data in the current buffer then calling the caller's callbacks
        /// so flushedUntilAddresses can be updated. When this function exits, there will be IOs in flight.
        /// </summary>
        /// <remarks>This write to the device is sector-aligned, which means the next fragment will probably rewrite the sector, since the currentPosition is probably
        /// somewhere in the middle of the sector.</remarks>
        /// <param name="externalCallback">Callback sent to the initial Flush() command. Called when we are done with this partial flush operation. 
        ///     It usually signals the <see cref="PageAsyncFlushResult{T}.done"/> event so the caller knows the flush is complete and it can continue.</param>
        /// <param name="externalContext">Context sent to <paramref name="externalCallback"/>.</param>
        /// <returns>The number of sector-aligned padding bytes. We don't want to back up and overwrite a partial sector, so we must adjust FlushedDiskTailOffset
        ///     to reflect the new boundary.</returns>
        internal unsafe int OnPartialFlushComplete(DeviceIOCompletionCallback externalCallback, object externalContext)
        {
            // TODO: TotalWrittenLength may exceed uint.MaxValue in which case the callback's numBytes will be incorrect.
            var numBytes = (uint)TotalWrittenLength;
            countdownCallbackAndContext.Increment();

            var buffer = GetCurrentBuffer();
            if (buffer.currentPosition > DiskPageHeader.Size)
            {
                Debug.Assert(RoundUp(buffer.flushedUntilPosition, SectorSize) == buffer.flushedUntilPosition, $"flushedUntilOffset {buffer.flushedUntilPosition} is not sector-aligned");
                Debug.Assert(RoundUp(buffer.currentPosition, Constants.kRecordAlignment) == buffer.currentPosition, $"buffer.currentPosition {buffer.currentPosition} is not record-aligned");
                Debug.Assert(buffer.currentPosition >= buffer.flushedUntilPosition, $"buffer.currentPosition {buffer.currentPosition} must be >= buffer.flushedUntilPosition {buffer.flushedUntilPosition}");

                // See if we have anything to flush to ensure sector-alignment. This is necessary to avoid rewriting sectors, which can be a problem for some devices
                // due to inefficiencies in rewriting or inability to back up (or both).
                var sectorEnd = RoundUp(buffer.currentPosition, SectorSize);
                if (sectorEnd > buffer.currentPosition)
                {
                    var recordEnd = RoundUp(buffer.currentPosition, Constants.kRecordAlignment);
                    if (sectorEnd > recordEnd)
                    {
                        // Force to sector alignment. If this would be more padding than forcing to record alignment would be, then we must handle this specially, so that all
                        // 3 scenarios are satisfied:
                        // 1. Flush: Forces sector alignment in two parts, the current Flush operation *and* OnBeginPartialFlush:
                        //    a. Here, if we have to force sector-alignment, we write into the "normal" next-record position (the location the next record would have started at if it
                        //       was not force-aligned to sector) an invalid RecordInfo that has the IsSectorForceAligned bit set. This is necessary for Scan, which would otherwise not
                        //       be able to determine that we should move to the next sector after the end of the previous record. The we increment FlushedDiskTailOffset to the next sector.
                        //       Note: if currentPosition is DiskPageHeader.Size there is no data in the buffer; the last write was to disk-page end boundary, which is automatically sector-aligned.
                        //    b. In OnBeginPartialFlush, we always set the first LogRecord of the flush to have IsSectorForceAligned bit set. This tells PatchExpandedAddresses to ensure
                        //       ClosedDiskTailOffset is sector-aligned. We have to always set this bit for the first record of the flush because we've already sector-aligned bufferPosition
                        //       and we have no (easy) way to know whether it was forced or not.
                        // 2. PatchExpandedAddresses: If the current LogRecord.Info.IsSectorForceAligned is true, adjusts ClosedDiskTailOffset to start on the next sector boundary,
                        //    to stay in sync with FlushedDiskTailOffset. Note: It would not work to have the IsSectorForcedAlign bit in the previous LogRecord, as that may have been evicted.
                        // 3. Scan: When this does next-record processing, if it lands on an invalid record with LogRecord.Info.IsSectorForceAligned true, it jumps to the next sector.
                        Debug.Assert(sectorEnd - recordEnd >= RecordInfo.GetLength(), $"sectorEnd - recordEnd ({sectorEnd - recordEnd}) should be >= RecordInfo.GetLength()");
                        ref var recordInfo = ref *(RecordInfo*)recordEnd;
                        recordInfo = default;
                        recordInfo.SetInvalid();
                        recordInfo.IsSectorForceAligned = true;
                    }

                    countdownCallbackAndContext.Set(externalCallback, externalContext, numBytes);

                    // Flush the final piece to disk, zero-initializing the sector-alignment padding.
                    new Span<byte>(buffer.memory.GetValidPointer() + buffer.currentPosition, sectorEnd - buffer.currentPosition).Clear();

                    var flushLength = sectorEnd - buffer.flushedUntilPosition;
                    var pageWriteCallbackContext = new DiskWriteCallbackContext();
                    buffer.FlushToDevice(buffer.memory.TotalValidSpan.Slice(buffer.flushedUntilPosition, flushLength), alignedDeviceAddress, FlushToDeviceCallback, pageWriteCallbackContext);
                    alignedDeviceAddress += (uint)flushLength;
                    var flushedUntilAdjustment = sectorEnd - buffer.currentPosition;
                    buffer.currentPosition = sectorEnd;
                    Debug.Assert(flushedUntilAdjustment == 0 || SectorSize > 0, $"flushedUntilAdjustment {flushedUntilAdjustment} is nonzero when SectorSize is 0");
                    return flushedUntilAdjustment;
                }
            }

            // No need for a sector-aligning final flush, so release the increment we added above. Do this on a background thread as this may be the final decrement
            // and will launch the callback directly, and we want any time-consuming work, such as chaining partial-page flushes, will not block the main Flush thread,
            // which can move on to the next (sub-)page to flush.
            _ = Task.Run(countdownCallbackAndContext.Decrement);
            return 0;
        }

        /// <summary>Flush to disk for a span associated with a particular buffer.</summary>
        internal void FlushToDevice(DiskPageWriteBuffer buffer, ReadOnlySpan<byte> span, DiskWriteCallbackContext pageWriteCallbackContext)
        {
            Debug.Assert(IsAligned(span.Length, SectorSize), "Span is not aligned to sector size");
            pageWriteCallbackContext.SetAndIncrementCountdownCallback(countdownCallbackAndContext);
            buffer.FlushToDevice(span, alignedDeviceAddress, FlushToDeviceCallback, pageWriteCallbackContext);
            alignedDeviceAddress += (uint)span.Length;
        }

        /// <summary>Flush to disk for a span that is not associated with a particular buffer, such as fully-interior pages of a large overflow key or value.</summary>
        internal unsafe void FlushToDevice(ReadOnlySpan<byte> span, DiskWriteCallbackContext pageWriteCallbackContext)
        {
            Debug.Assert(IsAligned(span.Length, SectorSize), "Span is not aligned to sector size");
            pageWriteCallbackContext.SetAndIncrementCountdownCallback(countdownCallbackAndContext);

            // The span must already be pinned, as it must remain pinned after this call returns; here, we used fixed only to convert it to a byte*.
            fixed (byte* spanPtr = span)
                device.WriteAsync((IntPtr)spanPtr, alignedDeviceAddress, (uint)span.Length, FlushToDeviceCallback, pageWriteCallbackContext);
            alignedDeviceAddress += (uint)span.Length;
        }

        private void FlushToDeviceCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                logger?.LogError($"{nameof(FlushToDeviceCallback)} error: {{errorCode}}", errorCode);

            // Try to signal the event; if we have finished the last write for this buffer, the count will hit zero and Set the event so any Waits we do on it will succeed.
            // We don't currently wait on the result of intermediate buffer flushes. If context is non-null, then it is the circular buffer owner and it
            // called this flush for OnFlushComplete, so we need to call it back to complete the original callback sequence.
            var pageWriteCallbackContext = (DiskWriteCallbackContext)context;
            pageWriteCallbackContext.Dispose();
        }

        /// <summary>
        /// Finish all the current partial flush IOs, then Release this <see cref="CircularDiskPageWriteBuffer"/> allocations. After the caller calls
        /// this, this instance will go out of scope.
        /// </summary>
        internal unsafe void OnFlushComplete()
        {
            // We should have no data to flush--the last partial flush, which may have been a full page, should have ended with PartialFlushComplete
            // which flushes the last of the data for that flush fragment.
            var buffer = GetCurrentBuffer();
            Debug.Assert(buffer.currentPosition == buffer.flushedUntilPosition, $"Unflushed data remains in buffer");

            // Call the originalCallback on a background thread so any time-consuming work will not block the main Flush thread from moving on to the next (sub-)page to flush.
            _ = Task.Run(EndFlushComplete);
        }

        private void EndFlushComplete()
        {
            for (var ii = 0; ii < buffers.Length; ii++)
            {
                ref var buffer = ref buffers[ii];
                if (buffer is not null)
                {
                    buffer.Wait();  // Make sure any pending writes complete
                    buffer.Dispose();
                    buffer = null;
                }
            }
        }

        /// <inheritdoc/>
        public override string ToString() 
            => $"currIdx {currentIndex}; pageBufSize {pageBufferSize}; UsablePageSize {UsablePageSize}; priCumLen {priorCumulativeLength}, alignDevAddr {alignedDeviceAddress}, isFirsPartial {isFirstPartialFlush}; SecSize {SectorSize}";
    }
}
