// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;

    /// <summary>
    /// This class drives object-serialization writing to the disk. It is reused by multiple "partial flushes": ranges on a single page (rare) or 
    /// full pages. We create one instance for all ranges of a top-level Flush() call; each partial range will call <see cref="OnBeginPartialFlush"/>,
    /// do its flushes, and call <see cref="OnPartialFlushComplete"/>. This reuse makes the most efficient use of the buffer allocations. It also
    /// requires tracking of "in-flight" device writes at multiple levels:
    /// <list type="bullet">
    ///     <item><see cref="DiskWriteBuffer"/>: A <see cref="CountdownEvent"/> tracks how many in-flight device writes are associated with that buffer.</item>
    ///     <item><see cref="CircularDiskWriteBuffer"/>: A separate <see cref="CountdownCallbackAndContext"/> instance tracks total in-flight device writes
    ///             for each <see cref="OnBeginPartialFlush"/> and <see cref="OnPartialFlushComplete"/> pair, both at the <see cref="DiskWriteBuffer"/> level
    ///             and without buffer association, such as direct writes from pinned byte[] spans. This lets us call the main-page callback once the main-page
    ///             and all associated object log writes are complete.</item>
    ///         <list type="bullet">
    ///             <item>This also contains a counter of how many <see cref="CountdownCallbackAndContext"/> instances are active (i.e. how many partial flush
    ///                 completion write batches are in-flight); when this hits 0, we can call <see cref="Dispose"/>.</item>
    ///         </list>
    /// </list>
    /// </summary>
    public class CircularDiskWriteBuffer : IDisposable
    {
        internal readonly SectorAlignedBufferPool bufferPool;
        internal readonly int bufferSize;
        internal readonly IDevice device;
        internal readonly ILogger logger;

        readonly DiskWriteBuffer[] buffers;

        /// <summary>Index of the current buffer</summary>
        int currentIndex;

        /// <summary>Device address to write to (segment and offset); incremented with each buffer flush or out-of-line write by the caller; all of these should be aligned to sector size,
        /// so this address remains sector-aligned.</summary>
        internal ObjectLogFilePositionInfo filePosition;

        /// <summary>Countdown event for global count of all buffers and all direct writes. Also triggers the external callback of a partial-flush sequence.</summary>
        /// <remarks>This is passed to all disk-write operations; multiple pending flushes may be in-flight with the callback unset; when the final flush (which may be a buffer-span, a direct write, or the
        /// final sector-aligning partial-flush completion flush), it allows the final pending flush to complete to know it *is* the final one and the callback can be called.</remarks>
        internal CountdownCallbackAndContext countdownCallbackAndContext;

        /// <summary>If true, <see cref="Dispose"/> has been called. Coordinates with <see cref="numInFlightRangeBatches"/> to indicate when we can call <see cref="DiskWriteBuffer.Dispose"/>.</summary>
        bool disposed;

        /// <summary>Tracks the number of in-flight partial flush completion write batches. Coordinates with <see cref="numInFlightRangeBatches"/> to indicate when we can call <see cref="DiskWriteBuffer.Dispose"/>.</summary>
        long numInFlightRangeBatches;

        internal CircularDiskWriteBuffer(SectorAlignedBufferPool bufferPool, int bufferSize, int numBuffers, IDevice device, ILogger logger)
        {
            this.bufferPool = bufferPool;
            this.bufferSize = bufferSize;
            this.device = device;
            this.logger = logger;

            buffers = new DiskWriteBuffer[numBuffers];
            currentIndex = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal DiskWriteBuffer GetCurrentBuffer() => buffers[currentIndex];

        internal DiskWriteBuffer MoveToAndInitializeNextBuffer()
        {
            currentIndex = (currentIndex + 1) & (buffers.Length - 1);
            return GetAndInitializeCurrentBuffer();
        }

        internal DiskWriteBuffer GetAndInitializeCurrentBuffer()
        {
            var buffer = GetCurrentBuffer();
            if (buffer is null)
            {
                buffer = new DiskWriteBuffer(bufferPool.Get(bufferSize), device, logger);
                buffers[currentIndex] = buffer;
            }

            // By this time the next device file write position has been updated, even if some of the preceding writes are still in-flight.
            var endPosition = filePosition.SegmentSize - filePosition.Offset;
            if (endPosition > (uint)bufferSize)
                endPosition = (uint)bufferSize;
            buffer.WaitUntilFreeAndInitialize((int)endPosition);
            return buffer;
        }

        internal ObjectLogFilePositionInfo GetNextRecordStartPosition()
        {
            var startFilePos = filePosition;
            var buffer = GetCurrentBuffer();
            if (buffer is not null)
                startFilePos.Offset += (uint)(buffer.currentPosition - buffer.flushedUntilPosition);
            return startFilePos;
        }

        /// <summary>Resets start positions for the next partial flush.</summary>
        internal DiskWriteBuffer OnBeginPartialFlush(ObjectLogFilePositionInfo filePos)
        {
            // We start every partial flush with the first buffer, starting at position 0.
            filePosition = filePos;
            currentIndex = 0;
            countdownCallbackAndContext = new();
            return GetAndInitializeCurrentBuffer();
        }

        /// <summary>Called when a <see cref="LogRecord"/> Write is completed. Ensures end-of-record alignment.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe void OnRecordComplete()
        {
            var buffer = GetCurrentBuffer();
            var recordAlignedCurrentPosition = RoundUp(buffer.currentPosition, Constants.kRecordAlignment);
            var padding = recordAlignedCurrentPosition - buffer.currentPosition;
            if (padding > 0)
            {
                // Assert there is still room in the buffer for the expansion (should always be as buffersize is a power of 2 much greater than kRecordAlignment)
                Debug.Assert(recordAlignedCurrentPosition <= buffer.memory.RequiredCapacity,
                            $"recordAlignedCurrentPosition {recordAlignedCurrentPosition} exceeds memory.RequiredTotalCapacity {buffer.memory.RequiredCapacity}");

                new Span<byte>(buffer.memory.GetValidPointer() + buffer.currentPosition, padding).Clear();
                buffer.currentPosition += padding;
            }
        }

        /// <summary>
        /// Finish all the current partial flush, including flushing any as-yet-unflushed data in the current buffer then calling the caller's callbacks
        /// so flushedUntilAddresses can be updated. When this function exits, there will be IOs in flight.
        /// </summary>
        /// <remarks>This write to the device is sector-aligned, which means the next fragment will probably rewrite the sector, since the currentPosition is probably
        /// somewhere in the middle of the sector.</remarks>
        /// <param name="mainLogPageSpan">The main log page span to write</param>
        /// <param name="mainLogDevice">The main log device to write to</param>
        /// <param name="alignedMainLogFlushAddress">The offset in the main log to write at</param>
        /// <param name="externalCallback">Callback sent to the initial Flush() command. Called when we are done with this partial flush operation. 
        ///     It usually signals the <see cref="PageAsyncFlushResult{T}.done"/> event so the caller knows the flush is complete and it can continue.</param>
        /// <param name="externalContext">Context sent to <paramref name="externalCallback"/>.</param>
        /// <param name="endObjectLogFilePosition">The ending file position after the partial flush is complete</param>
        internal unsafe void OnPartialFlushComplete(ReadOnlySpan<byte> mainLogPageSpan, IDevice mainLogDevice, ulong alignedMainLogFlushAddress,
                DeviceIOCompletionCallback externalCallback, object externalContext, out ObjectLogFilePositionInfo endObjectLogFilePosition)
        {
            // Lock this with a reference until we have set the callback and issue the write. This callback is for the main log page write, and
            // when the countdownCallbackAndContext.Decrement hits 0 again, we're done with this partial flush range and will call the external callback.
            countdownCallbackAndContext.Increment();
            countdownCallbackAndContext.Set(externalCallback, externalContext, (uint)mainLogPageSpan.Length);

            // Issue the last ObjectLog write for this partial flush. buffer.currentPosition should have been record-aligned by ObjectLogWriter.OnRecordComplete.
            var buffer = GetCurrentBuffer();
            Debug.Assert(IsAligned(buffer.currentPosition, Constants.kRecordAlignment), $"buffer.currentPosition {buffer.currentPosition} is not record-aligned");
            Debug.Assert(IsAligned(buffer.flushedUntilPosition, (int)device.SectorSize), $"flushedUntilOffset {buffer.flushedUntilPosition} is not sector-aligned");
            Debug.Assert(buffer.currentPosition >= buffer.flushedUntilPosition, $"buffer.currentPosition {buffer.currentPosition} must be >= buffer.flushedUntilPosition {buffer.flushedUntilPosition}");

            if (buffer.currentPosition > buffer.flushedUntilPosition)
            {
                // We have something to flush. First ensure sector-alignment of the flush; we'll "waste" some space to do so. This is necessary to avoid rewriting sectors,
                // which can be a problem for some devices due to inefficiencies in rewriting or inability to back up (or both).
                var sectorEnd = RoundUp(buffer.currentPosition, (int)device.SectorSize);
                if (sectorEnd > buffer.currentPosition)
                {
                    // Prepare to flush the final piece to disk by zero-initializing the sector-alignment padding.
                    new Span<byte>(buffer.memory.GetValidPointer() + buffer.currentPosition, sectorEnd - buffer.currentPosition).Clear();
                    buffer.currentPosition = sectorEnd;
                }

                // Now write the buffer to the device.
                buffer.FlushToDevice(ref filePosition, FlushToDeviceCallback, CreateDiskWriteCallbackContext());
            }

            // Update the object log file position for the caller.
            endObjectLogFilePosition = filePosition;

            // Write the main log page to the mainLogDevice.
            FlushToMainLogDevice(mainLogPageSpan, mainLogDevice, alignedMainLogFlushAddress, CreateDiskWriteCallbackContext());

            // We added a count to countdownCallbackAndContext at the start, and the callback state creation also added a count. Remove the one we added at the start.
            countdownCallbackAndContext.Decrement();
        }

        internal DiskWriteCallbackContext CreateDiskWriteCallbackContext() => new(countdownCallbackAndContext);
        internal DiskWriteCallbackContext CreateDiskWriteCallbackContext(RefCountedPinnedGCHandle refGcHandle) => new(countdownCallbackAndContext, refGcHandle);
        internal DiskWriteCallbackContext CreateDiskWriteCallbackContext(GCHandle gcHandle) => new(countdownCallbackAndContext, gcHandle);

        /// <summary>Flush the current buffer. If we are in an operation that filled previous buffers, those will have been flushed already by earlier calls.</summary>
        internal void FlushCurrentBuffer()
        {
            var buffer = GetCurrentBuffer();
            var writeCallbackContext = CreateDiskWriteCallbackContext();
            buffer.FlushToDevice(ref filePosition, FlushToDeviceCallback, writeCallbackContext);
        }

        /// <summary>Flush to disk for a span that is not associated with a particular buffer, such as fully-interior spans of a large overflow key or value.</summary>
        internal unsafe void FlushToDevice(ReadOnlySpan<byte> span, DiskWriteCallbackContext writeCallbackContext)
        {
            Debug.Assert(IsAligned(span.Length, (int)device.SectorSize), "Span is not aligned to sector size");

            // The span must already be pinned, as it must remain pinned after this call returns; here, we used fixed only to convert it to a byte*.
            fixed (byte* spanPtr = span)
                device.WriteAsync((IntPtr)spanPtr, filePosition.SegmentId, filePosition.Offset, (uint)span.Length, FlushToDeviceCallback, writeCallbackContext);
            filePosition.Offset += (uint)span.Length;
        }

        /// <summary>Flush a main-log page span to the main log device.</summary>
        internal unsafe void FlushToMainLogDevice(ReadOnlySpan<byte> span, IDevice mainLogDevice, ulong alignedMainLogFlushAddress, DiskWriteCallbackContext writeCallbackContext)
        {
            Debug.Assert(IsAligned(span.Length, (int)device.SectorSize), "Span is not aligned to sector size");
            Debug.Assert(IsAligned(alignedMainLogFlushAddress, (int)device.SectorSize), "mainLogAlignedDeviceOffset is not aligned to sector size");

            // The span must already be pinned, as it must remain pinned after this call returns; here, we used fixed only to convert it to a byte*.
            fixed (byte* spanPtr = span)
                mainLogDevice.WriteAsync((IntPtr)spanPtr, alignedMainLogFlushAddress, (uint)span.Length, FlushToDeviceCallback, writeCallbackContext);
        }

        private void FlushToDeviceCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                logger?.LogError($"{nameof(FlushToDeviceCallback)} error: {{errorCode}}", errorCode);

            // Try to signal the event; if we have finished the last write for this buffer, the count will hit zero and Set the event so any Waits we do on it will succeed.
            // We don't currently wait on the result of intermediate buffer flushes. If context is non-null, then it is the circular buffer owner and it
            // called this flush for OnFlushComplete, so we need to call it back to complete the original callback sequence.
            var writeCallbackContext = (DiskWriteCallbackContext)context;

            // If this returns 0 we have finished all in-flight writes for the writeCallbackContext.countdownCallbackAndContext instance, but there may be more instances
            // active, so adjust and check the global count, and if *that* is zero, check the disposed state (that ensures no further partial flush ranges will be sent).
            if (writeCallbackContext.Release() == 0 && Interlocked.Decrement(ref numInFlightRangeBatches) == 0 && disposed)
                ClearBuffers();

        }

        /// <inheritdoc/>
        public void Dispose()
        {
            // If we are here, then we have returned from the partial-flush loop and will not be incrementing numInFlightRangeBatches again, so if it is 0
            // we are done and can free the buffers.
            disposed = true;
            if (numInFlightRangeBatches == 0)
                ClearBuffers();
        }

        private void ClearBuffers()
        {
            // We should have no data to flush--the last partial flush should have ended with PartialFlushComplete which flushes the last of the data for that flush fragment,
            // and we wait for that to finish before calling the caller's callback. However, we may have to wait for flushed data to complete; this may be from either the
            // just-completed partial-flush range, or even from the range before that if the most recent range did not use all buffers; at the time this is called there may
            // be one or more in-flight countdownCallbackAndContexts. So we just wait.
            for (var ii = 0; ii < buffers.Length; ii++)
            {
                ref var buffer = ref buffers[ii];
                if (buffer is not null)
                {
                    buffer.Wait();
                    buffer.Dispose();
                    buffer = null;
                }
            }
        }

        /// <inheritdoc/>
        public override string ToString() 
            => $"currIdx {currentIndex}; bufSize {bufferSize}; filePos {filePosition}, SecSize {(int)device.SectorSize}";
    }
}
