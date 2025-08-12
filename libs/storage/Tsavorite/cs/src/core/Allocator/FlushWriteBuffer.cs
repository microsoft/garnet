// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;

    internal sealed unsafe class FlushWriteBuffer : IDisposable
    {
        /// <summary>Signals when writes are complete. Allows multiple writes of buffer subsections to be enqueed, and uses a ManualResetEvent
        ///     so it remains signaled until Reset() is called.</summary>
        CountdownEvent countdownEvent;

        internal SectorAlignedMemory memory;

        /// <summary>Current write position (we do not support read in this buffer). This class only supports Write and no Seek,
        /// so currentPosition equals the current length. Relevant for object serialization only; reset to 0 at start of DoSerialize().</summary>
        /// <remarks><see cref="currentPosition"/> is the current length, since it is *past* the last byte copied to the buffer.</remarks>
        internal int currentPosition;

        internal readonly IDevice device;
        internal readonly ILogger logger;

        internal FlushWriteBuffer(SectorAlignedMemory memory, IDevice device, ILogger logger)
        {
            this.memory = memory;
            this.device = device;
            this.logger = logger;
        }

        internal void Initialize(SectorAlignedBufferPool bufferPool, int bufferSize)
        {
            // First wait for any pending write in this buffer to complete.
            countdownEvent?.Wait();
            memory ??= bufferPool.Get(bufferSize);
            memory.valid_offset = memory.available_bytes = 0;
            currentPosition = 0;
        }

        internal int RemainingCapacity => memory.AlignedTotalCapacity - currentPosition;

        internal ReadOnlySpan<byte> GetTailSpan(int start) => new(memory.GetValidPointer() + start, currentPosition - start);

        internal void CopyFrom(ReadOnlySpan<byte> data)
        {
            data.CopyTo(memory.TotalValidSpan);
            currentPosition = data.Length;
        }

        /// <summary>Write the span directly to the device without changing the buffer.</summary>
        internal void FlushToDevice(ReadOnlySpan<byte> span, ulong alignedDeviceAddress, bool wait = false, object context = null)
        {
            if (countdownEvent is null)
                countdownEvent = new(1);
            else if (!countdownEvent.TryAddCount(1))
            {
                // This means we've enqueued a write of an earlier itermediate range flush such as for KeyInterior.
                // First wait to be sure the callback has signaled the contained event, then Reset the event with a new count.
                countdownEvent.Wait(); // This should usually be immediate
                countdownEvent.Reset(1);
            }

            // Pin the span to get a pointer; also, we may be writing a ReadOnlySpan that's around an unpinned byte[], e.g. KeyInterior, in which case
            // "wait" is true so we don't let the buffer unpin until the write is done.
            fixed (byte* spanPtr = span)
            {
                device.WriteAsync((IntPtr)spanPtr, alignedDeviceAddress, (uint)span.Length, FlushToDeviceCallback, context);
                if (wait)
                    countdownEvent.Wait();
            }
        }

        private void FlushToDeviceCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                logger?.LogError($"{nameof(FlushToDeviceCallback)} error: {{errorCode}}", errorCode);

            // Try to signal the event; if we have finished the last write, the count will hit zero and Set the event so any Waits we do on it will succeed.
            // We don't currently wait on the result of intermediate buffer flushes. If context is non-null, then it is the circular buffer owner and it
            // called this flush for OnFlushComplete, so we need to call it back to complete the original callback sequence.
            if (countdownEvent.Signal() && context is CircularFlushWriteBuffer buffers)
                buffers.EndFlushComplete();
        }

        /// <summary>
        /// End of record, so align <see cref="currentPosition"/> to record alignment, flushing if we hit end of buffer.
        /// </summary>
        /// <param name="alignedDeviceAddress">Device address to write to</param>
        /// <param name="numBytesFlushed">Number of bytes flushed if the alignment hit end of buffer</param>
        /// <returns>Whether we hit end of buffer; if so, the <see cref="CircularFlushWriteBuffer"/> will move to the next buffer.</returns>
        internal bool OnRecordComplete(ulong alignedDeviceAddress, out int numBytesFlushed)
        {
            // The buffer is aligned to sector size, which will be a multiple of kRecordAlignment, so this should always be true.
            // We might align such that we are right at the end of the buffer, which is OK but we need to move to the next buffer if that happens.
            var newCurrentPosition = RoundUp(currentPosition, Constants.kRecordAlignment);
            Debug.Assert(newCurrentPosition <= memory.AlignedTotalCapacity, $"newCurrentPosition {newCurrentPosition} exceeds memory.AlignedTotalCapacity {memory.AlignedTotalCapacity}");

            var alignmentIncrease = newCurrentPosition - currentPosition;
            if (alignmentIncrease > 0)
            {
                // Zeroinit the extra and update current position.
                new Span<byte>(memory.GetValidPointer() + currentPosition, alignmentIncrease).Clear();
                currentPosition = newCurrentPosition;
            }

            if (currentPosition >= memory.AlignedTotalCapacity)
            {
                FlushToDevice(memory.TotalValidSpan.Slice(0, currentPosition), alignedDeviceAddress);
                numBytesFlushed = currentPosition;
                return true;
            }

            numBytesFlushed = 0;
            return false;
        }

        public void Dispose()
        {
            memory?.Return();
            countdownEvent.Dispose();
        }

        /// <inheritdoc/>
        public override string ToString() 
            => $"currPos {currentPosition}; buf: {memory}";
    }

    internal class CircularFlushWriteBuffer : IDisposable
    {
        const int NumBuffers = 4;
        readonly SectorAlignedBufferPool bufferPool;
        readonly int bufferSize;
        internal readonly IDevice device;
        internal readonly ILogger logger;
        readonly DeviceIOCompletionCallback originalCallback;
        readonly object originalContext;
        uint totalWrittenLength;

        readonly FlushWriteBuffer[] buffers;
        int currentIndex;

        internal int SectorSize => (int)device.SectorSize;

        internal CircularFlushWriteBuffer(SectorAlignedBufferPool bufferPool, int bufferSize, IDevice device, ILogger logger, DeviceIOCompletionCallback originalCallback, object originalContext)
        {
            this.bufferPool = bufferPool;
            this.bufferSize = bufferSize;
            this.device = device;
            this.logger = logger;
            this.originalCallback = originalCallback;
            this.originalContext = originalContext;

            buffers = new FlushWriteBuffer[NumBuffers];
            currentIndex = 0;
        }

        internal FlushWriteBuffer GetAndInitializeCurrentBuffer()
        {
            var buffer = buffers[currentIndex];
            if (buffer is null)
                buffer = buffers[currentIndex] = new FlushWriteBuffer(bufferPool.Get(bufferSize), device, logger);
            buffer.Initialize(bufferPool, bufferSize);
            return buffer;
        }

        internal FlushWriteBuffer ShiftTailToNextBuffer(int start)
        {
            // Move to the next buffer and make sure it is available (writes have completed).
            var buffer1 = buffers[currentIndex];
            currentIndex = (currentIndex + 1) % NumBuffers;
            var buffer2 = GetAndInitializeCurrentBuffer();

            // Copy any partial tail to the next buffer.
            if (start < buffer1.currentPosition)
                buffer2.CopyFrom(buffer1.GetTailSpan(start));
            return buffer2;
        }

        internal FlushWriteBuffer OnRecordComplete(ulong alignedDeviceAddress, out int numBytesFlushed)
        {
            // We might align such that we are right at the end of the buffer, which is OK but we need to move to the next buffer if that happens.
            if (buffers[currentIndex].OnRecordComplete(alignedDeviceAddress, out numBytesFlushed))
            {
                currentIndex = (currentIndex + 1) % NumBuffers;
                return GetAndInitializeCurrentBuffer();
            }
            return buffers[currentIndex];
        }

        /// <summary>
        /// Finish all pending flushes and enqueue an invocation of the original callback. When this function exits, there will be IOs in flight.
        /// </summary>
        /// <param name="alignedDeviceAddress">Address on device to write to</param>
        /// <param name="totalWrittenLength">The total length we've written so far; includes record-length alignment but not sector-size alignment</param>
        internal unsafe void BeginFlushComplete(ulong alignedDeviceAddress, uint totalWrittenLength)
        {
            // If the current buffer has unflushed data, flush it with "this" as the context object. We need to do this because we
            // will let this circular buffer object go out of scope, so by passing "this" as the object we keep a reference alive.
            // When that flush is complete, it will call EndFlushComplete(). Note: we will never write data to a buffer after we have
            // issued a Flush() on it, until we wrap around the circular buffer and get to it again and Wait(). So if the currentPosition
            // is nonzero, we know there is unflushed data.
            var buffer = buffers[currentIndex];
            if (buffer.currentPosition != 0)
            {
                // Flush the final piece to disk, clearing the sector-alignment padding.
                var flushLength = RoundUp(buffer.currentPosition, SectorSize);
                if (flushLength > buffer.currentPosition)
                    new Span<byte>(buffer.memory.GetValidPointer() + buffer.currentPosition, flushLength - buffer.currentPosition).Clear();
                buffer.FlushToDevice(buffer.memory.TotalValidSpan.Slice(0, flushLength), alignedDeviceAddress, wait: false, this);
                return;
            }

            // No data to write. Rather than call the originalCallback inline, do it on a background thread so any time-consuming work will
            // not block the main Flush thread, which can move on to the next (sub-)page to flush.
            this.totalWrittenLength = totalWrittenLength;
            _ = Task.Run(EndFlushComplete);
        }

        internal void EndFlushComplete()
        {
            _ = Task.Run(() => originalCallback(0, totalWrittenLength, originalContext));
        }

        /// <inheritdoc/>
        public override string ToString() => $"currIdx {currentIndex}";

        public void Dispose()
        {
            foreach (var buffer in buffers)
                buffer.Dispose();
        }
    }
}
