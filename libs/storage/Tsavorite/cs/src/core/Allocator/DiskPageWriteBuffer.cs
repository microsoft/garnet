// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive

    internal sealed unsafe class DiskPageWriteBuffer : IDisposable
    {
        /// <summary>Signals when writes are complete. Allows multiple writes of buffer subsections to be enqueed, and uses a ManualResetEvent
        ///     so it remains signaled until Reset() is called.</summary>
        CountdownEvent countdownEvent;

        internal SectorAlignedMemory memory;

        /// <summary>Current write position (we do not support read in this buffer). This class only supports Write and no Seek,
        /// so currentPosition equals the current length. Relevant for object serialization only; reset to 0 at start of DoSerialize().</summary>
        /// <remarks><see cref="currentPosition"/> is the current length, since it is *past* the last byte copied to the buffer.</remarks>
        internal int currentPosition;

        /// <summary>Last position flushed to in this buffer (i.e. 0->flushedUntilPosition have been flushed). This is necessary because completing
        /// a flush fragment (partial page, or complete page in a multi-page flush) requires immediate flushing so the flushedUntilAddress can be updated.</summary>
        /// <remarks><see cref="flushedUntilPosition"/> is the current length, since it is *past* the last byte copied to the buffer.</remarks>
        internal int flushedUntilPosition;

        internal readonly IDevice device;
        internal readonly ILogger logger;

        internal DiskPageWriteBuffer(SectorAlignedMemory memory, IDevice device, ILogger logger)
        {
            this.memory = memory;
            this.device = device;
            this.logger = logger;
        }

        internal void Initialize(int sectorSize)
        {
            // First wait for any pending write in this buffer to complete. If this is our first time in this buffer there won't be a CountdownEvent yet.
            countdownEvent?.Wait();
            memory.valid_offset = memory.available_bytes = 0;
            currentPosition = (*(DiskPageHeader*)memory.GetValidPointer()).Initialize(sectorSize);
            flushedUntilPosition = currentPosition;
        }

        internal int RemainingCapacity => memory.AlignedTotalCapacity - DiskPageFooter.Size - currentPosition;  // DiskPageHeader.Size is included in currentPosition

        internal ReadOnlySpan<byte> GetTailSpan(int start) => new(memory.GetValidPointer() + start, currentPosition - start);

        internal void CopyFrom(ReadOnlySpan<byte> data)
        {
            data.CopyTo(memory.TotalValidSpan);
            currentPosition = data.Length;
        }

        internal static void IncrementOrResetCountdown(ref CountdownEvent countdownEvent)
        {
            if (countdownEvent is null)
                countdownEvent = new(1);
            else if (!countdownEvent.TryAddCount(1))
            {
                // This means we've enqueued one or more earlier writes which have completed.
                // First wait to be sure the callback has signaled the contained event, then Reset the event with a new count.
                countdownEvent.Wait(); // This should usually be immediate
                countdownEvent.Reset(1);
            }
        }

        /// <summary>
        /// Write the span directly to the device without changing the buffer. The span may be either an external span (e.g. a large overflow key or value, which must be pinned)
        /// or a slice of the buffer's memory span. Because of this, the caller controls adjustment of <see cref="currentPosition"/>, <see cref="flushedUntilPosition"/>, etc.
        /// </summary>
        /// <remarks>This takes a Span rather than a range because there may be a non-buffer write associated with the buffer; e.g. the buffer is used to store the beginning and ending
        /// of a page image (for alignment) and the interior is a direct write from external data.</remarks>
        internal void FlushToDevice(ReadOnlySpan<byte> span, ulong alignedDeviceAddress, DeviceIOCompletionCallback callback, DiskWriteCallbackContext pageWriteCallbackContext)
        {
            IncrementOrResetCountdown(ref countdownEvent);

            // The span must already be pinned, as it must remain pinned after this call returns; here, we used fixed only to convert it to a byte*.
            fixed (byte* spanPtr = span)
                device.WriteAsync((IntPtr)spanPtr, alignedDeviceAddress, (uint)span.Length, callback, pageWriteCallbackContext);
        }

        internal void Wait() => countdownEvent?.Wait();

        public void Dispose()
        {
            memory?.Return();
            countdownEvent.Dispose();
        }

        /// <inheritdoc/>
        public override string ToString() 
            => $"currPos {currentPosition}; buf: {memory}";
    }
}
