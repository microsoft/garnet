// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;

    internal sealed unsafe class DiskReadBuffer : IDisposable
    {
        internal readonly IDevice device;
        internal readonly ILogger logger;

        /// <summary>Signals when reads are complete. Allows multiple reads of buffer subsections to be enqueed, and uses a ManualResetEvent
        ///     so it remains signaled until Reset() is called (currently only one read for a single span of the buffer is done).</summary>
        internal CountdownEvent countdownEvent;

        /// <summary>The buffer to read (part of) the page image into.</summary>
        internal SectorAlignedMemory memory;

        /// <summary>
        /// This is the initialization value for <see cref="currentPosition"/>; it means there is no data available for this buffer and no
        /// in-flight read (we issue reads ahead of the buffer-array traversal, so this means that by the time we got to this buffer all
        /// the data had already been read.
        /// </summary>
        const int NoPosition = -1;

        /// <summary>Current read position (we do not support write in this buffer). This class only supports Read and no Seek,
        /// so currentPosition is always where <see cref="DiskStreamReader{TStoreFunctions}"/> will read from next.</summary>
        /// <remarks>This will be either 0 or greater than or equal to <see cref="DiskPageHeader.Size"/>.</remarks>
        internal int currentPosition;

        /// <summary>Non-inclusive last position in this buffer; the number of byte read. If <see cref="currentPosition"/> equals this, then we are out of space and
        /// must move to the next buffer.</summary>
        internal int endPosition;

        /// <summary>Set by <see cref="CircularDiskReadBuffer"/> for the last chunk to read. This also indicates we may have the optionals in that read.</summary>
        internal bool isLastChunk;

        /// <summary>Set by <see cref="CircularDiskReadBuffer"/> for the last chunk to read, if there is enough room on the page to read all the optionals as well.
        ///     If so, this must be subtracted from endPosition.</summary>
        internal int optionalLength;

        internal int AvailableLength => endPosition - currentPosition;  // DiskPageHeader.Size is included in currentPosition, which is before AvailableLength

        internal ReadOnlySpan<byte> AvailableSpan => new(memory.GetValidPointer() + currentPosition, endPosition - currentPosition);

        internal DiskReadBuffer(SectorAlignedMemory memory, IDevice device, ILogger logger)
        {
            this.memory = memory;
            countdownEvent = new CountdownEvent(0); // Start with 0; we'll increment at the time of read
            this.device = device;
            this.logger = logger;
        }

        internal void Initialize()
        {
            currentPosition = endPosition = NoPosition;
            isLastChunk = false;
            optionalLength = 0;
        }

        internal ReadOnlySpan<byte> GetTailSpan(int start) => new(memory.GetValidPointer() + start, currentPosition - start);

        /// <summary>
        /// Read the first chunk of an Object deserialization from the device.
        /// </summary>
        /// <param name="alignedReadStart">Sector-aligned position in the device</param>
        /// <param name="startPosition">Start position on the page (relative to start of page)</param>
        /// <param name="length">Number of bytes to read</param>
        /// <param name="callback">The <see cref="CircularDiskReadBuffer"/> callback.</param>
        internal void ReadFromDevice(ulong alignedReadStart, int startPosition, int length, DeviceIOCompletionCallback callback)
        {
            IncrementOrResetCountdown(ref countdownEvent);

            var startPadding = startPosition - RoundDown(startPosition, (int)device.SectorSize);
            var alignedBytesToRead = RoundUp((long)alignedReadStart + startPadding + length, (int)device.SectorSize);
            currentPosition = startPosition;
            device.ReadAsync(alignedReadStart, (IntPtr)memory.aligned_pointer + startPosition, (uint)alignedBytesToRead, callback, context: this);
        }

        internal static void IncrementOrResetCountdown(ref CountdownEvent countdownEvent) => DiskWriteBuffer.IncrementOrResetCountdown(ref countdownEvent);

        internal bool WaitForDataAvailable()
        {
            // Because we have issued reads ahead of the buffer wrap, if the currentPosition is NoPosition, we're done.
            if (currentPosition == NoPosition)
                return false;

            countdownEvent.Wait();
            return true;
        }

        public void Dispose()
        {
            memory?.Return();
            memory = null;
            countdownEvent.Dispose();
            countdownEvent = null;
        }

        /// <inheritdoc/>
        public override string ToString()
            => $"currPos {currentPosition}; endPos {endPosition}; countDown {countdownEvent?.CurrentCount}; buf: {memory}";
    }
}
