// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Tsavorite.core.Allocator
{
    internal struct ValueSingleAllocation
    {
        /// <summary>The <see cref="SectorAlignedMemory"/> of the non-overflow value buffer. May be passed in or allocated during Read().</summary>
        /// <remarks>Mutually exclusive with <see cref="overflowArray"/>.</remarks>
        internal SectorAlignedMemory memoryBuffer;

        /// <summary>The <see cref="OverflowByteArray"/> of the value buffer, allocated during Read().</summary>
        /// <remarks>Mutually exclusive with <see cref="memoryBuffer"/>.</remarks>
        internal OverflowByteArray overflowArray;

        /// <summary>The current position in <see cref="Span"/>.</summary>
        internal int currentPosition;

        /// <summary>The length of <see cref="Span"/>. Held as a field so as not to require an "if".</summary>
        internal int length;

        /// <summary>The span of the value allocation.</summary>
        internal readonly ReadOnlySpan<byte> ReadOnlySpan => memoryBuffer is null ? overflowArray.ReadOnlySpan : memoryBuffer.RequiredValidSpan;

        /// <summary>The span of the value allocation.</summary>
        internal readonly Span<byte> Span => memoryBuffer is null ? overflowArray.Span : memoryBuffer.RequiredValidSpan;

        internal bool IsEmpty => memoryBuffer is null && overflowArray.IsEmpty;

        internal int AvailableLength => length - currentPosition;

        /// <summary>The cumulative length read; this includes page-break metadata, optionals if read, etc. It allows us to position ourselves to the next record start after the
        /// Read() is complete.</summary>
        long readCumulativeLength;

        /// <summary>The cumulative length of object data read from the device.</summary>
        internal long valueCumulativeLength;

        internal void Set(SectorAlignedMemory memory, int currentPosition)
        {
            TODO("Need to set readCumulativeLength");

            Debug.Assert(IsEmpty, "Should not reassign");
            this.memoryBuffer = memory;
            length = Span.Length;
            this.currentPosition = currentPosition;
        }

        internal void Set(OverflowByteArray array)
        {
            Debug.Assert(IsEmpty, "Should not reassign");
            this.overflowArray = array;
            length = Span.Length;
        }

        internal void ExtractOptionals(RecordInfo recordInfo, int offsetToOptionals, out RecordOptionals optionals)
        {
            var longSpan = MemoryMarshal.Cast<byte, long>(Span.Slice(offsetToOptionals));
            int spanIndex = 0;
            optionals = default;
            if (recordInfo.HasETag)
            {
                Debug.Assert(LogRecord.ETagSize == sizeof(long), "LogRecord.ETagSize != sizeof(long)");
                optionals.eTag = longSpan[spanIndex++];
            }
            if (recordInfo.HasExpiration)
            {
                Debug.Assert(LogRecord.ExpirationSize == sizeof(long), "LogRecord.ExpirationSize != sizeof(long)");
                optionals.expiration = longSpan[spanIndex];
            }
        }

        public void Dispose()
        {
            memoryBuffer?.Return();
            memoryBuffer = default;
            overflowArray = default;
        }
    }
}
