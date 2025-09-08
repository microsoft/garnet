// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;

namespace Tsavorite.core
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

        internal readonly bool IsEmpty => memoryBuffer is null && overflowArray.IsEmpty;

        internal readonly int AvailableLength => length - currentPosition;

        /// <summary>The cumulative length of object data read from the device.</summary>
        internal long valueCumulativeLength;

        internal void Set(SectorAlignedMemory memory, int currentPosition)
        {
            Debug.Assert(IsEmpty, "Should not reassign");
            memoryBuffer = memory;
            length = Span.Length;
            this.currentPosition = currentPosition;
        }

        internal void Set(OverflowByteArray array)
        {
            Debug.Assert(IsEmpty, "Should not reassign");
            overflowArray = array;
            length = Span.Length;
        }

        internal readonly void ExtractOptionalsAtEnd(RecordInfo recordInfo, int optionalLength, out RecordOptionals optionals)
        {
            if (overflowArray.IsEmpty)
                overflowArray.ExtractOptionalsAtEnd(recordInfo, optionalLength, out optionals);
            else
                ExtractOptionalsAtEnd(recordInfo, memoryBuffer, optionalLength, out optionals);
        }

        internal static unsafe void ExtractOptionalsAtEnd(RecordInfo recordInfo, SectorAlignedMemory memory, int optionalLength, out RecordOptionals optionals)
        {
            // We no longer want the optionals to count in the "requested size"
            memory.required_bytes -= optionalLength;
            ExtractOptionals(recordInfo, memory.GetValidPointer() + memory.required_bytes, out optionals);
        }

        internal static unsafe void ExtractOptionals(RecordInfo recordInfo, byte* ptrToOptionals, out RecordOptionals optionals)
        {
            optionals = default;
            if (recordInfo.HasETag)
            {
                optionals.eTag = *(long*)ptrToOptionals;
                ptrToOptionals += LogRecord.ETagSize;
            }
            if (recordInfo.HasExpiration)
                optionals.expiration = *(long*)ptrToOptionals;
        }

        public void Dispose()
        {
            memoryBuffer?.Return();
            memoryBuffer = default;
            overflowArray = default;
        }
    }
}
