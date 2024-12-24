// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Tsavorite.core
{
    /// <summary>This is a temporary class to carry both <see cref="StringLogRecord"/> and <see cref="ObjectLogRecord"/>
    ///     until some other things have been done that will allow clean separation.</summary>
    /// <remarks>The space is laid out as:
    [StructLayout(LayoutKind.Explicit)]
    public struct LogRecord
    {
        // Unioned fields
        [FieldOffset(0)]
        public StringLogRecord StringLogRecord;
        [FieldOffset(0)]
        public ObjectLogRecord ObjectLogRecord;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord(long physicalAddress)
        {
            StringLogRecord = new(physicalAddress);
            ObjectLogRecord = default;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord(long physicalAddress, KeyOverflowAllocator keyAlloc, ObjectIdMap objectIdMap = null)
        {
            // This overload is primarily used in passing to IObjectSessionFunctions callbacks; the primary constructor is just for record parsing.
            StringLogRecord = new(physicalAddress);
            ObjectLogRecord = new(physicalAddress, keyAlloc, objectIdMap);
        }
    }
}