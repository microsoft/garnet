// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;

namespace Tsavorite.core
{
    /// <summary>
    /// This is the base interface from which any value of object type must derive for <see name="ObjectAllocator"/>.
    /// </summary>
    public interface IHeapObject : IDisposable
    {
        /// <summary>
        /// Total estimated size of the object in heap memory, including .NET object overheads, for Overflow allocations and Objects.
        /// </summary>
        public long HeapMemorySize { get; set; }

        /// <summary>
        /// Total serialized size of the object as a byte stream; the size it will take when written to disk or other storage.
        /// May be an estimate if <see cref="SerializedSizeIsExact"/> is false. Will be updated after an <see cref="IObjectSerializer{IHeapObject}.Serialize(IHeapObject)"/> call.
        /// </summary>
        public long SerializedSize { get; set; }

        /// <summary>
        /// Total serialized size of the object as a byte stream; the size it will take when written to disk or other storage.
        /// May be an estimate if 
        /// </summary>
        public bool SerializedSizeIsExact { get; }

        /// <summary>
        /// Create a cloned (shallow copy) of this object
        /// </summary>
        HeapObjectBase Clone();

        /// <summary>
        /// Top-level routine to Serialize to the binary writer; checks for cached checkpoint data and calls <see cref="DoSerialize(BinaryWriter)"/> if needed.
        /// </summary>
        void Serialize(BinaryWriter binaryWriter);

        /// <summary>
        /// Write the type of the object to the binary writer.
        /// </summary>
        void WriteType(BinaryWriter binaryWriter, bool isNull);

        /// <summary>
        /// Internal routine to Serialize to the binary writer.
        /// </summary>
        void DoSerialize(BinaryWriter writer);

        /// <summary>
        /// Copy the ValueObject from srcLogRecord to newLogRecord, cloning and caching serialized data if needed.
        /// </summary>
        void CacheSerializedObjectData(ref LogRecord srcLogRecord, ref LogRecord dstLogRecord);

        /// <summary>
        /// Clear any serialized data from <see cref="CacheSerializedObjectData(ref LogRecord, ref LogRecord)"/>
        /// </summary>
        void ClearSerializedObjectData();
    }
}
