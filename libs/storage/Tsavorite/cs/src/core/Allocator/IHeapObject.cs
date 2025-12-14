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
        /// The maximum length of a serialized Value object.
        /// </summary>
        public const long MaxSerializedObjectSize = 1L << 40;

        /// <summary>
        /// Total estimated size of the object in heap memory, including .NET object overheads, for Overflow allocations and Objects.
        /// </summary>
        long HeapMemorySize { get; }

        /// <summary>
        /// Create a cloned (shallow copy) of this object
        /// </summary>
        IHeapObject Clone();

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
        void CacheSerializedObjectData(ref LogRecord srcLogRecord, ref LogRecord dstLogRecord, ref RMWInfo rmwInfo);

        /// <summary>
        /// Clear any serialized data from <see cref="CacheSerializedObjectData(ref LogRecord, ref LogRecord, ref RMWInfo)"/>
        /// </summary>
        void ClearSerializedObjectData();
    }
}