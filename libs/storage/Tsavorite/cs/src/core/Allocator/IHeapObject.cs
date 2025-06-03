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
        /// Total size of the object in memory, including .NET object overheads.
        /// </summary>
        public long MemorySize { get; set; }

        /// <summary>
        /// Total serialized size of the object; the size it will take when written to disk or other storage.
        /// </summary>
        public long DiskSize { get; set; }

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
        /// <returns>True if srcLogRecord.ValueObject should be cleared upon return.</returns>
        void CopyObjectAndCacheSerializedDataIfNeeded<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord;
    }
}
