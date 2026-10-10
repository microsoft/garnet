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
        /// Clone this object into <paramref name="dstLogRecord"/>'s value slot (if not already set) and, when the
        /// source record is on the in-memory log, run the checkpoint-serialization state machine on <c>this</c> so
        /// the (v) snapshot is preserved for an in-flight checkpoint.
        /// </summary>
        /// <param name="dstLogRecord">Destination record; its ValueObject is set to a clone of <c>this</c> if not already set by the caller.</param>
        /// <param name="rmwInfo">RMW info; <see cref="RMWInfo.ClearSourceValueObject"/> is set when the caller may safely null the source's value slot.</param>
        /// <param name="srcIsOnMemoryLog">
        /// True when the source record resides on the in-memory log (state machine runs). False for pending-IO
        /// <see cref="DiskLogRecord"/> sources, where the (v) data is already persisted on disk, the source
        /// <c>this</c> is ephemeral and about to be disposed up the pending chain, and clone is all that's needed.
        /// </param>
        /// <param name="checkpointNeedsSourceImage">
        /// Whether an ongoing checkpoint still needs this object's (v) image. This only narrows caching *within* a
        /// checkpoint: the (v) image is captured when the destination is in the new version and this is also true.
        /// False means there is nothing to preserve - the source was itself written after this checkpoint's
        /// transaction start, or the Snapshot has already written the source's page - so the source value may be
        /// released immediately instead of serialized.
        /// </param>
        /// <returns>True if the (v) bytes were cached, so the caller can note that a later sweep has work to do.</returns>
        bool CacheSerializedObjectData(ref LogRecord dstLogRecord, ref RMWInfo rmwInfo, bool srcIsOnMemoryLog, bool checkpointNeedsSourceImage);

        /// <summary>
        /// Clear any serialized data from <see cref="CacheSerializedObjectData(ref LogRecord, ref RMWInfo, bool, bool)"/>.
        /// This must be idempotent and must not change object state when no cached data is present.
        /// </summary>
        void ClearSerializedObjectData();
    }
}