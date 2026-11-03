// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.IO;

namespace Tsavorite.core
{
    /// <summary>
    /// The object a CopyUpdate leaves in its superseded source's value slot when it releases the source's value object.
    /// </summary>
    /// <remarks>
    /// <para>A CopyUpdate releases its source's value at once, even while a flush or a checkpoint may still read the source. It changes
    /// only the source's <see cref="ObjectIdMap"/> slot, never the record's bytes: the record keeps its layout, its object id and its
    /// object-log position, so a device write of its page in flight is unaffected, and no other record reuses the slot. The real object
    /// is disposed and its heap credited to the size tracker.</para>
    /// <list type="bullet">
    /// <item><see cref="Released"/>: the source's value is gone. A flush writes the record with a null value, and
    /// <see cref="LogRecord.ValueObject"/> reads null.</item>
    /// <item>A Snapshot checkpoint's own placeholder (<see cref="SnapshotObjectLogState.ObjectSentinel"/>): that checkpoint captured the
    /// source's <c>(v)</c> value into its object log and stamped the record with the capture's position. A flush keeps the stamp while
    /// that checkpoint is active, as it does for a captured object, and otherwise writes a null value.</item>
    /// </list>
    /// <para>No flush or serializer passes a placeholder to the store's value serializer, and its members other than
    /// <see cref="Dispose"/> and <see cref="ClearSerializedObjectData"/> are never called.</para>
    /// </remarks>
    internal sealed class SupersededObjectValue : IHeapObject
    {
        /// <summary>The placeholder for a released value that no checkpoint captured.</summary>
        internal static readonly SupersededObjectValue Released = new(capturingSnapshot: null);

        /// <summary>The Snapshot checkpoint that captured the source's value, or null.</summary>
        internal readonly SnapshotObjectLogState CapturingSnapshot;

        internal SupersededObjectValue(SnapshotObjectLogState capturingSnapshot) => CapturingSnapshot = capturingSnapshot;

        /// <inheritdoc/>
        /// <remarks>Zero: the released object's heap was credited when it was released.</remarks>
        public long HeapMemorySize => 0;

        /// <inheritdoc/>
        public IHeapObject Clone()
        {
            Debug.Fail("A superseded record's value is never copied");
            return this;
        }

        /// <inheritdoc/>
        public void Serialize(BinaryWriter binaryWriter) => Debug.Fail("A superseded record's value is written as a null value");

        /// <inheritdoc/>
        public void WriteType(BinaryWriter binaryWriter, bool isNull) => Debug.Fail("A superseded record's value is written as a null value");

        /// <inheritdoc/>
        public void DoSerialize(BinaryWriter writer) => Debug.Fail("A superseded record's value is written as a null value");

        /// <inheritdoc/>
        public bool CacheSerializedObjectData(ref LogRecord dstLogRecord, bool srcIsOnMemoryLog, bool checkpointNeedsSourceImage)
        {
            Debug.Fail("A superseded record is never a CopyUpdate's source");
            return false;
        }

        /// <inheritdoc/>
        public void ClearSerializedObjectData() { }

        /// <inheritdoc/>
        /// <remarks>Does nothing: a placeholder is shared by many records.</remarks>
        public void Dispose() { }
    }
}