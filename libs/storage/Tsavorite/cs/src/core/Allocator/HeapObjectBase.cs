// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    public struct ObjectSizes
    {
        /// <summary>In-memory size, including .NET object overheads</summary>
        public long Memory;

        /// <summary>Serialized size, for disk IO or other storage</summary>
        public long Disk;

        public ObjectSizes(long memory, long disk)
        {
            Memory = memory;
            Disk = disk + sizeof(byte); // Additional byte for GarnetObjectBase.Type
        }

        [Conditional("DEBUG")]
        public void Verify() => Debug.Assert(Memory >= 0 && Disk >= 0, $"Invalid sizes [{Memory}, {Disk}]");
    }

    /// <summary>
    /// The base class for heap Value Objects in Tsavorite.
    /// </summary>
    public abstract class HeapObjectBase : IHeapObject
    {
        /// <inheritdoc />
        public long MemorySize { get => sizes.Memory; set => sizes.Memory = value; }

        /// <inheritdoc />
        public long DiskSize { get => sizes.Disk; set => sizes.Disk = value; }

        /// <summary>Combination of object sizes for memory and disk.</summary>
        public ObjectSizes sizes;

        /// <summary>The current internal serialization phase of the object.</summary>
        SerializationPhase SerializationPhase
        {
            get => (SerializationPhase)serializationPhaseInt;
            set => serializationPhaseInt = (int)value;
        }

        int serializationPhaseInt;

        /// <summary>The internal serialized bytes of the object.</summary>
        byte[] serializedBytes;

        /// <summary>
        /// Create a cloned (shallow copy) of this object
        /// </summary>
        public abstract HeapObjectBase Clone();

        /// <summary>
        /// Serialize to the binary writer.
        /// </summary>
        public abstract void DoSerialize(BinaryWriter writer);

        /// <summary>
        /// Transition the serialization phase of the object.
        /// </summary>
        public bool MakeTransition(SerializationPhase expectedPhase, SerializationPhase nextPhase)
            => Interlocked.CompareExchange(ref serializationPhaseInt, (int)nextPhase, (int)expectedPhase) == (int)expectedPhase;

        /// <inheritdoc />
        public abstract void Dispose();

        /// <inheritdoc />
        public abstract void WriteType(BinaryWriter writer, bool isNull);

        /// <inheritdoc />
        public void Serialize(BinaryWriter writer)
        {
            while (true)
            {
                // This is probably called from Flush, including for checkpoints. If CopyUpdate() has already serialized the object, we will use that
                // serialized state. Otherwise, we will serialize the object directly to the writer, and not create the serialized byte[]; only
                // CopyUpdater does that, as it must ensure the object's (v1) data is not changed during the checkpoint.
                if (SerializationPhase == SerializationPhase.REST && MakeTransition(SerializationPhase.REST, SerializationPhase.SERIALIZING))
                {
                    // Directly serialize to wire, do not cache serialized state
                    WriteType(writer, isNull: false);
                    DoSerialize(writer);
                    SerializationPhase = SerializationPhase.REST;
                    return;
                }

                // If we are here, SerializationPhase is one of the .SERIALIZ* states. This means that one of the following is true:
                // - Another thread is currently serializing this object (e.g. checkpoint and eviction)
                // - CopyUpdate() is serializing this object
                // - Serialization is complete. If the serializedBytes array is null, it means the checkpoint has completed and cleared it
                //   and the object has been superseded in the database so is no longer reachable, so we can write a null indicator.

                if (SerializationPhase == SerializationPhase.SERIALIZED)
                {
                    // If serialized state is cached, use that
                    var _serialized = serializedBytes;
                    if (_serialized != null)
                    {
                        WriteType(writer, isNull: false);
                        writer.Write(_serialized);
                    }
                    else
                    {
                        // Write null object to stream
                        WriteType(writer, isNull: true);
                    }
                    return;
                }

                Thread.Yield();
            }
        }

        /// <inheritdoc />
        public void CopyObjectAndCacheSerializedDataIfNeeded<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            // If this was a disk log record, we can just copy an object straight over if there is one; similar for Overflow (TODO: Stream Deserializer
            // of long overflow values directly to the overflow byte[] to avoid double-allocation).

            // It is not a DiskLogRecord, so it must be a LogRecord. First clone the source object to the destination log record.
            // Note that this does a shallow copy of the object's internal structures (e.g. List<>), which means subsequent modifications of newValue
            // in the (v+1) version of the record will modify data seen from the 'this' in the (v) record. Normally this is OK because the (v) version
            // of the record is not reachable once the (v+1) version is inserted, but if a checkpoint is ongoing, the (v) version is part of that.
            Debug.Assert(ReferenceEquals(this, srcLogRecord.ValueObject), $"{GetCurrentMethodName()} should be called on the Source LogRecord's ValueObject.");
            var newValueObject = Clone();

            Debug.Assert(dstLogRecord.Info.ValueIsObject, $"{GetCurrentMethodName()} should not be called for non-object {nameof(dstLogRecord)}.");
            _ = dstLogRecord.TrySetValueObject(newValueObject);

            // If we are not currently taking a checkpoint, we can delete the old version since the new version of the object is already created
            // in the new record, which we know has been inserted because this routine is called after a successful CAS.
            if (!dstLogRecord.Info.IsInNewVersion)
            {
                rmwInfo.ClearSourceValueObject = true;
                return;
            }

            // Create a serialized version for checkpoint version (v). This is only done for CopyUpdate during a checkpoint, to preserve the (v) data
            // of the object during a checkpoint while the (v+1) version of the record may modify the shallow-copied internal structures.
            while (true)
            {
                if (newValueObject.SerializationPhase == (int)SerializationPhase.REST && newValueObject.MakeTransition(SerializationPhase.REST, SerializationPhase.SERIALIZING))
                {
                    using var ms = new MemoryStream();
                    using var writer = new BinaryWriter(ms, Encoding.UTF8);
                    newValueObject.DoSerialize(writer);
                    newValueObject.serializedBytes = ms.ToArray();

                    newValueObject.SerializationPhase = SerializationPhase.SERIALIZED;    // This is the only place .SERIALIZED is set
                    break;
                }

                // If we're here, serializationState is one of the .SERIALIZ* states. CopyUpdate has a lock on the tag chain, so no other thread will
                // be running CopyUpdate. Therefore there are two possibilities:
                // 1. CopyUpdate has been called before and the state is .SERIALIZED and '_serialized' is created. We're done.
                // 2. Serialize() is running (likely in a Flush()) and the state is .SERIALIZING. We will Yield and loop to wait for it to finish.
                if (newValueObject.SerializationPhase >= SerializationPhase.SERIALIZED)
                    break;

                _ = Thread.Yield();
            }
        }
    }
}
