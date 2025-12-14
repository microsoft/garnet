// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    /// <summary>
    /// The base class for heap Value Objects in Tsavorite.
    /// </summary>
    public abstract class HeapObjectBase : IHeapObject
    {
        /// <inheritdoc />
        public long HeapMemorySize { get; protected set; }

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
        /// <remarks>The implementation of this method should NOT copy <see cref="serializedBytes"/>.</remarks>
        public abstract IHeapObject Clone();

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
        public void CacheSerializedObjectData(ref LogRecord srcLogRecord, ref LogRecord dstLogRecord, ref RMWInfo rmwInfo)
        {
            // We'll want to clone the source object to the destination log record so PostCopyUpdater can modify it.
            // Note that this does a shallow copy of the object's internal structures (e.g. List<>), which means subsequent modifications of newValue
            // in the (v+1) version of the record will modify data seen from the 'this' in the (v) record. Normally this is OK because the (v) version
            // of the record is not reachable once the (v+1) version is inserted, but if a checkpoint is ongoing, the (v) version is part of that.
            // (If this was an Overflow instead of an Object, then PostCopyUpdater will follow the normal RCU logic, creating a new ValueSpan which will
            // probably (but not necessarily) be another Overflow.)
            Debug.Assert(ReferenceEquals(this, srcLogRecord.ValueObject), $"{GetCurrentMethodName()} must be called on the Source LogRecord's ValueObject.");
            Debug.Assert(dstLogRecord.Info.ValueIsObject, $"{GetCurrentMethodName()} must be called for non-object {nameof(dstLogRecord)}.");

            // CopyUpdater may have already set the ValueObject so only set if not already set.
            if (!dstLogRecord.ValueObjectIsSet)
                _ = dstLogRecord.TrySetValueObject(srcLogRecord.ValueObject.Clone());

            // If we are not currently taking a checkpoint, we can delete the old version
            // since the new version of the object is already created.
            var oldValueObject = (HeapObjectBase)srcLogRecord.ValueObject;
            if (!srcLogRecord.Info.IsInNewVersion)
            {
                // Wait for any concurrent ongoing serialization of oldValue to complete
                while (true)
                {
                    if (oldValueObject.SerializationPhase == (int)SerializationPhase.REST && MakeTransition(SerializationPhase.REST, SerializationPhase.SERIALIZED))
                        break;

                    if ((int)oldValueObject.SerializationPhase >= (int)SerializationPhase.SERIALIZED)
                        break;

                    _ = Thread.Yield();
                }
                rmwInfo.ClearSourceValueObject = true;
                return;
            }

            // Create a serialized version for checkpoint version (v). This is only done for CopyUpdate during a checkpoint, to preserve the (v) data
            // of the object during a checkpoint while the (v+1) version of the record may modify the shallow-copied internal structures.
            while (true)
            {
                if (oldValueObject.SerializationPhase == (int)SerializationPhase.REST && oldValueObject.MakeTransition(SerializationPhase.REST, SerializationPhase.SERIALIZING))
                {
                    using var ms = new MemoryStream();
                    using var writer = new BinaryWriter(ms, Encoding.UTF8);
                    oldValueObject.DoSerialize(writer);
                    oldValueObject.serializedBytes = ms.ToArray();

                    oldValueObject.SerializationPhase = SerializationPhase.SERIALIZED;    // This is the only place .SERIALIZED is set
                    break;
                }

                // If we're here, serializationState is one of the .SERIALIZ* states. CopyUpdate has a lock on the tag chain, so no other thread will
                // be running CopyUpdate. Therefore there are two possibilities:
                // 1. CopyUpdate has been called before and the state is .SERIALIZED and '_serialized' is created. We're done.
                // 2. Serialize() is running (likely in a Flush()) and the state is .SERIALIZING. We will Yield and loop to wait for it to finish.
                if (oldValueObject.SerializationPhase >= SerializationPhase.SERIALIZED)
                    break;

                _ = Thread.Yield();
            }
        }

        /// <inheritdoc />
        public void ClearSerializedObjectData()
        {
            // Clear the serialized data, so it can be GC'd
            serializedBytes = null;
            SerializationPhase = SerializationPhase.REST; // Reset to initial state
        }
    }
}