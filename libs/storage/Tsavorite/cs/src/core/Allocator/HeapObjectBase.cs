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
                    // Directly serialize to wire, do not cache serialized state. Restore REST even if DoSerialize
                    // throws: leaving the object in SERIALIZING would make every later Serialize() and
                    // CacheSerializedObjectData() spin on it forever, wedging the checkpoint pipeline.
                    try
                    {
                        WriteType(writer, isNull: false);
                        DoSerialize(writer);
                    }
                    finally
                    {
                        SerializationPhase = SerializationPhase.REST;
                    }
                    return;
                }

                // If we are here, SerializationPhase is one of the .SERIALIZ* states. This means that one of the following is true:
                // - Another thread is currently serializing this object (e.g. checkpoint and eviction)
                // - CopyUpdate() is serializing this object
                // - Serialization is complete. If the serializedBytes array is null, the object has been superseded in the database so is no
                //   longer reachable, so we can write a null indicator: either no checkpoint needed its (v) image, a checkpoint has completed
                //   and cleared it, or a Snapshot checkpoint captured it into its own object log.

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
        public bool CacheSerializedObjectData(ref LogRecord dstLogRecord, ref RMWInfo rmwInfo, bool srcIsOnMemoryLog, bool checkpointNeedsSourceImage)
        {
            // We'll want to clone the source object to the destination log record so PostCopyUpdater can modify it.
            // Note that this does a shallow copy of the object's internal structures (e.g. List<>), which means subsequent modifications of newValue
            // in the (v+1) version of the record will modify data seen from the 'this' in the (v) record. Normally this is OK because the (v) version
            // of the record is not reachable once the (v+1) version is inserted, but if a checkpoint is ongoing, the (v) version is part of that.
            // (If this was an Overflow instead of an Object, then PostCopyUpdater will follow the normal RCU logic, creating a new ValueSpan which will
            // probably (but not necessarily) be another Overflow.)
            Debug.Assert(dstLogRecord.DataHeader.ValueIsObject, $"{GetCurrentMethodName()} must be called for non-object {nameof(dstLogRecord)}.");

            // CopyUpdater may have already set the ValueObject so only set if not already set.
            if (!dstLogRecord.ValueObjectIsSet)
                _ = dstLogRecord.TrySetValueObject(Clone());

            // For pending-IO (DiskLogRecord) sources, skip the checkpoint-serialization state machine:
            //   - the (v) data is already persisted on disk from a prior flush, so there's nothing to capture;
            //   - 'this' is an ephemeral deserialized object that will be disposed up the pending chain, so
            //     driving its SerializationPhase and allocating serializedBytes would be pure waste;
            //   - ClearSourceValueObject is ignored by InternalRMW for non-memory sources anyway.
            if (!srcIsOnMemoryLog)
                return false;

            // No (v) image is at stake. Outside a checkpoint the new record is in the source's version, so it supersedes the source in
            // every later checkpoint, and no completed checkpoint recovers a source that a flush is still writing: a flush that has not
            // serialized the source writes a null value for it. Within a checkpoint, either the checkpoint does not capture this source (it
            // was written after the transaction start), or a Snapshot checkpoint finds its (v) value elsewhere: the source is a read-cache
            // copy of a durable main-log record, or the checkpoint has sampled the end of its main-log range. Release the old version rather
            // than serializing it; the new version already exists.
            if (!dstLogRecord.Info.IsInNewVersion || !checkpointNeedsSourceImage)
            {
                MarkSuperseded();
                rmwInfo.ClearSourceValueObject = true;
                return false;
            }

            // Create a serialized version for checkpoint version (v). This is only done for CopyUpdate during a checkpoint, to preserve the (v) data
            // of the object during a checkpoint while the (v+1) version of the record may modify the shallow-copied internal structures.
            while (true)
            {
                if (SerializationPhase == (int)SerializationPhase.REST && MakeTransition(SerializationPhase.REST, SerializationPhase.SERIALIZING))
                {
                    try
                    {
                        using var ms = new MemoryStream();
                        using var writer = new BinaryWriter(ms, Encoding.UTF8);
                        DoSerialize(writer);
                        serializedBytes = ms.ToArray();
                    }
                    catch
                    {
                        // Publish no partial capture, and do not strand the object in SERIALIZING.
                        serializedBytes = null;
                        SerializationPhase = SerializationPhase.REST;
                        throw;
                    }

                    SerializationPhase = SerializationPhase.SERIALIZED;    // This is the only place .SERIALIZED is set
                    break;
                }

                // If we're here, serializationState is one of the .SERIALIZ* states. CopyUpdate has a lock on the tag chain, so no other thread will
                // be running CopyUpdate. Therefore there are two possibilities:
                // 1. CopyUpdate has been called before and the state is .SERIALIZED and '_serialized' is created. We're done.
                // 2. Serialize() is running (likely in a Flush()) and the state is .SERIALIZING. We will Yield and loop to wait for it to finish.
                if (SerializationPhase >= SerializationPhase.SERIALIZED)
                    break;

                _ = Thread.Yield();
            }

            return true;
        }

        /// <summary>
        /// Finish a Snapshot checkpoint's capture of this object as a CopyUpdate source: give the new record its clone, then mark this object
        /// superseded with no cached bytes.
        /// </summary>
        /// <remarks>
        /// The capture wrote this object's (v) value to the checkpoint's object log, so a later flush of the superseded record writes the null
        /// image rather than the live object, whose collections the clone shares and the (v+1) record goes on to mutate. The source keeps its
        /// value object, as it does for a cached image, so a flush that walks the record still finds its components.
        /// </remarks>
        /// <param name="dstLogRecord">The CopyUpdate's new record.</param>
        internal void CompleteCapture(ref LogRecord dstLogRecord)
        {
            Debug.Assert(dstLogRecord.DataHeader.ValueIsObject, $"{GetCurrentMethodName()} must be called for non-object {nameof(dstLogRecord)}.");
            if (!dstLogRecord.ValueObjectIsSet)
                _ = dstLogRecord.TrySetValueObject(Clone());
            MarkSuperseded();
        }

        /// <summary>Whether a CopyUpdate has superseded this object, so it has reached its final serialization phase.</summary>
        internal bool IsSuperseded => Volatile.Read(ref serializationPhaseInt) >= (int)SerializationPhase.SERIALIZED;

        /// <summary>Move to the final serialization phase without caching bytes, once any serialization in progress completes.</summary>
        void MarkSuperseded()
        {
            while (true)
            {
                if (SerializationPhase == SerializationPhase.REST && MakeTransition(SerializationPhase.REST, SerializationPhase.SERIALIZED))
                    return;
                if (SerializationPhase >= SerializationPhase.SERIALIZED)
                    return;
                _ = Thread.Yield();
            }
        }

        /// <inheritdoc />
        public void ClearSerializedObjectData()
        {
            // Release the cached (v) bytes so they can be GC'd, but deliberately leave the phase terminal.
            //
            // Bytes are only ever cached by CacheSerializedObjectData for a source object that a CopyUpdate
            // has just superseded, and Clone() is a shallow copy: the (v+1) record that replaced this one
            // shares this object's internal collections and keeps mutating them. Returning to REST would let
            // a later checkpoint take the direct-serialize path and enumerate those still-shared, still-live
            // collections with no synchronization against the writer.
            //
            // SERIALIZED with null bytes is the state Serialize() already documents as "superseded after
            // checkpoint completion": it writes a null indicator. That is safe because the superseding record
            // always sits at a higher address and carries the live data.
            _ = Interlocked.Exchange(ref serializedBytes, null);
        }
    }
}