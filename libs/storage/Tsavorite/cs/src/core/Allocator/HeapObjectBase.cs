// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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

        /// <summary>The object's phase word: a <see cref="SerializationPhase"/> in the low <see cref="PhaseBits"/> bits, and for
        /// <see cref="SerializationPhase.SNAPSHOT_STAMPED"/> and <see cref="SerializationPhase.CAPTURED"/> the tag of the Snapshot checkpoint
        /// they belong to in the bits above.</summary>
        /// <remarks>
        /// <para>The phase records which writer last published this object's record: a flush or a Snapshot checkpoint's capture writes the
        /// object to an object log and stamps the record with the position. A writer holds the object's claim
        /// (<see cref="SerializationPhase.SERIALIZING"/>) from that write until its stamps are complete, so two writers never interleave
        /// their stamps. The claim and the phase live in this existing field, so publication adds no per-object memory.</para>
        /// <para>A capture uses the phase to avoid rewriting a record whose image a device write may be reading: once a flush of the active
        /// checkpoint has published the record, the capture changes at most its marker bit. That is what lets the flushes write the live page
        /// rather than a copy of it. See website/docs/dev/tsavorite/objectlog-serialization.md section 13.</para>
        /// <para>This assumes the object belongs to one record. A copy-to-tail can still attach it to a second one; that sharing is separate.</para>
        /// </remarks>
        int serializationPhaseInt;

        /// <summary>The internal serialized bytes of the object.</summary>
        byte[] serializedBytes;

        /// <summary>The object whose claim the current thread holds while it writes the object to an object log, so <see cref="Serialize"/>
        /// called back by the store's value serializer runs under that claim rather than waiting for it.</summary>
        [ThreadStatic]
        static HeapObjectBase publishingObject;

        /// <summary>Number of low bits of the phase word that hold the <see cref="SerializationPhase"/>.</summary>
        internal const int PhaseBits = 3;
        const int PhaseMask = (1 << PhaseBits) - 1;

        /// <summary>The largest Snapshot checkpoint tag a phase word holds; tags wrap at this value.</summary>
        internal const int MaxCheckpointTag = int.MaxValue >> PhaseBits;

        /// <summary>The phase word of a superseded object that has no cached bytes.</summary>
        internal const int SupersededWord = (int)SerializationPhase.SERIALIZED;

        /// <summary>The phase word a ReadOnly flush leaves.</summary>
        internal const int ReadOnlyStampedWord = (int)SerializationPhase.READONLY_STAMPED;

        /// <summary>The phase word a Snapshot flush of the checkpoint tagged <paramref name="checkpointTag"/> leaves.</summary>
        internal static int SnapshotStampedWord(int checkpointTag) => (int)SerializationPhase.SNAPSHOT_STAMPED | (checkpointTag << PhaseBits);

        /// <summary>The phase word a capture by the checkpoint tagged <paramref name="checkpointTag"/> leaves.</summary>
        internal static int CapturedWord(int checkpointTag) => (int)SerializationPhase.CAPTURED | (checkpointTag << PhaseBits);

        static SerializationPhase PhaseOf(int word) => (SerializationPhase)(word & PhaseMask);

        static bool IsSupersededWord(int word) => PhaseOf(word) is SerializationPhase.SERIALIZED or SerializationPhase.CAPTURED;

        /// <summary>Whether <paramref name="word"/> says the Snapshot flush of the checkpoint tagged <paramref name="checkpointTag"/> published the record.</summary>
        internal static bool IsSnapshotStampedBy(int word, int checkpointTag) => word == SnapshotStampedWord(checkpointTag);

        /// <summary>Whether <paramref name="word"/> says a ReadOnly flush published the record.</summary>
        internal static bool IsReadOnlyStamped(int word) => word == ReadOnlyStampedWord;

        /// <summary>Whether <paramref name="word"/> says the checkpoint tagged <paramref name="checkpointTag"/> captured the object.</summary>
        internal static bool IsCapturedBy(int word, int checkpointTag) => word == CapturedWord(checkpointTag);

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
        /// <remarks>Compares and sets the whole phase word, so it matches only a phase that carries no checkpoint tag.</remarks>
        public bool MakeTransition(SerializationPhase expectedPhase, SerializationPhase nextPhase)
            => Interlocked.CompareExchange(ref serializationPhaseInt, (int)nextPhase, (int)expectedPhase) == (int)expectedPhase;

        /// <summary>Wait until no thread holds this object's claim, then take it unless the object is superseded.</summary>
        /// <param name="priorWord">The phase word found. Pass it, or the word for what the claim published, to <see cref="ReleaseClaim"/>.</param>
        /// <returns>True if this thread now holds the claim. False if the object is superseded; it is never claimed again, because a
        /// CopyUpdate's new record shares its collections and may already have changed them.</returns>
        /// <remarks>Waits by yielding, as every claim wait on this object does: a claim is held across an object-log write, which can take
        /// as long as serializing the object.</remarks>
        internal bool TryClaim(out int priorWord)
        {
            while (true)
            {
                var word = Volatile.Read(ref serializationPhaseInt);
                if (PhaseOf(word) == SerializationPhase.SERIALIZING)
                {
                    _ = Thread.Yield();
                    continue;
                }
                priorWord = word;
                if (IsSupersededWord(word))
                    return false;
                if (Interlocked.CompareExchange(ref serializationPhaseInt, (int)SerializationPhase.SERIALIZING, word) == word)
                    return true;
            }
        }

        /// <summary>End a claim taken by <see cref="TryClaim"/>, leaving the phase word <paramref name="word"/>.</summary>
        internal void ReleaseClaim(int word)
        {
            Debug.Assert(PhaseOf(Volatile.Read(ref serializationPhaseInt)) == SerializationPhase.SERIALIZING, "ReleaseClaim without a claim");
            Volatile.Write(ref serializationPhaseInt, word);
        }

        /// <summary>While this thread writes <paramref name="claimedObject"/>, whose claim it holds, to an object log, let
        /// <see cref="Serialize"/> run under that claim.</summary>
        internal static void BeginClaimedWrite(HeapObjectBase claimedObject) => publishingObject = claimedObject;

        /// <summary>End the write begun by <see cref="BeginClaimedWrite"/>.</summary>
        internal static void EndClaimedWrite() => publishingObject = null;

        /// <inheritdoc />
        public abstract void Dispose();

        /// <inheritdoc />
        public abstract void WriteType(BinaryWriter writer, bool isNull);

        /// <inheritdoc />
        public void Serialize(BinaryWriter writer)
        {
            // Called back by the value serializer inside a flush's or a capture's object-log write: that writer holds the claim, and never
            // claims a superseded object.
            if (ReferenceEquals(publishingObject, this))
            {
                WriteType(writer, isNull: false);
                DoSerialize(writer);
                return;
            }

            while (true)
            {
                var word = Volatile.Read(ref serializationPhaseInt);
                if (PhaseOf(word) == SerializationPhase.SERIALIZING)
                {
                    // A flush, a capture, CacheSerializedObjectData or another Serialize holds the claim.
                    _ = Thread.Yield();
                    continue;
                }

                if (IsSupersededWord(word))
                {
                    // A CopyUpdate superseded this object, and its new record may have changed the collections this object shares with
                    // it. Write the (v) bytes CacheSerializedObjectData cached, if any; otherwise a null value: no checkpoint needed the
                    // (v) image, a checkpoint has completed and cleared it, or a Snapshot checkpoint captured it into its own object log.
                    var serialized = serializedBytes;
                    if (serialized != null)
                    {
                        WriteType(writer, isNull: false);
                        writer.Write(serialized);
                    }
                    else
                        WriteType(writer, isNull: true);
                    return;
                }

                // Claim the object so no CopyUpdate changes it while it is written, and restore the phase it had: this serialization
                // publishes nothing. Restore it even if DoSerialize throws, or every later claim would wait forever.
                if (Interlocked.CompareExchange(ref serializationPhaseInt, (int)SerializationPhase.SERIALIZING, word) != word)
                    continue;
                try
                {
                    WriteType(writer, isNull: false);
                    DoSerialize(writer);
                }
                finally
                {
                    Volatile.Write(ref serializationPhaseInt, word);
                }
                return;
            }
        }

        /// <inheritdoc />
        public bool CacheSerializedObjectData(ref LogRecord dstLogRecord, bool srcIsOnMemoryLog, bool checkpointNeedsSourceImage)
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
            //     driving its SerializationPhase and allocating serializedBytes would be pure waste.
            if (!srcIsOnMemoryLog)
                return false;

            // No (v) image is at stake. Outside a checkpoint the new record is in the source's version, so it supersedes the source in
            // every later checkpoint, and no completed checkpoint recovers a source that a flush is still writing: a flush that has not
            // serialized the source writes a null value for it. Within a checkpoint, either the checkpoint does not capture this source (it
            // was written after the transaction start), or a Snapshot checkpoint finds its (v) value elsewhere: its main-log image is durable,
            // the source is a read-cache copy of a durable main-log record, or the checkpoint has sampled the end of its main-log range. The
            // CopyUpdate releases the old version rather than serializing it; the new version already exists.
            if (!dstLogRecord.Info.IsInNewVersion || !checkpointNeedsSourceImage)
            {
                MarkSuperseded();
                return false;
            }

            // Create a serialized version for checkpoint version (v). This is only done for CopyUpdate during a checkpoint, to preserve the (v) data
            // of the object during a checkpoint while the (v+1) version of the record may modify the shallow-copied internal structures.
            // CopyUpdate holds the tag chain's lock, so no other CopyUpdate runs on this object; a flush or a Serialize may hold the claim,
            // which TryClaim waits out. An object already superseded has its final bytes, or none.
            if (!TryClaim(out var priorWord))
                return true;
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
                ReleaseClaim(priorWord);
                throw;
            }

            ReleaseClaim(SupersededWord);    // The only place a superseded object gets cached bytes
            return true;
        }

        /// <summary>
        /// Finish a Snapshot checkpoint's capture of this object as a CopyUpdate source: give the new record its clone, and make sure this object
        /// is superseded. The capture has already set its final phase unless it failed before claiming the object.
        /// </summary>
        /// <remarks>
        /// The capture wrote this object's (v) value to the checkpoint's object log, so a later flush of the superseded record writes the null
        /// image rather than the live object, whose collections the clone shares and the (v+1) record goes on to mutate. The source keeps its
        /// value object, so a flush that walks the record still finds its components.
        /// </remarks>
        /// <param name="dstLogRecord">The CopyUpdate's new record.</param>
        internal void CompleteCapture(ref LogRecord dstLogRecord)
        {
            Debug.Assert(dstLogRecord.DataHeader.ValueIsObject, $"{GetCurrentMethodName()} must be called for non-object {nameof(dstLogRecord)}.");
            if (!dstLogRecord.ValueObjectIsSet)
                _ = dstLogRecord.TrySetValueObject(Clone());
            MarkSuperseded();
        }

        /// <summary>Move to the final serialization phase without caching bytes, once any claim in progress ends.</summary>
        void MarkSuperseded()
        {
            while (true)
            {
                var word = Volatile.Read(ref serializationPhaseInt);
                if (IsSupersededWord(word))
                    return;
                if (PhaseOf(word) != SerializationPhase.SERIALIZING
                        && Interlocked.CompareExchange(ref serializationPhaseInt, SupersededWord, word) == word)
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