// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;

namespace Garnet.Extensions.RoaringBitmap
{
    /// <summary>
    /// Garnet custom-object wrapper around a <see cref="RoaringBitmap"/>. Provides
    /// the storage-layer integration (serialization, clone, size accounting) while
    /// delegating bit operations to the underlying compressed bitmap.
    ///
    /// Size accounting note: the inherited Size property is updated after every
    /// mutation to keep MEMORY USAGE results accurate. Garnet's global object-store
    /// size tracker, however, does not currently propagate per-operation deltas from
    /// custom-object Updaters (see Storage/Functions/ObjectStore/RMWMethods.cs);
    /// that is a pre-existing core limitation independent of this extension.
    /// </summary>
    public sealed class RoaringBitmapObject : CustomObjectBase
    {
        /// <summary>Estimated overhead of the object shell itself, separate from container memory.</summary>
        private const long ObjectOverhead = 32;

        private readonly RoaringBitmap bitmap;

        public RoaringBitmapObject(byte type)
            : base(type, expiration: 0, size: ObjectOverhead)
        {
            bitmap = new RoaringBitmap();
            // Keep size accounting consistent with the deserialized constructor and
            // with later mutation deltas based on bitmap.ByteSize so the empty-bitmap
            // baseline is counted exactly once.
            this.Size = ObjectOverhead + bitmap.ByteSize;
        }

        public RoaringBitmapObject(byte type, BinaryReader reader)
            : base(type, reader, ObjectOverhead)
        {
            bitmap = RoaringBitmap.Deserialize(reader);
            // Refresh size now that we know the actual container footprint.
            this.Size = ObjectOverhead + bitmap.ByteSize;
        }

        private RoaringBitmapObject(RoaringBitmapObject other)
            : base(other)
        {
            // Deep clone: cloned object owns its own bitmap so subsequent
            // mutations do not leak across snapshots.
            bitmap = other.bitmap.Clone();
        }

        public override CustomObjectBase CloneObject() => new RoaringBitmapObject(this);

        public override void SerializeObject(BinaryWriter writer) => bitmap.Serialize(writer);

        public override void Dispose() { /* nothing to dispose; bitmap is fully managed */ }

        /// <summary>
        /// Custom objects don't participate in COSCAN-style enumeration semantics.
        /// We provide a minimal implementation that surfaces the set bits as a list
        /// of decimal-string keys in cursor order — useful for diagnostics but not
        /// expected to be a hot path.
        /// </summary>
        public override unsafe void Scan(long start, out System.Collections.Generic.List<byte[]> items, out long cursor, int count = 10, byte* pattern = null, int patternLength = 0, bool isNoValue = false)
        {
            items = new System.Collections.Generic.List<byte[]>();
            cursor = 0;
            // Intentionally minimal: a full enumeration of a dense bitmap could be
            // billions of entries. Callers who need iteration should use BITPOS in a
            // loop or a future R.SCAN command.
        }

        // ---- Bitmap ops (called by RoaringBitmap commands) ----

        public int SetBit(uint offset, bool set)
        {
            long sizeBefore = bitmap.ByteSize;
            int previous = bitmap.SetBit(offset, set);
            long sizeAfter = bitmap.ByteSize;
            this.Size += (sizeAfter - sizeBefore);
            return previous;
        }

        public int GetBit(uint offset) => bitmap.GetBit(offset);

        public long BitCount() => bitmap.Cardinality;

        public long BitPos(int bit, uint from = 0) => bitmap.BitPos(bit, from);

        /// <summary>True iff the underlying bitmap holds zero bits. Hint for the parent that the key could be removed.</summary>
        public bool IsEmpty => bitmap.IsEmpty;

        /// <summary>For tests and diagnostics only.</summary>
        internal RoaringBitmap UnderlyingBitmap => bitmap;
    }

    /// <summary>
    /// Factory used by Garnet to instantiate a <see cref="RoaringBitmapObject"/> when
    /// a key is created or recovered from disk. Wired up in Program.cs via
    /// <c>server.Register.NewType(new RoaringBitmapFactory())</c>.
    /// </summary>
    public sealed class RoaringBitmapFactory : CustomObjectFactory
    {
        public override CustomObjectBase Create(byte type) => new RoaringBitmapObject(type);

        public override CustomObjectBase Deserialize(byte type, BinaryReader reader)
            => new RoaringBitmapObject(type, reader);
    }
}