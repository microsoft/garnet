// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.Extensions.RoaringBitmap.Containers
{
    /// <summary>
    /// Container kinds used in serialization. Stable on-disk values — never renumber.
    /// </summary>
    internal enum ContainerKind : byte
    {
        Array = 1,
        Bitmap = 2,
        Run = 3,
    }

    /// <summary>
    /// Represents a 16-bit-key container holding a subset of the 65,536 values
    /// addressable by the low 16 bits of a uint32 element. The high 16 bits of
    /// each element are encoded by the container's position in the parent
    /// <see cref="RoaringBitmap"/>'s chunk dictionary, not by the container itself.
    ///
    /// All container types are mutable. Mutating operations may return a different
    /// container instance when the optimal representation changes (e.g., an
    /// <see cref="ArrayContainer"/> grown beyond the array threshold returns a
    /// <see cref="BitmapContainer"/>). Callers MUST replace their reference with
    /// the returned container after every mutation.
    /// </summary>
    internal interface IContainer
    {
        /// <summary>Container kind for serialization.</summary>
        ContainerKind Kind { get; }

        /// <summary>Number of bits set in this container. Always in [1, 65536]. Empty containers are removed by the parent.</summary>
        int Cardinality { get; }

        /// <summary>True if the bit at the given low 16-bit position is set.</summary>
        bool Contains(ushort value);

        /// <summary>
        /// Add <paramref name="value"/>. Returns the (possibly new) container reference and sets
        /// <paramref name="added"/> to true iff this is a new element.
        /// </summary>
        IContainer Add(ushort value, out bool added);

        /// <summary>
        /// Remove <paramref name="value"/>. Returns the (possibly new) container reference and sets
        /// <paramref name="removed"/> to true iff the element existed. Returns null when the
        /// container becomes empty (caller must remove the entry from the chunk map).
        /// </summary>
        IContainer Remove(ushort value, out bool removed);

        /// <summary>First low-16-bit value in the container in ascending order. Cardinality must be > 0.</summary>
        ushort First();

        /// <summary>Last low-16-bit value in the container in ascending order. Cardinality must be > 0.</summary>
        ushort Last();

        /// <summary>
        /// Returns the position of the next low-16-bit value &gt;= <paramref name="from"/> that is set,
        /// or -1 if no such value exists in this container.
        /// </summary>
        int NextSetBit(int from);

        /// <summary>
        /// Returns the position of the next low-16-bit value &gt;= <paramref name="from"/> that is NOT set,
        /// or -1 if no such position exists in [from, 65535].
        /// </summary>
        int NextUnsetBit(int from);

        /// <summary>Estimated heap byte cost of this container (excluding object header overhead).</summary>
        long ByteSize { get; }

        /// <summary>Deep clone.</summary>
        IContainer Clone();

        /// <summary>Serialize the container body (NOT including kind discriminator or cardinality — caller writes those).</summary>
        void SerializeBody(BinaryWriter writer);
    }
}