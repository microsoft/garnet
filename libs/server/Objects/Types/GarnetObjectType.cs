// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Type of Garnet object.
    /// This value is the first byte of every serialized object (checkpoints/object log) and is also
    /// carried in RespInputHeader.type, so it is PERSISTED. Values are EXPLICIT and permanent:
    ///   * Built-in type ids (0..<see cref="GarnetObjectTypeExtensions.LastReservedBuiltinType"/>)
    ///     must never change; add a new built-in with the next unused value in that reserved band.
    ///   * Custom object type ids are assigned dynamically from a FIXED base
    ///     (<see cref="CustomCommandManager"/>), independent of the built-in count, so adding a
    ///     built-in type never shifts persisted custom-object ids.
    ///   * 0xFC..0xFF are reserved for a future object-format version/escape byte and must never be
    ///     used as a type id.
    /// </summary>
    public enum GarnetObjectType : byte
    {
        /// <summary>
        /// Null type
        /// </summary>
        Null = 0,
        /// <summary>
        /// Sorted set
        /// </summary>
        SortedSet = 1,
        /// <summary>
        /// List
        /// </summary>
        List = 2,
        /// <summary>
        /// Hash
        /// </summary>
        Hash = 3,
        /// <summary>
        /// Set
        /// </summary>
        Set = 4,

        // Built-in types occupy the reserved band [0, LastReservedBuiltinType]. Add a new built-in
        // with the next value in that band and update GarnetObjectTypeExtensions.LastObjectType. Do
        // NOT insert before existing built-ins (values are persisted). Custom object types live in a
        // fixed range (CustomCommandManager) that does not move when built-ins are added.

        /// <summary>
        /// Indicating a Custom Object command
        /// </summary>
        All = 0xfb,

        // 0xFC..0xFF are reserved for a future object-serialization format version/escape byte
        // (see GarnetObjectSerializer); never assign them as an object type id.
    }

    public static class GarnetObjectTypeExtensions
    {
        /// <summary>
        /// Highest currently-assigned built-in object type.
        /// </summary>
        internal const GarnetObjectType LastObjectType = GarnetObjectType.Set;

        /// <summary>
        /// Inclusive upper bound of the reserved built-in type band. Built-in types may grow up to
        /// this value without shifting the fixed custom-object base.
        /// </summary>
        internal const GarnetObjectType LastReservedBuiltinType = (GarnetObjectType)0x3F;

        internal const GarnetObjectType FirstSpecialObjectType = GarnetObjectType.All;

        /// <summary>
        /// First reserved value for a future object-serialization format version/escape byte. Any
        /// serialized object whose first byte is >= this value is not a legacy [type][data] record
        /// and must be handled by a versioned reader (or rejected fail-fast).
        /// </summary>
        internal const byte ReservedObjectFormatByteStart = 0xFC;
    }
}