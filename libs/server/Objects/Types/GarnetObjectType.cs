// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Type of Garnet object
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
        SortedSet,
        /// <summary>
        /// List
        /// </summary>
        List,
        /// <summary>
        /// Hash
        /// </summary>
        Hash,
        /// <summary>
        /// Set
        /// </summary>        
        Set,

        // Any new object type inserted here should update GarnetObjectTypeExtensions.LastObjectType

        // Any new special type inserted here should update GarnetObjectTypeExtensions.FirstSpecialObjectType

        /// <summary>
        /// Indicating a Custom Object command
        /// </summary>
        All = 0xfb,
    }

    public static class GarnetObjectTypeExtensions
    {
        internal const GarnetObjectType LastObjectType = GarnetObjectType.Set;

        internal const GarnetObjectType FirstSpecialObjectType = GarnetObjectType.All;
    }
}