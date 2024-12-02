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
        /// Special type indicating PEXPIRE command
        /// </summary>
        PExpire = 0xf8,

        /// <summary>
        /// Special type indicating EXPIRETIME command
        /// </summary>
        ExpireTime = 0xf9,

        /// <summary>
        /// Special type indicating PEXPIRETIME command
        /// </summary>
        PExpireTime = 0xfa,

        /// <summary>
        /// Special type indicating PERSIST command
        /// </summary>
        Persist = 0xfd,

        /// <summary>
        /// Special type indicating TTL command
        /// </summary>
        Ttl = 0xfe,

        /// <summary>
        /// Special type indicating EXPIRE command
        /// </summary>
        Expire = 0xff,

        /// <summary>
        /// Special type indicating PTTL command
        /// </summary>
        PTtl = 0xfc,

        /// <summary>
        /// Indicating a Custom Object command
        /// </summary>
        All = 0xfb
    }

    public static class GarnetObjectTypeExtensions
    {
        internal const GarnetObjectType LastObjectType = GarnetObjectType.Set;

        internal const GarnetObjectType FirstSpecialObjectType = GarnetObjectType.PExpire;
    }
}