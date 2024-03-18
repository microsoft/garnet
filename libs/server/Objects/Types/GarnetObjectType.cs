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
        /// Null type (placeholder)
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

        /// <summary>
        /// Special type indicating PERSIT command
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
}