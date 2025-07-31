// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    /// <summary>
    /// Extensions
    /// </summary>
    public static class GarnetObject
    {
        /// <summary>
        /// Create initial value of object
        /// </summary>
        /// <param name="garnetObjectType"></param>
        /// <returns></returns>
        internal static IGarnetObject Create(GarnetObjectType garnetObjectType, long expiration = 0)
        {
            return garnetObjectType switch
            {
                GarnetObjectType.SortedSet => new SortedSetObject(expiration),
                GarnetObjectType.List => new ListObject(expiration),
                GarnetObjectType.Hash => new HashObject(expiration),
                GarnetObjectType.Set => new SetObject(expiration),
                _ => throw new Exception("Unsupported data type"),
            };
        }

        /// <summary>
        /// Check if object creation is necessary
        /// </summary>
        /// <returns></returns>
        internal static bool NeedToCreate(RespInputHeader header)
        {
            return header.type switch
            {
                GarnetObjectType.SortedSet => header.SortedSetOp switch
                {
                    SortedSetOperation.ZREM => false,
                    SortedSetOperation.ZPOPMIN => false,
                    SortedSetOperation.ZPOPMAX => false,
                    SortedSetOperation.ZREMRANGEBYLEX => false,
                    SortedSetOperation.ZREMRANGEBYSCORE => false,
                    SortedSetOperation.ZREMRANGEBYRANK => false,
                    SortedSetOperation.ZEXPIRE => false,
                    SortedSetOperation.ZCOLLECT => false,
                    _ => true,
                },
                GarnetObjectType.List => header.ListOp switch
                {
                    ListOperation.LPOP => false,
                    ListOperation.RPOP => false,
                    ListOperation.LRANGE => false,
                    ListOperation.LINDEX => false,
                    ListOperation.LTRIM => false,
                    ListOperation.LREM => false,
                    ListOperation.LINSERT => false,
                    ListOperation.LPUSHX => false,
                    ListOperation.RPUSHX => false,
                    _ => true,
                },
                GarnetObjectType.Set => header.SetOp switch
                {
                    SetOperation.SCARD => false,
                    SetOperation.SMEMBERS => false,
                    SetOperation.SREM => false,
                    SetOperation.SPOP => false,
                    _ => true,
                },
                GarnetObjectType.Hash => header.HashOp switch
                {
                    HashOperation.HEXPIRE => false,
                    HashOperation.HCOLLECT => false,
                    _ => true,
                },
                GarnetObjectType.Expire => false,
                GarnetObjectType.Persist => false,
                _ => true,
            };
        }
    }
}