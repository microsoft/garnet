// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;

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
                GarnetObjectType.Expire => false,
                GarnetObjectType.Persist => false,
                _ => true,
            };
        }

        /// <summary>
        /// Create an IGarnetObject from an input array.
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public static IGarnetObject Create(byte[] data)
        {
            using var ms = new MemoryStream(data);
            using var reader = new BinaryReader(ms);
            var type = (GarnetObjectType)reader.ReadByte();
            return type switch
            {
                GarnetObjectType.SortedSet => new SortedSetObject(reader, expiration),
                GarnetObjectType.List => new ListObject(reader, expiration),
                GarnetObjectType.Hash => new HashObject(reader, expiration),
                GarnetObjectType.Set => new SetObject(reader, expiration),
                _ => throw new Exception("Unsupported data type"),
            };
        }
    }
}