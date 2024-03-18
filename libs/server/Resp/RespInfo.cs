// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace Garnet.server
{
    /// <summary>
    /// Info on what is supported by server
    /// </summary>
    public static class RespInfo
    {
        /// <summary>
        /// Get set of RESP commands supported by Garnet server
        /// </summary>
        /// <returns></returns>
        public static HashSet<string> GetCommands()
        {
            return new HashSet<string> {
                // Admin ops
                "PING", "QUIT", "CLIENT", "SUBSCRIBE", "CONFIG", "ECHO", "INFO", "CLUSTER", "TIME", "RESET", "AUTH", "SELECT", "COMMAND", "MIGRATE", "ASKING", "LATENCY", "COMMITAOF", "FLUSHDB", "READONLY", "REPLICAOF", "MEMORY", "MONITOR", "TYPE", "MODULE", "REGISTERCS",
                // Basic ops
                "GET", "GETRANGE", "SET", "MGET", "MSET", "MSETNX", "SETRANGE", "GETSET", "PSETEX", "SETEX", "DEL", "UNLINK", "EXISTS", "RENAME", "EXPIRE", "PEXPIRE", "PERSIST", "TTL", "PTTL", "STRLEN", "GETDEL", "APPEND",
                // Number ops
                "INCR", "INCRBY", "DECR", "DECRBY",
                // Checkpointing
                "SAVE", "LASTSAVE", "BGSAVE", "BGREWRITEAOF",
                // Sorted Set
                "ZADD", "ZCARD", "ZPOPMAX", "ZSCORE", "ZREM", "ZCOUNT", "ZINCRBY", "ZRANK", "ZRANGE", "ZRANGEBYSCORE", "ZREVRANGE", "ZREVRANK", "ZREMRANGEBYLEX", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "ZLEXCOUNT", "ZPOPMIN", "ZRANDMEMBER", "ZDIFF", "ZSCAN",
                // List
                "LPOP", "LPUSH", "RPOP", "RPUSH", "LLEN", "LTRIM", "LRANGE", "LINDEX", "LINSERT", "LREM", "RPOPLPUSH", "LMOVE", "LPUSHX", "RPUSHX",
                // Hash
                "HSET", "HGET", "HMGET", "HMSET", "HDEL", "HLEN", "HEXISTS", "HGETALL", "HKEYS", "HVALS", "HINCRBY", "HINCRBYFLOAT", "HSETNX", "HRANDFIELD", "HSCAN",
                // Hyperloglog
                "PFADD", "PFCOUNT", "PFMERGE",
                // Bitmap
                "SETBIT", "GETBIT", "BITCOUNT","BITPOS", "BITOP", "BITFIELD",
                // Pub/sub
                "PUBLISH", "SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE",
                // Set
                "SADD", "SREM", "SPOP", "SMEMBERS", "SCARD", "SSCAN",
                //Scan ops
                "DBSIZE", "KEYS","SCAN",
                // Geospatial commands
                "GEOADD", "GEOHASH", "GEODIST", "GEOPOS", "GEOSEARCH",
                // Transactions: TODO
                "WATCH", "UNWATCH", "MULTI", "EXEC", "DISCARD",
            };
        }
    }
}