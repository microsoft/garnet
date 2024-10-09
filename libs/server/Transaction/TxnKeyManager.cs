// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class TransactionManager
    {
        /// <summary>
        /// Save key entry
        /// </summary>
        /// <param name="key"></param>
        /// <param name="isObject"></param>
        /// <param name="type"></param>
        public void SaveKeyEntryToLock(ArgSlice key, bool isObject, LockType type)
        {
            UpdateTransactionStoreType(isObject ? StoreType.Object : StoreType.Main);
            keyEntries.AddKey(key, isObject, type);
        }

        /// <summary>
        /// Verify key ownership
        /// </summary>
        /// <param name="key"></param>
        /// <param name="type"></param>
        public unsafe void VerifyKeyOwnership(ArgSlice key, LockType type)
        {
            if (!clusterEnabled) return;

            bool readOnly = type == LockType.Shared;
            if (!respSession.clusterSession.CheckSingleKeySlotVerify(key, readOnly, respSession.SessionAsking))
            {
                this.state = TxnState.Aborted;
                return;
            }
        }

        /// <summary>
        /// Returns a number of skipped args
        /// </summary>
        internal int GetKeys(RespCommand command, int inputCount, out ReadOnlySpan<byte> error)
        {
            error = CmdStrings.RESP_ERR_GENERIC_UNK_CMD;
            return command switch
            {
                RespCommand.SADD => SetObjectKeys(SetOperation.SADD, inputCount),
                RespCommand.SREM => SetObjectKeys(SetOperation.SREM, inputCount),
                RespCommand.SPOP => SetObjectKeys(SetOperation.SPOP, inputCount),
                RespCommand.SMEMBERS => SetObjectKeys(SetOperation.SMEMBERS, inputCount),
                RespCommand.SCARD => SetObjectKeys(SetOperation.SCARD, inputCount),
                RespCommand.SINTER => SetObjectKeys(SetOperation.SINTER, inputCount),
                RespCommand.SINTERSTORE => SetObjectKeys(SetOperation.SINTERSTORE, inputCount),
                RespCommand.SSCAN => SetObjectKeys(SetOperation.SSCAN, inputCount),
                RespCommand.SMOVE => SetObjectKeys(SetOperation.SMOVE, inputCount),
                RespCommand.SRANDMEMBER => SetObjectKeys(SetOperation.SRANDMEMBER, inputCount),
                RespCommand.SISMEMBER => SetObjectKeys(SetOperation.SISMEMBER, inputCount),
                RespCommand.SUNION => SetObjectKeys(SetOperation.SUNION, inputCount),
                RespCommand.SUNIONSTORE => SetObjectKeys(SetOperation.SUNIONSTORE, inputCount),
                RespCommand.SDIFF => SetObjectKeys(SetOperation.SDIFF, inputCount),
                RespCommand.SDIFFSTORE => SetObjectKeys(SetOperation.SDIFFSTORE, inputCount),
                RespCommand.ZADD => SortedSetObjectKeys(SortedSetOperation.ZADD, inputCount),
                RespCommand.ZREM => SortedSetObjectKeys(SortedSetOperation.ZREM, inputCount),
                RespCommand.ZCARD => SortedSetObjectKeys(SortedSetOperation.ZCARD, inputCount),
                RespCommand.ZPOPMAX => SortedSetObjectKeys(SortedSetOperation.ZPOPMAX, inputCount),
                RespCommand.ZSCORE => SortedSetObjectKeys(SortedSetOperation.ZSCORE, inputCount),
                RespCommand.ZMSCORE => SortedSetObjectKeys(SortedSetOperation.ZMSCORE, inputCount),
                RespCommand.ZCOUNT => SortedSetObjectKeys(SortedSetOperation.ZCOUNT, inputCount),
                RespCommand.ZINCRBY => SortedSetObjectKeys(SortedSetOperation.ZINCRBY, inputCount),
                RespCommand.ZRANK => SortedSetObjectKeys(SortedSetOperation.ZRANK, inputCount),
                RespCommand.ZRANGE => SortedSetObjectKeys(SortedSetOperation.ZRANGE, inputCount),
                RespCommand.ZRANGEBYSCORE => SortedSetObjectKeys(SortedSetOperation.ZRANGEBYSCORE, inputCount),
                RespCommand.ZREVRANK => SortedSetObjectKeys(SortedSetOperation.ZREVRANK, inputCount),
                RespCommand.ZREMRANGEBYLEX => SortedSetObjectKeys(SortedSetOperation.ZREMRANGEBYLEX, inputCount),
                RespCommand.ZREMRANGEBYRANK => SortedSetObjectKeys(SortedSetOperation.ZREMRANGEBYRANK, inputCount),
                RespCommand.ZREMRANGEBYSCORE => SortedSetObjectKeys(SortedSetOperation.ZREMRANGEBYSCORE, inputCount),
                RespCommand.ZLEXCOUNT => SortedSetObjectKeys(SortedSetOperation.ZLEXCOUNT, inputCount),
                RespCommand.ZPOPMIN => SortedSetObjectKeys(SortedSetOperation.ZPOPMIN, inputCount),
                RespCommand.ZRANDMEMBER => SortedSetObjectKeys(SortedSetOperation.ZRANDMEMBER, inputCount),
                RespCommand.ZDIFF => SortedSetObjectKeys(SortedSetOperation.ZDIFF, inputCount),
                RespCommand.GEOADD => SortedSetObjectKeys(SortedSetOperation.GEOADD, inputCount),
                RespCommand.GEOHASH => SortedSetObjectKeys(SortedSetOperation.GEOHASH, inputCount),
                RespCommand.GEODIST => SortedSetObjectKeys(SortedSetOperation.GEODIST, inputCount),
                RespCommand.GEOPOS => SortedSetObjectKeys(SortedSetOperation.GEOPOS, inputCount),
                RespCommand.GEOSEARCH => SortedSetObjectKeys(SortedSetOperation.GEOSEARCH, inputCount),
                RespCommand.ZREVRANGE => SortedSetObjectKeys(SortedSetOperation.ZREVRANGE, inputCount),
                RespCommand.ZREVRANGEBYSCORE => SortedSetObjectKeys(SortedSetOperation.ZREVRANGEBYSCORE, inputCount),
                RespCommand.LINDEX => ListObjectKeys((byte)ListOperation.LINDEX),
                RespCommand.LINSERT => ListObjectKeys((byte)ListOperation.LINSERT),
                RespCommand.LLEN => ListObjectKeys((byte)ListOperation.LLEN),
                RespCommand.LMOVE => ListObjectKeys((byte)ListOperation.LMOVE),
                RespCommand.LMPOP => ListKeys(true, LockType.Exclusive),
                RespCommand.LPOP => ListObjectKeys((byte)ListOperation.LPOP),
                RespCommand.LPUSH => ListObjectKeys((byte)ListOperation.LPUSH),
                RespCommand.LPUSHX => ListObjectKeys((byte)ListOperation.LPUSHX),
                RespCommand.LRANGE => ListObjectKeys((byte)ListOperation.LRANGE),
                RespCommand.LREM => ListObjectKeys((byte)ListOperation.LREM),
                RespCommand.LSET => ListObjectKeys((byte)ListOperation.LSET),
                RespCommand.LTRIM => ListObjectKeys((byte)ListOperation.LTRIM),
                RespCommand.RPOP => ListObjectKeys((byte)ListOperation.RPOP),
                RespCommand.RPUSH => ListObjectKeys((byte)ListOperation.RPUSH),
                RespCommand.RPOPLPUSH => ListObjectKeys((byte)ListOperation.RPOPLPUSH),
                RespCommand.RPUSHX => ListObjectKeys((byte)ListOperation.RPUSHX),
                RespCommand.HDEL => HashObjectKeys((byte)HashOperation.HDEL),
                RespCommand.HEXISTS => HashObjectKeys((byte)HashOperation.HEXISTS),
                RespCommand.HGET => HashObjectKeys((byte)HashOperation.HGET),
                RespCommand.HGETALL => HashObjectKeys((byte)HashOperation.HGETALL),
                RespCommand.HINCRBY => HashObjectKeys((byte)HashOperation.HINCRBY),
                RespCommand.HINCRBYFLOAT => HashObjectKeys((byte)HashOperation.HINCRBYFLOAT),
                RespCommand.HKEYS => HashObjectKeys((byte)HashOperation.HKEYS),
                RespCommand.HLEN => HashObjectKeys((byte)HashOperation.HLEN),
                RespCommand.HMGET => HashObjectKeys((byte)HashOperation.HMGET),
                RespCommand.HMSET => HashObjectKeys((byte)HashOperation.HMSET),
                RespCommand.HRANDFIELD => HashObjectKeys((byte)HashOperation.HRANDFIELD),
                RespCommand.HSCAN => HashObjectKeys((byte)HashOperation.HSCAN),
                RespCommand.HSET => HashObjectKeys((byte)HashOperation.HSET),
                RespCommand.HSETNX => HashObjectKeys((byte)HashOperation.HSETNX),
                RespCommand.HSTRLEN => HashObjectKeys((byte)HashOperation.HSTRLEN),
                RespCommand.HVALS => HashObjectKeys((byte)HashOperation.HVALS),
                RespCommand.GET => SingleKey(1, false, LockType.Shared),
                RespCommand.SET => SingleKey(1, false, LockType.Exclusive),
                RespCommand.GETRANGE => SingleKey(1, false, LockType.Shared),
                RespCommand.SETRANGE => SingleKey(1, false, LockType.Exclusive),
                RespCommand.PFADD => SingleKey(1, false, LockType.Exclusive),
                RespCommand.PFCOUNT => ListKeys(inputCount, false, LockType.Shared),
                RespCommand.PFMERGE => ListKeys(inputCount, false, LockType.Exclusive),
                RespCommand.SETEX => SingleKey(1, false, LockType.Exclusive),
                RespCommand.SETEXNX => SingleKey(1, false, LockType.Exclusive),
                RespCommand.SETEXXX => SingleKey(1, false, LockType.Exclusive),
                RespCommand.DEL => ListKeys(inputCount, false, LockType.Exclusive),
                RespCommand.EXISTS => SingleKey(1, false, LockType.Shared),
                RespCommand.RENAME => SingleKey(1, false, LockType.Exclusive),
                RespCommand.INCR => SingleKey(1, false, LockType.Exclusive),
                RespCommand.INCRBY => SingleKey(1, false, LockType.Exclusive),
                RespCommand.DECR => SingleKey(1, false, LockType.Exclusive),
                RespCommand.DECRBY => SingleKey(1, false, LockType.Exclusive),
                RespCommand.SETBIT => SingleKey(1, false, LockType.Exclusive),
                RespCommand.GETBIT => SingleKey(1, false, LockType.Shared),
                RespCommand.BITCOUNT => SingleKey(1, false, LockType.Shared),
                RespCommand.BITPOS => SingleKey(1, false, LockType.Exclusive),
                RespCommand.BITFIELD => SingleKey(1, false, LockType.Exclusive),
                RespCommand.EXPIRE => SingleKey(1, false, LockType.Exclusive),
                RespCommand.PEXPIRE => SingleKey(1, false, LockType.Exclusive),
                RespCommand.PERSIST => SingleKey(1, false, LockType.Exclusive),
                RespCommand.MGET => ListKeys(inputCount, false, LockType.Shared),
                RespCommand.MSET => MSETKeys(inputCount, false, LockType.Exclusive),
                RespCommand.MSETNX => MSETKeys(inputCount, false, LockType.Exclusive),
                RespCommand.UNLINK => ListKeys(inputCount, false, LockType.Exclusive),
                RespCommand.GETDEL => SingleKey(1, false, LockType.Exclusive),
                RespCommand.APPEND => SingleKey(1, false, LockType.Exclusive),
                _ => AdminCommands(command)
            };
        }

        private static int AdminCommands(RespCommand command)
        {
            return command switch
            {
                RespCommand.ECHO => 1,
                RespCommand.REPLICAOF => 1,
                RespCommand.SECONDARYOF => 1,
                RespCommand.CONFIG => 1,
                RespCommand.CLIENT => 1,
                RespCommand.PING => 1,
                RespCommand.PUBLISH => 1,
                _ => -1
            };
        }

        private int SortedSetObjectKeys(SortedSetOperation command, int inputCount)
        {

            return command switch
            {
                SortedSetOperation.ZADD => SingleKey(1, true, LockType.Exclusive),
                SortedSetOperation.ZREM => SingleKey(1, true, LockType.Exclusive),
                SortedSetOperation.ZCARD => SingleKey(1, true, LockType.Shared),
                SortedSetOperation.ZPOPMAX => SingleKey(1, true, LockType.Exclusive),
                SortedSetOperation.ZSCORE => SingleKey(1, true, LockType.Shared),
                SortedSetOperation.ZMSCORE => SingleKey(1, true, LockType.Shared),
                SortedSetOperation.ZCOUNT => SingleKey(1, true, LockType.Shared),
                SortedSetOperation.ZINCRBY => SingleKey(1, true, LockType.Exclusive),
                SortedSetOperation.ZRANK => SingleKey(1, true, LockType.Exclusive),
                SortedSetOperation.ZRANGE => SingleKey(1, true, LockType.Shared),
                SortedSetOperation.ZRANGEBYSCORE => SingleKey(1, true, LockType.Shared),
                SortedSetOperation.ZREVRANK => SingleKey(1, true, LockType.Exclusive),
                SortedSetOperation.ZREMRANGEBYLEX => SingleKey(1, true, LockType.Exclusive),
                SortedSetOperation.ZREMRANGEBYRANK => SingleKey(1, true, LockType.Exclusive),
                SortedSetOperation.ZREMRANGEBYSCORE => SingleKey(1, true, LockType.Exclusive),
                SortedSetOperation.ZLEXCOUNT => SingleKey(1, true, LockType.Exclusive),
                SortedSetOperation.ZPOPMIN => SingleKey(1, true, LockType.Exclusive),
                SortedSetOperation.ZRANDMEMBER => SingleKey(1, true, LockType.Exclusive),
                SortedSetOperation.ZDIFF => ListKeys(inputCount, true, LockType.Exclusive),
                SortedSetOperation.GEOADD => SingleKey(1, true, LockType.Exclusive),
                SortedSetOperation.GEOHASH => SingleKey(1, true, LockType.Shared),
                SortedSetOperation.GEODIST => SingleKey(1, true, LockType.Shared),
                SortedSetOperation.GEOPOS => SingleKey(1, true, LockType.Shared),
                SortedSetOperation.GEOSEARCH => SingleKey(1, true, LockType.Shared),
                SortedSetOperation.ZREVRANGE => SingleKey(1, true, LockType.Shared),
                SortedSetOperation.ZREVRANGEBYSCORE => SingleKey(1, true, LockType.Shared),
                _ => -1
            };
        }

        private int ListObjectKeys(byte subCommand)
        {
            return subCommand switch
            {
                (byte)ListOperation.LPUSH => SingleKey(1, true, LockType.Exclusive),
                (byte)ListOperation.LPOP => SingleKey(1, true, LockType.Exclusive),
                (byte)ListOperation.RPUSH => SingleKey(1, true, LockType.Exclusive),
                (byte)ListOperation.RPOP => SingleKey(1, true, LockType.Exclusive),
                (byte)ListOperation.LLEN => SingleKey(1, true, LockType.Shared),
                (byte)ListOperation.LTRIM => SingleKey(1, true, LockType.Exclusive),
                (byte)ListOperation.LRANGE => SingleKey(1, true, LockType.Shared),
                (byte)ListOperation.LINDEX => SingleKey(1, true, LockType.Shared),
                (byte)ListOperation.LINSERT => SingleKey(1, true, LockType.Exclusive),
                (byte)ListOperation.LREM => SingleKey(1, true, LockType.Exclusive),
                (byte)ListOperation.LSET => SingleKey(1, true, LockType.Exclusive),
                _ => -1
            };
        }

        private int HashObjectKeys(byte subCommand)
        {
            return subCommand switch
            {
                (byte)HashOperation.HSET => SingleKey(1, true, LockType.Exclusive),
                (byte)HashOperation.HMSET => SingleKey(1, true, LockType.Exclusive),
                (byte)HashOperation.HGET => SingleKey(1, true, LockType.Shared),
                (byte)HashOperation.HMGET => SingleKey(1, true, LockType.Shared),
                (byte)HashOperation.HGETALL => SingleKey(1, true, LockType.Shared),
                (byte)HashOperation.HDEL => SingleKey(1, true, LockType.Exclusive),
                (byte)HashOperation.HLEN => SingleKey(1, true, LockType.Shared),
                (byte)HashOperation.HEXISTS => SingleKey(1, true, LockType.Shared),
                (byte)HashOperation.HKEYS => SingleKey(1, true, LockType.Shared),
                (byte)HashOperation.HVALS => SingleKey(1, true, LockType.Shared),
                (byte)HashOperation.HINCRBY => SingleKey(1, true, LockType.Exclusive),
                (byte)HashOperation.HINCRBYFLOAT => SingleKey(1, true, LockType.Exclusive),
                (byte)HashOperation.HSETNX => SingleKey(1, true, LockType.Exclusive),
                (byte)HashOperation.HRANDFIELD => SingleKey(1, true, LockType.Shared),
                (byte)HashOperation.HSTRLEN => SingleKey(1, true, LockType.Shared),
                _ => -1
            };
        }

        private int SetObjectKeys(SetOperation subCommand, int inputCount)
        {
            return subCommand switch
            {
                SetOperation.SADD => SingleKey(1, true, LockType.Exclusive),
                SetOperation.SMEMBERS => SingleKey(1, true, LockType.Shared),
                SetOperation.SREM => SingleKey(1, true, LockType.Exclusive),
                SetOperation.SCARD => SingleKey(1, true, LockType.Exclusive),
                SetOperation.SRANDMEMBER => SingleKey(1, true, LockType.Shared),
                SetOperation.SPOP => SingleKey(1, true, LockType.Exclusive),
                SetOperation.SISMEMBER => SingleKey(1, true, LockType.Shared),
                SetOperation.SUNION => ListKeys(inputCount, true, LockType.Shared),
                SetOperation.SUNIONSTORE => XSTOREKeys(inputCount, true),
                SetOperation.SDIFF => ListKeys(inputCount, true, LockType.Shared),
                SetOperation.SDIFFSTORE => XSTOREKeys(inputCount, true),
                SetOperation.SMOVE => ListKeys(inputCount, true, LockType.Exclusive),
                SetOperation.SINTER => ListKeys(inputCount, true, LockType.Shared),
                SetOperation.SINTERSTORE => XSTOREKeys(inputCount, true),
                _ => -1
            };
        }

        /// <summary>
        /// Returns a single for commands that have a single key
        /// </summary>
        private int SingleKey(int arg, bool isObject, LockType type)
        {
            var key = respSession.parseState.GetArgSliceByRef(arg - 1);
            SaveKeyEntryToLock(key, isObject, type);
            SaveKeyArgSlice(key);
            return arg;
        }

        /// <summary>
        /// Returns a list of keys for commands: MGET, DEL, UNLINK
        /// </summary>
        private int ListKeys(int inputCount, bool isObject, LockType type)
        {
            for (var i = 0; i < inputCount; i++)
            {
                var key = respSession.parseState.GetArgSliceByRef(i);
                SaveKeyEntryToLock(key, isObject, type);
                SaveKeyArgSlice(key);
            }
            return inputCount;
        }

        /// <summary>
        /// Returns a list of keys for LMPOP command
        /// </summary>
        private int ListKeys(bool isObject, LockType type)
        {
            var numKeysArg = respSession.GetCommandAsArgSlice(out bool success);
            if (!success) return -2;

            if (!NumUtils.TryParse(numKeysArg.ReadOnlySpan, out int numKeys)) return -2;

            for (int i = 0; i < numKeys; i++)
            {
                var key = respSession.GetCommandAsArgSlice(out success);
                if (!success) return -2;
                SaveKeyEntryToLock(key, isObject, type);
                SaveKeyArgSlice(key);
            }
            return numKeys;
        }

        /// <summary>
        /// Returns a list of keys for MSET commands
        /// </summary>
        private int MSETKeys(int inputCount, bool isObject, LockType type)
        {
            for (var i = 0; i < inputCount; i += 2)
            {
                var key = respSession.parseState.GetArgSliceByRef(i);
                SaveKeyEntryToLock(key, isObject, type);
                SaveKeyArgSlice(key);
            }
            return inputCount;
        }

        /// <summary>
        /// Returns a list of keys for *STORE commands (e.g. SUNIONSTORE, ZINTERSTORE etc.)
        /// Where the first key's value is written to and the rest of the keys' values are read from.
        /// </summary>
        private int XSTOREKeys(int inputCount, bool isObject)
        {
            if (inputCount > 0)
            {
                var key = respSession.parseState.GetArgSliceByRef(0);
                SaveKeyEntryToLock(key, isObject, LockType.Exclusive);
                SaveKeyArgSlice(key);
            }

            for (var i = 1; i < inputCount; i++)
            {
                var key = respSession.parseState.GetArgSliceByRef(i);
                SaveKeyEntryToLock(key, isObject, LockType.Shared);
                SaveKeyArgSlice(key);
            }

            return inputCount;
        }
    }
}