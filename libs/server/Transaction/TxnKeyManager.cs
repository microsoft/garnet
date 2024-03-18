// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
            if (!clusterSession.CheckSingleKeySlotVerify(key, readOnly, respSession.SessionAsking))
            {
                this.state = TxnState.Aborted;
                return;
            }
        }

        /// <summary>
        /// Returns a number of skipped args
        /// </summary>
        internal int GetKeys(RespCommand command, int inputCount, out ReadOnlySpan<byte> error, byte subCommand)
        {
            error = CmdStrings.RESP_ERR;
            return command switch
            {
                RespCommand.SortedSet => SortedSetObjectKeys(subCommand, inputCount),
                RespCommand.List => ListObjectKeys(subCommand),
                RespCommand.Hash => HashObjectKeys(subCommand),
                RespCommand.Set => SetObjectKeys(subCommand),
                RespCommand.GET => SingleKey(1, false, LockType.Shared),
                RespCommand.SET => SingleKey(1, false, LockType.Exclusive),
                RespCommand.GETRANGE => SingleKey(1, false, LockType.Shared),
                RespCommand.SETRANGE => SingleKey(1, false, LockType.Exclusive),
                RespCommand.PFADD => SingleKey(1, false, LockType.Exclusive),
                RespCommand.PFCOUNT => ListKeys(inputCount, false, LockType.Shared),
                RespCommand.PFMERGE => ListKeys(inputCount, false, LockType.Exclusive),
                RespCommand.SETEX => SingleKey(3, false, LockType.Exclusive),
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

        private int AdminCommands(RespCommand command)
        {
            return command switch
            {
                RespCommand.ECHO => 1,
                RespCommand.REPLICAOF => 1,
                RespCommand.SECONDARYOF => 1,
                RespCommand.CONFIG => 1,
                RespCommand.CLIENT => 1,
                _ => -1
            };
        }

        private int SortedSetObjectKeys(byte subCommand, int inputCount)
        {

            return subCommand switch
            {
                (byte)SortedSetOperation.ZADD => SingleKey(1, true, LockType.Exclusive),
                (byte)SortedSetOperation.ZREM => SingleKey(1, true, LockType.Exclusive),
                (byte)SortedSetOperation.ZCARD => SingleKey(1, true, LockType.Shared),
                (byte)SortedSetOperation.ZPOPMAX => SingleKey(1, true, LockType.Exclusive),
                (byte)SortedSetOperation.ZSCORE => SingleKey(1, true, LockType.Shared),
                (byte)SortedSetOperation.ZCOUNT => SingleKey(1, true, LockType.Shared),
                (byte)SortedSetOperation.ZINCRBY => SingleKey(1, true, LockType.Exclusive),
                (byte)SortedSetOperation.ZRANK => SingleKey(1, true, LockType.Exclusive),
                (byte)SortedSetOperation.ZRANGE => SingleKey(1, true, LockType.Shared),
                (byte)SortedSetOperation.ZRANGEBYSCORE => SingleKey(1, true, LockType.Shared),
                (byte)SortedSetOperation.ZREVRANK => SingleKey(1, true, LockType.Exclusive),
                (byte)SortedSetOperation.ZREMRANGEBYLEX => SingleKey(1, true, LockType.Exclusive),
                (byte)SortedSetOperation.ZREMRANGEBYRANK => SingleKey(1, true, LockType.Exclusive),
                (byte)SortedSetOperation.ZREMRANGEBYSCORE => SingleKey(1, true, LockType.Exclusive),
                (byte)SortedSetOperation.ZLEXCOUNT => SingleKey(1, true, LockType.Exclusive),
                (byte)SortedSetOperation.ZPOPMIN => SingleKey(1, true, LockType.Exclusive),
                (byte)SortedSetOperation.ZRANDMEMBER => SingleKey(1, true, LockType.Exclusive),
                (byte)SortedSetOperation.ZDIFF => ListKeys(inputCount, true, LockType.Exclusive),
                (byte)SortedSetOperation.GEOADD => SingleKey(1, true, LockType.Exclusive),
                (byte)SortedSetOperation.GEOHASH => SingleKey(1, true, LockType.Shared),
                (byte)SortedSetOperation.GEODIST => SingleKey(1, true, LockType.Shared),
                (byte)SortedSetOperation.GEOPOS => SingleKey(1, true, LockType.Shared),
                (byte)SortedSetOperation.GEOSEARCH => SingleKey(1, true, LockType.Shared),
                (byte)SortedSetOperation.ZREVRANGE => SingleKey(1, true, LockType.Shared),
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
                _ => -1
            };
        }

        private int SetObjectKeys(byte subCommand)
        {
            return subCommand switch
            {
                (byte)SetOperation.SADD => SingleKey(1, true, LockType.Exclusive),
                (byte)SetOperation.SMEMBERS => SingleKey(1, true, LockType.Shared),
                (byte)SetOperation.SREM => SingleKey(1, true, LockType.Exclusive),
                (byte)SetOperation.SCARD => SingleKey(1, true, LockType.Exclusive),
                (byte)SetOperation.SPOP => SingleKey(1, true, LockType.Exclusive),
                _ => -1
            };
        }

        /// <summary>
        /// Returns a single for commands that have a single key
        /// </summary>
        private int SingleKey(int arg, bool isObject, LockType type)
        {
            bool success;
            for (int i = 1; i < arg; i++)
            {
                respSession.GetCommandAsArgSlice(out success);
                if (!success) return -2;
            }
            var key = respSession.GetCommandAsArgSlice(out success);
            if (!success) return -2;
            SaveKeyEntryToLock(key, isObject, type);
            SaveKeyArgSlice(key);
            return 1 + arg;
        }

        /// <summary>
        /// Returns a list of keys for commands: MGET, DEL, UNLINK
        /// </summary>
        private int ListKeys(int inputCount, bool isObject, LockType type)
        {
            for (int i = 1; i < inputCount; i++)
            {
                var key = respSession.GetCommandAsArgSlice(out bool success);
                if (!success) return -2;
                SaveKeyEntryToLock(key, isObject, type);
                SaveKeyArgSlice(key);
            }
            return inputCount;
        }

        /// <summary>
        /// Returns a list of keys for MSET commands
        /// </summary>
        private int MSETKeys(int inputCount, bool isObject, LockType type)
        {
            for (int i = 1; i < inputCount; i += 2)
            {
                var key = respSession.GetCommandAsArgSlice(out bool success);
                if (!success) return -2;
                var val = respSession.GetCommandAsArgSlice(out success);
                if (!success) return -2;
                SaveKeyEntryToLock(key, isObject, type);
                SaveKeyArgSlice(key);
            }
            return inputCount;
        }
    }
}