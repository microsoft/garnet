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
        /// Reset cached slot verification result
        /// </summary>
        public void ResetCacheSlotVerificationResult()
        {
            if (!clusterEnabled) return;
            respSession.clusterSession.ResetCachedSlotVerificationResult();
        }

        /// <summary>
        /// Reset cached slot verification result
        /// </summary>
        public void WriteCachedSlotVerificationMessage(ref MemoryResult<byte> output)
        {
            if (!clusterEnabled) return;
            respSession.clusterSession.WriteCachedSlotVerificationMessage(ref output);
        }

        /// <summary>
        /// Verify key ownership
        /// </summary>
        /// <param name="key"></param>
        /// <param name="type"></param>
        public unsafe void VerifyKeyOwnership(ArgSlice key, LockType type)
        {
            if (!clusterEnabled) return;

            var readOnly = type == LockType.Shared;
            if (!respSession.clusterSession.NetworkIterativeSlotVerify(key, readOnly, respSession.SessionAsking))
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
                RespCommand.APPEND => SingleKey(1, false, LockType.Exclusive),
                RespCommand.BITCOUNT => SingleKey(1, false, LockType.Shared),
                RespCommand.BITFIELD => SingleKey(1, false, LockType.Exclusive),
                RespCommand.BITFIELD_RO => SingleKey(1, false, LockType.Shared),
                RespCommand.BITPOS => SingleKey(1, false, LockType.Exclusive),
                RespCommand.BITOP => SSTOREKeys(inputCount, false),
                RespCommand.DECR => SingleKey(1, false, LockType.Exclusive),
                RespCommand.DECRBY => SingleKey(1, false, LockType.Exclusive),
                RespCommand.DEL => ListKeys(inputCount, StoreType.All, LockType.Exclusive),
                RespCommand.DELIFGREATER => SingleKey(1, false, LockType.Exclusive),
                RespCommand.EXISTS => ListKeys(inputCount, StoreType.All, LockType.Shared),
                RespCommand.EXPIRE => ListKeys(1, StoreType.All, LockType.Exclusive),
                RespCommand.EXPIREAT => ListKeys(1, StoreType.All, LockType.Exclusive),
                RespCommand.EXPIRETIME => ListKeys(1, StoreType.All, LockType.Shared),
                RespCommand.GEOADD => SingleKey(1, true, LockType.Exclusive),
                RespCommand.GEODIST => SingleKey(1, true, LockType.Shared),
                RespCommand.GEOHASH => SingleKey(1, true, LockType.Shared),
                RespCommand.GEOPOS => SingleKey(1, true, LockType.Shared),
                RespCommand.GEORADIUSBYMEMBER => GeoCommands(RespCommand.GEORADIUSBYMEMBER, inputCount),
                RespCommand.GEORADIUSBYMEMBER_RO => GeoCommands(RespCommand.GEORADIUSBYMEMBER_RO, inputCount),
                RespCommand.GEORADIUS => GeoCommands(RespCommand.GEORADIUS, inputCount),
                RespCommand.GEORADIUS_RO => GeoCommands(RespCommand.GEORADIUS_RO, inputCount),
                RespCommand.GEOSEARCH => GeoCommands(RespCommand.GEOSEARCH, inputCount),
                RespCommand.GEOSEARCHSTORE => GeoCommands(RespCommand.GEOSEARCHSTORE, inputCount),
                RespCommand.GET => SingleKey(1, false, LockType.Shared),
                RespCommand.GETBIT => SingleKey(1, false, LockType.Shared),
                RespCommand.GETDEL => SingleKey(1, false, LockType.Exclusive),
                RespCommand.GETEX => SingleKey(1, false, LockType.Exclusive),
                RespCommand.GETIFNOTMATCH => SingleKey(1, false, LockType.Shared),
                RespCommand.GETRANGE => SingleKey(1, false, LockType.Shared),
                RespCommand.GETSET => SingleKey(1, false, LockType.Exclusive),
                RespCommand.GETWITHETAG => SingleKey(1, false, LockType.Shared),
                RespCommand.HDEL => SingleKey(1, true, LockType.Exclusive),
                RespCommand.HEXISTS => SingleKey(1, true, LockType.Shared),
                RespCommand.HEXPIRETIME => SingleKey(1, true, LockType.Shared),
                RespCommand.HGET => SingleKey(1, true, LockType.Shared),
                RespCommand.HGETALL => SingleKey(1, true, LockType.Shared),
                RespCommand.HINCRBY => SingleKey(1, true, LockType.Exclusive),
                RespCommand.HINCRBYFLOAT => SingleKey(1, true, LockType.Exclusive),
                RespCommand.HKEYS => SingleKey(1, true, LockType.Shared),
                RespCommand.HPEXPIRE => SingleKey(1, true, LockType.Exclusive),
                RespCommand.HPEXPIREAT => SingleKey(1, true, LockType.Exclusive),
                RespCommand.HPEXPIRETIME => SingleKey(1, true, LockType.Shared),
                RespCommand.HPERSIST => SingleKey(1, true, LockType.Exclusive),
                RespCommand.HPTTL => SingleKey(1, true, LockType.Shared),
                RespCommand.HLEN => SingleKey(1, true, LockType.Shared),
                RespCommand.HMGET => SingleKey(1, true, LockType.Shared),
                RespCommand.HMSET => SingleKey(1, true, LockType.Exclusive),
                RespCommand.HRANDFIELD => SingleKey(1, true, LockType.Shared),
                RespCommand.HSCAN => SingleKey(1, true, LockType.Shared),
                RespCommand.HSET => SingleKey(1, true, LockType.Exclusive),
                RespCommand.HSETNX => SingleKey(1, true, LockType.Exclusive),
                RespCommand.HSTRLEN => SingleKey(1, true, LockType.Shared),
                RespCommand.HTTL => SingleKey(1, true, LockType.Shared),
                RespCommand.HVALS => SingleKey(1, true, LockType.Shared),
                RespCommand.INCR => SingleKey(1, false, LockType.Exclusive),
                RespCommand.INCRBY => SingleKey(1, false, LockType.Exclusive),
                RespCommand.INCRBYFLOAT => SingleKey(1, false, LockType.Exclusive),
                RespCommand.LCS => ListKeys(2, StoreType.Main, LockType.Shared),
                RespCommand.LINDEX => SingleKey(1, true, LockType.Shared),
                RespCommand.LINSERT => SingleKey(1, true, LockType.Exclusive),
                RespCommand.LLEN => SingleKey(1, true, LockType.Shared),
                RespCommand.LMOVE => ListKeys(2, StoreType.Object, LockType.Exclusive),
                RespCommand.LMPOP => ListKeys(true, LockType.Exclusive),
                RespCommand.LPOP => SingleKey(1, true, LockType.Exclusive),
                RespCommand.LPOS => SingleKey(1, true, LockType.Shared),
                RespCommand.LPUSH => SingleKey(1, true, LockType.Exclusive),
                RespCommand.LPUSHX => SingleKey(1, true, LockType.Exclusive),
                RespCommand.LRANGE => SingleKey(1, true, LockType.Shared),
                RespCommand.LREM => SingleKey(1, true, LockType.Exclusive),
                RespCommand.LSET => SingleKey(1, true, LockType.Exclusive),
                RespCommand.LTRIM => SingleKey(1, true, LockType.Exclusive),
                RespCommand.MGET => ListKeys(inputCount, StoreType.Main, LockType.Shared),
                RespCommand.MSET => MSETKeys(inputCount, false, LockType.Exclusive),
                RespCommand.MSETNX => MSETKeys(inputCount, false, LockType.Exclusive),
                RespCommand.PERSIST => SingleKey(1, false, LockType.Exclusive),
                RespCommand.PEXPIRE => ListKeys(1, StoreType.All, LockType.Exclusive),
                RespCommand.PEXPIRETIME => ListKeys(1, StoreType.All, LockType.Shared),
                RespCommand.PFADD => SingleKey(1, false, LockType.Exclusive),
                RespCommand.PFCOUNT => ListKeys(inputCount, StoreType.Main, LockType.Shared),
                RespCommand.PFMERGE => ListKeys(inputCount, StoreType.Main, LockType.Exclusive),
                RespCommand.PTTL => ListKeys(1, StoreType.All, LockType.Shared),
                RespCommand.RENAME => ListKeys(1, StoreType.All, LockType.Exclusive),
                RespCommand.RENAMENX => ListKeys(1, StoreType.All, LockType.Exclusive),
                RespCommand.RPOP => SingleKey(1, true, LockType.Exclusive),
                RespCommand.RPOPLPUSH => ListKeys(2, StoreType.Object, LockType.Exclusive),
                RespCommand.RPUSH => SingleKey(1, true, LockType.Exclusive),
                RespCommand.RPUSHX => SingleKey(1, true, LockType.Exclusive),
                RespCommand.SADD => SingleKey(1, true, LockType.Exclusive),
                RespCommand.SCARD => SingleKey(1, true, LockType.Shared),
                RespCommand.SDIFF => ListKeys(inputCount, StoreType.Object, LockType.Shared),
                RespCommand.SDIFFSTORE => SSTOREKeys(inputCount, true),
                RespCommand.SET => SingleKey(1, false, LockType.Exclusive),
                RespCommand.SETBIT => SingleKey(1, false, LockType.Exclusive),
                RespCommand.SETEXNX => SingleKey(1, false, LockType.Exclusive),
                RespCommand.SETEX => SingleKey(1, false, LockType.Exclusive),
                RespCommand.SETEXXX => SingleKey(1, false, LockType.Exclusive),
                RespCommand.SETIFGREATER => SingleKey(1, false, LockType.Exclusive),
                RespCommand.SETIFMATCH => SingleKey(1, false, LockType.Exclusive),
                RespCommand.SETRANGE => SingleKey(1, false, LockType.Exclusive),
                RespCommand.SINTER => ListKeys(inputCount, StoreType.Object, LockType.Shared),
                RespCommand.SINTERSTORE => SSTOREKeys(inputCount, true),
                RespCommand.SISMEMBER => SingleKey(1, true, LockType.Shared),
                RespCommand.SMEMBERS => SingleKey(1, true, LockType.Shared),
                RespCommand.SMISMEMBER => SingleKey(1, true, LockType.Shared),
                RespCommand.SMOVE => ListKeys(inputCount, StoreType.Object, LockType.Exclusive),
                RespCommand.SPOP => SingleKey(1, true, LockType.Exclusive),
                RespCommand.SRANDMEMBER => SingleKey(1, true, LockType.Shared),
                RespCommand.SREM => SingleKey(1, true, LockType.Exclusive),
                RespCommand.STRLEN => SingleKey(1, false, LockType.Shared),
                RespCommand.SSCAN => SingleKey(1, true, LockType.Shared),
                RespCommand.SUBSTR => SingleKey(1, false, LockType.Shared),
                RespCommand.SUNION => ListKeys(inputCount, StoreType.Object, LockType.Shared),
                RespCommand.SUNIONSTORE => SSTOREKeys(inputCount, true),
                RespCommand.TTL => ListKeys(1, StoreType.All, LockType.Shared),
                RespCommand.TYPE => ListKeys(1, StoreType.All, LockType.Shared),
                RespCommand.UNLINK => ListKeys(inputCount, StoreType.All, LockType.Exclusive),
                RespCommand.ZADD => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZCARD => SingleKey(1, true, LockType.Shared),
                RespCommand.ZCOUNT => SingleKey(1, true, LockType.Shared),
                RespCommand.ZDIFF => ListKeys(true, LockType.Shared),
                RespCommand.ZDIFFSTORE => ZSTOREKeys(inputCount, true),
                RespCommand.ZEXPIRETIME => SingleKey(1, true, LockType.Shared),
                RespCommand.ZINCRBY => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZINTER => ListKeys(inputCount, StoreType.Object, LockType.Exclusive),
                RespCommand.ZINTERSTORE => ZSTOREKeys(inputCount, true),
                RespCommand.ZLEXCOUNT => SingleKey(1, true, LockType.Shared),
                RespCommand.ZMPOP => ListKeys(true, LockType.Exclusive),
                RespCommand.ZMSCORE => SingleKey(1, true, LockType.Shared),
                RespCommand.ZPEXPIRE => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZPEXPIREAT => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZPEXPIRETIME => SingleKey(1, true, LockType.Shared),
                RespCommand.ZPERSIST => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZPTTL => SingleKey(1, true, LockType.Shared),
                RespCommand.ZPOPMAX => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZPOPMIN => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZRANDMEMBER => SingleKey(1, true, LockType.Shared),
                RespCommand.ZRANGE => SingleKey(1, true, LockType.Shared),
                RespCommand.ZRANGEBYLEX => SingleKey(1, true, LockType.Shared),
                RespCommand.ZRANGEBYSCORE => SingleKey(1, true, LockType.Shared),
                RespCommand.ZRANK => SingleKey(1, true, LockType.Shared),
                RespCommand.ZREM => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZREMRANGEBYLEX => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZREMRANGEBYRANK => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZREMRANGEBYSCORE => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZREVRANGE => SingleKey(1, true, LockType.Shared),
                RespCommand.ZREVRANGEBYLEX => SingleKey(1, true, LockType.Shared),
                RespCommand.ZREVRANGEBYSCORE => SingleKey(1, true, LockType.Shared),
                RespCommand.ZREVRANK => SingleKey(1, true, LockType.Shared),
                RespCommand.ZSCAN => SingleKey(1, true, LockType.Shared),
                RespCommand.ZSCORE => SingleKey(1, true, LockType.Shared),
                RespCommand.ZTTL => SingleKey(1, true, LockType.Shared),
                RespCommand.ZUNION => ListKeys(true, LockType.Shared),
                RespCommand.ZUNIONSTORE => ZSTOREKeys(inputCount, true),
                _ => OtherCommands(command)
            };
        }

        private static int OtherCommands(RespCommand command)
        {
            return command switch
            {
                RespCommand.CONFIG => 1,
                RespCommand.CLIENT => 1,
                RespCommand.DBSIZE => 1,
                RespCommand.ECHO => 1,
                RespCommand.FLUSHALL => 1,
                RespCommand.FLUSHDB => 1,
                RespCommand.HELLO => 1,
                RespCommand.INFO => 1,
                RespCommand.KEYS => 1,
                RespCommand.PING => 1,
                RespCommand.PUBLISH => 1,
                RespCommand.REPLICAOF => 1,
                RespCommand.ROLE => 1,
                RespCommand.SCAN => 1,
                RespCommand.SECONDARYOF => 1,
                RespCommand.SELECT => 1,
                RespCommand.SPUBLISH => 1,
                RespCommand.SWAPDB => 1,
                _ => -1
            };
        }

        private int GeoCommands(RespCommand command, int inputCount)
        {
            var idx = 0;

            // GEOSEARCHSTORE dest key....
            // While all other commands here start with GEOsomething key...
            if (command == RespCommand.GEOSEARCHSTORE)
            {
                var destinationKey = respSession.parseState.GetArgSliceByRef(idx++);
                SaveKeyEntryToLock(destinationKey, true, LockType.Exclusive);
                SaveKeyArgSlice(destinationKey);
            }

            // Either this is GEOSEARCHSTORE, and index 1 is sourcekey, or some other command and index 0 is sourcekey.
            var key = respSession.parseState.GetArgSliceByRef(idx++);
            SaveKeyEntryToLock(key, true, LockType.Shared);
            SaveKeyArgSlice(key);

            switch (command)
            {
                case RespCommand.GEOSEARCH:
                case RespCommand.GEORADIUS_RO:
                case RespCommand.GEORADIUSBYMEMBER_RO:
                    return 1;
                case RespCommand.GEOSEARCHSTORE:
                    return 2;
                case RespCommand.GEORADIUS:
                case RespCommand.GEORADIUSBYMEMBER:
                    // These commands may or may not store a result
                    for (var i = idx; i < inputCount - 1; ++i)
                    {
                        var span = respSession.parseState.GetArgSliceByRef(i).ReadOnlySpan;

                        if (span.EqualsUpperCaseSpanIgnoringCase(CmdStrings.STORE) ||
                            span.EqualsUpperCaseSpanIgnoringCase(CmdStrings.STOREDIST))
                        {
                            var destinationKey = respSession.parseState.GetArgSliceByRef(i + 1);
                            SaveKeyEntryToLock(destinationKey, true, LockType.Exclusive);
                            SaveKeyArgSlice(destinationKey);
                            break;
                        }
                    }

                    return 1;
                default:
                    // Should never reach here.
                    throw new NotSupportedException();
            }
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
        private int ListKeys(int inputCount, StoreType storeType, LockType type)
        {
            for (var i = 0; i < inputCount; i++)
            {
                var key = respSession.parseState.GetArgSliceByRef(i);
                if (storeType is StoreType.Main or StoreType.All)
                    SaveKeyEntryToLock(key, false, type);
                if (storeType is StoreType.Object or StoreType.All && !objectStoreBasicContext.IsNull)
                    SaveKeyEntryToLock(key, true, type);
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

            for (var i = 0; i < numKeys; i++)
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
        /// Returns a list of keys for set *STORE commands (e.g. SUNIONSTORE, SINTERSTORE etc.)
        /// Where the first key's value is written to and the rest of the keys' values are read from.
        /// </summary>
        private int SSTOREKeys(int inputCount, bool isObject)
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

        /// <summary>
        /// Returns a list of keys for complex *STORE commands (e.g. ZUNIONSTORE, ZINTERSTORE etc.)
        /// Where the first key's value is written to and the rest of the keys' values are read from.
        /// </summary>
        private int ZSTOREKeys(int inputCount, bool isObject)
        {
            if (inputCount > 0)
            {
                var key = respSession.parseState.GetArgSliceByRef(0);
                SaveKeyEntryToLock(key, isObject, LockType.Exclusive);
                SaveKeyArgSlice(key);
            }

            if ((inputCount < 2) || !respSession.parseState.TryGetInt(1, out var numKeysArg))
                return -2;

            for (var i = 2; i < inputCount && i < numKeysArg + 2; i++)
            {
                var key = respSession.parseState.GetArgSliceByRef(i);
                SaveKeyEntryToLock(key, isObject, LockType.Shared);
                SaveKeyArgSlice(key);
            }

            return inputCount;
        }
    }
}