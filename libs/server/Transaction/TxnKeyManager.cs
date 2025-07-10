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
                RespCommand.SADD => SetObjectKeys(command, inputCount),
                RespCommand.SREM => SetObjectKeys(command, inputCount),
                RespCommand.SPOP => SetObjectKeys(command, inputCount),
                RespCommand.SMEMBERS => SetObjectKeys(command, inputCount),
                RespCommand.SCARD => SetObjectKeys(command, inputCount),
                RespCommand.SINTER => SetObjectKeys(command, inputCount),
                RespCommand.SINTERSTORE => SetObjectKeys(command, inputCount),
                RespCommand.SSCAN => SetObjectKeys(command, inputCount),
                RespCommand.SMOVE => SetObjectKeys(command, inputCount),
                RespCommand.SRANDMEMBER => SetObjectKeys(command, inputCount),
                RespCommand.SISMEMBER => SetObjectKeys(command, inputCount),
                RespCommand.SMISMEMBER => SetObjectKeys(command, inputCount),
                RespCommand.SUNION => SetObjectKeys(command, inputCount),
                RespCommand.SUNIONSTORE => SetObjectKeys(command, inputCount),
                RespCommand.SDIFF => SetObjectKeys(command, inputCount),
                RespCommand.SDIFFSTORE => SetObjectKeys(command, inputCount),
                RespCommand.ZADD => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZREM => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZCARD => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZPOPMAX => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZSCORE => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZMSCORE => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZCOUNT => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZINCRBY => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZRANK => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZRANGE => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZRANGEBYLEX => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZRANGEBYSCORE => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZREVRANK => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZREMRANGEBYLEX => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZREMRANGEBYRANK => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZREMRANGEBYSCORE => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZLEXCOUNT => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZPOPMIN => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZRANDMEMBER => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZDIFF => SortedSetObjectKeys(command, inputCount),
                RespCommand.GEOADD => SortedSetObjectKeys(command, inputCount),
                RespCommand.GEOHASH => SortedSetObjectKeys(command, inputCount),
                RespCommand.GEODIST => SortedSetObjectKeys(command, inputCount),
                RespCommand.GEOPOS => SortedSetObjectKeys(command, inputCount),
                RespCommand.GEORADIUS => GeoCommands(RespCommand.GEORADIUS, inputCount),
                RespCommand.GEORADIUS_RO => GeoCommands(RespCommand.GEORADIUS_RO, inputCount),
                RespCommand.GEORADIUSBYMEMBER => GeoCommands(RespCommand.GEORADIUSBYMEMBER, inputCount),
                RespCommand.GEORADIUSBYMEMBER_RO => GeoCommands(RespCommand.GEORADIUSBYMEMBER_RO, inputCount),
                RespCommand.GEOSEARCH => GeoCommands(RespCommand.GEOSEARCH, inputCount),
                RespCommand.GEOSEARCHSTORE => GeoCommands(RespCommand.GEOSEARCHSTORE, inputCount),
                RespCommand.ZREVRANGE => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZREVRANGEBYLEX => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZREVRANGEBYSCORE => SortedSetObjectKeys(command, inputCount),
                RespCommand.LINDEX => ListObjectKeys(command),
                RespCommand.LINSERT => ListObjectKeys(command),
                RespCommand.LLEN => ListObjectKeys(command),
                RespCommand.LMOVE => ListObjectKeys(command),
                RespCommand.LMPOP => ListObjectKeys(command),
                RespCommand.LPOP => ListObjectKeys(command),
                RespCommand.LPUSH => ListObjectKeys(command),
                RespCommand.LPUSHX => ListObjectKeys(command),
                RespCommand.LRANGE => ListObjectKeys(command),
                RespCommand.LREM => ListObjectKeys(command),
                RespCommand.LSET => ListObjectKeys(command),
                RespCommand.LTRIM => ListObjectKeys(command),
                RespCommand.RPOP => ListObjectKeys(command),
                RespCommand.RPUSH => ListObjectKeys(command),
                RespCommand.RPOPLPUSH => ListObjectKeys(command),
                RespCommand.RPUSHX => ListObjectKeys(command),
                RespCommand.HDEL => HashObjectKeys(command),
                RespCommand.HEXISTS => HashObjectKeys(command),
                RespCommand.HGET => HashObjectKeys(command),
                RespCommand.HGETALL => HashObjectKeys(command),
                RespCommand.HINCRBY => HashObjectKeys(command),
                RespCommand.HINCRBYFLOAT => HashObjectKeys(command),
                RespCommand.HKEYS => HashObjectKeys(command),
                RespCommand.HLEN => HashObjectKeys(command),
                RespCommand.HMGET => HashObjectKeys(command),
                RespCommand.HMSET => HashObjectKeys(command),
                RespCommand.HRANDFIELD => HashObjectKeys(command),
                RespCommand.HSCAN => HashObjectKeys(command),
                RespCommand.HSET => HashObjectKeys(command),
                RespCommand.HSETNX => HashObjectKeys(command),
                RespCommand.HSTRLEN => HashObjectKeys(command),
                RespCommand.HVALS => HashObjectKeys(command),
                RespCommand.HEXPIRE => HashObjectKeys(command),
                RespCommand.HPEXPIRE => HashObjectKeys(command),
                RespCommand.HEXPIREAT => HashObjectKeys(command),
                RespCommand.HPEXPIREAT => HashObjectKeys(command),
                RespCommand.GET => SingleKey(1, false, LockType.Shared),
                RespCommand.GETIFNOTMATCH => SingleKey(1, false, LockType.Shared),
                RespCommand.GETWITHETAG => SingleKey(1, false, LockType.Shared),
                RespCommand.SET => SingleKey(1, false, LockType.Exclusive),
                RespCommand.SETIFMATCH => SingleKey(1, false, LockType.Exclusive),
                RespCommand.SETIFGREATER => SingleKey(1, false, LockType.Exclusive),
                RespCommand.GETRANGE => SingleKey(1, false, LockType.Shared),
                RespCommand.SETRANGE => SingleKey(1, false, LockType.Exclusive),
                RespCommand.PFADD => SingleKey(1, false, LockType.Exclusive),
                RespCommand.PFCOUNT => ListKeys(inputCount, StoreType.Main, LockType.Shared),
                RespCommand.PFMERGE => ListKeys(inputCount, StoreType.Main, LockType.Exclusive),
                RespCommand.SETEX => SingleKey(1, false, LockType.Exclusive),
                RespCommand.SETEXNX => SingleKey(1, false, LockType.Exclusive),
                RespCommand.SETEXXX => SingleKey(1, false, LockType.Exclusive),
                RespCommand.DEL => ListKeys(inputCount, StoreType.All, LockType.Exclusive),
                RespCommand.DELIFGREATER => SingleKey(1, false, LockType.Exclusive),
                RespCommand.EXISTS => ListKeys(inputCount, StoreType.All, LockType.Shared),
                RespCommand.RENAME => ListKeys(1, StoreType.All, LockType.Exclusive),
                RespCommand.INCR => SingleKey(1, false, LockType.Exclusive),
                RespCommand.INCRBY => SingleKey(1, false, LockType.Exclusive),
                RespCommand.INCRBYFLOAT => SingleKey(1, false, LockType.Exclusive),
                RespCommand.DECR => SingleKey(1, false, LockType.Exclusive),
                RespCommand.DECRBY => SingleKey(1, false, LockType.Exclusive),
                RespCommand.SETBIT => SingleKey(1, false, LockType.Exclusive),
                RespCommand.GETBIT => SingleKey(1, false, LockType.Shared),
                RespCommand.BITCOUNT => SingleKey(1, false, LockType.Shared),
                RespCommand.BITPOS => SingleKey(1, false, LockType.Shared),
                RespCommand.BITFIELD => SingleKey(1, false, LockType.Exclusive),
                RespCommand.EXPIRE => ListKeys(1, StoreType.All, LockType.Exclusive),
                RespCommand.PEXPIRE => ListKeys(1, StoreType.All, LockType.Exclusive),
                RespCommand.PERSIST => SingleKey(1, false, LockType.Exclusive),
                RespCommand.MGET => ListKeys(inputCount, StoreType.Main, LockType.Shared),
                RespCommand.MSET => MSETKeys(inputCount, false, LockType.Exclusive),
                RespCommand.MSETNX => MSETKeys(inputCount, false, LockType.Exclusive),
                RespCommand.UNLINK => ListKeys(inputCount, StoreType.All, LockType.Exclusive),
                RespCommand.GETDEL => SingleKey(1, false, LockType.Exclusive),
                RespCommand.APPEND => SingleKey(1, false, LockType.Exclusive),

                RespCommand.BITFIELD_RO => SingleKey(1, false, LockType.Shared),
                RespCommand.BITOP => SSTOREKeys(inputCount, false, 1),
                RespCommand.EXPIREAT => ListKeys(1, StoreType.All, LockType.Exclusive),
                RespCommand.EXPIRETIME => ListKeys(1, StoreType.All, LockType.Shared),
                RespCommand.GETEX => SingleKey(1, false, LockType.Exclusive),
                RespCommand.GETSET => SingleKey(1, false, LockType.Exclusive),

                RespCommand.HEXPIRETIME => HashObjectKeys(command),
                RespCommand.HPEXPIRETIME => HashObjectKeys(command),
                RespCommand.HPERSIST => HashObjectKeys(command),
                RespCommand.HPTTL => HashObjectKeys(command),
                RespCommand.HTTL => HashObjectKeys(command),

                RespCommand.LCS => ListKeys(2, StoreType.Main, LockType.Shared),

                RespCommand.LPOS => ListObjectKeys(command),
                RespCommand.SINTERCARD => SetObjectKeys(command, inputCount),

                RespCommand.PEXPIRETIME => ListKeys(1, StoreType.All, LockType.Shared),
                RespCommand.PEXPIREAT => SingleKey(1, false, LockType.Exclusive),
                RespCommand.PSETEX => SingleKey(1, false, LockType.Exclusive),
                RespCommand.PTTL => ListKeys(1, StoreType.All, LockType.Shared),
                RespCommand.RENAMENX => ListKeys(1, StoreType.All, LockType.Exclusive),
                RespCommand.STRLEN => SingleKey(1, false, LockType.Shared),
                RespCommand.SUBSTR => SingleKey(1, false, LockType.Shared),
                RespCommand.TTL => ListKeys(1, StoreType.All, LockType.Shared),
                RespCommand.TYPE => ListKeys(1, StoreType.All, LockType.Shared),
                RespCommand.SETNX => SingleKey(1, false, LockType.Exclusive),

                RespCommand.ZDIFFSTORE => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZEXPIRE => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZEXPIREAT => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZEXPIRETIME => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZINTER => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZINTERCARD => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZINTERSTORE => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZMPOP => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZPEXPIRE => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZPEXPIREAT => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZPEXPIRETIME => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZPERSIST => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZPTTL => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZRANGESTORE => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZSCAN => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZTTL => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZUNION => SortedSetObjectKeys(command, inputCount),
                RespCommand.ZUNIONSTORE => SortedSetObjectKeys(command, inputCount),
                
                RespCommand.COSCAN => SingleKey(1, true, LockType.Shared),
                _ => OtherCommands(command, out error)
            };
        }

        private int OtherCommands(RespCommand command, out ReadOnlySpan<byte> error)
        {
            error = CmdStrings.RESP_ERR_GENERIC_UNK_CMD;
            if (command == RespCommand.DEBUG)
            {
                if (respSession.CanRunDebug())
                    return 1;

                error = System.Text.Encoding.ASCII.GetBytes(string.Format(
                    CmdStrings.GenericErrCommandDisallowedWithOption, RespCommand.DEBUG, "enable-debug-command"));
                return -1;
            }

            return command switch
            {
                RespCommand.CONFIG_GET => 1,
                RespCommand.CONFIG_REWRITE => 1,
                RespCommand.CONFIG_SET => 1,
                RespCommand.COMMAND => 1,
                RespCommand.COMMAND_COUNT => 1,
                RespCommand.COMMAND_DOCS => 1,
                RespCommand.COMMAND_GETKEYS => 1,
                RespCommand.COMMAND_GETKEYSANDFLAGS => 1,
                RespCommand.COMMAND_INFO => 1,
                RespCommand.CLIENT_GETNAME => 1,
                RespCommand.CLIENT_ID => 1,
                RespCommand.CLIENT_INFO => 1,
                RespCommand.CLIENT_KILL => 1,
                RespCommand.DBSIZE => 1,
                RespCommand.ECHO => 1,
                RespCommand.FLUSHALL => 1,
                RespCommand.FLUSHDB => 1,
                RespCommand.HELLO => 1,
                RespCommand.INFO => 1,
                RespCommand.KEYS => 1,
                RespCommand.LATENCY_HELP => 1,
                RespCommand.LATENCY_HISTOGRAM => 1,
                RespCommand.LATENCY_RESET => 1,
                RespCommand.MEMORY_USAGE => 1,
                RespCommand.PING => 1,
                RespCommand.PUBLISH => 1,
                RespCommand.REPLICAOF => 1,
                RespCommand.ROLE => 1,
                RespCommand.SCAN => 1,
                RespCommand.SECONDARYOF => 1,
                RespCommand.SELECT => 1,
                RespCommand.SLOWLOG_GET => 1,
                RespCommand.SLOWLOG_HELP => 1,
                RespCommand.SLOWLOG_LEN => 1,
                RespCommand.SLOWLOG_RESET => 1,
                RespCommand.SPUBLISH => 1,
                RespCommand.SWAPDB => 1,
                RespCommand.TIME => 1,
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

        private int SortedSetObjectKeys(RespCommand command, int inputCount)
        {
            return command switch
            {
                RespCommand.ZADD => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZREM => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZCARD => SingleKey(1, true, LockType.Shared),
                RespCommand.ZPOPMAX => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZSCORE => SingleKey(1, true, LockType.Shared),
                RespCommand.ZMSCORE => SingleKey(1, true, LockType.Shared),
                RespCommand.ZCOUNT => SingleKey(1, true, LockType.Shared),
                RespCommand.ZINCRBY => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZRANK => SingleKey(1, true, LockType.Shared),
                RespCommand.ZRANGE => SingleKey(1, true, LockType.Shared),
                RespCommand.ZREVRANK => SingleKey(1, true, LockType.Shared),
                RespCommand.ZREMRANGEBYLEX => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZREMRANGEBYRANK => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZREMRANGEBYSCORE => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZLEXCOUNT => SingleKey(1, true, LockType.Shared),
                RespCommand.ZPOPMIN => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZRANDMEMBER => SingleKey(1, true, LockType.Shared),
                RespCommand.ZDIFF => ListKeys(true, LockType.Shared),
                RespCommand.GEOADD => SingleKey(1, true, LockType.Exclusive),
                RespCommand.GEOHASH => SingleKey(1, true, LockType.Shared),
                RespCommand.GEODIST => SingleKey(1, true, LockType.Shared),
                RespCommand.GEOPOS => SingleKey(1, true, LockType.Shared),

                RespCommand.ZDIFFSTORE => ZSTOREKeys(inputCount, true),
                RespCommand.ZEXPIRE => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZEXPIREAT => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZEXPIRETIME => SingleKey(1, true, LockType.Shared),
                RespCommand.ZINTER => ListKeys(inputCount, StoreType.Object, LockType.Shared),
                RespCommand.ZINTERCARD => ListKeys(inputCount, StoreType.Object, LockType.Shared),
                RespCommand.ZINTERSTORE => ZSTOREKeys(inputCount, true),
                RespCommand.ZMPOP => ListKeys(true, LockType.Exclusive),
                RespCommand.ZPEXPIRE => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZPEXPIREAT => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZPEXPIRETIME => SingleKey(1, true, LockType.Shared),
                RespCommand.ZPERSIST => SingleKey(1, true, LockType.Exclusive),
                RespCommand.ZPTTL => SingleKey(1, true, LockType.Shared),
                RespCommand.ZRANGEBYLEX => SingleKey(1, true, LockType.Shared),
                RespCommand.ZRANGEBYSCORE => SingleKey(1, true, LockType.Shared),
                RespCommand.ZRANGESTORE => SSTOREKeys(2, true),
                RespCommand.ZREVRANGE => SingleKey(1, true, LockType.Shared),
                RespCommand.ZREVRANGEBYLEX => SingleKey(1, true, LockType.Shared),
                RespCommand.ZREVRANGEBYSCORE => SingleKey(1, true, LockType.Shared),
                RespCommand.ZSCAN => SingleKey(1, true, LockType.Shared),
                RespCommand.ZTTL => SingleKey(1, true, LockType.Shared),
                RespCommand.ZUNION => ListKeys(true, LockType.Shared),
                RespCommand.ZUNIONSTORE => ZSTOREKeys(inputCount, true),
                _ => -1
            };
        }

        private int ListObjectKeys(RespCommand command)
        {
            return command switch
            {
                RespCommand.LPUSH => SingleKey(1, true, LockType.Exclusive),
                RespCommand.LPOP => SingleKey(1, true, LockType.Exclusive),
                RespCommand.RPUSH => SingleKey(1, true, LockType.Exclusive),
                RespCommand.RPOP => SingleKey(1, true, LockType.Exclusive),
                RespCommand.LLEN => SingleKey(1, true, LockType.Shared),
                RespCommand.LTRIM => SingleKey(1, true, LockType.Exclusive),
                RespCommand.LRANGE => SingleKey(1, true, LockType.Shared),
                RespCommand.LINDEX => SingleKey(1, true, LockType.Shared),
                RespCommand.LINSERT => SingleKey(1, true, LockType.Exclusive),
                RespCommand.LREM => SingleKey(1, true, LockType.Exclusive),
                RespCommand.LSET => SingleKey(1, true, LockType.Exclusive),

                RespCommand.LMOVE => ListKeys(2, StoreType.Object, LockType.Exclusive),
                RespCommand.LMPOP => ListKeys(true, LockType.Exclusive),
                RespCommand.LPOS => SingleKey(1, true, LockType.Shared),
                RespCommand.LPUSHX => SingleKey(1, true, LockType.Exclusive),
                RespCommand.RPOPLPUSH => ListKeys(2, StoreType.Object, LockType.Exclusive),
                RespCommand.RPUSHX => SingleKey(1, true, LockType.Exclusive),
                _ => -1
            };
        }

        private int HashObjectKeys(RespCommand command)
        {
            return command switch
            {
                RespCommand.HSET => SingleKey(1, true, LockType.Exclusive),
                RespCommand.HMSET => SingleKey(1, true, LockType.Exclusive),
                RespCommand.HGET => SingleKey(1, true, LockType.Shared),
                RespCommand.HMGET => SingleKey(1, true, LockType.Shared),
                RespCommand.HGETALL => SingleKey(1, true, LockType.Shared),
                RespCommand.HDEL => SingleKey(1, true, LockType.Exclusive),
                RespCommand.HLEN => SingleKey(1, true, LockType.Shared),
                RespCommand.HEXISTS => SingleKey(1, true, LockType.Shared),
                RespCommand.HKEYS => SingleKey(1, true, LockType.Shared),
                RespCommand.HVALS => SingleKey(1, true, LockType.Shared),
                RespCommand.HINCRBY => SingleKey(1, true, LockType.Exclusive),
                RespCommand.HINCRBYFLOAT => SingleKey(1, true, LockType.Exclusive),
                RespCommand.HSETNX => SingleKey(1, true, LockType.Exclusive),
                RespCommand.HRANDFIELD => SingleKey(1, true, LockType.Shared),
                RespCommand.HSTRLEN => SingleKey(1, true, LockType.Shared),
                RespCommand.HEXPIRE => SingleKey(1, true, LockType.Exclusive),
                RespCommand.HEXPIREAT => SingleKey(1, true, LockType.Exclusive),
                RespCommand.HPEXPIRE => SingleKey(1, true, LockType.Exclusive),
                RespCommand.HPEXPIREAT => SingleKey(1, true, LockType.Exclusive),

                RespCommand.HEXPIRETIME => SingleKey(1, true, LockType.Shared),
                RespCommand.HPEXPIRETIME => SingleKey(1, true, LockType.Shared),
                RespCommand.HPERSIST => SingleKey(1, true, LockType.Exclusive),
                RespCommand.HPTTL => SingleKey(1, true, LockType.Shared),
                RespCommand.HSCAN => SingleKey(1, true, LockType.Shared),
                RespCommand.HTTL => SingleKey(1, true, LockType.Shared),
                _ => -1
            };
        }

        private int SetObjectKeys(RespCommand command, int inputCount)
        {
            return command switch
            {
                RespCommand.SADD => SingleKey(1, true, LockType.Exclusive),
                RespCommand.SMEMBERS => SingleKey(1, true, LockType.Shared),
                RespCommand.SREM => SingleKey(1, true, LockType.Exclusive),
                RespCommand.SCARD => SingleKey(1, true, LockType.Shared),
                RespCommand.SRANDMEMBER => SingleKey(1, true, LockType.Shared),
                RespCommand.SPOP => SingleKey(1, true, LockType.Exclusive),
                RespCommand.SISMEMBER => SingleKey(1, true, LockType.Shared),
                RespCommand.SMISMEMBER => SingleKey(1, true, LockType.Shared),
                RespCommand.SSCAN => SingleKey(1, true, LockType.Shared),
                RespCommand.SUNION => ListKeys(inputCount, StoreType.Object, LockType.Shared),
                RespCommand.SUNIONSTORE => SSTOREKeys(inputCount, true),
                RespCommand.SDIFF => ListKeys(inputCount, StoreType.Object, LockType.Shared),
                RespCommand.SDIFFSTORE => SSTOREKeys(inputCount, true),
                RespCommand.SMOVE => ListKeys(inputCount, StoreType.Object, LockType.Exclusive),
                RespCommand.SINTER => ListKeys(inputCount, StoreType.Object, LockType.Shared),
                RespCommand.SINTERCARD => ListKeys(inputCount, StoreType.Object, LockType.Shared),
                RespCommand.SINTERSTORE => SSTOREKeys(inputCount, true),
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
        private int SSTOREKeys(int inputCount, bool isObject, int offset = 0)
        {
            if (inputCount > offset)
            {
                var key = respSession.parseState.GetArgSliceByRef(offset);
                SaveKeyEntryToLock(key, isObject, LockType.Exclusive);
                SaveKeyArgSlice(key);
            }

            for (var i = offset + 1; i < inputCount; i++)
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
        /// We can get number of read keys by checking the second argument.
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