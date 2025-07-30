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
                RespCommand.GEORADIUS => SortedSetObjectKeys(command, inputCount),
                RespCommand.GEORADIUS_RO => SortedSetObjectKeys(command, inputCount),
                RespCommand.GEORADIUSBYMEMBER => SortedSetObjectKeys(command, inputCount),
                RespCommand.GEORADIUSBYMEMBER_RO => SortedSetObjectKeys(command, inputCount),
                RespCommand.GEOSEARCH => SortedSetObjectKeys(command, inputCount),
                RespCommand.GEOSEARCHSTORE => SortedSetObjectKeys(command, inputCount),
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
                RespCommand.GET => SingleKey(StoreType.Main, LockType.Shared),
                RespCommand.GETIFNOTMATCH => SingleKey(StoreType.Main, LockType.Shared),
                RespCommand.GETWITHETAG => SingleKey(StoreType.Main, LockType.Shared),
                RespCommand.SET => SingleKey(StoreType.Main, LockType.Exclusive),
                RespCommand.SETIFMATCH => SingleKey(StoreType.Main, LockType.Exclusive),
                RespCommand.SETIFGREATER => SingleKey(StoreType.Main, LockType.Exclusive),
                RespCommand.GETRANGE => SingleKey(StoreType.Main, LockType.Shared),
                RespCommand.SETRANGE => SingleKey(StoreType.Main, LockType.Exclusive),
                RespCommand.PFADD => SingleKey(StoreType.Main, LockType.Exclusive),
                RespCommand.PFCOUNT => ListKeys(inputCount, StoreType.Main, LockType.Shared),
                RespCommand.PFMERGE => SingleWriteKeyListReadKeys(inputCount, false),
                RespCommand.SETEX => SingleKey(StoreType.Main, LockType.Exclusive),
                RespCommand.SETEXNX => SingleKey(StoreType.Main, LockType.Exclusive),
                RespCommand.SETEXXX => SingleKey(StoreType.Main, LockType.Exclusive),
                RespCommand.DEL => ListKeys(inputCount, StoreType.All, LockType.Exclusive),
                RespCommand.DELIFGREATER => SingleKey(StoreType.Main, LockType.Exclusive),
                RespCommand.EXISTS => ListKeys(inputCount, StoreType.All, LockType.Shared),
                RespCommand.RENAME => SingleKey(StoreType.All, LockType.Exclusive),
                RespCommand.INCR => SingleKey(StoreType.Main, LockType.Exclusive),
                RespCommand.INCRBY => SingleKey(StoreType.Main, LockType.Exclusive),
                RespCommand.INCRBYFLOAT => SingleKey(StoreType.Main, LockType.Exclusive),
                RespCommand.DECR => SingleKey(StoreType.Main, LockType.Exclusive),
                RespCommand.DECRBY => SingleKey(StoreType.Main, LockType.Exclusive),
                RespCommand.SETBIT => SingleKey(StoreType.Main, LockType.Exclusive),
                RespCommand.GETBIT => SingleKey(StoreType.Main, LockType.Shared),
                RespCommand.BITCOUNT => SingleKey(StoreType.Main, LockType.Shared),
                RespCommand.BITPOS => SingleKey(StoreType.Main, LockType.Shared),
                RespCommand.BITFIELD => SingleKey(StoreType.Main, LockType.Exclusive),
                RespCommand.EXPIRE => SingleKey(StoreType.All, LockType.Exclusive),
                RespCommand.PEXPIRE => SingleKey(StoreType.All, LockType.Exclusive),
                RespCommand.PERSIST => SingleKey(StoreType.Main, LockType.Exclusive),
                RespCommand.MGET => ListKeys(inputCount, StoreType.Main, LockType.Shared),
                RespCommand.MSET => MSETKeys(inputCount, LockType.Exclusive),
                RespCommand.MSETNX => MSETKeys(inputCount, LockType.Exclusive),
                RespCommand.UNLINK => ListKeys(inputCount, StoreType.All, LockType.Exclusive),
                RespCommand.GETDEL => SingleKey(StoreType.Main, LockType.Exclusive),
                RespCommand.APPEND => SingleKey(StoreType.Main, LockType.Exclusive),

                RespCommand.BITFIELD_RO => SingleKey(StoreType.Main, LockType.Shared),
                RespCommand.BITOP => SingleWriteKeyListReadKeys(inputCount, false, offset: 1),
                RespCommand.EXPIREAT => SingleKey(StoreType.All, LockType.Exclusive),
                RespCommand.EXPIRETIME => SingleKey(StoreType.All, LockType.Shared),
                RespCommand.GETEX => SingleKey(StoreType.Main, LockType.Exclusive),
                RespCommand.GETSET => SingleKey(StoreType.Main, LockType.Exclusive),

                RespCommand.HEXPIRETIME => HashObjectKeys(command),
                RespCommand.HPEXPIRETIME => HashObjectKeys(command),
                RespCommand.HPERSIST => HashObjectKeys(command),
                RespCommand.HPTTL => HashObjectKeys(command),
                RespCommand.HTTL => HashObjectKeys(command),

                RespCommand.LCS => ListKeys(2, StoreType.Main, LockType.Shared),

                RespCommand.LPOS => ListObjectKeys(command),
                RespCommand.SINTERCARD => SetObjectKeys(command, inputCount),

                RespCommand.PEXPIRETIME => SingleKey(StoreType.All, LockType.Shared),
                RespCommand.PEXPIREAT => SingleKey(StoreType.Main, LockType.Exclusive),
                RespCommand.PSETEX => SingleKey(StoreType.Main, LockType.Exclusive),
                RespCommand.PTTL => SingleKey(StoreType.All, LockType.Shared),
                RespCommand.RENAMENX => SingleKey(StoreType.All, LockType.Exclusive),
                RespCommand.STRLEN => SingleKey(StoreType.Main, LockType.Shared),
                RespCommand.SUBSTR => SingleKey(StoreType.Main, LockType.Shared),
                RespCommand.TTL => SingleKey(StoreType.All, LockType.Shared),
                RespCommand.TYPE => SingleKey(StoreType.All, LockType.Shared),
                RespCommand.SETNX => SingleKey(StoreType.Main, LockType.Exclusive),

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

                // TODO: Actually implement as commands are implemented
                RespCommand.VADD or RespCommand.VCARD or RespCommand.VDIM or RespCommand.VEMB or RespCommand.VGETATTR or RespCommand.VINFO or
                RespCommand.VLINKS or RespCommand.VRANDMEMBER or RespCommand.VREM or RespCommand.VSETATTR or RespCommand.VSIM => SingleKey(StoreType.Object, LockType.Exclusive),

                RespCommand.COSCAN => SingleKey(StoreType.Object, LockType.Shared),
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
                RespCommand.ECHO => 1,
                RespCommand.CONFIG => 1,
                RespCommand.CLIENT => 1,
                RespCommand.PING => 1,
                RespCommand.PUBLISH => 1,
                RespCommand.SPUBLISH => 1,
                RespCommand.SELECT => 1,
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
                RespCommand.ZADD => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.ZREM => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.ZCARD => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.ZPOPMAX => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.ZSCORE => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.ZMSCORE => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.ZCOUNT => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.ZINCRBY => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.ZRANK => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.ZRANGE => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.ZREVRANK => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.ZREMRANGEBYLEX => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.ZREMRANGEBYRANK => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.ZREMRANGEBYSCORE => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.ZLEXCOUNT => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.ZPOPMIN => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.ZRANDMEMBER => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.ZDIFF => ListReadKeysWithCount(LockType.Shared),
                RespCommand.GEOADD => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.GEOHASH => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.GEODIST => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.GEOPOS => SingleKey(StoreType.Object, LockType.Shared),

                RespCommand.GEORADIUS => GeoCommands(RespCommand.GEORADIUS, inputCount),
                RespCommand.GEORADIUS_RO => GeoCommands(RespCommand.GEORADIUS_RO, inputCount),
                RespCommand.GEORADIUSBYMEMBER => GeoCommands(RespCommand.GEORADIUSBYMEMBER, inputCount),
                RespCommand.GEORADIUSBYMEMBER_RO => GeoCommands(RespCommand.GEORADIUSBYMEMBER_RO, inputCount),
                RespCommand.GEOSEARCH => GeoCommands(RespCommand.GEOSEARCH, inputCount),
                RespCommand.GEOSEARCHSTORE => GeoCommands(RespCommand.GEOSEARCHSTORE, inputCount),

                RespCommand.ZDIFFSTORE => SingleWriteKeyListReadKeysWithCount(inputCount),
                RespCommand.ZEXPIRE => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.ZEXPIREAT => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.ZEXPIRETIME => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.ZINTER => ListReadKeysWithCount(LockType.Shared),
                RespCommand.ZINTERCARD => ListReadKeysWithCount(LockType.Shared),
                RespCommand.ZINTERSTORE => SingleWriteKeyListReadKeysWithCount(inputCount),
                RespCommand.ZMPOP => ListReadKeysWithCount(LockType.Exclusive),
                RespCommand.ZPEXPIRE => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.ZPEXPIREAT => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.ZPEXPIRETIME => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.ZPERSIST => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.ZPTTL => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.ZRANGEBYLEX => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.ZRANGEBYSCORE => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.ZRANGESTORE => SingleWriteKeyListReadKeys(2),
                RespCommand.ZREVRANGE => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.ZREVRANGEBYLEX => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.ZREVRANGEBYSCORE => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.ZSCAN => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.ZTTL => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.ZUNION => ListReadKeysWithCount(LockType.Shared),
                RespCommand.ZUNIONSTORE => SingleWriteKeyListReadKeysWithCount(inputCount),
                _ => -1
            };
        }

        private int ListObjectKeys(RespCommand command)
        {
            return command switch
            {
                RespCommand.LPUSH => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.LPOP => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.RPUSH => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.RPOP => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.LLEN => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.LTRIM => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.LRANGE => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.LINDEX => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.LINSERT => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.LREM => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.LSET => SingleKey(StoreType.Object, LockType.Exclusive),

                RespCommand.LMOVE => ListKeys(2, StoreType.Object, LockType.Exclusive),
                RespCommand.LMPOP => ListReadKeysWithCount(LockType.Exclusive),
                RespCommand.LPOS => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.LPUSHX => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.RPOPLPUSH => ListKeys(2, StoreType.Object, LockType.Exclusive),
                RespCommand.RPUSHX => SingleKey(StoreType.Object, LockType.Exclusive),
                _ => -1
            };
        }

        private int HashObjectKeys(RespCommand command)
        {
            return command switch
            {
                RespCommand.HSET => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.HMSET => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.HGET => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.HMGET => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.HGETALL => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.HDEL => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.HLEN => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.HEXISTS => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.HKEYS => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.HVALS => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.HINCRBY => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.HINCRBYFLOAT => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.HSETNX => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.HRANDFIELD => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.HSTRLEN => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.HEXPIRE => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.HEXPIREAT => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.HPEXPIRE => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.HPEXPIREAT => SingleKey(StoreType.Object, LockType.Exclusive),

                RespCommand.HEXPIRETIME => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.HPEXPIRETIME => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.HPERSIST => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.HPTTL => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.HSCAN => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.HTTL => SingleKey(StoreType.Object, LockType.Shared),
                _ => -1
            };
        }

        private int SetObjectKeys(RespCommand command, int inputCount)
        {
            return command switch
            {
                RespCommand.SADD => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.SMEMBERS => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.SREM => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.SCARD => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.SRANDMEMBER => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.SPOP => SingleKey(StoreType.Object, LockType.Exclusive),
                RespCommand.SISMEMBER => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.SMISMEMBER => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.SSCAN => SingleKey(StoreType.Object, LockType.Shared),
                RespCommand.SUNION => ListKeys(inputCount, StoreType.Object, LockType.Shared),
                RespCommand.SUNIONSTORE => SingleWriteKeyListReadKeys(inputCount),
                RespCommand.SDIFF => ListKeys(inputCount, StoreType.Object, LockType.Shared),
                RespCommand.SDIFFSTORE => SingleWriteKeyListReadKeys(inputCount),
                RespCommand.SMOVE => ListKeys(2, StoreType.Object, LockType.Exclusive),
                RespCommand.SINTER => ListKeys(inputCount, StoreType.Object, LockType.Shared),
                RespCommand.SINTERCARD => ListReadKeysWithCount(LockType.Shared),
                RespCommand.SINTERSTORE => SingleWriteKeyListReadKeys(inputCount),
                _ => -1
            };
        }

        /// <summary>
        /// Returns a single for commands that have a single key
        /// </summary>
        private int SingleKey(StoreType storeType, LockType type)
        {
            var key = respSession.parseState.GetArgSliceByRef(0);
            if (storeType is StoreType.Main or StoreType.All)
                SaveKeyEntryToLock(key, false, type);
            if (storeType is StoreType.Object or StoreType.All && !objectStoreBasicContext.IsNull)
                SaveKeyEntryToLock(key, true, type);
            SaveKeyArgSlice(key);
            return 1;
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
        private int ListReadKeysWithCount(LockType type, bool isObject = true)
        {
            var numKeysArg = respSession.GetCommandAsArgSlice(out var success);
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
        private int MSETKeys(int inputCount, LockType type, bool isObject = true)
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
        private int SingleWriteKeyListReadKeys(int inputCount, bool isObject = true, int offset = 0)
        {
            if (inputCount <= offset)
                return 0;

            var key = respSession.parseState.GetArgSliceByRef(offset);
            SaveKeyEntryToLock(key, isObject, LockType.Exclusive);
            SaveKeyArgSlice(key);

            for (var i = offset + 1; i < inputCount; i++)
            {
                key = respSession.parseState.GetArgSliceByRef(i);
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
        private int SingleWriteKeyListReadKeysWithCount(int inputCount, bool isObject = true)
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