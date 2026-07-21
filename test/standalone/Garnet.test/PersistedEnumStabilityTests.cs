// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Guards the numeric values of enums that are persisted to disk (AOF / checkpoints) or streamed to
    /// replicas. Changing an existing member's value silently corrupts data written by an older version,
    /// so these golden snapshots must only ever GROW (append), never change existing entries. If a test
    /// here fails because you intentionally changed a persisted format, bump AofHeader.AofHeaderVersion
    /// (and add the appropriate legacy remap) rather than editing the expected value.
    /// </summary>
    [TestFixture]
    public class PersistedEnumStabilityTests
    {
        // Frozen numeric values of the RespCommand WRITE block (the only persisted RespCommand values).
        private static readonly Dictionary<RespCommand, int> ExpectedWriteCommandValues = new()
        {
            [RespCommand.APPEND] = 1,
            [RespCommand.BITFIELD] = 2,
            [RespCommand.BZMPOP] = 3,
            [RespCommand.BZPOPMAX] = 4,
            [RespCommand.BZPOPMIN] = 5,
            [RespCommand.DECR] = 6,
            [RespCommand.DECRBY] = 7,
            [RespCommand.DEL] = 8,
            [RespCommand.DELIFEXPIM] = 9,
            [RespCommand.DELIFGREATER] = 10,
            [RespCommand.EXPIRE] = 11,
            [RespCommand.EXPIREAT] = 12,
            [RespCommand.FLUSHALL] = 13,
            [RespCommand.FLUSHDB] = 14,
            [RespCommand.GEOADD] = 15,
            [RespCommand.GEORADIUS] = 16,
            [RespCommand.GEORADIUSBYMEMBER] = 17,
            [RespCommand.GEOSEARCHSTORE] = 18,
            [RespCommand.GETDEL] = 19,
            [RespCommand.GETEX] = 20,
            [RespCommand.GETSET] = 21,
            [RespCommand.HCOLLECT] = 22,
            [RespCommand.HDEL] = 23,
            [RespCommand.HEXPIRE] = 24,
            [RespCommand.HPEXPIRE] = 25,
            [RespCommand.HEXPIREAT] = 26,
            [RespCommand.HPEXPIREAT] = 27,
            [RespCommand.HPERSIST] = 28,
            [RespCommand.HINCRBY] = 29,
            [RespCommand.HINCRBYFLOAT] = 30,
            [RespCommand.HMSET] = 31,
            [RespCommand.HSET] = 32,
            [RespCommand.HSETNX] = 33,
            [RespCommand.INCR] = 34,
            [RespCommand.INCRBY] = 35,
            [RespCommand.INCRBYFLOAT] = 36,
            [RespCommand.LINSERT] = 37,
            [RespCommand.LMOVE] = 38,
            [RespCommand.LMPOP] = 39,
            [RespCommand.LPOP] = 40,
            [RespCommand.LPUSH] = 41,
            [RespCommand.LPUSHX] = 42,
            [RespCommand.LREM] = 43,
            [RespCommand.LSET] = 44,
            [RespCommand.LTRIM] = 45,
            [RespCommand.BLPOP] = 46,
            [RespCommand.BRPOP] = 47,
            [RespCommand.BLMOVE] = 48,
            [RespCommand.BRPOPLPUSH] = 49,
            [RespCommand.BLMPOP] = 50,
            [RespCommand.MIGRATE] = 51,
            [RespCommand.MSET] = 52,
            [RespCommand.MSETNX] = 53,
            [RespCommand.PERSIST] = 54,
            [RespCommand.PEXPIRE] = 55,
            [RespCommand.PEXPIREAT] = 56,
            [RespCommand.PFADD] = 57,
            [RespCommand.PFMERGE] = 58,
            [RespCommand.PSETEX] = 59,
            [RespCommand.RENAME] = 60,
            [RespCommand.RICREATE] = 61,
            [RespCommand.RIDEL] = 62,
            [RespCommand.RIPROMOTE] = 63,
            [RespCommand.RIRESTORE] = 64,
            [RespCommand.RISET] = 65,
            [RespCommand.RESTORE] = 66,
            [RespCommand.RENAMENX] = 67,
            [RespCommand.RPOP] = 68,
            [RespCommand.RPOPLPUSH] = 69,
            [RespCommand.RPUSH] = 70,
            [RespCommand.RPUSHX] = 71,
            [RespCommand.SADD] = 72,
            [RespCommand.SDIFFSTORE] = 73,
            [RespCommand.SET] = 74,
            [RespCommand.SETBIT] = 75,
            [RespCommand.SETEX] = 76,
            [RespCommand.SETEXNX] = 77,
            [RespCommand.SETEXXX] = 78,
            [RespCommand.SETNX] = 79,
            [RespCommand.SETIFMATCH] = 80,
            [RespCommand.SETIFGREATER] = 81,
            [RespCommand.SETWITHETAG] = 82,
            [RespCommand.SETKEEPTTL] = 83,
            [RespCommand.SETKEEPTTLXX] = 84,
            [RespCommand.SETRANGE] = 85,
            [RespCommand.SINTERSTORE] = 86,
            [RespCommand.SMOVE] = 87,
            [RespCommand.SPOP] = 88,
            [RespCommand.SREM] = 89,
            [RespCommand.SUNIONSTORE] = 90,
            [RespCommand.SWAPDB] = 91,
            [RespCommand.UNLINK] = 92,
            [RespCommand.VADD] = 93,
            [RespCommand.VREM] = 94,
            [RespCommand.VSETATTR] = 95,
            [RespCommand.ZADD] = 96,
            [RespCommand.ZCOLLECT] = 97,
            [RespCommand.ZDIFFSTORE] = 98,
            [RespCommand.ZEXPIRE] = 99,
            [RespCommand.ZPEXPIRE] = 100,
            [RespCommand.ZEXPIREAT] = 101,
            [RespCommand.ZPEXPIREAT] = 102,
            [RespCommand.ZPERSIST] = 103,
            [RespCommand.ZINCRBY] = 104,
            [RespCommand.ZMPOP] = 105,
            [RespCommand.ZINTERSTORE] = 106,
            [RespCommand.ZPOPMAX] = 107,
            [RespCommand.ZPOPMIN] = 108,
            [RespCommand.ZRANGESTORE] = 109,
            [RespCommand.ZREM] = 110,
            [RespCommand.ZREMRANGEBYLEX] = 111,
            [RespCommand.ZREMRANGEBYRANK] = 112,
            [RespCommand.ZREMRANGEBYSCORE] = 113,
            [RespCommand.ZUNIONSTORE] = 114,
            [RespCommand.BITOP] = 115,
            [RespCommand.BITOP_AND] = 116,
            [RespCommand.BITOP_OR] = 117,
            [RespCommand.BITOP_XOR] = 118,
            [RespCommand.BITOP_NOT] = 119,
            [RespCommand.BITOP_DIFF] = 120,
        };

        private static readonly Dictionary<HashOperation, int> ExpectedHashOps = new()
        {
            [HashOperation.HCOLLECT] = 0,
            [HashOperation.HEXPIRE] = 1,
            [HashOperation.HTTL] = 2,
            [HashOperation.HPERSIST] = 3,
            [HashOperation.HGET] = 4,
            [HashOperation.HMGET] = 5,
            [HashOperation.HSET] = 6,
            [HashOperation.HMSET] = 7,
            [HashOperation.HSETNX] = 8,
            [HashOperation.HLEN] = 9,
            [HashOperation.HDEL] = 10,
            [HashOperation.HEXISTS] = 11,
            [HashOperation.HGETALL] = 12,
            [HashOperation.HKEYS] = 13,
            [HashOperation.HVALS] = 14,
            [HashOperation.HINCRBY] = 15,
            [HashOperation.HINCRBYFLOAT] = 16,
            [HashOperation.HRANDFIELD] = 17,
            [HashOperation.HSCAN] = 18,
            [HashOperation.HSTRLEN] = 19,
        };

        private static readonly Dictionary<SortedSetOperation, int> ExpectedSortedSetOps = new()
        {
            [SortedSetOperation.ZADD] = 0,
            [SortedSetOperation.ZCARD] = 1,
            [SortedSetOperation.ZPOPMAX] = 2,
            [SortedSetOperation.ZSCORE] = 3,
            [SortedSetOperation.ZREM] = 4,
            [SortedSetOperation.ZCOUNT] = 5,
            [SortedSetOperation.ZINCRBY] = 6,
            [SortedSetOperation.ZRANK] = 7,
            [SortedSetOperation.ZRANGE] = 8,
            [SortedSetOperation.GEOADD] = 9,
            [SortedSetOperation.GEOHASH] = 10,
            [SortedSetOperation.GEODIST] = 11,
            [SortedSetOperation.GEOPOS] = 12,
            [SortedSetOperation.GEOSEARCH] = 13,
            [SortedSetOperation.ZREVRANK] = 14,
            [SortedSetOperation.ZREMRANGEBYLEX] = 15,
            [SortedSetOperation.ZREMRANGEBYRANK] = 16,
            [SortedSetOperation.ZREMRANGEBYSCORE] = 17,
            [SortedSetOperation.ZLEXCOUNT] = 18,
            [SortedSetOperation.ZPOPMIN] = 19,
            [SortedSetOperation.ZRANDMEMBER] = 20,
            [SortedSetOperation.ZDIFF] = 21,
            [SortedSetOperation.ZSCAN] = 22,
            [SortedSetOperation.ZMSCORE] = 23,
            [SortedSetOperation.ZEXPIRE] = 24,
            [SortedSetOperation.ZTTL] = 25,
            [SortedSetOperation.ZPERSIST] = 26,
            [SortedSetOperation.ZCOLLECT] = 27,
        };

        private static readonly Dictionary<ListOperation, int> ExpectedListOps = new()
        {
            [ListOperation.LPOP] = 0,
            [ListOperation.LPUSH] = 1,
            [ListOperation.LPUSHX] = 2,
            [ListOperation.RPOP] = 3,
            [ListOperation.RPUSH] = 4,
            [ListOperation.RPUSHX] = 5,
            [ListOperation.LLEN] = 6,
            [ListOperation.LTRIM] = 7,
            [ListOperation.LRANGE] = 8,
            [ListOperation.LINDEX] = 9,
            [ListOperation.LINSERT] = 10,
            [ListOperation.LREM] = 11,
            [ListOperation.RPOPLPUSH] = 12,
            [ListOperation.LMOVE] = 13,
            [ListOperation.LSET] = 14,
            [ListOperation.BRPOP] = 15,
            [ListOperation.BLPOP] = 16,
            [ListOperation.LPOS] = 17,
        };

        private static readonly Dictionary<SetOperation, int> ExpectedSetOps = new()
        {
            [SetOperation.SADD] = 0,
            [SetOperation.SREM] = 1,
            [SetOperation.SPOP] = 2,
            [SetOperation.SMEMBERS] = 3,
            [SetOperation.SCARD] = 4,
            [SetOperation.SSCAN] = 5,
            [SetOperation.SMOVE] = 6,
            [SetOperation.SRANDMEMBER] = 7,
            [SetOperation.SISMEMBER] = 8,
            [SetOperation.SMISMEMBER] = 9,
            [SetOperation.SUNION] = 10,
            [SetOperation.SUNIONSTORE] = 11,
            [SetOperation.SDIFF] = 12,
            [SetOperation.SDIFFSTORE] = 13,
            [SetOperation.SINTER] = 14,
            [SetOperation.SINTERSTORE] = 15,
        };

        [Test]
        public void RespCommandWriteBlockValuesAreStable()
        {
            foreach (var (cmd, expected) in ExpectedWriteCommandValues)
                ClassicAssert.AreEqual(expected, (int)cmd, $"RespCommand.{cmd} persisted value changed");

            // FirstWriteCommand/LastWriteCommand anchor the persisted range.
            ClassicAssert.AreEqual(1, (int)RespCommand.APPEND, "FirstWriteCommand (APPEND) must be 1");
            ClassicAssert.AreEqual(120, (int)RespCommand.BITOP_DIFF, "LastWriteCommand (BITOP_DIFF) must be 120");
        }

        [Test]
        public void ObjectSubOpValuesAreStable()
        {
            foreach (var (op, expected) in ExpectedHashOps)
                ClassicAssert.AreEqual(expected, (int)op, $"HashOperation.{op} value changed");
            foreach (var (op, expected) in ExpectedSortedSetOps)
                ClassicAssert.AreEqual(expected, (int)op, $"SortedSetOperation.{op} value changed");
            foreach (var (op, expected) in ExpectedListOps)
                ClassicAssert.AreEqual(expected, (int)op, $"ListOperation.{op} value changed");
            foreach (var (op, expected) in ExpectedSetOps)
                ClassicAssert.AreEqual(expected, (int)op, $"SetOperation.{op} value changed");
        }

        [Test]
        public void GarnetObjectTypeBuiltinValuesAreStable()
        {
            ClassicAssert.AreEqual(0, (int)GarnetObjectType.Null);
            ClassicAssert.AreEqual(1, (int)GarnetObjectType.SortedSet);
            ClassicAssert.AreEqual(2, (int)GarnetObjectType.List);
            ClassicAssert.AreEqual(3, (int)GarnetObjectType.Hash);
            ClassicAssert.AreEqual(4, (int)GarnetObjectType.Set);
            ClassicAssert.AreEqual(0xFB, (int)GarnetObjectType.All);
        }

        [Test]
        public void LegacyCustomObjectTypeIdFailsFastOnDeserialize()
        {
            // Older builds based custom object type ids at LastObjectType+1 (=5), which now lies in the
            // reserved built-in band below the fixed custom base (0x40). Deserializing such a record must
            // fail fast rather than silently return null (which would drop the object and lose data).
            var serializer = new GarnetObjectSerializer(new CustomCommandManager());
            var legacyCustomObjectBytes = new byte[] { 5 };

            Assert.Throws<GarnetException>(() => serializer.Deserialize(legacyCustomObjectBytes));
        }

        [Test]
        public void ObjectSubOpsFitInByte()
        {
            // SubId occupies a single header byte (RespInputHeader.subId); every sub-op value must fit.
            ClassicAssert.LessOrEqual(Enum.GetValues<HashOperation>().Max(x => (int)x), 255);
            ClassicAssert.LessOrEqual(Enum.GetValues<SortedSetOperation>().Max(x => (int)x), 255);
            ClassicAssert.LessOrEqual(Enum.GetValues<ListOperation>().Max(x => (int)x), 255);
            ClassicAssert.LessOrEqual(Enum.GetValues<SetOperation>().Max(x => (int)x), 255);
        }

        [Test]
        public void WriteBlockContainsExactlyWriteCommands()
        {
            // Every command in [FirstWriteCommand, LastWriteCommand] must be a write/mutating command,
            // and no mutating command may exist outside that dense block. This makes "only writes are
            // persisted, and they are contiguous at the start" a CI-enforced invariant.
            foreach (var cmd in Enum.GetValues<RespCommand>())
            {
                if (cmd == RespCommand.NONE || cmd == RespCommand.INVALID)
                    continue;
                if (!RespCommandsInfo.TryGetRespCommandInfo(cmd, out var info))
                    continue;

                var isInWriteBlock = (int)cmd >= 1 && (int)cmd <= 120;
                var isWrite = info.AclCategories.HasFlag(RespAclCategories.Write);

                if (isWrite)
                    ClassicAssert.IsTrue(isInWriteBlock, $"Write command {cmd} is outside the write block [1,120]");
                else
                    ClassicAssert.IsFalse(isInWriteBlock, $"Non-write command {cmd} is inside the write block [1,120]");
            }
        }

        [Test]
        public void LegacyV3MappingIsComplete()
        {
            // Building the map throws if any v3 name no longer resolves (rename/removal guard).
            Assert.DoesNotThrow(LegacyRespCommand.EnsureInitialized);

            // A v3 write command translates to the current RespCommand of the same name.
            // In v3, APPEND had value 103 (writes followed reads); it must map to the current APPEND.
            ClassicAssert.AreEqual(RespCommand.APPEND, LegacyRespCommand.FromV3((RespCommand)103));
            // NONE and custom-range values pass through unchanged.
            ClassicAssert.AreEqual(RespCommand.NONE, LegacyRespCommand.FromV3(RespCommand.NONE));
            ClassicAssert.AreEqual(RespCommand.INVALID, LegacyRespCommand.FromV3(RespCommand.INVALID));
        }

        [Test]
        public void BuiltinRespCommandSpaceDoesNotReachCustomRange()
        {
            // Custom raw-string command ids are anchored just below INVALID; the built-in command space
            // must never grow into that range.
            ClassicAssert.Less((int)RespCommandExtensions.LastValidCommand, (ushort)RespCommand.INVALID - 256,
                "Built-in RespCommand space has grown into the custom raw-string command range");
        }
    }
}