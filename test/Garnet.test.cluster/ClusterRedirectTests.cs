// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public unsafe class ClusterRedirectTests
    {
        ClusterTestContext context;

        public enum TestFlags : byte
        {
            READONLY = 0x1,
            READ_WRITE = 0x2,
            SINGLEKEY = 0x4,
            MULTIKEY = 0x8,
            KEY_EXISTS = 0x10,
            ASKING = 0x20
        }

        private static bool CheckFlag(TestFlags input, TestFlags check) => (input & check) == check;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public void TearDown()
        {
            context.TearDown();
        }

        public class CommandInfo(
            string cmdTag,
            string[] setupCmd,
            string testCmd,
            string[] cleanupCmd,
            string response,
            string[] arrayResponse,
ClusterRedirectTests.TestFlags testFlags)
        {
            public string cmdTag = cmdTag;
            public string[] setupCmd = setupCmd;
            public string testCmd = testCmd;
            public string[] cleanupCmd = cleanupCmd;
            public string response = response;
            public string[] arrayResponseOrig = arrayResponse;
            public string[] arrayResponse = arrayResponse;
            public TestFlags testFlags = testFlags;

            private static bool ReplaceKey(ref byte[] key, string cmdStr, int i, out string newCmdStr)
            {
                newCmdStr = cmdStr;
                if (cmdStr != null && cmdStr.Contains($"<key#{i}>"))
                {
                    newCmdStr = cmdStr.Replace($"<key#{i}>", Encoding.ASCII.GetString(key));
                    return true;
                }
                return false;
            }

            private static bool ReplaceValue(ref byte[] value, string cmdStr, int i, out string newCmdStr)
            {
                newCmdStr = cmdStr;
                if (cmdStr != null && cmdStr.Contains($"<s#{i}>"))
                {
                    newCmdStr = cmdStr.Replace($"<s#{i}>", Encoding.ASCII.GetString(value));
                    return true;
                }
                return false;
            }

            public (string[], string, string[], string) GenerateSingleKeyCmdInstance(
                ref ClusterTestUtils clusterTestUtils,
                int keyLen,
                int valueLen,
                out List<int> slots,
                int restrictToSlot = -1)
            {
                var setupCmdInstance = (string[])setupCmd?.Clone();
                var testCmdInstance = testCmd;
                var cleanupCmdInstance = (string[])cleanupCmd?.Clone();
                var responseInstance = response;
                arrayResponse = (string[])arrayResponseOrig?.Clone();

                var i = 0;
                slots = [];
                while (true)
                {
                    var key = new byte[keyLen];
                    var value = new byte[valueLen];
                    if (restrictToSlot == -1)
                        clusterTestUtils.RandomBytes(ref key);
                    else
                    {
                        clusterTestUtils.RandomBytesRestrictedToSlot(ref key, restrictToSlot);
                        ClassicAssert.AreEqual(restrictToSlot, ClusterTestUtils.HashSlot(key));
                    }
                    clusterTestUtils.RandomBytes(ref value);


                    var successKey = false;
                    if (setupCmdInstance != null)
                        for (var j = 0; j < setupCmdInstance.Length; j++)
                            successKey |= ReplaceKey(ref key, setupCmdInstance[j], i, out setupCmdInstance[j]);
                    if (cleanupCmdInstance != null)
                        for (var j = 0; j < cleanupCmdInstance.Length; j++)
                            successKey |= ReplaceKey(ref key, cleanupCmdInstance[j], i, out cleanupCmdInstance[j]);
                    successKey |= ReplaceKey(ref key, testCmdInstance, i, out testCmdInstance);

                    var successValue = false;
                    if (setupCmdInstance != null)
                        for (var j = 0; j < setupCmdInstance.Length; j++)
                            successValue |= ReplaceValue(ref value, setupCmdInstance[j], i, out setupCmdInstance[j]);
                    if (cleanupCmdInstance != null)
                        for (var j = 0; j < cleanupCmdInstance.Length; j++)
                            successValue |= ReplaceValue(ref value, cleanupCmdInstance[j], i, out cleanupCmdInstance[j]);
                    successValue |= ReplaceValue(ref value, testCmdInstance, i, out testCmdInstance);
                    successValue |= ReplaceValue(ref value, responseInstance, i, out responseInstance);
                    if (arrayResponse != null)
                        for (var j = 0; j < arrayResponse.Length; j++)
                            successValue |= ReplaceValue(ref value, arrayResponse[j], i, out arrayResponse[j]);

                    if (successValue || successKey)
                    {
                        if (successKey)
                            slots.Add(ClusterTestUtils.HashSlot(key));
                        i++;
                        continue;
                    }
                    break;
                }
                return (setupCmdInstance, testCmdInstance, cleanupCmdInstance, responseInstance);
            }
        }

        static readonly List<CommandInfo> singleKeyCommands = new List<CommandInfo>()
        {
            #region basicCommands
            //1. GET
            new ("GET", ["SET <key#0> <s#0>"],"GET <key#0>", ["DEL <key#0>"], "<s#0>", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("GET",["SET <key#0> <s#0>"],"GET <key#0>", ["DEL <key#0>"], "<s#0>", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),
            
            //2. SET
            new ("SET", null, "SET <key#0> <s#0>", ["DEL <key#0>"], "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("SET", null, "SET <key#0> <s#0>", ["DEL <key#0>"], "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("SET", ["SET <key#0> <s#0>"], "SET <key#0> <s#0>",["DEL <key#0>"], "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //3.SETEX
            new ("SETEX", null, "SETEX <key#0> 1000 <s#0>", ["DEL <key#0>"], "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("SETEX", null, "SETEX <key#0> 1000 <s#0>", ["DEL <key#0>"], "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("SETEX", ["SETEX <key#0> 1000 <s#0>"], "SETEX <key#0> 1000 <s#0>", ["DEL <key#0>"], "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //4. SETEXNX
            new ("SETEXNX", null, "SET <key#0> <s#0> EX 100 NX", ["DEL <key#0>"], "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("SETEXNX", null, "SET <key#0> <s#0> EX 100 NX", ["DEL <key#0>"], "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            
            //5. DEL
            new ("DEL",["SET <key#0> <s#0>"], "DEL <key#0>", null, "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("DEL",["SET <key#0> <s#0>"], "DEL <key#0>", null, "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),

            //6. EXISTS
            new ("EXISTS", ["SET <key#0> <s#0>"], "EXISTS <key#0>", ["DEL <key#0>"], "1", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("EXISTS", ["SET <key#0> <s#0>"], "EXISTS <key#0>", ["DEL <key#0>"], "1", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //7. INCR
            new ("INCR", ["SET <key#0> 100"], "INCR <key#0>", ["DEL <key#0>"], "101", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("INCR", ["SET <key#0> 100"], "INCR <key#0>", ["DEL <key#0>"], "101", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("INCR", ["SET <key#0> 100"], "INCR <key#0>", ["DEL <key#0>"], "101", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //8. INCRBY
            new ("INCRBY", ["SET <key#0> 100"], "INCRBY <key#0> 10", ["DEL <key#0>"], "110", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("INCRBY", ["SET <key#0> 100"], "INCRBY <key#0> 10", ["DEL <key#0>"], "110", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("INCRBY", ["SET <key#0> 100"], "INCRBY <key#0> 10", ["DEL <key#0>"], "110", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),            

            //9. DECR
            new ("DECR", ["SET <key#0> 100"], "DECR <key#0>", ["DEL <key#0>"], "99", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("DECR", ["SET <key#0> 100"], "DECR <key#0>", ["DEL <key#0>"], "99", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("DECR", ["SET <key#0> 100"], "DECR <key#0>", ["DEL <key#0>"], "99", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),            
            
            //10. DECRBY
            new ("DECRBY", ["SET <key#0> 100"], "DECRBY <key#0> 10", ["DEL <key#0>"], "90", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("DECRBY", ["SET <key#0> 100"], "DECRBY <key#0> 10", ["DEL <key#0>"], "90", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("DECRBY", ["SET <key#0> 100"], "DECRBY <key#0> 10", ["DEL <key#0>"], "90", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),            

            //11. EXPIRE
            new ("EXPIRE", ["SET <key#0> <s#0>"], "EXPIRE <key#0> 100", ["DEL <key#0>"], "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("EXPIRE", ["SET <key#0> <s#0>"], "EXPIRE <key#0> 100", ["DEL <key#0>"], "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("EXPIRE", ["SET <key#0> <s#0>"], "EXPIRE <key#0> 100", ["DEL <key#0>"], "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),
            #endregion

            #region bitmapCommands
            //1. SETBIT
            new ("SETBIT", null, "SETBIT <key#0> 13 1", ["DEL <key#0>"], "0", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("SETBIT", null, "SETBIT <key#0> 13 1", ["DEL <key#0>"], "0", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("SETBIT", ["SETBIT <key#0> 13 1"], "SETBIT <key#0> 13 1", ["DEL <key#0>"], "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //2. GETBIT
            new ("GETBIT", ["SETBIT <key#0> 13 1"], "GETBIT <key#0> 13", ["DEL <key#0>"], "1", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("GETBIT", ["SETBIT <key#0> 13 1"], "GETBIT <key#0> 13", ["DEL <key#0>"], "1", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //3. BITCOUNT
            new ("BITCOUNT", ["SET <key#0> abc"], "BITCOUNT <key#0>", ["DEL <key#0>"], "10", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("BITCOUNT", ["SET <key#0> abc"], "BITCOUNT <key#0>", ["DEL <key#0>"], "10", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //4. BITPOS
            new ("BITPOS", ["SETBIT <key#0> 13 1"], "BITPOS <key#0> 1", ["DEL <key#0>"], "13", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("BITPOS", ["SETBIT <key#0> 13 1"], "BITPOS <key#0> 1", ["DEL <key#0>"], "13", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //5. BITFIELD
            new ("BITFIELD", null, "BITFIELD <key#0> SET u16 13 16 GET u16 13 INCRBY u16 13 10", ["DEL <key#0>"], null, [ "0", "16", "26" ], (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("BITFIELD", null, "BITFIELD <key#0> SET u16 13 16 GET u16 13 INCRBY u16 13 10", ["DEL <key#0>"], null, [ "0", "16", "26" ], (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("BITFIELD", ["BITFIELD <key#0> SET u16 13 16 GET u16 13 INCRBY u16 13 10"], "BITFIELD <key#0> SET u16 13 16 GET u16 13 INCRBY u16 13 10", ["DEL <key#0>"], null, [ "26", "16", "26" ], (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),
            #endregion

            #region hllCommands
            //1. PFADD
            new ("PFADD", null, "PFADD <key#0> <s#0>", ["DEL <key#0>"], "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("PFADD", null, "PFADD <key#0> <s#0>", ["DEL <key#0>"], "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("PFADD", ["PFADD <key#0> <s#0>"], "PFADD <key#0> <s#0>", ["DEL <key#0>"], "0", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            new ("PFADD", null, "PFADD <key#0> <s#0> <s#1> <s#2>", ["DEL <key#0>"], "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("PFADD", null, "PFADD <key#0> <s#0> <s#1> <s#2>", ["DEL <key#0>"], "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("PFADD", ["PFADD <key#0> <s#0> <s#1> <s#2>"], "PFADD <key#0> <s#0> <s#1> <s#2>", ["DEL <key#0>"], "0", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //2. PFCOUNT
            new ("PFCOUNT", ["PFADD <key#0> a b c d e f"],"PFCOUNT <key#0>", ["DEL <key#0>"], "6", null, (TestFlags.SINGLEKEY | TestFlags.READONLY)),
            new ("PFCOUNT", ["PFADD <key#0> a b c d e f"],"PFCOUNT <key#0>", ["DEL <key#0>"], "6", null, (TestFlags.SINGLEKEY | TestFlags.READONLY | TestFlags.ASKING)),
            new ("PFCOUNT", ["PFADD <key#0> a b c d e f"],"PFCOUNT <key#0>", ["DEL <key#0>"], "6", null, (TestFlags.SINGLEKEY | TestFlags.READONLY | TestFlags.KEY_EXISTS)),
            #endregion

            #region sortedSetCommands
            //1. ZADD
            new ("ZADD", null,"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f", ["DEL <key#0>"], "5", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("ZADD", null,"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f", ["DEL <key#0>"], "5", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("ZADD", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZADD <key#0> 15 w 2 z 5 x 3 y 8 r", ["DEL <key#0>"], "5", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //2. ZREM
            new ("ZREM", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZREM <key#0> a b c", ["DEL <key#0>"], "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("ZREM", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZREM <key#0> a b c", ["DEL <key#0>"], "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("ZREM", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZREM <key#0> a b c", ["DEL <key#0>"], "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //3. ZCARD
            new ("ZCARD", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZCARD <key#0>", ["DEL <key#0>"], "5", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("ZCARD", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZCARD <key#0>", ["DEL <key#0>"], "5", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //4. ZPOPMAX
            new ("ZPOPMAX", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZPOPMAX <key#0>", ["DEL <key#0>"], null, ["a", "15"], (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("ZPOPMAX", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZPOPMAX <key#0>", ["DEL <key#0>"], null, ["a", "15"], (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("ZPOPMAX", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZPOPMAX <key#0>", ["DEL <key#0>"], null, ["a", "15"], (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //5. ZPOPMIN
            new ("ZPOPMIN", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZPOPMIN <key#0>", ["DEL <key#0>"], null, ["b", "2"], (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("ZPOPMIN", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZPOPMIN <key#0>", ["DEL <key#0>"], null, ["b", "2"], (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("ZPOPMIN", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZPOPMIN <key#0>", ["DEL <key#0>"], null, ["b", "2"], (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //6. ZSCORE
            new ("ZSCORE", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZSCORE <key#0> f", ["DEL <key#0>"], "8", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("ZSCORE", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZSCORE <key#0> f", ["DEL <key#0>"], "8", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //7. ZLEXCOUNT
            new ("ZLEXCOUNT", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZLEXCOUNT <key#0> - +", ["DEL <key#0>"], "5", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("ZLEXCOUNT", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZLEXCOUNT <key#0> - +", ["DEL <key#0>"], "5", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //8. ZCOUNT
            new ("ZCOUNT", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZCOUNT <key#0> -inf +inf", ["DEL <key#0>"], "5", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("ZCOUNT", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZCOUNT <key#0> -inf +inf", ["DEL <key#0>"], "5", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //9. ZINCRBY
            new ("ZINCRBY", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZINCRBY <key#0> 10 c", ["DEL <key#0>"], "15", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("ZINCRBY", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZINCRBY <key#0> 10 c", ["DEL <key#0>"], "15", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("ZINCRBY", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZINCRBY <key#0> 10 c", ["DEL <key#0>"], "15", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //10. ZRANK, TryZREVRANK
            new ("ZRANK", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZRANK <key#0> a", ["DEL <key#0>"], "4", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("ZRANK", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZRANK <key#0> a", ["DEL <key#0>"], "4", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //11. ZRANGE, RANGEBYSCORE, ZREVRANGE
            new ("ZRANGE", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZRANGE <key#0> 4 7", ["DEL <key#0>"], null, [ "a" ], (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("ZRANGE", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZRANGE <key#0> 4 7", ["DEL <key#0>"], null, [ "a" ], (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //12. ZREMRANGEBYLEX
            new ("ZREMRANGEBYLEX", ["ZADD <key#0> 0 aaaa 0 b 0 c 0 d 0 e", "ZADD <key#0> 0 foo 0 zap 0 zip 0 ALPHA 0 alpha"], "ZREMRANGEBYLEX <key#0> [alpha [omega", ["DEL <key#0>"], "6", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("ZREMRANGEBYLEX", [ "ZADD <key#0> 0 aaaa 0 b 0 c 0 d 0 e", "ZADD <key#0> 0 foo 0 zap 0 zip 0 ALPHA 0 alpha"], "ZREMRANGEBYLEX <key#0> [alpha [omega", ["DEL <key#0>"], "6", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("ZREMRANGEBYLEX", [ "ZADD <key#0> 0 aaaa 0 b 0 c 0 d 0 e", "ZADD <key#0> 0 foo 0 zap 0 zip 0 ALPHA 0 alpha"], "ZREMRANGEBYLEX <key#0> [alpha [omega", ["DEL <key#0>"], "6", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //13. ZREMRANGEBYRANK
            new ("ZREMRANGEBYRANK", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZREMRANGEBYRANK <key#0> 4 10", ["DEL <key#0>"], "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("ZREMRANGEBYRANK", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZREMRANGEBYRANK <key#0> 4 10", ["DEL <key#0>"], "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("ZREMRANGEBYRANK", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZREMRANGEBYRANK <key#0> 4 10", ["DEL <key#0>"], "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //14. ZREMRANGEBYSCORE
            new ("ZREMRANGEBYSCORE", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZREMRANGEBYSCORE <key#0> 4 10", ["DEL <key#0>"], "2", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("ZREMRANGEBYSCORE", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZREMRANGEBYSCORE <key#0> 4 10", ["DEL <key#0>"], "2", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("ZREMRANGEBYSCORE", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZREMRANGEBYSCORE <key#0> 4 10", ["DEL <key#0>"], "2", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //15. ZRANDMEMBER
            new ("ZRANDMEMBER", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZRANDMEMBER <key#0>", ["DEL <key#0>"], null, null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("ZRANDMEMBER", ["ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"], "ZRANDMEMBER <key#0>", ["DEL <key#0>"], null, null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),
            #endregion

            #region listCommands
            //1. LPUSH
            new ("LPUSH", null,"LPUSH <key#0> 1 2 3 4", ["DEL <key#0>"], "4", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("LPUSH", null,"LPUSH <key#0> 1 2 3 4", ["DEL <key#0>"], "4", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("LPUSH", ["LPUSH <key#0> 1 2 3 4"],"LPUSH <key#0> 5 6 7 8", ["DEL <key#0>"], "8", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //2.LPOP
            new ("LPOP", ["LPUSH <key#0> hello world"],"LPOP <key#0>", ["DEL <key#0>"], "world", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("LPOP", ["LPUSH <key#0> hello world"],"LPOP <key#0>", ["DEL <key#0>"], "world", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("LPOP", ["LPUSH <key#0> hello world"],"LPOP <key#0>", ["DEL <key#0>"], "world", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //3.LLEN
            new ("LLEN", ["LPUSH <key#0> hello world"],"LLEN <key#0>", ["DEL <key#0>"], "2", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("LLEN", ["LPUSH <key#0> hello world"],"LLEN <key#0>", ["DEL <key#0>"], "2", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //3. LTRIM
            new ("LTRIM", ["LPUSH <key#0> 1 2 3 4"],"LTRIM <key#0> 0 2", ["DEL <key#0>"], "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("LTRIM", ["LPUSH <key#0> 1 2 3 4"],"LTRIM <key#0> 0 2", ["DEL <key#0>"], "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("LTRIM", ["LPUSH <key#0> 1 2 3 4"],"LTRIM <key#0> 0 2", ["DEL <key#0>"], "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //4.LRANGE
            new ("LRANGE", ["LPUSH <key#0> 1 2 3 4 5 6"],"LRANGE <key#0> 0 2", ["DEL <key#0>"], null, ["6", "5", "4"], (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("LRANGE", ["LPUSH <key#0> 1 2 3 4 5 6"],"LRANGE <key#0> 0 2", ["DEL <key#0>"], null, ["6", "5", "4"], (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //5.LINDEX
            new ("LINDEX", ["LPUSH <key#0> 1 2 3 4 5 6"],"LINDEX <key#0> 2", ["DEL <key#0>"], "4", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("LINDEX", ["LPUSH <key#0> 1 2 3 4 5 6"],"LINDEX <key#0> 2", ["DEL <key#0>"], "4", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //6. LINSERT
            new ("LINSERT", ["LPUSH <key#0> world hello"],"LINSERT <key#0> BEFORE world there", ["DEL <key#0>"], "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("LINSERT", ["LPUSH <key#0> world hello"],"LINSERT <key#0> BEFORE world there", ["DEL <key#0>"], "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("LINSERT", ["LPUSH <key#0> world hello"],"LINSERT <key#0> BEFORE world there", ["DEL <key#0>"], "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //7. LREM
            new ("LREM", ["LPUSH <key#0> a b c a d e a f"],"LREM <key#0> 0 a", ["DEL <key#0>"], "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("LREM", ["LPUSH <key#0> a b c a d e a f"],"LREM <key#0> 0 a", ["DEL <key#0>"], "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("LREM", ["LPUSH <key#0> a b c a d e a f"],"LREM <key#0> 0 a", ["DEL <key#0>"], "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),
            #endregion

            #region setCommands
            //1. SADD
            new ("SADD", null,"SADD <key#0> 1 2 3 4", ["DEL <key#0>"], "4", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("SADD", null,"SADD <key#0> 1 2 3 4", ["DEL <key#0>"], "4", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("SADD", ["SADD <key#0> 1 2 3 4"],"SADD <key#0> 5 6 7 8", ["DEL <key#0>"], "4", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //2. SMEMBERS
            new ("SMEMBERS", ["SADD <key#0> 1 2 3 4"],"SMEMBERS <key#0>", ["DEL <key#0>"], null, ["1", "2", "3", "4"], (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("SMEMBERS", ["SADD <key#0> 1 2 3 4"],"SMEMBERS <key#0>", ["DEL <key#0>"], null, ["1", "2", "3", "4"], (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //3. SREM
            new ("SREM", ["SADD <key#0> a b c a d e a f"],"SREM <key#0> a b c", ["DEL <key#0>"], "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("SREM", ["SADD <key#0> a b c a d e a f"],"SREM <key#0> a b c", ["DEL <key#0>"], "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("SREM", ["SADD <key#0> a b c a d e a f"],"SREM <key#0> a b c", ["DEL <key#0>"], "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //4. SCARD
            new ("SCARD", ["SADD <key#0> hello world"],"SCARD <key#0>", ["DEL <key#0>"], "2", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("SCARD", ["SADD <key#0> hello world"],"SCARD <key#0>", ["DEL <key#0>"], "2", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //5.SPOP
            new ("SPOP", ["SADD <key#0> a b c d e f"],"SPOP <key#0>", ["DEL <key#0>"], null, null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("SPOP", ["SADD <key#0> a b c d e f"],"SPOP <key#0>", ["DEL <key#0>"], null, null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("SPOP", ["SADD <key#0> a b c d e f"],"SPOP <key#0>", ["DEL <key#0>"], null, null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),
            #endregion

            #region hashmapCommands
            //1. HSET, HMSET, HSETNX
            new ("HSET", null,"HSET <key#0> a 1 b 2 c 3 d 4", ["DEL <key#0>"], "4", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("HSET", null,"HSET <key#0> a 1 b 2 c 3 d 4", ["DEL <key#0>"], "4", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("HSET", ["HSET <key#0> a 1 b 2 c 3 d 4"],"HSET <key#0> e 1 f 2 g 3 h 4", ["DEL <key#0>"], "4", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //2. HGET, HRANDFIELD
            new ("HGET", ["HSET <key#0> a 1 b 2 c 3 d 4"],"HGET <key#0> c", ["DEL <key#0>"], "3", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("HGET", ["HSET <key#0> a 1 b 2 c 3 d 4"],"HGET <key#0> c", ["DEL <key#0>"], "3", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //3. HDEL
            new ("HDEL", ["HSET <key#0> a 1 b 2 c 3 d 4"],"HDEL <key#0> c", ["DEL <key#0>"], "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("HDEL", ["HSET <key#0> a 1 b 2 c 3 d 4"],"HDEL <key#0> c", ["DEL <key#0>"], "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("HDEL", ["HSET <key#0> a 1 b 2 c 3 d 4"],"HDEL <key#0> c", ["DEL <key#0>"], "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //4. HLEN
            new ("HLEN", ["HSET <key#0> a 1 b 2 c 3 d 4"],"HLEN <key#0>", ["DEL <key#0>"], "4", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("HLEN", ["HSET <key#0> a 1 b 2 c 3 d 4"],"HLEN <key#0>", ["DEL <key#0>"], "4", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //5. HEXISTS
            new ("HEXISTS", ["HSET <key#0> a 1 b 2 c 3 d 4"],"HEXISTS <key#0> c", ["DEL <key#0>"], "1", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("HEXISTS", ["HSET <key#0> a 1 b 2 c 3 d 4"],"HEXISTS <key#0> c", ["DEL <key#0>"], "1", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //6. HKEYS
            new ("HKEYS", ["HSET <key#0> a 1 b 2 c 3 d 4"],"HKEYS <key#0>", ["DEL <key#0>"], null, ["a", "b", "c", "d"], (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("HKEYS", ["HSET <key#0> a 1 b 2 c 3 d 4"],"HKEYS <key#0>", ["DEL <key#0>"], null, ["a", "b", "c", "d"], (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //7. HVALS
            new ("HVALS", ["HSET <key#0> a 1 b 2 c 3 d 4"],"HVALS <key#0>", ["DEL <key#0>"], null, ["1", "2", "3", "4"], (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("HVALS", ["HSET <key#0> a 1 b 2 c 3 d 4"],"HVALS <key#0>", ["DEL <key#0>"], null, ["1", "2", "3", "4"], (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //8. HINCRBY
            new ("HINCRBY", ["HSET <key#0> a 1 b 2 c 3 d 4"],"HINCRBY <key#0> c 10", ["DEL <key#0>"], "13", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("HINCRBY", ["HSET <key#0> a 1 b 2 c 3 d 4"],"HINCRBY <key#0> c 10", ["DEL <key#0>"], "13", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("HINCRBY", ["HSET <key#0> a 1 b 2 c 3 d 4"],"HINCRBY <key#0> c 10", ["DEL <key#0>"], "13", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //9. HINCRBYFLOAT
            new ("HINCRBYFLOAT", ["HSET <key#0> a 1 b 2 c 3 d 4"],"HINCRBYFLOAT <key#0> c 10", ["DEL <key#0>"], "13", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("HINCRBYFLOAT", ["HSET <key#0> a 1 b 2 c 3 d 4"],"HINCRBYFLOAT <key#0> c 10", ["DEL <key#0>"], "13", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("HINCRBYFLOAT", ["HSET <key#0> a 1 b 2 c 3 d 4"],"HINCRBYFLOAT <key#0> c 10", ["DEL <key#0>"], "13", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),
            #endregion

            #region geoCommands
            //1. GEOADD
            new ("GEOADD", null,"GEOADD <key#0> 13.361389 38.115556 P 15.087269 37.502669 C", ["DEL <key#0>"], "2", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new ("GEOADD", null,"GEOADD <key#0> 13.361389 38.115556 P 15.087269 37.502669 C", ["DEL <key#0>"], "2", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("GEOADD", ["GEOADD <key#0> 13.361389 38.115556 P 15.087269 37.502669 C"],"GEOADD <key#0> 13.361389 38.115556 P 15.087269 37.502669 C", ["DEL <key#0>"], "0", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //2. GEOHASH
            new ("GEOHASH", ["GEOADD <key#0> 13.361389 38.115556 P 15.087269 37.502669 C"],"GEOHASH <key#0> P C", ["DEL <key#0>"], null, null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("GEOHASH", ["GEOADD <key#0> 13.361389 38.115556 P 15.087269 37.502669 C"],"GEOHASH <key#0> P C", ["DEL <key#0>"], null, null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //3. GEODIST, GEOSEARCH
            new ("GEODIST", ["GEOADD <key#0> 13.361389 38.115556 P 15.087269 37.502669 C"],"GEODIST <key#0> P C km", ["DEL <key#0>"], "166.27412635918458", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new ("GEODIST", ["GEOADD <key#0> 13.361389 38.115556 P 15.087269 37.502669 C"],"GEODIST <key#0> P C km", ["DEL <key#0>"], "166.27412635918458", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),
            #endregion
        };

        static readonly List<CommandInfo> multiKeyCommands = new()
        {
            #region basicCommands
            //1. MSET
            new ("MSET", null, "MSET <key#0> <s#0> <key#1> <s#1> <key#2> <s#2>", ["DEL <key#0>", "DEL <key#1>", "DEL <key#2>"], "OK", null, (TestFlags.MULTIKEY | TestFlags.READ_WRITE)),
            new ("MSET", null, "MSET <key#0> <s#0> <key#1> <s#1> <key#2> <s#2>", ["DEL <key#0>", "DEL <key#1>", "DEL <key#2>"], "OK", null, (TestFlags.MULTIKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("MSET", ["MSET <key#0> <s#0> <key#1> <s#1> <key#2> <s#2>"], "MSET <key#0> <s#0> <key#1> <s#1> <key#2> <s#2>", ["DEL <key#0>", "DEL <key#1>", "DEL <key#2>"], "OK", null, (TestFlags.MULTIKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //2. MGET
            new ("MGET", ["MSET <key#0> <s#0> <key#1> <s#1> <key#2> <s#2>"],"MGET <key#0> <key#1> <key#2>", ["DEL <key#0>", "DEL <key#1>", "DEL <key#2>"], null, ["<s#0>", "<s#1>", "<s#2>"], (TestFlags.READONLY | TestFlags.MULTIKEY | TestFlags.KEY_EXISTS)),
            new ("MGET", ["MSET <key#0> <s#0> <key#1> <s#1> <key#2> <s#2>"],"MGET <key#0> <key#1> <key#2>", ["DEL <key#0>", "DEL <key#1>", "DEL <key#2>"], null, ["<s#0>", "<s#1>", "<s#2>"], (TestFlags.READONLY | TestFlags.MULTIKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //3. EXISTS
            new ("EXISTS", ["MSET <key#0> <s#0> <key#1> <s#1> <key#2> <s#2>"],"EXISTS <key#0> <key#1> <key#2>", ["DEL <key#0>", "DEL <key#1>", "DEL <key#2>"], "3", ["<s#0>", "<s#1>", "<s#2>"], (TestFlags.READONLY | TestFlags.MULTIKEY | TestFlags.KEY_EXISTS)),
            new ("EXISTS", ["MSET <key#0> <s#0> <key#1> <s#1> <key#2> <s#2>"],"EXISTS <key#0> <key#1> <key#2>", ["DEL <key#0>", "DEL <key#1>", "DEL <key#2>"], "3", ["<s#0>", "<s#1>", "<s#2>"], (TestFlags.READONLY | TestFlags.MULTIKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),
            #endregion

            #region hllCommands
            //1. PFCOUNT: because PFCOUNT is caching the estimate HLL it is technically updating the data structure so it is treated as R/W command
            new ("PFCOUNT", ["PFADD <key#0> h e l l o", "PFADD <key#1> w o r l d", "PFADD <key#2> t e s t"], "PFCOUNT <key#0> <key#1> <key#2>", ["DEL <key#0>", "DEL <key#1>", "DEL <key#2>"], "9", null, (TestFlags.MULTIKEY | TestFlags.READONLY)),
            new ("PFCOUNT", ["PFADD <key#0> h e l l o", "PFADD <key#1> w o r l d", "PFADD <key#2> t e s t"], "PFCOUNT <key#0> <key#1> <key#2>", ["DEL <key#0>", "DEL <key#1>", "DEL <key#2>"], "9", null, (TestFlags.MULTIKEY | TestFlags.READONLY | TestFlags.ASKING)),
            new ("PFCOUNT", ["PFADD <key#0> h e l l o", "PFADD <key#1> w o r l d", "PFADD <key#2> t e s t"], "PFCOUNT <key#0> <key#1> <key#2>", ["DEL <key#0>", "DEL <key#1>", "DEL <key#2>"], "9", null, (TestFlags.MULTIKEY | TestFlags.READONLY | TestFlags.KEY_EXISTS)),

            //2. PFMERGE
            new ("PFMERGE", ["PFADD <key#0> h e l l o", "PFADD <key#1> w o r l d"], "PFMERGE <key#0> <key#1>", ["DEL <key#0>", "DEL <key#1>"], "OK", null, (TestFlags.MULTIKEY | TestFlags.READ_WRITE)),
            new ("PFMERGE", ["PFADD <key#0> h e l l o", "PFADD <key#1> w o r l d"], "PFMERGE <key#0> <key#1>", ["DEL <key#0>", "DEL <key#1>"], "OK", null, (TestFlags.MULTIKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new ("PFMERGE", ["PFADD <key#0> h e l l o", "PFADD <key#1> w o r l d"], "PFMERGE <key#0> <key#1>", ["DEL <key#0>", "DEL <key#1>"], "OK", null, (TestFlags.MULTIKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //3. PFMERGE
            new ("PFMERGE", ["PFADD <key#0> h e l l o", "PFADD <key#1> w o r l d", "PFADD <key#2> t e s t"], "PFMERGE <key#0> <key#1> <key#2>", ["DEL <key#0>", "DEL <key#1>", "DEL <key#2>"], "OK", null, (TestFlags.MULTIKEY | TestFlags.READ_WRITE)),
            #endregion  

            #region bitmapCommands
            //1. BITOP
            new ("BITOP", ["MSET <key#0> hello <key#1> world <key#2> test"],"BITOP AND <key#3> <key#0> <key#1> <key#2>", ["DEL <key#0>", "DEL <key#1>", "DEL <key#2>", "DEL <key#3>"], "5", null, (TestFlags.MULTIKEY | TestFlags.READ_WRITE)),
            new ("BITOP", ["MSET <key#0> hello <key#1> world <key#2> test"],"BITOP AND <key#3> <key#0> <key#1> <key#2>", ["DEL <key#0>", "DEL <key#1>", "DEL <key#2>", "DEL <key#3>"], "5", null, (TestFlags.MULTIKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            #endregion

            #region sortedSetCommands
            //2. ZDIFF
            new ("ZDIFF", ["ZADD <key#0> 1 one 2 two 3 three", "ZADD <key#1> 1 one 2 two"],"ZDIFF 2 <key#0> <key#1> WITHSCORES", ["DEL <key#0>", "DEL <key#1>"], null, ["three", "3"], (TestFlags.READONLY | TestFlags.MULTIKEY | TestFlags.KEY_EXISTS)),
            new ("ZDIFF", ["ZADD <key#0> 1 one 2 two 3 three", "ZADD <key#1> 1 one 2 two"],"ZDIFF 2 <key#0> <key#1> WITHSCORES", ["DEL <key#0>", "DEL <key#1>"], null, ["three", "3"], (TestFlags.READONLY | TestFlags.MULTIKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),
            #endregion
        };

        private static (ResponseState, string, string[]) SendToNodeFromSlot(ref LightClientRequest[] connections, string cmd, int slot, string cmdTag, bool checkAssert = true)
        {
            var nodeIndex = ClusterTestUtils.GetSourceNodeIndexFromSlot(ref connections, (ushort)slot);
            var result = connections[nodeIndex].SendCommand(cmd);

            var status = ClusterTestUtils.ParseResponseState(
                result,
                out _,
                out _,
                out var value,
                out var values);
            if (checkAssert)
                ClassicAssert.AreEqual(status, ResponseState.OK, cmdTag);
            return (status, value, values);
        }

        private (ResponseState, string, string[]) SendAndRedirectToNode(ref LightClientRequest[] connections, string cmd, int slot, string cmdTag)
        {
            var nodeIndex = ClusterTestUtils.GetSourceNodeIndexFromSlot(ref connections, (ushort)slot);
            var otherNodeIndex = context.r.Next(0, connections.Length);
            while (otherNodeIndex == nodeIndex) otherNodeIndex = context.r.Next(0, connections.Length);

            var result = connections[otherNodeIndex].SendCommand(cmd);
            var status = ClusterTestUtils.ParseResponseState(result, out var _slot, out var endpoint, out var _value, out var _values);
            ClassicAssert.AreEqual(status, ResponseState.MOVED, cmd);
            ClassicAssert.AreEqual(_slot, slot, cmd);
            ClassicAssert.AreEqual(endpoint, connections[nodeIndex].EndPoint, cmd);

            result = connections[nodeIndex].SendCommand(cmd);
            status = ClusterTestUtils.ParseResponseState(result, out _, out _, out _value, out _values);
            ClassicAssert.AreEqual(status, ResponseState.OK, cmdTag);

            return (status, _value, _values);
        }

        private void SendToImportingNode(
            ref LightClientRequest[] connections,
            int sourceNodeIndex,
            int targetNodeIndex,
            CommandInfo command,
            int migrateSlot)
        {
            var (setupCmd, testCmd, cleanCmd, response) = command.GenerateSingleKeyCmdInstance(ref context.clusterTestUtils, 4, 4, out var slots, migrateSlot);
            if (CheckFlag(command.testFlags, TestFlags.ASKING))
            {
                ClusterTestUtils.Asking(ref connections[targetNodeIndex]);
                if (setupCmd != null)
                {
                    for (var j = 0; j < setupCmd.Length; j++)
                    {
                        var resp = connections[targetNodeIndex].SendCommand(setupCmd[j]);
                        var respStatus = ClusterTestUtils.ParseResponseState(resp, out _, out _, out _, out _);
                        ClassicAssert.AreEqual(respStatus, ResponseState.OK);
                        ClusterTestUtils.Asking(ref connections[targetNodeIndex]);
                    }
                }
            }

            var result = connections[targetNodeIndex].SendCommand(testCmd);
            var status = ClusterTestUtils.ParseResponseState(result, out var _slot, out var endpoint, out var _value, out var _values);

            if (CheckFlag(command.testFlags, TestFlags.ASKING))
            {
                ClassicAssert.AreEqual(status, ResponseState.OK, command.cmdTag);
                if (command.response != null)
                    ClassicAssert.AreEqual(_value, response, command.cmdTag);
                else if (command.arrayResponse != null)
                {
                    ClassicAssert.AreEqual(_values.Length, command.arrayResponse.Length);
                    for (var i = 0; i < _values.Length; i++)
                        ClassicAssert.AreEqual(_values[i], command.arrayResponse[i], command.cmdTag);
                }

                if (cleanCmd != null)
                {
                    for (var j = 0; j < cleanCmd.Length; j++)
                    {
                        ClusterTestUtils.Asking(ref connections[targetNodeIndex]);
                        var resp = connections[targetNodeIndex].SendCommand(cleanCmd[j]);
                        var respStatus = ClusterTestUtils.ParseResponseState(resp, out _, out _, out _, out _);
                        ClassicAssert.AreEqual(respStatus, ResponseState.OK);
                    }
                }
            }
            else
            {
                ClassicAssert.AreEqual(status, ResponseState.MOVED);
                ClassicAssert.AreEqual(endpoint, connections[sourceNodeIndex].EndPoint);
                ClassicAssert.AreEqual(_slot, slots[0]);
            }
        }

        private void SendToMigratingNode(
            ref LightClientRequest[] connections,
            int sourceNodeIndex,
            string sourceNodeId,
            int targetNodeIndex,
            string targetNodeId,
            CommandInfo command,
            int migrateSlot)
        {
            var (setupCmd, testCmd, cleanCmd, response) = command.GenerateSingleKeyCmdInstance(ref context.clusterTestUtils, 4, 4, out var slots, migrateSlot);
            ResponseState status = default;
            byte[] result;

            if (CheckFlag(command.testFlags, TestFlags.KEY_EXISTS))
            {
                if (setupCmd != null)
                {
                    for (var j = 0; j < setupCmd.Length; j++)
                    {
                        var resp = connections[sourceNodeIndex].SendCommand(setupCmd[j]);
                        var respStatus = ClusterTestUtils.ParseResponseState(resp, out _, out _, out _, out _);
                        ClassicAssert.AreEqual(respStatus, ResponseState.OK);
                    }
                }
            }

            var respMigrating = ClusterTestUtils.SetSlot(ref connections[sourceNodeIndex], migrateSlot, "MIGRATING", targetNodeId);
            ClassicAssert.AreEqual(respMigrating, "OK");

            result = connections[sourceNodeIndex].SendCommand(testCmd);
            status = ClusterTestUtils.ParseResponseState(result, out _, out var endpoint, out _, out _);

            var respMigratingStable = ClusterTestUtils.SetSlot(ref connections[sourceNodeIndex], migrateSlot, "STABLE", "");
            ClassicAssert.AreEqual(respMigratingStable, "OK");

            if (CheckFlag(command.testFlags, TestFlags.KEY_EXISTS | TestFlags.READONLY))
            {
                ClassicAssert.AreEqual(status, ResponseState.OK, command.cmdTag);
            }
            else if (CheckFlag(command.testFlags, (TestFlags.KEY_EXISTS)))
            {
                ClassicAssert.AreEqual(status, ResponseState.OK, command.cmdTag);
            }
            else
            {
                ClassicAssert.AreEqual(status, ResponseState.ASK, command.cmdTag);
                ClassicAssert.AreEqual(endpoint, connections[targetNodeIndex].EndPoint, command.cmdTag);
            }

            if (CheckFlag(command.testFlags, TestFlags.KEY_EXISTS))
            {
                if (cleanCmd != null)
                {
                    for (var j = 0; j < cleanCmd.Length; j++)
                    {
                        var resp = connections[sourceNodeIndex].SendCommand(cleanCmd[j]);
                        var respStatus = ClusterTestUtils.ParseResponseState(resp, out _, out _, out _, out _);
                        ClassicAssert.AreEqual(respStatus, ResponseState.OK);
                    }
                }
            }
        }

        [Test, Order(1)]
        [Category("CLUSTER")]
        public void ClusterSingleKeyRedirectionTests()
        {
            context.logger.LogDebug("0. ClusterSingleKeyRedirectionTests started");
            var Port = ClusterTestContext.Port;
            var Shards = context.defaultShards;

            context.CreateInstances(Shards, cleanClusterConfig: true);
            context.CreateConnection();

            var connections = ClusterTestUtils.CreateLightRequestConnections([.. Enumerable.Range(Port, Shards)]);
            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            //1. Regular operation redirect responses
            foreach (var command in singleKeyCommands)
            {
                var (setupCmd, testCmd, cleanCmd, response) = command.GenerateSingleKeyCmdInstance(ref context.clusterTestUtils, 4, 4, out var slots);
                if (setupCmd != null)
                    for (var j = 0; j < setupCmd.Length; j++)
                        _ = SendToNodeFromSlot(ref connections, setupCmd[j], slots[0], command.cmdTag);

                if (testCmd != null)
                {
                    var (status, value, values) = SendAndRedirectToNode(ref connections, testCmd, slots[0], command.cmdTag);
                    ClassicAssert.AreEqual(status, ResponseState.OK, command.cmdTag);
                    if (command.response != null)
                    {
                        ClassicAssert.AreEqual(value, response, command.testCmd);
                    }
                    else if (command.arrayResponse != null)
                    {
                        ClassicAssert.AreEqual(values.Length, command.arrayResponse.Length);
                        for (var i = 0; i < values.Length; i++)
                            ClassicAssert.AreEqual(values[i], command.arrayResponse[i], command.cmdTag);
                    }
                }

                if (cleanCmd != null)
                    for (var j = 0; j < cleanCmd.Length; j++)
                        _ = SendToNodeFromSlot(ref connections, cleanCmd[j], slots[0], command.cmdTag);
            }

            //2. Check response during migration
            foreach (var command in singleKeyCommands)
            {
                var migrateSlot = context.r.Next(0, 16384);
                var sourceNodeIndex = ClusterTestUtils.GetSourceNodeIndexFromSlot(ref connections, (ushort)migrateSlot);
                var targetNodeIndex = context.clusterTestUtils.GetRandomTargetNodeIndex(ref connections, sourceNodeIndex);
                var sourceNodeId = ClusterTestUtils.GetNodeIdFromNode(ref connections[sourceNodeIndex]);
                var targetNodeId = ClusterTestUtils.GetNodeIdFromNode(ref connections[targetNodeIndex]);

                var respImporting = ClusterTestUtils.SetSlot(ref connections[targetNodeIndex], migrateSlot, "IMPORTING", sourceNodeId);
                ClassicAssert.AreEqual(respImporting, "OK");
                SendToImportingNode(ref connections, sourceNodeIndex, targetNodeIndex, command, migrateSlot);

                var respImportingStable = ClusterTestUtils.SetSlot(ref connections[targetNodeIndex], migrateSlot, "STABLE", "");
                ClassicAssert.AreEqual(respImportingStable, "OK");
                SendToMigratingNode(ref connections, sourceNodeIndex, sourceNodeId, targetNodeIndex, targetNodeId, command, migrateSlot);
            }

            connections.ToList().ForEach(x => x.Dispose());
            context.logger.LogDebug("1. ClusterSingleKeyRedirectionTests done");
        }

        [Test, Order(2)]
        [Category("CLUSTER")]
        public void ClusterMultiKeyRedirectionTests()
        {
            context.logger.LogDebug("0. ClusterMultiKeyRedirectionTests started");
            var Port = ClusterTestContext.Port;
            var Shards = context.defaultShards;

            context.CreateInstances(Shards, cleanClusterConfig: true);
            context.CreateConnection();

            var connections = ClusterTestUtils.CreateLightRequestConnections([.. Enumerable.Range(Port, Shards)]);
            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            //1. test regular operation redirection
            foreach (var command in multiKeyCommands)
            {
                var restrictedSlot = context.r.Next(0, 16384);
                var (setupCmd, testCmd, cleanCmd, response) = command.GenerateSingleKeyCmdInstance(ref context.clusterTestUtils, 4, 4, out var slots, restrictedSlot);

                for (var i = 0; i < slots.Count; i++)
                    ClassicAssert.AreEqual(slots[i], restrictedSlot);

                if (setupCmd != null)
                    for (var j = 0; j < setupCmd.Length; j++)
                        _ = SendToNodeFromSlot(ref connections, setupCmd[j], slots[0], command.cmdTag);

                if (testCmd != null)
                {
                    var (status, value, values) = SendAndRedirectToNode(ref connections, testCmd, slots[0], command.cmdTag);
                    ClassicAssert.AreEqual(status, ResponseState.OK, command.cmdTag);
                    if (command.response != null)
                    {
                        ClassicAssert.AreEqual(value, response, command.cmdTag);
                    }
                    else
                    {
                        ClassicAssert.AreEqual(values.Length, command.arrayResponse.Length);
                        for (var i = 0; i < values.Length; i++)
                            ClassicAssert.AreEqual(values[i], command.arrayResponse[i], command.cmdTag);
                    }
                }

                if (cleanCmd != null)
                    for (var j = 0; j < cleanCmd.Length; j++)
                        _ = SendToNodeFromSlot(ref connections, cleanCmd[j], slots[0], command.cmdTag);
            }

            //2. test crosslot
            foreach (var command in multiKeyCommands)
            {
                var (setupCmd, testCmd, cleanCmd, response) = command.GenerateSingleKeyCmdInstance(ref context.clusterTestUtils, 4, 4, out var slots);

                if (testCmd != null)
                {
                    var (status, value, values) = SendToNodeFromSlot(ref connections, testCmd, slots.Last(), command.cmdTag, false);
                    ClassicAssert.AreEqual(ResponseState.CROSSSLOT, status, testCmd);
                }
            }

            //3. test migrating
            foreach (var command in multiKeyCommands)
            {
                var migrateSlot = context.r.Next(0, 16384);
                var sourceNodeIndex = ClusterTestUtils.GetSourceNodeIndexFromSlot(ref connections, (ushort)migrateSlot);
                var targetNodeIndex = context.clusterTestUtils.GetRandomTargetNodeIndex(ref connections, sourceNodeIndex);
                var sourceNodeId = ClusterTestUtils.GetNodeIdFromNode(ref connections[sourceNodeIndex]);
                var targetNodeId = ClusterTestUtils.GetNodeIdFromNode(ref connections[targetNodeIndex]);

                var respImporting = ClusterTestUtils.SetSlot(ref connections[targetNodeIndex], migrateSlot, "IMPORTING", sourceNodeId);
                ClassicAssert.AreEqual(respImporting, "OK");
                SendToImportingNode(ref connections, sourceNodeIndex, targetNodeIndex, command, migrateSlot);

                var respImportingStable = ClusterTestUtils.SetSlot(ref connections[targetNodeIndex], migrateSlot, "STABLE", "");
                ClassicAssert.AreEqual(respImportingStable, "OK");
                SendToMigratingNode(ref connections, sourceNodeIndex, sourceNodeId, targetNodeIndex, targetNodeId, command, migrateSlot);
            }

            connections.ToList().ForEach(x => x.Dispose());
            context.logger.LogDebug("1. ClusterMultiKeyRedirectionTests done");
        }

        [Test, Order(3)]
        [Category("CLUSTER")]
        public void ClusterHostnamePreferredRedirectionTests()
        {
            context.logger.LogDebug("0. ClusterHostnamePreferredRedirectionTests started");
            var Port = ClusterTestContext.Port;
            var Shards = context.defaultShards;

            context.CreateInstances(Shards, cleanClusterConfig: true, clusterPreferredEndpointType: ClusterPreferredEndpointType.Hostname, useClusterAnnounceHostname: true);
            context.CreateConnection();

            var connections = ClusterTestUtils.CreateLightRequestConnections([.. Enumerable.Range(Port, Shards)]);
            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            //1. Regular operation redirect responses
            foreach (var command in singleKeyCommands)
            {
                var (setupCmd, testCmd, cleanCmd, response) = command.GenerateSingleKeyCmdInstance(ref context.clusterTestUtils, 4, 4, out var slots);
                if (setupCmd != null)
                    for (var j = 0; j < setupCmd.Length; j++)
                        _ = SendToNodeFromSlot(ref connections, setupCmd[j], slots[0], command.cmdTag);

                if (testCmd != null)
                {
                    var (status, value, values) = SendAndRedirectToNode(ref connections, testCmd, slots[0], command.cmdTag);
                    ClassicAssert.AreEqual(status, ResponseState.OK, command.cmdTag);
                    if (command.response != null)
                    {
                        ClassicAssert.AreEqual(value, response, command.testCmd);
                    }
                    else if (command.arrayResponse != null)
                    {
                        ClassicAssert.AreEqual(values.Length, command.arrayResponse.Length);
                        for (var i = 0; i < values.Length; i++)
                            ClassicAssert.AreEqual(values[i], command.arrayResponse[i], command.cmdTag);
                    }
                }

                if (cleanCmd != null)
                    for (var j = 0; j < cleanCmd.Length; j++)
                        _ = SendToNodeFromSlot(ref connections, cleanCmd[j], slots[0], command.cmdTag);
            }

            //2. Check response during migration
            foreach (var command in singleKeyCommands)
            {
                var migrateSlot = context.r.Next(0, 16384);
                var sourceNodeIndex = ClusterTestUtils.GetSourceNodeIndexFromSlot(ref connections, (ushort)migrateSlot);
                var targetNodeIndex = context.clusterTestUtils.GetRandomTargetNodeIndex(ref connections, sourceNodeIndex);
                var sourceNodeId = ClusterTestUtils.GetNodeIdFromNode(ref connections[sourceNodeIndex]);
                var targetNodeId = ClusterTestUtils.GetNodeIdFromNode(ref connections[targetNodeIndex]);

                var respImporting = ClusterTestUtils.SetSlot(ref connections[targetNodeIndex], migrateSlot, "IMPORTING", sourceNodeId);
                ClassicAssert.AreEqual(respImporting, "OK");
                SendToImportingNode(ref connections, sourceNodeIndex, targetNodeIndex, command, migrateSlot);

                var respImportingStable = ClusterTestUtils.SetSlot(ref connections[targetNodeIndex], migrateSlot, "STABLE", "");
                ClassicAssert.AreEqual(respImportingStable, "OK");
                SendToMigratingNode(ref connections, sourceNodeIndex, sourceNodeId, targetNodeIndex, targetNodeId, command, migrateSlot);
            }

            connections.ToList().ForEach(x => x.Dispose());
            context.logger.LogDebug("1. ClusterHostnamePreferredRedirectionTests done");
        }

        [Test, Order(4)]
        [Category("CLUSTER")]
        public void ClusterHostnamePreferredAndNonHostnameRedirectionTests()
        {
            context.logger.LogDebug("0. ClusterHostnamePreferredAndNonHostnameRedirectionTests started");
            var Port = ClusterTestContext.Port;
            var Shards = context.defaultShards;

            context.CreateInstances(Shards, cleanClusterConfig: true, clusterPreferredEndpointType: ClusterPreferredEndpointType.Hostname);
            context.CreateConnection();

            var connections = ClusterTestUtils.CreateLightRequestConnections([.. Enumerable.Range(Port, Shards)]);
            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            foreach (var command in singleKeyCommands)
            {
                var (setupCmd, testCmd, cleanCmd, response) = command.GenerateSingleKeyCmdInstance(ref context.clusterTestUtils, 4, 4, out var slots);
                if (setupCmd != null)
                    for (var j = 0; j < setupCmd.Length; j++)
                        _ = SendToNodeFromSlot(ref connections, setupCmd[j], slots[0], command.cmdTag);

                if (testCmd != null)
                {
                    var nodeIndex = ClusterTestUtils.GetSourceNodeIndexFromSlot(ref connections, (ushort)slots[0]);
                    var otherNodeIndex = context.r.Next(0, connections.Length);
                    while (otherNodeIndex == nodeIndex) otherNodeIndex = context.r.Next(0, connections.Length);

                    var result = connections[otherNodeIndex].SendCommand(testCmd);
                    ClassicAssert.True(result.AsSpan().StartsWith(ClusterTestUtils.MOVED));

                    var strResp = Encoding.ASCII.GetString(result);
                    var data = strResp.Split(' ');
                    var endpointSplit = data[2].Split(':');

                    ClassicAssert.AreEqual("?", endpointSplit[0]);
                }
            }

            connections.ToList().ForEach(x => x.Dispose());
            context.logger.LogDebug("1. ClusterHostnamePreferredAndNonHostnameRedirectionTests done");
        }

        [Test, Order(5)]
        [Category("CLUSTER")]
        public void ClusterUnknownEndpointPreferredTests()
        {
            context.logger.LogDebug("0. ClusterUnknownEndpointPreferredTests started");
            var Port = ClusterTestContext.Port;
            var Shards = context.defaultShards;

            context.CreateInstances(Shards, cleanClusterConfig: true, clusterPreferredEndpointType: ClusterPreferredEndpointType.Unknown, useClusterAnnounceHostname: false);
            context.CreateConnection();

            var connections = ClusterTestUtils.CreateLightRequestConnections([.. Enumerable.Range(Port, Shards)]);
            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            foreach (var command in singleKeyCommands)
            {
                var (setupCmd, testCmd, cleanCmd, response) = command.GenerateSingleKeyCmdInstance(ref context.clusterTestUtils, 4, 4, out var slots);
                if (setupCmd != null)
                    for (var j = 0; j < setupCmd.Length; j++)
                        _ = SendToNodeFromSlot(ref connections, setupCmd[j], slots[0], command.cmdTag);

                if (testCmd != null)
                {
                    var nodeIndex = ClusterTestUtils.GetSourceNodeIndexFromSlot(ref connections, (ushort)slots[0]);
                    var otherNodeIndex = context.r.Next(0, connections.Length);
                    while (otherNodeIndex == nodeIndex) otherNodeIndex = context.r.Next(0, connections.Length);

                    var result = connections[otherNodeIndex].SendCommand(testCmd);
                    ClassicAssert.True(result.AsSpan().StartsWith(ClusterTestUtils.MOVED));

                    var strResp = Encoding.ASCII.GetString(result);
                    var data = strResp.Split(' ');
                    var endpointSplit = data[2].Split(':');

                    ClassicAssert.AreEqual("?", endpointSplit[0]);
                }
            }

            connections.ToList().ForEach(x => x.Dispose());
            context.logger.LogDebug("1. ClusterUnknownEndpointPreferredTests done");
        }
    }
}