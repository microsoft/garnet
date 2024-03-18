// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public unsafe class ClusterRedirectTests
    {
        GarnetServer[] nodes = null;
        Random r = new Random();
        int defaultShards = 3;
        ClusterTestUtils clusterTestUtils;
        string TestFolder;
        EndPointCollection endpoints;
        ILoggerFactory loggerFactory;
        ILogger logger;
        public TextWriter logTextWriter = TestContext.Progress;

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

        private GarnetServer[] SpinupCluster(int shards)
        {
            TestUtils.DeleteDirectory(TestFolder, true);
            endpoints = TestUtils.GetEndPoints(defaultShards, 7000);
            GarnetServer[] nodes = TestUtils.CreateGarnetCluster(
                TestFolder,
                disablePubSub: true,
                endpoints: endpoints,
                cleanClusterConfig: true);
            foreach (var node in nodes)
                node.Start();
            return nodes;
        }

        private void DisposeCluster()
        {
            if (nodes != null)
            {
                for (int i = 0; i < nodes.Length; i++)
                {
                    if (nodes[i] != null)
                    {
                        nodes[i].Dispose();
                        nodes[i] = null;
                    }
                }
            }
            TestUtils.DeleteDirectory(TestFolder, true);
        }

        [SetUp]
        public void Setup()
        {
            TestFolder = TestUtils.UnitTestWorkingDir() + "\\";
            loggerFactory = TestUtils.CreateLoggerFactoryInstance(TestContext.Progress, LogLevel.Error);
            nodes = SpinupCluster(defaultShards);
            r = new Random(674386);
            logger = loggerFactory.CreateLogger(TestContext.CurrentContext.Test.Name);
            clusterTestUtils = new ClusterTestUtils(endpoints, textWriter: logTextWriter);
            clusterTestUtils.Connect(logger);
        }

        [TearDown]
        public void TearDown()
        {
            clusterTestUtils?.Dispose();
            DisposeCluster();
            loggerFactory.Dispose();
        }

        public class CommandInfo
        {
            public string cmdTag;
            public string[] setupCmd;
            public string testCmd;
            public string[] cleanupCmd;
            public string response;
            public string[] arrayResponseOrig;
            public string[] arrayResponse;
            public TestFlags testFlags;

            public CommandInfo(
                string cmdTag,
                string[] setupCmd,
                string testCmd,
                string[] cleanupCmd,
                string response,
                string[] arrayResponse,
                TestFlags testFlags)
            {
                this.cmdTag = cmdTag;
                this.setupCmd = setupCmd;
                this.testCmd = testCmd;
                this.cleanupCmd = cleanupCmd;
                this.response = response;
                this.arrayResponseOrig = arrayResponse;
                this.arrayResponse = arrayResponse;
                this.testFlags = testFlags;
            }

            private bool ReplaceKey(ref byte[] key, string cmdStr, int i, out string newCmdStr)
            {
                newCmdStr = cmdStr;
                if (cmdStr != null && cmdStr.Contains($"<key#{i}>"))
                {
                    newCmdStr = cmdStr.Replace($"<key#{i}>", Encoding.ASCII.GetString(key));
                    return true;
                }
                return false;
            }

            private bool ReplaceValue(ref byte[] value, string cmdStr, int i, out string newCmdStr)
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

                int i = 0;
                slots = new();
                while (true)
                {
                    byte[] key = new byte[keyLen];
                    byte[] value = new byte[valueLen];
                    if (restrictToSlot == -1)
                        clusterTestUtils.RandomBytes(ref key);
                    else
                    {
                        clusterTestUtils.RandomBytesRestrictedToSlot(ref key, restrictToSlot);
                        Assert.AreEqual(restrictToSlot, ClusterTestUtils.HashSlot(key));
                    }
                    clusterTestUtils.RandomBytes(ref value);


                    bool successKey = false;
                    if (setupCmdInstance != null)
                        for (int j = 0; j < setupCmdInstance.Length; j++)
                            successKey |= ReplaceKey(ref key, setupCmdInstance[j], i, out setupCmdInstance[j]);
                    if (cleanupCmdInstance != null)
                        for (int j = 0; j < cleanupCmdInstance.Length; j++)
                            successKey |= ReplaceKey(ref key, cleanupCmdInstance[j], i, out cleanupCmdInstance[j]);
                    successKey |= ReplaceKey(ref key, testCmdInstance, i, out testCmdInstance);

                    bool successValue = false;
                    if (setupCmdInstance != null)
                        for (int j = 0; j < setupCmdInstance.Length; j++)
                            successValue |= ReplaceValue(ref value, setupCmdInstance[j], i, out setupCmdInstance[j]);
                    if (cleanupCmdInstance != null)
                        for (int j = 0; j < cleanupCmdInstance.Length; j++)
                            successValue |= ReplaceValue(ref value, cleanupCmdInstance[j], i, out cleanupCmdInstance[j]);
                    successValue |= ReplaceValue(ref value, testCmdInstance, i, out testCmdInstance);
                    successValue |= ReplaceValue(ref value, responseInstance, i, out responseInstance);
                    if (arrayResponse != null)
                        for (int j = 0; j < arrayResponse.Length; j++)
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
            new CommandInfo("GET", new string[]{"SET <key#0> <s#0>"},"GET <key#0>", new string[]{"DEL <key#0>"}, "<s#0>", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("GET",new string[]{"SET <key#0> <s#0>"},"GET <key#0>", new string[]{"DEL <key#0>"}, "<s#0>", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),
            
            //2. SET
            new CommandInfo("SET", null, "SET <key#0> <s#0>", new string[]{"DEL <key#0>"}, "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("SET", null, "SET <key#0> <s#0>", new string[]{"DEL <key#0>"}, "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("SET", new string[]{"SET <key#0> <s#0>"}, "SET <key#0> <s#0>",new string[]{"DEL <key#0>"}, "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //3.SETEX
            new CommandInfo("SETEX", null, "SETEX <key#0> 1000 <s#0>", new string[]{"DEL <key#0>"}, "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("SETEX", null, "SETEX <key#0> 1000 <s#0>", new string[]{"DEL <key#0>"}, "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("SETEX", new string[]{"SETEX <key#0> 1000 <s#0>"}, "SETEX <key#0> 1000 <s#0>", new string[]{"DEL <key#0>"}, "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //4. SETEXNX
            new CommandInfo("SETEXNX", null, "SET <key#0> <s#0> EX 100 NX", new string[]{"DEL <key#0>"}, "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("SETEXNX", null, "SET <key#0> <s#0> EX 100 NX", new string[]{"DEL <key#0>"}, "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            
            //5. DEL
            new CommandInfo("DEL",new string[]{"SET <key#0> <s#0>"}, "DEL <key#0>", null, "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("DEL",new string[]{"SET <key#0> <s#0>"}, "DEL <key#0>", null, "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),

            //6. EXISTS
            new CommandInfo("EXISTS", new string[]{"SET <key#0> <s#0>"}, "EXISTS <key#0>", new string[]{"DEL <key#0>"}, "1", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("EXISTS", new string[]{"SET <key#0> <s#0>" }, "EXISTS <key#0>", new string[]{"DEL <key#0>"}, "1", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),            

            //7. INCR
            new CommandInfo("INCR", new string[]{"SET <key#0> 100"}, "INCR <key#0>", new string[]{"DEL <key#0>"}, "101", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("INCR", new string[]{"SET <key#0> 100"}, "INCR <key#0>", new string[]{"DEL <key#0>"}, "101", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("INCR", new string[]{"SET <key#0> 100"}, "INCR <key#0>", new string[]{"DEL <key#0>"}, "101", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //8. INCRBY
            new CommandInfo("INCRBY", new string[]{"SET <key#0> 100"}, "INCRBY <key#0> 10", new string[]{"DEL <key#0>"}, "110", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("INCRBY", new string[]{"SET <key#0> 100"}, "INCRBY <key#0> 10", new string[]{"DEL <key#0>"}, "110", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("INCRBY", new string[]{"SET <key#0> 100"}, "INCRBY <key#0> 10", new string[]{"DEL <key#0>"}, "110", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),            

            //9. DECR
            new CommandInfo("DECR", new string[]{"SET <key#0> 100"}, "DECR <key#0>", new string[]{"DEL <key#0>"}, "99", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("DECR", new string[]{"SET <key#0> 100"}, "DECR <key#0>", new string[]{"DEL <key#0>"}, "99", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("DECR", new string[]{"SET <key#0> 100"}, "DECR <key#0>", new string[]{"DEL <key#0>"}, "99", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),            
            
            //10. DECRBY
            new CommandInfo("DECRBY", new string[]{"SET <key#0> 100"}, "DECRBY <key#0> 10", new string[]{"DEL <key#0>"}, "90", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("DECRBY", new string[]{"SET <key#0> 100"}, "DECRBY <key#0> 10", new string[]{"DEL <key#0>"}, "90", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("DECRBY", new string[]{"SET <key#0> 100"}, "DECRBY <key#0> 10", new string[]{"DEL <key#0>"}, "90", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),            

            //11. EXPIRE
            new CommandInfo("EXPIRE", new string[]{"SET <key#0> <s#0>"}, "EXPIRE <key#0> 100", new string[]{"DEL <key#0>"}, "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("EXPIRE", new string[]{"SET <key#0> <s#0>"}, "EXPIRE <key#0> 100", new string[]{"DEL <key#0>"}, "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("EXPIRE", new string[]{"SET <key#0> <s#0>"}, "EXPIRE <key#0> 100", new string[]{"DEL <key#0>"}, "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),
            #endregion

            #region bitmapCommands
            //1. SETBIT
            new CommandInfo("SETBIT", null, "SETBIT <key#0> 13 1", new string[]{"DEL <key#0>"}, "0", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("SETBIT", null, "SETBIT <key#0> 13 1", new string[]{"DEL <key#0>"}, "0", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("SETBIT", new string[]{"SETBIT <key#0> 13 1"}, "SETBIT <key#0> 13 1", new string[]{"DEL <key#0>"}, "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //2. GETBIT
            new CommandInfo("GETBIT", new string[]{"SETBIT <key#0> 13 1"}, "GETBIT <key#0> 13", new string[]{"DEL <key#0>"}, "1", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("GETBIT", new string[]{"SETBIT <key#0> 13 1"}, "GETBIT <key#0> 13", new string[]{"DEL <key#0>"}, "1", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //3. BITCOUNT
            new CommandInfo("BITCOUNT", new string[]{"SET <key#0> abc"}, "BITCOUNT <key#0>", new string[]{"DEL <key#0>"}, "10", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("BITCOUNT", new string[]{"SET <key#0> abc"}, "BITCOUNT <key#0>", new string[]{"DEL <key#0>"}, "10", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //4. BITPOS
            new CommandInfo("BITPOS", new string[]{"SETBIT <key#0> 13 1"}, "BITPOS <key#0> 1", new string[]{"DEL <key#0>"}, "13", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("BITPOS", new string[]{"SETBIT <key#0> 13 1"}, "BITPOS <key#0> 1", new string[]{"DEL <key#0>"}, "13", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //5. BITFIELD
            new CommandInfo("BITFIELD", null, "BITFIELD <key#0> SET u16 13 16 GET u16 13 INCRBY u16 13 10", new string[]{"DEL <key#0>"}, null, new string[]{ "0", "16", "26" }, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("BITFIELD", null, "BITFIELD <key#0> SET u16 13 16 GET u16 13 INCRBY u16 13 10", new string[]{"DEL <key#0>"}, null, new string[]{ "0", "16", "26" }, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("BITFIELD", new string[]{"BITFIELD <key#0> SET u16 13 16 GET u16 13 INCRBY u16 13 10"}, "BITFIELD <key#0> SET u16 13 16 GET u16 13 INCRBY u16 13 10", new string[]{"DEL <key#0>"}, null, new string[]{ "26", "16", "26" }, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),
            #endregion

            #region hllCommands
            //1. PFADD
            new CommandInfo("PFADD", null, "PFADD <key#0> <s#0>", new string[]{"DEL <key#0>"}, "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("PFADD", null, "PFADD <key#0> <s#0>", new string[]{"DEL <key#0>"}, "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("PFADD", new string[]{"PFADD <key#0> <s#0>"}, "PFADD <key#0> <s#0>", new string[]{"DEL <key#0>"}, "0", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            new CommandInfo("PFADD", null, "PFADD <key#0> <s#0> <s#1> <s#2>", new string[]{"DEL <key#0>"}, "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("PFADD", null, "PFADD <key#0> <s#0> <s#1> <s#2>", new string[]{"DEL <key#0>"}, "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("PFADD", new string[]{"PFADD <key#0> <s#0> <s#1> <s#2>"}, "PFADD <key#0> <s#0> <s#1> <s#2>", new string[]{"DEL <key#0>"}, "0", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //2. PFCOUNT
            new CommandInfo("PFCOUNT", new string[]{"PFADD <key#0> a b c d e f"},"PFCOUNT <key#0>", new string[]{"DEL <key#0>"}, "6", null, (TestFlags.SINGLEKEY | TestFlags.READONLY)),
            new CommandInfo("PFCOUNT", new string[]{"PFADD <key#0> a b c d e f"},"PFCOUNT <key#0>", new string[]{"DEL <key#0>"}, "6", null, (TestFlags.SINGLEKEY | TestFlags.READONLY | TestFlags.ASKING)),
            new CommandInfo("PFCOUNT", new string[]{"PFADD <key#0> a b c d e f"},"PFCOUNT <key#0>", new string[]{"DEL <key#0>"}, "6", null, (TestFlags.SINGLEKEY | TestFlags.READONLY | TestFlags.KEY_EXISTS)),
            #endregion

            #region sortedSetCommands
            //1. ZADD
            new CommandInfo("ZADD", null,"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f", new string[]{"DEL <key#0>"}, "5", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("ZADD", null,"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f", new string[]{"DEL <key#0>"}, "5", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("ZADD", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZADD <key#0> 15 w 2 z 5 x 3 y 8 r", new string[]{"DEL <key#0>"}, "5", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //2. ZREM
            new CommandInfo("ZREM", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZREM <key#0> a b c", new string[]{"DEL <key#0>"}, "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("ZREM", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZREM <key#0> a b c", new string[]{"DEL <key#0>"}, "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("ZREM", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZREM <key#0> a b c", new string[]{"DEL <key#0>"}, "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //3. ZCARD
            new CommandInfo("ZCARD", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZCARD <key#0>", new string[]{"DEL <key#0>"}, "5", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("ZCARD", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZCARD <key#0>", new string[]{"DEL <key#0>"}, "5", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //4. ZPOPMAX
            new CommandInfo("ZPOPMAX", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZPOPMAX <key#0>", new string[]{"DEL <key#0>"}, null, new string[]{ "a", "15" }, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("ZPOPMAX", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZPOPMAX <key#0>", new string[]{"DEL <key#0>"}, null, new string[]{ "a", "15" }, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("ZPOPMAX", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZPOPMAX <key#0>", new string[]{"DEL <key#0>"}, null, new string[]{ "a", "15" }, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //5. ZPOPMIN
            new CommandInfo("ZPOPMIN", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZPOPMIN <key#0>", new string[]{"DEL <key#0>"}, null, new string[]{ "b", "2" }, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("ZPOPMIN", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZPOPMIN <key#0>", new string[]{"DEL <key#0>"}, null, new string[]{ "b", "2" }, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("ZPOPMIN", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZPOPMIN <key#0>", new string[]{"DEL <key#0>"}, null, new string[]{ "b", "2" }, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //6. ZSCORE
            new CommandInfo("ZSCORE", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZSCORE <key#0> f", new string[]{"DEL <key#0>"}, "8", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("ZSCORE", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZSCORE <key#0> f", new string[]{"DEL <key#0>"}, "8", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //7. ZLEXCOUNT
            new CommandInfo("ZLEXCOUNT", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZLEXCOUNT <key#0> - +", new string[]{"DEL <key#0>"}, "5", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("ZLEXCOUNT", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZLEXCOUNT <key#0> - +", new string[]{"DEL <key#0>"}, "5", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //8. ZCOUNT
            new CommandInfo("ZCOUNT", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZCOUNT <key#0> -inf +inf", new string[]{"DEL <key#0>"}, "5", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("ZCOUNT", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZCOUNT <key#0> -inf +inf", new string[]{"DEL <key#0>"}, "5", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //9. ZINCRBY
            new CommandInfo("ZINCRBY", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZINCRBY <key#0> 10 c", new string[]{"DEL <key#0>"}, "15", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("ZINCRBY", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZINCRBY <key#0> 10 c", new string[]{"DEL <key#0>"}, "15", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("ZINCRBY", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZINCRBY <key#0> 10 c", new string[]{"DEL <key#0>"}, "15", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //10. ZRANK, TryZREVRANK
            new CommandInfo("ZRANK", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZRANK <key#0> a", new string[]{"DEL <key#0>"}, "4", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("ZRANK", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZRANK <key#0> a", new string[]{"DEL <key#0>"}, "4", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //11. ZRANGE, RANGEBYSCORE, ZREVRANGE
            new CommandInfo("ZRANGE", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZRANGE <key#0> 4 7", new string[]{"DEL <key#0>"}, null, new string[]{ "a" }, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("ZRANGE", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZRANGE <key#0> 4 7", new string[]{"DEL <key#0>"}, null, new string[]{ "a" }, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //12. ZREMRANGEBYLEX
            new CommandInfo("ZREMRANGEBYLEX", new string[]{"ZADD <key#0> 0 aaaa 0 b 0 c 0 d 0 e", "ZADD <key#0> 0 foo 0 zap 0 zip 0 ALPHA 0 alpha"}, "ZREMRANGEBYLEX <key#0> [alpha [omega", new string[]{"DEL <key#0>"}, "6", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("ZREMRANGEBYLEX", new string[]{ "ZADD <key#0> 0 aaaa 0 b 0 c 0 d 0 e", "ZADD <key#0> 0 foo 0 zap 0 zip 0 ALPHA 0 alpha"}, "ZREMRANGEBYLEX <key#0> [alpha [omega", new string[]{"DEL <key#0>"}, "6", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("ZREMRANGEBYLEX", new string[]{ "ZADD <key#0> 0 aaaa 0 b 0 c 0 d 0 e", "ZADD <key#0> 0 foo 0 zap 0 zip 0 ALPHA 0 alpha"}, "ZREMRANGEBYLEX <key#0> [alpha [omega", new string[]{"DEL <key#0>"}, "6", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //13. ZREMRANGEBYRANK
            new CommandInfo("ZREMRANGEBYRANK", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZREMRANGEBYRANK <key#0> 4 10", new string[]{"DEL <key#0>"}, "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("ZREMRANGEBYRANK", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZREMRANGEBYRANK <key#0> 4 10", new string[]{"DEL <key#0>"}, "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("ZREMRANGEBYRANK", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZREMRANGEBYRANK <key#0> 4 10", new string[]{"DEL <key#0>"}, "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //14. ZREMRANGEBYSCORE
            new CommandInfo("ZREMRANGEBYSCORE", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZREMRANGEBYSCORE <key#0> 4 10", new string[]{"DEL <key#0>"}, "2", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("ZREMRANGEBYSCORE", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZREMRANGEBYSCORE <key#0> 4 10", new string[]{"DEL <key#0>"}, "2", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("ZREMRANGEBYSCORE", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZREMRANGEBYSCORE <key#0> 4 10", new string[]{"DEL <key#0>"}, "2", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //15. ZRANDMEMBER
            new CommandInfo("ZRANDMEMBER", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZRANDMEMBER <key#0>", new string[]{"DEL <key#0>"}, null, null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("ZRANDMEMBER", new string[]{"ZADD <key#0> 15 a 2 b 5 c 3 e 8 f"}, "ZRANDMEMBER <key#0>", new string[]{"DEL <key#0>"}, null, null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),
            #endregion

            #region listCommands
            //1. LPUSH
            new CommandInfo("LPUSH", null,"LPUSH <key#0> 1 2 3 4", new string[]{"DEL <key#0>"}, "4", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("LPUSH", null,"LPUSH <key#0> 1 2 3 4", new string[]{"DEL <key#0>"}, "4", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("LPUSH", new string[]{"LPUSH <key#0> 1 2 3 4"},"LPUSH <key#0> 5 6 7 8", new string[]{"DEL <key#0>"}, "8", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //2.LPOP
            new CommandInfo("LPOP", new string[]{"LPUSH <key#0> hello world"},"LPOP <key#0>", new string[]{"DEL <key#0>"}, "world", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("LPOP", new string[]{"LPUSH <key#0> hello world"},"LPOP <key#0>", new string[]{"DEL <key#0>"}, "world", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("LPOP", new string[]{"LPUSH <key#0> hello world"},"LPOP <key#0>", new string[]{"DEL <key#0>"}, "world", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //3.LLEN
            new CommandInfo("LLEN", new string[]{"LPUSH <key#0> hello world"},"LLEN <key#0>", new string[]{"DEL <key#0>"}, "2", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("LLEN", new string[]{"LPUSH <key#0> hello world"},"LLEN <key#0>", new string[]{"DEL <key#0>"}, "2", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //3. LTRIM
            new CommandInfo("LTRIM", new string[]{"LPUSH <key#0> 1 2 3 4"},"LTRIM <key#0> 0 2", new string[]{"DEL <key#0>"}, "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("LTRIM", new string[]{"LPUSH <key#0> 1 2 3 4"},"LTRIM <key#0> 0 2", new string[]{"DEL <key#0>"}, "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("LTRIM", new string[]{"LPUSH <key#0> 1 2 3 4"},"LTRIM <key#0> 0 2", new string[]{"DEL <key#0>"}, "OK", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //4.LRANGE
            new CommandInfo("LRANGE", new string[]{"LPUSH <key#0> 1 2 3 4 5 6"},"LRANGE <key#0> 0 2", new string[]{"DEL <key#0>"}, null, new string[] {"6", "5", "4"}, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("LRANGE", new string[]{"LPUSH <key#0> 1 2 3 4 5 6"},"LRANGE <key#0> 0 2", new string[]{"DEL <key#0>"}, null, new string[] {"6", "5", "4"}, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //5.LINDEX
            new CommandInfo("LINDEX", new string[]{"LPUSH <key#0> 1 2 3 4 5 6"},"LINDEX <key#0> 2", new string[]{"DEL <key#0>"}, "4", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("LINDEX", new string[]{"LPUSH <key#0> 1 2 3 4 5 6"},"LINDEX <key#0> 2", new string[]{"DEL <key#0>"}, "4", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //6. LINSERT
            new CommandInfo("LINSERT", new string[]{"LPUSH <key#0> world hello"},"LINSERT <key#0> BEFORE world there", new string[]{"DEL <key#0>"}, "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("LINSERT", new string[]{"LPUSH <key#0> world hello"},"LINSERT <key#0> BEFORE world there", new string[]{"DEL <key#0>"}, "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("LINSERT", new string[]{"LPUSH <key#0> world hello"},"LINSERT <key#0> BEFORE world there", new string[]{"DEL <key#0>"}, "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //7. LREM
            new CommandInfo("LREM", new string[]{"LPUSH <key#0> a b c a d e a f"},"LREM <key#0> 0 a", new string[]{"DEL <key#0>"}, "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("LREM", new string[]{"LPUSH <key#0> a b c a d e a f"},"LREM <key#0> 0 a", new string[]{"DEL <key#0>"}, "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("LREM", new string[]{"LPUSH <key#0> a b c a d e a f"},"LREM <key#0> 0 a", new string[]{"DEL <key#0>"}, "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),
            #endregion

            #region setCommands
            //1. SADD
            new CommandInfo("SADD", null,"SADD <key#0> 1 2 3 4", new string[]{"DEL <key#0>"}, "4", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("SADD", null,"SADD <key#0> 1 2 3 4", new string[]{"DEL <key#0>"}, "4", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("SADD", new string[]{"SADD <key#0> 1 2 3 4"},"SADD <key#0> 5 6 7 8", new string[]{"DEL <key#0>"}, "4", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //2. SMEMBERS
            new CommandInfo("SMEMBERS", new string[]{"SADD <key#0> 1 2 3 4"},"SMEMBERS <key#0>", new string[]{"DEL <key#0>"}, null, new string[] {"1", "2", "3", "4"}, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("SMEMBERS", new string[]{"SADD <key#0> 1 2 3 4"},"SMEMBERS <key#0>", new string[]{"DEL <key#0>"}, null, new string[] {"1", "2", "3", "4"}, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //3. SREM
            new CommandInfo("SREM", new string[]{"SADD <key#0> a b c a d e a f"},"SREM <key#0> a b c", new string[]{"DEL <key#0>"}, "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("SREM", new string[]{"SADD <key#0> a b c a d e a f"},"SREM <key#0> a b c", new string[]{"DEL <key#0>"}, "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("SREM", new string[]{"SADD <key#0> a b c a d e a f"},"SREM <key#0> a b c", new string[]{"DEL <key#0>"}, "3", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //4. SCARD
            new CommandInfo("SCARD", new string[]{"SADD <key#0> hello world"},"SCARD <key#0>", new string[]{"DEL <key#0>"}, "2", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("SCARD", new string[]{"SADD <key#0> hello world"},"SCARD <key#0>", new string[]{"DEL <key#0>"}, "2", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //5.SPOP
            new CommandInfo("SPOP", new string[]{"SADD <key#0> a b c d e f"},"SPOP <key#0>", new string[]{"DEL <key#0>"}, null, null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("SPOP", new string[]{"SADD <key#0> a b c d e f"},"SPOP <key#0>", new string[]{"DEL <key#0>"}, null, null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("SPOP", new string[]{"SADD <key#0> a b c d e f"},"SPOP <key#0>", new string[]{"DEL <key#0>"}, null, null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),
            #endregion

            #region hashmapCommands
            //1. HSET, HMSET, HSETNX
            new CommandInfo("HSET", null,"HSET <key#0> a 1 b 2 c 3 d 4", new string[]{"DEL <key#0>"}, "4", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("HSET", null,"HSET <key#0> a 1 b 2 c 3 d 4", new string[]{"DEL <key#0>"}, "4", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("HSET", new string[] {"HSET <key#0> a 1 b 2 c 3 d 4"},"HSET <key#0> e 1 f 2 g 3 h 4", new string[]{"DEL <key#0>"}, "4", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //2. HGET, HRANDFIELD
            new CommandInfo("HGET", new string[] {"HSET <key#0> a 1 b 2 c 3 d 4"},"HGET <key#0> c", new string[]{"DEL <key#0>"}, "3", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("HGET", new string[] {"HSET <key#0> a 1 b 2 c 3 d 4"},"HGET <key#0> c", new string[]{"DEL <key#0>"}, "3", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //3. HDEL
            new CommandInfo("HDEL", new string[] {"HSET <key#0> a 1 b 2 c 3 d 4"},"HDEL <key#0> c", new string[]{"DEL <key#0>"}, "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("HDEL", new string[] {"HSET <key#0> a 1 b 2 c 3 d 4"},"HDEL <key#0> c", new string[]{"DEL <key#0>"}, "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("HDEL", new string[] {"HSET <key#0> a 1 b 2 c 3 d 4"},"HDEL <key#0> c", new string[]{"DEL <key#0>"}, "1", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //4. HLEN
            new CommandInfo("HLEN", new string[] {"HSET <key#0> a 1 b 2 c 3 d 4"},"HLEN <key#0>", new string[]{"DEL <key#0>"}, "4", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("HLEN", new string[] {"HSET <key#0> a 1 b 2 c 3 d 4"},"HLEN <key#0>", new string[]{"DEL <key#0>"}, "4", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //5. HEXISTS
            new CommandInfo("HEXISTS", new string[] {"HSET <key#0> a 1 b 2 c 3 d 4"},"HEXISTS <key#0> c", new string[]{"DEL <key#0>"}, "1", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("HEXISTS", new string[] {"HSET <key#0> a 1 b 2 c 3 d 4"},"HEXISTS <key#0> c", new string[]{"DEL <key#0>"}, "1", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //6. HKEYS
            new CommandInfo("HKEYS", new string[] {"HSET <key#0> a 1 b 2 c 3 d 4"},"HKEYS <key#0>", new string[]{"DEL <key#0>"}, null, new string[]{"a", "b", "c", "d"}, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("HKEYS", new string[] {"HSET <key#0> a 1 b 2 c 3 d 4"},"HKEYS <key#0>", new string[]{"DEL <key#0>"}, null, new string[]{"a", "b", "c", "d"}, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //7. HVALS
            new CommandInfo("HVALS", new string[] {"HSET <key#0> a 1 b 2 c 3 d 4"},"HVALS <key#0>", new string[]{"DEL <key#0>"}, null, new string[]{"1", "2", "3", "4"}, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("HVALS", new string[] {"HSET <key#0> a 1 b 2 c 3 d 4"},"HVALS <key#0>", new string[]{"DEL <key#0>"}, null, new string[]{"1", "2", "3", "4"}, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //8. HINCRBY
            new CommandInfo("HINCRBY", new string[] {"HSET <key#0> a 1 b 2 c 3 d 4"},"HINCRBY <key#0> c 10", new string[]{"DEL <key#0>"}, "13", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("HINCRBY", new string[] {"HSET <key#0> a 1 b 2 c 3 d 4"},"HINCRBY <key#0> c 10", new string[]{"DEL <key#0>"}, "13", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("HINCRBY", new string[] {"HSET <key#0> a 1 b 2 c 3 d 4"},"HINCRBY <key#0> c 10", new string[]{"DEL <key#0>"}, "13", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //9. HINCRBYFLOAT
            new CommandInfo("HINCRBYFLOAT", new string[] {"HSET <key#0> a 1 b 2 c 3 d 4"},"HINCRBYFLOAT <key#0> c 10", new string[]{"DEL <key#0>"}, "13", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("HINCRBYFLOAT", new string[] {"HSET <key#0> a 1 b 2 c 3 d 4"},"HINCRBYFLOAT <key#0> c 10", new string[]{"DEL <key#0>"}, "13", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("HINCRBYFLOAT", new string[] {"HSET <key#0> a 1 b 2 c 3 d 4"},"HINCRBYFLOAT <key#0> c 10", new string[]{"DEL <key#0>"}, "13", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),
            #endregion

            #region geoCommands
            //1. GEOADD
            new CommandInfo("GEOADD", null,"GEOADD <key#0> 13.361389 38.115556 P 15.087269 37.502669 C", new string[]{"DEL <key#0>"}, "2", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE)),
            new CommandInfo("GEOADD", null,"GEOADD <key#0> 13.361389 38.115556 P 15.087269 37.502669 C", new string[]{"DEL <key#0>"}, "2", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("GEOADD", new string[]{"GEOADD <key#0> 13.361389 38.115556 P 15.087269 37.502669 C"},"GEOADD <key#0> 13.361389 38.115556 P 15.087269 37.502669 C", new string[]{"DEL <key#0>"}, "0", null, (TestFlags.SINGLEKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //2. GEOHASH
            new CommandInfo("GEOHASH", new string[]{"GEOADD <key#0> 13.361389 38.115556 P 15.087269 37.502669 C"},"GEOHASH <key#0> P C", new string[]{"DEL <key#0>"}, null, null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("GEOHASH", new string[]{"GEOADD <key#0> 13.361389 38.115556 P 15.087269 37.502669 C"},"GEOHASH <key#0> P C", new string[]{"DEL <key#0>"}, null, null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),

            //3. GEODIST, GEOSEARCH
            new CommandInfo("GEODIST", new string[]{"GEOADD <key#0> 13.361389 38.115556 P 15.087269 37.502669 C"},"GEODIST <key#0> P C km", new string[]{"DEL <key#0>"}, "166.27412635918458", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("GEODIST", new string[]{"GEOADD <key#0> 13.361389 38.115556 P 15.087269 37.502669 C"},"GEODIST <key#0> P C km", new string[]{"DEL <key#0>"}, "166.27412635918458", null, (TestFlags.READONLY | TestFlags.SINGLEKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),
            #endregion
        };

        static readonly List<CommandInfo> multiKeyCommands = new List<CommandInfo>()
        {
            #region basicCommands
            //1. MSET
            new CommandInfo("MSET", null, "MSET <key#0> <s#0> <key#1> <s#1> <key#2> <s#2>", new string[]{"DEL <key#0>", "DEL <key#1>", "DEL <key#2>"}, "OK", null, (TestFlags.MULTIKEY | TestFlags.READ_WRITE)),
            new CommandInfo("MSET", null, "MSET <key#0> <s#0> <key#1> <s#1> <key#2> <s#2>", new string[]{"DEL <key#0>", "DEL <key#1>", "DEL <key#2>"}, "OK", null, (TestFlags.MULTIKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("MSET", new string[]{"MSET <key#0> <s#0> <key#1> <s#1> <key#2> <s#2>"}, "MSET <key#0> <s#0> <key#1> <s#1> <key#2> <s#2>", new string[]{"DEL <key#0>", "DEL <key#1>", "DEL <key#2>"}, "OK", null, (TestFlags.MULTIKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //2. MGET
            new CommandInfo("MGET", new string[]{"MSET <key#0> <s#0> <key#1> <s#1> <key#2> <s#2>"},"MGET <key#0> <key#1> <key#2>", new string[]{"DEL <key#0>", "DEL <key#1>", "DEL <key#2>"}, null, new string[]{ "<s#0>", "<s#1>", "<s#2>" }, (TestFlags.READONLY | TestFlags.MULTIKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("MGET", new string[]{"MSET <key#0> <s#0> <key#1> <s#1> <key#2> <s#2>"},"MGET <key#0> <key#1> <key#2>", new string[]{"DEL <key#0>", "DEL <key#1>", "DEL <key#2>"}, null, new string[]{ "<s#0>", "<s#1>", "<s#2>" }, (TestFlags.READONLY | TestFlags.MULTIKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),
            #endregion

            #region hllCommands
            //1. PFCOUNT: because PFCOUNT is caching the estimate HLL it is technically updating the data structure so it is treated as R/W command
            new CommandInfo("PFCOUNT", new string[]{"PFADD <key#0> h e l l o", "PFADD <key#1> w o r l d", "PFADD <key#2> t e s t"}, "PFCOUNT <key#0> <key#1> <key#2>", new string[]{"DEL <key#0>", "DEL <key#1>", "DEL <key#2>"}, "12", null, (TestFlags.MULTIKEY | TestFlags.READONLY)),
            new CommandInfo("PFCOUNT", new string[]{"PFADD <key#0> h e l l o", "PFADD <key#1> w o r l d", "PFADD <key#2> t e s t"}, "PFCOUNT <key#0> <key#1> <key#2>", new string[]{"DEL <key#0>", "DEL <key#1>", "DEL <key#2>"}, "12", null, (TestFlags.MULTIKEY | TestFlags.READONLY | TestFlags.ASKING)),
            new CommandInfo("PFCOUNT", new string[]{"PFADD <key#0> h e l l o", "PFADD <key#1> w o r l d", "PFADD <key#2> t e s t"}, "PFCOUNT <key#0> <key#1> <key#2>", new string[]{"DEL <key#0>", "DEL <key#1>", "DEL <key#2>"}, "12", null, (TestFlags.MULTIKEY | TestFlags.READONLY | TestFlags.KEY_EXISTS)),

            //2. PFMERGE
            new CommandInfo("PFMERGE", new string[]{"PFADD <key#0> h e l l o", "PFADD <key#1> w o r l d"}, "PFMERGE <key#0> <key#1>", new string[]{"DEL <key#0>", "DEL <key#1>"}, "OK", null, (TestFlags.MULTIKEY | TestFlags.READ_WRITE)),
            new CommandInfo("PFMERGE", new string[]{"PFADD <key#0> h e l l o", "PFADD <key#1> w o r l d"}, "PFMERGE <key#0> <key#1>", new string[]{"DEL <key#0>", "DEL <key#1>"}, "OK", null, (TestFlags.MULTIKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            new CommandInfo("PFMERGE", new string[]{"PFADD <key#0> h e l l o", "PFADD <key#1> w o r l d"}, "PFMERGE <key#0> <key#1>", new string[]{"DEL <key#0>", "DEL <key#1>"}, "OK", null, (TestFlags.MULTIKEY | TestFlags.READ_WRITE | TestFlags.KEY_EXISTS)),

            //3. PFMERGE
            new CommandInfo("PFMERGE", new string[]{"PFADD <key#0> h e l l o", "PFADD <key#1> w o r l d", "PFADD <key#2> t e s t"}, "PFMERGE <key#0> <key#1> <key#2>", new string[]{"DEL <key#0>", "DEL <key#1>", "DEL <key#2>"}, "OK", null, (TestFlags.MULTIKEY | TestFlags.READ_WRITE)),
            #endregion  

            #region bitmapCommands
            //1. BITOP
            new CommandInfo("BITOP", new string[]{"MSET <key#0> hello <key#1> world <key#2> test"},"BITOP AND <key#3> <key#0> <key#1> <key#2>", new string[]{"DEL <key#0>", "DEL <key#1>", "DEL <key#2>", "DEL <key#3>"}, "5", null, (TestFlags.MULTIKEY | TestFlags.READ_WRITE)),
            new CommandInfo("BITOP", new string[]{"MSET <key#0> hello <key#1> world <key#2> test"},"BITOP AND <key#3> <key#0> <key#1> <key#2>", new string[]{"DEL <key#0>", "DEL <key#1>", "DEL <key#2>", "DEL <key#3>"}, "5", null, (TestFlags.MULTIKEY | TestFlags.READ_WRITE | TestFlags.ASKING)),
            #endregion

            #region sortedSetCommands
            //2. ZDIFF
            new CommandInfo("ZDIFF", new string[]{"ZADD <key#0> 1 one 2 two 3 three", "ZADD <key#1> 1 one 2 two"},"ZDIFF 2 <key#0> <key#1> WITHSCORES", new string[]{"DEL <key#0>", "DEL <key#1>"}, null, new string[]{"three", "3"}, (TestFlags.READONLY | TestFlags.MULTIKEY | TestFlags.KEY_EXISTS)),
            new CommandInfo("ZDIFF", new string[]{"ZADD <key#0> 1 one 2 two 3 three", "ZADD <key#1> 1 one 2 two"},"ZDIFF 2 <key#0> <key#1> WITHSCORES", new string[]{"DEL <key#0>", "DEL <key#1>"}, null, new string[]{"three", "3"}, (TestFlags.READONLY | TestFlags.MULTIKEY | TestFlags.KEY_EXISTS | TestFlags.ASKING)),
            #endregion
        };

        private (ResponseState, string, string[]) SendToNodeFromSlot(ref LightClientRequest[] connections, string cmd, int slot, string cmdTag, bool checkAssert = true)
        {
            int nodeIndex = ClusterTestUtils.GetSourceNodeIndexFromSlot(ref connections, (ushort)slot);
            var result = connections[nodeIndex].SendCommand(cmd);

            var status = ClusterTestUtils.ParseResponseState(
                result,
                out var _slot,
                out var _address,
                out var _port,
                out var value,
                out var values);
            if (checkAssert)
                Assert.AreEqual(status, ResponseState.OK, cmdTag);
            return (status, value, values);
        }

        private (ResponseState, string, string[]) SendAndRedirectToNode(ref LightClientRequest[] connections, string cmd, int slot, string cmdTag)
        {
            int nodeIndex = ClusterTestUtils.GetSourceNodeIndexFromSlot(ref connections, (ushort)slot);
            int otherNodeIndex = r.Next(0, connections.Length);
            while (otherNodeIndex == nodeIndex) otherNodeIndex = r.Next(0, connections.Length);

            var result = connections[otherNodeIndex].SendCommand(cmd);
            var status = ClusterTestUtils.ParseResponseState(result, out var _slot, out var _address, out var _port, out var _value, out var _values);
            Assert.AreEqual(status, ResponseState.MOVED);
            Assert.AreEqual(_slot, slot);
            Assert.AreEqual(_address, connections[nodeIndex].Address);
            Assert.AreEqual(_port, connections[nodeIndex].Port);

            result = connections[nodeIndex].SendCommand(cmd);
            status = ClusterTestUtils.ParseResponseState(result, out slot, out _address, out _port, out _value, out _values);
            Assert.AreEqual(status, ResponseState.OK, cmdTag);

            return (status, _value, _values);
        }

        private void SendToImportingNode(
            ref LightClientRequest[] connections,
            int sourceNodeIndex,
            int targetNodeIndex,
            CommandInfo command,
            int migrateSlot)
        {
            var (setupCmd, testCmd, cleanCmd, response) = command.GenerateSingleKeyCmdInstance(ref clusterTestUtils, 4, 4, out var slots, migrateSlot);
            if (CheckFlag(command.testFlags, TestFlags.ASKING))
            {
                ClusterTestUtils.Asking(ref connections[targetNodeIndex]);
                if (setupCmd != null)
                {
                    for (int j = 0; j < setupCmd.Length; j++)
                    {
                        var resp = connections[targetNodeIndex].SendCommand(setupCmd[j]);
                        var respStatus = ClusterTestUtils.ParseResponseState(resp, out _, out _, out _, out _, out _);
                        Assert.AreEqual(respStatus, ResponseState.OK);
                        ClusterTestUtils.Asking(ref connections[targetNodeIndex]);
                    }
                }
            }

            var result = connections[targetNodeIndex].SendCommand(testCmd);
            var status = ClusterTestUtils.ParseResponseState(result, out var _slot, out var _address, out var _port, out var _value, out var _values);

            if (CheckFlag(command.testFlags, TestFlags.ASKING))
            {
                Assert.AreEqual(status, ResponseState.OK, command.cmdTag);
                if (command.response != null)
                    Assert.AreEqual(_value, response, command.cmdTag);
                else if (command.arrayResponse != null)
                {
                    Assert.AreEqual(_values.Length, command.arrayResponse.Length);
                    for (int i = 0; i < _values.Length; i++)
                        Assert.AreEqual(_values[i], command.arrayResponse[i], command.cmdTag);
                }

                if (cleanCmd != null)
                {
                    for (int j = 0; j < cleanCmd.Length; j++)
                    {
                        ClusterTestUtils.Asking(ref connections[targetNodeIndex]);
                        var resp = connections[targetNodeIndex].SendCommand(cleanCmd[j]);
                        var respStatus = ClusterTestUtils.ParseResponseState(resp, out _, out _, out _, out _, out _);
                        Assert.AreEqual(respStatus, ResponseState.OK);
                    }
                }
            }
            else
            {
                Assert.AreEqual(status, ResponseState.MOVED);
                Assert.AreEqual(_address, connections[sourceNodeIndex].Address);
                Assert.AreEqual(_port, connections[sourceNodeIndex].Port);
                Assert.AreEqual(_slot, slots[0]);
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
            var (setupCmd, testCmd, cleanCmd, response) = command.GenerateSingleKeyCmdInstance(ref clusterTestUtils, 4, 4, out var slots, migrateSlot);
            ResponseState status = default;
            byte[] result;
            int _slot = 0;
            string _address = "";
            int _port = 0;
            string _value;
            string[] _values;

            if (CheckFlag(command.testFlags, TestFlags.KEY_EXISTS))
            {
                if (setupCmd != null)
                {
                    for (int j = 0; j < setupCmd.Length; j++)
                    {
                        var resp = connections[sourceNodeIndex].SendCommand(setupCmd[j]);
                        var respStatus = ClusterTestUtils.ParseResponseState(resp, out _, out _, out _, out _, out _);
                        Assert.AreEqual(respStatus, ResponseState.OK);
                    }
                }
            }

            var respMigrating = ClusterTestUtils.SetSlot(ref connections[sourceNodeIndex], migrateSlot, "MIGRATING", targetNodeId);
            Assert.AreEqual(respMigrating, "OK");

            result = connections[sourceNodeIndex].SendCommand(testCmd);
            status = ClusterTestUtils.ParseResponseState(result, out _slot, out _address, out _port, out _value, out _values);

            var respMigratingStable = ClusterTestUtils.SetSlot(ref connections[sourceNodeIndex], migrateSlot, "STABLE", "");
            Assert.AreEqual(respMigratingStable, "OK");

            if (CheckFlag(command.testFlags, (TestFlags.KEY_EXISTS | TestFlags.READONLY)))
            {
                Assert.AreEqual(status, ResponseState.OK, command.cmdTag);
            }
            else if (CheckFlag(command.testFlags, (TestFlags.KEY_EXISTS)))
            {
                Assert.AreEqual(status, ResponseState.MIGRATING, command.cmdTag);
            }
            else
            {
                Assert.AreEqual(status, ResponseState.ASK, command.cmdTag);
                Assert.AreEqual(_port, connections[targetNodeIndex].Port, command.cmdTag);
                Assert.AreEqual(_address, connections[targetNodeIndex].Address, command.cmdTag);
            }

            if (CheckFlag(command.testFlags, TestFlags.KEY_EXISTS))
            {
                if (cleanCmd != null)
                {
                    for (int j = 0; j < cleanCmd.Length; j++)
                    {
                        var resp = connections[sourceNodeIndex].SendCommand(cleanCmd[j]);
                        var respStatus = ClusterTestUtils.ParseResponseState(resp, out _, out _, out _, out _, out _);
                        Assert.AreEqual(respStatus, ResponseState.OK);
                    }
                }
            }
        }

        [Test, Order(1)]
        [Category("CLUSTER")]
        public void ClusterSingleKeyRedirectionTests()
        {
            var logger = loggerFactory.CreateLogger("ClusterSingleKeyRedirectionTests");
            logger.LogDebug("0. ClusterSingleKeyRedirectionTests started");
            int Port = TestUtils.Port;
            int Shards = defaultShards;
            LightClientRequest[] connections = ClusterTestUtils.CreateLightRequestConnections(Enumerable.Range(Port, Shards).ToArray());
            clusterTestUtils.SimpleSetupCluster(logger: logger);

            //1. Regular operation redirect responses
            foreach (var command in singleKeyCommands)
            {
                var (setupCmd, testCmd, cleanCmd, response) = command.GenerateSingleKeyCmdInstance(ref clusterTestUtils, 4, 4, out var slots);
                if (setupCmd != null)
                    for (int j = 0; j < setupCmd.Length; j++)
                        SendToNodeFromSlot(ref connections, setupCmd[j], slots[0], command.cmdTag);

                if (testCmd != null)
                {
                    var (status, value, values) = SendAndRedirectToNode(ref connections, testCmd, slots[0], command.cmdTag);
                    Assert.AreEqual(status, ResponseState.OK, command.cmdTag);
                    if (command.response != null)
                    {
                        Assert.AreEqual(value, response, command.cmdTag);
                    }
                    else if (command.arrayResponse != null)
                    {
                        Assert.AreEqual(values.Length, command.arrayResponse.Length);
                        for (int i = 0; i < values.Length; i++)
                            Assert.AreEqual(values[i], command.arrayResponse[i], command.cmdTag);
                    }
                }

                if (cleanCmd != null)
                    for (int j = 0; j < cleanCmd.Length; j++)
                        SendToNodeFromSlot(ref connections, cleanCmd[j], slots[0], command.cmdTag);
            }

            //2. Check response during migration
            foreach (var command in singleKeyCommands)
            {
                int migrateSlot = r.Next(0, 16384);
                int sourceNodeIndex = ClusterTestUtils.GetSourceNodeIndexFromSlot(ref connections, (ushort)migrateSlot);
                int targetNodeIndex = clusterTestUtils.GetRandomTargetNodeIndex(ref connections, sourceNodeIndex);
                string sourceNodeId = ClusterTestUtils.GetNodeIdFromNode(ref connections[sourceNodeIndex]);
                string targetNodeId = ClusterTestUtils.GetNodeIdFromNode(ref connections[targetNodeIndex]);

                var respImporting = ClusterTestUtils.SetSlot(ref connections[targetNodeIndex], migrateSlot, "IMPORTING", sourceNodeId);
                Assert.AreEqual(respImporting, "OK");
                SendToImportingNode(ref connections, sourceNodeIndex, targetNodeIndex, command, migrateSlot);

                var respImportingStable = ClusterTestUtils.SetSlot(ref connections[targetNodeIndex], migrateSlot, "STABLE", "");
                Assert.AreEqual(respImportingStable, "OK");
                SendToMigratingNode(ref connections, sourceNodeIndex, sourceNodeId, targetNodeIndex, targetNodeId, command, migrateSlot);
            }

            connections.ToList().ForEach(x => x.Dispose());
            logger.LogDebug("1. ClusterSingleKeyRedirectionTests done");
        }

        [Test, Order(2)]
        [Category("CLUSTER")]
        public void ClusterMultiKeyRedirectionTests()
        {
            var logger = loggerFactory.CreateLogger("ClusterMultiKeyRedirectionTests");
            logger.LogDebug("0. ClusterMultiKeyRedirectionTests started");
            int Port = TestUtils.Port;
            int Shards = defaultShards;
            LightClientRequest[] connections = ClusterTestUtils.CreateLightRequestConnections(Enumerable.Range(Port, Shards).ToArray());
            clusterTestUtils.SimpleSetupCluster(logger: logger);

            //1. test regular operation redirection
            foreach (var command in multiKeyCommands)
            {
                int restrictedSlot = r.Next(0, 16384);
                var (setupCmd, testCmd, cleanCmd, response) = command.GenerateSingleKeyCmdInstance(ref clusterTestUtils, 4, 4, out var slots, restrictedSlot);

                for (int i = 0; i < slots.Count; i++)
                    Assert.AreEqual(slots[i], restrictedSlot);

                if (setupCmd != null)
                    for (int j = 0; j < setupCmd.Length; j++)
                        SendToNodeFromSlot(ref connections, setupCmd[j], slots[0], command.cmdTag);

                if (testCmd != null)
                {
                    var (status, value, values) = SendAndRedirectToNode(ref connections, testCmd, slots[0], command.cmdTag);
                    Assert.AreEqual(status, ResponseState.OK, command.cmdTag);
                    if (command.response != null)
                    {
                        Assert.AreEqual(value, response, command.cmdTag);
                    }
                    else
                    {
                        Assert.AreEqual(values.Length, command.arrayResponse.Length);
                        for (int i = 0; i < values.Length; i++)
                            Assert.AreEqual(values[i], command.arrayResponse[i], command.cmdTag);
                    }
                }

                if (cleanCmd != null)
                    for (int j = 0; j < cleanCmd.Length; j++)
                        SendToNodeFromSlot(ref connections, cleanCmd[j], slots[0], command.cmdTag);
            }

            //2. test crosslot
            foreach (var command in multiKeyCommands)
            {
                var (setupCmd, testCmd, cleanCmd, response) = command.GenerateSingleKeyCmdInstance(ref clusterTestUtils, 4, 4, out var slots);

                if (testCmd != null)
                {
                    var (status, value, values) = SendToNodeFromSlot(ref connections, testCmd, slots.Last(), command.cmdTag, false);
                    Assert.AreEqual(ResponseState.CROSSSLOT, status);
                }
            }

            //3. test migrating
            foreach (var command in multiKeyCommands)
            {
                int migrateSlot = r.Next(0, 16384);
                int sourceNodeIndex = ClusterTestUtils.GetSourceNodeIndexFromSlot(ref connections, (ushort)migrateSlot);
                int targetNodeIndex = clusterTestUtils.GetRandomTargetNodeIndex(ref connections, sourceNodeIndex);
                string sourceNodeId = ClusterTestUtils.GetNodeIdFromNode(ref connections[sourceNodeIndex]);
                string targetNodeId = ClusterTestUtils.GetNodeIdFromNode(ref connections[targetNodeIndex]);

                var respImporting = ClusterTestUtils.SetSlot(ref connections[targetNodeIndex], migrateSlot, "IMPORTING", sourceNodeId);
                Assert.AreEqual(respImporting, "OK");
                SendToImportingNode(ref connections, sourceNodeIndex, targetNodeIndex, command, migrateSlot);

                var respImportingStable = ClusterTestUtils.SetSlot(ref connections[targetNodeIndex], migrateSlot, "STABLE", "");
                Assert.AreEqual(respImportingStable, "OK");
                SendToMigratingNode(ref connections, sourceNodeIndex, sourceNodeId, targetNodeIndex, targetNodeId, command, migrateSlot);
            }

            connections.ToList().ForEach(x => x.Dispose());
            logger.LogDebug("1. ClusterMultiKeyRedirectionTests done");
        }
    }
}