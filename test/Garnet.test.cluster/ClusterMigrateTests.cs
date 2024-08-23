﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture(false), NonParallelizable]
    public unsafe class ClusterMigrateTests(bool UseTLS)
    {
        public (Action, string)[] GetUnitTests()
        {
            (Action, string)[] x =
            [
                //1
                new(ClusterSimpleInitialize, "ClusterSimpleInitialize()"),
                //2
                new(ClusterSimpleSlotInfo, "ClusterSimpleSlotInfo()"),
                //3
                new(ClusterAddDelSlots, "ClusterAddDelSlots()"),
                //4
                new(ClusterSlotChangeStatus, "ClusterSlotChangeStatus()"),
                //5
                new(ClusterRedirectMessage, "ClusterRedirectMessage()"),
                //6
                new(ClusterSimpleMigrateSlots, "ClusterSimpleMigrateSlots()"),
                //7
                new(ClusterSimpleMigrateSlotsExpiry, "ClusterSimpleMigrateSlotsExpiry()"),
                //8
                new(ClusterSimpleMigrateSlotsWithObjects, "ClusterSimpleMigrateSlotsWithObjects()"),
                //9
                new(ClusterSimpleMigrateKeys, "ClusterSimpleMigrateKeys()"),
                //10
                new(ClusterSimpleMigrateKeysWithObjects, "ClusterSimpleMigrateKeysWithObjects()"),
                //11
                new(ClusterSimpleMigrateWithReadWrite, "ClusterSimpleMigrateWithReadWrite()"),
            ];
            return x;
        }

        private void RandomBytes(ref byte[] data, int startOffset = -1, int endOffset = -1)
        {
            context.clusterTestUtils.RandomBytes(ref data, startOffset, endOffset);
        }

        private byte[] RandomBytes(byte[] data, int startOffset = -1, int endOffset = -1)
        {
            return context.clusterTestUtils.RandomBytes(data, startOffset, endOffset);
        }

        ClusterTestContext context;
        readonly string authPassword = null;
        readonly int defaultShards = 3;
        readonly Dictionary<string, LogLevel> authenticationTests = new()
        {
            {"ClusterSimpleMigrateWithAuth", LogLevel.Error }
        };

        readonly Dictionary<string, LogLevel> monitorTests = new()
        {
            {"ClusterTLSSlotChangeStatus", LogLevel.Error }
        };

        public TextWriter logTextWriter = TestContext.Progress;

        [SetUp]
        public virtual void Setup()
        {
            context = new ClusterTestContext();
            context.Setup(monitorTests);
        }

        [TearDown]
        public virtual void TearDown()
        {
            context?.TearDown();
        }

        public class ByteArrayComparer : IEqualityComparer<byte[]>
        {
            public bool Equals(byte[] a, byte[] b)
            {
                if (a == null || b == null)
                    return a == b;
                return a.SequenceEqual(b);
            }
            public int GetHashCode(byte[] key)
            {
                if (key == null)
                    throw new ArgumentNullException(nameof(key));
                return key.Sum(i => i);
            }
        }

        private int CreateSingleSlotData(
            int keyLen,
            int valueLen,
            int keyTagEnd,
            int keyCount,
            out Dictionary<byte[], byte[]> data,
            long expiration = -1,
            HashSet<int> restrictedToSlots = null)
        {
            var key = new byte[keyLen];
            var value = new byte[valueLen];

            ClassicAssert.IsTrue(keyTagEnd < valueLen);
            ushort slot;
            do
            {
                RandomBytes(ref key);
                key[0] = (byte)'{';
                key[keyTagEnd] = (byte)'}';
                data = new(new ByteArrayComparer());
                slot = ClusterTestUtils.HashSlot(key);
                if (restrictedToSlots == null) break;
            } while (!restrictedToSlots.Contains(slot));

            var db = context.clusterTestUtils.GetMultiplexer().GetDatabase(0);
            for (int i = 0; i < keyCount; i++)
            {
                RandomBytes(ref key, keyTagEnd + 1);
                RandomBytes(ref value);

                var ss = ClusterTestUtils.HashSlot(key);
                ClassicAssert.AreEqual(slot, ss);
                if (!data.ContainsKey(key))
                    data.Add(key, value);
                data[key] = value;
                bool status;
                if (expiration == -1)
                    status = db.StringSet(key, value);
                else
                {
                    // Odd positioned keys set with expiration data
                    // Even positioned keys set without expiration.
                    if ((i & 0x1) > 0)
                    {
                        status = db.StringSet(key, value, TimeSpan.FromSeconds(expiration));
                    }
                    else
                    {
                        status = db.StringSet(key, value);
                    }
                }

                ClassicAssert.IsTrue(status);

                var _v = (byte[])db.StringGet(key);
                ClassicAssert.AreEqual(value, _v);
            }
            return slot;
        }

        private void CreateMultiSlotData(
            int slotCount,
            int keyLen,
            int valueLen,
            int keyTagEnd,
            int keyCount,
            out Dictionary<int, Dictionary<byte[], byte[]>> data,
            HashSet<int> restrictedToSlots = null)
        {
            var db = context.clusterTestUtils.GetMultiplexer().GetDatabase(0);
            Dictionary<ushort, byte[]> slotsTokey = [];
            data = [];
            var key = new byte[keyLen];
            var value = new byte[valueLen];

            ClassicAssert.IsTrue(slotCount < keyCount);
            for (var i = 0; i < slotCount; i++)
            {
                ushort slot;
                byte[] newKey;
                do
                {
                restrictedSlot:
                    newKey = RandomBytes(key);
                    newKey[0] = (byte)'{';
                    newKey[keyTagEnd] = (byte)'}';
                    slot = ClusterTestUtils.HashSlot(newKey);

                    if (restrictedToSlots != null && !restrictedToSlots.Contains(slot))
                        goto restrictedSlot;

                } while (slotsTokey.ContainsKey(slot));
                slotsTokey.Add(slot, newKey);
                data[slot] = new(new ByteArrayComparer());
            }

            int j = 0;
            List<ushort> slots = [.. slotsTokey.Keys];
            for (int i = 0; i < keyCount; i++)
            {
                key = slotsTokey[slots[j]];
                var newKey = new byte[key.Length];
                var newValue = new byte[value.Length];

                Array.Copy(key, 0, newKey, 0, key.Length);
                Array.Copy(value, 0, newValue, 0, value.Length);
                RandomBytes(ref newKey, keyTagEnd + 1);
                RandomBytes(ref newValue);

                var slot = ClusterTestUtils.HashSlot(newKey);
                ClassicAssert.AreEqual(slot, slots[j]);
                ClassicAssert.IsTrue(slotsTokey.ContainsKey((ushort)slot));

                if (!data[slot].ContainsKey(newKey))
                    data[slot].Add(newKey, newValue);
                else
                    data[slot][newKey] = newValue;

                ClassicAssert.IsTrue(db.StringSet(newKey, newValue));
                var _v = (byte[])db.StringGet(newKey);
                ClassicAssert.AreEqual(newValue, _v);
                j = j + 1 < slots.Count ? j + 1 : 0;
            }
        }

        [Test, Order(1)]
        [Category("CLUSTER")]
        public void ClusterSimpleInitialize()
        {
            context.CreateInstances(defaultShards, useTLS: UseTLS);
            context.CreateConnection(useTLS: UseTLS);
            context.logger.LogDebug("0. ClusterSimpleInitialize started");

            context.logger.LogDebug("1. InitSimpleCluster started");
            var (_, slots) = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);
            context.logger.LogDebug("2. InitSimpleCluster done");

            var slots2 = context.clusterTestUtils.GetOwnedSlotsFromNode(0, context.logger);
            slots2.AddRange(context.clusterTestUtils.GetOwnedSlotsFromNode(1, context.logger));
            slots2.AddRange(context.clusterTestUtils.GetOwnedSlotsFromNode(2, context.logger));

            ClassicAssert.AreEqual(slots, slots2);
            context.logger.LogDebug("3. ClusterSimpleInitialize done");
        }

        [Test, Order(2)]
        [Category("CLUSTER")]
        public void ClusterSimpleSlotInfo()
        {
            context.CreateInstances(defaultShards, useTLS: UseTLS);
            context.CreateConnection(useTLS: UseTLS);

            context.logger.LogDebug("0. ClusterSimpleSlotInfoTest started");
            context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var keyCount = 100;
            context.logger.LogDebug("1. Creating slot data {keyCount} started", keyCount);
            var slot = CreateSingleSlotData(keyLen: 16, valueLen: 16, keyTagEnd: 6, keyCount, out _);
            context.logger.LogDebug("2. Creating slot data {keyCount} done", keyCount);

            var sourceIndex = context.clusterTestUtils.GetSourceNodeIndexFromSlot((ushort)slot, context.logger);
            var expectedKeyCount = context.clusterTestUtils.CountKeysInSlot(slot, context.logger);
            ClassicAssert.AreEqual(expectedKeyCount, keyCount);
            _ = context.clusterTestUtils.CountKeysInSlot(-1, context.logger);
            _ = context.clusterTestUtils.CountKeysInSlot(ushort.MaxValue, context.logger);

            var result = context.clusterTestUtils.GetKeysInSlot(sourceIndex, slot, expectedKeyCount, context.logger);
            ClassicAssert.AreEqual(result.Count, keyCount);
            _ = context.clusterTestUtils.GetKeysInSlot(-1, expectedKeyCount);
            _ = context.clusterTestUtils.GetKeysInSlot(ushort.MaxValue, expectedKeyCount);

            context.logger.LogDebug("3. ClusterSimpleSlotInfoTest done");
        }

        [Test, Order(3)]
        [Category("CLUSTER")]
        public void ClusterAddDelSlots()
        {
            context.CreateInstances(defaultShards, useTLS: UseTLS);
            context.CreateConnection(useTLS: UseTLS);
            context.logger.LogDebug("0. ClusterAddDelSlotsTest started");

            #region AddSlots
            string resp;
            resp = context.clusterTestUtils.AddDelSlots(0, [1, 2, 3, 4, 4, 5, 6], addslot: true);
            ClassicAssert.AreEqual("ERR Slot 4 specified multiple times", resp);

            resp = context.clusterTestUtils.AddDelSlots(0, [-1, 2, 3, 4, 4, 5, 6], addslot: true);
            ClassicAssert.AreEqual("ERR Slot out of range", resp);

            resp = context.clusterTestUtils.AddDelSlots(0, [16384, 2, 3, 4, 4, 5, 6], addslot: true);
            ClassicAssert.AreEqual("ERR Slot out of range", resp);

            resp = context.clusterTestUtils.AddDelSlots(0, [1, 2, 3, 4, 5, 6], addslot: true);
            ClassicAssert.AreEqual("OK", resp);

            resp = context.clusterTestUtils.AddDelSlots(0, [1, 2, 3, 4, 5, 6], addslot: true);
            ClassicAssert.AreEqual("ERR Slot 1 is already busy", resp);

            resp = context.clusterTestUtils.AddDelSlotsRange(0, [new(1, 6)], addslot: true);
            ClassicAssert.AreEqual("ERR Slot 1 is already busy", resp);

            resp = context.clusterTestUtils.AddDelSlotsRange(0, [new(10, 30), new(20, 40)], addslot: true);
            ClassicAssert.AreEqual("ERR Slot 20 specified multiple times", resp);

            resp = context.clusterTestUtils.AddDelSlotsRange(0, [new(10, 30), new(40, 80)], true);
            ClassicAssert.AreEqual("OK", resp);

            HashSet<int> slots = new(context.clusterTestUtils.GetOwnedSlotsFromNode(0, context.logger));

            foreach (var _slot in new List<int>() { 1, 2, 3, 4, 5, 6 })
                ClassicAssert.IsTrue(slots.Contains(_slot));

            foreach (var _slot in Enumerable.Range(10, 21).ToList())
                ClassicAssert.IsTrue(slots.Contains(_slot));

            foreach (var _slot in Enumerable.Range(40, 41).ToList())
                ClassicAssert.IsTrue(slots.Contains(_slot));
            #endregion

            #region DelSlots
            byte[] key = Encoding.ASCII.GetBytes("{abc}0");
            byte[] val = Encoding.ASCII.GetBytes("1234");
            var slot = HashSlotUtils.HashSlot(key);
            resp = context.clusterTestUtils.AddDelSlots(0, [slot], true);
            ClassicAssert.AreEqual(resp, "OK");


            var respState = context.clusterTestUtils.SetKey(0, key, val, out var _, out var _, out var _, logger: context.logger);
            ClassicAssert.AreEqual(respState, ResponseState.OK);

            resp = context.clusterTestUtils.AddDelSlots(0, [slot], false);
            ClassicAssert.AreEqual(resp, "OK");

            respState = context.clusterTestUtils.SetKey(0, key, val, out var _, out var _, out var _, logger: context.logger);
            ClassicAssert.AreEqual(respState, ResponseState.CLUSTERDOWN);

            resp = context.clusterTestUtils.GetKey(0, key, out var _, out var _, out var _, out var _, logger: context.logger);
            ClassicAssert.AreEqual(resp, "CLUSTERDOWN");

            resp = context.clusterTestUtils.AddDelSlots(0, [slot], true);
            ClassicAssert.AreEqual(resp, "OK");

            resp = context.clusterTestUtils.GetKey(0, key, out var _, out var _, out var _, out var _, logger: context.logger);
            ClassicAssert.AreEqual(resp, val);

            #endregion

            context.logger.LogDebug("1. ClusterAddDelSlotsTest done");
        }

        [Test, Order(4)]
        [Category("CLUSTER")]
        public void ClusterSlotChangeStatus()
        {
            context.logger.LogDebug("0. ClusterSlotChangeStatusTest started");
            context.CreateInstances(defaultShards, useTLS: UseTLS);
            context.CreateConnection(useTLS: UseTLS);
            context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);
            var sourcePortIndex = 1;
            var targetPortIndex = 2;
            var otherNodeIndex = 0;

            var key = Encoding.ASCII.GetBytes("{abc}0");
            var val = Encoding.ASCII.GetBytes("1234");
            var respState = context.clusterTestUtils.SetKey(sourcePortIndex, key, val, out _, out _, out _, logger: context.logger);
            ClassicAssert.AreEqual(respState, ResponseState.OK);
            var slot = (int)HashSlotUtils.HashSlot(key);
            var expectedSlot = 7638;
            ClassicAssert.AreEqual(expectedSlot, slot);

            var sourceNodeId = context.clusterTestUtils.GetNodeIdFromNode(sourcePortIndex, context.logger);
            var targetNodeId = context.clusterTestUtils.GetNodeIdFromNode(targetPortIndex, context.logger);

            #region SETSLOT_IMPORTING
            //Set Importing Tests
            //1. don't know node error
            var resp = context.clusterTestUtils.SetSlot(targetPortIndex, slot, "IMPORTING", sourceNodeId[..10], context.logger);
            ClassicAssert.AreEqual(resp, $"ERR I don't know about node {sourceNodeId[..10]}");

            //2. cannot import slot already owned by node
            resp = context.clusterTestUtils.SetSlot(sourcePortIndex, slot, "IMPORTING", targetNodeId, context.logger);
            ClassicAssert.AreEqual(resp, $"ERR This is a local hash slot {slot} and is already imported");

            //3. out of range import error
            resp = context.clusterTestUtils.SetSlot(targetPortIndex, -1, "IMPORTING", sourceNodeId, context.logger);
            ClassicAssert.AreEqual(resp, "ERR Slot out of range");

            //4. out of range import error
            resp = context.clusterTestUtils.SetSlot(targetPortIndex, 16384, "IMPORTING", sourceNodeId, context.logger);
            ClassicAssert.AreEqual(resp, "ERR Slot out of range");

            //5. import OK
            resp = context.clusterTestUtils.SetSlot(targetPortIndex, slot, "IMPORTING", sourceNodeId, context.logger);
            ClassicAssert.AreEqual(resp, "OK");

            //6. cannot import multiple times
            resp = context.clusterTestUtils.SetSlot(targetPortIndex, slot, "IMPORTING", sourceNodeId, context.logger);
            ClassicAssert.AreEqual(resp, $"ERR Slot already scheduled for import from {sourceNodeId}");

            //7. cannot import slot not owned by given source
            resp = context.clusterTestUtils.SetSlot(targetPortIndex, 7, "IMPORTING", sourceNodeId, context.logger);
            ClassicAssert.AreEqual(resp, $"ERR Slot {7} is not owned by {sourceNodeId}");
            #endregion

            #region SETSLOT_MIGRATING
            //Set Migrating Tests
            //1. out of range error
            resp = context.clusterTestUtils.SetSlot(sourcePortIndex, -1, "MIGRATING", targetNodeId, context.logger);
            ClassicAssert.AreEqual(resp, "ERR Slot out of range");

            //2. out of range error
            resp = context.clusterTestUtils.SetSlot(sourcePortIndex, 16384, "MIGRATING", targetNodeId, context.logger);
            ClassicAssert.AreEqual(resp, "ERR Slot out of range");

            //3. cannot migrate to self
            resp = context.clusterTestUtils.SetSlot(sourcePortIndex, slot, "MIGRATING", sourceNodeId, context.logger);
            ClassicAssert.AreEqual(resp, "ERR Can't MIGRATE to myself");

            //4. don't know about node
            resp = context.clusterTestUtils.SetSlot(sourcePortIndex, slot, "MIGRATING", targetNodeId[..10], context.logger);
            ClassicAssert.AreEqual(resp, $"ERR I don't know about node {targetNodeId[..10]}");

            //5. do not own slot
            resp = context.clusterTestUtils.SetSlot(sourcePortIndex, 7, "MIGRATING", targetNodeId, context.logger);
            ClassicAssert.AreEqual(resp, $"ERR I'm not the owner of hash slot {7}");

            //6. migration OK
            resp = context.clusterTestUtils.SetSlot(sourcePortIndex, slot, "MIGRATING", targetNodeId, context.logger);
            ClassicAssert.AreEqual(resp, "OK");
            #endregion

            #region TEST_REDIRECTION
            //0. other node alway redirect to source node            
            resp = context.clusterTestUtils.GetKey(otherNodeIndex, key, out slot, out var address, out var port, out var responseState, logger: context.logger);
            ClassicAssert.AreEqual(ResponseState.MOVED, responseState);
            ClassicAssert.AreEqual(resp, "MOVED");
            ClassicAssert.AreEqual(expectedSlot, slot);
            ClassicAssert.AreEqual(address, context.clusterTestUtils.GetEndPoint(sourcePortIndex).Address.ToString());
            ClassicAssert.AreEqual(port, context.clusterTestUtils.GetEndPoint(sourcePortIndex).Port);

            //1. Can read source migrating
            resp = context.clusterTestUtils.GetKey(sourcePortIndex, key, out _, out _, out _, out responseState, logger: context.logger);
            ClassicAssert.AreEqual(ResponseState.OK, responseState);
            ClassicAssert.AreEqual(resp, val);

            //2. Request on source node redirect with asking for new keys to target node
            resp = context.clusterTestUtils.GetKey(sourcePortIndex, Encoding.ASCII.GetBytes("{abc}1"), out slot, out address, out port, out responseState, logger: context.logger);
            ClassicAssert.AreEqual(ResponseState.ASK, responseState);
            ClassicAssert.AreEqual(resp, "ASK");
            ClassicAssert.AreEqual(expectedSlot, slot);
            ClassicAssert.AreEqual(address, context.clusterTestUtils.GetEndPoint(targetPortIndex).Address.ToString());
            ClassicAssert.AreEqual(port, context.clusterTestUtils.GetEndPoint(targetPortIndex).Port);

            //3. request on target node without asking redirect to source node.
            resp = context.clusterTestUtils.GetKey(targetPortIndex, Encoding.ASCII.GetBytes("{abc}1"), out slot, out address, out port, out responseState, logger: context.logger);
            ClassicAssert.AreEqual(ResponseState.MOVED, responseState);
            ClassicAssert.AreEqual(resp, "MOVED");
            ClassicAssert.AreEqual(expectedSlot, slot);
            ClassicAssert.AreEqual(address, context.clusterTestUtils.GetEndPoint(sourcePortIndex).Address.ToString());
            ClassicAssert.AreEqual(port, context.clusterTestUtils.GetEndPoint(sourcePortIndex).Port);

            //4. request write on source node to existing key try-again migrating
            respState = context.clusterTestUtils.SetKey(sourcePortIndex, Encoding.ASCII.GetBytes("{abc}0"), Encoding.ASCII.GetBytes("5678"), out _, out _, out _, logger: context.logger);
            ClassicAssert.AreEqual(respState, ResponseState.OK);

            //5. request write on source node to new key redirect.
            respState = context.clusterTestUtils.SetKey(sourcePortIndex, Encoding.ASCII.GetBytes("{abc}1"), Encoding.ASCII.GetBytes("5678"), out slot, out address, out port, logger: context.logger);
            ClassicAssert.AreEqual(respState, ResponseState.ASK);
            ClassicAssert.AreEqual(expectedSlot, slot);
            ClassicAssert.AreEqual(address, context.clusterTestUtils.GetEndPoint(targetPortIndex).Address.ToString());
            ClassicAssert.AreEqual(port, context.clusterTestUtils.GetEndPoint(targetPortIndex).Port);

            //6. request on target after asking response empty for new key
            resp = context.clusterTestUtils.GetKey(targetPortIndex, Encoding.ASCII.GetBytes("{abc}1"), out _, out _, out _, out responseState, true, logger: context.logger);
            ClassicAssert.AreEqual(ResponseState.OK, responseState);
            ClassicAssert.AreEqual(null, resp);

            #endregion

            #region RESET_SLOT_STATE
            resp = context.clusterTestUtils.SetSlot(targetPortIndex, expectedSlot, "STABLE", "", logger: context.logger);
            ClassicAssert.AreEqual(resp, "OK");
            resp = context.clusterTestUtils.GetKey(targetPortIndex, Encoding.ASCII.GetBytes("{abc}1"), out slot, out address, out port, out responseState, logger: context.logger);
            ClassicAssert.AreEqual(ResponseState.MOVED, responseState);
            ClassicAssert.AreEqual(resp, "MOVED");
            ClassicAssert.AreEqual(expectedSlot, slot);
            ClassicAssert.AreEqual(address, context.clusterTestUtils.GetEndPoint(sourcePortIndex).Address.ToString());
            ClassicAssert.AreEqual(port, context.clusterTestUtils.GetEndPoint(sourcePortIndex).Port);

            resp = context.clusterTestUtils.SetSlot(sourcePortIndex, expectedSlot, "STABLE", "", logger: context.logger);
            ClassicAssert.AreEqual(resp, "OK");
            resp = context.clusterTestUtils.GetKey(sourcePortIndex, Encoding.ASCII.GetBytes("{abc}1"), out _, out _, out _, out responseState, logger: context.logger);
            ClassicAssert.AreEqual(ResponseState.OK, responseState);
            #endregion

            context.logger.LogDebug("1. ClusterSlotChangeStatusTest done");
        }

        [Test, Order(5)]
        [Category("CLUSTER")]
        public void ClusterRedirectMessage()
        {
            context.logger.LogDebug("0. ClusterRedirectMessageTest started");
            var Shards = 2;
            context.CreateInstances(Shards, useTLS: UseTLS);
            context.CreateConnection(useTLS: UseTLS);
            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);
            var key = Encoding.ASCII.GetBytes("{abc}0");
            var slot = ClusterTestUtils.HashSlot(key);

            List<byte[]> keys = [];
            List<byte[]> vals = [];

            for (var i = 0; i < 5; i++)
            {
                var newKey = new byte[key.Length];
                Array.Copy(key, 0, newKey, 0, key.Length);
                newKey[^1] = (byte)(newKey[^1] + i);
                keys.Add(newKey);
                vals.Add(newKey);
            }

            var sourceNodeIndex = 0;
            var otherNodeIndex = 1;

            var resp = context.clusterTestUtils.SetMultiKey(sourceNodeIndex, keys, vals, out var _, out var _, out var _);
            ClassicAssert.AreEqual(resp, "OK");

            _ = context.clusterTestUtils.GetMultiKey(sourceNodeIndex, keys, out var valuesGet, out _, out _, out _);
            ClassicAssert.AreEqual(valuesGet, vals);

            keys[0][1] = (byte)('w');
            resp = context.clusterTestUtils.GetMultiKey(sourceNodeIndex, keys, out _, out _, out _, out _);
            ClassicAssert.AreEqual(resp, "CROSSSLOT");

            resp = context.clusterTestUtils.SetMultiKey(sourceNodeIndex, keys, vals, out _, out _, out _);
            ClassicAssert.AreEqual(resp, "CROSSSLOT");

            keys[0][1] = (byte)('a');
            ClassicAssert.AreEqual(ClusterTestUtils.HashSlot(keys[0]), ClusterTestUtils.HashSlot(keys[1]));
            resp = context.clusterTestUtils.GetMultiKey(otherNodeIndex, keys, out _, out var _slot, out var _address, out var _port);
            ClassicAssert.AreEqual(resp, "MOVED");
            ClassicAssert.AreEqual(_slot, slot);
            ClassicAssert.AreEqual(_address, context.clusterTestUtils.GetEndPoint(sourceNodeIndex).Address.ToString());
            ClassicAssert.AreEqual(_port, context.clusterTestUtils.GetEndPoint(sourceNodeIndex).Port);

            resp = context.clusterTestUtils.SetMultiKey(otherNodeIndex, keys, vals, out _slot, out _address, out _port);
            ClassicAssert.AreEqual(resp, "MOVED");
            ClassicAssert.AreEqual(_slot, slot);
            ClassicAssert.AreEqual(_address, context.clusterTestUtils.GetEndPoint(sourceNodeIndex).Address.ToString());
            ClassicAssert.AreEqual(_port, context.clusterTestUtils.GetEndPoint(sourceNodeIndex).Port);

            context.logger.LogDebug("1. ClusterRedirectMessageTest done");
        }

        [Test, Order(6)]
        [Category("CLUSTER")]
        public void ClusterSimpleMigrateSlots()
        {
            context.logger.LogDebug("0. ClusterSimpleMigrateSlotsTest started");
            var Port = TestUtils.Port;
            var Shards = defaultShards;
            context.CreateInstances(Shards, useTLS: UseTLS);
            context.CreateConnection(useTLS: UseTLS);

            var (_, slots) = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var msp = context.clusterTestUtils.GetSlotPortMapFromNode(0, context.logger);
            for (var i = 1; i < Shards; i++)
                msp = ClusterTestUtils.MergeSlotPortMap(msp, context.clusterTestUtils.GetSlotPortMapFromNode(i, context.logger));
            ClassicAssert.AreEqual(msp.Count, 16384);

            context.logger.LogDebug("1. Creating data");
            var keyCount = 100;
            var slot = CreateSingleSlotData(keyLen: 16, valueLen: 16, keyTagEnd: 6, keyCount, out var data);
            var sourceIndex = context.clusterTestUtils.GetSourceNodeIndexFromSlot((ushort)slot, context.logger);
            var expectedKeyCount = context.clusterTestUtils.CountKeysInSlot(slot);
            ClassicAssert.AreEqual(expectedKeyCount, keyCount);
            context.logger.LogDebug("2. Data created {keyCount}", keyCount);

            var sourcePort = msp[(ushort)slot];
            var targetPort = msp[(ushort)context.r.Next(0, 16384)];
            while (sourcePort == targetPort)
                targetPort = msp[(ushort)context.r.Next(0, 16384)];

            // Check data are inserted correctly
            foreach (var entry in data)
            {
                var value = context.clusterTestUtils.GetKey(context.clusterTestUtils.GetEndPointFromPort(sourcePort), entry.Key, out var _slot, out var _address, out var _port, out var responseState);
                ClassicAssert.AreEqual(ResponseState.OK, responseState);
                ClassicAssert.AreEqual(Encoding.ASCII.GetString(entry.Value), value, $"data not inserted correctly => expected: {Encoding.ASCII.GetString(entry.Value)}, actual: {value}");
                ClassicAssert.AreEqual(sourcePort, _port);
                ClassicAssert.AreEqual((ushort)slot, _slot);
            }

            context.logger.LogDebug("3. Initiating async migration");
            // Initiate Migration            
            context.clusterTestUtils.MigrateSlots(sourcePort, targetPort, new List<int>() { slot }, logger: context.logger);

            context.logger.LogDebug("4. Checking keys starting");
            // Wait for keys to become available for reading
            var keysList = data.Keys.ToList();
            for (var i = 0; i < keysList.Count; i++)
            {
                var value = context.clusterTestUtils.GetKey(context.clusterTestUtils.GetEndPointFromPort(targetPort), keysList[i], out var _slot, out var _address, out var _port, out var responseState);
                while (responseState != ResponseState.OK)
                {
                    _ = Thread.Yield();
                    value = context.clusterTestUtils.GetKey(context.clusterTestUtils.GetEndPointFromPort(targetPort), keysList[i], out _slot, out _address, out _port, out responseState);
                }

                ClassicAssert.AreEqual(targetPort, _port, $"[{sourcePort}] => [{targetPort}] == {_port} | expected: {targetPort}, actual: {_port}");
                ClassicAssert.AreEqual(data[keysList[i]], Encoding.ASCII.GetBytes(value), $"[{sourcePort}] => [{targetPort}] == {_port} | expected: {Encoding.ASCII.GetString(data[keysList[i]])}, actual: {value}");
            }
            context.logger.LogDebug("5. Checking keys done");

            context.logger.LogDebug("6. Checking configuration update starting");
            // Check if configuration has updated by
            var otherPorts = context.clusterTestUtils.GetEndPoints().Select(x => ((IPEndPoint)x).Port).Where(x => x != sourcePort || x != targetPort);
            while (true)
            {
                var targetSlotPortMap = context.clusterTestUtils.GetSlotPortMapFromServer(targetPort, context.logger);
                var sourceSlotPortMap = context.clusterTestUtils.GetSlotPortMapFromServer(sourcePort, context.logger);

                var moved = false;
                foreach (var p in otherPorts)
                {
                    var movedPort = context.clusterTestUtils.GetMovedAddress(p, (ushort)slot, context.logger);
                    moved |= movedPort == targetPort;
                }

                // Check if slot is accesible only from target and not source,
                // and other nodes have been informed.
                if (moved && targetSlotPortMap.ContainsKey((ushort)slot) &&
                    !sourceSlotPortMap.ContainsKey((ushort)slot))
                    break;
            }

            context.logger.LogDebug("7. Checking configuration update done");
            context.clusterTestUtils.WaitForMigrationCleanup(context.logger);
            context.logger.LogDebug("8. ClusterSimpleMigrateSlotsTest done");
        }

        [Test, Order(7)]
        [Category("CLUSTER")]
        public void ClusterSimpleMigrateSlotsExpiry()
        {
            context.logger.LogDebug("0. ClusterSimpleMigrateSlotsExpiryTest started");
            context.CreateInstances(defaultShards, useTLS: UseTLS);
            context.CreateConnection(useTLS: UseTLS);
            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var keyExpiryCount = 10;
            context.logger.LogDebug("1. Creating expired key data {keyExpiryCount}", keyExpiryCount);
            var slot = CreateSingleSlotData(keyLen: 16, valueLen: 16, keyTagEnd: 6, keyExpiryCount, out var data, 1);
            Thread.Sleep(5000);

            var keyCountRet = context.clusterTestUtils.CountKeysInSlot(slot);
            ClassicAssert.AreEqual(keyCountRet, keyExpiryCount / 2);
            context.logger.LogDebug("2. Count keys in slot after expiry");

            keyCountRet = 100;
            context.logger.LogDebug("3. Creating slot data {keyCountRet} with expiry started", keyCountRet);

            var _slot = CreateSingleSlotData(keyLen: 16, valueLen: 16, keyTagEnd: 6, keyExpiryCount, out data, 20, new HashSet<int> { 7638 });
            var sourceNodeIndex = context.clusterTestUtils.GetSourceNodeIndexFromSlot((ushort)_slot, context.logger);
            var targetNodeIndex = 2;
            ClassicAssert.AreNotEqual(_slot, slot);
            ClassicAssert.AreEqual(_slot, 7638);

            context.logger.LogDebug("4. Creating slot data {keyCountRet} with expiry done", keyCountRet);

            context.logger.LogDebug("5. Initiating migration");
            context.clusterTestUtils.MigrateSlots(context.clusterTestUtils.GetEndPoint(sourceNodeIndex), context.clusterTestUtils.GetEndPoint(targetNodeIndex), new List<int>() { _slot }, logger: context.logger);
            context.logger.LogDebug("6. Finished migration");

            context.logger.LogDebug("7. Checking migrating keys started");
            do
            {
                _ = Thread.Yield();
                keyCountRet = context.clusterTestUtils.CountKeysInSlot(targetNodeIndex, slot, context.logger);
            } while (keyCountRet == -1 || keyCountRet > keyExpiryCount / 2);
            ClassicAssert.AreEqual(keyExpiryCount / 2, keyCountRet);
            context.logger.LogDebug("8. Checking migrating keys done");

            context.clusterTestUtils.WaitForMigrationCleanup(context.logger);
            context.logger.LogDebug("9. ClusterSimpleMigrateSlotsExpiryTest done");
        }

        private (string, List<Tuple<int, byte[]>>) DoZADD(int nodeIndex, byte[] key, int memberCount, int memberSize = 8, int scoreMin = int.MinValue, int scoreMax = int.MaxValue)
        {
            var server = context.clusterTestUtils.GetServer(nodeIndex);
            List<Tuple<int, byte[]>> data = [];
            HashSet<int> scores = [];
            ICollection<object> args = [key];
            for (var i = 0; i < memberCount; i++)
            {
                var score = context.r.Next(scoreMin, scoreMax);
                while (scores.Contains(score))
                    score = context.r.Next(scoreMin, scoreMax);
                var member = new byte[memberSize];
                RandomBytes(ref member);

                data.Add(new(score, member));
                args.Add(score);
                args.Add(Encoding.ASCII.GetString(member));
            }

            try
            {
                var result = (string)server.Execute("zadd", args);
                data.Sort((x, y) => x.Item1.CompareTo(y.Item1));
                return (result, data);
            }
            catch (Exception ex)
            {
                context.logger?.LogError(ex, "An error occured at ZADD");
                Assert.Fail();
                return ("ZADD error", data);
            }
        }

        private string DoZCOUNT(int nodeIndex, byte[] key, out int count, out string address, out int port, out int slot, int scoreMin = int.MinValue, int scoreMax = int.MaxValue, ILogger logger = null)
        {
            count = -1;
            var server = context.clusterTestUtils.GetServer(nodeIndex);
            ICollection<object> args =
            [
                Encoding.ASCII.GetString(key),
                scoreMin,
                scoreMax
            ];

            try
            {
                var result = server.Execute("zcount", args, CommandFlags.NoRedirect);
                count = int.Parse((string)result);
                address = ((IPEndPoint)server.EndPoint).Address.ToString();
                port = ((IPEndPoint)server.EndPoint).Port;
                slot = ClusterTestUtils.HashSlot(key);
                return "OK";
            }
            catch (Exception e)
            {
                var tokens = e.Message.Split(' ');
                if (tokens.Length > 10 && tokens[2].Equals("MOVED"))
                {
                    address = tokens[5].Split(':')[0];
                    port = int.Parse(tokens[5].Split(':')[1]);
                    slot = int.Parse(tokens[8]);
                    logger?.LogDebug("MOVED: {address} {port} {slot}", address, port, slot);
                    return "MOVED";
                }
                else if (tokens.Length > 10 && tokens[0].Equals("Endpoint"))
                {
                    address = tokens[1].Split(':')[0];
                    port = int.Parse(tokens[1].Split(':')[1]);
                    slot = int.Parse(tokens[4]);
                    logger?.LogDebug("ASK: {address} {port} {slot}", address, port, slot);
                    return "ASK";
                }
                else if (e.Message.StartsWith("CLUSTERDOWN"))
                {
                    address = null;
                    port = -1;
                    slot = -1;
                    logger?.LogDebug("CLUSTERDOWN: {address} {port} {slot}", address, port, slot);
                    return "CLUSTERDOWN";
                }
                logger?.LogError(e, "An error occured at DoZCOUNT");
                address = null;
                port = -1;
                slot = -1;
                return e.Message;
            }
        }

        private (string, List<string>) DoZRANGE(int nodeIndex, byte[] key, out string address, out int port, out int slot, ILogger logger = null, int scoreMin = 0, int scoreMax = 100)
        {
            var server = context.clusterTestUtils.GetServer(nodeIndex);
            ICollection<object> args =
            [
                Encoding.ASCII.GetString(key),
                scoreMin,
                scoreMax
            ];
            try
            {
                var result = server.Execute("zrange", args, CommandFlags.NoRedirect);
                address = ((IPEndPoint)server.EndPoint).Address.ToString();
                port = ((IPEndPoint)server.EndPoint).Port;
                slot = ClusterTestUtils.HashSlot(key);
                return ("OK", ((RedisResult[])result).Select(x => (string)x).ToList());
            }
            catch (Exception e)
            {
                var tokens = e.Message.Split(' ');
                if (tokens.Length > 10 && tokens[2].Equals("MOVED"))
                {
                    address = tokens[5].Split(':')[0];
                    port = int.Parse(tokens[5].Split(':')[1]);
                    slot = int.Parse(tokens[8]);
                    logger?.LogWarning("MOVED: {address} {port} {slot}", address, port, slot);
                    return ("MOVED", null);
                }
                else if (tokens.Length > 10 && tokens[0].Equals("Endpoint"))
                {
                    address = tokens[1].Split(':')[0];
                    port = int.Parse(tokens[1].Split(':')[1]);
                    slot = int.Parse(tokens[4]);
                    logger?.LogWarning("ASK: {address} {port} {slot}", address, port, slot);
                    return ("ASK", null);
                }
                else if (e.Message.StartsWith("CLUSTERDOWN"))
                {
                    address = null;
                    port = -1;
                    slot = -1;
                    logger?.LogWarning("CLUSTERDOWN: {address} {port} {slot}", address, port, slot);
                    return ("CLUSTERDOWN", null);
                }
                logger?.LogError(e, "An error occured DoZRANGE");
                address = null;
                port = -1;
                slot = -1;
                return (e.Message, null);
            }
        }

        [Test, Order(8)]
        [Category("CLUSTER")]
        public void ClusterSimpleMigrateSlotsWithObjects()
        {
            context.logger.LogDebug("0. ClusterSimpleMigrateSlotsWithObjectsTest started");
            var Port = TestUtils.Port;
            var Shards = defaultShards;
            context.CreateInstances(defaultShards, useTLS: UseTLS);
            context.CreateConnection(useTLS: UseTLS);
            var (_, slots) = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var sourceNodeIndex = 1;
            var targetNodeIndex = 2;
            var sourceNodeId = context.clusterTestUtils.GetNodeIdFromNode(sourceNodeIndex, context.logger);
            var targetNodeId = context.clusterTestUtils.GetNodeIdFromNode(targetNodeIndex, context.logger);

            var key = Encoding.ASCII.GetBytes("{abc}0");
            var slot = ClusterTestUtils.HashSlot(key);
            var memberCount = 10;
            ClassicAssert.AreEqual(7638, slot);

            context.logger.LogDebug("1. Loading object keys data started");
            List<Tuple<int, byte[]>> memberPair;
            (_, memberPair) = DoZADD(sourceNodeIndex, key, memberCount);
            var resp = DoZCOUNT(sourceNodeIndex, key, out var count, out var _Address, out var _Port, out var _Slot, logger: context.logger);
            ClassicAssert.AreEqual(resp, "OK");
            ClassicAssert.AreEqual(count, memberCount);
            List<string> members;
            (resp, members) = DoZRANGE(sourceNodeIndex, key, out _Address, out _Port, out _Slot, context.logger);
            ClassicAssert.AreEqual(memberPair.Select(x => x.Item2).ToList(), members);

            context.logger.LogDebug("2. Loading object keys data done");

            var sourceEndPoint = context.clusterTestUtils.GetEndPoint(sourceNodeIndex);
            var targetEndPoint = context.clusterTestUtils.GetEndPoint(targetNodeIndex);
            context.logger.LogDebug("3. Migrating slot {slot} started {sourceEndPoint.Port} to {targetEndPoint.Port} started", slot, sourceEndPoint.Port, targetEndPoint.Port);
            context.clusterTestUtils.MigrateSlots(context.clusterTestUtils.GetEndPoint(sourceNodeIndex), context.clusterTestUtils.GetEndPoint(targetNodeIndex), new List<int>() { slot }, logger: context.logger);
            context.logger.LogDebug("4. Migrating slot {slot} started {sourceEndPoint.Port} to {targetEndPoint.Port} done", slot, sourceEndPoint.Port, targetEndPoint.Port);

            context.logger.LogDebug("5. Checking migrated keys started");
            count = 0;
            do
            {
                resp = DoZCOUNT(targetNodeIndex, key, out count, out _Address, out _Port, out _Slot, logger: context.logger);
            }
            while (!resp.Equals("OK"));
            ClassicAssert.AreEqual(count, memberCount);

            context.logger.LogDebug("6. Checking migrated keys done");

            (resp, members) = DoZRANGE(targetNodeIndex, key, out _Address, out _Port, out _Slot);
            ClassicAssert.AreEqual(memberPair.Select(x => Encoding.ASCII.GetString(x.Item2)).ToList(), members);
            context.clusterTestUtils.WaitForMigrationCleanup(context.logger);
            context.logger.LogDebug("7. ClusterSimpleMigrateSlotsWithObjectsTest done");
        }

        [Test, Order(9)]
        [Category("CLUSTER")]
        public void ClusterSimpleMigrateKeys()
        {
            context.logger.LogDebug("0. ClusterSimpleMigrateKeysTest started");
            context.CreateInstances(defaultShards, useTLS: UseTLS);
            context.CreateConnection(useTLS: UseTLS);
            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var otherNodeIndex = 0;
            var sourceNodeIndex = 1;
            var targetNodeIndex = 2;
            var sourceNodeId = context.clusterTestUtils.GetNodeIdFromNode(sourceNodeIndex, context.logger);
            var targetNodeId = context.clusterTestUtils.GetNodeIdFromNode(targetNodeIndex, context.logger);

            var keyCount = 10;
            var key = Encoding.ASCII.GetBytes("{abc}a");
            List<byte[]> keys = [];
            var _workingSlot = ClusterTestUtils.HashSlot(key);
            ClassicAssert.AreEqual(7638, _workingSlot);

            context.logger.LogDebug("1. Loading test keys {keyCount}", keyCount);
            for (var i = 0; i < keyCount; i++)
            {
                var newKey = new byte[key.Length];
                Array.Copy(key, 0, newKey, 0, key.Length);
                newKey[^1] = (byte)(newKey[^1] + i);
                keys.Add(newKey);
                ClassicAssert.AreEqual(_workingSlot, ClusterTestUtils.HashSlot(newKey));

                var resp = context.clusterTestUtils.SetKey(sourceNodeIndex, newKey, newKey, out _, out var address, out var port, logger: context.logger);
                ClassicAssert.AreEqual(resp, ResponseState.OK);
                ClassicAssert.AreEqual(address, context.clusterTestUtils.GetEndPoint(sourceNodeIndex).Address.ToString());
                ClassicAssert.AreEqual(port, context.clusterTestUtils.GetEndPoint(sourceNodeIndex).Port);
            }
            context.logger.LogDebug("2. Test keys loaded");

            // Start migration
            var respImport = context.clusterTestUtils.SetSlot(targetNodeIndex, _workingSlot, "IMPORTING", sourceNodeId, logger: context.logger);
            ClassicAssert.AreEqual(respImport, "OK");
            context.logger.LogDebug("3. Set slot {_slot} to IMPORTING state on node {port}", _workingSlot, context.clusterTestUtils.GetEndPoint(targetNodeIndex).Port);

            var respMigrate = context.clusterTestUtils.SetSlot(sourceNodeIndex, _workingSlot, "MIGRATING", targetNodeId, logger: context.logger);
            ClassicAssert.AreEqual(respMigrate, "OK");
            context.logger.LogDebug("4. Set slot {_slot} to MIGRATING state on node {port}", _workingSlot, context.clusterTestUtils.GetEndPoint(sourceNodeIndex).Port);

            var countKeys = context.clusterTestUtils.CountKeysInSlot(sourceNodeIndex, _workingSlot, context.logger);
            ClassicAssert.AreEqual(countKeys, keyCount);
            context.logger.LogDebug("5. CountKeysInSlot {countKeys}", countKeys);

            var keysInSlot = context.clusterTestUtils.GetKeysInSlot(sourceNodeIndex, _workingSlot, countKeys, context.logger);
            ClassicAssert.AreEqual(keys, keysInSlot);
            context.logger.LogDebug("6. GetKeysInSlot {keysInSlot.Count}", keysInSlot.Count);

            context.logger.LogDebug("7. MigrateKeys starting");
            context.clusterTestUtils.MigrateKeys(context.clusterTestUtils.GetEndPoint(sourceNodeIndex), context.clusterTestUtils.GetEndPoint(targetNodeIndex), keysInSlot, context.logger);
            context.logger.LogDebug("8. MigrateKeys done");

            var respNodeTarget = context.clusterTestUtils.SetSlot(targetNodeIndex, _workingSlot, "NODE", targetNodeId, logger: context.logger);
            ClassicAssert.AreEqual(respNodeTarget, "OK");
            context.logger.LogDebug("9a. SetSlot {_slot} to target NODE {port}", _workingSlot, context.clusterTestUtils.GetEndPoint(targetNodeIndex).Port);
            context.clusterTestUtils.BumpEpoch(targetNodeIndex, waitForSync: true, logger: context.logger);

            var respNodeSource = context.clusterTestUtils.SetSlot(sourceNodeIndex, _workingSlot, "NODE", targetNodeId, logger: context.logger);
            ClassicAssert.AreEqual(respNodeSource, "OK");
            context.logger.LogDebug("9b. SetSlot {_slot} to source NODE {port}", _workingSlot, context.clusterTestUtils.GetEndPoint(sourceNodeIndex).Port);
            context.clusterTestUtils.BumpEpoch(sourceNodeIndex, waitForSync: true, logger: context.logger);
            // End Migration

            context.logger.LogDebug("10. Checking config epoch");
            var targetConfigEpochFromTarget = context.clusterTestUtils.GetConfigEpochOfNodeFromNodeIndex(targetNodeIndex, targetNodeId, context.logger);
            var targetConfigEpochFromSource = context.clusterTestUtils.GetConfigEpochOfNodeFromNodeIndex(sourceNodeIndex, targetNodeId, context.logger);
            var targetConfigEpochFromOther = context.clusterTestUtils.GetConfigEpochOfNodeFromNodeIndex(otherNodeIndex, targetNodeId, context.logger);

            while (targetConfigEpochFromOther != targetConfigEpochFromTarget || targetConfigEpochFromSource != targetConfigEpochFromTarget)
            {
                _ = Thread.Yield();
                targetConfigEpochFromTarget = context.clusterTestUtils.GetConfigEpochOfNodeFromNodeIndex(targetNodeIndex, targetNodeId, context.logger);
                targetConfigEpochFromSource = context.clusterTestUtils.GetConfigEpochOfNodeFromNodeIndex(sourceNodeIndex, targetNodeId, context.logger);
                targetConfigEpochFromOther = context.clusterTestUtils.GetConfigEpochOfNodeFromNodeIndex(otherNodeIndex, targetNodeId, context.logger);
            }
            ClassicAssert.AreEqual(targetConfigEpochFromTarget, targetConfigEpochFromOther);
            ClassicAssert.AreEqual(targetConfigEpochFromTarget, targetConfigEpochFromSource);
            context.logger.LogDebug("11. Success config epoch");

            context.logger.LogDebug("13. Checking migrate keys starting");
            foreach (var _key in keys)
            {
                var resp = context.clusterTestUtils.GetKey(otherNodeIndex, _key, out var slot, out var address, out var port, out var responseState, logger: context.logger);
                while (port != context.clusterTestUtils.GetEndPoint(targetNodeIndex).Port && responseState != ResponseState.OK)
                {
                    resp = context.clusterTestUtils.GetKey(otherNodeIndex, _key, out slot, out address, out port, out responseState, logger: context.logger);
                }
                ClassicAssert.AreEqual(resp, "MOVED");
                ClassicAssert.AreEqual(_workingSlot, slot);
                ClassicAssert.AreEqual(context.clusterTestUtils.GetEndPoint(targetNodeIndex).Address.ToString(), address);
                ClassicAssert.AreEqual(context.clusterTestUtils.GetEndPoint(targetNodeIndex).Port, port);

                resp = context.clusterTestUtils.GetKey(targetNodeIndex, _key, out _, out _, out _, out responseState, logger: context.logger);
                ClassicAssert.AreEqual(responseState, ResponseState.OK);
                ClassicAssert.AreEqual(resp, _key);
            }
            context.logger.LogDebug("14. Checking migrate keys done");

            context.clusterTestUtils.WaitForMigrationCleanup(context.logger);
            context.logger.LogDebug("15. ClusterSimpleMigrateKeysTest done");
        }

        [Test, Order(10)]
        [Category("CLUSTER")]
        public void ClusterSimpleMigrateKeysWithObjects()
        {
            context.logger.LogDebug("0. ClusterSimpleMigrateKeysWithObjectsTest started");
            var Port = TestUtils.Port;
            var Shards = defaultShards;
            context.CreateInstances(Shards, useTLS: UseTLS);
            context.CreateConnection(useTLS: UseTLS);
            var (_, slots) = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var otherNodeIndex = 0;
            var sourceNodeIndex = 1;
            var targetNodeIndex = 2;
            var sourceNodeId = context.clusterTestUtils.GetNodeIdFromNode(sourceNodeIndex, context.logger);
            var targetNodeId = context.clusterTestUtils.GetNodeIdFromNode(targetNodeIndex, context.logger);

            var memberCount = 10;
            var keyCount = 10;
            var key = Encoding.ASCII.GetBytes("{abc}a");
            List<byte[]> keys = [];
            var _slot = ClusterTestUtils.HashSlot(key);
            ClassicAssert.AreEqual(7638, _slot);

            context.logger.LogDebug("1. Creating data started {keyCount}", keyCount);
            Dictionary<byte[], List<Tuple<int, byte[]>>> data = new(new ByteArrayComparer());
            for (var i = 0; i < keyCount; i++)
            {
                var newKey = new byte[key.Length];
                Array.Copy(key, 0, newKey, 0, key.Length);
                newKey[^1] = (byte)(newKey[^1] + i);
                keys.Add(newKey);
                ClassicAssert.AreEqual(_slot, ClusterTestUtils.HashSlot(newKey));

                var (_, memberPair) = DoZADD(sourceNodeIndex, newKey, memberCount);
                data.Add(newKey, memberPair);
            }

            context.logger.LogDebug("2. Creating data done {keyCount}", keyCount);

            string _Address;
            int _Port;
            int _Slot;
            context.logger.LogDebug("3. Checking keys before migration started");
            foreach (var _key in data.Keys)
            {
                var resp = DoZCOUNT(sourceNodeIndex, key, out var count, out _Address, out _Port, out _Slot, logger: context.logger);
                ClassicAssert.AreEqual("OK", resp);
                ClassicAssert.AreEqual(data[_key].Count, count);

                List<string> members;
                (resp, members) = DoZRANGE(sourceNodeIndex, _key, out _Address, out _Port, out _Slot);
                var expectedMembers = data[_key].Select(x => Encoding.ASCII.GetString(x.Item2)).ToList();
                ClassicAssert.AreEqual(expectedMembers, members);
                context.logger.LogDebug("2. Loading object keys data done");
            }
            context.logger.LogDebug("4. Checking keys before migration done");

            // Start Migration
            var respImport = context.clusterTestUtils.SetSlot(targetNodeIndex, _slot, "IMPORTING", sourceNodeId, logger: context.logger);
            ClassicAssert.AreEqual("OK", respImport, "IMPORTING");
            context.logger.LogDebug("5. Set slot {_slot} to IMPORTING state on node {port}", _slot, context.clusterTestUtils.GetEndPoint(targetNodeIndex).Port);

            var respMigrate = context.clusterTestUtils.SetSlot(sourceNodeIndex, _slot, "MIGRATING", targetNodeId, logger: context.logger);
            ClassicAssert.AreEqual("OK", respMigrate, "MIGRATING");
            context.logger.LogDebug("6. Set slot {_slot} to MIGRATING state on node {port}", _slot, context.clusterTestUtils.GetEndPoint(sourceNodeIndex).Port);

            var countKeys = context.clusterTestUtils.CountKeysInSlot(sourceNodeIndex, _slot, context.logger);
            ClassicAssert.AreEqual(countKeys, keyCount);
            context.logger.LogDebug("7. CountKeysInSlot {countKeys}", countKeys);

            var keysInSlot = context.clusterTestUtils.GetKeysInSlot(sourceNodeIndex, _slot, countKeys, context.logger);
            ClassicAssert.AreEqual(keys, keysInSlot);
            context.logger.LogDebug("8. GetKeysInSlot {keysInSlot.Count}", keysInSlot.Count);

            context.logger.LogDebug("9. MigrateKeys starting");
            context.clusterTestUtils.MigrateKeys(context.clusterTestUtils.GetEndPoint(sourceNodeIndex), context.clusterTestUtils.GetEndPoint(targetNodeIndex), keysInSlot, context.logger);
            context.logger.LogDebug("10. MigrateKeys done");

            var respNodeTarget = context.clusterTestUtils.SetSlot(targetNodeIndex, _slot, "NODE", targetNodeId, logger: context.logger);
            ClassicAssert.AreEqual(respNodeTarget, "OK");
            context.logger.LogDebug("9a. SetSlot {_slot} to target NODE {port}", _slot, context.clusterTestUtils.GetEndPoint(targetNodeIndex).Port);
            var respNodeSource = context.clusterTestUtils.SetSlot(sourceNodeIndex, _slot, "NODE", targetNodeId, logger: context.logger);
            ClassicAssert.AreEqual(respNodeSource, "OK");
            context.logger.LogDebug("9b. SetSlot {_slot} to source NODE {port}", _slot, context.clusterTestUtils.GetEndPoint(sourceNodeIndex).Port);
            // End Migration

            context.logger.LogDebug("10. Checking config epoch");
            var targetConfigEpochFromTarget = context.clusterTestUtils.GetConfigEpochOfNodeFromNodeIndex(targetNodeIndex, targetNodeId, context.logger);
            var targetConfigEpochFromSource = context.clusterTestUtils.GetConfigEpochOfNodeFromNodeIndex(sourceNodeIndex, targetNodeId, context.logger);
            var targetConfigEpochFromOther = context.clusterTestUtils.GetConfigEpochOfNodeFromNodeIndex(otherNodeIndex, targetNodeId, context.logger);

            while (targetConfigEpochFromOther != targetConfigEpochFromTarget || targetConfigEpochFromSource != targetConfigEpochFromTarget)
            {
                _ = Thread.Yield();
                targetConfigEpochFromTarget = context.clusterTestUtils.GetConfigEpochOfNodeFromNodeIndex(targetNodeIndex, targetNodeId, context.logger);
                targetConfigEpochFromSource = context.clusterTestUtils.GetConfigEpochOfNodeFromNodeIndex(sourceNodeIndex, targetNodeId, context.logger);
                targetConfigEpochFromOther = context.clusterTestUtils.GetConfigEpochOfNodeFromNodeIndex(otherNodeIndex, targetNodeId, context.logger);
            }
            ClassicAssert.AreEqual(targetConfigEpochFromTarget, targetConfigEpochFromOther);
            ClassicAssert.AreEqual(targetConfigEpochFromTarget, targetConfigEpochFromSource);
            context.logger.LogDebug("11. Success config epoch");

            context.logger.LogDebug("14. Checking migrate keys starting");
            foreach (var _key in data.Keys)
            {
                var resp = DoZCOUNT(targetNodeIndex, key, out var count, out _Address, out _Port, out _Slot, logger: context.logger);
                ClassicAssert.AreEqual(resp, "OK");
                ClassicAssert.AreEqual(data[_key].Count, count);

                List<string> members;
                (resp, members) = DoZRANGE(targetNodeIndex, _key, out _Address, out _Port, out _Slot, context.logger);
                var expectedMembers = data[_key].Select(x => Encoding.ASCII.GetString(x.Item2)).ToList();
                ClassicAssert.AreEqual(expectedMembers, members);
                context.logger.LogDebug("2. Loading object keys data done");
            }
            context.logger.LogDebug("15. Checking migrate keys done");
            context.clusterTestUtils.WaitForMigrationCleanup(context.logger);
            context.logger.LogDebug("16. ClusterSimpleMigrateKeysWithObjectsTest done");
        }

        private void MigrateSlotsTask(int sourceNodePort, int targetNodePort, List<int> slots, ILogger logger = null)
        {
            context.clusterTestUtils.MigrateSlots(sourceNodePort, targetNodePort, slots, logger: logger);
        }

        List<(int, byte[], byte[])> operatedOnData;

        private void OperateOnSlotsTask(Dictionary<int, Dictionary<byte[], byte[]>> data, int targetNodeIndex)
        {
            var Port = TestUtils.Port;
            var Shards = defaultShards;
            var Ports = Enumerable.Range(Port, Shards).ToList();
            var connections = ClusterTestUtils.CreateLightRequestConnections([.. Ports]);
            operatedOnData = [];

            foreach (var slot in data.Keys)
            {
                foreach (var entry in data[slot])
                    operatedOnData.Add(new(slot, entry.Key, entry.Value));
            }

            var iterCount = 0;
            var maxIter = 100;
            while (true)
            {
                if (iterCount++ > maxIter) break;
                var entryIndex = context.r.Next(0, operatedOnData.Count);
                var nodeIndex = context.r.Next(0, Shards);
                var get = context.r.Next(0, 1) == 0;
                var oldEntry = operatedOnData[entryIndex];
                var key = oldEntry.Item2;
                var value = oldEntry.Item3;

            retryRequest:

                if (get)
                {
                    var getValue = context.clusterTestUtils.GetKey(nodeIndex, key, out var slot, out var redirectAddressA, out var redirectPortA, out var status, logger: context.logger);
                    switch (status)
                    {
                        case ResponseState.OK:
                            ClassicAssert.AreEqual(Encoding.ASCII.GetString(value), getValue, $"{Encoding.ASCII.GetString(value)} => {getValue}");
                            break;
                        case ResponseState.MOVED: // Everyone redirect to node that is current owner
                            var srcNodeIndex = context.clusterTestUtils.GetEndPointIndexFromPort(redirectPortA);
                            ClassicAssert.AreNotEqual(srcNodeIndex, -1);
                            getValue = context.clusterTestUtils.GetKey(srcNodeIndex, key, out _, out var redirectAddressB, out var redirectPortB, out status, logger: context.logger);
                            if (status == ResponseState.OK)
                                ClassicAssert.AreEqual(value, getValue, $"{redirectPortA} => {redirectPortB}");
                            else if (status == ResponseState.MOVED)// can redirect again if source has made target the owner
                            {
                                //ClassicAssert.AreEqual(connections[targetNodeIndex].Port, redirectPortB,
                                //    $"{connections[nodeIndex].Port} => {redirectPortA} => {redirectPortB}");
                                //ClassicAssert.AreEqual(connections[targetNodeIndex].Address, redirectAddressB);
                            }
                            break;
                        case ResponseState.ASK:
                            ClassicAssert.AreEqual(connections[targetNodeIndex].Port, redirectPortA);
                            ClassicAssert.AreEqual(connections[targetNodeIndex].Address, redirectAddressA);
                            break;
                        case ResponseState.CLUSTERDOWN:
                            goto retryRequest;
                        default:
                            Assert.Fail($"{status} {getValue}");
                            break;
                    }
                }
                else
                {
                    var newValue = new byte[value.Length];
                    RandomBytes(ref newValue);
                    var status = context.clusterTestUtils.SetKey(nodeIndex, key, newValue, out _, out var address, out var port, logger: context.logger);
                    switch (status)
                    {
                        case ResponseState.OK:
                            operatedOnData[entryIndex] = new(oldEntry.Item1, oldEntry.Item2, newValue);
                            break;
                        case ResponseState.MOVED: //everyone redirect to node that is current owner
                            var srcNodeIndex = context.clusterTestUtils.GetEndPointIndexFromPort(port);
                            ClassicAssert.AreNotEqual(srcNodeIndex, -1);
                            status = context.clusterTestUtils.SetKey(srcNodeIndex, key, newValue, out _, out address, out port, logger: context.logger);
                            if (status == ResponseState.OK)
                                operatedOnData[entryIndex] = new(oldEntry.Item1, oldEntry.Item2, newValue);
                            else if (status == ResponseState.MOVED)
                            {
                                //srcNodeIndex = clusterTestUtils.GetEndPointIndexFromPort(port);
                                //ClassicAssert.AreNotEqual(srcNodeIndex, -1);
                                //status = clusterTestUtils.SetKey(srcNodeIndex, key, newValue, out _, out address, out port);
                                //if (status == ResponseState.OK)
                                //    operatedOnData[entryIndex] = new(oldEntry.Item1, oldEntry.Item2, newValue);
                            }

                            break;
                        case ResponseState.ASK:
                            ClassicAssert.AreEqual(connections[targetNodeIndex].Port, port);
                            ClassicAssert.AreEqual(connections[targetNodeIndex].Address, address);
                            break;
                        case ResponseState.CLUSTERDOWN:
                            goto retryRequest;
                        default:
                            Assert.Fail($"{status}");
                            break;
                    }
                }
            }
        }

        [Test, Order(11)]
        [Category("CLUSTER")]
        public void ClusterSimpleMigrateWithReadWrite()
        {
            context.logger.LogDebug("0. ClusterSimpleMigrateTestWithReadWrite started");
            var Shards = defaultShards;
            context.CreateInstances(Shards, useTLS: UseTLS);
            context.CreateConnection(useTLS: UseTLS);
            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var sourceNodeIndex = 1;
            var targetNodeIndex = 2;
            var keyLen = 8;
            var valLen = 32;
            var slotCount = 10;
            var keyCount = 100;
            context.logger.LogDebug("1. CreateMultiSlotData {keyCount} started", keyCount);
            CreateMultiSlotData(
                slotCount,
                keyLen,
                valLen,
                4,
                keyCount,
                out var data,
                new HashSet<int>(context.clusterTestUtils.GetOwnedSlotsFromNode(sourceNodeIndex, context.logger)));
            context.logger.LogDebug("2. CreateMultiSlotData {keyCount} done", keyCount);

            context.logger.LogDebug("2. Running workload and migration task");
            var migratedSlots = data.Keys.ToList();
            MigrateSlotsTask(
                            context.clusterTestUtils.GetEndPoint(sourceNodeIndex).Port,
                            context.clusterTestUtils.GetEndPoint(targetNodeIndex).Port,
                            migratedSlots,
                            logger: context.logger);
            OperateOnSlotsTask(data, targetNodeIndex);
            context.logger.LogDebug("3. Migration and workload done");

            context.logger.LogDebug("4. Checking keys after migration started");
            foreach (var entry in operatedOnData)
            {
                var key = entry.Item2;
                var val = entry.Item3;

                var resp = context.clusterTestUtils.GetKey(targetNodeIndex, key, out var slot, out var address, out var port, out var responseState, logger: context.logger);
                while (responseState != ResponseState.OK)
                {
                    _ = Thread.Yield();
                    resp = context.clusterTestUtils.GetKey(targetNodeIndex, key, out slot, out address, out port, out responseState, logger: context.logger);
                }
                ClassicAssert.AreEqual(ResponseState.OK, responseState);
                ClassicAssert.AreEqual(val, resp, $"{val} != {resp}");
                ClassicAssert.AreEqual(context.clusterTestUtils.GetEndPoint(targetNodeIndex).Port, port);
                ClassicAssert.AreEqual(context.clusterTestUtils.GetEndPoint(targetNodeIndex).Address.ToString(), address);
            }

            context.logger.LogDebug("5. Checking keys after migration done");
            context.logger.LogDebug("6. ClusterSimpleMigrateTestWithReadWrite done");
            context.clusterTestUtils.WaitForMigrationCleanup(context.logger);
        }

        [Test, Order(12)]
        [Category("CLUSTER")]
        public void ClusterSimpleTxn()
        {
            context.CreateInstances(defaultShards, useTLS: UseTLS);
            context.CreateConnection(useTLS: UseTLS);

            List<(string, ICollection<object>)> commands = [];
            var keyA = new RedisKey("{wxz}A");
            var keyB = new RedisKey("{wxz}B");

            commands.Add(("set", new List<object>() { keyA, "1" }));
            commands.Add(("set", new List<object>() { keyB, "2" }));
            commands.Add(("get", new List<object>() { keyA }));
            commands.Add(("get", new List<object>() { keyB }));

            var resp = context.clusterTestUtils.ExecuteTxnForShard(0, commands);
            ClassicAssert.AreEqual(ResponseState.CLUSTERDOWN, resp.state);

            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);
            var config = context.clusterTestUtils.ClusterNodes(0);

            var nodeForKeyA = config.GetBySlot(keyA);
            var nodeForKeyB = config.GetBySlot(keyB);
            ClassicAssert.AreEqual(nodeForKeyA, nodeForKeyB);

            resp = context.clusterTestUtils.ExecuteTxnForShard(0, commands);
            ClassicAssert.AreEqual(ResponseState.OK, resp.state);

            var arrResult = (RedisResult[])resp.result;
            ClassicAssert.AreEqual("OK", (string)arrResult[0]);
            ClassicAssert.AreEqual("OK", (string)arrResult[1]);
            ClassicAssert.AreEqual("1", (string)arrResult[2]);
            ClassicAssert.AreEqual("2", (string)arrResult[3]);

            resp = context.clusterTestUtils.ExecuteTxnForShard(1, commands);
            ClassicAssert.AreEqual(ResponseState.MOVED, resp.state);
        }

        private static readonly object[] _slotranges =
        [
            new object[] { new List<int>() { 5500, 5510 } },
            new object[] { new List<int>() { 6000, 6015, 9020, 9050 } }
        ];

        [Test, Order(13)]
        [Category("CLUSTER")]
        [TestCaseSource("_slotranges")]
        public void ClusterSimpleMigrateSlotsRanges(List<int> migrateRange)
        {
            context.logger.LogDebug("0. ClusterSimpleMigrateSlotsRanges started");
            var Shards = defaultShards;
            context.CreateInstances(Shards, useTLS: UseTLS);
            context.CreateConnection(useTLS: UseTLS);
            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var sourceNodeIndex = 1;
            var targetNodeIndex = 2;
            var targetNodeId = context.clusterTestUtils.GetNodeIdFromNode(targetNodeIndex, context.logger);

            var config = context.clusterTestUtils.ClusterNodes(sourceNodeIndex, context.logger).Nodes.First();
            ClassicAssert.IsTrue(config.IsMyself);

            var sourceEndPoint = context.clusterTestUtils.GetEndPoint(sourceNodeIndex);
            var targetEndPoint = context.clusterTestUtils.GetEndPoint(targetNodeIndex);
            context.clusterTestUtils.MigrateSlots(
                sourceEndPoint,
                targetEndPoint,
                migrateRange,
                range: true,
                logger: context.logger);

            while (true)
            {
                var _config = context.clusterTestUtils.ClusterNodes(targetNodeIndex, context.logger);
                var success = true;
                for (var i = 0; i < migrateRange.Count; i += 2)
                {
                    var start = migrateRange[i];
                    var end = migrateRange[i + 1];

                    for (var j = start; j <= end; j++)
                    {
                        var node = _config.GetBySlot(j);
                        if (!node.NodeId.Equals(targetNodeId))
                            success = false;
                    }
                }
                if (success)
                    break;

                _ = Thread.Yield();
            }

            context.clusterTestUtils.WaitForMigrationCleanup(context.logger);
        }

        [Test, Order(14)]
        [Category("CLUSTER")]
        [TestCaseSource("_slotranges")]
        public void ClusterSimpleMigrateWithAuth(List<int> migrateRange)
        {
            context.logger.LogDebug("0. ClusterSimpleMigrateWithAuth started");
            var Shards = defaultShards;
            context.CreateInstances(Shards, useTLS: UseTLS);
            context.CreateConnection(useTLS: UseTLS);
            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var sourceNodeIndex = 1;
            var targetNodeIndex = 2;
            var targetNodeId = context.clusterTestUtils.GetNodeIdFromNode(targetNodeIndex, context.logger);

            var config = context.clusterTestUtils.ClusterNodes(sourceNodeIndex, context.logger).Nodes.First();
            ClassicAssert.IsTrue(config.IsMyself);

            var sourceEndPoint = context.clusterTestUtils.GetEndPoint(sourceNodeIndex);
            var targetEndPoint = context.clusterTestUtils.GetEndPoint(targetNodeIndex);
            context.clusterTestUtils.MigrateSlots(
                sourceEndPoint,
                targetEndPoint,
                migrateRange,
                range: true,
                authPassword: authPassword,
                logger: context.logger);

            while (true)
            {
                var _config = context.clusterTestUtils.ClusterNodes(targetNodeIndex, context.logger);
                var success = true;
                for (var i = 0; i < migrateRange.Count; i += 2)
                {
                    var start = migrateRange[i];
                    var end = migrateRange[i + 1];

                    for (int j = start; j <= end; j++)
                    {
                        var node = _config.GetBySlot(j);
                        if (!node.NodeId.Equals(targetNodeId))
                            success = false;
                    }
                }
                if (success)
                    break;

                _ = Thread.Yield();
            }

            context.clusterTestUtils.WaitForMigrationCleanup(context.logger);
        }

        [Test, Order(15)]
        [Category("CLUSTER")]
        public void ClusterAllowWritesDuringMigrateTest()
        {
            context.logger.LogDebug("0. ClusterSimpleMigrateTestWithReadWrite started");
            var Shards = defaultShards;
            context.CreateInstances(Shards, useTLS: UseTLS);
            context.CreateConnection(useTLS: UseTLS);
            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var keyExists = Encoding.ASCII.GetBytes("{abc}01");
            var keyNotExists = Encoding.ASCII.GetBytes("{abc}02");
            var oldValue = Encoding.ASCII.GetBytes("initialValue");
            var newValue = Encoding.ASCII.GetBytes("newValue");

            var config = context.clusterTestUtils.ClusterNodes(0, logger: context.logger);
            var n01 = config.GetBySlot(keyExists);
            var n02 = config.GetBySlot(keyNotExists);
            ClassicAssert.AreEqual(n01.NodeId, n02.NodeId);

            // Create key before migration
            var status = context.clusterTestUtils.SetKey(n01.EndPoint.ToIPEndPoint(), keyExists, oldValue, out _, out _, out _, logger: context.logger);
            ClassicAssert.AreEqual(ResponseState.OK, status);

            // Get slot mapping
            var slot = context.clusterTestUtils.ClusterKeySlot(n01.EndPoint.ToIPEndPoint(), Encoding.ASCII.GetString(keyExists));
            var anyOtherNode = context.clusterTestUtils.GetAnyOtherNode(n01.EndPoint.ToIPEndPoint(), logger: context.logger);

            // Set slot to MIGRATING state
            var resp = context.clusterTestUtils.SetSlot(n01.EndPoint.ToIPEndPoint(), slot, "MIGRATING", anyOtherNode.NodeId);
            ClassicAssert.AreEqual("OK", resp);

            // Set slot to IMPORTING state
            resp = context.clusterTestUtils.SetSlot(anyOtherNode.EndPoint.ToIPEndPoint(), slot, "IMPORTING", n01.NodeId);
            ClassicAssert.AreEqual("OK", resp);

            // Operate on existing key during migration
            OperateOnExistingKey(n01.EndPoint, keyExists, oldValue, newValue);

            // Operate on non-existing key during migration
            OperateOnNonExistentKey(n01.EndPoint, keyNotExists, oldValue);

            // Run background write workload
            var task = Task.Run(() => WriteWorkload(n01.EndPoint.ToIPEndPoint(), keyExists));

            Thread.Sleep(TimeSpan.FromSeconds(2));

            // Migrate key to target node
            context.clusterTestUtils.MigrateKeys(n01.EndPoint.ToIPEndPoint(), anyOtherNode.EndPoint.ToIPEndPoint(), [keyExists], context.logger);

            // Wait for write workload to finish
            // Should finish when ResponseState != OK which means migration has completed
            oldValue = task.Result;
            oldValue ??= newValue;

            // Operate on migrated key that should not exist so the expected response will be redirect ASK
            OperateOnNonExistentKey(n01.EndPoint, keyExists, newValue);

            // Assign slot to target node
            var respNodeTarget = context.clusterTestUtils.SetSlot(anyOtherNode.EndPoint.ToIPEndPoint(), slot, "NODE", anyOtherNode.NodeId, logger: context.logger);
            ClassicAssert.AreEqual(respNodeTarget, "OK");
            context.clusterTestUtils.BumpEpoch(anyOtherNode.EndPoint.ToIPEndPoint(), waitForSync: true, logger: context.logger);

            // Relinquish slot ownership from source node
            var respNodeSource = context.clusterTestUtils.SetSlot(n01.EndPoint.ToIPEndPoint(), slot, "NODE", n01.NodeId, logger: context.logger);
            ClassicAssert.AreEqual(respNodeSource, "OK");
            context.clusterTestUtils.BumpEpoch(n01.EndPoint.ToIPEndPoint(), waitForSync: true, logger: context.logger);

            // Operate on existing key after migration
            OperateOnExistingKey(anyOtherNode.EndPoint, keyExists, oldValue, newValue);

            // Operate on non existent when slot is in MIGRATING state
            void OperateOnNonExistentKey(EndPoint endPoint, byte[] key, byte[] value)
            {
                // Perform write => expected response ASK
                status = context.clusterTestUtils.SetKey(endPoint.ToIPEndPoint(), key, value, out int _slot, out string address, out int port, logger: context.logger);
                ClassicAssert.AreEqual(ResponseState.ASK, status);
                ClassicAssert.AreEqual(slot, _slot);
                ClassicAssert.AreEqual(anyOtherNode.EndPoint.ToIPEndPoint().Address.ToString(), address);
                ClassicAssert.AreEqual(anyOtherNode.EndPoint.ToIPEndPoint().Port, port);

                // Perform read => expected response ASK
                _ = context.clusterTestUtils.GetKey(endPoint.ToIPEndPoint(), key, out _slot, out address, out port, out status, logger: context.logger);
                ClassicAssert.AreEqual(ResponseState.ASK, status);
                ClassicAssert.AreEqual(slot, _slot);
                ClassicAssert.AreEqual(anyOtherNode.EndPoint.ToIPEndPoint().Address.ToString(), address);
                ClassicAssert.AreEqual(anyOtherNode.EndPoint.ToIPEndPoint().Port, port);
            }

            // Operate on existing key when slot is in MIGRATING state
            void OperateOnExistingKey(EndPoint endPoint, byte[] key, byte[] oldValue, byte[] newValue)
            {
                // Perform read => expected response OK
                var _value = context.clusterTestUtils.GetKey(endPoint.ToIPEndPoint(), keyExists, out _, out _, out _, out status, logger: context.logger);
                ClassicAssert.AreEqual(ResponseState.OK, status);
                ClassicAssert.AreEqual(oldValue, _value);

                //  Perform write => expected response OK
                status = context.clusterTestUtils.SetKey(endPoint.ToIPEndPoint(), key, newValue, out _, out _, out _, logger: context.logger);
                ClassicAssert.AreEqual(ResponseState.OK, status);

                // Perform read => expected response OK
                _value = context.clusterTestUtils.GetKey(endPoint.ToIPEndPoint(), keyExists, out _, out _, out _, out status, logger: context.logger);
                ClassicAssert.AreEqual(ResponseState.OK, status);
                ClassicAssert.AreEqual(newValue, _value);
            }

            Task<byte[]> WriteWorkload(IPEndPoint endPoint, byte[] key, int keyLen = 16)
            {
                var value = new byte[keyLen];
                byte[] setValue = null;
                while (true)
                {
                    context.clusterTestUtils.RandomBytes(ref value);
                    var status = context.clusterTestUtils.SetKey(endPoint, key, value, out int _slot, out string address, out int port, logger: context.logger);

                    if (status == ResponseState.OK)
                    {
                        setValue ??= new byte[keyLen];
                        // If succeeded keep track of setValue
                        value.AsSpan().CopyTo(setValue.AsSpan());
                    }
                    else
                        // If failed then return last setValue
                        return Task.FromResult(setValue);
                }
            }
        }

        [Test, Order(16)]
        [Category("CLUSTER")]
        public void ClusterMigrateForgetTest()
        {
            context.logger.LogDebug("0. ClusterSimpleMigrateSlotsRanges started");
            var Shards = defaultShards;
            context.CreateInstances(Shards, useTLS: UseTLS);
            context.CreateConnection(useTLS: UseTLS);
            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var sourceNodeIndex = 0;
            var targetNodeIndex = 1;
            var sourceNodeId = context.clusterTestUtils.ClusterMyId(sourceNodeIndex, context.logger);
            var targetNodeId = context.clusterTestUtils.ClusterMyId(targetNodeIndex, context.logger);

            var numSlots = 3;
            for (var slot = 0; slot < numSlots; slot++)
            {
                var migresp = context.clusterTestUtils.SetSlot(sourceNodeIndex, slot, "MIGRATING", targetNodeId, context.logger);
                ClassicAssert.AreEqual("OK", migresp);

                var slotState = context.clusterTestUtils.SlotState(sourceNodeIndex, slot, context.logger);
                ClassicAssert.AreEqual(3, slotState.Length);
                ClassicAssert.AreEqual(slot.ToString(), slotState[0]);
                ClassicAssert.AreEqual(">", slotState[1]);
                ClassicAssert.AreEqual(targetNodeId, slotState[2]);
            }

            var resp = context.clusterTestUtils.ClusterForget(sourceNodeIndex, targetNodeId, 100, context.logger);
            ClassicAssert.AreEqual("OK", resp);

            for (var slot = 0; slot < numSlots; slot++)
            {
                var slotState = context.clusterTestUtils.SlotState(sourceNodeIndex, slot, context.logger);
                ClassicAssert.AreEqual(3, slotState.Length);
                ClassicAssert.AreEqual(slot.ToString(), slotState[0]);
                ClassicAssert.AreEqual("=", slotState[1]);
                ClassicAssert.AreEqual(sourceNodeId, slotState[2]);
            }
        }

        [Test, Order(16)]
        [Category("CLUSTER")]
        public void ClusterMigrateDataSlotsRange()
        {
            var Shards = 2;
            context.CreateInstances(Shards, useTLS: UseTLS);
            context.CreateConnection(useTLS: UseTLS);

            var srcNodeIndex = 0;
            var dstNodeIndex = 1;
            ClassicAssert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(srcNodeIndex, [(0, 16383)], addslot: true, logger: context.logger));

            context.clusterTestUtils.SetConfigEpoch(srcNodeIndex, srcNodeIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(dstNodeIndex, dstNodeIndex + 2, logger: context.logger);
            context.clusterTestUtils.Meet(srcNodeIndex, dstNodeIndex, logger: context.logger);

            var keySize = 16;
            var keyCount = 1024;
            List<byte[]> keys = [];

            context.logger.LogDebug("1. Loading test keys {keyCount}", keyCount);
            for (var i = 0; i < keyCount; i++)
            {
                var key = new byte[keySize];
                context.clusterTestUtils.RandomBytes(ref key);

                var resp = context.clusterTestUtils.SetKey(srcNodeIndex, key, key, out _, out var address, out var port, logger: context.logger);
                ClassicAssert.AreEqual(resp, ResponseState.OK);
                ClassicAssert.AreEqual(address, context.clusterTestUtils.GetEndPoint(srcNodeIndex).Address.ToString());
                ClassicAssert.AreEqual(port, context.clusterTestUtils.GetEndPoint(srcNodeIndex).Port);
                keys.Add(key);
            }

            var srcDBsize = context.clusterTestUtils.DBSize(srcNodeIndex, context.logger);
            var dstDBsize = context.clusterTestUtils.DBSize(dstNodeIndex, context.logger);
            ClassicAssert.AreEqual(keyCount, srcDBsize);
            ClassicAssert.AreEqual(0, dstDBsize);

            var sourceEndPoint = context.clusterTestUtils.GetEndPoint(srcNodeIndex);
            var targetEndPoint = context.clusterTestUtils.GetEndPoint(dstNodeIndex);
            context.clusterTestUtils.MigrateSlots(
                sourceEndPoint,
                targetEndPoint,
                [0, 16383],
                range: true,
                logger: context.logger);

            context.clusterTestUtils.WaitForMigrationCleanup(srcNodeIndex, logger: context.logger);
            srcDBsize = context.clusterTestUtils.DBSize(srcNodeIndex, context.logger);
            dstDBsize = context.clusterTestUtils.DBSize(dstNodeIndex, context.logger);
            ClassicAssert.AreEqual(0, srcDBsize);
            ClassicAssert.AreEqual(keyCount, dstDBsize);

            foreach (var key in keys)
            {
                var resp = context.clusterTestUtils.GetKey(dstNodeIndex, key, out _, out _, out _, out var responseState, logger: context.logger);
                ClassicAssert.AreEqual(ResponseState.OK, responseState);
                ClassicAssert.AreEqual(Encoding.ASCII.GetString(key), resp);
            }
        }
    }
}