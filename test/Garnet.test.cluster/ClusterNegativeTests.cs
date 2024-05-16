// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class ClusterNegativeTests
    {
        ClusterTestContext context;

        readonly HashSet<string> monitorTests = [];

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup(monitorTests);
        }

        [TearDown]
        public void TearDown()
        {
            context.TearDown();
        }

        [Test, Order(1)]
        [Category("CLUSTER")]
        [TestCase("bumpepoch", new int[] { 1, 2, 3 })]
        [TestCase("failover", new int[] { 3, 4 })]
        [TestCase("forget", new int[] { 0, 3, 4 })]
        [TestCase("info", new int[] { 1, 2, 3 })]
        [TestCase("help", new int[] { 1, 2, 3 })]
        [TestCase("meet", new int[] { 0, 1, 3, 4 })]
        [TestCase("myid", new int[] { 1, 2, 3 })]
        [TestCase("myparentid", new int[] { 1, 2, 3 })]
        [TestCase("endpoint", new int[] { 0, 2, 3 })]
        [TestCase("nodes", new int[] { 1, 2, 3 })]
        [TestCase("set-config-epoch", new int[] { 0, 2, 3 })]
        [TestCase("shards", new int[] { 1, 2, 3 })]
        [TestCase("reset", new int[] { 3, 4, 5 })]
        [TestCase("addslots", new int[] { 0, 17000 })]
        [TestCase("addslotsrange", new int[] { 0, 3, 5, 7 })]
        [TestCase("banlist", new int[] { 1, 2, 3, 4 })]
        [TestCase("countkeysinslot", new int[] { 0, 2, 3 })]
        [TestCase("delslots", new int[] { 0, 1700 })]
        [TestCase("delslotsrange", new int[] { 0, 3, 5, 7 })]
        [TestCase("delkeysinslot", new int[] { 0, 2, 3, 4 })]
        [TestCase("delkeysinslotrange", new int[] { 0, 3, 5, 7 })]
        [TestCase("getkeysinslot", new int[] { 0, 1, 3, 4 })]
        [TestCase("keyslot", new int[] { 0, 2, 3, 4 })]
        [TestCase("setslot", new int[] { 0, 1, 4, 5 })]
        [TestCase("setslotsrange", new int[] { 0, 1, 2 })]
        [TestCase("slots", new int[] { 1, 2, 3 })]
        [TestCase("slotstate", new int[] { 1, 2, 3 })]
        [TestCase("MIGRATE", new int[] { 0, 1, 2, 4, 5 })]
        [TestCase("mtasks", new int[] { 1, 2, 3, 4 })]
        [TestCase("replicas", new int[] { 1, 2, 3, 4 })]
        [TestCase("replicate", new int[] { 0, 3, 4 })]
        [TestCase("AOFSYNC", new int[] { 0, 1, 3, 4 })]
        [TestCase("APPENDLOG", new int[] { 0, 1, 2, 3, 4, 6 })]
        [TestCase("INITIATE_REPLICA_SYNC", new int[] { 0, 1, 2, 3, 4, 6 })]
        [TestCase("SEND_CKPT_METADATA", new int[] { 0, 1, 2, 4, 5, 6 })]
        [TestCase("SEND_CKPT_FILE_SEGMENT", new int[] { 0, 1, 2, 3, 4, 6 })]
        [TestCase("BEGIN_REPLICA_RECOVER", new int[] { 0, 1, 2, 3, 4, 5, 6, 8, 9 })]
        [TestCase("FAILAUTHREQ", new int[] { 0, 2, 4 })]
        [TestCase("FAILSTOPWRITES", new int[] { 0, 2, 3, 4 })]
        [TestCase("FAILREPLICATIONOFFSET", new int[] { 0, 2, 3, 4 })]
        public void ClusterCommandWrongParameters(string subcommand, params int[] invalidCount)
        {
            context.CreateInstances(1);

            using var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.NoDelay = true;
            socket.Connect(IPAddress.Loopback, 7000);

            var clusterCMD = $"$7\r\ncluster\r\n${subcommand.Length}\r\n{subcommand}\r\n";
            var errorCmd = subcommand.ToUpper();

            if (subcommand.Equals("set-config-epoch"))
                errorCmd = "SETCONFIGEPOCH";

            var expectedResp = $"-ERR wrong number of arguments for '{errorCmd}' command\r\n";
            foreach (var count in invalidCount)
            {
                var packet = $"*{2 + count}\r\n" + clusterCMD;
                for (var i = 0; i < count; i++)
                    packet += $"$3\r\nabc\r\n";

                var buffer = new byte[1024];
                var packetBytes = Encoding.ASCII.GetBytes(packet);
                var sent = socket.Send(packetBytes);
                Assert.AreEqual(packetBytes.Length, sent);
                int read;
                if ((read = socket.Receive(buffer)) > 0)
                {
                    var resp = Encoding.ASCII.GetString(buffer, 0, read);
                    Assert.AreEqual(expectedResp, resp);
                    break;
                }
            }
        }

        [Test, Order(2)]
        [Category("CLUSTER")]
        [TestCase(1024)]
        [TestCase(10240)]
        public void ClusterAddSlotsPartialPackage(int chunkSize)
        {
            context.CreateInstances(1);
            using var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.NoDelay = true;
            socket.Connect(IPAddress.Loopback, 7000);

            var slots = Enumerable.Range(0, 8192).ToList();
            var packet = $"*{2 + slots.Count}\r\n$7\r\ncluster\r\n$8\r\naddslots\r\n";

            foreach (var slot in slots)
                packet += $"${slot.ToString().Length}\r\n{slot}\r\n";

            Span<byte> packetBytes = Encoding.ASCII.GetBytes(packet);
            for (var i = 0; i < packetBytes.Length; i += chunkSize)
            {
                var size = i + chunkSize < packetBytes.Length ? chunkSize : packetBytes.Length - i;
                var slicePacket = packetBytes.Slice(i, size);
                var sent = socket.Send(slicePacket);
                Assert.AreEqual(slicePacket.Length, sent);
                Thread.Sleep(100);
            }

            var buffer = new byte[1024];
            int read;
            if ((read = socket.Receive(buffer)) > 0)
            {
                var resp = Encoding.ASCII.GetString(buffer, 0, read);
                Assert.AreEqual("+OK\r\n", resp);
            }
        }
    }
}