// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Garnet.server.TLS;
using GarnetClusterManagement;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    public enum ResponseState : byte
    {
        OK,
        MOVED,
        CLUSTERDOWN,
        ASK,
        MIGRATING,
        CROSSSLOT,
        REPLICA_WERR,
        NONE
    }

    public enum ClusterInfoTag : byte
    {
        NODEID = 0,
        ADDRESS = 1,
        FLAGS = 2,
        PRIMARY = 3,
        PING_SENT = 4,
        PONG_RECEIVED = 5,
        CONFIG_EPOCH = 6,
        LINK_STATE = 7,
        SLOT = 8,
    }

    public struct NodeNetInfo
    {
        public string address;
        public int port;
        public string nodeid;
        public string hostname;
        public bool isPrimary;
    }

    public struct SlotItem
    {
        public ushort startSlot;
        public ushort endSlot;
        public NodeNetInfo[] nnInfo;
    }

    public enum NodeRole
    {
        PRIMARY,
        REPLICA
    }

    public struct NodeInfo
    {
        public int nodeIndex;
        public string nodeid;
        public string address;
        public int port;
        public NodeRole role;
        public long replicationOffset;
    }

    public struct ShardInfo
    {
        public List<(int, int)> slotRanges;
        public List<NodeInfo> nodes;
    }

    public enum ReplicationInfoItem : byte
    {
        ROLE,
        CONNECTED_REPLICAS,
        PRIMARY_REPLID,
        REPLICATION_OFFSET,

        STORE_CURRENT_SAFE_AOF_ADDRESS,
        STORE_RECOVERED_SAFE_AOF_ADDRESS,
        PRIMARY_SYNC_IN_PROGRESS,
        PRIMARY_FAILOVER_STATE,
        RECOVER_STATUS,
        LAST_FAILOVER_STATE,
        SYNC_DRIVER_COUNT
    }

    public enum StoreInfoItem
    {
        CurrentVersion,
        LastCheckpointedVersion,
        RecoveredVersion
    }

    public struct PersistencInfo
    {
        public AofAddress CommittedBeginAddress;
        public AofAddress CommittedUntilAddress;
        public AofAddress FlushedUntilAddress;
        public AofAddress BeginAddress;
        public AofAddress TailAddress;
        public AofAddress SafeAofAddress;
    };

    public static class EndpointExtensions
    {
        public static IPEndPoint ToIPEndPoint(this EndPoint endPoint)
        {
            return (IPEndPoint)endPoint;
        }
    }

    public partial class ClusterTestUtils
    {
        private void ThrowException(string msg)
        {
            Dispose();
            throw new ArgumentException(msg);
        }

        public IPEndPoint[] GetEndpoints()
            => [.. endpoints.Select(x => (IPEndPoint)x)];

        public IPEndPoint[] GetEndpointsWithout(IPEndPoint endPoint) =>
            [.. endpoints.Select(x => (IPEndPoint)x).Where(x => x.Port != endPoint.Port || x.Address != endPoint.Address)];

        public RedisResult Execute(IPEndPoint endPoint, string cmd, ICollection<object> args, bool skipLogging = false, ILogger logger = null, CommandFlags flags = CommandFlags.None)
        {
            if (!skipLogging)
                logger?.LogInformation("({address}:{port}) > {cmd} {args}", endPoint.Address, endPoint.Port, cmd, string.Join(' ', args));
            try
            {
                var server = GetServer(endPoint);
                var resp = server.Execute(cmd, args, flags: flags);
                return resp;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error occured {cmd} {msg}", cmd, ex.Message);
                return RedisResult.Create((RedisValue)ex.Message);
            }
        }

        public RedisResult NodesV2(IPEndPoint endPoint, ILogger logger = null)
            => Execute(endPoint, "cluster", ["nodes"], skipLogging: true, logger);

        public string NodesMyself(IPEndPoint endPoint, ClusterInfoTag tag, ILogger logger)
        {
            var nodeConfigInfo = string.Empty;
            var nodeConfigStr = (string)NodesV2(endPoint, logger);
            if (nodeConfigStr != null)
            {
                var lines = nodeConfigStr.ToString().Split('\n');
                var properties = lines[0].ToString().Split(' ');
                int index = (int)tag;
                nodeConfigInfo = index < properties.Length ? properties[index].Trim() : string.Empty;
            }
            return nodeConfigInfo;
        }

        public string GetLocalNodeId(int nodeIndex, ILogger logger = null)
            => ClusterNodes(nodeIndex, logger).Nodes.First().NodeId;

        public string[] NodesMyself(IPEndPoint endPoint, ClusterInfoTag[] tags)
        {
            var nodeConfigInfo = new string[tags.Length];
            var nodeConfigStr = (string)NodesV2(endPoint);
            if (nodeConfigStr != null)
            {
                var lines = nodeConfigStr.ToString().Split('\n');
                var properties = lines[0].ToString().Split(' ');
                for (int i = 0; i < tags.Length; i++)
                {
                    var tag = tags[i];
                    int index = (int)tag;
                    nodeConfigInfo[i] = index < properties.Length ? properties[index].Trim() : string.Empty;
                }
            }
            return nodeConfigInfo;
        }

        public List<string[]> NodesAll(IPEndPoint endPoint, ClusterInfoTag[] tags, string nodeid = null)
        {
            var nodeConfigInfo = new List<string[]>();
            var nodeConfigStr = (string)NodesV2(endPoint);
            if (nodeConfigStr != null)
            {
                var lines = nodeConfigStr.ToString().Split('\n');
                for (var i = 0; i < lines.Length; i++)
                {
                    var properties = lines[i].ToString().Split(' ');
                    if (nodeid != null && !nodeid.Equals(properties[0].Trim()))
                        continue;

                    nodeConfigInfo.Add(new string[tags.Length]);
                    for (var j = 0; j < tags.Length; j++)
                    {
                        var tag = tags[j];
                        var index = (int)tag;
                        nodeConfigInfo[^1][j] = index < properties.Length ? properties[index].Trim() : string.Empty;
                    }
                }
            }
            return nodeConfigInfo;
        }

        private async Task<bool> WaitForEpochSync(IPEndPoint endPoint)
        {
            var endpoints = GetEndpointsWithout(endPoint);
            var configInfo = NodesMyself(endPoint, [ClusterInfoTag.NODEID, ClusterInfoTag.CONFIG_EPOCH]);
            while (true)
            {
                await Task.Delay(endpoints.Length * 100);
            retry:
                foreach (var endpoint in endpoints)
                {
                    var _configInfo = NodesAll(endpoint, [ClusterInfoTag.NODEID, ClusterInfoTag.CONFIG_EPOCH], configInfo[0]);
                    if (_configInfo[0][0] == configInfo[1])
                        ThrowException($"WaitForEpochSync unexpected node id {_configInfo[0][0]} {configInfo[1]}");

                    var epoch = long.Parse(configInfo[1]);
                    var _epoch = long.Parse(_configInfo[0][1]);
                    if (_epoch < epoch)
                        goto retry;
                }
                break;
            }
            return true;
        }

        public string ClusterMyId(int sourceNodeIndex, ILogger logger = null)
            => ClusterMyId((IPEndPoint)endpoints[sourceNodeIndex]);

        public string ClusterMyId(IPEndPoint source, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(source);
                var resp = server.Execute("cluster", "myid");
                return (string)resp;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occured");
                return null;
            }
        }

        private static List<(int, int)>[] GetSlotRanges(int primary_count)
        {
            var slotRanges = new List<(int, int)>[primary_count];
            int slotCount = 16384;

            for (int i = 0; i < primary_count; i++)
                slotRanges[i] = new List<(int, int)>();

            int idx = 0;
            for (int i = 0; i < primary_count; i++)
            {
                int startSlot = i * (slotCount - 1) / primary_count + (i == 0 ? 0 : 1);
                int endSlot = (i + 1) * (slotCount - 1) / primary_count;
                slotRanges[idx++].Add((startSlot, endSlot));
            }

            return slotRanges;
        }

        private ClientClusterConfig GetClusterConfig(int primary_count, int node_count, List<(int, int)>[] slotRanges, ILogger logger)
        {
            ClientClusterConfig clusterConfig = new(node_count);
            var endpoints = GetEndpoints();
            int j = 0;
            for (int i = 0; i < node_count; i++)
            {
                var nodeid = NodesMyself(endpoints[i], ClusterInfoTag.NODEID, logger: logger);
                var hostname = NodesMyself(endpoints[i], ClusterInfoTag.ADDRESS, logger: logger).Split(',')[1];
                bool isPrimary = i < primary_count;
                var primaryId = isPrimary ? string.Empty : NodesMyself(endpoints[j], ClusterInfoTag.NODEID, logger: logger);
                j = isPrimary ? 0 : (j + 1) % primary_count;

                clusterConfig.AddWorker(nodeid,
                    endpoints[i].Address.ToString(),
                    endpoints[i].Port,
                    i + 1 + (!isPrimary ? 1 : 0),
                    isPrimary ? Garnet.cluster.NodeRole.PRIMARY : Garnet.cluster.NodeRole.REPLICA,
                    primaryId,
                    hostname,
                    isPrimary && i < slotRanges.Length ? slotRanges[i] : null);
            }
            return clusterConfig;
        }

        private static bool NodesEqual(ClientClusterNode ccNode, ClusterNode cNode)
        {
            if (!ccNode.NodeId.Equals(cNode.NodeId)) return false;
            if (ccNode.IsReplica != cNode.IsReplica) return false;

            var a = ccNode.Slots.ToArray();
            var b = cNode.Slots.ToArray();
            if (a.Length != b.Length) return false;

            for (int i = 0; i < a.Length; i++)
            {
                if (a[i].From != b[i].From)
                    return false;

                if (a[i].To != b[i].To)
                    return false;
            }
            return true;
        }

        private void WaitForSync(ClientClusterConfig clusterConfig)
        {
            var expectedConfig = clusterConfig.GetConfigInfo();
            while (true)
            {
            retry:
                var servers = redis.GetServers();
                foreach (var server in servers)
                {
                    int count = expectedConfig.Count;
                    var nodes = server.ClusterNodes()?.Nodes;
                    if (nodes != null)
                    {
                        foreach (var node in nodes)
                        {
                            if (expectedConfig.TryGetValue(node.NodeId, out var raw))
                            {
                                ClientClusterNode ccNode = new ClientClusterNode(raw.Trim());
                                if (NodesEqual(ccNode, node)) count--;
                            }
                        }
                    }

                    if (count > 0)
                    {
                        BackOff(cancellationToken: context.cts.Token);
                        goto retry;
                    }
                }
                break;
            }
        }

        public (List<ShardInfo>, List<ushort>) SimpleSetupCluster(
            int primary_count = -1,
            int replica_count = -1,
            bool assignSlots = true,
            List<(int, int)>[] customSlotRanges = null,
            ILogger logger = null)
        {
            var endpoints = GetEndpoints();
            var node_count = endpoints.Length;
            primary_count = primary_count < 0 ? endpoints.Length : primary_count;
            replica_count = replica_count < 0 ? 0 : replica_count;
            ClassicAssert.AreEqual(node_count, primary_count + primary_count * replica_count, $"Error primary per replica misconfig mCount: {primary_count}, rCount:{replica_count}");

            var slotRanges = customSlotRanges == null ? GetSlotRanges(primary_count) : customSlotRanges;
            var clusterConfig = GetClusterConfig(primary_count, node_count, slotRanges, logger);

            var shards = new List<ShardInfo>();
            var slots = new List<ushort>();

            // Assign slots to primaries
            for (int i = 0; i < slotRanges.Length; i++)
            {
                foreach (var slotRange in slotRanges[i])
                {
                    var endpoint = endpoints[i];
                    AddSlotsRange(endpoint, new List<(int, int)> { slotRange }, logger);
                    slots.AddRange(Enumerable.Range(slotRange.Item1, slotRange.Item2 - slotRange.Item1 + 1).Select(x => (ushort)x));
                    ShardInfo shardInfo = new()
                    {
                        slotRanges = new List<(int, int)>() { (slotRange.Item1, slotRange.Item2) },
                        nodes = new()
                            {
                                new NodeInfo()
                                {
                                    nodeIndex = i,
                                    nodeid = GetNodeIdFromNode(i, logger),
                                    address = endpoint.Address.ToString(),
                                    port = endpoint.Port,
                                    role = NodeRole.PRIMARY,
                                    replicationOffset = 0
                                }
                            }
                    };
                    shards.Add(shardInfo);
                }
            }

            //Set-config-epoch
            for (int i = 0; i < endpoints.Length; i++)
                SetConfigEpoch(endpoints[i], i + 1, logger);

            //Initiate meet
            var _firstEndpoint = endpoints[0];
            for (int i = 1; i < endpoints.Length; i++)
                Meet(_firstEndpoint, endpoints[i], logger);

            //WaitForClusterJoin(clusterConfig);
            //WaitForSync(clusterConfig);

            //Assign replicas
            if (replica_count > 0)
            {
                int j = 0;
                for (int i = primary_count; i < endpoints.Length; i++)
                {
                    var primaryId = GetLocalNodeId(j);
                    var replicaId = GetLocalNodeId(i);

                    // Wait until replica knows primary
                    WaitUntilNodeIdIsKnown(i, primaryId, logger);

                    // Wait until primary knows this replica in order for
                    // TryConnectToReplica to succeed for AOF sync
                    WaitUntilNodeIdIsKnown(j, replicaId, logger);

                    var primaryEndpoint = endpoints[i];
                    string resp = (string)ClusterReplicate(primaryEndpoint, primaryId, logger: logger);
                    {
                        var msg = "";
                        for (int k = 0; k < endpoints.Length; k++)
                        {
                            var ns = ClusterNodes(k, logger).Nodes;

                            var rawStr = $"[{k}]\n";
                            foreach (var n in ns)
                            {
                                rawStr += "\t" + n.Raw + "\n";
                            }
                            rawStr += $"[{k}]\n";
                            //logger?.LogError(rawStr);
                            msg += rawStr;
                        }
                        ClassicAssert.AreEqual("OK", resp, msg);
                    }

                    shards[j].nodes.Add(
                        new NodeInfo()
                        {
                            nodeIndex = i,
                            nodeid = GetNodeIdFromNode(i, logger),
                            address = GetAddressFromNodeIndex(i),
                            port = GetPortFromNodeIndex(i),
                            role = NodeRole.REPLICA,
                            replicationOffset = 0
                        }
                    );
                    j = (j + 1) % primary_count;
                }

                // WaitForReplicas to connect
                j = 0;
                for (int i = 0; i < primary_count; i++)
                {
                    WaitForConnectedReplicaCount(i, replica_count, logger);
                }

                //WaitForClusterJoin(clusterConfig, true);
            }
            WaitForSync(clusterConfig);
            return (shards, slots);
        }

        public ITransaction CreateClusterTransaction()
        {
            var db = redis.GetDatabase();
            return db.CreateTransaction();
        }

        public struct ClusterResponse
        {
            public string address;
            public int port;
            public int slot;
            public ResponseState state;
            public RedisResult result;

            public ClusterResponse(string address, int port, int slot, ResponseState state, RedisResult result = null)
            {
                this.address = address;
                this.port = port;
                this.slot = slot;
                this.state = state;
                this.result = result;
            }
        }

        public ClusterResponse ExecuteTxnForShard(int nodeIndex, List<(string, ICollection<object>)> commands, ILogger logger = null)
        {
            RedisResult result = default;
            try
            {
                var server = GetServer(nodeIndex);

                var txnblockResp = (string)server.Execute("MULTI", new List<object>(), CommandFlags.NoRedirect);
                ClassicAssert.AreEqual(txnblockResp, "OK");
                foreach (var cmd in commands)
                {
                    var respCmd = (string)server.Execute(cmd.Item1, cmd.Item2, CommandFlags.NoRedirect);
                    ClassicAssert.AreEqual(respCmd, "QUEUED");
                }

                result = server.Execute("EXEC", new List<object>(), CommandFlags.NoRedirect);
            }
            catch (Exception ex)
            {
                var tokens = ex.Message.Split(' ');
                if (tokens.Length > 10 && tokens[2].Equals("MOVED"))
                {
                    var address = tokens[5].Split(':')[0];
                    var port = int.Parse(tokens[5].Split(':')[1]);
                    var slot = int.Parse(tokens[8]);
                    var responseState = ResponseState.MOVED;

                    return new ClusterResponse(address, port, slot, responseState, RedisResult.Create(ex.Message, ResultType.Error));
                }
                else if (tokens.Length > 10 && tokens[0].Equals("Endpoint"))
                {
                    var address = tokens[1].Split(':')[0];
                    var port = int.Parse(tokens[1].Split(':')[1]);
                    var slot = int.Parse(tokens[4]);
                    var responseState = ResponseState.ASK;
                    return new ClusterResponse(address, port, slot, responseState, RedisResult.Create(ex.Message, ResultType.Error));
                }
                else if (ex.Message.StartsWith("CLUSTERDOWN"))
                {
                    var responseState = ResponseState.CLUSTERDOWN;
                    return new ClusterResponse("", -1, -1, responseState, RedisResult.Create(ex.Message, ResultType.Error));
                }
                logger?.LogError(ex, "Unexpected exception");
                return new ClusterResponse("", -1, -1, ResponseState.NONE, RedisResult.Create(ex.Message, ResultType.Error));
            }
            return new ClusterResponse("", -1, -1, ResponseState.OK, result);
        }
    }

    public unsafe partial class ClusterTestUtils
    {
        static readonly TimeSpan backoff = TimeSpan.FromSeconds(1);
        static readonly byte[] bresp_OK = Encoding.ASCII.GetBytes("+OK\r\n");
        static readonly byte[] ascii_chars = Encoding.ASCII.GetBytes("abcdefghijklmnopqrstvuwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
        public Random r;
        ConnectionMultiplexer redis = null;
        GarnetClientSession[] gcsConnections = null;
        readonly EndPointCollection endpoints;
        string[] nodeIds;

        TextWriter textWriter;

        readonly bool allowAdmin;
        readonly bool disablePubSub;
        readonly bool useTLS;
        readonly string authUsername;
        readonly string authPassword;
        readonly X509CertificateCollection certificates;
        readonly ClusterTestContext context;

        public ClusterTestUtils(
            EndPointCollection endpoints,
            ClusterTestContext context = null,
            TextWriter textWriter = null,
            bool UseTLS = false,
            string authUsername = null,
            string authPassword = null,
            X509CertificateCollection certificates = null)
        {
            r = new Random(674386);
            this.context = context;
            this.useTLS = UseTLS;
            this.allowAdmin = true;
            this.disablePubSub = true;
            this.authUsername = authUsername;
            this.authPassword = authPassword;
            this.textWriter = textWriter;
            this.endpoints = endpoints;
            this.certificates = certificates;
        }

        public int HashSlot(RedisKey key)
        => redis.HashSlot(key);

        public static void BackOff(TimeSpan timeSpan = default) => Thread.Sleep(timeSpan == default ? backoff : timeSpan);

        public static void BackOff(CancellationToken cancellationToken, TimeSpan timeSpan = default, string msg = null)
        {
            if (cancellationToken.IsCancellationRequested)
                ClassicAssert.Fail(msg ?? "Cancellation Requested");
            Thread.Sleep(timeSpan == default ? backoff : timeSpan);
        }

        public void Connect(bool cluster = true, ILogger logger = null)
        {
            InitMultiplexer(GetRedisConfig(endpoints), textWriter, logger: logger);
            if (cluster)
                this.nodeIds = GetNodeIds(logger: logger);
        }

        private void InitMultiplexer(ConfigurationOptions redisConfig, TextWriter textWriter, bool failAssert = true, ILogger logger = null)
        {
            try
            {
                redis = ConnectionMultiplexer.Connect(redisConfig, null);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error occurred at InitMultiplexer");
                if (failAssert)
                    Assert.Fail(ex.Message);
            }
        }

        public ConfigurationOptions GetRedisConfig(EndPointCollection endpoints)
        {
            return TestUtils.GetConfig(
                endpoints,
                allowAdmin: allowAdmin,
                useTLS: useTLS,
                disablePubSub: disablePubSub,
                authUsername: authUsername,
                authPassword: authPassword,
                certificates: certificates);
        }

        public void Dispose()
        {
            CloseConnections();
        }

        public void CloseConnections()
        {
            redis?.Close(false);
            redis?.Dispose();

            if (gcsConnections != null)
            {
                foreach (var gcs in gcsConnections)
                    gcs?.Dispose();
            }
        }

        public string[] GetNodeIds(List<int> nodes = null, ILogger logger = null)
        {
            string[] nodeIds = new string[endpoints.Count];
            if (nodes == null)
            {
                for (int i = 0; i < nodeIds.Length; i++)
                    nodeIds[i] = NodesMyself((IPEndPoint)endpoints[i], ClusterInfoTag.NODEID, logger: logger);
            }
            else
            {
                for (int i = 0; i < nodes.Count; i++)
                {
                    var j = nodes[i];
                    nodeIds[j] = NodesMyself((IPEndPoint)endpoints[j], ClusterInfoTag.NODEID, logger: logger);
                }
            }
            return nodeIds;
        }

        public void Reconnect(List<int> nodes = null, TextWriter textWriter = null, ILogger logger = null)
        {
            CloseConnections();
            EndPointCollection endPoints = endpoints;
            if (nodes != null)
            {
                endPoints = new EndPointCollection();
                foreach (var nodeIndex in nodes)
                {
                    var endpoint = (IPEndPoint)endpoints[nodeIndex];
                    endPoints.Add(endpoint.Address, endpoint.Port);
                }
            }
            var connOpts = GetRedisConfig(endPoints);
            InitMultiplexer(connOpts, textWriter, logger: logger);
            nodeIds = GetNodeIds(nodes, logger);
        }

        public EndPointCollection GetEndPoints() => endpoints;

        public ConnectionMultiplexer GetMultiplexer() => redis;

        public IDatabase GetDatabase() => redis.GetDatabase(0);

        public GarnetClientSession GetGarnetClientSession(int nodeIndex, bool useTLS = false)
        {
            gcsConnections ??= new GarnetClientSession[endpoints.Count];

            if (gcsConnections[nodeIndex] == null)
            {
                SslClientAuthenticationOptions sslOptions = null;
                if (useTLS)
                {
                    sslOptions = new SslClientAuthenticationOptions
                    {
                        ClientCertificates = [CertificateUtils.GetMachineCertificateByFile(certFile, certPassword)],
                        TargetHost = "GarnetTest",
                        AllowRenegotiation = false,
                        RemoteCertificateValidationCallback = TestUtils.ValidateServerCertificate,
                    };
                }
                gcsConnections[nodeIndex] = new GarnetClientSession(GetEndPoint(nodeIndex), new(), tlsOptions: sslOptions);
                gcsConnections[nodeIndex].Connect();
            }
            return gcsConnections[nodeIndex];
        }

        public const string certFile = "testcert.pfx";
        public const string certPassword = "placeholder";

        public GarnetClientSession CreateGarnetClientSession(int nodeIndex, bool useTLS = false)
        {
            SslClientAuthenticationOptions sslOptions = null;
            if (useTLS)
            {
                sslOptions = new SslClientAuthenticationOptions
                {
                    ClientCertificates = [CertificateUtils.GetMachineCertificateByFile(certFile, certPassword)],
                    TargetHost = "GarnetTest",
                    AllowRenegotiation = false,
                    RemoteCertificateValidationCallback = TestUtils.ValidateServerCertificate,
                };
            }

            return new(endpoints[nodeIndex], new(), tlsOptions: sslOptions);
        }

        public IServer GetServer(int nodeIndex) => redis.GetServer(GetEndPoint(nodeIndex));

        public IServer GetServer(IPEndPoint endPoint) => redis.GetServer(endPoint);

        public string GetAddressFromNodeIndex(int nodeIndex) => ((IPEndPoint)endpoints[nodeIndex]).Address.ToString();

        public int GetPortFromNodeIndex(int nodeIndex) => ((IPEndPoint)endpoints[nodeIndex]).Port;

        public int GetNodeIndexFromPort(int port)
        {
            for (int i = 0; i < endpoints.Count; i++)
                if (GetPortFromNodeIndex(i) == port)
                    return i;

            return -1;
        }

        public IPEndPoint GetEndPoint(int nodeIndex) => (IPEndPoint)endpoints[nodeIndex];

        public IPEndPoint GetEndPointFromPort(int Port) => endpoints.Select(x => (IPEndPoint)x).First(x => x.Port == Port);

        public void RandomBytesRestrictedToSlot(ref byte[] data, int slot, int startOffset = -1, int endOffset = -1)
        {
            RandomBytes(ref data, startOffset, endOffset);
            while (HashSlot(data) != slot) RandomBytes(ref data, startOffset, endOffset);
        }

        public void RandomBytesRestrictedToSlot(ref Random r, ref byte[] data, int slot, int startOffset = -1, int endOffset = -1)
        {
            RandomBytes(ref data, startOffset, endOffset);
            while (HashSlot(data) != slot) RandomBytes(ref r, ref data, startOffset, endOffset);
        }

        public void InitRandom(int seed)
        {
            r = new Random(seed);
        }

        public void RandomBytes(ref byte[] data, int startOffset = -1, int endOffset = -1)
            => RandomBytes(ref r, ref data, startOffset, endOffset);

        public static void RandomBytes(ref Random r, ref byte[] data, int startOffset = -1, int endOffset = -1)
        {
            startOffset = startOffset == -1 ? 0 : startOffset;
            endOffset = endOffset == -1 ? data.Length : endOffset;
            for (var i = startOffset; i < endOffset; i++)
                data[i] = ascii_chars[r.Next(ascii_chars.Length)];
        }

        public byte[] RandomBytes(byte[] data, int startOffset = -1, int endOffset = -1)
        {
            byte[] newData = new byte[data.Length];
            Array.Copy(data, 0, newData, 0, data.Length);
            RandomBytes(ref newData, startOffset, endOffset);
            return newData;
        }

        public List<int> RandomList(int count, int maxVal)
        {
            List<int> list = new List<int>();
            var size = r.Next(1, count);
            for (int i = 0; i < size; i++)
            {
                list.Add(r.Next(0, maxVal));
            }
            return list;
        }

        public List<int> RandomHset(int count, int maxVal)
        {
            HashSet<int> hset = new HashSet<int>();
            var size = r.Next(1, count);
            for (int i = 0; i < size; i++)
            {
                hset.Add(r.Next(0, maxVal));
            }
            return [.. hset];
        }

        public string RandomStr(int length, int startOffset = -1, int endOffset = -1)
        {
            byte[] data = new byte[length];
            RandomBytes(ref data, startOffset, endOffset);
            return Encoding.ASCII.GetString(data);
        }

        public static ushort HashSlot(byte[] key)
        {
            fixed (byte* ptr = key)
            {
                byte* keyPtr = ptr;
                return HashSlotUtils.HashSlot(keyPtr, key.Length);
            }
        }

        public int GetRandomTargetNodeIndex(ref LightClientRequest[] connections, int sourceNodeIndex)
        {
            int targetNodeIndex = r.Next(0, connections.Length);
            while (targetNodeIndex == sourceNodeIndex) targetNodeIndex = r.Next(0, connections.Length);
            return targetNodeIndex;
        }

        public static (int, int) LightReceive(byte* buf, int bytesRead, int opType)
        {
            string result = null;
            byte* ptr = buf;
            string[] resultArray = null;
            int count = 0;

            switch (*buf)
            {
                case (byte)'+':
                    if (!RespReadResponseUtils.TryReadSimpleString(out result, ref ptr, buf + bytesRead))
                        return (0, 0);
                    count++;
                    break;
                case (byte)':':
                    if (!RespReadResponseUtils.TryReadIntegerAsString(out result, ref ptr, buf + bytesRead))
                        return (0, 0);
                    count++;
                    break;
                case (byte)'-':
                    if (!RespReadResponseUtils.TryReadErrorAsString(out result, ref ptr, buf + bytesRead))
                        return (0, 0);
                    count++;
                    break;
                case (byte)'$':
                    if (!RespReadResponseUtils.TryReadStringWithLengthHeader(out result, ref ptr, buf + bytesRead))
                        return (0, 0);
                    count++;
                    break;
                case (byte)'*':
                    if (!RespReadResponseUtils.TryReadStringArrayWithLengthHeader(out resultArray, ref ptr, buf + bytesRead))
                        return (0, 0);
                    count++;
                    break;
                default:
                    throw new Exception("Unexpected response: " + Encoding.ASCII.GetString(new Span<byte>(buf, bytesRead)).Replace("\n", "|").Replace("\r", "") + "]");
            }
            return (bytesRead, count);
        }

        public static string ParseRespToString(byte[] data, out string[] resultArray)
        {
            resultArray = null;
            string result = null;
            fixed (byte* buf = data)
            {
                byte* ptr = buf;
                if (buf[0] == '$' && buf[1] == '-' && buf[2] == '1' && buf[3] == '\r' && buf[4] == '\n')
                    return "(empty)";

                switch (*buf)
                {
                    case (byte)'+':
                        RespReadResponseUtils.TryReadSimpleString(out result, ref ptr, buf + data.Length);
                        break;
                    case (byte)':':
                        RespReadResponseUtils.TryReadIntegerAsString(out result, ref ptr, buf + data.Length);
                        break;
                    case (byte)'-':
                        RespReadResponseUtils.TryReadErrorAsString(out result, ref ptr, buf + data.Length);
                        break;
                    case (byte)'$':
                        RespReadResponseUtils.TryReadStringWithLengthHeader(out result, ref ptr, buf + data.Length);
                        break;
                    case (byte)'*':
                        RespReadResponseUtils.TryReadStringArrayWithLengthHeader(out resultArray, ref ptr, buf + data.Length);
                        break;
                    default:
                        throw new Exception("Unexpected response: " + Encoding.ASCII.GetString(new Span<byte>(buf, data.Length)).Replace("\n", "|").Replace("\r", "") + "]");
                }
            }

            return result;
        }

        public static ReadOnlySpan<byte> MOVED => "-MOVED"u8;
        public static ReadOnlySpan<byte> ASK => "-ASK"u8;
        public static ReadOnlySpan<byte> MIGRATING => "-MIGRATING"u8;
        public static ReadOnlySpan<byte> CROSSSLOT => "-CROSSSLOT"u8;
        public static ReadOnlySpan<byte> CLUSTERDOWN => "-CLUSTERDOWN"u8;

        public static ResponseState ParseResponseState(
            byte[] result,
            out int slot,
            out IPEndPoint endpoint,
            out string returnValue,
            out string[] returnValueArray)
        {
            returnValue = null;
            returnValueArray = null;
            slot = default;
            endpoint = null;

            if (result[0] == (byte)'+' || result[0] == (byte)':' || result[0] == '*' || result[0] == '$')
            {
                returnValue = ParseRespToString(result, out returnValueArray);
                return ResponseState.OK;
            }
            else if (result.AsSpan().StartsWith(MOVED))
            {
                GetEndPointFromResponse(result, out slot, out endpoint);
                return ResponseState.MOVED;
            }
            else if (result.AsSpan().StartsWith(ASK))
            {
                GetEndPointFromResponse(result, out slot, out endpoint);
                return ResponseState.ASK;
            }
            else if (result.AsSpan().StartsWith(MIGRATING))
            {
                return ResponseState.MIGRATING;
            }
            else if (result.AsSpan().StartsWith(CROSSSLOT))
            {
                return ResponseState.CROSSSLOT;
            }
            else if (result.AsSpan().StartsWith(CLUSTERDOWN))
            {
                return ResponseState.CLUSTERDOWN;
            }
            else
                ClassicAssert.IsFalse(true);
            return ResponseState.NONE;
        }

        public static ResponseState ParseResponseState(
            ref LightClientRequest node,
            byte[] key,
            byte[] result,
            out int slot,
            out IPEndPoint endpoint,
            out byte[] value,
            out string[] values)
        {
            value = null;
            values = null;
            slot = -1;
            endpoint = null;
            if (result[0] == (byte)'+' || result[0] == (byte)':' || result[0] == '*' || result[0] == '$')
            {
                endpoint = node.EndPoint as IPEndPoint;
                slot = HashSlot(key);
                var strValue = ParseRespToString(result, out values);
                if (strValue != null)
                    value = Encoding.ASCII.GetBytes(strValue);
                return ResponseState.OK;
            }
            else if (result.AsSpan()[..MOVED.Length].SequenceEqual(MOVED))
            {
                GetEndPointFromResponse(result, out slot, out endpoint);
                return ResponseState.MOVED;
            }
            else if (result.AsSpan()[..ASK.Length].SequenceEqual(ASK))
            {
                GetEndPointFromResponse(result, out slot, out endpoint);
                return ResponseState.ASK;
            }
            else if (result.AsSpan()[..MIGRATING.Length].SequenceEqual(MIGRATING))
            {
                return ResponseState.MIGRATING;
            }
            else if (result.AsSpan()[..CROSSSLOT.Length].SequenceEqual(CROSSSLOT))
            {
                return ResponseState.CROSSSLOT;
            }
            else if (result.AsSpan()[..CLUSTERDOWN.Length].SequenceEqual(CLUSTERDOWN))
            {
                return ResponseState.CLUSTERDOWN;
            }
            else
                ClassicAssert.IsFalse(true, Encoding.ASCII.GetString(result.AsSpan()[..32]));
            return ResponseState.NONE;
        }

        public static LightClientRequest[] CreateLightRequestConnections(int[] Ports)
        {
            LightClientRequest[] lightClientRequests = new LightClientRequest[Ports.Length];
            for (int i = 0; i < Ports.Length; i++)
            {
                lightClientRequests[i] = new LightClientRequest(new IPEndPoint(IPAddress.Loopback, Ports[i]), 0, LightReceive);
            }
            return lightClientRequests;
        }

        public Dictionary<ushort, int> GetSlotPortMapFromNode(int nodeIndex, ILogger logger)
        {
            var slots = GetOwnedSlotsFromNode(nodeIndex, logger);
            int Port = ((IPEndPoint)endpoints[nodeIndex]).Port;
            return slots.Select(slot => new KeyValuePair<ushort, int>((ushort)slot, Port)).ToDictionary(x => x.Key, x => x.Value);
        }

        public Dictionary<ushort, int> GetSlotPortMapFromServer(int Port, ILogger logger)
        {
            var endPoint = GetEndPointFromPort(Port);
            var slots = GetOwnedSlotsFromNode(endPoint, logger);
            return slots.Select(slot => new KeyValuePair<ushort, int>((ushort)slot, Port)).ToDictionary(x => x.Key, x => x.Value);
        }

        public static Dictionary<ushort, int> MergeSlotPortMap(Dictionary<ushort, int> a, Dictionary<ushort, int> b)
        {
            foreach (var pair in b)
            {
                ClassicAssert.IsTrue(!a.ContainsKey(pair.Key));
                a.Add(pair.Key, pair.Value);
            }
            return a;
        }

        public string AddSlotsRange(int nodeIndex, List<(int, int)> ranges, ILogger logger)
            => (string)AddSlotsRange((IPEndPoint)endpoints[nodeIndex], ranges, logger);

        public RedisResult AddSlotsRange(IPEndPoint endPoint, List<(int, int)> ranges, ILogger logger)
        {
            ICollection<object> args = new List<object>() { "addslotsrange" };
            foreach (var range in ranges)
            {
                args.Add(range.Item1);
                args.Add(range.Item2);
            }
            return Execute(endPoint, "cluster", args, logger: logger);
        }

        public void SetConfigEpoch(int sourceNodeIndex, long epoch, ILogger logger = null)
            => SetConfigEpoch((IPEndPoint)endpoints[sourceNodeIndex], epoch, logger);

        public void SetConfigEpoch(IPEndPoint endPoint, long epoch, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(endPoint);
                var resp = server.Execute("cluster", "set-config-epoch", $"{epoch}");
                ClassicAssert.AreEqual((string)resp, "OK");
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occured");
                Assert.Fail(ex.Message);
            }
        }

        public void BumpEpoch(int nodeIndex, bool waitForSync = false, ILogger logger = null)
            => BumpEpoch((IPEndPoint)endpoints[nodeIndex], waitForSync: waitForSync, logger);

        public void BumpEpoch(IPEndPoint endPoint, bool waitForSync = false, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(endPoint);
                var resp = server.Execute("cluster", "bumpepoch");
                ClassicAssert.AreEqual((string)resp, "OK");
                if (waitForSync)
                    WaitForEpochSync(endPoint).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occured");
                Assert.Fail(ex.Message);
            }
        }

        public void WaitForConfigPropagation(int fromNode, List<int> nodes = null, ILogger logger = null)
        {
            if (nodes == null)
                nodes = [.. Enumerable.Range(0, endpoints.Count)];
            var fromNodeConfig = ClusterNodes(fromNode, logger: logger);
            while (true)
            {
            retry:
                foreach (var nodeIndex in nodes)
                {
                    if (nodeIndex == fromNode)
                        continue;

                    var nodeConfig = ClusterNodes(nodeIndex, logger: logger);
                    if (!MatchConfig(fromNodeConfig, nodeConfig))
                    {
                        BackOff(cancellationToken: context.cts.Token);
                        goto retry;
                    }
                }
                break;
            }
        }

        public static bool MatchConfig(ClusterConfiguration configA, ClusterConfiguration configB)
        {
            foreach (var nodeA in configA.Nodes)
            {
                bool found = false;
                foreach (var nodeB in configB.Nodes)
                {

                    if (!nodeA.NodeId.Equals(nodeB.NodeId))
                        continue;

                    found = true;

                    //Check if node info is same
                    if (nodeA.IsReplica != nodeB.IsReplica)
                        return false;

                    if (nodeA.Parent != nodeB.Parent)
                        return false;

                    if (!nodeA.EndPoint.Equals(nodeB.EndPoint))
                        return false;

                    //Check if slot info is same
                    var slotsA = nodeA.Slots.ToArray();
                    var slotsB = nodeB.Slots.ToArray();

                    if (slotsA.Length != slotsB.Length)
                        return false;

                    for (int i = 0; i < slotsA.Length; i++)
                    {
                        if (slotsA[i].From != slotsB[i].From)
                            return false;

                        if (slotsA[i].To != slotsB[i].To)
                            return false;
                    }
                }

                if (!found) return false;
            }

            return true;
        }

        public void Authenticate(int sourceNodeIndex, string username, string password, ILogger logger = null)
            => Authenticate((IPEndPoint)endpoints[sourceNodeIndex], username, password, logger);

        public void Authenticate(IPEndPoint source, string username, string password, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(source);
                server.Execute("auth", username, password);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred");
                Assert.Fail(ex.Message);
            }
        }

        public void Meet(int sourceNodeIndex, int meetNodeIndex, ILogger logger = null)
            => Meet((IPEndPoint)endpoints[sourceNodeIndex], (IPEndPoint)endpoints[meetNodeIndex], logger);

        public void Meet(IPEndPoint source, IPEndPoint target, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(source);
                var resp = server.Execute("cluster", "meet", $"{target.Address}", $"{target.Port}");
                ClassicAssert.AreEqual((string)resp, "OK");
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred");
                Assert.Fail(ex.Message);
            }
        }

        public void Meet(int sourceNodeIndex, int meetNodeIndex, string hostname, ILogger logger = null)
            => Meet((IPEndPoint)endpoints[sourceNodeIndex], (IPEndPoint)endpoints[meetNodeIndex], hostname, logger);

        public void Meet(IPEndPoint source, IPEndPoint target, string hostname, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(source);
                var resp = server.Execute("cluster", "meet", $"{hostname}", $"{target.Port}");
                ClassicAssert.AreEqual((string)resp, "OK");
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred");
                Assert.Fail(ex.Message);
            }
        }

        public string[] BanList(int sourceNodeIndex, ILogger logger = null)
            => BanList((IPEndPoint)endpoints[sourceNodeIndex], logger);

        public string[] BanList(IPEndPoint source, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(source);
                var resp = server.Execute("cluster", "banlist");
                return (string[])resp;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred");
                Assert.Fail(ex.Message);
                return null;
            }
        }

        public static List<string> Nodes(ref LightClientRequest lightClientRequest)
        {
            var result = lightClientRequest.SendCommand($"cluster nodes");
            var strResult = Encoding.ASCII.GetString(result);
            var data = strResult.Split('\n');
            List<string> nodeConfig = new();
            for (int i = 1; ; i++)
            {
                if (data[i].Equals("\r"))
                    break;
                nodeConfig.Add(data[i]);
            }
            return nodeConfig;
        }

        public List<string> Nodes(int nodeIndex, ILogger logger)
        {
            return Nodes((IPEndPoint)endpoints[nodeIndex], logger: logger);
        }

        public List<string> Nodes(IPEndPoint endPoint, ILogger logger)
        {
            try
            {
                var server = redis.GetServer(endPoint);
                var strResult = (string)server.Execute("cluster", "nodes");
                var data = strResult.Split('\n');
                List<string> nodeConfig = new();

                for (int i = 0; ; i++)
                {
                    if (data[i] == "")
                        break;
                    nodeConfig.Add(data[i]);
                }
                return nodeConfig;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occured");
                Assert.Fail(ex.Message);
                return null;
            }
        }

        public Dictionary<string, Dictionary<ClusterInfoTag, string>> NodesDict(int nodeIndex, ILogger logger)
        {
            return NodesDict((IPEndPoint)endpoints[nodeIndex], logger);
        }

        public Dictionary<string, Dictionary<ClusterInfoTag, string>> NodesDict(IPEndPoint endPoint, ILogger logger)
        {
            try
            {
                var server = redis.GetServer(endPoint);
                var strResult = (string)server.Execute("cluster", "nodes");
                var data = strResult.Split('\n');
                Dictionary<string, Dictionary<ClusterInfoTag, string>> nodeConfig = new();

                for (int i = 0; i < data.Length; i++)
                {
                    if (data[i] == "") continue;

                    var properties = data[i].Split(' ');
                    var nodeid = properties[0].Trim();
                    nodeConfig.Add(nodeid, new());

                    nodeConfig[nodeid].Add(ClusterInfoTag.ADDRESS, properties[(int)ClusterInfoTag.ADDRESS].Trim());
                    nodeConfig[nodeid].Add(ClusterInfoTag.FLAGS, properties[(int)ClusterInfoTag.FLAGS].Trim());
                    nodeConfig[nodeid].Add(ClusterInfoTag.PRIMARY, properties[(int)ClusterInfoTag.PRIMARY].Trim());
                    nodeConfig[nodeid].Add(ClusterInfoTag.PING_SENT, properties[(int)ClusterInfoTag.PING_SENT].Trim());
                    nodeConfig[nodeid].Add(ClusterInfoTag.PONG_RECEIVED, properties[(int)ClusterInfoTag.PONG_RECEIVED].Trim());
                    nodeConfig[nodeid].Add(ClusterInfoTag.CONFIG_EPOCH, properties[(int)ClusterInfoTag.CONFIG_EPOCH].Trim());
                    nodeConfig[nodeid].Add(ClusterInfoTag.LINK_STATE, properties[(int)ClusterInfoTag.LINK_STATE].Trim());
                    if (properties.Length > (int)ClusterInfoTag.SLOT)
                        nodeConfig[nodeid].Add(ClusterInfoTag.SLOT, properties[(int)ClusterInfoTag.SLOT].Trim());
                }

                return nodeConfig;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error occurred");
                Assert.Fail(ex.Message);
                return null;
            }
        }

        public string GetConfigEpochOfNodeFromNodeIndex(int nodeIndex, string nodeidOfConfigEpoch, ILogger logger)
        {
            var dict = NodesDict(nodeIndex, logger);
            return dict[nodeidOfConfigEpoch][ClusterInfoTag.CONFIG_EPOCH];
        }

        public long GetConfigEpoch(int nodeIndex, ILogger logger)
        {
            var dict = NodesDict(nodeIndex, logger);
            return long.Parse(dict[nodeIds[nodeIndex]][ClusterInfoTag.CONFIG_EPOCH]);
        }

        public void WaitAll(ILogger logger)
        {
            foreach (var endPoint in endpoints)
            {
                while (Nodes((IPEndPoint)endPoint, logger).Count != endpoints.Count)
                {
                    BackOff(cancellationToken: context.cts.Token);
                }
            }
        }

        public bool IsKnown(int nodeIndex, int knownNodeIndex, ILogger logger = null)
        {
            var toKnowNodeId = ClusterNodes(knownNodeIndex).Nodes.First().NodeId;
            var nodeConfig = ClusterNodes(nodeIndex);

            return nodeConfig.Nodes.Any(x => x.NodeId.Equals(toKnowNodeId, StringComparison.OrdinalIgnoreCase));
        }

        public void WaitUntilNodeIsKnown(int nodeIndex, int toKnowNode, ILogger logger = null)
        {
            var toKnowNodeId = ClusterNodes(toKnowNode).Nodes.First().NodeId;
            WaitUntilNodeIdIsKnown(nodeIndex, toKnowNodeId, logger);
        }

        public void WaitUntilNodeIsKnownByAllNodes(int nodeIndex, ILogger logger = null)
        {
            var c = ClusterNodes(nodeIndex, logger);
            var nodeId = c.Nodes.First().NodeId;
            var retry = 0;
            while (true)
            {
                var configs = new List<ClusterConfiguration>();
                for (var i = 0; i < endpoints.Count; i++)
                {
                    if (i == nodeIndex) continue;
                    configs.Add(ClusterNodes(i, logger));
                }

                var count = 0;
                foreach (var config in configs)
                    foreach (var node in config.Nodes)
                        if (nodeId.Equals(node.NodeId)) count++;

                if (count == endpoints.Count - 1) break;
                if (retry++ > 1_000_000) Assert.Fail("retry config sync at WaitUntilNodeIsKnownByAllNodes reached");
                BackOff(cancellationToken: context.cts.Token);
            }
        }

        public void WaitUntilNodeIdIsKnown(int nodeIndex, string nodeId, ILogger logger = null)
        {
            while (true)
            {
                var nodes = ClusterNodes(nodeIndex, logger).Nodes;

                var found = false;
                foreach (var node in nodes)
                    if (node.NodeId.Equals(nodeId))
                        found = true;
                if (found) break;
                BackOff(cancellationToken: context.cts.Token);
            }
        }

        public int CountKeysInSlot(int slot, ILogger logger = null)
        {
            var db = redis.GetDatabase(0);
            RedisResult result;
            try
            {
                result = db.Execute("cluster", "countkeysinslot", slot.ToString());
            }
            catch (Exception ex)
            {
                logger?.LogInformation(ex, "Exception occurred in CountKeysInSlot");
                if (slot < 0 || slot > ushort.MaxValue - 1)
                {
                    ClassicAssert.AreEqual("ERR Slot out of range", ex.Message);
                    return 0;
                }
                return 0;
            }
            return ResultType.Integer == result.Resp2Type ? int.Parse(result.ToString()) : 0;
        }

        public int CountKeysInSlot(int nodeIndex, int slot, ILogger logger = null)
        {
            var server = redis.GetServer((IPEndPoint)endpoints[nodeIndex]);
            RedisResult resp;
            try
            {
                resp = server.Execute("cluster", "countkeysinslot", $"{slot}");
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "Exception occurred in CountKeysInSlot");
                return -1;
            }
            return (int)resp;
        }

        public RedisResult[] GetKeysInSlot(int slot, int count, ILogger logger = null)
        {
            var db = redis.GetDatabase(0);
            RedisResult result = null;
            try
            {
                result = db.Execute("cluster", "getkeysinslot", slot.ToString(), count.ToString());
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "Exception occurred in GetKeysInSlot");
                if (slot < 0 || slot > ushort.MaxValue - 1)
                {
                    ClassicAssert.AreEqual(ex.Message, "ERR Slot out of range");
                    return null;
                }
            }

            return (RedisResult[])result;
        }

        public List<byte[]> GetKeysInSlot(int nodeIndex, int slot, int keyCount, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer((IPEndPoint)endpoints[nodeIndex]);
                var resp = server.Execute("cluster", "getkeysinslot", $"{slot}", $"{keyCount}");

                return [.. ((RedisResult[])resp).Select(x => Encoding.ASCII.GetBytes((string)x))];
            }
            catch (Exception ex)
            {
                logger?.LogError("GetKeysInsSlot {msg}", ex.Message);
                Assert.Fail(ex.Message);
                return null;
            }
        }

        public List<int> GetOwnedSlotsFromNode(int nodeIndex, ILogger logger)
        {
            var endPoint = (IPEndPoint)endpoints[nodeIndex];
            return GetOwnedSlotsFromNode(endPoint, logger);
        }

        public List<int> GetOwnedSlotsFromNode(IPEndPoint endPoint, ILogger logger)
        {
            var nodeConfig = Nodes(endPoint, logger);
            var nodeInfo = nodeConfig[0].Split(' ');

            var slots = new List<int>();
            if (nodeInfo.Length >= (int)ClusterInfoTag.SLOT)
            {
                for (int i = (int)ClusterInfoTag.SLOT; i < nodeInfo.Length; i++)
                {
                    var range = nodeInfo[i].Split('-');
                    ushort slotStart = ushort.Parse(range[0]);
                    ushort slotEnd;
                    if (range.Length > 1)
                    {
                        slotEnd = ushort.Parse(range[1]);
                        slots.AddRange(Enumerable.Range(slotStart, slotEnd - slotStart + 1));
                    }
                    else
                        slots.Add(slotStart);
                }
            }
            return slots;
        }

        public static string GetNodeIdFromNode(ref LightClientRequest sourceNode)
        {
            var nodeConfig = Nodes(ref sourceNode);
            var nodeInfo = nodeConfig[0].Split(' ');
            return nodeInfo[(int)ClusterInfoTag.NODEID];
        }

        public string GetNodeIdFromNode(int nodeIndex, ILogger logger)
        {
            var nodeConfig = Nodes(nodeIndex, logger: logger);
            var nodeInfo = nodeConfig[0].Split(' ');
            return nodeInfo[(int)ClusterInfoTag.NODEID];
        }

        public int GetMovedAddress(int port, ushort slot, ILogger logger)
        {
            var nodeConfig = Nodes(GetEndPointFromPort(port), logger);
            foreach (var configLine in nodeConfig)
            {
                var nodeInfo = configLine.Split(' ');
                for (int i = (int)ClusterInfoTag.SLOT; i < nodeInfo.Length; i++)
                {
                    var range = nodeInfo[i].Split('-');
                    ushort slotStart = ushort.Parse(range[0]);
                    ushort slotEnd;
                    if (range.Length > 1)
                    {
                        slotEnd = ushort.Parse(range[1]);
                        if (slot >= slotStart && slot <= slotEnd)
                        {
                            var portStr = nodeInfo[(int)ClusterInfoTag.ADDRESS].Split('@')[0].Split(':')[1];
                            return int.Parse(portStr);
                        }
                    }
                    else
                    {
                        if (slot == slotStart)
                        {
                            var portStr = nodeInfo[(int)ClusterInfoTag.ADDRESS].Split('@')[0].Split(':')[1];
                            return int.Parse(portStr);
                        }
                    }
                }
            }
            return -1;
        }

        public int GetEndPointIndexFromPort(int port)
        {
            for (int i = 0; i < endpoints.Count; i++)
            {
                if (((IPEndPoint)endpoints[i]).Port == port)
                    return i;
            }
            return -1;
        }

        public static int GetSourceNodeIndexFromSlot(ref LightClientRequest[] connections, ushort slot)
        {
            for (int j = 0; j < connections.Length; j++)
            {
                var nodeConfig = Nodes(ref connections[j])[0];
                var nodeInfo = nodeConfig.Split(' ');
                for (var i = (int)ClusterInfoTag.SLOT; i < nodeInfo.Length; i++)
                {
                    var range = nodeInfo[i].Split('-');
                    var slotStart = ushort.Parse(range[0]);
                    int slotEnd;
                    if (range.Length > 1)
                    {
                        slotEnd = ushort.Parse(range[1]);
                        if (slot >= slotStart && slot <= slotEnd)
                        {
                            return j;
                        }
                    }
                    else
                    {
                        if (slot == slotStart)
                        {
                            return j;
                        }
                    }
                }
            }
            return -1;
        }

        public int GetSourceNodeIndexFromSlot(ushort slot, ILogger logger)
        {
            for (var j = 0; j < endpoints.Count; j++)
            {
                var nodeConfig = Nodes((IPEndPoint)endpoints[j], logger)[0];
                var nodeInfo = nodeConfig.Split(' ');
                for (var i = (int)ClusterInfoTag.SLOT; i < nodeInfo.Length; i++)
                {
                    var range = nodeInfo[i].Split('-');
                    var slotStart = ushort.Parse(range[0]);
                    ushort slotEnd;
                    if (range.Length > 1)
                    {
                        slotEnd = ushort.Parse(range[1]);
                        if (slot >= slotStart && slot <= slotEnd)
                        {
                            return j;
                        }
                    }
                    else
                    {
                        if (slot == slotStart)
                        {
                            return j;
                        }
                    }
                }
            }
            return -1;
        }

        public static void GetEndPointFromResponse(byte[] resp, out int slot, out IPEndPoint endpoint)
        {
            var strResp = Encoding.ASCII.GetString(resp);
            var data = strResp.Split(' ');
            slot = int.Parse(data[1]);

            var endpointSplit = data[2].Split(':');
            endpoint = new IPEndPoint(
                IPAddress.Parse(endpointSplit[0]),
                int.Parse(endpointSplit[1].Split('\r')[0]));
        }

        public string AddDelSlots(int nodeIndex, List<int> slots, bool addslot, ILogger logger = null)
        {
            var endPoint = ((IPEndPoint)endpoints[nodeIndex]);
            var server = redis.GetServer(endPoint);
            var objects = slots.Select(x => (object)x).ToList();
            objects.Insert(0, addslot ? "addslots" : "delslots");

            try
            {
                return (string)server.Execute("cluster", [.. objects]);
            }
            catch (Exception e)
            {
                logger?.LogError("AddDelSlots error {msg}", e.Message);
                // No fail because testing failing responses
                return e.Message;
            }
        }

        public string AddDelSlotsRange(int nodeIndex, List<(int, int)> ranges, bool addslot, ILogger logger = null)
        {
            var endPoint = (IPEndPoint)endpoints[nodeIndex];
            var server = redis.GetServer(endPoint);
            var objects = ranges.SelectMany(x => new List<object> { x.Item1, x.Item2 }).ToList();
            objects.Insert(0, addslot ? "addslotsrange" : "delslotsrange");

            try
            {
                return (string)server.Execute("cluster", [.. objects]);
            }
            catch (Exception e)
            {
                logger?.LogError("AddDelSlotsRange error {msg}", e.Message);
                // No fail testing resp response
                return e.Message;
            }
        }

        public static string SetSlot(ref LightClientRequest node, int slot, string state, string nodeid)
        {
            byte[] resp;
            if (nodeid != "")
            {
                resp = node.SendCommand($"cluster setslot {slot} {state} {nodeid}");
            }
            else
            {
                resp = node.SendCommand($"cluster setslot {slot} {state}");
            }

            return ParseRespToString(resp, out _);
        }

        public string SetSlot(int nodeIndex, int slot, string state, string nodeid, ILogger logger = null)
        {
            var endPoint = GetEndPoint(nodeIndex);
            return SetSlot(endPoint, slot, state, nodeid, logger);
        }

        public string SetSlot(IPEndPoint endPoint, int slot, string state, string nodeid, ILogger logger = null)
        {
            var server = GetServer(endPoint);
            try
            {
                string ret;
                if (nodeid != "")
                {
                    ret = (string)server.Execute("cluster", "setslot", $"{slot}", $"{state}", $"{nodeid}");
                }
                else
                {
                    ret = (string)server.Execute("cluster", "setslot", $"{slot}", $"{state}");
                }

                return ret;
            }
            catch (RedisTimeoutException tex)
            {
                logger?.LogError(tex, "Timeout exception");
                Assert.Fail(tex.Message);
                return tex.Message;
            }
            catch (Exception e)
            {
                // No fail because testing responses
                return e.Message;
            }
        }

        public string[] SlotState(int nodeIndex, int slot, ILogger logger = null)
            => SlotState(GetEndPoint(nodeIndex), slot, logger);

        public string[] SlotState(IPEndPoint endpoint, int slot, ILogger logger = null)
        {
            try
            {
                var server = GetServer(endpoint);
                var resp = (string)server.Execute("cluster", "slotstate", $"{slot}");
                return resp.Split(" ");
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "SlotState");
                Assert.Fail(ex.Message);
            }
            return null;
        }

        public void MigrateSlotsIndex(int sourceNodeIndex, int targetNodeIndex, List<int> slots, bool range = false, string authPassword = null, ILogger logger = null)
        {
            var srcPort = GetPortFromNodeIndex(sourceNodeIndex);
            var dstPort = GetPortFromNodeIndex(targetNodeIndex);
            MigrateSlots(srcPort, dstPort, slots, range, authPassword, logger);
        }

        public void MigrateSlots(int sourcePort, int targetPort, List<int> slots, bool range = false, string authPassword = null, ILogger logger = null)
        {
            var sourceEndPoint = GetEndPointFromPort(sourcePort);
            var targetEndPoint = GetEndPointFromPort(targetPort);
            MigrateSlots(sourceEndPoint, targetEndPoint, slots, range, authPassword, logger);
        }

        public void MigrateSlots(IPEndPoint source, IPEndPoint target, List<int> slots, bool range = false, string authPassword = null, ILogger logger = null)
        {
            // MIGRATE host port <key | ""> destination-db timeout [COPY] [REPLACE] [[AUTH password] | [AUTH2 username password]] [KEYS key [key...]]
            var server = redis.GetServer(source);
            ICollection<object> args = new List<object>
            {
                target.Address.ToString(),
                target.Port,
                "",
                0,
                -1
            };

            if (authPassword != null)
            {
                args.Add("AUTH");
                args.Add(authPassword);
            }
            args.Add(range ? "SLOTSRANGE" : "SLOTS");

            foreach (var slot in slots)
                args.Add(slot);

            try
            {
                var resp = server.Execute("migrate", args);
                ClassicAssert.AreEqual((string)resp, "OK");
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred");
                Assert.Fail(ex.Message);
            }
        }

        public void MigrateKeys(IPEndPoint source, IPEndPoint target, List<byte[]> keys, ILogger logger)
        {
            var server = redis.GetServer(source);
            ICollection<object> args = new List<object>()
            {
                target.Address.ToString(),
                target.Port,
                "",
                0,
                -1,
                "KEYS"
            };
            foreach (var key in keys)
                args.Add(Encoding.ASCII.GetString(key));

            var elapsed = Stopwatch.GetTimestamp();
            try
            {
                var resp = server.Execute("migrate", args);
                ClassicAssert.AreEqual((string)resp, "OK");
            }
            catch (Exception ex)
            {
                elapsed = Stopwatch.GetTimestamp() - elapsed;
                logger?.LogError(ex, "An error has occurred");
                logger?.LogError("timeoutSpan: {elapsed}", TimeSpan.FromTicks(elapsed));
                Assert.Fail(ex.Message);
            }
        }

        public int MigrateTasks(IPEndPoint endPoint, ILogger logger)
        {
            var server = redis.GetServer(endPoint);
            try
            {
                var result = server.Execute("cluster", "MTASKS");
                return int.Parse((string)result);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred");
                Assert.Fail(ex.Message);
                return -1;
            }
        }

        public void WaitForMigrationCleanup(int nodeIndex, ILogger logger = null)
            => WaitForMigrationCleanup(endpoints[nodeIndex].ToIPEndPoint(), logger);

        public void WaitForMigrationCleanup(IPEndPoint endPoint, ILogger logger)
        {
            while (MigrateTasks(endPoint, logger) > 0) { BackOff(cancellationToken: context.cts.Token); }
        }

        public void WaitForMigrationCleanup(ILogger logger)
        {
            foreach (var endPoint in endpoints)
                WaitForMigrationCleanup((IPEndPoint)endPoint, logger);
        }

        public static void Asking(ref LightClientRequest sourceNode)
        {
            var result = sourceNode.SendCommand($"ASKING");
            ClassicAssert.IsTrue(result.AsSpan()[..bresp_OK.Length].SequenceEqual(bresp_OK));
        }

        public void PingAll(ILogger logger)
        {
            try
            {
                for (int i = 0; i < endpoints.Count; i++)
                {
                    var endPoint = ((IPEndPoint)endpoints[i]);
                    var server = redis.GetServer(endPoint);
                    var resp = server.Execute("PING");
                    while (((string)resp) != "PONG")
                        resp = server.Execute("PING");
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred");
                Assert.Fail(ex.Message);
            }
        }

        public List<SlotItem> ClusterSlots(int nodeIndex, ILogger logger = null)
            => ClusterSlots((IPEndPoint)endpoints[nodeIndex], logger);

        public List<SlotItem> ClusterSlots(IPEndPoint endPoint, ILogger logger = null)
        {
            List<SlotItem> slotItems = new();
            try
            {
                var server = redis.GetServer(endPoint);
                var result = server.Execute("cluster", "slots");
                if (result.IsNull)
                    return null;

                var slotRanges = (RedisResult[])result;
                foreach (var slotRange in slotRanges)
                {
                    SlotItem slotItem = default;
                    var info = (RedisResult[])slotRange;
                    var (startSlot, endSlot) = ((int)info[0], (int)info[1]);
                    ClassicAssert.IsTrue(startSlot >= 0 && startSlot <= 16383);
                    ClassicAssert.IsTrue(endSlot >= 0 && endSlot <= 16383);
                    slotItem.startSlot = (ushort)startSlot;
                    slotItem.endSlot = (ushort)endSlot;

                    slotItem.nnInfo = new NodeNetInfo[info.Length - 2];
                    for (int i = 2; i < info.Length; i++)
                    {
                        var nodeInfo = (RedisResult[])info[i];
                        var address = (string)nodeInfo[0];
                        var port = (int)nodeInfo[1];
                        var nodeid = (string)nodeInfo[2];
                        var hostNameInfo = ((RedisResult[])nodeInfo[3]);
                        var hostname = (string)hostNameInfo[1];

                        slotItem.nnInfo[i - 2].address = address;
                        slotItem.nnInfo[i - 2].port = port;
                        slotItem.nnInfo[i - 2].nodeid = nodeid;
                        slotItem.nnInfo[i - 2].hostname = hostname;
                        slotItem.nnInfo[i - 2].isPrimary = (i == 2);
                    }
                    slotItems.Add(slotItem);
                }
                return slotItems;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; ClusterSlots");
                Assert.Fail(ex.Message);
                return null;
            }
        }

        public string ClusterReplicate(int replicaNodeIndex, int primaryNodeIndex, bool async = false, bool failEx = true, ILogger logger = null)
        {
            var primaryId = ClusterMyId(primaryNodeIndex, logger: logger);
            return ClusterReplicate(replicaNodeIndex, primaryId, async: async, failEx: failEx, logger: logger);
        }

        public string ClusterReplicate(int sourceNodeIndex, string primaryNodeId, bool async = false, bool failEx = true, ILogger logger = null)
            => ClusterReplicate((IPEndPoint)endpoints[sourceNodeIndex], primaryNodeId, async: async, failEx: failEx, logger);

        public string ClusterReplicate(IPEndPoint endPoint, string primaryNodeId, bool async = false, bool failEx = true, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(endPoint);
                List<object> args = async ? ["replicate", primaryNodeId, "async"] : ["replicate", primaryNodeId, "sync"];
                var result = (string)server.Execute("cluster", args);
                ClassicAssert.AreEqual("OK", result);
                return result;
            }
            catch (Exception ex)
            {
                if (failEx)
                {
                    logger?.LogError(ex, "An error has occured; ClusterReplicate");
                    Assert.Fail(ex.Message);
                }
                return ex.Message;
            }
        }

        public string ClusterFailover(int sourceNodeIndex, string option = null, ILogger logger = null)
            => ClusterFailover((IPEndPoint)endpoints[sourceNodeIndex], option, logger);

        public string ClusterFailover(IPEndPoint endPoint, string option = null, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(endPoint);
                List<object> args = option == null ? ["failover"] : ["failover", option];
                var result = (string)server.Execute("cluster", args);
                ClassicAssert.AreEqual("OK", result);
                return result;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; ClusterFailover");
                Assert.Fail(ex.Message);
                return ex.Message;
            }
        }

        public string ReplicaOf(int replicaNodeIndex, int primaryNodeIndex = -1, bool failEx = true, ILogger logger = null)
            => ReplicaOf((IPEndPoint)endpoints[replicaNodeIndex],
                primaryNodeIndex >= 0 ? (IPEndPoint)endpoints[primaryNodeIndex] : null, failEx: failEx, logger);

        public string ReplicaOf(IPEndPoint replicaNode, IPEndPoint primaryNode = null, bool failEx = true, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(replicaNode);
                var args = new List<object>() {
                    primaryNode == null ? "NO" : primaryNode.Address.ToString(),
                    primaryNode == null ? "ONE" : primaryNode.Port.ToString()
                    };
                var result = (string)server.Execute("replicaof", args);
                ClassicAssert.AreEqual("OK", result);
                return result;
            }
            catch (Exception ex)
            {
                if (failEx)
                {
                    logger?.LogError("An error has occurred; ReplicaOf {msg}", ex.Message);
                    Assert.Fail(ex.Message);
                }
                return ex.Message;
            }
        }

        public string ClusterForget(int nodeIndex, string nodeid, int expirySeconds, ILogger logger = null)
            => ClusterForget((IPEndPoint)endpoints[nodeIndex], nodeid, expirySeconds, logger);

        public string ClusterForget(IPEndPoint endPoint, string nodeId, int expirySeconds, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(endPoint);
                var args = new List<object>() {
                    "forget",
                    Encoding.ASCII.GetBytes(nodeId),
                    Encoding.ASCII.GetBytes(expirySeconds.ToString())
                };
                var result = (string)server.Execute("cluster", args);
                ClassicAssert.AreEqual("OK", result);
                return result;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; ClusterForget");
                Assert.Fail(ex.Message);
                return ex.Message;
            }
        }

        public string ClusterReset(int nodeIndex, bool soft = true, int expiry = 60, ILogger logger = null)
            => ClusterReset((IPEndPoint)endpoints[nodeIndex], soft, expiry, logger);

        public string ClusterReset(IPEndPoint endPoint, bool soft = true, int expiry = 60, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(endPoint);
                var args = new List<object>() {
                    "reset",
                    soft ? "soft" : "hard",
                    expiry.ToString()
                };

                var result = (string)server.Execute("cluster", args);
                ClassicAssert.AreEqual("OK", result);
                return result;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; ClusterReset");
                Assert.Fail(ex.Message);
                return ex.Message;
            }
        }

        public int ClusterKeySlot(int nodeIndex, string key, ILogger logger = null)
            => ClusterKeySlot((IPEndPoint)endpoints[nodeIndex], key, logger);

        public int ClusterKeySlot(IPEndPoint endPoint, string key, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(endPoint);
                var args = new List<object>() {
                    "keyslot",
                    Encoding.ASCII.GetBytes(key)
                };

                var result = (string)server.Execute("cluster", args);
                return int.Parse(result);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; ClusterKeySlot");
                Assert.Fail();
                return -1;
            }
        }

        public void FlushAll(int nodeIndex, ILogger logger = null)
            => FlushAll((IPEndPoint)endpoints[nodeIndex], logger);

        public void FlushAll(IPEndPoint endPoint, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(endPoint);
                server.FlushAllDatabases();
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; FlushAllDatabases");
                Assert.Fail();
            }
        }

        public string ClusterStatus(int[] nodeIndices)
        {
            var clusterStatus = "";
            foreach (var index in nodeIndices)
                clusterStatus += $"{GetNodeInfo(ClusterNodes(index))}\n";

            static string GetNodeInfo(ClusterConfiguration nodeConfig)
            {
                var output = $"[{nodeConfig.Origin}]";

                foreach (var node in nodeConfig.Nodes)
                    output += $"\n\t{node.Raw}";
                return output;
            }
            return clusterStatus;
        }

        public ClusterConfiguration ClusterNodes(int nodeIndex, ILogger logger = null)
            => ClusterNodes((IPEndPoint)endpoints[nodeIndex], logger);

        public ClusterConfiguration ClusterNodes(IPEndPoint endPoint, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(endPoint);
                return server.ClusterNodes();
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; ClusterNodes");
                Assert.Fail();
                return null;
            }
        }

        public void WaitClusterNodesSync(int syncOnNodeIndex, int count, ILogger logger)
        {
            while (true)
            {
                var config = ClusterNodes(syncOnNodeIndex, logger);
                if (config.Nodes.Count == count)
                    break;
                BackOff(cancellationToken: context.cts.Token);
            }

        retrySync:
            var configNodes = ClusterNodes(syncOnNodeIndex, logger).Nodes.ToList();
            configNodes.Sort((x, y) => x.NodeId.CompareTo(y.NodeId));
            for (var i = 0; i < endpoints.Count; i++)
            {
                if (i == syncOnNodeIndex) continue;
                var otherConfigNodes = ClusterNodes(i, logger).Nodes.ToList();
                otherConfigNodes.Sort((x, y) => x.NodeId.CompareTo(y.NodeId));

                if (configNodes.Count != otherConfigNodes.Count) goto retrySync;

                for (var j = 0; j < configNodes.Count; j++)
                {
                    if (!configNodes[j].Equals(otherConfigNodes[j]))
                    {
                        BackOff(cancellationToken: context.cts.Token);
                        goto retrySync;
                    }
                }
            }
        }

        public List<ShardInfo> ClusterShards(int nodeIndex, ILogger logger = null)
            => ClusterShards((IPEndPoint)endpoints[nodeIndex], logger);

        public List<ShardInfo> ClusterShards(IPEndPoint endPoint, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(endPoint);
                var result = server.Execute("cluster", "shards");
                if (result.IsNull)
                    return null;
                List<ShardInfo> shards = [];

                var shardArray = (RedisResult[])result;
                foreach (var shard in shardArray.Select(v => (RedisResult[])v))
                {
                    ClassicAssert.AreEqual(4, shard.Length);
                    var slots = (RedisResult[])shard[1];
                    var nodes = (RedisResult[])shard[3];

                    ShardInfo shardInfo = new()
                    {
                        slotRanges = []
                    };
                    for (var i = 0; i < slots.Length; i += 2)
                        shardInfo.slotRanges.Add(((int)slots[i], (int)slots[i + 1]));

                    shardInfo.nodes = [];
                    foreach (var node in nodes.Select(v => (RedisResult[])v))
                    {
                        ClassicAssert.AreEqual(12, node.Length);
                        NodeInfo nodeInfo = new()
                        {
                            nodeIndex = GetNodeIndexFromPort((int)node[3]),
                            nodeid = (string)node[1],
                            port = (int)node[3],
                            address = (string)node[5],
                            role = Enum.Parse<NodeRole>((string)node[7]),
                            replicationOffset = (long)node[9]
                        };
                        shardInfo.nodes.Add(nodeInfo);
                    }

                    shards.Add(shardInfo);
                }

                return shards;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; ClusterShards");
                Assert.Fail(ex.Message);
                return null;
            }
        }

        public ResponseState SetKey(int nodeIndex, byte[] key, byte[] value, out int slot, out IPEndPoint actualEndpoint, bool asking = false, int expiry = -1, ILogger logger = null)
        {
            var endPoint = GetEndPoint(nodeIndex);
            return SetKey(endPoint, key, value, out slot, out actualEndpoint, asking, expiry, logger);
        }

        public ResponseState SetKey(IPEndPoint endPoint, byte[] key, byte[] value, out int slot, out IPEndPoint actualEndpoint, bool asking = false, int expiry = -1, ILogger logger = null)
        {
            var server = GetServer(endPoint);
            slot = -1;
            actualEndpoint = endPoint;

            if (asking)
            {
                try
                {
                    var resp = (string)server.Execute("ASKING");
                    ClassicAssert.AreEqual("OK", resp);
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, $"{nameof(SetKey)}");
                    Assert.Fail(ex.Message);
                }
            }

            try
            {
                if (expiry == -1)
                {
                    ICollection<object> args = [key, value];
                    var resp = (string)server.Execute("set", args, CommandFlags.NoRedirect);
                    ClassicAssert.AreEqual("OK", resp);
                    return ResponseState.OK;
                }
                else
                {
                    ICollection<object> args = [key, expiry, value];
                    var resp = (string)server.Execute("setex", args, CommandFlags.NoRedirect);
                    ClassicAssert.AreEqual("OK", resp);
                    return ResponseState.OK;
                }
            }
            catch (Exception e)
            {
                var tokens = e.Message.Split(' ');
                if (tokens.Length > 10 && tokens[2].Equals("MOVED"))
                {
                    var endpointSplit = tokens[5].Split(':');
                    actualEndpoint = new IPEndPoint(IPAddress.Parse(endpointSplit[0]), int.Parse(endpointSplit[1]));
                    slot = int.Parse(tokens[8]);
                    return ResponseState.MOVED;
                }
                else if (tokens.Length > 10 && tokens[0].Equals("Endpoint"))
                {
                    var endpointSplit = tokens[1].Split(':');
                    actualEndpoint = new IPEndPoint(IPAddress.Parse(endpointSplit[0]), int.Parse(endpointSplit[1]));
                    slot = int.Parse(tokens[4]);
                    return ResponseState.ASK;
                }
                else if (e.Message.StartsWith("CLUSTERDOWN"))
                {
                    return ResponseState.CLUSTERDOWN;
                }
                else if (e.Message.StartsWith("MIGRATING"))
                {
                    return ResponseState.MIGRATING;
                }
                else if (e.Message.StartsWith("WERR"))
                {
                    return ResponseState.REPLICA_WERR;
                }
                logger?.LogError(e, "Unexpected exception");
                Assert.Fail(e.Message);
                return ResponseState.NONE;
            }
        }

        public string GetKey(int nodeIndex, byte[] key, out int slot, out IPEndPoint actualEndpoint, out ResponseState responseState, bool asking = false, ILogger logger = null)
        {
            var endPoint = GetEndPoint(nodeIndex);
            return GetKey(endPoint, key, out slot, out actualEndpoint, out responseState, asking, logger);
        }

        public string GetKey(IPEndPoint endPoint, byte[] key, out int slot, out IPEndPoint actualEndpoint, out ResponseState responseState, bool asking = false, ILogger logger = null)
        {
            slot = -1;
            actualEndpoint = endPoint;
            responseState = ResponseState.NONE;
            var server = GetServer(endPoint);
            string result;
            if (asking)
            {
                try
                {
                    var resp = (string)server.Execute("ASKING");
                    ClassicAssert.AreEqual("OK", resp);
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, $"{nameof(SetKey)}");
                    Assert.Fail(ex.Message);
                }
            }

            try
            {
                ICollection<object> args = new List<object>() { (object)key };
                result = (string)server.Execute("get", args, CommandFlags.NoRedirect);
                slot = HashSlot(key);
                responseState = ResponseState.OK;
                return result;
            }
            catch (Exception e)
            {
                var tokens = e.Message.Split(' ');
                if (tokens.Length > 10 && tokens[2].Equals("MOVED"))
                {
                    var endpointSplit = tokens[5].Split(':');
                    actualEndpoint = new IPEndPoint(IPAddress.Parse(endpointSplit[0]), int.Parse(endpointSplit[1]));
                    slot = int.Parse(tokens[8]);
                    responseState = ResponseState.MOVED;
                    return "MOVED";
                }
                else if (tokens.Length > 10 && tokens[0].Equals("Endpoint"))
                {
                    var endpointSplit = tokens[1].Split(':');
                    actualEndpoint = new IPEndPoint(IPAddress.Parse(endpointSplit[0]), int.Parse(endpointSplit[1]));

                    slot = int.Parse(tokens[4]);
                    responseState = ResponseState.ASK;
                    return "ASK";
                }
                else if (tokens[0].Equals("ASK"))
                {
                    var endpointSplit = tokens[2].Split(':');
                    actualEndpoint = new IPEndPoint(IPAddress.Parse(endpointSplit[0]), int.Parse(endpointSplit[1]));

                    slot = int.Parse(tokens[1]);
                    responseState = ResponseState.ASK;
                    return "ASK";
                }
                else if (e.Message.StartsWith("CLUSTERDOWN"))
                {
                    responseState = ResponseState.CLUSTERDOWN;
                    return "CLUSTERDOWN";
                }
                logger?.LogError(e, "Unexpected exception");
                Assert.Fail(e.Message);
                return e.Message;
            }
        }

        public string SetMultiKey(int nodeIndex, List<byte[]> keys, List<byte[]> values, out int slot, out string address, out int port)
        {
            slot = -1;
            address = null;
            port = -1;
            var server = GetServer(nodeIndex);

            ICollection<object> args = new List<object>();
            for (var i = 0; i < keys.Count; i++)
            {
                args.Add(keys[i]);
                args.Add(values[i]);
            }

            try
            {
                return (string)server.Execute("mset", args, CommandFlags.NoRedirect);
            }
            catch (Exception e)
            {
                var tokens = e.Message.Split(' ');
                if (tokens.Length > 10 && tokens[2].Equals("MOVED"))
                {
                    address = tokens[5].Split(':')[0];
                    port = int.Parse(tokens[5].Split(':')[1]);
                    slot = int.Parse(tokens[8]);
                    return "MOVED";
                }
                else if (tokens.Length > 10 && tokens[0].Equals("Endpoint"))
                {
                    address = tokens[1].Split(':')[0];
                    port = int.Parse(tokens[1].Split(':')[1]);
                    slot = int.Parse(tokens[4]);
                    return "ASK";
                }

                if (e.Message.StartsWith("CROSSSLOT"))
                {
                    return "CROSSSLOT";
                }
                Assert.Fail(e.Message);
                return e.Message;
            }
        }

        public string GetMultiKey(int nodeIndex, List<byte[]> keys, out List<byte[]> getResult, out int slot, out string address, out int port)
        {
            getResult = null;
            slot = -1;
            address = null;
            port = -1;
            var server = GetServer(nodeIndex);

            ICollection<object> args = new List<object>();
            for (int i = 0; i < keys.Count; i++)
            {
                args.Add(keys[i]);
            }

            try
            {
                var result = server.Execute("mget", args, CommandFlags.NoRedirect);
                getResult = [.. ((RedisResult[])result).Select(x => (byte[])x)];
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
                    return "MOVED";
                }
                else if (tokens.Length > 10 && tokens[0].Equals("Endpoint"))
                {
                    address = tokens[1].Split(':')[0];
                    port = int.Parse(tokens[1].Split(':')[1]);
                    slot = int.Parse(tokens[4]);
                    return "ASK";
                }

                if (e.Message.StartsWith("CROSSSLOT"))
                {
                    return "CROSSSLOT";
                }

                Assert.Fail(e.Message);
                return e.Message;
            }
        }

        public int Lpush(int nodeIndex, string key, List<int> elements, ILogger logger = null)
        {
            try
            {
                var server = GetServer(nodeIndex);
                var args = new List<object>() { key };

                for (int i = elements.Count - 1; i >= 0; i--) args.Add(elements[i]);

                var result = (int)server.Execute("LPUSH", args);
                ClassicAssert.AreEqual(elements.Count, result);
                return result;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "lpush error");
                Assert.Fail(ex.Message);
                return -1;
            }
        }

        public List<int> Lrange(int nodeIndex, string key, ILogger logger = null)
        {
            try
            {
                var server = GetServer(nodeIndex);
                var args = new List<object>() { key, "0", "-1" };

                var result = server.Execute("LRANGE", args);
                return [.. ((int[])result)];
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "lrange error");
                Assert.Fail(ex.Message);
                return null;
            }
        }

        public void Sadd(int nodeIndex, string key, List<int> elements, ILogger logger = null)
        {
            try
            {
                var server = GetServer(nodeIndex);
                var args = new List<object>() { key };

                for (int i = elements.Count - 1; i >= 0; i--) args.Add(elements[i]);

                var result = (int)server.Execute("SADD", args);
                ClassicAssert.AreEqual(elements.Count, result);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "sadd error");
                Assert.Fail(ex.Message);
            }
        }

        public List<int> Smembers(int nodeIndex, string key, ILogger logger = null)
        {
            try
            {
                var server = GetServer(nodeIndex);
                var args = new List<object>() { key };

                var result = server.Execute("SMEMBERS", args);
                return [.. ((int[])result)];
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "smembers error");
                Assert.Fail(ex.Message);
                return null;
            }
        }

        public AofAddress GetStoreCurrentAofAddress(int nodeIndex, ILogger logger = null)
            => GetStoreCurrentAofAddress((IPEndPoint)endpoints[nodeIndex], logger);

        public AofAddress GetStoreCurrentAofAddress(IPEndPoint endPoint, ILogger logger = null)
        {
            try
            {
                var storeCurrentSafeAofAddress = GetReplicationInfo(endPoint, [ReplicationInfoItem.STORE_CURRENT_SAFE_AOF_ADDRESS], logger)[0].Item2;
                return AofAddress.FromString(storeCurrentSafeAofAddress);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; GetStoreCurrentAofAddress");
                Assert.Fail(ex.Message);
                return default;
            }
        }

        public AofAddress GetStoreRecoveredAofAddress(int nodeIndex, ILogger logger = null)
            => GetStoreRecoveredAofAddress((IPEndPoint)endpoints[nodeIndex], logger);

        public AofAddress GetStoreRecoveredAofAddress(IPEndPoint endPoint, ILogger logger = null)
        {
            try
            {
                var storeRecoveredSafeAofAddress = GetReplicationInfo(endPoint, [ReplicationInfoItem.STORE_RECOVERED_SAFE_AOF_ADDRESS], logger)[0].Item2;
                return AofAddress.FromString(storeRecoveredSafeAofAddress);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occured; GetStoreRecoveredAofAddress");
                Assert.Fail(ex.Message);
                return default;
            }
        }

        public long GetConnectedReplicas(int nodeIndex, ILogger logger = null)
            => GetConnectedReplicas((IPEndPoint)endpoints[nodeIndex], logger);

        public long GetConnectedReplicas(IPEndPoint endPoint, ILogger logger = null)
        {
            try
            {
                var replicaCount = GetReplicationInfo(endPoint, [ReplicationInfoItem.CONNECTED_REPLICAS], logger)[0].Item2;
                return long.Parse(replicaCount);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; GetConnectedReplicas");
                Assert.Fail(ex.Message);
                return 0;
            }
        }

        public Role RoleCommand(int nodeIndex, ILogger logger = null)
            => RoleCommand(endpoints[nodeIndex].ToIPEndPoint(), logger);

        public Role RoleCommand(IPEndPoint endPoint, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(endPoint);
                return server.Role();
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "{command}", nameof(NodeRole));
                Assert.Fail(ex.Message);
                return null;
            }
        }

        public string GetReplicationRole(int nodeIndex, ILogger logger = null)
            => GetReplicationRole((IPEndPoint)endpoints[nodeIndex], logger);

        public string GetReplicationRole(IPEndPoint endPoint, ILogger logger = null)
        {
            try
            {
                return GetReplicationInfo(endPoint, [ReplicationInfoItem.ROLE], logger)[0].Item2;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; GetReplicationRole");
                Assert.Fail(ex.Message);
                return null;
            }
        }

        public AofAddress GetReplicationOffset(int nodeIndex, ILogger logger = null)
            => GetReplicationOffset((IPEndPoint)endpoints[nodeIndex], logger);

        public AofAddress GetReplicationOffset(IPEndPoint endPoint, ILogger logger = null)
        {
            try
            {
                var offset = GetReplicationInfo(endPoint, [ReplicationInfoItem.REPLICATION_OFFSET], logger)[0].Item2;
                return AofAddress.FromString(offset);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; GetReplicationOffset");
                Assert.Fail(ex.Message);
                return AofAddress.Create(1, -1);
            }
        }

        public bool GetReplicationSyncStatus(int nodeIndex, ILogger logger = null)
            => GetReplicationSyncStatus((IPEndPoint)endpoints[nodeIndex], logger);

        public bool GetReplicationSyncStatus(IPEndPoint endPoint, ILogger logger = null)
        {
            try
            {
                var sync = GetReplicationInfo(endPoint, [ReplicationInfoItem.PRIMARY_SYNC_IN_PROGRESS], logger)[0].Item2;
                return bool.Parse(sync);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; GetReplicationSyncStatus");
                Assert.Fail(ex.Message);
                return false;
            }
        }

        public string GetFailoverState(int nodeIndex, ILogger logger = null)
            => GetFailoverState((IPEndPoint)endpoints[nodeIndex], logger);

        public string GetFailoverState(IPEndPoint endPoint, ILogger logger = null)
        {
            try
            {
                var failoverState = GetReplicationInfo(endPoint, [ReplicationInfoItem.PRIMARY_FAILOVER_STATE], logger)[0].Item2;
                return failoverState;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; GetFailoverState");
                Assert.Fail(ex.Message);
                return null;
            }
        }

        public List<(ReplicationInfoItem, string)> GetReplicationInfo(int nodeIndex, ReplicationInfoItem[] infoItems, ILogger logger = null)
            => GetReplicationInfo((IPEndPoint)endpoints[nodeIndex], infoItems, logger);

        private List<(ReplicationInfoItem, string)> GetReplicationInfo(IPEndPoint endPoint, ReplicationInfoItem[] infoItems, ILogger logger = null)
        {
            var server = redis.GetServer(endPoint);
            try
            {
                var result = server.InfoRawAsync("replication").Result;
                return ProcessReplicationInfo(result, infoItems);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occured; GetReplicationInfo");
                Assert.Fail(ex.Message);
            }
            return null;
        }

        public PersistencInfo GetPersistenceInfo(int nodeIndex, ILogger logger = null)
            => GetPersistenceInfo((IPEndPoint)endpoints[nodeIndex], logger);

        private PersistencInfo GetPersistenceInfo(IPEndPoint endPoint, ILogger logger = null)
        {
            var server = redis.GetServer(endPoint);
            try
            {
                var result = server.InfoRawAsync("persistence").Result;
                return ProcessPersistenceInfo(result);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occured; GetReplicationInfo");
                Assert.Fail(ex.Message);
            }
            return default;

            PersistencInfo ProcessPersistenceInfo(string infoSection)
            {
                var pinfo = new PersistencInfo();
                var data = infoSection.Split('\n');
                foreach (var item in data)
                {
                    if (item.StartsWith("CommittedBeginAddress:"))
                    {
                        pinfo.CommittedBeginAddress = AofAddress.FromString(item.Split(":")[1].Trim());
                    }
                    else if (item.StartsWith("CommittedUntilAddress:"))
                    {
                        pinfo.CommittedUntilAddress = AofAddress.FromString(item.Split(":")[1].Trim());
                    }
                    else if (item.StartsWith("FlushedUntilAddress:"))
                    {
                        pinfo.FlushedUntilAddress = AofAddress.FromString(item.Split(":")[1].Trim());
                    }
                    else if (item.StartsWith("BeginAddress:"))
                    {
                        pinfo.BeginAddress = AofAddress.FromString(item.Split(":")[1].Trim());
                    }
                    else if (item.StartsWith("TailAddress:"))
                    {
                        pinfo.TailAddress = AofAddress.FromString(item.Split(":")[1].Trim());
                    }
                    else if (item.StartsWith("SafeAofAddress:"))
                    {
                        pinfo.SafeAofAddress = AofAddress.FromString(item.Split(":")[1].Trim());
                    }
                }
                return pinfo;
            }
        }

        private static List<(ReplicationInfoItem, string)> ProcessReplicationInfo(string infoSection, ReplicationInfoItem[] infoItem)
        {
            var items = new List<(ReplicationInfoItem, string)>();
            var data = infoSection.Split('\n');
            string startsWith;
            foreach (var ii in infoItem)
            {
                foreach (var item in data)
                {
                    switch (ii)
                    {
                        case ReplicationInfoItem.ROLE:
                            startsWith = "role:";
                            if (item.StartsWith(startsWith)) items.Add((ii, item.Split(startsWith)[1].Trim()));
                            continue;
                        case ReplicationInfoItem.CONNECTED_REPLICAS:
                            startsWith = "connected_slaves:";
                            if (item.StartsWith(startsWith)) items.Add((ii, item.Split(startsWith)[1].Trim()));
                            continue;
                        case ReplicationInfoItem.SYNC_DRIVER_COUNT:
                            startsWith = "sync_driver_count:";
                            if (item.StartsWith(startsWith)) items.Add((ii, item.Split(startsWith)[1].Trim()));
                            continue;
                        case ReplicationInfoItem.PRIMARY_REPLID:
                            startsWith = "master_replid:";
                            if (item.StartsWith(startsWith)) items.Add((ii, item.Split(startsWith)[1].Trim()));
                            continue;
                        case ReplicationInfoItem.REPLICATION_OFFSET:
                            startsWith = "master_repl_offset:";
                            if (item.StartsWith(startsWith)) items.Add((ii, item.Split(startsWith)[1].Trim()));
                            continue;
                        case ReplicationInfoItem.STORE_CURRENT_SAFE_AOF_ADDRESS:
                            startsWith = "store_current_safe_aof_address:";
                            if (item.StartsWith(startsWith)) items.Add((ii, item.Split(startsWith)[1].Trim()));
                            continue;
                        case ReplicationInfoItem.STORE_RECOVERED_SAFE_AOF_ADDRESS:
                            startsWith = "store_recovered_safe_aof_address:";
                            if (item.StartsWith(startsWith)) items.Add((ii, item.Split(startsWith)[1].Trim()));
                            continue;
                        case ReplicationInfoItem.PRIMARY_SYNC_IN_PROGRESS:
                            startsWith = "master_sync_in_progress:";
                            if (item.StartsWith(startsWith)) items.Add((ii, item.Split(startsWith)[1].Trim()));
                            continue;
                        case ReplicationInfoItem.PRIMARY_FAILOVER_STATE:
                            startsWith = "master_failover_state:";
                            if (item.StartsWith(startsWith)) items.Add((ii, item.Split(startsWith)[1].Trim()));
                            continue;
                        case ReplicationInfoItem.RECOVER_STATUS:
                            startsWith = "recover_status:";
                            if (item.StartsWith(startsWith)) items.Add((ii, item.Split(startsWith)[1].Trim()));
                            break;
                        case ReplicationInfoItem.LAST_FAILOVER_STATE:
                            startsWith = "last_failover_state:";
                            if (item.StartsWith(startsWith)) items.Add((ii, item.Split(startsWith)[1].Trim()));
                            break;
                        default:
                            Assert.Fail($"type {infoItem} not supported!");
                            return null;
                    }
                }
            }
            if (items.Count == 0) Assert.Fail($"Getting replication info for item {infoItem} \n {infoSection} \n");
            return items;
        }

        public int GetStoreCurrentVersion(int nodeIndex, ILogger logger = null)
        {
            var result = GetStoreInfo(endpoints[nodeIndex].ToIPEndPoint(), [StoreInfoItem.CurrentVersion], logger);
            ClassicAssert.AreEqual(1, result.Count);
            return int.Parse(result[0].Item2);
        }

        public List<(StoreInfoItem, string)> GetStoreInfo(int nodeIndex, HashSet<StoreInfoItem> infoItems, ILogger logger = null)
            => GetStoreInfo(endpoints[nodeIndex].ToIPEndPoint(), infoItems, logger);

        private List<(StoreInfoItem, string)> GetStoreInfo(IPEndPoint endPoint, HashSet<StoreInfoItem> infoItems, ILogger logger = null)
        {
            var fields = new List<(StoreInfoItem, string)>();
            try
            {
                var server = redis.GetServer(endPoint);
                var result = server.InfoRawAsync("store").Result;
                var data = result.Split('\n');
                foreach (var line in data)
                {
                    if (line.StartsWith('#'))
                        continue;
                    var field = line.Trim().Split(':');

                    if (!Enum.TryParse(field[0], ignoreCase: true, out StoreInfoItem type))
                        continue;

                    if (infoItems.Contains(type))
                        fields.Add((type, field[1]));
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; GetReplicationInfo");
                Assert.Fail(ex.Message);
            }

            return fields;
        }

        public string GetInfo(int nodeIndex, string section, string segment, ILogger logger = null)
            => GetInfo(endpoints[nodeIndex].ToIPEndPoint(), section, segment, logger);

        public string GetInfo(IPEndPoint endPoint, string section, string segment, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(endPoint);
                var result = server.Info(section);
                ClassicAssert.AreEqual(1, result.Length, "section does not exist");
                foreach (var item in result[0])
                    if (item.Key.Equals(segment))
                        return item.Value;
                Assert.Fail($"Segment not available for {section} section");
                return "";
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; GetFailoverState");
                Assert.Fail(ex.Message);
                return null;
            }
        }

        public void WaitForReplicaAofSync(int primaryIndex, int secondaryIndex, ILogger logger = null, CancellationToken cancellation = default)
        {
            AofAddress primaryReplicationOffset;
            AofAddress secondaryReplicationOffset1;
            while (true)
            {
                cancellation.ThrowIfCancellationRequested();

                primaryReplicationOffset = GetReplicationOffset(primaryIndex, logger);
                secondaryReplicationOffset1 = GetReplicationOffset(secondaryIndex, logger);
                if (primaryReplicationOffset.Equals(secondaryReplicationOffset1))
                    break;

                var primaryMainStoreVersion = context.clusterTestUtils.GetStoreCurrentVersion(primaryIndex, logger);
                var replicaMainStoreVersion = context.clusterTestUtils.GetStoreCurrentVersion(secondaryIndex, logger);
                BackOff(cancellationToken: context.cts.Token, msg: $"[{endpoints[primaryIndex]}]: {primaryMainStoreVersion},{primaryReplicationOffset} != [{endpoints[secondaryIndex]}]: {replicaMainStoreVersion},{secondaryReplicationOffset1}");
            }
            logger?.LogInformation("[{primaryEndpoint}]{primaryReplicationOffset} ?? [{endpoints[secondaryEndpoint}]{secondaryReplicationOffset1}", endpoints[primaryIndex], primaryReplicationOffset, endpoints[secondaryIndex], secondaryReplicationOffset1);
        }

        public void WaitForConnectedReplicaCount(int primaryIndex, long minCount, ILogger logger = null)
        {
            while (true)
            {

                var items = GetReplicationInfo(primaryIndex, [ReplicationInfoItem.ROLE, ReplicationInfoItem.CONNECTED_REPLICAS], logger);
                var role = items[0].Item2;
                ClassicAssert.AreEqual(role, "master");

                try
                {
                    var count = long.Parse(items[1].Item2);
                    if (count == minCount) break;
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "An error occurred at WaitForConnectedReplicaCount");
                    Assert.Fail(ex.Message);
                }

                BackOff(cancellationToken: context.cts.Token);
            }
        }

        public void WaitForReplicaRecovery(int nodeIndex, ILogger logger = null)
        {
            while (true)
            {
                var items = GetReplicationInfo(nodeIndex, [ReplicationInfoItem.ROLE, ReplicationInfoItem.PRIMARY_SYNC_IN_PROGRESS], logger);
                var role = items[0].Item2;
                if (role.Equals("slave"))
                {
                    try
                    {
                        var syncInProgress = bool.Parse(items[1].Item2);
                        if (!syncInProgress) break;
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "An error occurred at WaitForConnectedReplicaCount");
                        Assert.Fail(ex.Message);
                    }
                    BackOff(cancellationToken: context.cts.Token);
                }
            }
        }

        public void WaitForNoFailover(int nodeIndex, ILogger logger = null)
        {
            while (true)
            {
                var failoverState = GetFailoverState(nodeIndex, logger);
                if (failoverState.Equals("no-failover")) break;
                BackOff(cancellationToken: context.cts.Token);
            }
        }

        public void WaitForFailoverCompleted(int nodeIndex, ILogger logger = null)
        {
            while (true)
            {
                var infoItem = context.clusterTestUtils.GetReplicationInfo(nodeIndex, [ReplicationInfoItem.LAST_FAILOVER_STATE], logger: context.logger);
                if (infoItem[0].Item2.Equals("failover-completed"))
                    break;
                BackOff(cancellationToken: context.cts.Token, msg: nameof(WaitForFailoverCompleted));
            }
        }

        public void WaitForPrimaryRole(int nodeIndex, ILogger logger = null)
        {
            while (true)
            {
                var role = RoleCommand(nodeIndex, logger);
                if (role.Value.Equals("master")) break;
                BackOff(cancellationToken: context.cts.Token);
            }
        }

        public void Checkpoint(int nodeIndex, ILogger logger = null)
            => Checkpoint((IPEndPoint)endpoints[nodeIndex], logger: logger);

        public void Checkpoint(IPEndPoint endPoint, ILogger logger = null)
        {
            var server = redis.GetServer(endPoint);
            try
            {
                var previousSaveTicks = (long)server.Execute("LASTSAVE");
#pragma warning disable CS0618 // Type or member is obsolete
                server.Save(SaveType.ForegroundSave);
#pragma warning restore CS0618 // Type or member is obsolete

                //// Spin wait for checkpoint to complete
                //while (true)
                //{
                //    var lastSaveTicks = (long)server.Execute("LASTSAVE");
                //    if (previousSaveTicks < lastSaveTicks) break;
                //    BackOff(TimeSpan.FromSeconds(1));
                //}
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; StoreWrapper.Checkpoint");
                Assert.Fail();
            }
        }

        public DateTime LastSave(int nodeIndex, ILogger logger = null)
            => LastSave((IPEndPoint)endpoints[nodeIndex], logger: logger);

        public DateTime LastSave(IPEndPoint endPoint, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(endPoint);
                return server.LastSave();
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; WaitCheckpoint");
                Assert.Fail();
            }
            return default;
        }

        public void WaitCheckpoint(int nodeIndex, DateTime time, ILogger logger = null)
            => WaitCheckpoint((IPEndPoint)endpoints[nodeIndex], time: time, logger: logger);

        public void WaitCheckpoint(IPEndPoint endPoint, DateTime time, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(endPoint);
                while (true)
                {
                    var lastSaveTime = server.LastSave();
                    if (lastSaveTime >= time)
                        break;
                    BackOff(cancellationToken: context.cts.Token);
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; WaitCheckpoint");
                Assert.Fail();
            }
        }

        public int IncrBy(int nodeIndex, string key, long value, ILogger logger = null)
            => IncrBy((IPEndPoint)endpoints[nodeIndex], key, value, logger);

        public int IncrBy(IPEndPoint endPoint, string key, long value, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(endPoint);
                return (int)server.Execute("incrby", key, value);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occured; IncrBy");
                Assert.Fail();
            }
            return -1;
        }

        public void ConfigSet(int nodeIndex, string parameter, string value, ILogger logger = null)
            => ConfigSet((IPEndPoint)endpoints[nodeIndex], parameter, value, logger);

        public void ConfigSet(IPEndPoint endPoint, string parameter, string value, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(endPoint);
                var resp = (string)server.Execute("config", "set", parameter, value);
                ClassicAssert.AreEqual("OK", resp);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; ConfigSet");
                Assert.Fail();
            }
        }

        public void ConfigSet(int nodeIndex, string[] parameter, string[] value, ILogger logger = null)
            => ConfigSet((IPEndPoint)endpoints[nodeIndex], parameter, value, logger);

        public void ConfigSet(IPEndPoint endPoint, string[] parameter, string[] value, ILogger logger = null)
        {
            try
            {
                ClassicAssert.AreEqual(parameter.Length, value.Length, $"set config parameter/value length missmatch {parameter.Length} != {value.Length}");
                ICollection<object> args = new List<object>() { "set" };
                for (int i = 0; i < parameter.Length; i++)
                {
                    args.Add(parameter[i]);
                    args.Add(value[i]);
                }

                var server = redis.GetServer(endPoint);
                var resp = (string)server.Execute("config", args);
                ClassicAssert.AreEqual("OK", resp);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; ConfigSet");
                Assert.Fail();
            }
        }

        public void AclLoad(int nodeIndex, ILogger logger = null)
            => AclLoad((IPEndPoint)endpoints[nodeIndex], logger);

        public void AclLoad(IPEndPoint endPoint, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(endPoint);
                var resp = (string)server.Execute("ACL", "LOAD");
                ClassicAssert.AreEqual("OK", resp);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; AclLoad");
                Assert.Fail();
            }
        }

        public ClusterNode GetAnyOtherNode(IPEndPoint endPoint, ILogger logger = null)
        {
            var config = ClusterNodes(endPoint, logger);
            foreach (var node in config.Nodes)
            {
                if (!node.EndPoint.ToIPEndPoint().Equals(endPoint))
                    return node;
            }

            Assert.Fail("Single node cluster");
            return null;
        }

        public int DBSize(int nodeIndex, ILogger logger = null)
            => DBSize(endpoints[nodeIndex].ToIPEndPoint(), logger);

        public int DBSize(IPEndPoint endPoint, ILogger logger = null)
        {
            try
            {
                var server = redis.GetServer(endPoint);
                var count = (int)server.Execute("DBSIZE");
                return count;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred; DBSize");
                Assert.Fail();
                return -1;
            }
        }
    }
}