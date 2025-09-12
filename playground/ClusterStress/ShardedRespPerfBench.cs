// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using Garnet.common;
using StackExchange.Redis;

namespace Resp.benchmark
{
    internal struct ClusterShardConnection
    {
        public LightClient primary;
        public LightClient[] replicas;
        public int count;
    }

    /// <summary>
    /// Dummy clients issuing commands as fast as possible, with varying number of
    /// threads, to stress server side.
    /// </summary>
    public class ShardedRespPerfBench
    {
        readonly Options opts;
        readonly int Start;
        readonly ManualResetEventSlim waiter = new();

        readonly ClusterConfiguration clusterConfig;
        readonly ushort[] slotMap = new ushort[16384];

        volatile bool done = false;
        long total_ops_done = 0;

        public ShardedRespPerfBench(Options opts, int Start)
        {
            this.opts = opts;
            this.Start = Start;
            clusterConfig = GetClusterConfig();
            InitSlotMap();
            PrintClusterConfig();
            CheckAllSlotsCovered();
        }

        private ClusterConfiguration GetClusterConfig()
        {
            using var redis = ConnectionMultiplexer.Connect(BenchUtils.GetConfig(opts.Address, opts.Port, useTLS: opts.EnableTLS, tlsHost: opts.TlsHost, allowAdmin: true));
            var clusterConfig = redis.GetServer(opts.Address + ":" + opts.Port).ClusterNodes();
            return clusterConfig;
        }

        private void CheckAllSlotsCovered()
        {
            var nodes = clusterConfig.Nodes.ToArray();

            var slotMap = new byte[16384];
            foreach (var node in nodes)
            {
                var slotRanges = node.Slots;
                foreach (var slotRange in slotRanges)
                {
                    for (int i = slotRange.From; i <= slotRange.To; i++)
                        slotMap[i] = 1;
                }
            }

            for (int i = 0; i < slotMap.Length; i++)
            {
                if (slotMap[i] == 0)
                {
                    throw new Exception($"Slot {i} not covered");
                }
            }
        }

        private void InitSlotMap()
        {
            var nodes = clusterConfig.Nodes.ToArray();
            ushort j = 0;
            foreach (var node in nodes)
            {
                var slotRanges = node.Slots;
                foreach (var slotRange in slotRanges)
                    for (int i = slotRange.From; i <= slotRange.To; i++)
                        slotMap[i] = j;
                j++;
            }
        }

        private unsafe ClusterShardConnection[] InitConnections(OpType opType, int bufferSize)
        {
            var lighClientOnResponseDelegate = new LightClient.OnResponseDelegateUnsafe(ReqGen.OnResponse);
            var pNodes = clusterConfig.Nodes.ToList().FindAll(p => !p.IsReplica).ToArray();
            ClusterShardConnection[] clusterShards = new ClusterShardConnection[pNodes.Length];
            for (int i = 0; i < pNodes.Length; i++)
            {
                clusterShards[i].count = 1 + (opts.ReplicaReads ? pNodes[i].Children.Count : 0);
                IPEndPoint pEndpoint = (IPEndPoint)pNodes[i].EndPoint;
                clusterShards[i].primary = new LightClient(
                    pEndpoint,
                    (int)opType,
                    lighClientOnResponseDelegate,
                    bufferSize,
                    opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);
                clusterShards[i].primary.Connect();
                clusterShards[i].primary.Authenticate(opts.Auth);

                if (opts.ReplicaReads)
                {
                    clusterShards[i].replicas = new LightClient[pNodes[i].Children.Count];
                    for (int j = 0; j < clusterShards[i].replicas.Length; j++)
                    {
                        IPEndPoint rEndpoint = (IPEndPoint)pNodes[i].Children[j].EndPoint;
                        clusterShards[i].replicas[j] = new LightClient(
                            rEndpoint,
                            (int)opType,
                            lighClientOnResponseDelegate,
                            bufferSize,
                            opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);
                        clusterShards[i].replicas[j].Connect();
                        clusterShards[i].replicas[j].Authenticate(opts.Auth);
                        clusterShards[i].replicas[j].ReadOnly();
                    }
                }
            }
            return clusterShards;
        }

        private ref LightClient GetConnection(ref Random r, ref ClusterShardConnection[] clusterShards, int slot, bool readOnly = false)
        {
            int shardIdx = slotMap[slot];
            if (opts.ReplicaReads)
            {
                int nodeIdx = readOnly ? r.Next(0, clusterShards[shardIdx].count) : 0;
                if (nodeIdx == 0)
                    return ref clusterShards[shardIdx].primary;
                else
                    return ref clusterShards[shardIdx].replicas[nodeIdx - 1];
            }
            else
            {
                return ref clusterShards[shardIdx].primary;
            }
        }

        private static bool IsReadOnly(OpType opType)
        {
            return opType switch
            {
                OpType.MSET => false,
                OpType.GET => true,
                _ => throw new Exception($"{opType} not supported for cluster benchmark!")
            };
        }

        private void PrintClusterConfig()
        {
            Console.WriteLine("Cluster Retrieved Configuration...");
            var nodes = clusterConfig.Nodes.ToArray();
            Array.Sort(nodes, (x, y) => ((IPEndPoint)x.EndPoint).Address.ToString().CompareTo(((IPEndPoint)y.EndPoint).Address.ToString()));
            foreach (var node in nodes)
            {
                var endpoint = (IPEndPoint)node.EndPoint;
                Console.Write($"host: {endpoint.Address}:{endpoint.Port}, ");
                Console.Write($"role: {((node.IsReplica || node.Slots.Count == 0) ? "REPLICA" : "PRIMARY")}, ");
                var slotRanges = node.Slots;

                Console.Write("slotRanges: ");
                foreach (var slotRange in slotRanges)
                    Console.Write($"{slotRange.From} - {slotRange.To} ");
                Console.WriteLine("");
            }
            Console.WriteLine("<--------------------------------------------->");
        }

        /// <summary>
        /// Load DB with DbSize keys, starting from Start; specified #threads
        /// e.g., "0" => "0", "1" => "1", and so on
        /// </summary>
        /// <param name="loadDbThreads"></param>
        /// <param name="BatchSize"></param>
        /// <param name="keyLen"></param>
        /// <param name="valueLen"></param>
        /// <param name="numericValue"></param>
        public void LoadData(
            int loadDbThreads = 8,
            int BatchSize = 1 << 12,
            int keyLen = default,
            int valueLen = default,
            bool numericValue = false)
        {
            loadDbThreads = opts.DbSize / loadDbThreads > BatchSize ? loadDbThreads : 1;
            var rg = new ReqGen(
                Start,
                opts.DbSize,
                NumOps: opts.DbSize,
                BatchSize: BatchSize,
                opType: OpType.MSET,
                clusterConfig: clusterConfig,
                shard: opts.Shard,
                randomGen: false,
                randomServe: false,
                keyLen,
                valueLen,
                numericValue,
                verbose: true,
                flatBufferClient: false,
                ttl: opts.Ttl);
            rg.GenerateForCluster();

            LightOperate(
                opType: OpType.MSET,
                TotalOps: opts.DbSize,
                BatchSize: BatchSize,
                NumThreads: loadDbThreads,
                OpsPerThread: opts.DbSize / loadDbThreads,
                runTime: default,
                rg: rg,
                randomGen: false,
                randomServe: false,
                keyLen: keyLen,
                valueLen: valueLen);
        }

        public void Run(
            OpType opType,
            int TotalOps,
            int[] NumThreads,
            int BatchSize = 1 << 12,
            TimeSpan runTime = default,
            bool randomGen = true,
            bool randomServe = true,
            int keyLen = default,
            int valueLen = default,
            int ttl = 0)
        {
            var rg = new ReqGen(
                Start,
                opts.DbSize,
                TotalOps,
                BatchSize,
                opType,
                clusterConfig,
                shard: opts.Shard,
                randomGen,
                randomServe,
                keyLen,
                valueLen,
                ttl: ttl);
            rg.GenerateForCluster();

            foreach (var numThread in NumThreads)
            {
                GC.Collect();
                GC.WaitForFullGCComplete();
                LightOperate(opType, TotalOps, BatchSize, numThread, 0, runTime, rg, randomGen, randomServe, burst: opts.Burst);
            }
        }

        public ReqGen LightOperate(
            OpType opType,
            int TotalOps,
            int BatchSize,
            int NumThreads,
            int OpsPerThread = 0,
            TimeSpan runTime = default,
            ReqGen rg = null,
            bool randomGen = true,
            bool randomServe = true,
            int keyLen = default,
            int valueLen = default,
            bool numericValue = false,
            bool verbose = true,
            bool burst = false)
        {
            if (rg == null)
            {
                rg = new ReqGen(
                    Start,
                    opts.DbSize,
                    TotalOps,
                    BatchSize,
                    opType,
                    clusterConfig: clusterConfig,
                    shard: opts.Shard,
                    randomGen,
                    randomServe,
                    keyLen,
                    valueLen,
                    numericValue,
                    verbose,
                    flatBufferClient: (opts.Client == ClientType.SERedis || opts.Client == ClientType.GarnetClientSession),
                    ttl: opts.Ttl);
                rg.GenerateForCluster();
            }

            if (verbose)
            {
                Console.WriteLine();
                Console.WriteLine($"Operation type: {opType}");
                Console.WriteLine($"Num threads: {NumThreads}");
            }

            // Query database
            Thread[] workers = new Thread[NumThreads];

            // Run the experiment.
            for (int idx = 0; idx < NumThreads; idx++)
            {
                int x = idx;
                workers[idx] = opts.Client switch
                {
                    ClientType.LightClient => opts.ReplicaReads ? new Thread(() => LightOperateThreadRunnerClusterReplicaReads(x, OpsPerThread, opType, rg, burst)) :
                                                new Thread(() => LightOperateThreadRunnerCluster(x, OpsPerThread, opType, rg, burst)),
                    _ => throw new Exception($"ClientType {opts.Client} not supported"),
                };
            }

            // Start threads.
            foreach (Thread worker in workers)
                worker.Start();

            waiter.Set();

            Stopwatch swatch = new();
            swatch.Start();
            if (OpsPerThread == 0)
            {
                if (runTime == default) runTime = TimeSpan.FromSeconds(15); // default
                Thread.Sleep(runTime);
                done = true;
            }
            foreach (Thread worker in workers)
                worker.Join();

            swatch.Stop();

            double seconds = swatch.ElapsedMilliseconds / 1000.0;
            double opsPerSecond = total_ops_done / seconds;

            if (verbose)
            {
                Console.WriteLine($"Total time: {swatch.ElapsedMilliseconds:N2}ms for {total_ops_done:N2} ops");
                Console.WriteLine($"Throughput: {opsPerSecond:N2} ops/sec");
            }

            done = false;
            total_ops_done = 0;
            waiter.Reset();

            return rg;
        }

        private unsafe void LightOperateThreadRunnerCluster(int threadid, int NumOps, OpType opType, ReqGen rg, bool burst)
        {
            var lighClientOnResponseDelegate = new LightClient.OnResponseDelegateUnsafe(ReqGen.OnResponse);
            var nodes = clusterConfig?.Nodes.ToArray();
            LightClient[] clients = new LightClient[clusterConfig.Nodes.Count];
            for (int i = 0; i < clients.Length; i++)
            {
                var endpoint = (IPEndPoint)nodes[i].EndPoint;
                clients[i] = new LightClient(
                    endpoint,
                    (int)opType,
                    lighClientOnResponseDelegate,
                    rg.GetBufferSize(),
                    opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);

                clients[i].Connect();
                clients[i].Authenticate(opts.Auth);
            }

            int maxReqs = (NumOps / rg.BatchCount);
            int numReqs = 0;

            waiter.Wait();

            Random r = new(threadid);
            Stopwatch sw = new();
            sw.Start();
            while (!done)
            {
                byte[] buf = rg.GetRequestInterleaved(ref r, out var len, out var slot);
                int clientIdx = slotMap[slot];

                if (burst) clients[clientIdx].CompletePendingRequests();
                clients[clientIdx].Send(buf, len, (opType == OpType.MSET || opType == OpType.MPFADD) ? 1 : rg.BatchCount);
                if (!burst) clients[clientIdx].CompletePendingRequests();
                numReqs++;
                if (numReqs == maxReqs) break;
            }
            sw.Stop();

            Interlocked.Add(ref total_ops_done, numReqs * rg.BatchCount);
        }

        private unsafe void LightOperateThreadRunnerClusterReplicaReads(int threadid, int NumOps, OpType opType, ReqGen rg, bool burst)
        {
            var clusterShards = InitConnections(opType, rg.GetBufferSize());
            int maxReqs = (NumOps / rg.BatchCount);
            int numReqs = 0;
            //Console.WriteLine($"{NumOps}:{rg.BatchCount}:{maxReqs}");

            waiter.Wait();

            Random r = new(threadid);
            Stopwatch sw = new();
            sw.Start();
            while (!done)
            {
                //byte[] buf = rg.GetRequest(out var len, out var slot);
                byte[] buf = rg.GetRequestInterleaved(ref r, out var len, out var slot);
                ref LightClient client = ref GetConnection(ref r, ref clusterShards, slot, readOnly: IsReadOnly(opType));

                if (burst) client.CompletePendingRequests();
                client.Send(buf, len, (opType == OpType.MSET || opType == OpType.MPFADD) ? 1 : rg.BatchCount);
                if (!burst) client.CompletePendingRequests();
                numReqs++;
                if (numReqs == maxReqs) break;
            }
            sw.Stop();

            Interlocked.Add(ref total_ops_done, numReqs * rg.BatchCount);
        }
    }
}