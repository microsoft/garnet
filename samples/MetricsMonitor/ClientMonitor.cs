// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using StackExchange.Redis;

namespace MetricsMonitor
{
    public class ClientMonitor
    {
        IConnectionMultiplexer connectionMultiplexer;
        Options opts;
        ClusterConfiguration clusterConfig;

        public ClientMonitor(Options opts)
        {
            this.opts = opts;
            connectionMultiplexer = ConnectionMultiplexer.Connect(
                Configuration.GetConfig(opts.Address, opts.Port, useTLS: opts.EnableTLS, tlsHost: opts.TlsHost, allowAdmin: true));
            if (opts.Cluster)
                clusterConfig = GetClusterConfig();
        }

        private ClusterConfiguration GetClusterConfig()
        {
            var clusterConfig = connectionMultiplexer.GetServer(opts.Address + ":" + opts.Port).ClusterNodes();
            return clusterConfig;
        }

        public void StartMonitor()
        {
            if (opts.Cluster)
            {
                if (opts.MetricsType == Metric.LATENCY)
                    MonitorClusterLatencyMetrics();
                else if (opts.MetricsType == Metric.INFO)
                    MonitorClusterServerSideMetrics();
            }
            else
            {
                if (opts.MetricsType == Metric.LATENCY)
                    MonitorServerSideLatency();
                else if (opts.MetricsType == Metric.INFO)
                    MonitorServerSideMetrics();
            }
        }

        public void MonitorServerSideLatency()
        {
            var db = connectionMultiplexer.GetDatabase(0);
            string eventStr = opts.LatencyEvent.ToString();
            Console.WriteLine("Reseting histogram..");
            var reset = db.Execute("latency", "reset", "command");
            //Wait for async reset
            Thread.Sleep(1000);
            List<object> cmdArgs = new List<object>()
            {
                "histogram",
                eventStr
            };
            int iter = 0;
            Console.WriteLine($"[Capturing server side latency metrics for event ({opts.LatencyEvent}) using client]");
            while (true)
            {
                RedisResult[] resp = (RedisResult[])db.Execute("latency", cmdArgs);
                ProcessLatencyHistogram(resp, ref iter);
                Thread.Sleep(TimeSpan.FromSeconds(opts.Poll));
            }
        }

        private bool ProcessLatencyHistogram(RedisResult[] resp, ref int iter)
        {
            if (resp.Length != 2)
                return false;

            string name = (string)resp[0];
            RedisResult[] info = (RedisResult[])resp[1];

            if (info.Length != 6)
                return false;

            int calls = Int32.Parse((string)info[1]);
            string units = (string)info[2];
            RedisResult[] latency = (RedisResult[])info[5];

            if (latency.Length != 14)
                return false;

            if (iter++ == 0)
                Console.WriteLine($"{latency[0]} (us); {latency[2]} (us); {latency[4]} (us); {latency[6]} (us); {latency[8]} (us); {latency[10]} (us); {latency[12]} (us); cnt");

            Console.WriteLine("{0:0.0}; {1:0.0}; {2:0.0}; {3:0.0}; {4:0.0}; {5:0.0}; {6:0.0}; {7}",
                latency[1],
                latency[3],
                latency[5],
                latency[7],
                latency[9],
                latency[11],
                latency[13],
                calls);

            return true;
        }

        public void MonitorServerSideMetrics()
        {
            var db = connectionMultiplexer.GetDatabase(0);
            var infoType = opts.InfoType;
            Console.WriteLine($"[Capturing server side metrics for event ({infoType}) using client]");
            while (true)
            {
                var resp = db.Execute("info", infoType.ToString());
                connectionMultiplexer.GetEndPoints();
                Console.WriteLine((string)resp);
                Thread.Sleep(TimeSpan.FromSeconds(opts.Poll));
            }
        }

        public void MonitorClusterServerSideMetrics()
        {
            var nodes = clusterConfig?.Nodes.ToArray();
            var infoType = opts.InfoType;
            Console.WriteLine($"[Capturing server side metrics for event ({infoType}) using client]");

            while (true)
            {
                for (int i = 0; i < nodes.Length; i++)
                {
                    var endpoint = (IPEndPoint)nodes[i].EndPoint;
                    Console.WriteLine($"<<< {endpoint.Address}:{endpoint.Port} >>>");
                    var server = connectionMultiplexer.GetServer(endpoint);
                    var info = server.Info(infoType.ToString());
                    foreach (var group in info)
                    {
                        var key = group.Key;
                        foreach (var pair in group)
                        {
                            Console.WriteLine($"{pair.Key}:{pair.Value}");
                        }
                    }
                    Console.WriteLine();
                }
                Thread.Sleep(TimeSpan.FromSeconds(opts.Poll));
            }
        }

        public void MonitorClusterLatencyMetrics()
        {
            var nodes = clusterConfig?.Nodes.ToArray();
            var latencyType = opts.LatencyEvent;
            Console.WriteLine($"[Capturing server side latency metrics for event ({latencyType}) using client]");

            List<object> cmdArgs = new List<object>()
            {
                "histogram",
                latencyType.ToString(),
            };
            int iter = 1;
            while (true)
            {
                for (int i = 0; i < nodes.Length; i++)
                {
                    var endpoint = (IPEndPoint)nodes[i].EndPoint;
                    Console.WriteLine($"<<< {endpoint.Address}:{endpoint.Port} >>>");
                    var server = connectionMultiplexer.GetServer(endpoint);
                    RedisResult[] resp = (RedisResult[])server.Execute("latency", cmdArgs);
                    ProcessLatencyHistogram(resp, ref iter);
                    Console.WriteLine();
                }
                Thread.Sleep(TimeSpan.FromSeconds(opts.Poll));
            }
        }


    }
}