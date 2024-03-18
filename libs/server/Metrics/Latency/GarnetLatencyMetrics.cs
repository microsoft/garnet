// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Garnet.common;
using HdrHistogram;

namespace Garnet.server
{
    /// <summary>
    /// Latency metrics emitted from RespServerSession
    /// </summary>
    internal class GarnetLatencyMetrics
    {
        public static readonly LatencyMetricsType[] defaultLatencyTypes =
            Enum.GetValues(typeof(LatencyMetricsType)).Cast<LatencyMetricsType>().ToArray();

        // Whether each latency type in LatencyMetricsType enum is in ticks or is a directly reported value
        static readonly bool[] defaultLatencyTypesTicks = new bool[6] { true, true, true, false, false, true };

        public LatencyMetricsEntry[] metrics;

        public GarnetLatencyMetrics()
        {
            Init();
        }

        void Init()
        {
            Debug.Assert(defaultLatencyTypes.Length == defaultLatencyTypesTicks.Length);
            metrics = new LatencyMetricsEntry[defaultLatencyTypes.Length];
            foreach (var cmd in defaultLatencyTypes)
                metrics[(int)cmd] = new LatencyMetricsEntry();
        }

        public void Merge(GarnetLatencyMetricsSession lm)
        {
            if (lm.metrics == null) return;
            int ver = lm.PriorVersion; // Use prior version for merge
            for (int i = 0; i < metrics.Length; i++)
                if (lm.metrics[i].latency[ver].TotalCount > 0)
                    metrics[i].latency.Add(lm.metrics[i].latency[ver]);
        }

        public void Reset(LatencyMetricsType cmd)
        {
            int idx = (int)cmd;
            metrics[idx].latency.Reset();
        }

        private List<MetricsItem> GetPercentiles(int idx)
        {
            if (metrics[idx].latency.TotalCount == 0)
                return new();
            var curr = metrics[idx].latency;


            if (defaultLatencyTypesTicks[idx])
            {
                var _min = (curr.GetValueAtPercentile(0) / OutputScalingFactor.TimeStampToMicroseconds).ToString("N2");
                var _5 = (curr.GetValueAtPercentile(5) / OutputScalingFactor.TimeStampToMicroseconds).ToString("N2");
                var _50 = (curr.GetValueAtPercentile(50) / OutputScalingFactor.TimeStampToMicroseconds).ToString("N2");
                var _mean = (curr.GetMean() / OutputScalingFactor.TimeStampToMicroseconds).ToString("N2");
                var _95 = (curr.GetValueAtPercentile(95) / OutputScalingFactor.TimeStampToMicroseconds).ToString("N2");
                var _99 = (curr.GetValueAtPercentile(99) / OutputScalingFactor.TimeStampToMicroseconds).ToString("N2");
                var _999 = (curr.GetValueAtPercentile(99.9) / OutputScalingFactor.TimeStampToMicroseconds).ToString("N2");

                List<MetricsItem> percentiles = new()
                {
                    new("calls", curr.TotalCount.ToString()),
                    new("min", _min),
                    new("5th", _5),
                    new("50th", _50),
                    new("mean", _mean),
                    new("95th", _95),
                    new("99th", _99),
                    new("99.9th", _999)
                };

                return percentiles;
            }
            else
            {
                var _min = curr.GetValueAtPercentile(0).ToString();
                var _5 = curr.GetValueAtPercentile(5).ToString();
                var _50 = curr.GetValueAtPercentile(50).ToString();
                var _mean = curr.GetMean().ToString("0.00");
                var _95 = curr.GetValueAtPercentile(95).ToString();
                var _99 = curr.GetValueAtPercentile(99).ToString();
                var _999 = curr.GetValueAtPercentile(99.9).ToString();

                List<MetricsItem> percentiles = new()
                {
                    new("calls", curr.TotalCount.ToString()),
                    new("min", _min),
                    new("5th", _5),
                    new("50th", _50),
                    new("mean", _mean),
                    new("95th", _95),
                    new("99th", _99),
                    new("99.9th", _999)
                };

                return percentiles;
            }
        }

        public bool GetRespHistogram(int idx, out string response, LatencyMetricsType eventType)
        {
            response = "";
            if (metrics[idx].latency.TotalCount == 0)
                return false;

            var p = GetPercentiles(idx);
            Debug.Assert(p != null);

            string cmdType = eventType.ToString();
            response = $"${cmdType.Length}\r\n{cmdType}\r\n";
            response += "*6\r\n";
            response += $"$5\r\ncalls\r\n";
            response += $":{p[0].Value}\r\n";
            response += $"$4\r\nsize\r\n";
            response += $":{metrics[idx].latency.GetEstimatedFootprintInBytes()}\r\n";
            if (defaultLatencyTypesTicks[idx])
                response += $"$14\r\nhistogram_usec\r\n";
            else
                response += $"$13\r\nhistogram_cnt\r\n";

            response += $"*{(p.Count - 1) * 2}\r\n";
            for (int i = 1; i < p.Count; i++)
            {
                response += $"${p[i].Name.Length}\r\n{p[i].Name}\r\n";
                response += $"${p[i].Value.Length}\r\n{p[i].Value}\r\n";
            }
            return true;
        }

        public string GetRespHistograms(HashSet<LatencyMetricsType> events)
        {
            int cmdCount = 0;
            string response = "";
            foreach (var eventType in events)
            {
                int idx = (int)eventType;
                if (GetRespHistogram(idx, out var cmdHistogram, eventType))
                {
                    response += cmdHistogram;
                    cmdCount++;
                }
            }
            return cmdCount == 0 ? "*0\r\n" : $"*{cmdCount * 2}\r\n" + response;
        }

        public MetricsItem[] GetLatencyMetrics(LatencyMetricsType latencyMetricsType)
        {
            int idx = (int)latencyMetricsType;
            return GetPercentiles(idx)?.ToArray();
        }

        public IEnumerable<(LatencyMetricsType, MetricsItem[])> GetLatencyMetrics(LatencyMetricsType[] latencyMetricsTypes)
        {
            for (int i = 0; i < latencyMetricsTypes.Length; i++)
            {
                var eventType = latencyMetricsTypes[i];
                MetricsItem[] infoItems = GetLatencyMetrics(eventType);
                if (infoItems != null)
                    yield return (eventType, infoItems);
            }
        }


        public void Dump(int idx)
        {
            if (metrics[idx].latency.TotalCount > 0)
            {
                Console.WriteLine("min (us); 5th (us); median (us); avg (us); 95th (us); 99th (us); 99.9th (us); cnt");
                Console.WriteLine("{0:0.0}; {1:0.0}; {2:0.0}; {3:0.0}; {4:0.0}; {5:0.0}; {6:0.0}; {7}",
                    metrics[idx].latency.GetValueAtPercentile(0) / OutputScalingFactor.TimeStampToMicroseconds,
                    metrics[idx].latency.GetValueAtPercentile(5) / OutputScalingFactor.TimeStampToMicroseconds,
                    metrics[idx].latency.GetValueAtPercentile(50) / OutputScalingFactor.TimeStampToMicroseconds,
                    metrics[idx].latency.GetMean() / OutputScalingFactor.TimeStampToMicroseconds,
                    metrics[idx].latency.GetValueAtPercentile(95) / OutputScalingFactor.TimeStampToMicroseconds,
                    metrics[idx].latency.GetValueAtPercentile(99) / OutputScalingFactor.TimeStampToMicroseconds,
                    metrics[idx].latency.GetValueAtPercentile(99.9) / OutputScalingFactor.TimeStampToMicroseconds,
                    metrics[idx].latency.TotalCount);
            }
        }

        public void Dump(LatencyMetricsType cmd)
        {
            int idx = (int)cmd;
            Dump(idx);
        }
    }
}