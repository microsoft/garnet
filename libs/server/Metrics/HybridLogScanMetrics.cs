// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Text;

namespace Garnet.server.Metrics
{
    public class HybridLogScanMetrics
    {
        private readonly Dictionary<string, Dictionary<string, (long count, long size)>> ScanMetrics = new();

        public void AddScanMetric(string region, string state, long size)
        {
            if (!ScanMetrics.TryGetValue(region, out var regionMetrics))
            {
                regionMetrics = new Dictionary<string, (long count, long size)>();
                ScanMetrics[region] = regionMetrics;
            }

            if (!regionMetrics.ContainsKey(state))
            {
                regionMetrics[state] = (1, size);
            }
            else
            {
                regionMetrics[state] = (regionMetrics[state].count + 1, regionMetrics[state].size + size);
            }
        }

        public string DumpScanMetricsInfo()
        {
            var sb = new StringBuilder();
            sb.AppendLine();
            foreach (var region in ScanMetrics.Keys)
            {
                sb.AppendLine($"# Region: {region}");
                foreach (var state in ScanMetrics[region].Keys)
                {
                    var (count, size) = ScanMetrics[region][state];
                    sb.AppendLine($"  State: {state}, Count: {count}, Size: {size}");
                }
            }
            return sb.ToString();
        }
    }
}