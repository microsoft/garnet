// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using System.Text;
using Garnet.common;

namespace Resp.benchmark
{
    /// <summary>
    /// Discovers cluster topology by issuing CLUSTER NODES to one of the cluster endpoints.
    /// Uses the existing --host and --port options to connect to any single cluster node.
    /// </summary>
    public class ClusterTopology
    {
        readonly Options opts;

        public ClusterTopology(Options opts)
        {
            this.opts = opts;
        }

        /// <summary>
        /// Connect to the configured node (--host/--port) and parse CLUSTER NODES to get primary shard info.
        /// Validates that all 16384 slots are assigned.
        /// </summary>
        public ShardInfo[] DiscoverPrimaryShards()
        {
            var clusterNodesResponse = IssueClusterNodes(opts.Address, opts.Port);

            if (clusterNodesResponse == null)
                throw new Exception($"Failed to get CLUSTER NODES response from {opts.Address}:{opts.Port}");

            var shards = ParseClusterNodesResponse(clusterNodesResponse);
            ValidateFullSlotCoverage(shards);

            return shards;
        }

        /// <summary>
        /// Validate that all 16384 slots (0-16383) are assigned across the discovered shards.
        /// </summary>
        private static void ValidateFullSlotCoverage(ShardInfo[] shards)
        {
            var covered = new bool[16384];

            foreach (var shard in shards)
            {
                foreach (var (start, end) in shard.SlotRanges)
                {
                    for (int s = start; s <= end; s++)
                        covered[s] = true;
                }
            }

            var missing = new List<int>();
            for (int i = 0; i < 16384; i++)
            {
                if (!covered[i])
                    missing.Add(i);
            }

            if (missing.Count > 0)
            {
                // Report first few missing ranges
                var ranges = CompactRanges(missing);
                var rangeStr = string.Join(", ", ranges.Take(5).Select(r => r.Start == r.End ? $"{r.Start}" : $"{r.Start}-{r.End}"));
                if (ranges.Count > 5)
                    rangeStr += $" ... ({missing.Count} total unassigned slots)";

                throw new Exception($"Cluster does not have full slot coverage. Missing slots: {rangeStr}");
            }
        }

        private static List<(int Start, int End)> CompactRanges(List<int> slots)
        {
            var ranges = new List<(int Start, int End)>();
            if (slots.Count == 0) return ranges;

            int start = slots[0], end = slots[0];
            for (int i = 1; i < slots.Count; i++)
            {
                if (slots[i] == end + 1)
                    end = slots[i];
                else
                {
                    ranges.Add((start, end));
                    start = end = slots[i];
                }
            }
            ranges.Add((start, end));
            return ranges;
        }

        private unsafe string IssueClusterNodes(string host, int port)
        {
            var onResponse = new LightClient.OnResponseDelegateUnsafe(OnClusterNodesResponse);
            using var client = new LightClient(
                new IPEndPoint(IPAddress.Parse(host), port),
                0,
                onResponse,
                1 << 17, // Large buffer for CLUSTER NODES response
                opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);

            client.Connect();
            client.Authenticate(opts.Auth);

            var request = Encoding.ASCII.GetBytes("*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n");
            fixed (byte* reqPtr = request)
            {
                client.Send(reqPtr, request.Length, 1);
                client.CompletePendingRequests();
            }

            // Parse the bulk string response from the response buffer
            fixed (byte* buf = client.ResponseBuffer)
            {
                return ParseBulkStringResponse(buf, client.ResponseBuffer.Length);
            }
        }

        private static unsafe string ParseBulkStringResponse(byte* buf, int length)
        {
            // RESP bulk string: $<len>\r\n<data>\r\n
            if (length == 0 || *buf != '$')
                return null;

            byte* ptr = buf + 1;
            byte* end = buf + length;

            // Read length
            int strLen = 0;
            while (ptr < end && *ptr != '\r')
            {
                strLen = strLen * 10 + (*ptr - '0');
                ptr++;
            }

            // Skip \r\n
            ptr += 2;

            if (ptr + strLen > end)
                return null;

            return Encoding.ASCII.GetString(ptr, strLen);
        }

        private static unsafe (int, int) OnClusterNodesResponse(byte* buf, int bytesRead, int opType)
        {
            // Count bulk strings
            int count = 0;
            for (int i = 0; i < bytesRead; i++)
                if (buf[i] == '$') count++;
            return (count, 0);
        }

        /// <summary>
        /// Parse the CLUSTER NODES response into ShardInfo array (primaries only).
        /// Format per line: {nodeId} {ip}:{port}@{cport} {flags} {master} {ping} {pong} {epoch} {link} {slots...}
        /// </summary>
        internal static ShardInfo[] ParseClusterNodesResponse(string response)
        {
            var shards = new List<ShardInfo>();
            var lines = response.Split('\n', StringSplitOptions.RemoveEmptyEntries);

            foreach (var line in lines)
            {
                var trimmed = line.Trim();
                if (string.IsNullOrEmpty(trimmed))
                    continue;

                var fields = trimmed.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (fields.Length < 8)
                    continue;

                var flags = fields[2];

                // Only include primary (master) nodes
                if (!flags.Contains("master"))
                    continue;

                // Check link state is connected
                var linkState = fields[7];
                if (linkState != "connected")
                {
                    Console.WriteLine($"Warning: Primary node {fields[0]} is not in 'connected' state ({linkState}), skipping.");
                    continue;
                }

                var shard = new ShardInfo { NodeId = fields[0] };

                // Parse address: ip:port@cport
                var addrField = fields[1];
                var atIdx = addrField.IndexOf('@');
                var addrPart = atIdx > 0 ? addrField[..atIdx] : addrField;
                var colonIdx = addrPart.LastIndexOf(':');
                shard.Address = addrPart[..colonIdx];
                shard.Port = int.Parse(addrPart[(colonIdx + 1)..]);

                // Parse slot ranges (fields 8+)
                for (int i = 8; i < fields.Length; i++)
                {
                    var slotField = fields[i];

                    // Skip importing/migrating slots like [123-<-nodeId] or [123->-nodeId]
                    if (slotField.StartsWith('['))
                        continue;

                    if (slotField.Contains('-'))
                    {
                        var rangeParts = slotField.Split('-');
                        if (int.TryParse(rangeParts[0], out var start) && int.TryParse(rangeParts[1], out var end))
                            shard.SlotRanges.Add((start, end));
                    }
                    else
                    {
                        if (int.TryParse(slotField, out var slot))
                            shard.SlotRanges.Add((slot, slot));
                    }
                }

                if (shard.SlotRanges.Count > 0)
                    shards.Add(shard);
            }

            if (shards.Count == 0)
                throw new Exception("No primary shards with assigned slots found in CLUSTER NODES response.");

            return shards.ToArray();
        }
    }
}
