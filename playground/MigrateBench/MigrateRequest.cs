// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System.Diagnostics;
using System.Net;
using System.Runtime.CompilerServices;
using Garnet.client;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace MigrateBench
{
    public enum MigrateRequestType
    {
        KEYS,
        SLOTS,
        SLOTSRANGE
    }

    public class MigrateRequest
    {
        readonly Options opts;

        readonly string sourceAddress;
        readonly int sourcePort;
        readonly string targetAddress;
        readonly int targetPort;
        readonly int timeout = 100000;
        readonly ILogger logger;

        GarnetClientSession sourceNode;
        GarnetClientSession targetNode;

        string targetNodeId;
        string sourceNodeId;

        IPEndPoint sourceNodeEndpoint;
        IPEndPoint targetNodeEndpoint;

        public static T MeasureElapsed<T>(long startTimestamp, TaskAwaiter<T> awaiter, string msg, ref long totalElapsed, bool verbose = false, ILogger logger = null)
        {
            var result = awaiter.GetResult();
            var elapsed = Stopwatch.GetTimestamp() - startTimestamp;
            totalElapsed += elapsed;
            var t = TimeSpan.FromTicks(elapsed);
            if (verbose)
                logger?.LogInformation("[{msg}] ElapsedTime = {elapsed} seconds", msg, t.TotalSeconds);
            return result;
        }

        public MigrateRequest(Options opts, ILogger logger = null)
        {
            this.opts = opts;
            var sourceEndpoint = opts.SourceEndpoint.Split(':');
            var targetEndpoint = opts.TargetEndpoint.Split(':');
            sourceAddress = sourceEndpoint[0];
            sourcePort = int.Parse(sourceEndpoint[1]);
            targetAddress = targetEndpoint[0];
            targetPort = int.Parse(targetEndpoint[1]);

            sourceNode = new(sourceAddress, sourcePort, new NetworkBufferSettings(1 << 22), logger: logger);
            targetNode = new(targetAddress, targetPort, new NetworkBufferSettings(1 << 22), logger: logger);
            this.timeout = (int)TimeSpan.FromSeconds(opts.Timeout).TotalMilliseconds;
            this.logger = logger;
        }

        public void Run()
        {
            try
            {
                sourceNode.Connect();
                targetNode.Connect();

                long myIdElapsed = 0;
                sourceNodeId = MeasureElapsed(Stopwatch.GetTimestamp(), sourceNode.ExecuteAsync(["CLUSTER", "MYID"]).GetAwaiter(), "CLUSTER_MYID", ref myIdElapsed, opts.Verbose, logger);
                targetNodeId = MeasureElapsed(Stopwatch.GetTimestamp(), targetNode.ExecuteAsync(["CLUSTER", "MYID"]).GetAwaiter(), "CLUSTER_MYID", ref myIdElapsed, opts.Verbose, logger);

                var endpoint = MeasureElapsed(Stopwatch.GetTimestamp(), sourceNode.ExecuteAsync(["CLUSTER", "ENDPOINT", sourceNodeId]).GetAwaiter(), "CLUSTER_ENDPOINT", ref myIdElapsed, opts.Verbose, logger);
                if (!IPEndPoint.TryParse(endpoint, out sourceNodeEndpoint))
                {
                    logger?.LogError("ERR Source Endpoint ({endpoint}) is not valid!", endpoint);
                    return;
                }
                endpoint = MeasureElapsed(Stopwatch.GetTimestamp(), targetNode.ExecuteAsync(["CLUSTER", "ENDPOINT", targetNodeId]).GetAwaiter(), "CLUSTER_ENDPOINT", ref myIdElapsed, opts.Verbose, logger);
                if (!IPEndPoint.TryParse(endpoint, out targetNodeEndpoint))
                {
                    logger?.LogError("ERR Target Endpoint ({endpoint}) is not valid!", endpoint);
                    return;
                }

                var sourceNodeKeys = dbsize(ref sourceNode);
                var targetNodeKeys = dbsize(ref targetNode);
                logger?.LogInformation("SourceNode: {endpoint} KeyCount: {keys}", opts.SourceEndpoint, sourceNodeKeys);
                logger?.LogInformation("TargetNode: {endpoint} KeyCount: {keys}", opts.TargetEndpoint, targetNodeKeys);

                switch (opts.migrateRequestType)
                {
                    case MigrateRequestType.KEYS:
                        MigrateKeys();
                        break;
                    case MigrateRequestType.SLOTS:
                        MigrateSlots();
                        break;
                    case MigrateRequestType.SLOTSRANGE:
                        MigrateSlotRanges();
                        break;
                    default:
                        throw new Exception("Error invalid migrate request type");
                }

                var sourceNodeKeys2 = dbsize(ref sourceNode);
                var targetNodeKeys2 = dbsize(ref targetNode);
                var keysTransferred = targetNodeKeys2 - targetNodeKeys;
                logger?.LogInformation("SourceNode: {endpoint} KeyCount after migration: {keys}", opts.SourceEndpoint, sourceNodeKeys2);
                logger?.LogInformation("TargetNode: {endpoint} KeyCount after migration: {keys}", opts.TargetEndpoint, targetNodeKeys2);
                logger?.LogInformation("KeysTransferred: {keys}", keysTransferred);

                int dbsize(ref GarnetClientSession c)
                {
                    if (!opts.Dbsize) return 0;
                    var resp = MeasureElapsed(Stopwatch.GetTimestamp(), c.ExecuteAsync("dbsize").GetAwaiter(), "DBSIZE", ref myIdElapsed, opts.Verbose, logger);
                    return int.Parse(resp);
                }
            }
            finally
            {
                sourceNode.Dispose();
                targetNode.Dispose();
            }
        }

        private void MigrateSlotRanges()
        {
            try
            {
                var slots = opts.Slots.ToArray();
                if (!Common.Validate(slots, logger))
                    return;

                // migrate 192.168.1.20 7001 "" 0 5000 SLOTSRANGE 1000 7000
                ICollection<string> migrate = ["MIGRATE", targetNodeEndpoint.Address.ToString(), targetNodeEndpoint.Port.ToString(), "", "0", timeout.ToString(), "REPLACE", "SLOTSRANGE"];
                foreach (var slot in slots)
                    migrate.Add(slot.ToString());

                var request = migrate.ToArray();
                var startTimestamp = Stopwatch.GetTimestamp();
                logger?.LogInformation("Issuing MIGRATE SLOTSRANGE");
                var resp = sourceNode.ExecuteAsync(request).GetAwaiter().GetResult();

                logger?.LogInformation("Waiting for Migration to Complete");
                resp = sourceNode.ExecuteAsync("CLUSTER", "MTASKS").GetAwaiter().GetResult();
                while (int.Parse(resp) > 0)
                {
                    Thread.Yield();
                    resp = sourceNode.ExecuteAsync("CLUSTER", "MTASKS").GetAwaiter().GetResult();
                }

                var elapsed = Stopwatch.GetTimestamp() - startTimestamp;
                var t = TimeSpan.FromTicks(elapsed);
                logger?.LogInformation("SLOTSRANGE Elapsed Time: {elapsed} seconds", t.TotalSeconds);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error at MigrateSlotsRange");
            }
        }

        private void MigrateSlots()
        {
            try
            {
                var _slots = Common.GetSingleSlots(opts, logger);
                if (_slots == null) return;

                ICollection<string> migrate = ["MIGRATE", targetNodeEndpoint.Address.ToString(), targetNodeEndpoint.Port.ToString(), "", "0", timeout.ToString(), "REPLACE", "SLOTS"];
                foreach (var slot in _slots)
                    migrate.Add(slot.ToString());

                var request = migrate.ToArray();
                var startTimestamp = Stopwatch.GetTimestamp();
                logger?.LogInformation("Issuing MIGRATE SLOTS");
                var resp = sourceNode.ExecuteAsync(request).GetAwaiter().GetResult();

                logger?.LogInformation("Waiting for Migration to Complete");
                resp = sourceNode.ExecuteAsync("CLUSTER", "MTASKS").GetAwaiter().GetResult();
                while (int.Parse(resp) > 0)
                {
                    Thread.Yield();
                    resp = sourceNode.ExecuteAsync("CLUSTER", "MTASKS").GetAwaiter().GetResult();
                }

                var elapsed = Stopwatch.GetTimestamp() - startTimestamp;
                var t = TimeSpan.FromTicks(elapsed);
                logger?.LogInformation("SLOTS Elapsed Time: {elapsed} seconds", t.TotalSeconds);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error at MigrateSlots");
            }
        }

        private void MigrateKeys()
        {
            try
            {
                var _slots = Common.GetSingleSlots(opts, logger);
                if (_slots == null) return;

                long myIdElapsed = 0;
                long setSlotElapsed = 0;
                long countKeysElapsed = 0;
                long getKeysElapsed = 0;
                long migrateKeysElapsed = 0;
                var sourceNodeId = MeasureElapsed(Stopwatch.GetTimestamp(), sourceNode.ExecuteAsync("cluster", "myid").GetAwaiter(), "CLUSTER_MYID", ref myIdElapsed, opts.Verbose, logger);
                var targetNodeId = MeasureElapsed(Stopwatch.GetTimestamp(), targetNode.ExecuteAsync("cluster", "myid").GetAwaiter(), "CLUSTER_MYID", ref myIdElapsed, opts.Verbose, logger);

                string[] stable = ["cluster", "setslot", "<slot>", "stable"];
                string[] migrating = ["cluster", "setslot", "<slot>", "migrating", targetNodeId];
                string[] importing = ["cluster", "setslot", "<slot>", "importing", sourceNodeId];
                string[] node = ["cluster", "setslot", "<slot>", "node", targetNodeId];
                string[] countkeysinslot = ["cluster", "countkeysinslot", "<slot>"];
                string[] getkeysinslot = ["cluster", "getkeysinslot", "<slot>", "<count>"];
                var startTimestamp = Stopwatch.GetTimestamp();
                foreach (var slot in _slots)
                {
                    var slotStr = slot.ToString();
                    stable[2] = slotStr;
                    migrating[2] = slotStr;
                    importing[2] = slotStr;
                    node[2] = slotStr;
                    countkeysinslot[2] = slotStr;
                    getkeysinslot[2] = slotStr;

                    var resp = MeasureElapsed(Stopwatch.GetTimestamp(), targetNode.ExecuteAsync(stable).GetAwaiter(), "SETSLOT_STABLE", ref setSlotElapsed, opts.Verbose, logger);
                    resp = MeasureElapsed(Stopwatch.GetTimestamp(), targetNode.ExecuteAsync(importing).GetAwaiter(), "SETSLOT_IMPORTING", ref setSlotElapsed, opts.Verbose, logger);

                    resp = MeasureElapsed(Stopwatch.GetTimestamp(), sourceNode.ExecuteAsync(stable).GetAwaiter(), "SETSLOT_STABLE", ref setSlotElapsed, opts.Verbose, logger);
                    resp = MeasureElapsed(Stopwatch.GetTimestamp(), sourceNode.ExecuteAsync(migrating).GetAwaiter(), "SETSLOT_MIGRATING", ref setSlotElapsed, opts.Verbose, logger);

                    getkeysinslot[3] = MeasureElapsed(Stopwatch.GetTimestamp(), sourceNode.ExecuteAsync(countkeysinslot).GetAwaiter(), "COUNTKEYSINSLOT", ref countKeysElapsed, opts.Verbose, logger);
                    var keys = MeasureElapsed(Stopwatch.GetTimestamp(), sourceNode.ExecuteForArrayAsync(getkeysinslot).GetAwaiter(), "GETKEYSINSLOT", ref getKeysElapsed, opts.Verbose, logger);

                    ICollection<string> migrate = ["MIGRATE", targetNodeEndpoint.Address.ToString(), targetNodeEndpoint.Port.ToString(), "", "0", timeout.ToString(), "REPLACE", "KEYS"];
                    foreach (var key in keys)
                        migrate.Add(key);

                    var request = migrate.ToArray();
                    resp = MeasureElapsed(Stopwatch.GetTimestamp(), sourceNode.ExecuteAsync(request).GetAwaiter(), $"MIGRATE ({slot}, {keys.Length})", ref migrateKeysElapsed, opts.Verbose, logger);

                    resp = MeasureElapsed(Stopwatch.GetTimestamp(), targetNode.ExecuteAsync(node).GetAwaiter(), "NODE_TARGET", ref setSlotElapsed, opts.Verbose, logger);
                    resp = MeasureElapsed(Stopwatch.GetTimestamp(), sourceNode.ExecuteAsync(node).GetAwaiter(), "NODE_SOURCE", ref setSlotElapsed, opts.Verbose, logger);
                }

                var elapsed = Stopwatch.GetTimestamp() - startTimestamp;
                var t = TimeSpan.FromTicks(elapsed);
                logger?.LogInformation("KEYS Elapsed Time: {elapsed} seconds", t.TotalSeconds);
                logger?.LogInformation("SetSlot Elapsed Time: {elapsed} seconds", TimeSpan.FromTicks(setSlotElapsed).TotalSeconds);
                logger?.LogInformation("CountKeys Elapsed Time: {elapsed} seconds", TimeSpan.FromTicks(countKeysElapsed).TotalSeconds);
                logger?.LogInformation("GetKeys Elapsed Time: {elapsed} seconds", TimeSpan.FromTicks(getKeysElapsed).TotalSeconds);
                logger?.LogInformation("MigrateKeys Elapsed Time: {elapsed} seconds", TimeSpan.FromTicks(migrateKeysElapsed).TotalSeconds);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error at MigrateKeys");
            }
        }
    }
}