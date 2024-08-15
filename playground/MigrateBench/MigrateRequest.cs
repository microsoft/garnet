// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System.Diagnostics;
using Garnet.client;
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

        public MigrateRequest(Options opts, ILogger logger = null)
        {
            this.opts = opts;
            var sourceEndpoint = opts.SourceEndpoint.Split(':');
            var targetEndpoint = opts.TargetEndpoint.Split(':');
            sourceAddress = sourceEndpoint[0];
            sourcePort = int.Parse(sourceEndpoint[1]);
            targetAddress = targetEndpoint[0];
            targetPort = int.Parse(targetEndpoint[1]);

            sourceNode = new(sourceAddress, sourcePort, bufferSize: 1 << 22);
            targetNode = new(targetAddress, targetPort, bufferSize: 1 << 22);
            this.logger = logger;
        }

        public void Run()
        {
            try
            {
                sourceNode.Connect();
                targetNode.Connect();
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
                    var resp = c.ExecuteAsync("dbsize").GetAwaiter().GetResult();
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
                if ((slots.Length & 0x1) > 0)
                {
                    logger?.LogError("Malformed SLOTRANGES input; please provide pair of ranges");
                    return;
                }

                // migrate 192.168.1.20 7001 "" 0 5000 SLOTSRANGE 1000 7000            
                ICollection<string> migrate = ["MIGRATE", targetAddress, targetPort.ToString(), "", "0", timeout.ToString(), "REPLACE", "SLOTSRANGE"];
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
                logger?.LogInformation("SlotsRange Elapsed Time: {elapsed} seconds", t.TotalSeconds);
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
                var slots = opts.Slots.ToArray();
                if ((slots.Length & 0x1) > 0)
                {
                    logger?.LogError("Malformed SLOTSRANGES input; please provide pair of ranges");
                    return;
                }

                var _slots = new List<int>();
                for (var i = 0; i < slots.Length; i += 2)
                {
                    var startSlot = slots[i];
                    var endSlot = slots[i + 1];
                    for (var j = startSlot; j <= endSlot; j++)
                        _slots.Add(j);
                }

                ICollection<string> migrate = ["MIGRATE", targetAddress, targetPort.ToString(), "", "0", timeout.ToString(), "REPLACE", "SLOTS"];
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
                logger?.LogInformation("SlotsRange Elapsed Time: {elapsed} seconds", t.TotalSeconds);
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
                var slots = opts.Slots.ToArray();
                if ((slots.Length & 0x1) > 0)
                {
                    logger?.LogError("Malformed SLOTSRANGES input; please provide pair of ranges");
                    return;
                }

                var _slots = new List<int>();
                for (var i = 0; i < slots.Length; i += 2)
                {
                    var startSlot = slots[i];
                    var endSlot = slots[i + 1];
                    for (var j = startSlot; j < endSlot; j++)
                        _slots.Add(j);
                }

                var sourceNodeId = sourceNode.ExecuteAsync("cluster", "myid").GetAwaiter().GetResult();
                var targetNodeId = targetNode.ExecuteAsync("cluster", "myid").GetAwaiter().GetResult();

                string[] migrating = ["cluster", "setslot", "<slot>", "migrating", targetNodeId];
                string[] importing = ["cluster", "setslot", "<slot>", "importing", sourceNodeId];
                string[] node = ["cluster", "setslot", "<slot>", "node", targetNodeId];
                string[] countkeysinslot = ["cluster", "countkeysinslot", "<slot>"];
                string[] getkeysinslot = ["cluster", "getkeysinslot", "<slot>"];
                foreach (var slot in _slots)
                {
                    var slotStr = slot.ToString();
                    migrating[2] = slotStr;
                    importing[2] = slotStr;
                    node[2] = slotStr;
                    countkeysinslot[2] = slotStr;
                    getkeysinslot[2] = slotStr;

                    var resp = sourceNode.ExecuteAsync(migrating).GetAwaiter().GetResult();
                    resp = targetNode.ExecuteAsync(importing).GetAwaiter().GetResult();
                    resp = sourceNode.ExecuteAsync(countkeysinslot).GetAwaiter().GetResult();
                    var keys = sourceNode.ExecuteForArrayAsync(getkeysinslot).GetAwaiter().GetResult();

                    ICollection<string> migrate = ["MIGRATE", targetAddress, targetPort.ToString(), "", "0", timeout.ToString(), "REPLACE", "KEYS"];
                    foreach (var key in keys)
                        migrate.Add(key);

                    var request = migrate.ToArray();
                    logger?.LogInformation("KeyCount: {keys}", keys.Length);
                    resp = sourceNode.ExecuteAsync(request).GetAwaiter().GetResult();

                    resp = targetNode.ExecuteAsync(node).GetAwaiter().GetResult();
                    resp = sourceNode.ExecuteAsync(node).GetAwaiter().GetResult();
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error at MigrateKeys");
            }
        }
    }
}
