// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.client;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace MigrateBench
{
    public class MigrateSlotWalk
    {
        readonly Options opts;
        readonly ILogger logger;

        GarnetClientSession n0;
        GarnetClientSession n1;
        GarnetClientSession n2;

        public MigrateSlotWalk(Options opts, ILogger logger = null)
        {
            this.opts = opts;

            var endpoints = opts.Endpoints.ToArray();

            if (endpoints.Length != 3)
                throw new Exception("Exactly 3 nodes are needed for this scenario");

            var endpoint = endpoints[0].Split(':');
            n0 = new GarnetClientSession(endpoint[0], int.Parse(endpoint[1]), new NetworkBufferSettings(1 << 22));
            endpoint = endpoints[1].Split(':');
            n1 = new GarnetClientSession(endpoint[0], int.Parse(endpoint[1]), new NetworkBufferSettings(1 << 22));
            endpoint = endpoints[2].Split(':');
            n2 = new GarnetClientSession(endpoint[0], int.Parse(endpoint[1]), new NetworkBufferSettings(1 << 22));

            this.logger = logger;
        }

        public void Run()
        {
            try
            {
                n0.Connect();
                n1.Connect();
                n2.Connect();

                var n0Id = n0.ExecuteAsync("cluster", "myid").GetAwaiter().GetResult();
                var n1Id = n1.ExecuteAsync("cluster", "myid").GetAwaiter().GetResult();
                var n2Id = n2.ExecuteAsync("cluster", "myid").GetAwaiter().GetResult();

                string[] importing = ["cluster", "setslot", "<slot>", "importing", "<source-node-id>"];
                string[] migrating = ["cluster", "setslot", "<slot>", "migrating", "<target-node-id>"];
                string[] node = ["cluster", "setslot", "<slot>", "node", "<target-node-id>"];

                var _slots = Common.GetSingleSlots(opts, logger);
                if (_slots == null) return;

                foreach (var slot in _slots)
                {
                    /*
                     * Migrate from n0 to n1
                     */
                    importing[2] = slot.ToString(); importing[4] = n0Id;
                    migrating[2] = slot.ToString(); migrating[4] = n1Id;
                    node[2] = slot.ToString(); node[4] = n1Id;

                    // n1 IMPORTING slot
                    logger?.LogInformation("{command}", string.Join(" ", importing));
                    var resp = "";
                    // wait until import succeeds because node might not know that I am the owner yet
                    while (true)
                    {
                        try
                        {
                            resp = n1.ExecuteAsync(importing).GetAwaiter().GetResult();
                            break;
                        }
                        catch
                        {
                            Thread.Yield();
                        }
                    }

                    logger?.LogInformation("{command}", string.Join(" ", migrating));
                    // n0 MIGRATING slot                    
                    resp = n0.ExecuteAsync(migrating).GetAwaiter().GetResult();
                    logger?.LogInformation("{command}", string.Join(" ", node));
                    // n1 NODE SLOT n1
                    resp = n1.ExecuteAsync(node).GetAwaiter().GetResult();
                    // n0 NODE SLOT n1
                    resp = n0.ExecuteAsync(node).GetAwaiter().GetResult();

                    /*
                     * Migrate from n1 to n2
                     */
                    importing[2] = slot.ToString(); importing[4] = n1Id;
                    migrating[2] = slot.ToString(); migrating[4] = n2Id;
                    node[2] = slot.ToString(); node[4] = n2Id;

                    logger?.LogInformation("{command}", string.Join(" ", importing));
                    // wait until import succeeds because node might not know that I am the owner yet
                    while (true)
                    {
                        try
                        {
                            resp = n2.ExecuteAsync(importing).GetAwaiter().GetResult();
                            break;
                        }
                        catch
                        {
                            Thread.Yield();
                        }
                    }

                    logger?.LogInformation("{command}", string.Join(" ", migrating));
                    // n1 MIGRATING slot                    
                    resp = n1.ExecuteAsync(migrating).GetAwaiter().GetResult();
                    logger?.LogInformation("{command}", string.Join(" ", node));
                    // n2 NODE SLOT n2
                    resp = n2.ExecuteAsync(node).GetAwaiter().GetResult();
                    // n1 NODE SLOT n2
                    resp = n1.ExecuteAsync(node).GetAwaiter().GetResult();
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "MigrateSlotWalk.Run");
            }
            finally
            {
                n0.Dispose();
                n1.Dispose();
                n2.Dispose();
            }
        }
    }
}
