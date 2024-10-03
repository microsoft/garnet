// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.client;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace MigrateBench
{
    struct Node
    {
        public GarnetClientSession gc;
        public string nodeId;
        public string address;
        public string port;
    }

    public class MigrateSlotWalk
    {
        readonly Options opts;
        readonly ILogger logger;

        Node[] nodes;

        public MigrateSlotWalk(Options opts, ILogger logger = null)
        {
            this.opts = opts;

            var endpoints = opts.Endpoints.ToArray();

            if (endpoints.Length != 3)
                throw new Exception("Exactly 3 nodes are needed for this scenario");

            nodes = new Node[endpoints.Length];
            for (var i = 0; i < nodes.Length; i++)
            {
                var endpoint = endpoints[i].Split(':');
                nodes[i].address = endpoint[0];
                nodes[i].port = endpoint[1];
                nodes[i].gc = new GarnetClientSession(nodes[i].address, int.Parse(nodes[i].port), new NetworkBufferSettings(1 << 22));
            }

            this.logger = logger;
        }

        public void Run()
        {
            try
            {
                for (var i = 0; i < nodes.Length; i++)
                {
                    nodes[i].gc.Connect();
                    nodes[i].nodeId = nodes[i].gc.ExecuteAsync("cluster", "myid").GetAwaiter().GetResult();
                }

                string[] importing = ["cluster", "setslot", "<slot>", "importing", "<source-node-id>"];
                string[] migrating = ["cluster", "setslot", "<slot>", "migrating", "<target-node-id>"];
                string[] node = ["cluster", "setslot", "<slot>", "node", "<target-node-id>"];
                string[] countkeysinslot = ["cluster", "countkeysinslot", "<slot>"];
                string[] getkeysinslot = ["cluster", "getkeysinslot", "<slot>", "<count>"];

                var _slots = Common.GetSingleSlots(opts, logger);
                if (_slots == null) return;

                foreach (var slot in _slots)
                {
                    var _src = 0;
                    var _tgt = 1;

                    importing[2] = slot.ToString();
                    migrating[2] = slot.ToString();
                    node[2] = slot.ToString();
                    countkeysinslot[2] = slot.ToString();
                    getkeysinslot[2] = slot.ToString();


                    while (_tgt < nodes.Length)
                    {
                        importing[4] = nodes[_src].nodeId;
                        migrating[4] = nodes[_tgt].nodeId;
                        node[4] = nodes[_tgt].nodeId;

                        // n1 IMPORTING slot
                        logger?.LogInformation("{command}", string.Join(" ", importing));

                        var resp = "";
                        // wait until import succeeds because node might not know that I am the owner yet
                        while (true)
                        {
                            try
                            {
                                resp = nodes[_tgt].gc.ExecuteAsync(importing).GetAwaiter().GetResult();
                                break;
                            }
                            catch
                            {
                                Thread.Yield();
                            }
                        }

                        logger?.LogInformation("{command}", string.Join(" ", migrating));
                        // n0 MIGRATING slot                    
                        resp = nodes[_src].gc.ExecuteAsync(migrating).GetAwaiter().GetResult();

                        getkeysinslot[3] = nodes[_src].gc.ExecuteAsync(countkeysinslot).GetAwaiter().GetResult();

                        if (int.Parse(getkeysinslot[3]) > 0)
                        {
                            var keys = nodes[_src].gc.ExecuteForArrayAsync(getkeysinslot).GetAwaiter().GetResult();

                            ICollection<string> migrate = ["MIGRATE", nodes[_tgt].address, nodes[_tgt].port, "", "0", "10000", "REPLACE", "KEYS"];
                            foreach (var key in keys)
                                migrate.Add(key);

                            nodes[_src].gc.ExecuteAsync([.. migrate]).GetAwaiter();
                        }

                        logger?.LogInformation("{command}", string.Join(" ", node));
                        // n1 NODE SLOT n1
                        resp = nodes[_tgt].gc.ExecuteAsync(node).GetAwaiter().GetResult();
                        // n0 NODE SLOT n1
                        resp = nodes[_src].gc.ExecuteAsync(node).GetAwaiter().GetResult();

                        _src++;
                        _tgt++;
                    }
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "MigrateSlotWalk.Run");
            }
            finally
            {
                for (var i = 0; i < nodes.Length; i++)
                    nodes[i].gc.Dispose();
            }
        }
    }
}