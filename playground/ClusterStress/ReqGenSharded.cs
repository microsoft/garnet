// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;

namespace Resp.benchmark
{
    public unsafe partial class ReqGen
    {
        private void GenerateRandomKeys()
        {
            for (int i = 0; i < DbSize; i++)
            {
                int key = Start + keyRandomGen.Next(DbSize);
                int slot = Garnet.common.NumUtils.HashSlot(System.Text.Encoding.ASCII.GetBytes(key.ToString()));
                databaseKeys[slot].Add(key);
            }
        }

        private void ValidateKeysPerSlot(int keysPerSlot)
        {
            foreach (var keys in databaseKeys)
            {
                if (keys.Count != keysPerSlot)
                    throw new Exception($"keysPerSlot not assigned {keysPerSlot} {keys.Count}");
            }
        }

        private void GenerateKeysCoverAllSlots()
        {
            int slotCount = 16384;
            int keysPerSlot = ((DbSize - 1) / slotCount) + 1;

            for (int i = 0; i < DbSize; i++)
            {
            retry:
                int key = Start + keyRandomGen.Next();
                int slot = Garnet.common.NumUtils.HashSlot(System.Text.Encoding.ASCII.GetBytes(key.ToString()));

                if (databaseKeys[slot].Count < keysPerSlot)
                    databaseKeys[slot].Add(key);
                else
                    goto retry;
            }
            ValidateKeysPerSlot(keysPerSlot);
        }

        public void GenerateShardedKeys()
        {
            InitializeRNG();
            currSlot = 0;
            currKeyInSlot = 0;
            if (DbSize > (1 << 20))
                GenerateRandomKeys();
            else
                GenerateKeysCoverAllSlots();
        }

        /// <summary>
        /// Create a map of interleaved slots. Used to load data across shards equally independent of db size
        /// </summary>
        private void InitInterleaveSlots()
        {
            var pNodes = clusterConfig.Nodes.ToList().FindAll(p => !p.IsReplica && p.Slots.Count > 0).ToArray();

            LinkedList<int>[] shardSlots = new LinkedList<int>[pNodes.Length];
            for (int i = 0; i < shardSlots.Length; i++)
            {
                shardSlots[i] = new LinkedList<int>();
                var slotRanges = pNodes[i].Slots;
                foreach (var slotRange in slotRanges)
                {
                    for (int j = slotRange.From; j <= slotRange.To; j++)
                        shardSlots[i].AddLast(j);
                }
            }

            int k = shard == -1 ? 0 : shard;
            for (int i = 0; i < interleavedSlots.Length; i++)
            {
                interleavedSlots[i] = shardSlots[k].First();
                shardSlots[k].RemoveFirst();
                shardSlots[k].AddLast(interleavedSlots[i]);
                k = shard == -1 ? (k + 1 < shardSlots.Length ? k + 1 : 0) : (shard);
            }
        }
    }
}