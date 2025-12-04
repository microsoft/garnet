// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace Resp.benchmark
{
    public unsafe partial class OnlineReqGen
    {
        int[] slotPrefixes;

        public OnlineReqGen(int thread_id, int DbSize, bool randomGen = true, bool zipf = false, int keyLen = default, int valueLen = default, int objectDbSize = -1, bool cluster = true)
        {
            this.randomGen = randomGen;
            this.DbSize = DbSize;
            this.zipf = zipf;

            this.keyLen = Math.Max(NumUtils.NumDigits(DbSize), keyLen);
            this.valueLen = valueLen == default ? 8 : valueLen;
            valueBuffer = new byte[this.valueLen];
            keyBuffer = GC.AllocateArray<byte>(this.keyLen, true);
            keyBufferPtr = (byte*)Unsafe.AsPointer(ref keyBuffer[0]);

            InitializeRNGCluster(31337 + thread_id, 41337 + thread_id);
        }

        private void InitializeRNGCluster(int keySeed = -1, int valueSeed = -1)
        {
            if (zipf)
                zipfg = new ZipfGenerator(new RandomGenerator(), DbSize, 0.99);
            keyRandomGen = keySeed == -1 ? new Random(Guid.NewGuid().GetHashCode()) : new Random(keySeed);
            valueRandomGen = valueSeed == -1 ? new Random(Guid.NewGuid().GetHashCode()) : new Random(valueSeed);

            GenerateCRCPrefixesForAllSlots();
        }

        private void GenerateCRCPrefixesForAllSlots()
        {
            HashSet<int> slots = new();
            for (int i = 0; i < 16384; i++)
                slots.Add(i);
            slotPrefixes = new int[16384];
            while (slots.Count > 0)
            {
                int keyPrefix = keyRandomGen.Next(0, int.MaxValue);
                int slot = Garnet.common.HashSlotUtils.HashSlot(Encoding.ASCII.GetBytes(keyPrefix.ToString()));
                if (slots.Contains(slot))
                {
                    slotPrefixes[slot] = keyPrefix;
                    slots.Remove(slot);
                }
            }
        }

        private byte[] GetClusterKeyBytes(int key)
        {
            string keyStr = "{" + key.ToString() + "}";
            return Encoding.ASCII.GetBytes(keyStr.PadRight(keyLen, 'X'));
        }

        public byte[] GenerateKeyBytes(out int slot)
        {
            int key = randomGen ? (zipf ? zipfg.Next() : keyRandomGen.Next(DbSize)) : (keyIndex++ % DbSize);
            slot = Garnet.common.HashSlotUtils.HashSlot(Encoding.ASCII.GetBytes(key.ToString()));
            byte[] keyBytes = GetClusterKeyBytes(key);
#if DEBUG
            int _slot = Garnet.common.HashSlotUtils.HashSlot(keyBytes);
            System.Diagnostics.Debug.Assert(_slot == slot, $"GenerateKeyBytes slot number incosistence {_slot}:{slot}");
#endif
            return keyBytes;
        }

        public string GenerateKey(out int slot)
        {
            int key = randomGen ? (zipf ? zipfg.Next() : keyRandomGen.Next(DbSize)) : (keyIndex++ % DbSize);
            slot = Garnet.common.HashSlotUtils.HashSlot(Encoding.ASCII.GetBytes(key.ToString()));
            byte[] keyBytes = GetClusterKeyBytes(key);
#if DEBUG
            int _slot = Garnet.common.HashSlotUtils.HashSlot(keyBytes);
            System.Diagnostics.Debug.Assert(_slot == slot, $"GenerateKeyBytes slot number incosistence {_slot}:{slot}");
#endif
            return Encoding.ASCII.GetString(keyBytes);
        }

        public string GenerateKeyInSlot(out int slot)
        {
            slot = (randomGen ? (zipf ? zipfg.Next() : keyRandomGen.Next(DbSize)) : (keyIndex++ % DbSize)) & 16383;
            string keyStr = "{" + slotPrefixes[slot] + "}" + PadRandom(keyLen);
            return keyStr;
        }
    }
}