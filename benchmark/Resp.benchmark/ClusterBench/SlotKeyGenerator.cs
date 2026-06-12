// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using Garnet.common;

namespace Resp.benchmark
{
    /// <summary>
    /// Generates keys that route to a specific shard using Redis hash-tag mechanism.
    /// Finds a single tag that maps to any slot in the shard, then varies the suffix
    /// outside the {} to produce unique keys that all route to the same shard.
    /// </summary>
    public class SlotKeyGenerator
    {
        /// <summary>
        /// A single hash-tag string whose CRC16 mod 16384 lands in one of the shard's assigned slots.
        /// </summary>
        private readonly string tag;

        /// <summary>
        /// Key length (total bytes including hash-tag wrapper).
        /// </summary>
        private readonly int keyLen;

        /// <summary>
        /// The prefix "{tag}" that all generated keys start with.
        /// </summary>
        private readonly string keyPrefix;

        public SlotKeyGenerator(ShardInfo shard, int keyLen)
        {
            this.keyLen = Math.Max(keyLen, 8);
            this.tag = FindTagForShard(shard);
            this.keyPrefix = $"{{{tag}}}";
        }

        /// <summary>
        /// Generate a key that routes to this shard.
        /// Format: {tag}keyIndex padded to keyLen.
        /// </summary>
        public string GenerateKey(Random rng, int keyIndex = -1)
        {
            var idx = keyIndex >= 0 ? keyIndex : rng.Next(int.MaxValue);
            var sb = new StringBuilder(keyLen);
            sb.Append(keyPrefix);
            sb.Append(idx);

            while (sb.Length < keyLen)
                sb.Append('X');

            if (sb.Length > keyLen)
                return sb.ToString(0, keyLen);

            return sb.ToString();
        }

        /// <summary>
        /// Find a short ASCII tag that hashes to any slot owned by this shard.
        /// With a typical shard owning thousands of slots, a 1-char tag is almost always found instantly.
        /// </summary>
        private static unsafe string FindTagForShard(ShardInfo shard)
        {
            byte* buf = stackalloc byte[3];

            // Try 1-char tags (92 candidates, shard typically owns ~5000 slots — instant hit)
            for (byte c = 33; c < 127; c++)
            {
                if (c is (byte)'{' or (byte)'}') continue;
                buf[0] = c;
                if (shard.OwnsSlot(HashSlotUtils.HashSlot(buf, 1)))
                    return ((char)c).ToString();
            }

            // Try 2-char tags (8,464 combos — guaranteed for any shard with slots)
            for (byte c1 = 33; c1 < 127; c1++)
            {
                if (c1 is (byte)'{' or (byte)'}') continue;
                buf[0] = c1;
                for (byte c2 = 33; c2 < 127; c2++)
                {
                    if (c2 is (byte)'{' or (byte)'}') continue;
                    buf[1] = c2;
                    if (shard.OwnsSlot(HashSlotUtils.HashSlot(buf, 2)))
                        return $"{(char)c1}{(char)c2}";
                }
            }

            // Try 3-char tags (fallback)
            for (byte c1 = 33; c1 < 127; c1++)
            {
                if (c1 is (byte)'{' or (byte)'}') continue;
                buf[0] = c1;
                for (byte c2 = 33; c2 < 127; c2++)
                {
                    if (c2 is (byte)'{' or (byte)'}') continue;
                    buf[1] = c2;
                    for (byte c3 = 33; c3 < 127; c3++)
                    {
                        if (c3 is (byte)'{' or (byte)'}') continue;
                        buf[2] = c3;
                        if (shard.OwnsSlot(HashSlotUtils.HashSlot(buf, 3)))
                            return $"{(char)c1}{(char)c2}{(char)c3}";
                    }
                }
            }

            throw new Exception($"Failed to find a hash-tag for shard {shard}");
        }
    }
}
