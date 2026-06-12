// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using Garnet.common;

namespace Resp.benchmark
{
    /// <summary>
    /// Generates keys that hash to specific slots using Redis hash-tag mechanism.
    /// Pre-computes a tag for each slot in the assigned range so key generation is O(1).
    /// </summary>
    public class SlotKeyGenerator
    {
        /// <summary>
        /// Precomputed hash tags: slotTags[slot] = a short string whose CRC16 mod 16384 == slot.
        /// Only populated for assigned slots.
        /// </summary>
        private readonly string[] slotTags;

        /// <summary>
        /// Flat array of assigned slots for random selection.
        /// </summary>
        private readonly ushort[] assignedSlots;

        /// <summary>
        /// Key length (total bytes including hash-tag wrapper).
        /// </summary>
        private readonly int keyLen;

        public int AssignedSlotCount => assignedSlots.Length;

        public SlotKeyGenerator(ShardInfo shard, int keyLen)
        {
            this.keyLen = Math.Max(keyLen, 8); // Minimum key length to accommodate hash-tag
            slotTags = new string[16384];
            var slotList = new List<ushort>();

            foreach (var (start, end) in shard.SlotRanges)
            {
                for (int slot = start; slot <= end; slot++)
                {
                    slotTags[slot] = FindTagForSlot((ushort)slot);
                    slotList.Add((ushort)slot);
                }
            }

            assignedSlots = slotList.ToArray();
        }

        /// <summary>
        /// Generate a key that hashes to a randomly selected assigned slot.
        /// Format: {tag}XXXX... where tag determines the slot and X is padding.
        /// </summary>
        /// <param name="rng">Random number generator for slot selection</param>
        /// <param name="keyIndex">Optional sequential key index for deterministic suffix</param>
        public string GenerateKey(Random rng, int keyIndex = -1)
        {
            var slot = assignedSlots[rng.Next(assignedSlots.Length)];
            return GenerateKeyForSlot(slot, keyIndex);
        }

        /// <summary>
        /// Generate a key that hashes to a specific slot.
        /// </summary>
        public string GenerateKeyForSlot(ushort slot, int keyIndex = -1)
        {
            var tag = slotTags[slot];
            if (tag == null)
                throw new ArgumentException($"Slot {slot} is not assigned to this shard.");

            // Build key: {tag}suffix
            var sb = new StringBuilder(keyLen);
            sb.Append('{');
            sb.Append(tag);
            sb.Append('}');

            if (keyIndex >= 0)
            {
                var indexStr = keyIndex.ToString();
                sb.Append(indexStr);
            }

            // Pad to desired key length
            while (sb.Length < keyLen)
                sb.Append('X');

            // Truncate if over (shouldn't happen with reasonable keyLen)
            if (sb.Length > keyLen)
                return sb.ToString(0, keyLen);

            return sb.ToString();
        }

        /// <summary>
        /// Generate key bytes for a randomly selected assigned slot.
        /// </summary>
        public byte[] GenerateKeyBytes(Random rng, int keyIndex = -1)
        {
            return Encoding.ASCII.GetBytes(GenerateKey(rng, keyIndex));
        }

        /// <summary>
        /// Get a random slot from the assigned slots.
        /// </summary>
        public ushort GetRandomSlot(Random rng)
        {
            return assignedSlots[rng.Next(assignedSlots.Length)];
        }

        /// <summary>
        /// Get all assigned slots.
        /// </summary>
        public ushort[] GetAssignedSlots() => assignedSlots;

        /// <summary>
        /// Find a short ASCII string whose CRC16 mod 16384 equals the target slot.
        /// Uses brute-force search over 1-4 character combinations.
        /// </summary>
        private static unsafe string FindTagForSlot(ushort targetSlot)
        {
            // Try 1-char, 2-char, 3-char, then 4-char tags
            Span<byte> buf = stackalloc byte[4];

            // Single character (covers 0-255 at most)
            for (byte c = 33; c < 127; c++) // Printable ASCII excluding '{' and '}'
            {
                if (c == '{' || c == '}') continue;
                buf[0] = c;
                fixed (byte* ptr = buf)
                {
                    if (HashSlotUtils.HashSlot(ptr, 1) == targetSlot)
                        return ((char)c).ToString();
                }
            }

            // Two characters
            for (byte c1 = 33; c1 < 127; c1++)
            {
                if (c1 == '{' || c1 == '}') continue;
                buf[0] = c1;
                for (byte c2 = 33; c2 < 127; c2++)
                {
                    if (c2 == '{' || c2 == '}') continue;
                    buf[1] = c2;
                    fixed (byte* ptr = buf)
                    {
                        if (HashSlotUtils.HashSlot(ptr, 2) == targetSlot)
                            return $"{(char)c1}{(char)c2}";
                    }
                }
            }

            // Three characters (should cover all 16384 slots)
            for (byte c1 = 33; c1 < 127; c1++)
            {
                if (c1 == '{' || c1 == '}') continue;
                buf[0] = c1;
                for (byte c2 = 33; c2 < 127; c2++)
                {
                    if (c2 == '{' || c2 == '}') continue;
                    buf[1] = c2;
                    for (byte c3 = 33; c3 < 127; c3++)
                    {
                        if (c3 == '{' || c3 == '}') continue;
                        buf[2] = c3;
                        fixed (byte* ptr = buf)
                        {
                            if (HashSlotUtils.HashSlot(ptr, 3) == targetSlot)
                                return $"{(char)c1}{(char)c2}{(char)c3}";
                        }
                    }
                }
            }

            // Four characters (fallback, guaranteed to find)
            for (byte c1 = 33; c1 < 127; c1++)
            {
                if (c1 == '{' || c1 == '}') continue;
                buf[0] = c1;
                for (byte c2 = 33; c2 < 127; c2++)
                {
                    if (c2 == '{' || c2 == '}') continue;
                    buf[1] = c2;
                    for (byte c3 = 33; c3 < 127; c3++)
                    {
                        if (c3 == '{' || c3 == '}') continue;
                        buf[2] = c3;
                        for (byte c4 = 33; c4 < 127; c4++)
                        {
                            if (c4 == '{' || c4 == '}') continue;
                            buf[3] = c4;
                            fixed (byte* ptr = buf)
                            {
                                if (HashSlotUtils.HashSlot(ptr, 4) == targetSlot)
                                    return $"{(char)c1}{(char)c2}{(char)c3}{(char)c4}";
                            }
                        }
                    }
                }
            }

            throw new Exception($"Failed to find a hash-tag for slot {targetSlot}");
        }
    }
}
