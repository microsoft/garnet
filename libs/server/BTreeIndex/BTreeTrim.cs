// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server.BTreeIndex
{
    public unsafe partial class BTree
    {
        // ─── Trim semantics ───────────────────────────────────────────────────────
        // The previous structural trim (which physically removed leaves and rebalanced
        // internal nodes) had multiple bugs that left dangling child pointers in the
        // root, crashing the test host on Dispose. This implementation tombstones in
        // place: it walks leaves from `head` forward, flips Value.Valid to false, and
        // decrements per-leaf and global valid counters. Reads (BTree.Get / LastAlive)
        // already filter on Value.Valid, so XLEN/XRANGE/XLAST observe the correct
        // post-trim state.
        //
        // Trade-offs of tombstone-only trim:
        //   • Memory: tombstoned leaves stay allocated. Reclamation would require a
        //     correct restructuring trim (TODO).
        //   • Disk: callers must NOT TruncateUntil the underlying log past tombstoned
        //     addresses, since those addresses are still referenced by BTree.Get's
        //     `startVal`/`tombstones` outputs on subsequent range reads.
        //   • Internal-node validCount is left stale. It's only consumed by this trim
        //     code path itself; everywhere else (Length, range scans, LastAlive, Delete)
        //     uses leaf-level validCount or stats.numValidKeys, both of which we update.
        //
        // The signatures match the previous TrimByLength / TrimByID for drop-in use.
        // ──────────────────────────────────────────────────────────────────────────

        /// <summary>
        /// Tombstone the smallest entries until at most <paramref name="length"/> valid
        /// entries remain.
        /// </summary>
        public void TrimByLength(ulong length, out ulong entriesTrimmed, out Value headValue, out ReadOnlySpan<byte> headValidKey, out uint numLeavesDeleted, bool approximateTrimming = false)
        {
            entriesTrimmed = 0;
            numLeavesDeleted = 0;

            if (length >= stats.numValidKeys)
            {
                FindFirstAlive(out headValue, out headValidKey);
                return;
            }

            ulong toRemove = stats.numValidKeys - length;
            BTreeNode* leaf = head;

            while (leaf != null && toRemove > 0)
            {
                if (approximateTrimming && leaf != tail && leaf->info->validCount <= toRemove)
                {
                    // Whole-leaf tombstone path: faster, but trims to a leaf boundary
                    // rather than the exact requested count.
                    var beforeValid = leaf->info->validCount;
                    int n = leaf->info->count;
                    for (int i = 0; i < n; i++)
                    {
                        if (leaf->data.values[i].Valid)
                            leaf->data.values[i].Valid = false;
                    }
                    leaf->info->validCount = 0;
                    stats.numValidKeys -= beforeValid;
                    entriesTrimmed += beforeValid;
                    toRemove -= beforeValid;
                    leaf = leaf->info->next;
                    continue;
                }

                int count = leaf->info->count;
                for (int i = 0; i < count && toRemove > 0; i++)
                {
                    if (!leaf->data.values[i].Valid) continue;
                    leaf->data.values[i].Valid = false;
                    leaf->info->validCount--;
                    stats.numValidKeys--;
                    entriesTrimmed++;
                    toRemove--;
                }

                if (toRemove > 0)
                    leaf = leaf->info->next;
            }

            FindFirstAlive(out headValue, out headValidKey);
        }

        /// <summary>
        /// Tombstone every entry whose key is strictly less than <paramref name="key"/>.
        /// </summary>
        public void TrimByID(byte* key, out ulong entriesTrimmed, out Value headValue, out ReadOnlySpan<byte> headValidKey, out uint numLeavesDeleted)
        {
            entriesTrimmed = 0;
            numLeavesDeleted = 0;

            BTreeNode* leaf = head;
            while (leaf != null)
            {
                int count = leaf->info->count;
                for (int i = 0; i < count; i++)
                {
                    // Keys within a leaf are sorted ascending, so the first key >= cutoff
                    // ends the scan for this whole subtree (since later leaves have larger keys).
                    if (BTreeNode.Compare(leaf->GetKey(i), key) >= 0)
                    {
                        FindFirstAlive(out headValue, out headValidKey);
                        return;
                    }
                    if (!leaf->data.values[i].Valid) continue;
                    leaf->data.values[i].Valid = false;
                    leaf->info->validCount--;
                    stats.numValidKeys--;
                    entriesTrimmed++;
                }
                leaf = leaf->info->next;
            }

            FindFirstAlive(out headValue, out headValidKey);
        }

        /// <summary>
        /// Walk leaves from <see cref="head"/> forward and return the first non-tombstoned
        /// (key, value). Used to identify the new "head valid entry" after a trim.
        /// </summary>
        void FindFirstAlive(out Value headValue, out ReadOnlySpan<byte> headValidKey)
        {
            headValue = default;
            headValidKey = default;

            BTreeNode* leaf = head;
            while (leaf != null)
            {
                int count = leaf->info->count;
                for (int i = 0; i < count; i++)
                {
                    if (leaf->data.values[i].Valid)
                    {
                        headValue = leaf->data.values[i];
                        headValidKey = new ReadOnlySpan<byte>(leaf->GetKey(i), BTreeNode.KEY_SIZE);
                        return;
                    }
                }
                leaf = leaf->info->next;
            }
        }
    }
}