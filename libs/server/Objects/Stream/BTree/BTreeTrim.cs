// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Garnet.server.BTreeIndex
{
    public unsafe partial class BTree
    {
        // ─── Trim semantics ───────────────────────────────────────────────────────
        // XTRIM always drops the OLDEST entries — a contiguous prefix of both the key
        // order and the log-address order, since stream IDs and log addresses both grow
        // monotonically. Trim therefore proceeds in two passes:
        //
        //   1. Tombstone pass (TrimByLength / TrimByID): walk leaves from `head` forward,
        //      flip Value.Valid to false, and decrement per-leaf validCount and the global
        //      stats.numValidKeys. Reads (BTree.Get / FirstAlive / LastAlive) already filter
        //      on Value.Valid, so XLEN/XRANGE observe the correct post-trim state.
        //
        //   2. Left-edge reclaim pass (ReclaimDeadHeadLeaves): free the contiguous run of
        //      whole leaves at the head that became fully tombstoned (validCount == 0), and
        //      remove them from the index via a left-spine cascade. Because trim only ever
        //      removes a left prefix, the structural change is confined to the leftmost
        //      root→head spine and removes whole subtrees — never an arbitrary mid-tree
        //      deletion. This bounds index memory under repeated XTRIM.
        //
        // What stays tombstone-only (intentional):
        //   • The boundary leaf that straddles the trim cut keeps its live entries, so it is
        //     NOT freed; its dead prefix remains as in-leaf tombstones and becomes the new
        //     `head`. This is the "internal fragmentation" we accept on the cut leaf.
        //   • Mid-stream XDEL tombstones are never compacted (Delete only tombstones).
        //   • The tail leaf is never freed.
        //
        // Dispose-safety invariant (the prior structural trim crashed here):
        //   BTree.Deallocate() → Free(root) recurses over children[0..count]. After every
        //   trim, each internal node's `count` and `children[0..count]` must be exact (all
        //   live), and the leaf linked-list + `head`/`tail`/`depth` must be consistent. All
        //   structural surgery funnels through the single RemoveLeftmostChildren primitive
        //   plus DetachLeftmostLeaf so this invariant is maintained in one place. Tests run
        //   ValidateStructure() and Deallocate() after heavy trims to guard it.
        //
        // Log interaction:
        //   StreamObjectImpl.Trim truncates the underlying log up to the new head address.
        //   Reclaiming whole dead leaves only shrinks the index; the boundary leaf's
        //   tombstones may reference addresses below the new BeginAddress, which ReadRange
        //   handles by clamping scanStart to BeginAddress before scanning.
        //
        // Internal-node validCount is left approximate (it is not decremented on trim and is
        // only ever consumed as a diagnostic — Length / range scans / FirstAlive / LastAlive /
        // Delete use leaf-level validCount or stats.numValidKeys, both of which we maintain).
        // ──────────────────────────────────────────────────────────────────────────

        /// <summary>
        /// Tombstone the smallest entries until at most <paramref name="length"/> valid
        /// entries remain, then reclaim any whole leaves that became fully tombstoned at the
        /// head of the stream.
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

            ReclaimDeadHeadLeaves(out numLeavesDeleted);
            FindFirstAlive(out headValue, out headValidKey);
        }

        /// <summary>
        /// Tombstone every entry whose key is strictly less than <paramref name="key"/>, then
        /// reclaim any whole leaves that became fully tombstoned at the head of the stream.
        /// </summary>
        public void TrimByID(byte* key, out ulong entriesTrimmed, out Value headValue, out ReadOnlySpan<byte> headValidKey, out uint numLeavesDeleted)
        {
            entriesTrimmed = 0;
            numLeavesDeleted = 0;

            BTreeNode* leaf = head;
            bool reachedCutoff = false;
            while (leaf != null && !reachedCutoff)
            {
                int count = leaf->info->count;
                for (int i = 0; i < count; i++)
                {
                    // Keys within a leaf are sorted ascending, so the first key >= cutoff
                    // ends the scan for this whole subtree (since later leaves have larger keys).
                    if (BTreeNode.Compare(leaf->GetKey(i), key) >= 0)
                    {
                        reachedCutoff = true;
                        break;
                    }
                    if (!leaf->data.values[i].Valid) continue;
                    leaf->data.values[i].Valid = false;
                    leaf->info->validCount--;
                    stats.numValidKeys--;
                    entriesTrimmed++;
                }
                if (!reachedCutoff)
                    leaf = leaf->info->next;
            }

            ReclaimDeadHeadLeaves(out numLeavesDeleted);
            FindFirstAlive(out headValue, out headValidKey);
        }

        /// <summary>
        /// Free the contiguous run of whole leaves at the head of the stream that became fully
        /// tombstoned (validCount == 0) by the preceding trim pass, removing each from the
        /// index. The boundary leaf that still holds live entries is kept (its dead prefix
        /// stays tombstoned in place), and the tail leaf is never freed.
        /// </summary>
        void ReclaimDeadHeadLeaves(out uint numLeavesDeleted)
        {
            numLeavesDeleted = 0;
            while (head != tail && head->info->validCount == 0)
            {
                RemoveLeftmostLeaf();
                numLeavesDeleted++;
            }
        }

        /// <summary>
        /// Remove the current head leaf (which must be fully tombstoned and not the tail) from
        /// both the leaf linked-list and the index, cascading through and freeing any internal
        /// node that becomes empty, then collapsing the root while it is left with a single
        /// child.
        /// </summary>
        void RemoveLeftmostLeaf()
        {
            Debug.Assert(head != tail, "never remove the tail leaf");
            Debug.Assert(head->info->validCount == 0, "only fully-dead leaves are reclaimed");
            Debug.Assert(head->info->type == BTreeNodeType.Leaf, "head must be a leaf");

            BTreeNode* dead = head;
            BTreeNode* newHead = dead->info->next;
            Debug.Assert(newHead != null, "a non-tail leaf must have a successor");

            // depth > 1 here: a single-leaf tree has head == tail, excluded by the caller, so
            // the root is internal and the dead head hangs off the leftmost root→leaf spine.
            Debug.Assert(root->info->type == BTreeNodeType.Internal, "depth must be > 1 to have a non-tail head");
            bool rootEmptied = DetachLeftmostLeaf(root, dead);
            Debug.Assert(!rootEmptied, "root retains the tail-side subtree, so it can never empty via head trim");

            // Unlink and free the dead leaf.
            head = newHead;
            newHead->info->previous = null;
            FreeLeafForTrim(dead);

            // Collapse thin root(s) left with a single child after the left spine was removed.
            while (stats.depth > 1 && root->info->type == BTreeNodeType.Internal && root->info->count == 0)
            {
                BTreeNode* oldRoot = root;
                root = root->GetChild(0);
                FreeInternalForTrim(oldRoot);
                stats.depth--;
                Debug.Assert(root == rootToTailLeaf[stats.depth - 1], "after collapse, root must equal the right-spine node at the new depth");
            }
        }

        /// <summary>
        /// Recursively remove the leftmost leaf (<paramref name="dead"/>) from the subtree
        /// rooted at the internal node <paramref name="node"/>. Drops the leftmost child slot
        /// at the lowest internal node, freeing any internal descendant that becomes empty as
        /// the removal cascades up. Returns true if <paramref name="node"/> itself becomes
        /// empty (0 children), signaling its parent to drop it. <paramref name="node"/> is
        /// never freed here — the caller (or root-collapse) handles that.
        /// </summary>
        bool DetachLeftmostLeaf(BTreeNode* node, BTreeNode* dead)
        {
            BTreeNode* child0 = node->GetChild(0);
            if (child0->info->type == BTreeNodeType.Leaf)
            {
                Debug.Assert(child0 == dead, "the leftmost leaf of the leftmost spine must be the dead head leaf");
                if (node->info->count >= 1)
                {
                    RemoveLeftmostChildren(node, 1);
                    return false;
                }
                // node had exactly one child (the dead leaf); it is now empty.
                return true;
            }

            bool childEmptied = DetachLeftmostLeaf(child0, dead);
            if (!childEmptied)
                return false;

            // child0 (an internal node) emptied: free it and drop its slot from `node`.
            FreeInternalForTrim(child0);
            if (node->info->count >= 1)
            {
                RemoveLeftmostChildren(node, 1);
                return false;
            }
            return true; // node also emptied
        }

        /// <summary>
        /// Remove the <paramref name="k"/> leftmost children (and their <paramref name="k"/>
        /// leftmost separator keys) from an internal node, shifting the survivors down.
        /// Precondition: <paramref name="node"/> is internal and 1 &lt;= k &lt;= node.count, so
        /// at least one child remains (removing the final child is handled by the caller
        /// freeing the node instead). keys[i] is the min key of children[i+1], so dropping
        /// children[0..k-1] drops separators keys[0..k-1] and keeps keys[k..count-1].
        /// </summary>
        void RemoveLeftmostChildren(BTreeNode* node, int k)
        {
            int count = node->info->count;
            Debug.Assert(node->info->type == BTreeNodeType.Internal, "RemoveLeftmostChildren expects an internal node");
            Debug.Assert(k >= 1 && k <= count, "RemoveLeftmostChildren must leave at least one child");

            int childCount = count + 1;
            int remainingChildren = childCount - k;

            // Shift children[k..count] down to [0..remainingChildren-1]. Forward iteration is
            // safe: each destination slot i (< i+k) is only ever read as a source at the
            // earlier step i-k, before it is overwritten.
            for (int i = 0; i < remainingChildren; i++)
                node->data.children[i] = node->data.children[i + k];

            // Drop the k leftmost separators: shift keys[k..count-1] down to [0..]. Span.CopyTo
            // has memmove (overlap-safe) semantics.
            int remainingKeys = count - k;
            if (remainingKeys > 0)
            {
                var src = new Span<byte>(node->keys + (long)k * BTreeNode.KEY_SIZE, remainingKeys * BTreeNode.KEY_SIZE);
                var dst = new Span<byte>(node->keys, remainingKeys * BTreeNode.KEY_SIZE);
                src.CopyTo(dst);
            }

            node->info->count = remainingKeys; // == count - k
        }

        /// <summary>
        /// Free a single leaf node detached by trim (non-recursive) and update stats.
        /// </summary>
        void FreeLeafForTrim(BTreeNode* leaf)
        {
            stats.numKeys -= leaf->info->count;
            if (leaf->memoryHandle != null)
                NativeMemory.AlignedFree(leaf->memoryHandle);
            stats.numLeafNodes--;
            stats.numDeallocates++;
        }

        /// <summary>
        /// Free a single internal node detached by trim (non-recursive) and update stats.
        /// </summary>
        void FreeInternalForTrim(BTreeNode* node)
        {
            if (node->memoryHandle != null)
                NativeMemory.AlignedFree(node->memoryHandle);
            stats.numInternalNodes--;
            stats.numDeallocates++;
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