// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server.BTreeIndex
{
    public unsafe partial class BTree
    {
        /// <summary>
        /// Test/diagnostic helper: walk the tree top-down and verify the structural invariants
        /// that <see cref="Deallocate()"/> (recursive free) and the read paths rely on:
        ///   • every internal node has all children[0..count] non-null,
        ///   • each separator keys[i] equals the min key of child[i+1],
        ///   • leaves all sit at <see cref="BTreeStats.depth"/>,
        ///   • the leaf linked-list is consistent with head/tail and previous/next links,
        ///   • per-leaf validCount and stats.numValidKeys / numLeafNodes match a fresh walk.
        /// Throws <see cref="InvalidOperationException"/> on the first violation. O(nodes);
        /// intended for tests, not the hot path.
        /// </summary>
        public void ValidateStructure()
        {
            if (root == null)
                throw new InvalidOperationException("root is null");
            if (head == null || tail == null)
                throw new InvalidOperationException("head/tail is null");

            // 1) Validate node invariants recursively; capture the tree's leftmost leaf.
            var leftmostLeaf = ValidateNode(root, 1);

            // 2) head must be the leftmost leaf with no predecessor; tail must end the list.
            if (leftmostLeaf != head)
                throw new InvalidOperationException("head does not match the leftmost leaf");
            if (head->info->previous != null)
                throw new InvalidOperationException("head->previous must be null");
            if (tail->info->next != null)
                throw new InvalidOperationException("tail->next must be null");

            // 3) Walk the leaf list head→tail; verify links, validCounts, and totals.
            BTreeNode* prev = null;
            BTreeNode* leaf = head;
            ulong liveCount = 0;
            ulong leafNodes = 0;
            while (leaf != null)
            {
                if (leaf->info->type != BTreeNodeType.Leaf)
                    throw new InvalidOperationException("leaf-list node is not a leaf");
                if (leaf->info->previous != prev)
                    throw new InvalidOperationException("leaf->previous link inconsistent");

                uint vc = 0;
                for (int i = 0; i < leaf->info->count; i++)
                {
                    if (leaf->data.values[i].Valid) vc++;
                }
                if (vc != leaf->info->validCount)
                    throw new InvalidOperationException($"leaf validCount mismatch: walked {vc}, stored {leaf->info->validCount}");

                liveCount += vc;
                leafNodes++;
                prev = leaf;
                leaf = leaf->info->next;
            }

            if (prev != tail)
                throw new InvalidOperationException("leaf list does not terminate at tail");
            if (liveCount != stats.numValidKeys)
                throw new InvalidOperationException($"numValidKeys mismatch: walked {liveCount}, stats {stats.numValidKeys}");
            if (leafNodes != stats.numLeafNodes)
                throw new InvalidOperationException($"numLeafNodes mismatch: walked {leafNodes}, stats {stats.numLeafNodes}");
        }

        /// <summary>
        /// Recursively validate the subtree rooted at <paramref name="node"/> and return its
        /// leftmost leaf. Verifies child presence, separator/child-min-key agreement, depth
        /// placement, and capacity bounds.
        /// </summary>
        BTreeNode* ValidateNode(BTreeNode* node, int level)
        {
            if (node == null)
                throw new InvalidOperationException("null node in tree");

            if (node->info->type == BTreeNodeType.Leaf)
            {
                if (level != stats.depth)
                    throw new InvalidOperationException($"leaf at level {level}, expected depth {stats.depth}");
                if (node->info->count > BTreeNode.LEAF_CAPACITY)
                    throw new InvalidOperationException("leaf exceeds capacity");
                return node;
            }

            int count = node->info->count;
            if (count < 0 || count > BTreeNode.INTERNAL_CAPACITY)
                throw new InvalidOperationException($"internal count {count} out of range at level {level}");

            BTreeNode* leftmost = null;
            for (int i = 0; i <= count; i++)
            {
                BTreeNode* child = node->GetChild(i);
                if (child == null)
                    throw new InvalidOperationException($"null child[{i}] at level {level}");

                var childLeftmost = ValidateNode(child, level + 1);
                if (i == 0)
                {
                    leftmost = childLeftmost;
                }
                else
                {
                    // separator keys[i-1] must equal the min key of children[i].
                    byte* sep = node->GetKey(i - 1);
                    byte* childMin = childLeftmost->GetKey(0);
                    if (BTreeNode.Compare(sep, childMin) != 0)
                        throw new InvalidOperationException($"separator[{i - 1}] != min key of child[{i}] at level {level}");
                }
            }
            return leftmost;
        }
    }
}