// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;

namespace Garnet.server.BTreeIndex
{
    public unsafe partial class BTree
    {
        public void TrimByID(byte* key, out ulong entriesTrimmed, out Value headValue, out ReadOnlySpan<byte> headValidKey, out uint numLeavesDeleted)
            => TrimByID(key, out int _, out entriesTrimmed, out headValue, out headValidKey, out numLeavesDeleted);

        public void TrimByLength(ulong length, out ulong entriesTrimmed, out Value headValue, out ReadOnlySpan<byte> headValidKey, out uint numLeavesDeleted, bool approximateTrimming = false)
            => TrimByLength(ref root, length, out entriesTrimmed, out headValue, out headValidKey, out numLeavesDeleted, approximateTrimming);

        private void TrimByID(byte* key, out int underflowingNodes, out ulong entriesTrimmed, out Value headValidValue, out ReadOnlySpan<byte> headValidKey, out uint numLeavesDeleted)
        {
            underflowingNodes = 0;
            entriesTrimmed = 0;
            numLeavesDeleted = 0;

            var nodesTraversed = new BTreeNode*[MAX_TREE_DEPTH];
            BTreeNode* leaf = null;
            TraverseToLeaf(ref leaf, ref nodesTraversed, key, out int[] internalSlots);

            // find index for key in leaf node - this returns the index of first key >= given key
            var index = leaf->LowerBound(key);
            headValidKey = new ReadOnlySpan<byte>(leaf->GetKey(index), BTreeNode.KEY_SIZE);
            headValidValue = leaf->GetValue(index);

            // insert tombstones until index to mark as deleted 
            for (var i = 0; i < index; i++)
            {
                leaf->SetValueValid(i, false);
                leaf->info->validCount--;
                entriesTrimmed++;
            }

            if (leaf == head)
            {
                numLeavesDeleted = 0;
                return;
            }

            // traverse the leaf level to delete preceding leaf nodes
            var node = leaf->info->previous;
            var nodesToTraverseInSubtree = internalSlots[1] - 1;
            uint deletedValidCount = (uint)(leaf->info->count - leaf->info->validCount);
            var totalDeletedValidCount = deletedValidCount;
            while (node != null)
            {
                var validCount = node->info->validCount;
                var count = node->info->count;
                if (nodesToTraverseInSubtree >= 0)
                {
                    deletedValidCount += validCount;
                    nodesToTraverseInSubtree--;
                }
                totalDeletedValidCount += validCount;

                var prev = node->info->previous;
                if (prev == null)
                {
                    Debug.Assert(node == head, "Head node should not have a previous node");
                }

                stats.numLeafNodes--;
                stats.numKeys -= count;
                stats.numValidKeys -= validCount;
                entriesTrimmed += validCount;

                // deallocate the node
                Deallocate(ref node);
                numLeavesDeleted++;

                // continue iteration
                node = prev;
            }

            leaf->info->previous = null;
            head = leaf;

            bool rootReassigned = false;
            // traverse internal nodes except root and delete preceding internal nodes
            for (int i = 1; i < stats.depth - 1; i++)
            {
                node = nodesTraversed[i];
                var slotOfKey = internalSlots[i];

                if (slotOfKey > 0)
                {
                    // shift children leftwards until slotOfKey (inclusive) using ReadOnlySpan<byte>
                    var sourceSpan = new ReadOnlySpan<byte>(node->keys + (slotOfKey - 1) * BTreeNode.KEY_SIZE, ((slotOfKey - 1)) * BTreeNode.KEY_SIZE);
                    var destinationSpan = new Span<byte>(node->keys, ((slotOfKey - 1)) * BTreeNode.KEY_SIZE);
                    sourceSpan.CopyTo(destinationSpan);

                    var sourceChildrenSpan = new ReadOnlySpan<byte>(node->data.children + (slotOfKey - 1) + 1, ((slotOfKey - 1)) * sizeof(BTreeNode*));
                    var destinationChildrenSpan = new Span<byte>(node->data.children, ((slotOfKey - 1)) * sizeof(BTreeNode*));
                    sourceChildrenSpan.CopyTo(destinationChildrenSpan);
                }
                var prevCount = node->info->count;
                node->info->count -= slotOfKey;
                node->info->validCount -= deletedValidCount;

                if (prevCount > BTreeNode.INTERNAL_CAPACITY / 2 && node->info->count < BTreeNode.INTERNAL_CAPACITY / 2)
                {
                    underflowingNodes++;
                }

                node = nodesTraversed[i]->info->previous;
                deletedValidCount = 0;
                while (node != null)
                {
                    var temp = node->info->previous;
                    if (nodesToTraverseInSubtree >= 0)
                    {
                        deletedValidCount += node->info->validCount;
                        nodesToTraverseInSubtree--;
                    }
                    Deallocate(ref node);
                    stats.numInternalNodes--;
                    node = temp;
                }
                nodesTraversed[i]->info->previous = null;
                // corner case: slotOfKey points to last child => after deletion only one child remains
                // delete all partent levels and re-assign root
                if (i + 1 < stats.depth)
                {
                    var nextSlot = internalSlots[i + 1];
                    if (nextSlot == nodesTraversed[i + 1]->info->count)
                    {
                        var newRoot = nodesTraversed[i];
                        var originalDepth = stats.depth;
                        for (int j = i + 1; j < originalDepth; j++)
                        {
                            var curr = nodesTraversed[j];
                            while (curr != null)
                            {
                                var pre = curr->info->previous;
                                Deallocate(ref curr);
                                stats.numInternalNodes--;
                                curr = pre;
                            }
                            stats.depth--;
                        }
                        root = newRoot;
                        rootReassigned = true;
                        break;
                    }
                }
            }
            if (!rootReassigned && stats.depth > 1 && nodesTraversed[stats.depth - 1] != null)
            {
                nodesTraversed[stats.depth - 1]->info->validCount -= totalDeletedValidCount;
            }
        }
    
        private void TrimByLength(ref BTreeNode* node, ulong length, out ulong entriesTrimmed, out Value headValidValue, out ReadOnlySpan<byte> headValidKey, out uint numLeavesDeleted, bool approximateTrimming)
        {
            var depth = stats.depth - 1;
            ulong currentValidCount = 0;
            BTreeNode* current = node;
            int[] internalSlots = new int[MAX_TREE_DEPTH];
            int underflowingNodes = 0;
            entriesTrimmed = 0;
            numLeavesDeleted = 0;
            headValidKey = default;
            BTreeNode*[] nodesTraversed = new BTreeNode*[MAX_TREE_DEPTH];

            // stream is already smaller than desired length to trim to
            if (length >= stats.numValidKeys)
            {
                headValidValue = current->GetValue(0);
                headValidKey = new ReadOnlySpan<byte>(current->GetKey(0), BTreeNode.KEY_SIZE);
                return;
            }

            // set the starting node (root) as the last node in nodes traversed array
            nodesTraversed[depth] = current;
            // while we have not traversed the entire depth of the tree?\
            while (depth > 0)
            {
                // current node is internal node
                if (current->info->type == BTreeNodeType.Internal)
                {
                    // iterate over the children in the internal node from right to left, largest to smallest
                    for (var i = current->info->count; i >= 0; i--)
                    {
                        var child = current->GetChild(i);

                        // cumulative valid count is less than desired length so just keep accumulating
                        if (currentValidCount + child->info->validCount < length)
                        {
                            currentValidCount += child->info->validCount;
                        }
                        else
                        {
                            // if the cumulative valid count including this child exceeds the desired length, then we have found the node to split at
                            // track the node in nodes traversed and the slot index in internalSlots
                            nodesTraversed[depth - 1] = child;
                            internalSlots[depth] = i;
                            // current holds the node we will continue traversing from to find the split point
                            current = child;
                            break;
                        }
                    }
                }
                depth--;
            }

            // After traversing down from root (depth stats.depth-1) to leaf level (depth 0),
            // current is guaranteed to be a leaf node
            Debug.Assert(current->info->type == BTreeNodeType.Leaf, "Current must be a leaf after traversal");

            // In approximate trimming mode, we don't attempt to trim within the node itself.
            if (approximateTrimming)
            {
                headValidValue = current->GetValue(0);
                headValidKey = new ReadOnlySpan<byte>(current->GetKey(0), BTreeNode.KEY_SIZE);
            }
            else
            {
                // length is the desired length to trim to and currentValidCount is the cumulative valid count before this node.
                // since we are in non-approximate mode, we need to trim within this node to reach the exact desired length.
                // keepInCurrent holds that little diff we may not have accumulated once we reached the current node itself.
                ulong keepInCurrent = length - currentValidCount;
                ulong kept = 0;
                headValidValue = default;
                headValidKey = default;
                // iterate over entries in current node right to left (largest to smallest).
                for (int i = (int)current->info->count - 1; i >= 0; i--)
                {
                    // only consider valid entries
                    if (current->IsValueValid(i))
                    {
                        // we keep a key in the current node only if we have not yet reached the desired keepInCurrent count
                        if (kept < keepInCurrent)
                        {
                            if (kept == 0)
                            {
                                headValidValue = current->GetValue(i);
                                headValidKey = new ReadOnlySpan<byte>(current->GetKey(i), BTreeNode.KEY_SIZE);
                            }
                            kept++;
                        }
                        else
                        {
                            // once we have reached the desired keepInCurrent count, we mark remaining keys as deleted;
                            // since we iterate right to left, we're now deleting entries to the LEFT (older entries)
                            // Mark as deleted.
                            current->SetValueValid(i, false);
                            current->info->validCount--;
                            entriesTrimmed++;
                            stats.numValidKeys--;
                        }
                    }
                }
            }

            // now current node has been trimmed internally. Proceed to removing preceding nodes

            // nodes are linked list at leaf level, so we can traverse backwards
            var leaf = current->info->previous;
            uint deletedValidCount = 0;
            var nodesToTraverseInSubtree = internalSlots[depth + 1] - 1;
            while (leaf != null)
            {
                var count = leaf->info->count;
                var validCount = leaf->info->validCount;

                if (nodesToTraverseInSubtree >= 0)
                {
                    deletedValidCount += validCount;
                    nodesToTraverseInSubtree--;
                }
                var prev = leaf->info->previous;
                if (prev == null)
                {
                    Debug.Assert(leaf == head, "Head node should not have a previous node");
                }
                stats.numLeafNodes--;
                stats.numKeys -= count;
                stats.numValidKeys -= validCount;
                entriesTrimmed += validCount;

                // deallocate the node
                Deallocate(ref leaf);
                numLeavesDeleted++;
                leaf = prev;
            }

            // disconnect current from previous nodes, and make the current the new head node
            current->info->previous = null;
            head = current;
            // traverse the internal nodes except root and delete preceding internal nodes
            for (int i = 1; i < stats.depth - 1; i++)
            {
                var slotOfKey = internalSlots[i];
                var inner = nodesTraversed[i];
                if (inner == null)
                {
                    break;
                }
                if (slotOfKey > 0)
                {
                    // shift keys and children from slotOfKey to beginning
                    var sourceSpan = new ReadOnlySpan<byte>(inner->keys + (slotOfKey - 1) * BTreeNode.KEY_SIZE, ((slotOfKey - 1)) * BTreeNode.KEY_SIZE);
                    var destinationSpan = new Span<byte>(inner->keys, ((slotOfKey - 1)) * BTreeNode.KEY_SIZE);
                    sourceSpan.CopyTo(destinationSpan);

                    var sourceChildrenSpan = new ReadOnlySpan<byte>(inner->data.children + (slotOfKey - 1) + 1, ((slotOfKey - 1)) * sizeof(BTreeNode*));
                    var destinationChildrenSpan = new Span<byte>(inner->data.children, ((slotOfKey - 1)) * sizeof(BTreeNode*));
                    sourceChildrenSpan.CopyTo(destinationChildrenSpan);
                }
                var prevCount = inner->info->count;
                inner->info->count -= slotOfKey;
                nodesTraversed[i]->info->validCount -= deletedValidCount;

                if (prevCount > BTreeNode.INTERNAL_CAPACITY / 2 && inner->info->count < BTreeNode.INTERNAL_CAPACITY / 2)
                {
                    underflowingNodes++;
                }
                deletedValidCount = 0;
                nodesToTraverseInSubtree = slotOfKey - 1;
                inner = inner->info->previous;
                while (inner != null && inner != root)
                {
                    var temp = inner->info->previous;
                    if (nodesToTraverseInSubtree >= 0)
                    {
                        deletedValidCount += inner->info->validCount;
                        nodesToTraverseInSubtree--;
                    }
                    Deallocate(ref inner);
                    stats.numInternalNodes--;
                    inner = temp;
                }
                nodesTraversed[i]->info->previous = null;
                // corner case: slotOfKey points to last child => after deletion only one child remains
                // delete all parent levels and re-assign root
                if (i + 1 < stats.depth)
                {
                    var nextSlot = internalSlots[i + 1];
                    if (nextSlot == nodesTraversed[i + 1]->info->count)
                    {
                        var newRoot = nodesTraversed[i];
                        var originalDepth = stats.depth;
                        for (int j = i + 1; j < originalDepth; j++)
                        {
                            var curr = nodesTraversed[j];
                            while (curr != null)
                            {
                                var pre = curr->info->previous;
                                Deallocate(ref curr);
                                stats.numInternalNodes--;
                                curr = pre;
                            }
                            stats.depth--;
                        }
                        root = newRoot;
                        break;
                    }
                }
            }
        }
    }
}