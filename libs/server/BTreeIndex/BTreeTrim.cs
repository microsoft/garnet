// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Garnet.server.BTreeIndex
{
    public unsafe partial class BTree
    {
        public void TrimByID(byte* key, out int underflowingNodes, out ulong validKeysRemoved, out Value headValidValue, out byte[] headValidKey, out uint numLeavesDeleted)
        {
            underflowingNodes = 0;
            BTreeNode*[] nodesTraversed = new BTreeNode*[MAX_TREE_DEPTH];
            BTreeNode* leaf = null;
            validKeysRemoved = 0;
            numLeavesDeleted = 0;
            // first find the leaf node that could contain the key 
            TraverseToLeaf(ref leaf, ref nodesTraversed, key, out int[] internalSlots);

            // find the index for the key in the leaf node. Note: this would return the index of the first key greater than or equal to the given key
            var index = leaf->LowerBound(key);
            headValidValue = leaf->GetValue(index);
            headValidKey = new byte[BTreeNode.KEY_SIZE];
            var headValidKeyPtr = leaf->GetKey(index);
            Buffer.MemoryCopy(headValidKeyPtr, Unsafe.AsPointer(ref headValidKey[0]), BTreeNode.KEY_SIZE, BTreeNode.KEY_SIZE);

            // TODO: shift entries in current leaf... for now, simply insert tombstones until this point
            for (var i = 0; i < index; i++)
            {
                leaf->SetValueValid(i, false);
                leaf->info->validCount--;
            }

            if (leaf == head)
            {
                // we are already at head so we don't need to do any other traversals 
                numLeavesDeleted = 0;
                return;
            }

            // we will now traverse the leaf level of the tree and delete all preceding nodes 
            BTreeNode* node = leaf->info->previous;

            // # nodes to traverse in the subtree rooted at the leaf's parent (leaf is at nodesTraversed[0]). 
            // We subtract one since we delete preceding nodes.
            var nodesToTraverseInSubtree = internalSlots[1] - 1;
            uint deletedValidCount = 0;

            while (node != null)
            {
                var count = node->info->count;
                var validCount = node->info->validCount;
                if (nodesToTraverseInSubtree >= 0)
                {
                    deletedValidCount += validCount;
                    nodesToTraverseInSubtree--;
                }

                BTreeNode* prev = node->info->previous;
                if (prev == null)
                {
                    // should have reached the head, so do a sanity check 
                    Debug.Assert(node == head);
                }

                // update stats
                stats.numLeafNodes--;
                stats.numKeys -= count;
                stats.numValidKeys -= validCount;
                validKeysRemoved += validCount;

                // deallocate the node 
                node->Deallocate();
                Marshal.FreeHGlobal((IntPtr)node);
                numLeavesDeleted++;

                // assign node to temp to continue
                node = prev;
            }
            leaf->info->previous = null;
            // set leaf as the new head 
            head = leaf;

            // now we will traverse the internal nodes (except root) and delete all preceding nodes
            for (int i = 1; i < stats.depth - 1; i++)
            {
                // first handle the node in the nodesTraversed 
                node = nodesTraversed[i];
                var slotOfKey = internalSlots[i];

                if (slotOfKey > 0)
                {
                    // shift keys and children leftwards until slotOfKey (inclusive)
                    Buffer.MemoryCopy(node->keys + (slotOfKey - 1) * BTreeNode.KEY_SIZE, node->keys, ((slotOfKey - 1)) * BTreeNode.KEY_SIZE, ((slotOfKey - 1)) * BTreeNode.KEY_SIZE);
                    Buffer.MemoryCopy(node->data.children + (slotOfKey - 1) + 1, node->data.children, ((slotOfKey - 1)) * sizeof(BTreeNode*), ((slotOfKey - 1)) * sizeof(BTreeNode*));
                }

                var prev_count = node->info->count;
                // update count in node 
                node->info->count -= slotOfKey;
                nodesTraversed[i]->info->validCount -= deletedValidCount;

                if (prev_count > BTreeNode.INTERNAL_CAPACITY / 2 && node->info->count < BTreeNode.INTERNAL_CAPACITY / 2)
                {
                    // TODO: handle underflow... for now, simply track how many such nodes we may have 
                    underflowingNodes++;
                }

                // reset deleted valid count for next level
                deletedValidCount = 0;

                // next, handle all preceding internal nodes
                node = nodesTraversed[i]->info->previous;
                while (node != null)
                {
                    BTreeNode* temp = node->info->previous;
                    if (nodesToTraverseInSubtree >= 0)
                    {
                        deletedValidCount += node->info->validCount;
                        nodesToTraverseInSubtree--;
                    }

                    node->Deallocate();

                    Marshal.FreeHGlobal((IntPtr)node);

                    // update stats
                    stats.numInternalNodes--;
                    node = temp;
                }
                // set the previous of nodesTraversed[i] to null as it is the new head in the level 
                nodesTraversed[i]->info->previous = null;

                // handle corner case where slotOfKey in the internal node points to the last child => after deletion, only one child remains. 
                // in this case, delete all parent levels and re-assign root. 
                if (i + 1 < stats.depth)
                {
                    var nextSlot = internalSlots[i + 1];
                    if (nextSlot == nodesTraversed[i + 1]->info->count)
                    {
                        BTreeNode* newRoot = nodesTraversed[i];
                        var orig_depth = stats.depth;
                        for (int j = i + 1; j < orig_depth; j++)
                        {
                            BTreeNode* curr = nodesTraversed[j];
                            while (curr != null)
                            {
                                BTreeNode* pre = curr->info->previous;
                                curr->Deallocate();
                                Marshal.FreeHGlobal((IntPtr)curr);
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

        public void TrimByID(byte* key, out ulong validKeysRemoved, out Value headValue, out byte[] headValidKey, out uint numLeavesDeleted)
        {
            int underflowingNodes;
            TrimByID(key, out underflowingNodes, out validKeysRemoved, out headValue, out headValidKey, out numLeavesDeleted);
        }

        // function to trim the tree up to a given length
        // for every internal node, we will scan the children from right to left and check its valid count from the info
        // if the valid count is less than the length, we will keep the node and all its children to the right
        // if the valid count is greater than or equal to the length, we add this node to the nodesTraversed at the current depth and continue from this child at the next level. 
        // once we reach the leaf level, we will trim everything to the left, and subsequently traverse the nodesTraversed and perform the same operation at every level. 
        public void TrimByLength(ref BTreeNode* node, ulong length, ref BTreeNode*[] nodesTraversed, out ulong validKeysRemoved, out Value headValidValue, out byte[] headValidKey, out uint numLeavesDeleted)
        {

            var depth = stats.depth - 1;
            ulong currentValidCount = 0;
            BTreeNode* current = node;
            int[] internalSlots = new int[MAX_TREE_DEPTH];
            int underflowingNodes = 0;
            validKeysRemoved = 0;
            numLeavesDeleted = 0;
            headValidKey = new byte[BTreeNode.KEY_SIZE];
            // if the length is greater than the total number of valid keys, we will not trim anything
            if (length >= stats.numValidKeys)
            {
                headValidValue = current->GetValue(0);
                Buffer.MemoryCopy(current->GetKey(0), Unsafe.AsPointer(ref headValidKey[0]), BTreeNode.KEY_SIZE, BTreeNode.KEY_SIZE);
                return;
            }
            while (depth > 0)
            {
                if (current->info->type == BTreeNodeType.Internal)
                {
                    for (var i = current->info->count; i >= 0; i--)
                    {
                        // get the child node
                        BTreeNode* child = current->GetChild(i);
                        // if adding the child node's valid count wille exceed the length, we will continue on this child. Otherwise, we will keep this node and all its children to the right.
                        if (currentValidCount + child->info->validCount >= length)
                        {
                            nodesTraversed[depth] = current;
                            internalSlots[depth] = i;
                            current = child;
                            break;
                        }
                        else
                        {
                            currentValidCount += child->info->validCount;
                        }
                    }
                }
                depth--;
            }

            // we have reached the leaf level. We will now trim everything to the left of the current node.
            headValidValue = current->GetValue(0);
            Buffer.MemoryCopy(current->GetKey(0), Unsafe.AsPointer(ref headValidKey[0]), BTreeNode.KEY_SIZE, BTreeNode.KEY_SIZE);

            BTreeNode* leaf = current->info->previous;
            // might have to make sure that we are in a leaf node 
            Debug.Assert(leaf->info->type == BTreeNodeType.Leaf);

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
                BTreeNode* prev = leaf->info->previous;
                if (prev == null)
                {
                    // should have reached the head, so do a sanity check 
                    Debug.Assert(leaf == head);
                }

                // update stats
                stats.numLeafNodes--;
                stats.numKeys -= count;
                stats.numValidKeys -= validCount;
                validKeysRemoved += validCount;

                // deallocate the node
                leaf->Deallocate();
                Marshal.FreeHGlobal((IntPtr)leaf);
                numLeavesDeleted++;

                // assign node to temp to continue
                leaf = prev;
            }
            current->info->previous = null;
            // set current as the new head
            head = current;

            // now we will traverse the nodesTraversed and delete every node to its left, except the root node                     
            for (int i = 1; i <= stats.depth - 1; i++)
            {
                var slotOfKey = internalSlots[i];
                BTreeNode* inner;
                inner = nodesTraversed[i];

                if (inner == null)
                {
                    break;
                }

                if (slotOfKey > 0)
                {
                    // shift keys and children from slotOfKey to the beginning
                    Buffer.MemoryCopy(inner->keys + (slotOfKey - 1) * BTreeNode.KEY_SIZE, inner->keys, (slotOfKey - 1) * BTreeNode.KEY_SIZE, (slotOfKey - 1) * BTreeNode.KEY_SIZE);
                    Buffer.MemoryCopy(inner->data.children + slotOfKey, inner->data.children, (slotOfKey) * sizeof(BTreeNode*), (slotOfKey) * sizeof(BTreeNode*));
                }

                var prev_count = inner->info->count;
                // update count in node 
                inner->info->count -= slotOfKey;

                nodesTraversed[i]->info->validCount -= deletedValidCount;

                if (prev_count > BTreeNode.INTERNAL_CAPACITY / 2 && inner->info->count < BTreeNode.INTERNAL_CAPACITY / 2)
                {
                    // TODO: handle underflow... for now, simply track how many such nodes we may have 
                    underflowingNodes++;
                }

                // grab all validCounts for nodes that we are deleting.
                // subtract from parent's validCount for those we deleted
                deletedValidCount = 0;
                nodesToTraverseInSubtree = slotOfKey - 1;
                inner = inner->info->previous;
                while (inner != null && inner != root)
                {
                    BTreeNode* temp = inner->info->previous;
                    if (nodesToTraverseInSubtree >= 0)
                    {
                        deletedValidCount += inner->info->validCount;
                        nodesToTraverseInSubtree--;
                    }
                    inner->Deallocate();
                    Marshal.FreeHGlobal((IntPtr)inner);
                    stats.numInternalNodes--;
                    inner = temp;
                }
                nodesTraversed[i]->info->previous = null;

                // check the subsequent level in the tree
                // if slotOfKey points to the last child, then all parent levels will be deleted
                if (i + 1 < stats.depth)
                {
                    var nextSlot = internalSlots[i + 1];
                    if (nextSlot == nodesTraversed[i + 1]->info->count)
                    {
                        BTreeNode* newRoot = nodesTraversed[i];
                        var orig_depth = stats.depth;
                        for (int j = i + 1; j < orig_depth; j++)
                        {
                            BTreeNode* curr = nodesTraversed[j];
                            while (curr != null)
                            {
                                BTreeNode* pre = curr->info->previous;
                                curr->Deallocate();
                                Marshal.FreeHGlobal((IntPtr)curr);
                                stats.numInternalNodes--;
                                curr = pre;
                            }
                            stats.depth--;
                        }
                        // now that we have deleted all parent nodes, set root to correct node
                        root = newRoot;
                        break;
                    }
                }
            }
        }

        public void TrimByLength(ulong length, out ulong validKeysRemoved, out Value headValue, out byte[] headValidKey, out uint numLeavesDeleted)
        {
            BTreeNode*[] nodesTraversed = new BTreeNode*[MAX_TREE_DEPTH];
            TrimByLength(ref root, length, ref nodesTraversed, out validKeysRemoved, out headValue, out headValidKey, out numLeavesDeleted);
        }
    }
}