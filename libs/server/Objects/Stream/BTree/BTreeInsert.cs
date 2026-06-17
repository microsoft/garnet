// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Garnet.server.BTreeIndex
{
    public unsafe partial class BTree
    {
        /// <summary>
        /// Insert a key-value pair into the B+tree. Directly inserts into the tail leaf node. NOT a general insert. 
        /// It expects the key being inserted to be greater than or equal to the current tailMinKey, which is the minimum key in the tail leaf node. 
        /// This is optimized for fast inserts of keys in ascending order, which is the pattern used by Streams in Garnet.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public void InsertIntoTail(byte* key, Value value)
        {
            BTreeNode* leaf = null;
            stats.totalFastInserts++;
            stats.totalInserts++;
            stats.numKeys++;
            stats.numValidKeys++;
            leaf = tail;
            InsertToLeafNode(ref leaf, key, value);
        }

        private void InsertToLeafNode(ref BTreeNode* leaf, byte* key, Value value)
        {
            if (leaf->info->count < BTreeNode.LEAF_CAPACITY)
            {
                // append to end of leaf node by setting key+val, and updating indices
                leaf->SetKey(leaf->info->count, key);
                leaf->SetValue(leaf->info->count, value);
                leaf->info->count++;
                leaf->info->validCount++;
                return;
            }
            SplitLeafNode(ref leaf, key, value);
        }

        private void SplitLeafNode(ref BTreeNode* leaf, byte* key, Value value)
        {
            var memoryBlock = (IntPtr*)NativeMemory.AlignedAlloc((nuint)BTreeNode.PAGE_SIZE, (nuint)BTreeNode.PAGE_SIZE);
            stats.numAllocates++;
            BTreeNode* newLeaf = BTreeNode.Create(BTreeNodeType.Leaf, memoryBlock);

            Debug.Assert(leaf->info->next == null, "leaf being split should be the tail leaf");
            Debug.Assert(leaf->info->count == SPLIT_LEAF_POSITION, "leaf being split should be full before we split!");

            // setup new leaf and link previous leaf to it
            newLeaf->info->previous = leaf;
            newLeaf->info->next = null;
            newLeaf->info->count = 1; // the item being inserted will land in this new leaf so set count to 1 pro-actively
            newLeaf->info->validCount = 1; // ...and that item is a live entry, so its validCount starts at 1
            leaf->info->next = newLeaf;
            stats.numLeafNodes++;

            // new key goes to the new leaf at the 0th index
            int srcIdx = SPLIT_LEAF_POSITION;

            // add key+val to new leaf. Since we are essentially creating a brand new tail, we will not deal with any transfers of values
            newLeaf->SetKey(0, key);
            newLeaf->SetValue(0, value);

            // the leaf that is split will also be the tail node; so update the tail pointer
            Debug.Assert(leaf == tail, "The leaf that was split should also have been the tail node since we are only supporting inserts at the tail.");
            tail = newLeaf;
            tailMinKey = newLeaf->GetKey(0);
            rootToTailLeaf[0] = newLeaf;

            // validCount in internal nodes of the index excludes the validCount of the tail leaf node (optimizing for performance to avoid traversal)
            // thus, when we split the tail leaf, we push up the validCount of the leaf that we split to the internal node
            uint validCount = leaf->info->validCount;

            // update the parent node with the new key
            PushUpKeyInInternalNode(tailMinKey, ref newLeaf, validCount);
        }

        private void PushUpKeyInInternalNode(byte* key, ref BTreeNode* child, uint newValidCount)
        {
            /*
             After enough ascending inserts, the rightmost path R1 → N2 → L8 is maxed out (R1 full with 2 keys, N2 full with 2 keys, tail leaf L8 full with 3 keys):
                                      R1 [100 | 190]            depth = 3   (vc = 24)
                             /              |              \
                   R0 [40|70]         N1 [130|160]        N2 [220|250]      (vc = 6)
                    / |  \             /  |  \             /  |  \
                 L0  L1  L2         L3  L4  L5          L6  L7  L8           <- L8 = tail, FULL
                [10..][..][..]     [..][..][..]        [..][..][250,260,270]

             rootToTailLeaf:  [0]=L8   [1]=N2   [2]=R1
            Every node on the path [0]=L8 → [1]=N2 → [2]=R1 is full. Now insert 280.

            AFTER — depth-4, with a new thin right spine

                                               R2 [280]                 depth = 4   (vc = 27)
                                    /                         \
                         R1 [100 | 190]                         N4 [ ]            <- 0 keys, 1 child
                       /        |        \                         \
               R0[40|70]   N1[130|160]   N2[220|250]                N3 [ ]        <- 0 keys, 1 child
                / | \        / | \         / | \                       \
              L0 L1 L2    L3 L4 L5     L6 L7 L8                         L9[280]   <- tail
                                               ^sealed (still under N2)

             rootToTailLeaf:  [0]=L9   [1]=N3   [2]=N4   [3]=R2
             */

            int i;
            // starts from parent of leaf node that triggered the push-up. 
            // if the parent has space, insert the key and child pointer, and return. Otherwise, split and cascade up. 
            for (i = 1; i < stats.depth; i++)
            {
                var node = rootToTailLeaf[i];
                var index = node->info->count;

                if (node->info->count < BTreeNode.INTERNAL_CAPACITY)
                {
                    // we can insert 
                    InsertToInternalNodeWithinCapacity(ref node, key, ref child, index, newValidCount);

                    // update validCounts in the parent nodes
                    for (var j = i + 1; j < stats.depth; j++)
                    {
                        rootToTailLeaf[j]->info->validCount += newValidCount;
                    }
                    return;
                }

                // "split" internal node
                node->info->validCount += newValidCount;
                BTreeNode* newNode = CreateInternalNode(ref node);
                // link newly created internal node and child node. This will create a thin spine on the right side.
                newNode->SetChild(0, child);

                Debug.Assert(tail != head, "This path cannot be invoked when only 1 node exists in the BTree");
                rootToTailLeaf[i] = newNode;

                child = newNode;
            }

            // create a root that will point to the previous root on the left and the last newNode we created on the right
            CreateNewRoot(key, child);
        }

        public void InsertToInternalNodeWithinCapacity(ref BTreeNode* node, byte* key, ref BTreeNode* child, int index, uint newValidCount)
        {
            Debug.Assert(node->info->count == index, "Insertions are of the largest value so far so this shjould be the case even in internal nodes");

            node->SetKey(index, key);
            node->SetChild(index + 1, child);
            node->info->count++;
            node->info->validCount += newValidCount;
        }

        private BTreeNode* CreateInternalNode(ref BTreeNode* node)
        {
            var memoryBlock = (IntPtr*)NativeMemory.AlignedAlloc((nuint)BTreeNode.PAGE_SIZE, (nuint)BTreeNode.PAGE_SIZE);
            stats.numAllocates++;
            BTreeNode* newNode = BTreeNode.Create(BTreeNodeType.Internal, memoryBlock);
            stats.numInternalNodes++;
            Debug.Assert(node->info->count == BTreeNode.INTERNAL_CAPACITY, "Node should be full before split");
            newNode->info->count = 0;
            newNode->info->next = node->info->next;
            newNode->info->previous = node;
            node->info->next = newNode;
            return newNode;
        }

        private void CreateNewRoot(byte* key, BTreeNode* newlySplitNode)
        {
            var memoryBlock = (IntPtr*)NativeMemory.AlignedAlloc((nuint)BTreeNode.PAGE_SIZE, (nuint)BTreeNode.PAGE_SIZE);
            stats.numAllocates++;
            BTreeNode* newRoot = BTreeNode.Create(BTreeNodeType.Internal, memoryBlock);

            // Set the new root's key.
            newRoot->info->count = 1;
            newRoot->SetKey(0, key);

            // Set children: left child is the old root; right child is the newly split node.
            newRoot->SetChild(0, root);
            newRoot->SetChild(1, newlySplitNode);

            newRoot->info->validCount = root->info->validCount;
            if (newlySplitNode != tail)
            {
                newRoot->info->validCount += newlySplitNode->info->validCount;
            }
            newRoot->info->next = newRoot->info->previous = null;
            root = newRoot;
            rootToTailLeaf[stats.depth] = newRoot;
            stats.depth++;
            stats.numInternalNodes++;
        }
    }
}