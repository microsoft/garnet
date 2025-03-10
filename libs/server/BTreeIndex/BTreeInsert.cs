// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;

namespace Garnet.server.BTreeIndex
{
    public unsafe partial class BTree
    {
        public bool Insert(byte* key, Value value)
        {
            BTreeNode* leaf = null;
            stats.totalFastInserts++;
            stats.totalInserts++;
            stats.numKeys++;
            stats.numValidKeys++;
            leaf = tail;
            return InsertToLeafNode(ref leaf, ref rootToTailLeaf, key, value);
        }
        public bool InsertToLeafNode(ref BTreeNode* leaf, ref BTreeNode*[] nodesTraversed, byte* key, Value value)
        {
            int index;

            // if leaf has space 
            if (leaf->info.count < BTreeNode.LEAF_CAPACITY)
            {
                // append to end of leaf node 
                leaf->SetKey(leaf->info.count, key);
                leaf->SetValue(leaf->info.count, value);
                leaf->info.count++;
                leaf->info.validCount++;
                return true;
            }
            index = leaf->info.count;
            // split the leaf node
            return SplitLeafNode(ref leaf, ref nodesTraversed, key, value, index);
        }

        public bool SplitLeafNode(ref BTreeNode* leaf, ref BTreeNode*[] nodesTraversed, byte* key, Value value, int index)
        {
            var newLeaf = CreateNewLeafNode(ref leaf);

            // count valid keys in the new leaf, starting at splitLeafPosition in the old leaf 
            uint newLeafValidCount = 0;
            for (int i = SPLIT_LEAF_POSITION; i < BTreeNode.LEAF_CAPACITY; i++)
            {
                if (leaf->data.values[i].Valid)
                {
                    newLeafValidCount++;
                }
            }
            leaf->info.validCount -= newLeafValidCount;
            newLeaf->info.validCount = newLeafValidCount;

            // since input will always arrive sorted as timestamp, the new key always goes to the new leaf
            var newIndex = index - SPLIT_LEAF_POSITION;
            Buffer.MemoryCopy(leaf->keys + leaf->info.count * BTreeNode.KEY_SIZE, newLeaf->keys, newIndex * BTreeNode.KEY_SIZE, newIndex * BTreeNode.KEY_SIZE);
            newLeaf->SetKey(newIndex, key);

            Buffer.MemoryCopy(leaf->keys + index * BTreeNode.KEY_SIZE, newLeaf->keys + (newIndex + 1) * BTreeNode.KEY_SIZE, (BTreeNode.LEAF_CAPACITY - index) * BTreeNode.KEY_SIZE, (BTreeNode.LEAF_CAPACITY - index) * BTreeNode.KEY_SIZE);
            Buffer.MemoryCopy(leaf->data.values + leaf->info.count, newLeaf->data.values, newIndex * sizeof(Value), newIndex * sizeof(Value));
            newLeaf->SetValue(newIndex, value);
            Buffer.MemoryCopy(leaf->data.values + index, newLeaf->data.values + newIndex + 1, (BTreeNode.LEAF_CAPACITY - index) * sizeof(Value), (BTreeNode.LEAF_CAPACITY - index) * sizeof(Value));
            newLeaf->info.validCount++;

            uint validCount = 0;
            // the leaf that is split will also be the tail node; so update the tail pointer
            if (leaf == tail)
            {
                tail = newLeaf;
                tailMinKey = newLeaf->GetKey(0);
                // validCount in internal nodes of the index excludes the validCount of the tail leaf node (optimizing for performance to avoid traversal)
                // thus, when we split the tail leaf, we push up the validCount of the leaf that we split to the internal node
                validCount = leaf->info.validCount;
            }

            // update the parent node with the new key

            return true;
        }

        public BTreeNode* CreateNewLeafNode(ref BTreeNode* leafToSplit)
        {
            // BTreeNode* newLeaf = (BTreeNode*)Marshal.AllocHGlobal(sizeof(BTreeNode)).ToPointer();
            // newLeaf->memoryBlock = (IntPtr)newLeaf;
            var memoryBlock = bufferPool.Get(BTreeNode.PAGE_SIZE);
            var memory = (IntPtr)memoryBlock.aligned_pointer;
            BTreeNode* newLeaf = (BTreeNode*)memory;
            // newLeaf->memoryHandle = memoryBlock;
            newLeaf->Initialize(BTreeNodeType.Leaf, memoryBlock);
            leafToSplit->info.count = SPLIT_LEAF_POSITION;
            leafToSplit->info.next = newLeaf;
            newLeaf->info.previous = leafToSplit;
            newLeaf->info.next = leafToSplit->info.next;
            newLeaf->info.count = BTreeNode.LEAF_CAPACITY + 1 - SPLIT_LEAF_POSITION;
            stats.numLeafNodes++;
            return newLeaf;
        }

        public void PushUpKeyInInternalNode(ref BTreeNode*[] nodesTraversed, byte* key, ref BTreeNode* child, int splitPos, uint newValidCount)
        {
            int i;
            // starts from parent of leaf node that triggered the push-up. 
            // if the parent has space, insert the key and child pointer, and return. Otherwise, split and cascade up. 
            for (i = 1; i < stats.depth; i++)
            {
                var node = nodesTraversed[i];
                var index = node->UpperBound(key);

                if (node->info.count < BTreeNode.INTERNAL_CAPACITY)
                {
                    // TODO: potentially get rid of this as we will also only be appending to end of internal node due to sorted insertions
                    Buffer.MemoryCopy(node->keys + index * BTreeNode.KEY_SIZE, node->keys + ((index + 1) * BTreeNode.KEY_SIZE),
                    (node->info.count - index) * BTreeNode.KEY_SIZE, (node->info.count - index) * BTreeNode.KEY_SIZE);
                    // move all children 
                    for (var j = node->info.count; j > index; j--)
                    {
                        node->SetChild(j + 1, node->GetChild(j));
                    }

                    node->SetKey(index, key);
                    node->SetChild(index + 1, child);
                    node->info.count++;
                    node->info.validCount += newValidCount;

                    // insert does not cascade up, so update validCounts in the parent nodes
                    for (var j = i + 1; j < stats.depth; j++)
                    {
                        nodesTraversed[j]->info.validCount += newValidCount;
                    }
                    return;
                }

                // split internal node
                var newNode = SplitInternalNode(ref node, ref nodesTraversed, ref key, ref child, splitPos, index, i);
                if (rootToTailLeaf[i] == node && tail != head && BTreeNode.Compare(key, tailMinKey) <= 0)
                {
                    rootToTailLeaf[i] = newNode;
                }
                child = newNode;
            }
            // split root
            CreateNewRoot(key, child);
        }

        public BTreeNode* CreateInternalNode(ref BTreeNode* node, int splitPos)
        {
            // BTreeNode* newNode = (BTreeNode*)Marshal.AllocHGlobal(sizeof(BTreeNode));
            var memoryBlock = bufferPool.Get(BTreeNode.PAGE_SIZE);
            var memory = (IntPtr)memoryBlock.aligned_pointer;
            BTreeNode* newNode = (BTreeNode*)memory;
            // newNode->memoryHandle = memoryBlock;
            newNode->Initialize(BTreeNodeType.Internal, memoryBlock);
            stats.numInternalNodes++;

            node->info.count = splitPos;
            newNode->info.count = (BTreeNode.INTERNAL_CAPACITY - splitPos);
            newNode->info.next = node->info.next;
            newNode->info.previous = node;
            node->info.next = newNode;
            return newNode;
        }

        public BTreeNode* SplitInternalNode(ref BTreeNode* nodeToSplit, ref BTreeNode*[] nodesTraversed, ref byte* key, ref BTreeNode* child, int splitPos, int index, int level)
        {
            var newNode = CreateInternalNode(ref nodeToSplit, splitPos);

            // scan keys from splitPos to get number of valid keys in the new node
            uint newValidCount = 0;
            for (int i = splitPos; i < BTreeNode.INTERNAL_CAPACITY; i++)
            {
                if (nodeToSplit->GetChild(i) != null)
                {
                    newValidCount += nodeToSplit->GetChild(i)->info.validCount;
                }
            }
            newNode->info.validCount = newValidCount;

            // we are inserting in sorted order, so child always goes to newNode
            Buffer.MemoryCopy(nodeToSplit->keys + (nodeToSplit->info.count + 1) * BTreeNode.KEY_SIZE, newNode->keys, (index - nodeToSplit->info.count - 1) * BTreeNode.KEY_SIZE, (index - nodeToSplit->info.count - 1) * BTreeNode.KEY_SIZE);
            Buffer.MemoryCopy(nodeToSplit->keys + index * BTreeNode.KEY_SIZE, newNode->keys + (index - nodeToSplit->info.count) * BTreeNode.KEY_SIZE, (BTreeNode.INTERNAL_CAPACITY - index) * BTreeNode.KEY_SIZE, (BTreeNode.INTERNAL_CAPACITY - index) * BTreeNode.KEY_SIZE);
            newNode->SetKey(index - nodeToSplit->info.count - 1, key);
            Buffer.MemoryCopy(nodeToSplit->data.children + 1 + nodeToSplit->info.count, newNode->data.children, (index - nodeToSplit->info.count) * sizeof(BTreeNode*), (index - nodeToSplit->info.count) * sizeof(BTreeNode*));
            Buffer.MemoryCopy(nodeToSplit->data.children + 1 + index, newNode->data.children + 1 + index - nodeToSplit->info.count, newNode->info.count * sizeof(BTreeNode*), newNode->info.count * sizeof(BTreeNode*));
            newNode->SetChild(index - nodeToSplit->info.count, child);
            key = nodeToSplit->GetKey(nodeToSplit->info.count);

            return newNode;
        }


        public void CreateNewRoot(byte* key, BTreeNode* newlySplitNode)
        {
            // BTreeNode* leftNode = (BTreeNode*)Marshal.AllocHGlobal(sizeof(BTreeNode)).ToPointer();
            var memoryBlock = bufferPool.Get(BTreeNode.PAGE_SIZE);
            var memory = (IntPtr)memoryBlock.aligned_pointer;
            BTreeNode* leftNode = (BTreeNode*)memory;
            // leftNode->memoryHandle = memoryBlock;
            leftNode->Initialize(root->info.type, memoryBlock);

            // copy the root node to the left node
            // Buffer.MemoryCopy(root->info, leftNode->info, BTreeNode.PAGE_SIZE, BTreeNode.PAGE_SIZE);

            // if root is a leaf, upgrade to internal node
            if (root->info.type == BTreeNodeType.Leaf)
            {
                root->UpgradeToInternal();
            }

            root->info.count = 1;
            root->SetKey(0, key);
            root->SetChild(0, leftNode);
            root->SetChild(1, newlySplitNode);
            root->info.next = root->info.previous = null;
            root->info.validCount = leftNode->info.validCount;
            if (newlySplitNode != tail)
            {
                root->info.validCount += newlySplitNode->info.validCount;
            }
            newlySplitNode->info.previous = leftNode;

            if (root == head)
            {
                head = leftNode;
            }
            if (rootToTailLeaf[stats.depth - 1] == root)
            {
                if (tail == root)
                {
                    tail = leftNode;
                }
                rootToTailLeaf[stats.depth - 1] = leftNode;
            }
            rootToTailLeaf[stats.depth] = root;
            stats.depth++;
            stats.numInternalNodes++;
        }
    }
}