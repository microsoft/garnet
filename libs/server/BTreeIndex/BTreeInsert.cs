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

            // // count valid keys in the new leaf, starting at splitLeafPosition in the old leaf 
            // uint newLeafValidCount = 0;
            // for (int i = SPLIT_LEAF_POSITION; i < BTreeNode.LEAF_CAPACITY; i++)
            // {
            //     if (leaf->data.values[i].Valid)
            //     {
            //         newLeafValidCount++;
            //     }
            // }
            // leaf->info.validCount -= newLeafValidCount;
            // newLeaf->info.validCount = newLeafValidCount;

            // // since input will always arrive sorted as timestamp, the new key always goes to the new leaf
            // var newIndex = index - SPLIT_LEAF_POSITION;
            // Buffer.MemoryCopy(leaf->keys + leaf->info.count * BTreeNode.KEY_SIZE, newLeaf->keys, newIndex * BTreeNode.KEY_SIZE, newIndex * BTreeNode.KEY_SIZE);
            // newLeaf->SetKey(newIndex, key);

            // Buffer.MemoryCopy(leaf->keys + index * BTreeNode.KEY_SIZE, newLeaf->keys + (newIndex + 1) * BTreeNode.KEY_SIZE, (BTreeNode.LEAF_CAPACITY - index) * BTreeNode.KEY_SIZE, (BTreeNode.LEAF_CAPACITY - index) * BTreeNode.KEY_SIZE);
            // Buffer.MemoryCopy(leaf->data.values + leaf->info.count, newLeaf->data.values, newIndex * sizeof(Value), newIndex * sizeof(Value));
            // newLeaf->SetValue(newIndex, value);
            // Buffer.MemoryCopy(leaf->data.values + index, newLeaf->data.values + newIndex + 1, (BTreeNode.LEAF_CAPACITY - index) * sizeof(Value), (BTreeNode.LEAF_CAPACITY - index) * sizeof(Value));
            // newLeaf->info.validCount++;

            newLeaf->SetKey(0, key);
            newLeaf->SetValue(0, value);
            newLeaf->info.count = 1;
            newLeaf->info.validCount = 1;
            

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
            PushUpKeyInInternalNode(ref nodesTraversed, newLeaf->GetKey(0), ref newLeaf, SPLIT_INTERNAL_POSITION, validCount);
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
            newLeaf->info.previous = leafToSplit;
            newLeaf->info.next = leafToSplit->info.next;
            newLeaf->info.count = BTreeNode.LEAF_CAPACITY - SPLIT_LEAF_POSITION;
            leafToSplit->info.next = newLeaf;
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
            // Buffer.MemoryCopy(nodeToSplit->keys + (nodeToSplit->info.count + 1) * BTreeNode.KEY_SIZE, newNode->keys, (index - nodeToSplit->info.count - 1) * BTreeNode.KEY_SIZE, (index - nodeToSplit->info.count - 1) * BTreeNode.KEY_SIZE);
            // Buffer.MemoryCopy(nodeToSplit->keys + index * BTreeNode.KEY_SIZE, newNode->keys + (index - nodeToSplit->info.count) * BTreeNode.KEY_SIZE, (BTreeNode.INTERNAL_CAPACITY - index) * BTreeNode.KEY_SIZE, (BTreeNode.INTERNAL_CAPACITY - index) * BTreeNode.KEY_SIZE);
            // newNode->SetKey(index - nodeToSplit->info.count - 1, key);
            // Buffer.MemoryCopy(nodeToSplit->data.children + 1 + nodeToSplit->info.count, newNode->data.children, (index - nodeToSplit->info.count) * sizeof(BTreeNode*), (index - nodeToSplit->info.count) * sizeof(BTreeNode*));
            // Buffer.MemoryCopy(nodeToSplit->data.children + 1 + index, newNode->data.children + 1 + index - nodeToSplit->info.count, newNode->info.count * sizeof(BTreeNode*), newNode->info.count * sizeof(BTreeNode*));
            // newNode->SetChild(index - nodeToSplit->info.count, child);

            newNode->SetChild(0, nodeToSplit->GetChild(nodeToSplit->info.count)); // left child pointer of the new node part
            newNode->SetKey(0, key);
            newNode->SetChild(1, child);
            newNode->info.count = 1;
            // key = nodeToSplit->GetKey(nodeToSplit->info.count);
            key = newNode->GetKey(0);

            var childvalid = child->info.validCount;

            return newNode;
        }


        public void CreateNewRoot(byte* key, BTreeNode* newlySplitNode)
        {
            // BTreeNode* leftNode = (BTreeNode*)Marshal.AllocHGlobal(sizeof(BTreeNode)).ToPointer();
            var memoryBlock = bufferPool.Get(BTreeNode.PAGE_SIZE);
            var memory = (IntPtr)memoryBlock.aligned_pointer;
            BTreeNode* newRoot = (BTreeNode*)memory;
            // leftNode->memoryHandle = memoryBlock;
            newRoot->Initialize(BTreeNodeType.Internal, memoryBlock);

            // Set the new root's key to the key being pushed up (key from newlySplitNode).
            newRoot->info.count = 1;
            newRoot->SetKey(0, key);
            // Set its children: left child is the old root; right child is the newly split node.
            newRoot->SetChild(0, root);
            newRoot->SetChild(1, newlySplitNode);

            // Update the valid count (if desired, handle validCount appropriately).
                newRoot->info.validCount = root->info.validCount;
            if (newlySplitNode != tail)
            {
                newRoot->info.validCount += newlySplitNode->info.validCount;
            }
            newRoot->info.next = newRoot->info.previous = null;

            root = newRoot;
            rootToTailLeaf[stats.depth] = newRoot;
            stats.depth++;
            stats.numInternalNodes++;
        }
    }
}