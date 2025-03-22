// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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
            return InsertToLeafNode(ref leaf, ref rootToTailLeaf, key, value,true);
        }
        public bool InsertToLeafNode(ref BTreeNode* leaf, ref BTreeNode*[] nodesTraversed, byte* key, Value value, bool appendToLeaf = false)
        {
            int index;
            if(appendToLeaf)
            {
                // if leaf has space 
                if (leaf->info->count < BTreeNode.LEAF_CAPACITY)
                {
                    // append to end of leaf node 
                    leaf->SetKey(leaf->info->count, key);
                    leaf->SetValue(leaf->info->count, value);
                    leaf->info->count++;
                    leaf->info->validCount++;
                    return true;
                }
                index = leaf->info->count;
                return SplitLeafNode(ref leaf, ref nodesTraversed, key, value, index);
            }
            
            // find the index where the key should be inserted
            index = leaf->LowerBound(key);
            if (index < leaf->info->count && BTreeNode.Compare(key, leaf->GetKey(index)) == 0)
            {
                // insert is actually an update
                leaf->SetValue(index, value);
                return false;
            }

            if (leaf->info->count < BTreeNode.LEAF_CAPACITY)
            {
                Buffer.MemoryCopy(leaf->keys + index * BTreeNode.KEY_SIZE, leaf->keys + ((index + 1) * BTreeNode.KEY_SIZE), (leaf->info->count - index) * BTreeNode.KEY_SIZE, (leaf->info->count - index) * BTreeNode.KEY_SIZE);
                Buffer.MemoryCopy(leaf->data.values + index, leaf->data.values + index + 1, (leaf->info->count - index) * sizeof(Value), (leaf->info->count - index) * sizeof(Value));

                leaf->SetKey(index, key);
                leaf->SetValue(index, value);
                leaf->info->count++;
                leaf->info->validCount++;
                return true;
            }
            return SplitLeafNode(ref leaf, ref nodesTraversed, key, value, index);
        }

        public bool SplitLeafNode(ref BTreeNode* leaf, ref BTreeNode*[] nodesTraversed, byte* key, Value value, int index)
        {
            // var newLeaf = CreateNewLeafNode(ref leaf);
            // var memoryBlock = bufferPool.Get(BTreeNode.PAGE_SIZE);
            var memoryBlock = (IntPtr*)Marshal.AllocHGlobal(BTreeNode.PAGE_SIZE).ToPointer();
            stats.numAllocates++;
            BTreeNode* newLeaf = BTreeNode.Create(BTreeNodeType.Leaf, memoryBlock);
            // newLeaf->memoryHandle = memoryBlock;
            // newLeaf->Initialize(BTreeNodeType.Leaf, memoryBlock);
            Debug.Assert(leaf!=null);

            leaf->info->count = SPLIT_LEAF_POSITION;
            newLeaf->info->previous = leaf;
            newLeaf->info->next = leaf->info->next;
            newLeaf->info->count = BTreeNode.LEAF_CAPACITY + 1 - SPLIT_LEAF_POSITION;
            leaf->info->next = newLeaf;
            stats.numLeafNodes++;

            // scan the keys from splitLeafPos to get the number of valid keys in the new leaf 
            uint newLeafValidCount = 0;
            for (var i = SPLIT_LEAF_POSITION; i < BTreeNode.LEAF_CAPACITY; i++)
            {
                if (leaf->data.values[i].Valid)
                {
                    newLeafValidCount++;
                }
            }
            leaf->info->validCount -= newLeafValidCount;
            newLeaf->info->validCount = newLeafValidCount;

            // newLeaf->SetKey(0, key);
            // newLeaf->SetValue(0, value);
            // newLeaf->info->count = 1;
            // newLeaf->info->validCount = 1;
            // insert the new key to either the old node or the newly created node, based on the index
            if (index >= leaf->info->count)
            {
                // new key goes to the new leaf
                var newIndex = index - leaf->info->count;
                Buffer.MemoryCopy(leaf->keys + leaf->info->count * BTreeNode.KEY_SIZE, newLeaf->keys, newIndex * BTreeNode.KEY_SIZE, newIndex * BTreeNode.KEY_SIZE);
                newLeaf->SetKey(newIndex, key);

                Buffer.MemoryCopy(leaf->keys + index * BTreeNode.KEY_SIZE, newLeaf->keys + (newIndex + 1) * BTreeNode.KEY_SIZE, (BTreeNode.LEAF_CAPACITY - index) * BTreeNode.KEY_SIZE, (BTreeNode.LEAF_CAPACITY - index) * BTreeNode.KEY_SIZE);
                Buffer.MemoryCopy(leaf->data.values + leaf->info->count, newLeaf->data.values, newIndex * sizeof(Value), newIndex * sizeof(Value));
                newLeaf->SetValue(newIndex, value);
                Buffer.MemoryCopy(leaf->data.values + index, newLeaf->data.values + newIndex + 1, (BTreeNode.LEAF_CAPACITY - index) * sizeof(Value), (BTreeNode.LEAF_CAPACITY - index) * sizeof(Value));
                newLeaf->info->validCount++;
            }
            else
            {
                Buffer.MemoryCopy(leaf->keys + (leaf->info->count - 1) * BTreeNode.KEY_SIZE, newLeaf->keys, newLeaf->info->count * BTreeNode.KEY_SIZE, newLeaf->info->count * BTreeNode.KEY_SIZE);
                Buffer.MemoryCopy(leaf->keys + index * BTreeNode.KEY_SIZE, leaf->keys + ((index + 1) * BTreeNode.KEY_SIZE), (leaf->info->count - index - 1) * BTreeNode.KEY_SIZE, (leaf->info->count - index - 1) * BTreeNode.KEY_SIZE);
                leaf->SetKey(index, key);

                Buffer.MemoryCopy(leaf->data.values + leaf->info->count - 1, newLeaf->data.values, newLeaf->info->count * sizeof(Value), newLeaf->info->count * sizeof(Value));
                Buffer.MemoryCopy(leaf->data.values + index, leaf->data.values + index + 1, (leaf->info->count - index - 1) * sizeof(Value), (leaf->info->count - index - 1) * sizeof(Value));
                leaf->SetValue(index, value);
                leaf->info->validCount++;
            }
            
            uint validCount = 0;
            // the leaf that is split will also be the tail node; so update the tail pointer
            if (leaf == tail)
            {
                tail = newLeaf;
                tailMinKey = newLeaf->GetKey(0);
                rootToTailLeaf[0] = newLeaf;
                // validCount in internal nodes of the index excludes the validCount of the tail leaf node (optimizing for performance to avoid traversal)
                // thus, when we split the tail leaf, we push up the validCount of the leaf that we split to the internal node
                validCount = leaf->info->validCount;
            }

            // update the parent node with the new key
            PushUpKeyInInternalNode(ref nodesTraversed, newLeaf->GetKey(0), ref newLeaf, SPLIT_INTERNAL_POSITION, validCount);
            return true;
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

                if (node->info->count < BTreeNode.INTERNAL_CAPACITY)
                {
                    // // TODO: potentially get rid of this as we will also only be appending to end of internal node due to sorted insertions
                    // Buffer.MemoryCopy(node->keys + index * BTreeNode.KEY_SIZE, node->keys + ((index + 1) * BTreeNode.KEY_SIZE),
                    // (node->info->count - index) * BTreeNode.KEY_SIZE, (node->info->count - index) * BTreeNode.KEY_SIZE);
                    // // move all children 
                    // for (var j = node->info->count; j > index; j--)
                    // {
                    //     node->SetChild(j + 1, node->GetChild(j));
                    // }

                    // node->SetKey(index, key);
                    // node->SetChild(index + 1, child);
                    // node->info->count++;
                    // node->info->validCount += newValidCount;

                    // we can insert 
                    InsertToInternalNodeWithinCapacity(ref node, key, ref child, ref nodesTraversed, index, newValidCount);

                    // insert does not cascade up, so update validCounts in the parent nodes
                    for (var j = i + 1; j < stats.depth; j++)
                    {
                        nodesTraversed[j]->info->validCount += newValidCount;
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

        public void InsertToInternalNodeWithinCapacity(ref BTreeNode* node, byte* key, ref BTreeNode* child, ref BTreeNode*[] nodesTraversed, int index, uint newValidCount)
        {
            // move all keys to the right 
            Buffer.MemoryCopy(node->keys + index * BTreeNode.KEY_SIZE, node->keys + ((index + 1) * BTreeNode.KEY_SIZE), (node->info->count - index) * BTreeNode.KEY_SIZE, (node->info->count - index) * BTreeNode.KEY_SIZE);
            // move all children starting from index+1 to the right using a for loop
            for (var j = node->info->count; j > index; j--)
            {
                node->SetChild(j + 1, node->GetChild(j));
            }

            // insert 
            node->SetKey(index, key);
            node->SetChild(index + 1, child);
            node->info->count++;
            node->info->validCount += newValidCount;
        }

        public BTreeNode* CreateInternalNode(ref BTreeNode* node, int splitPos)
        {
            // BTreeNode* newNode = (BTreeNode*)Marshal.AllocHGlobal(sizeof(BTreeNode));
            // var memoryBlock = bufferPool.Get(BTreeNode.PAGE_SIZE);
            var memoryBlock = (IntPtr*)Marshal.AllocHGlobal(BTreeNode.PAGE_SIZE).ToPointer();
            stats.numAllocates++;
            // BTreeNode* newNode = (BTreeNode*)memory;
            // // newNode->memoryHandle = memoryBlock;
            // newNode->Initialize(BTreeNodeType.Internal, memoryBlock);
            BTreeNode* newNode = BTreeNode.Create(BTreeNodeType.Internal, memoryBlock);
            stats.numInternalNodes++;
            node->info->count = splitPos;
            newNode->info->count = BTreeNode.INTERNAL_CAPACITY - splitPos;
            newNode->info->next = node->info->next;
            newNode->info->previous = node;
            node->info->next = newNode;
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
                    newValidCount += nodeToSplit->GetChild(i)->info->validCount;
                }
            }
            newNode->info->validCount = newValidCount;

            // we are inserting in sorted order, so child always goes to newNode
            // Buffer.MemoryCopy(nodeToSplit->keys + (nodeToSplit->info->count + 1) * BTreeNode.KEY_SIZE, newNode->keys, (index - nodeToSplit->info->count - 1) * BTreeNode.KEY_SIZE, (index - nodeToSplit->info->count - 1) * BTreeNode.KEY_SIZE);
            // Buffer.MemoryCopy(nodeToSplit->keys + index * BTreeNode.KEY_SIZE, newNode->keys + (index - nodeToSplit->info->count) * BTreeNode.KEY_SIZE, (BTreeNode.INTERNAL_CAPACITY - index) * BTreeNode.KEY_SIZE, (BTreeNode.INTERNAL_CAPACITY - index) * BTreeNode.KEY_SIZE);
            // newNode->SetKey(index - nodeToSplit->info->count - 1, key);
            // Buffer.MemoryCopy(nodeToSplit->data.children + 1 + nodeToSplit->info->count, newNode->data.children, (index - nodeToSplit->info->count) * sizeof(BTreeNode*), (index - nodeToSplit->info->count) * sizeof(BTreeNode*));
            // Buffer.MemoryCopy(nodeToSplit->data.children + 1 + index, newNode->data.children + 1 + index - nodeToSplit->info->count, newNode->info->count * sizeof(BTreeNode*), newNode->info->count * sizeof(BTreeNode*));
            // newNode->SetChild(index - nodeToSplit->info->count, child);

            // newNode->SetChild(0, nodeToSplit->GetChild(nodeToSplit->info->count)); // left child pointer of the new node part
            // newNode->SetKey(0, key);
            // newNode->SetChild(1, child);
            // newNode->info->count = 1;
            // // key = nodeToSplit->GetKey(nodeToSplit->info->count);
            // key = newNode->GetKey(0);

            // var childvalid = child->info->validCount;

            if (index > nodeToSplit->info->count)
            {
                // child goes to newNode
                Buffer.MemoryCopy(nodeToSplit->keys + (nodeToSplit->info->count + 1) * BTreeNode.KEY_SIZE, newNode->keys, (index - nodeToSplit->info->count - 1) * BTreeNode.KEY_SIZE, (index - nodeToSplit->info->count - 1) * BTreeNode.KEY_SIZE);
                Buffer.MemoryCopy(nodeToSplit->keys + index * BTreeNode.KEY_SIZE, newNode->keys + (index - nodeToSplit->info->count) * BTreeNode.KEY_SIZE, (BTreeNode.INTERNAL_CAPACITY - index) * BTreeNode.KEY_SIZE, (BTreeNode.INTERNAL_CAPACITY - index) * BTreeNode.KEY_SIZE);
                newNode->SetKey(index - nodeToSplit->info->count - 1, key);
                Buffer.MemoryCopy(nodeToSplit->data.children + 1 + nodeToSplit->info->count, newNode->data.children, (index - nodeToSplit->info->count) * sizeof(BTreeNode*), (index - nodeToSplit->info->count) * sizeof(BTreeNode*));
                Buffer.MemoryCopy(nodeToSplit->data.children + 1 + index, newNode->data.children + 1 + index - nodeToSplit->info->count, newNode->info->count * sizeof(BTreeNode*), newNode->info->count * sizeof(BTreeNode*));
                newNode->SetChild(index - nodeToSplit->info->count, child);
                key = nodeToSplit->GetKey(nodeToSplit->info->count);
            }
            else if (index == nodeToSplit->info->count)
            {
                Buffer.MemoryCopy(nodeToSplit->keys + nodeToSplit->info->count * BTreeNode.KEY_SIZE, newNode->keys, newNode->info->count * BTreeNode.KEY_SIZE, newNode->info->count * BTreeNode.KEY_SIZE);
                Buffer.MemoryCopy(nodeToSplit->data.children + 1 + nodeToSplit->info->count, newNode->data.children + 1, newNode->info->count * sizeof(BTreeNode*), newNode->info->count * sizeof(BTreeNode*));
                newNode->SetChild(0, child);
            }
            else
            {
                // child goes to old node
                Buffer.MemoryCopy(nodeToSplit->keys + nodeToSplit->info->count * BTreeNode.KEY_SIZE, newNode->keys, newNode->info->count * BTreeNode.KEY_SIZE, newNode->info->count * BTreeNode.KEY_SIZE);
                Buffer.MemoryCopy(nodeToSplit->keys + index * BTreeNode.KEY_SIZE, nodeToSplit->keys + ((index + 1) * BTreeNode.KEY_SIZE), (nodeToSplit->info->count - index) * BTreeNode.KEY_SIZE, (nodeToSplit->info->count - index) * BTreeNode.KEY_SIZE);
                nodeToSplit->SetKey(index, key);
                Buffer.MemoryCopy(nodeToSplit->data.children + nodeToSplit->info->count, newNode->data.children, newNode->info->count * sizeof(BTreeNode*), newNode->info->count * sizeof(BTreeNode*));
                Buffer.MemoryCopy(nodeToSplit->data.children + index + 1, nodeToSplit->data.children + index + 2, (nodeToSplit->info->count - index + 1) * sizeof(BTreeNode*), (nodeToSplit->info->count - index + 1) * sizeof(BTreeNode*));
                nodeToSplit->SetChild(index + 1, child);
                key = nodeToSplit->GetKey(nodeToSplit->info->count);
            }

            return newNode;
        }


        public void CreateNewRoot(byte* key, BTreeNode* newlySplitNode)
        {
            // BTreeNode* leftNode = (BTreeNode*)Marshal.AllocHGlobal(sizeof(BTreeNode)).ToPointer();
            // var memoryBlock = bufferPool.Get(BTreeNode.PAGE_SIZE);
            var memoryBlock = (IntPtr*)Marshal.AllocHGlobal(BTreeNode.PAGE_SIZE).ToPointer();
            stats.numAllocates++;
            // BTreeNode* newRoot = (BTreeNode*)memory;
            // // leftNode->memoryHandle = memoryBlock;
            // newRoot->Initialize(BTreeNodeType.Internal, memoryBlock);
            BTreeNode* newRoot = BTreeNode.Create(BTreeNodeType.Internal, memoryBlock);

            // Set the new root's key to the key being pushed up (key from newlySplitNode).
            newRoot->info->count = 1;
            newRoot->SetKey(0, key);
            // Set its children: left child is the old root; right child is the newly split node.
            newRoot->SetChild(0, root);
            newRoot->SetChild(1, newlySplitNode);

            // Update the valid count (if desired, handle validCount appropriately).
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