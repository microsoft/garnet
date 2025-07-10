// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;

namespace Garnet.server.BTreeIndex
{
    public unsafe partial class BTree
    {
        /// <summary>
        /// Insert a key-value pair into the B+tree. Directly inserts into the tail leaf node. 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns>true if insertion is successful</returns>
        public bool Insert(byte* key, Value value)
        {
            BTreeNode* leaf = null;
            stats.totalFastInserts++;
            stats.totalInserts++;
            stats.numKeys++;
            stats.numValidKeys++;
            leaf = tail;
            return InsertToLeafNode(ref leaf, ref rootToTailLeaf, key, value, true);
        }

        public bool Insert(byte* key, ReadOnlySpan<byte> keySpan, Value value)
        {
            BTreeNode* leaf = null;
            stats.totalFastInserts++;
            stats.totalInserts++;
            stats.numKeys++;
            stats.numValidKeys++;
            leaf = tail;
            return InsertToLeafNode(ref leaf, ref rootToTailLeaf, key, value, true);
        }
        public bool InsertToLeafNode(ref BTreeNode* leaf, ref BTreeNode*[] nodesTraversed, byte* key, Value value, bool appendToLeaf = false)
        {
            int index;
            if (appendToLeaf)
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
                // move keys to the right of index
                var sourceSpan = new ReadOnlySpan<byte>(leaf->keys + index * BTreeNode.KEY_SIZE, (leaf->info->count - index) * BTreeNode.KEY_SIZE);
                var destinationSpan = new Span<byte>(leaf->keys + ((index + 1) * BTreeNode.KEY_SIZE), (leaf->info->count - index) * BTreeNode.KEY_SIZE);
                sourceSpan.CopyTo(destinationSpan);

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
            var memoryBlock = (IntPtr*)NativeMemory.AlignedAlloc((nuint)BTreeNode.PAGE_SIZE, (nuint)BTreeNode.PAGE_SIZE);
            stats.numAllocates++;
            BTreeNode* newLeaf = BTreeNode.Create(BTreeNodeType.Leaf, memoryBlock);

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
            // insert the new key to either the old node or the newly created node, based on the index
            if (index >= leaf->info->count)
            {
                // new key goes to the new leaf
                var newIndex = index - leaf->info->count;

                // move the keys from old node to the new node using ReadOnlySpan
                var sourceSpan = new ReadOnlySpan<byte>(leaf->keys + index * BTreeNode.KEY_SIZE, newIndex * BTreeNode.KEY_SIZE);
                var destinationSpan = new Span<byte>(newLeaf->keys, newIndex * BTreeNode.KEY_SIZE);
                sourceSpan.CopyTo(destinationSpan);

                // add key to new leaf
                newLeaf->SetKey(newIndex, key);

                var existingLeafKeysSpan = new ReadOnlySpan<byte>(leaf->keys + index * BTreeNode.KEY_SIZE, (BTreeNode.LEAF_CAPACITY - index) * BTreeNode.KEY_SIZE);
                var newLeafKeysSpan = new Span<byte>(newLeaf->keys + (newIndex + 1) * BTreeNode.KEY_SIZE, (BTreeNode.LEAF_CAPACITY - index) * BTreeNode.KEY_SIZE);
                existingLeafKeysSpan.CopyTo(newLeafKeysSpan);

                var existingLeafValuesSpan = new ReadOnlySpan<Value>(leaf->data.values + leaf->info->count, newIndex * sizeof(Value));
                var newLeafValuesSpan = new Span<Value>(newLeaf->data.values, newIndex * sizeof(Value));
                existingLeafValuesSpan.CopyTo(newLeafValuesSpan);
                newLeaf->SetValue(newIndex, value);

                var existingLeafValuesSpan2 = new ReadOnlySpan<Value>(leaf->data.values + index, (BTreeNode.LEAF_CAPACITY - index) * sizeof(Value));
                var newLeafValuesSpan2 = new Span<Value>(newLeaf->data.values + newIndex + 1, (BTreeNode.LEAF_CAPACITY - index) * sizeof(Value));
                existingLeafValuesSpan2.CopyTo(newLeafValuesSpan2);
                newLeaf->info->validCount++;
            }
            else
            {
                var existingLeafKeysSpan = new ReadOnlySpan<byte>(leaf->keys + (leaf->info->count - 1) * BTreeNode.KEY_SIZE, newLeaf->info->count * BTreeNode.KEY_SIZE);
                var newLeafKeysSpan = new Span<byte>(newLeaf->keys, newLeaf->info->count * BTreeNode.KEY_SIZE);
                existingLeafKeysSpan.CopyTo(newLeafKeysSpan);

                var existingLeafKeysSpan2 = new ReadOnlySpan<byte>(leaf->keys + index * BTreeNode.KEY_SIZE, (leaf->info->count - index - 1) * BTreeNode.KEY_SIZE);
                var newLeafKeysSpan2 = new Span<byte>(leaf->keys + ((index + 1) * BTreeNode.KEY_SIZE), (leaf->info->count - index - 1) * BTreeNode.KEY_SIZE);
                existingLeafKeysSpan2.CopyTo(newLeafKeysSpan2);
                leaf->SetKey(index, key);

                var existingLeafValuesSpan = new ReadOnlySpan<Value>(leaf->data.values + leaf->info->count - 1, newLeaf->info->count * sizeof(Value));
                var newLeafValuesSpan = new Span<Value>(newLeaf->data.values, newLeaf->info->count * sizeof(Value));
                existingLeafValuesSpan.CopyTo(newLeafValuesSpan);

                var existingLeafValuesSpan2 = new ReadOnlySpan<Value>(leaf->data.values + index, (leaf->info->count - index - 1) * sizeof(Value));
                var newLeafValuesSpan2 = new Span<Value>(leaf->data.values + index + 1, (leaf->info->count - index - 1) * sizeof(Value));
                existingLeafValuesSpan2.CopyTo(newLeafValuesSpan2);
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
                    // we can insert 
                    InsertToInternalNodeWithinCapacity(ref node, key, ref child, ref nodesTraversed, index, newValidCount);

                    // update validCounts in the parent nodes
                    for (var j = i + 1; j < stats.depth; j++)
                    {
                        nodesTraversed[j]->info->validCount += newValidCount;
                    }
                    return;
                }

                // split internal node
                node->info->validCount += newValidCount;
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
            var sourceSpan = new ReadOnlySpan<byte>(node->keys + index * BTreeNode.KEY_SIZE, (node->info->count - index) * BTreeNode.KEY_SIZE);
            var destinationSpan = new Span<byte>(node->keys + ((index + 1) * BTreeNode.KEY_SIZE), (node->info->count - index) * BTreeNode.KEY_SIZE);
            sourceSpan.CopyTo(destinationSpan);

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
            var memoryBlock = (IntPtr*)NativeMemory.AlignedAlloc((nuint)BTreeNode.PAGE_SIZE, (nuint)BTreeNode.PAGE_SIZE);
            stats.numAllocates++;
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

            if (index > nodeToSplit->info->count)
            {
                // child goes to newNode
                var sourceSpan = new ReadOnlySpan<byte>(nodeToSplit->keys + (nodeToSplit->info->count + 1) * BTreeNode.KEY_SIZE, (index - nodeToSplit->info->count - 1) * BTreeNode.KEY_SIZE);
                var destinationSpan = new Span<byte>(newNode->keys, (index - nodeToSplit->info->count - 1) * BTreeNode.KEY_SIZE);
                sourceSpan.CopyTo(destinationSpan);

                var existingNodeKeysSpan = new ReadOnlySpan<byte>(nodeToSplit->keys + index * BTreeNode.KEY_SIZE, (BTreeNode.INTERNAL_CAPACITY - index) * BTreeNode.KEY_SIZE);
                var newNodeKeysSpan = new Span<byte>(newNode->keys + (index - nodeToSplit->info->count) * BTreeNode.KEY_SIZE, (BTreeNode.INTERNAL_CAPACITY - index) * BTreeNode.KEY_SIZE);
                existingNodeKeysSpan.CopyTo(newNodeKeysSpan);
                newNode->SetKey(index - nodeToSplit->info->count - 1, key);

                var existingNodeChildrenSpan = new ReadOnlySpan<byte>(nodeToSplit->data.children + 1 + nodeToSplit->info->count, (index - nodeToSplit->info->count) * sizeof(BTreeNode*));
                var newNodeChildrenSpan = new Span<byte>(newNode->data.children, (index - nodeToSplit->info->count) * sizeof(BTreeNode*));
                existingNodeChildrenSpan.CopyTo(newNodeChildrenSpan);

                var existingNodeChildrenSpan2 = new ReadOnlySpan<byte>(nodeToSplit->data.children + 1 + index, newNode->info->count * sizeof(BTreeNode*));
                var newNodeChildrenSpan2 = new Span<byte>(newNode->data.children + 1 + index - nodeToSplit->info->count, newNode->info->count * sizeof(BTreeNode*));
                existingNodeChildrenSpan2.CopyTo(newNodeChildrenSpan2);
                newNode->SetChild(index - nodeToSplit->info->count, child);
                key = nodeToSplit->GetKey(nodeToSplit->info->count);
            }
            else if (index == nodeToSplit->info->count)
            {
                var sourceSpan = new ReadOnlySpan<byte>(nodeToSplit->keys + nodeToSplit->info->count * BTreeNode.KEY_SIZE, newNode->info->count * BTreeNode.KEY_SIZE);
                var destinationSpan = new Span<byte>(newNode->keys, newNode->info->count * BTreeNode.KEY_SIZE);
                sourceSpan.CopyTo(destinationSpan);

                var existingNodeChildrenSpan = new ReadOnlySpan<byte>(nodeToSplit->data.children + 1 + nodeToSplit->info->count, newNode->info->count * sizeof(BTreeNode*));
                var newNodeChildrenSpan = new Span<byte>(newNode->data.children + 1, newNode->info->count * sizeof(BTreeNode*));
                existingNodeChildrenSpan.CopyTo(newNodeChildrenSpan);
                newNode->SetChild(0, child);
            }
            else
            {
                // child goes to old node
                var sourceSpan = new ReadOnlySpan<byte>(nodeToSplit->keys + nodeToSplit->info->count * BTreeNode.KEY_SIZE, newNode->info->count * BTreeNode.KEY_SIZE);
                var destinationSpan = new Span<byte>(newNode->keys, newNode->info->count * BTreeNode.KEY_SIZE);
                sourceSpan.CopyTo(destinationSpan);

                var existingNodeKeysSpan = new ReadOnlySpan<byte>(nodeToSplit->keys + index * BTreeNode.KEY_SIZE, (nodeToSplit->info->count - index) * BTreeNode.KEY_SIZE);
                var newNodeKeysSpan = new Span<byte>(nodeToSplit->keys + ((index + 1) * BTreeNode.KEY_SIZE), (nodeToSplit->info->count - index) * BTreeNode.KEY_SIZE);
                existingNodeKeysSpan.CopyTo(newNodeKeysSpan);
                nodeToSplit->SetKey(index, key);

                var existingNodeChildrenSpan = new ReadOnlySpan<byte>(nodeToSplit->data.children + nodeToSplit->info->count, newNode->info->count * sizeof(BTreeNode*));
                var newNodeChildrenSpan = new Span<byte>(newNode->data.children, newNode->info->count * sizeof(BTreeNode*));
                existingNodeChildrenSpan.CopyTo(newNodeChildrenSpan);

                var existingNodeChildrenSpan2 = new ReadOnlySpan<byte>(nodeToSplit->data.children + index + 1, (nodeToSplit->info->count - index + 1) * sizeof(BTreeNode*));
                var newNodeChildrenSpan2 = new Span<byte>(nodeToSplit->data.children + index + 2, (nodeToSplit->info->count - index + 1) * sizeof(BTreeNode*));
                existingNodeChildrenSpan2.CopyTo(newNodeChildrenSpan2);
                nodeToSplit->SetChild(index + 1, child);
                key = nodeToSplit->GetKey(nodeToSplit->info->count);
            }

            return newNode;
        }


        public void CreateNewRoot(byte* key, BTreeNode* newlySplitNode)
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