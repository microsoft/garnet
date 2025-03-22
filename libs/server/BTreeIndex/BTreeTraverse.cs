// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server.BTreeIndex
{
    public unsafe partial class BTree
    {
        public byte* TraverseToLeaf(ref BTreeNode* node, ref BTreeNode*[] nodesTraversed, byte* key)
        {
            byte* leafMax = null;
            BTreeNode* child = root;
            for (var i = stats.depth - 1; i > 0; --i)
            {
                node = child;
                nodesTraversed[i] = child;
                var slot = node->UpperBound(key);
                if (slot != node->info->count)
                {
                    leafMax = node->GetKey(slot);
                }
                child = node->GetChild(slot);
            }
            node = child;
            nodesTraversed[0] = child;
            return leafMax;
        }

        public byte* TraverseToLeaf(ref BTreeNode* node, ref BTreeNode*[] nodesTraversed, byte* key, out int[] slots)
        {
            slots = new int[MAX_TREE_DEPTH];
            byte* leafMax = null;
            BTreeNode* child = root;
            for (var i = stats.depth - 1; i > 0; --i)
            {
                node = child;
                nodesTraversed[i] = child;
                var slot = node->UpperBound(key);
                slots[i] = slot;
                if (slot != node->info->count)
                {
                    leafMax = node->GetKey(slot);
                }
                child = node->GetChild(slot);
            }
            node = child;
            nodesTraversed[0] = child;
            return leafMax;
        }
    }
}