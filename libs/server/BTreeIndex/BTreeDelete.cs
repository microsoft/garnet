// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server.BTreeIndex
{
    public unsafe partial class BTree
    {
        /// <summary>
        /// Delete a key from the B+tree
        /// </summary>
        /// <param name="key">key to delete</param>
        /// <returns>true if key was tombstoned</returns>
        public bool Delete(byte* key)
        {
            BTreeNode* leaf = null;
            var nodesTraversed = new BTreeNode*[MAX_TREE_DEPTH];

            TraverseToLeaf(ref leaf, ref nodesTraversed, key);
            var index = leaf->LowerBound(key);
            if (index >= leaf->info->count || BTreeNode.Compare(key, leaf->GetKey(index)) != 0)
            {
                return false;
            }

            // insert a tombstone for the delete 
            leaf->InsertTombstone(index);
            leaf->info->validCount--;
            stats.numValidKeys--;
            return true;
        }
    }
}