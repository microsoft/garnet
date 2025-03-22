// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Garnet.server.BTreeIndex
{
    public unsafe partial class BTree
    {
        /// <summary>
        /// Point lookup in the index
        /// </summary>
        /// <param name="key">lookup key</param>
        /// <returns></returns>
        public Value Get(byte* key)
        {
            BTreeNode* leaf = null;
            var nodesTraversed = new BTreeNode*[MAX_TREE_DEPTH];
            TraverseToLeaf(ref leaf, ref nodesTraversed, key);

            var index = leaf->LowerBound(key);
            if (index < leaf->info->count && BTreeNode.Compare(key, leaf->GetKey(index)) == 0)
            {
                var value = leaf->GetValue(index);
                if (value.Valid)
                {
                    return value;
                }
            }
            return default;
        }

        /// <summary>
        /// Range lookup in the index
        /// </summary>
        /// <param name="start">start key for the range lookup</param>
        /// <param name="end">end key for the range lookup</param>
        /// <param name="startVal">address of the start key</param>
        /// <param name="endVal">address of end key</param>
        /// <param name="tombstones">list of tombstones</param>
        /// <param name="limit">limit entries scanned in the range lookup</param>
        /// <param name="reverse">reverse lookup</param>
        /// <returns></returns>
        public int Get(byte* start, byte* end, out Value startVal, out Value endVal, out List<Value> tombstones, long limit = -1, bool reverse = false)
        {
            Debug.Assert(reverse ?
            BTreeNode.Compare(start, end) >= 0 : BTreeNode.Compare(start, end) <= 0,
            "Start key should be less than or equal to end key");
            int count = 0;
            tombstones = new List<Value>();
            BTreeNode* startLeaf = null, endLeaf = null;
            BTreeNode*[] nodesTraversed = new BTreeNode*[MAX_TREE_DEPTH];
            int startIndex, endIndex;

            // find the leaf node for the start key
            TraverseToLeaf(ref startLeaf, ref nodesTraversed, start);
            // find the leaf node for the end key
            TraverseToLeaf(ref endLeaf, ref nodesTraversed, end);

            if (reverse)
            {
                // we find the first slot > start and subtract one index to get the start index
                startIndex = startLeaf->UpperBound(start) - 1;
                startVal = startLeaf->GetValue(startIndex);

                // we find the first value greater than equal to key and that will be the last index 
                endIndex = endLeaf->LowerBound(end);
                endVal = endLeaf->GetValue(endIndex);
            }
            else
            {
                // find the first key in the start leaf that is greater than or equal to the start key
                startIndex = startLeaf->LowerBound(start);
                startVal = startLeaf->GetValue(startIndex);
                // find the last key in the end leaf that is less than or equal to the end key
                endIndex = endLeaf->UpperBound(end) - 1;
                endVal = endLeaf->GetValue(endIndex);
            }


            // now, we iterate over the leaves between startLeaf[startIndex] and endLeaf[endIndex] (inclusive) and collect all tombstones
            BTreeNode* leaf = startLeaf;
            uint numScanned = 0;
            while (leaf != null)
            {
                int first, last;
                bool breakOutOfOuterLoop = false;
                if (reverse)
                {
                    // we would like an inverse traversal 
                    first = leaf == startLeaf ? startIndex : leaf->info->count - 1;
                    last = leaf == endLeaf ? endIndex : 0;
                }
                else
                {
                    last = leaf == endLeaf ? endIndex : leaf->info->count - 1;
                    first = leaf == startLeaf ? startIndex : 0;
                }

                for (var i = first; ;)
                {
                    numScanned++;
                    var value = leaf->GetValue(i);
                    if (!value.Valid)
                    {
                        byte[] key = new byte[BTreeNode.KEY_SIZE];
                        Buffer.MemoryCopy(leaf->GetKey(i), Unsafe.AsPointer(ref key[0]), BTreeNode.KEY_SIZE, BTreeNode.KEY_SIZE);
                        tombstones.Add(leaf->GetValue(i));
                    }
                    else
                    {
                        // entry will be part of result set 
                        count++;
                        if (limit != -1 && count >= limit)
                        {
                            // update address as required
                            if (reverse)
                            {
                                startVal = value;
                            }
                            else
                            {
                                endVal = value;
                            }
                            breakOutOfOuterLoop = true;
                            break;
                        }
                    }

                    if (reverse)
                    {
                        if (i <= last)
                        {
                            break;
                        }
                        i--;
                    }
                    else
                    {
                        if (i >= last)
                        {
                            break;
                        }
                        i++;
                    }
                }
                // if we have reached the endLeaf
                if (leaf == endLeaf || breakOutOfOuterLoop)
                {
                    break;
                }

                leaf = reverse ? leaf->info->previous : leaf->info->next;
            }
            return count;
        }
    }
}