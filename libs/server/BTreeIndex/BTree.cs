// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace Garnet.server.BTreeIndex
{
    public unsafe partial class BTree
    {
        BTreeNode* root;
        BTreeNode* head;
        BTreeNode* tail;
        byte* tailMinKey;
        public static readonly int MAX_TREE_DEPTH = 10; // maximum allowed depth of the tree
        static int DEFAULT_SPLIT_LEAF_POSITION = (BTreeNode.LEAF_CAPACITY + 1) / 2; // position at which leaf node is split
        static int SPLIT_LEAF_POSITION = BTreeNode.LEAF_CAPACITY; // position at which leaf node is split
        static int SPLIT_INTERNAL_POSITION = BTreeNode.INTERNAL_CAPACITY; // position at which internal node is split

        BTreeNode*[] rootToTailLeaf; // array of nodes from root to tail leaf
        public BTreeStats stats; // statistics about the tree

        /// <summary>
        /// Initializes a new instance of the <see cref="BTree"/> class.
        /// </summary>
        public BTree(uint sectorSize)
        {
            // HK TODO: This lives in memory always, so why allocate on native heap?
            var memoryBlock = (IntPtr*)NativeMemory.AlignedAlloc((nuint)BTreeNode.PAGE_SIZE, (nuint)BTreeNode.PAGE_SIZE);
            root = BTreeNode.Create(BTreeNodeType.Leaf, memoryBlock);
            head = tail = root;
            root->info->next = root->info->previous = null;
            root->info->count = 0;
            tailMinKey = null;
            rootToTailLeaf = new BTreeNode*[MAX_TREE_DEPTH];
            stats = new BTreeStats();
            stats.depth = 1;
            stats.numLeafNodes = 1;
            stats.numAllocates = 1;
        }

        /// <summary>
        /// Frees the memory allocated for a node
        /// </summary>
        /// <param name="node">BTreeNode to free from memory</param>
        private void Free(ref BTreeNode* node)
        {
            if (node == null)
                return;

            // If this is an internal node, free all its children first
            if (node->info->type == BTreeNodeType.Internal)
            {
                for (int i = 0; i <= node->info->count; i++)
                {
                    var child = node->data.children[i];
                    Free(ref child);
                    node->data.children[i] = null;
                }
            }

            // Free the memory handle
            if (node->memoryHandle != null)
            {
                NativeMemory.Free(node->memoryHandle);
                stats.numDeallocates++;
                node = null;
            }
        }

        /// <summary>
        /// Frees the memory allocated for a node
        /// </summary>
        /// <param name="node"></param>
        public static void FreeNode(ref BTreeNode* node)
        {
            if (node == null)
                return;

            // If this is an internal node, free all its children first
            if (node->info->type == BTreeNodeType.Internal)
            {
                for (int i = 0; i <= node->info->count; i++)
                {
                    var child = node->data.children[i];
                    FreeNode(ref child);
                    node->data.children[i] = null;
                }
            }

            // Free the memory handle
            if (node->memoryHandle != null)
            {
                NativeMemory.Free(node->memoryHandle);
                node = null;
            }
        }

        public static void Deallocate(ref BTreeNode* node)
        {
            // Free the memory handle
            if (node->memoryHandle != null)
            {
                NativeMemory.Free(node->memoryHandle);
                node->info = null;
                node->keys = null;
                node->data.values = null;
                node->data.children = null;
                node->memoryHandle = null;
            }
        }

        /// <summary>
        /// Deallocates the memory allocated for the B+Tree
        /// </summary>
        public void Deallocate()
        {
            if (root == null)
                return;
            Free(ref root);
            Console.WriteLine("free complete");
            stats.printStats();
            root = null;
            head = null;
            tail = null;
        }

        /// <summary>
        /// Destructor for the B+tree
        /// </summary>
        ~BTree()
        {
            Deallocate();
        }

        public ulong FastInserts => stats.totalFastInserts;
        public ulong LeafCount => stats.numLeafNodes;
        public ulong InternalCount => stats.numInternalNodes;

        public ulong ValidCount => StatsValidCount();

        public long RootValidCount => GetValidCount(root);

        public long TailValidCount => GetValidCount(tail);

        public long Count()
        {
            return stats.numKeys;
        }
        public ulong StatsValidCount()
        {
            return stats.numValidKeys;
        }

        public long GetValidCount(BTreeNode* node)
        {
            return node->info->validCount;
        }

        /// <summary>
        /// Retrieves the first entry in the B+Tree (smallest key)
        /// </summary>
        /// <returns>entry fetched</returns>
        public KeyValuePair<byte[], Value> First()
        {
            BTreeNode* leaf = head;
            if (leaf == null)
            {
                return default;
            }
            byte[] keyBytes = new ReadOnlySpan<byte>(leaf->GetKey(0), BTreeNode.KEY_SIZE).ToArray();
            return new KeyValuePair<byte[], Value>(keyBytes, leaf->GetValue(0));
        }

        /// <summary>
        /// Retrieves the last entry in the B+Tree (largest key)
        /// </summary>
        /// <returns>entry fetched</returns>
        public KeyValuePair<byte[], Value> Last()
        {
            BTreeNode* leaf = tail;
            if (leaf == null)
            {
                return default;
            }
            byte[] keyBytes = new ReadOnlySpan<byte>(leaf->GetKey(leaf->info->count - 1), BTreeNode.KEY_SIZE).ToArray();
            return new KeyValuePair<byte[], Value>(keyBytes, leaf->GetValue(leaf->info->count - 1));
        }

    }
}