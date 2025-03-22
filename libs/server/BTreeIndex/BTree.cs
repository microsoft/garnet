// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tsavorite.core;

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

        SectorAlignedBufferPool bufferPool;

        /// <summary>
        /// Initializes a new instance of the <see cref="BTree"/> class.
        /// </summary>
        public BTree(uint sectorSize)
        {
            bufferPool = new SectorAlignedBufferPool(1, BTreeNode.PAGE_SIZE);
            // var memoryBlock = bufferPool.Get(BTreeNode.PAGE_SIZE);
            var memoryBlock = (IntPtr*)Marshal.AllocHGlobal(BTreeNode.PAGE_SIZE).ToPointer();
            stats.numAllocates = 1;
            // root = (BTreeNode*)memory;
            // root->memoryHandle = memoryBlock;
            // root->Initialize(BTreeNodeType.Leaf, memoryBlock);
            root = BTreeNode.Create(BTreeNodeType.Leaf, memoryBlock);
            head = tail = root;
            root->info->next = root->info->previous = null;
            root->info->count = 0;
            tailMinKey = null;
            rootToTailLeaf = new BTreeNode*[MAX_TREE_DEPTH];
            stats = new BTreeStats();
            stats.depth = 1;
            stats.numLeafNodes = 1;
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
                Marshal.FreeHGlobal((IntPtr)node->memoryHandle);
                stats.numDeallocates++;
                node = null;
            }
            
            

        
            // if (node->memoryHandle != null)
            // {
            //     node->memoryHandle.Return();
            //     stats.numDeallocates++;
            //     node->memoryHandle = null;
            // }
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
            
            // Marshal.FreeHGlobal((IntPtr)root);
            // Marshal.FreeHGlobal((IntPtr)head);
            // Marshal.FreeHGlobal((IntPtr)tail);
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
            byte[] keyBytes = new byte[BTreeNode.KEY_SIZE];
            Buffer.MemoryCopy(leaf->GetKey(0), Unsafe.AsPointer(ref keyBytes[0]), BTreeNode.KEY_SIZE, BTreeNode.KEY_SIZE);
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
            byte[] keyBytes = new byte[BTreeNode.KEY_SIZE];
            Buffer.MemoryCopy(leaf->GetKey(leaf->info->count - 1), Unsafe.AsPointer(ref keyBytes[0]), BTreeNode.KEY_SIZE, BTreeNode.KEY_SIZE);
            return new KeyValuePair<byte[], Value>(keyBytes, leaf->GetValue(leaf->info->count - 1));
        }

    }
}