// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.X86;

namespace Garnet.server.BTreeIndex
{

    public enum BTreeNodeType
    {
        Internal,
        Leaf
    }

    /// <summary>
    /// Represents information stored in a node in the B+tree
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    public unsafe struct NodeData
    {
        [FieldOffset(0)]
        public Value* values;
        [FieldOffset(0)]
        public BTreeNode** children;
    }

    /// <summary>
    /// A leaf value in the B+tree: a logical log address together with a validity (tombstone) flag,
    /// packed into a single 64-bit word. The sign bit (bit 63) holds validity and the low 63 bits hold
    /// the address. Tsavorite logical addresses occupy at most 48 bits, so the high bits (including the
    /// sign bit) are always clear on a real address and free to repurpose.
    /// </summary>
    public struct Value
    {
        // Sign bit marks a live (non-tombstoned) entry; the remaining bits hold the log address.
        const long ValidBitMask = 1L << 63;
        const long AddressBitMask = ~ValidBitMask;

        long word;

        /// <summary>Whether the entry is live (true) or a tombstone (false).</summary>
        public bool Valid
        {
            readonly get => (word & ValidBitMask) != 0;
            set => word = value ? word | ValidBitMask : word & AddressBitMask;
        }

        /// <summary>The logical log address the entry points to.</summary>
        public long address
        {
            readonly get => word & AddressBitMask;
            set => word = (word & ValidBitMask) | (value & AddressBitMask);
        }

        public Value(long address)
        {
            word = address | ValidBitMask;
        }
    }

    public unsafe struct NodeInfo
    {
        public BTreeNodeType type;
        public int count;
        public BTreeNode* next;
        public BTreeNode* previous;
        public uint validCount; // valid keys (non-tombstone keys) in the node.
    }

    /// <summary>
    /// Represents a node in the B+tree
    /// Memory layout:
    /// +-----------------------------------+
    /// | BTreeNode (HEADER_SIZE bytes)     |
    /// |   - NodeInfo* info                |
    /// |   - NodeData data                 |
    /// |   - byte* keys                    |
    /// |   - IntPtr* memoryHandle          |
    /// +-----------------------------------+
    /// | NodeInfo (METADATA_SIZE bytes)    |
    /// |   - BTreeNodeType type            |
    /// |   - int count                     |
    /// |   - BTreeNode* next               |
    /// |   - BTreeNode* previous           |
    /// |   - uint validCount               |
    /// +-----------------------------------+
    /// | Keys array: capacity * KEY_SIZE   |
    /// +-----------------------------------+
    /// | Data array: either Value[] (leaf) |
    /// | or BTreeNode*[] (internal)        |
    /// +-----------------------------------+
    /// Expects an allocated block of memory (of size BTreeNode.PAGE_SIZE) to be passed as handle
    /// Stores handle for deallocation
    /// BTreeNode struct also contained within the 4KB block to allow pointers to created nodes to be passed around
    /// as well as allow for on-demand allocation/deallocation.
    /// NOTE: currently reverted to MemoryMarshal for allocation of handles due to undefined behavior with SectorAlignedMemory.
    /// </summary>
    public unsafe struct BTreeNode
    {
        public static int HEADER_SIZE = sizeof(BTreeNode);
        public static int PAGE_SIZE = 4096; // This must be increased if you want to store the BTreeNode header in the block.
        public static int KEY_SIZE = 16; // key size in bytes.
        public static int METADATA_SIZE = sizeof(NodeInfo);
        public static int LEAF_CAPACITY = (PAGE_SIZE - HEADER_SIZE - METADATA_SIZE) / (KEY_SIZE + sizeof(Value));
        public static int INTERNAL_CAPACITY = (PAGE_SIZE - HEADER_SIZE - METADATA_SIZE - sizeof(BTreeNode*)) / (KEY_SIZE + sizeof(BTreeNode*));

        public NodeInfo* info;
        public NodeData data;
        public byte* keys;
        public IntPtr* memoryHandle;

        public static BTreeNode* Create(BTreeNodeType type, IntPtr* handle)
        {
            // Place the node header at the beginning of the block.
            BTreeNode* node = (BTreeNode*)handle;
            node->memoryHandle = handle;

            // Define the start of the payload right after the header.
            byte* payloadPtr = (byte*)(handle) + HEADER_SIZE;

            // The NodeInfo will be stored at the start of the payload.
            node->info = (NodeInfo*)payloadPtr;
            node->info->type = type;
            node->info->count = 0;
            node->info->next = null;
            node->info->previous = null;
            node->info->validCount = 0;

            // Data for keys follows the Nodeinfo->
            byte* keysPtr = payloadPtr + METADATA_SIZE;
            node->keys = keysPtr;

            int capacity = (type == BTreeNodeType.Leaf) ? LEAF_CAPACITY : INTERNAL_CAPACITY;
            int keysSize = capacity * KEY_SIZE;
            byte* dataSectionPtr = keysPtr + keysSize;

            // Set up NodeData in-place.
            if (type == BTreeNodeType.Leaf)
            {
                node->data.values = (Value*)dataSectionPtr;
            }
            else
            {
                node->data.children = (BTreeNode**)dataSectionPtr;
            }

            return node;
        }

        public byte* GetKey(int index)
        {
            byte* keyAddress = keys + (index * KEY_SIZE);
            return keyAddress;
        }

        public void SetKey(int index, byte* keyData)
        {
            var sourceSpan = new ReadOnlySpan<byte>(keyData, KEY_SIZE);
            var destinationSpan = new Span<byte>(keys + (index * KEY_SIZE), KEY_SIZE);
            sourceSpan.CopyTo(destinationSpan);
        }

        public void SetChild(int index, BTreeNode* child)
        {
            data.children[index] = child;
        }

        public BTreeNode* GetChild(int index)
        {
            return data.children[index];
        }

        public void SetValue(int index, Value value)
        {
            data.values[index] = value;
        }

        public Value GetValue(int index)
        {
            return data.values[index];
        }

        public void SetValueValid(int index, bool valid)
        {
            data.values[index].Valid = valid;
        }

        public bool IsValueValid(int index)
        {
            return data.values[index].Valid;
        }

        public void InsertTombstone(int index)
        {
            data.values[index].Valid = false;
        }

        /// <summary>
        /// Returns the index of the first key greater than the given key
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public int UpperBound(byte* key)
        {
            if (info->count == 0)
            {
                return 0;
            }
            int left = 0, right = info->count - 1;
            while (left <= right)
            {
                var mid = left + (right - left) / 2;
                byte* midKey = GetKey(mid);
                int cmp = Compare(key, midKey);
                if (cmp < 0)
                {
                    right = mid - 1;
                }
                else
                {
                    left = mid + 1;
                }
            }
            return left;
        }

        /// <summary>
        /// Returns the index of the first key less than the given key
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public int LowerBound(byte* key)
        {
            if (info->count == 0)
            {
                return 0;
            }
            // Binary search for the first key >= given key
            int left = 0, right = info->count - 1;
            while (left <= right)
            {
                var mid = left + (right - left) / 2;
                byte* midKey = GetKey(mid);
                int cmp = Compare(midKey, key);
                if (cmp == 0)
                {
                    return mid;
                }
                else if (cmp < 0)
                {
                    left = mid + 1;
                }
                else
                {
                    right = mid - 1;
                }
            }
            return left;
        }

        /// <summary>
        /// Compares two keys
        /// </summary>
        /// <param name="key1"></param>
        /// <param name="key2"></param>
        /// <returns>-1 if key1 is less than key2; 0 if key1 == key2; 1 if key1 > key2</returns>
        public static int Compare(byte* key1, byte* key2)
        {

            if (Sse2.IsSupported)
            {
                var v1 = Sse2.LoadVector128(key1);
                var v2 = Sse2.LoadVector128(key2);

                var mask = Sse2.MoveMask(Sse2.CompareEqual(v1, v2));

                if (mask != 0xFFFF) // Not all bytes are equal
                {
                    // Find the index of the first differing byte
                    int index = BitOperations.TrailingZeroCount(~mask); // Invert mask to find first zero (differing byte)
                    return key1[index] < key2[index] ? -1 : 1;
                }

                return 0; // Arrays are equal
            }
            else
            {
                return new Span<byte>(key1, KEY_SIZE).SequenceCompareTo(new Span<byte>(key2, KEY_SIZE));
            }
        }
    }

    /// <summary>
    /// Statistics about the B+Tree
    /// </summary>
    public struct BTreeStats
    {
        // general index stats
        public int depth;
        public ulong numLeafNodes;
        public ulong numInternalNodes;

        // workload specific stats
        public long totalInserts;           // cumulative number of inserts to the index
        public long totalDeletes;           // cumulative number of deletes to the index
        public ulong totalFastInserts;       // cumulative number of fast inserts to the index
        public long numKeys;                // number of keys currently indexed
        public ulong numValidKeys;           //  number of keys that are not tombstoned 
        public ulong numAllocates;
        public ulong numDeallocates;
        public BTreeStats()
        {
            depth = 0;
            numLeafNodes = 0;
            numInternalNodes = 0;
            totalInserts = 0;
            totalDeletes = 0;
            totalFastInserts = 0;
            numKeys = 0;
            numValidKeys = 0;
            numAllocates = 0;
            numDeallocates = 0;
        }

        public void printStats()
        {
            Console.WriteLine($"Depth: {depth}");
            Console.WriteLine($"Number of leaf nodes: {numLeafNodes}");
            Console.WriteLine($"Number of internal nodes: {numInternalNodes}");
            Console.WriteLine($"Total inserts: {totalInserts}");
            Console.WriteLine($"Total deletes: {totalDeletes}");
            Console.WriteLine($"Total fast inserts: {totalFastInserts}");
            Console.WriteLine($"Number of keys: {numKeys}");
            Console.WriteLine($"Number of valid keys: {numValidKeys}");
            Console.WriteLine($"Number of allocates: {numAllocates}");
            Console.WriteLine($"Number of deallocates: {numDeallocates}");
        }
    }
}