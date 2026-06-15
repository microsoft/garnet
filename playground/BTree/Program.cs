// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Garnet.server;
using Garnet.server.BTreeIndex;

class Program
{
    /// <summary>
    /// Playground that builds a B+tree from monotonically increasing IDs and prints the
    /// tree structure every time the root splits (i.e., the tree grows a level).
    /// For each level the leftmost (MIN) and rightmost (MAX) node are printed as
    /// representatives, showing first/middle/last key and small per-node stats.
    /// The value stored for each key does not matter here, so we always use Value 0.
    /// </summary>
    static unsafe void Main(string[] args)
    {
        ulong N = 100_000_000;
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i] == "-N" && i + 1 < args.Length)
            {
                N = ulong.Parse(args[i + 1]);
                break;
            }
        }

        // Build monotonically increasing stream IDs (ms = i + 1, seq = 0).
        var streamIDs = new StreamID[N];
        for (ulong i = 0; i < N; i++)
        {
            streamIDs[i] = new StreamID(i + 1, 0);
        }

        var tree = new BTree((uint)BTreeNode.PAGE_SIZE);
        Console.WriteLine($"Inserting {N} monotonic IDs; printing tree structure on each root split.");
        Console.WriteLine($"LEAF_CAPACITY = {BTreeNode.LEAF_CAPACITY}, INTERNAL_CAPACITY = {BTreeNode.INTERNAL_CAPACITY}");

        int lastDepth = tree.stats.depth;
        for (ulong i = 0; i < N; i++)
        {
            tree.InsertIntoTail((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0]), new Value(0));

            // A root split is the only event that increases depth in this insert-only,
            // monotonically-increasing workload, so a depth increase == a root split.
            if (tree.stats.depth > lastDepth)
            {
                PrintRootSplit(tree, i + 1);
                lastDepth = tree.stats.depth;
            }
        }

        Console.WriteLine("Final Tree Print | not a split");
        PrintRootSplit(tree, 0);

        Console.WriteLine();
        Console.WriteLine("=== FINAL STATS ===");
        tree.Deallocate();
    }

    /// <summary>
    /// Prints the tree structure after a root split. For every level from the root down to
    /// the leaves it prints aggregate stats over ALL nodes at that level (node count, total
    /// keys, total valid keys), followed by the leftmost (min) and rightmost (max) node as
    /// representatives. Levels are enumerated by walking parent-to-child pointers, so the
    /// node counts reflect the true width of each level.
    /// </summary>
    static unsafe void PrintRootSplit(BTree tree, ulong insertNum)
    {
        int depth = tree.stats.depth;

        Console.WriteLine();
        Console.WriteLine($"=== ROOT SPLIT: insert #{insertNum}, new depth = {depth} ===");

        // Level-order traversal: 'current' holds every node at the level being printed,
        // starting at the root and expanding to children as we descend.
        var current = new List<IntPtr> { (IntPtr)tree.Root };
        int level = depth - 1;
        while (true)
        {
            long nodeCount = current.Count;
            long totalKeys = 0;
            long totalValid = 0;
            foreach (var p in current)
            {
                var node = (BTreeNode*)p;
                totalKeys += node->info->count;
                totalValid += node->info->validCount;
            }

            var minNode = (BTreeNode*)current[0];
            var maxNode = (BTreeNode*)current[current.Count - 1];
            bool isLeaf = minNode->info->type == BTreeNodeType.Leaf;
            bool isRoot = level == depth - 1;
            string typeStr = isLeaf ? "Leaf" : "Internal";

            // For internal levels the number of child pointers is (keys + nodes); for leaf
            // levels totalKeys is simply the number of indexed entries.
            string extra = isLeaf ? "" : $" children={totalKeys + nodeCount}";
            Console.WriteLine($"  level {level} [{typeStr}] nodes={nodeCount} totalKeys={totalKeys} totalValid={totalValid}{extra}");

            if (isRoot)
            {
                // The root always has exactly one separator key right after a split; print it.
                PrintNode(minNode, "root");
            }
            else if (minNode == maxNode)
            {
                PrintNode(minNode, "only");
            }
            else
            {
                PrintNode(minNode, "min");
                PrintNode(maxNode, "max");
            }

            if (isLeaf)
            {
                break;
            }

            // Expand to the next (lower) level by collecting every child pointer.
            var next = new List<IntPtr>();
            foreach (var p in current)
            {
                var node = (BTreeNode*)p;
                for (int c = 0; c <= node->info->count; c++)
                {
                    next.Add((IntPtr)node->GetChild(c));
                }
            }
            current = next;
            level--;
        }
    }

    /// <summary>
    /// Prints a single representative node: key count, valid count, and its keys. A node with
    /// a single key (e.g., the root right after a split) prints that one key as <c>key=</c>;
    /// wider nodes print the first, middle, and last key. Keys are decoded as big-endian
    /// ms:seq stream IDs.
    /// </summary>
    static unsafe void PrintNode(BTreeNode* node, string role)
    {
        int count = node->info->count;

        string keys;
        if (count == 0)
        {
            keys = "(empty)";
        }
        else if (count == 1)
        {
            keys = $"key={FormatKey(node->GetKey(0))}";
        }
        else
        {
            string first = FormatKey(node->GetKey(0));
            string mid = FormatKey(node->GetKey(count / 2));
            string last = FormatKey(node->GetKey(count - 1));
            keys = $"first={first} mid={mid} last={last}";
        }

        Console.WriteLine($"      {role,-4} count={count,-4} valid={node->info->validCount,-6} {keys}");
    }

    /// <summary>
    /// Decodes a 16-byte B+tree key as a stream ID (big-endian ms in bytes 0-7, seq in 8-15).
    /// </summary>
    static unsafe string FormatKey(byte* key)
    {
        ulong ms = BinaryPrimitives.ReadUInt64BigEndian(new ReadOnlySpan<byte>(key, 8));
        ulong seq = BinaryPrimitives.ReadUInt64BigEndian(new ReadOnlySpan<byte>(key + 8, 8));
        return $"{ms}:{seq}";
    }
}
