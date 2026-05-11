// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// This is a node in the freelists, implemented as a union of two ints and a long. The long is used for Interlocks.
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    internal struct SimpleFreeStackNode(int slot, int version)
    {
        internal const int Nil = -1;

        /// <summary>The next free node in the stack, or Empty if this is the last node.</summary>
        [FieldOffset(0)]
        internal int Slot = slot;

        /// <summary>The slot in the main elementArray.</summary>
        [FieldOffset(4)]
        internal int Version = version;

        /// <summary>The word is used for Interlocked operations, containing <see cref="Version"/> and <see cref="Slot"/>.</summary>
        [FieldOffset(0)]
        internal long word;

        internal bool IsNil => Slot == Nil;

        public override string ToString() => $"Slot {Slot}, Version {Version}, IsNil {IsNil}";
    }

    /// <summary>
    /// This is a queue containing items that may be ref or value types, but does not call Dispose; if TItem is IDisposable
    /// it must be owned/disposed elsewhere.
    /// </summary>
    /// <remarks>
    /// This queue does not use latches or pointers. Instead it uses int indexes into the elementArray, and a version number
    /// to avoid the ABA issue. This does mean that each item in the array is a struct containing the item and the node information,
    /// so is 8 bytes (2 ints) larger than the item alone; we need to track 'next' indexes explicitly rather than rely on push/pop
    /// ordering because CAS contention will alter that order. This space overhead is a tradeoff for avoiding the ABA issue without the
    /// overhead of ConcurrentStack allocations or latches.
    /// </remarks>
    class SimpleConcurrentStack<TItem>
    {
        internal struct ArrayElement
        {
            internal TItem Item;
            internal SimpleFreeStackNode Node;

            public override readonly string ToString() => $"[Node {Node}]; Item {Item}";
        }

        public const int DefaultInitialCapacity = 1024;

        /// <summary>The actual stack, as a simple growable vector</summary>
        internal MultiLevelPageArray<ArrayElement> elementArray;

        /// <summary>
        /// This is the head of the chain of stack nodes, which are used to track the stack slots in the elementArray.
        /// </summary>
        internal SimpleFreeStackNode stack;

        /// <summary>
        /// This is the head of the chain of free nodes, which are used to track the free slots in the elementArray.
        /// </summary>
        internal SimpleFreeStackNode freeNodes;

        public SimpleConcurrentStack()
        {
            elementArray = new();
            stack = new(SimpleFreeStackNode.Nil, version: 0);
            freeNodes = new(SimpleFreeStackNode.Nil, version: 0);
        }

        /// <summary>
        /// This is not named "Count" because our <see cref="stack"/> and <see cref="freeNodes"/> do not adjust <see cref="elementArray"/>'s Tail.
        /// So we support just the high-water mark (mostly for test).
        /// </summary>
        public int MaxCount => elementArray.Count;

        public bool IsEmpty => stack.IsNil;

        /// <summary>
        /// Public API: Push an item onto the stack.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Push(TItem item)
        {
            if (GetNodeFromFreeList(out SimpleFreeStackNode node))
                node.Version++;
            else
                node = new(elementArray.Allocate(), version: 0);

            // This node is going onto the stack so create an ArrayElement with its item set to the passed item and the node's version.
            // We'll update the element's slot to the stack head inside the retry loop.
            var element = new ArrayElement { Item = item, Node = new(SimpleFreeStackNode.Nil, node.Version) };

            while (true)
            {
                // The element's slot is the 'next' pointer; update it to what is currently in 'head' to maintain the chain.
                var head = stack;
                element.Node.Slot = head.Slot;
                elementArray.Set(node.Slot, element);

                if (Interlocked.CompareExchange(ref stack.word, node.word, head.word) == head.word)
                    return;
                _ = Thread.Yield();
            }
        }

        /// <summary>
        /// Public API: Pop an item from the stack.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryPop(out TItem item)
        {
            while (true)
            {
                var current = stack;
                if (current.IsNil)
                {
                    item = default;
                    return false;
                }

                // For an element in elementArray at slot elementSlot, its node.Slot refers to the next slot in the chain; node.Version is for the current slot,
                // which is also the version for the node whose .Slot == elementSlot.
                var element = elementArray[current.Slot];
                var nextSlot = element.Node.Slot;
                var nextVersion = element.Node.IsNil ? 0 : elementArray[nextSlot].Node.Version;

                var next = new SimpleFreeStackNode(nextSlot, nextVersion);
                if (Interlocked.CompareExchange(ref stack.word, next.word, current.word) == current.word)
                {
                    item = element.Item;
                    AddNodeToFreeList(current);
                    return true;
                }
                _ = Thread.Yield();
            }
        }

        /// <summary>Put a node that was popped from the <see cref="stack"/> onto the <see cref="freeNodes"/>.</summary>
        void AddNodeToFreeList(SimpleFreeStackNode node)
        {
            ++node.Version;

            // This node is going onto the freeList so create an ArrayElement with its item set to 'default' and the node's version.
            // We'll update the element's slot to the freeList head inside the retry loop.
            var element = new ArrayElement { Node = new(SimpleFreeStackNode.Nil, node.Version) };

            while (true)
            {
                // The element's slot is the 'next' pointer; update it to what is currently in 'head' to maintain the chain.
                var head = freeNodes;
                element.Node.Slot = head.Slot;
                elementArray.Set(node.Slot, element);

                if (Interlocked.CompareExchange(ref freeNodes.word, node.word, head.word) == head.word)
                    return;
                _ = Thread.Yield();
            }
        }

        bool GetNodeFromFreeList(out SimpleFreeStackNode node)
        {
            while (true)
            {
                node = freeNodes;
                if (node.IsNil)
                {
                    node = default;
                    return false;
                }

                // For elementArray[elementSlot], node.Slot refers to the next slot in the chain; node.Version is for the current slot, which is also 
                // the version for the node whose .Slot == elementSlot.
                var element = elementArray.Get(node.Slot);
                var version = element.Node.IsNil ? 0 : elementArray.Get(element.Node.Slot).Node.Version;

                var head = new SimpleFreeStackNode(element.Node.Slot, version);
                if (Interlocked.CompareExchange(ref freeNodes.word, head.word, node.word) == node.word)
                    return true;
                _ = Thread.Yield();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear(int retainedChapterCount = 1 << MultiLevelPageArray.FreeListClearRetainedChapterSizeBits)
        {
            elementArray.Clear(retainedChapterCount);
            stack = new(SimpleFreeStackNode.Nil, version: 0);
            freeNodes = new(SimpleFreeStackNode.Nil, version: 0);
        }

        public override string ToString() => $"elements {elementArray.Count}; [stack {stack}]; [freeList {freeNodes}]";
    }
}