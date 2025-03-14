// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;

#nullable enable

namespace Garnet.common.Collections
{
    // A binary search tree is a red-black tree if it satisfies the following red-black properties:
    // 1. Every node is either red or black
    // 2. Every leaf (nil node) is black
    // 3. If a node is red, the both its children are black
    // 4. Every simple path from a node to a descendant leaf contains the same number of black nodes
    //
    // The basic idea of a red-black tree is to represent 2-3-4 trees as standard BSTs but to add one extra bit of information
    // per node to encode 3-nodes and 4-nodes.
    // 4-nodes will be represented as:   B
    //                                 R   R
    //
    // 3 -node will be represented as:   B     or     B
    //                                 R   B        B   R
    //
    // For a detailed description of the algorithm, take a look at "Algorithms" by Robert Sedgewick.

    internal enum NodeColor : byte
    {
        Black,
        Red
    }

    internal delegate bool TreeWalkPredicate<T>(SortedSet<T>.Node node);

    internal enum TreeRotation : byte
    {
        Left,
        LeftRight,
        Right,
        RightLeft
    }

    [DebuggerDisplay("Count = {Count}")]
    public partial class SortedSet<T> : ICollection<T>
    {
        private Node? root;
        private int count;
        private int version;

        internal const int StackAllocThreshold = 128;

        public SortedSet()
        {
            Comparer = Comparer<T>.Default;
        }

        public SortedSet(IComparer<T>? comparer)
        {
            Comparer = comparer ?? Comparer<T>.Default;
        }

        /// <summary>
        /// Does an in-order tree walk and calls the delegate for each node.
        /// </summary>
        /// <param name="action">
        /// The delegate to invoke on each node.
        /// If the delegate returns <c>false</c>, the walk is stopped.
        /// </param>
        /// <returns><c>true</c> if the entire tree has been walked; otherwise, <c>false</c>.</returns>
        internal virtual bool InOrderTreeWalk(TreeWalkPredicate<T> action)
        {
            if (root == null)
            {
                return true;
            }

            // The maximum height of a red-black tree is 2 * log2(n+1).
            // See page 264 of "Introduction to algorithms" by Thomas H. Cormen
            // Note: It's not strictly necessary to provide the stack capacity, but we don't
            // want the stack to unnecessarily allocate arrays as it grows.
            var stack = new Stack<Node>(2 * Log2(Count + 1));
            Node? current = root;

            while (current != null)
            {
                stack.Push(current);
                current = current.Left;
            }

            while (stack.Count != 0)
            {
                current = stack.Pop();
                if (!action(current))
                {
                    return false;
                }

                Node? node = current.Right;
                while (node != null)
                {
                    stack.Push(node);
                    node = node.Left;
                }
            }

            return true;
        }

        /// <summary>
        /// Does a left-to-right breadth-first tree walk and calls the delegate for each node.
        /// </summary>
        /// <param name="action">
        /// The delegate to invoke on each node.
        /// If the delegate returns <c>false</c>, the walk is stopped.
        /// </param>
        /// <returns><c>true</c> if the entire tree has been walked; otherwise, <c>false</c>.</returns>
        internal virtual bool BreadthFirstTreeWalk(TreeWalkPredicate<T> action)
        {
            if (root == null)
            {
                return true;
            }

            var processQueue = new Queue<Node>();
            processQueue.Enqueue(root);

            Node current;
            while (processQueue.Count != 0)
            {
                current = processQueue.Dequeue();
                if (!action(current))
                {
                    return false;
                }

                if (current.Left != null)
                {
                    processQueue.Enqueue(current.Left);
                }
                if (current.Right != null)
                {
                    processQueue.Enqueue(current.Right);
                }
            }

            return true;
        }


        public int Count
        {
            get
            {
                VersionCheck(updateCount: true);
                return count;
            }
        }

        public IComparer<T> Comparer { get; } = default!;

        bool ICollection<T>.IsReadOnly => false;


        // Virtual function for TreeSubSet, which may need to update its count.
        internal virtual void VersionCheck(bool updateCount = false) { }
        // Virtual function for TreeSubSet, which may need the count variable of the parent set.
        internal virtual int TotalCount() => Count;

        // Virtual function for TreeSubSet, which may need to do range checks.
        internal virtual bool IsWithinRange(T item) => true;

        void ICollection<T>.Add(T item) => Add(item);

        public virtual bool Add(T item)
        {
            if (root == null)
            {
                // The tree is empty and this is the first item.
                root = new Node(item, NodeColor.Black);
                count = 1;
                version++;
                return true;
            }

            // Search for a node at bottom to insert the new node.
            // If we can guarantee the node we found is not a 4-node, it would be easy to do insertion.
            // We split 4-nodes along the search path.
            Node? current = root;
            Node? parent = null;
            Node? grandParent = null;
            Node? greatGrandParent = null;

            // Even if we don't actually add to the set, we may be altering its structure (by doing rotations and such).
            // So update `_version` to disable any instances of Enumerator/TreeSubSet from working on it.
            version++;

            int order = 0;
            while (current != null)
            {
                order = Comparer.Compare(item, current.Item);
                if (order == 0)
                {
                    // We could have changed root node to red during the search process.
                    // We need to set it to black before we return.
                    root.ColorBlack();
                    return false;
                }

                // Split a 4-node into two 2-nodes.
                if (current.Is4Node)
                {
                    current.Split4Node();
                    // We could have introduced two consecutive red nodes after split. Fix that by rotation.
                    if (Node.IsNonNullRed(parent))
                    {
                        InsertionBalance(current, ref parent!, grandParent!, greatGrandParent!);
                    }
                }

                greatGrandParent = grandParent;
                grandParent = parent;
                parent = current;
                current = (order < 0) ? current.Left : current.Right;
            }

            Debug.Assert(parent != null);
            // We're ready to insert the new node.
            Node node = new Node(item, NodeColor.Red);
            if (order > 0)
            {
                parent.Right = node;
            }
            else
            {
                parent.Left = node;
            }

            // The new node will be red, so we will need to adjust colors if its parent is also red.
            if (parent.IsRed)
            {
                InsertionBalance(node, ref parent!, grandParent!, greatGrandParent!);
            }

            // The root node is always black.
            root.ColorBlack();
            ++count;
            return true;
        }

        public virtual bool Remove(T item)
        {
            if (root == null)
            {
                return false;
            }

            // Search for a node and then find its successor.
            // Then copy the item from the successor to the matching node, and delete the successor.
            // If a node doesn't have a successor, we can replace it with its left child (if not empty),
            // or delete the matching node.
            //
            // In top-down implementation, it is important to make sure the node to be deleted is not a 2-node.
            // Following code will make sure the node on the path is not a 2-node.

            // Even if we don't actually remove from the set, we may be altering its structure (by doing rotations
            // and such). So update our version to disable any enumerators/subsets working on it.
            version++;

            Node? current = root;
            Node? parent = null;
            Node? grandParent = null;
            Node? match = null;
            Node? parentOfMatch = null;
            bool foundMatch = false;
            while (current != null)
            {
                if (current.Is2Node)
                {
                    // Fix up 2-node
                    if (parent == null)
                    {
                        // `current` is the root. Mark it red.
                        current.ColorRed();
                    }
                    else
                    {
                        Node sibling = parent.GetSibling(current);
                        if (sibling.IsRed)
                        {
                            // If parent is a 3-node, flip the orientation of the red link.
                            // We can achieve this by a single rotation.
                            // This case is converted to one of the other cases below.
                            Debug.Assert(parent.IsBlack);
                            if (parent.Right == sibling)
                            {
                                parent.RotateLeft();
                            }
                            else
                            {
                                parent.RotateRight();
                            }

                            parent.ColorRed();
                            sibling.ColorBlack(); // The red parent can't have black children.
                            // `sibling` becomes the child of `grandParent` or `root` after rotation. Update the link from that node.
                            ReplaceChildOrRoot(grandParent, parent, sibling);
                            // `sibling` will become the grandparent of `current`.
                            grandParent = sibling;
                            if (parent == match)
                            {
                                parentOfMatch = sibling;
                            }

                            sibling = parent.GetSibling(current);
                        }

                        Debug.Assert(Node.IsNonNullBlack(sibling));

                        if (sibling.Is2Node)
                        {
                            parent.Merge2Nodes();
                        }
                        else
                        {
                            // `current` is a 2-node and `sibling` is either a 3-node or a 4-node.
                            // We can change the color of `current` to red by some rotation.
                            Node newGrandParent = parent.Rotate(parent.GetRotation(current, sibling))!;

                            newGrandParent.Color = parent.Color;
                            parent.ColorBlack();
                            current.ColorRed();

                            ReplaceChildOrRoot(grandParent, parent, newGrandParent);
                            if (parent == match)
                            {
                                parentOfMatch = newGrandParent;
                            }
                        }
                    }
                }

                // We don't need to compare after we find the match.
                int order = foundMatch ? -1 : Comparer.Compare(item, current.Item);
                if (order == 0)
                {
                    // Save the matching node.
                    foundMatch = true;
                    match = current;
                    parentOfMatch = parent;
                }

                grandParent = parent;
                parent = current;
                // If we found a match, continue the search in the right sub-tree.
                current = order < 0 ? current.Left : current.Right;
            }

            // Move successor to the matching node position and replace links.
            if (match != null)
            {
                ReplaceNode(match, parentOfMatch!, parent!, grandParent!);
                --count;
            }

            root?.ColorBlack();
            return foundMatch;
        }

        public virtual void Clear()
        {
            root = null;
            count = 0;
            ++version;
        }

        public virtual bool Contains(T item) => FindNode(item) != null;

        public void CopyTo(T[] array) => CopyTo(array, 0, Count);

        public void CopyTo(T[] array, int index) => CopyTo(array, index, Count);

        public void CopyTo(T[] array, int index, int count)
        {
            ArgumentNullException.ThrowIfNull(array);
            ArgumentOutOfRangeException.ThrowIfNegative(index);
            ArgumentOutOfRangeException.ThrowIfNegative(count);

            if (count > array.Length - index)
            {
                throw new ArgumentException();
            }

            count += index; // Make `count` the upper bound.

            InOrderTreeWalk(node =>
            {
                if (index >= count)
                {
                    return false;
                }

                array[index++] = node.Item;
                return true;
            });
        }


        public Enumerator GetEnumerator() => new Enumerator(this);

        IEnumerator<T> IEnumerable<T>.GetEnumerator() => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<T>)this).GetEnumerator();


        // After calling InsertionBalance, we need to make sure `current` and `parent` are up-to-date.
        // It doesn't matter if we keep `grandParent` and `greatGrandParent` up-to-date, because we won't
        // need to split again in the next node.
        // By the time we need to split again, everything will be correctly set.
        private void InsertionBalance(Node current, ref Node parent, Node grandParent, Node greatGrandParent)
        {
            Debug.Assert(parent != null);
            Debug.Assert(grandParent != null);

            bool parentIsOnRight = grandParent.Right == parent;
            bool currentIsOnRight = parent.Right == current;

            Node newChildOfGreatGrandParent;
            if (parentIsOnRight == currentIsOnRight)
            {
                // Same orientation, single rotation
                newChildOfGreatGrandParent = currentIsOnRight ? grandParent.RotateLeft() : grandParent.RotateRight();
            }
            else
            {
                // Different orientation, double rotation
                newChildOfGreatGrandParent = currentIsOnRight ? grandParent.RotateLeftRight() : grandParent.RotateRightLeft();
                // Current node now becomes the child of `greatGrandParent`
                parent = greatGrandParent;
            }

            // `grandParent` will become a child of either `parent` of `current`.
            grandParent.ColorRed();
            newChildOfGreatGrandParent.ColorBlack();

            ReplaceChildOrRoot(greatGrandParent, grandParent, newChildOfGreatGrandParent);
        }

        /// <summary>
        /// Replaces the child of a parent node, or replaces the root if the parent is <c>null</c>.
        /// </summary>
        /// <param name="parent">The (possibly <c>null</c>) parent.</param>
        /// <param name="child">The child node to replace.</param>
        /// <param name="newChild">The node to replace <paramref name="child"/> with.</param>
        private void ReplaceChildOrRoot(Node? parent, Node child, Node newChild)
        {
            if (parent != null)
            {
                parent.ReplaceChild(child, newChild);
            }
            else
            {
                root = newChild;
            }
        }

        /// <summary>
        /// Replaces the matching node with its successor.
        /// </summary>
        private void ReplaceNode(Node match, Node parentOfMatch, Node successor, Node parentOfSuccessor)
        {
            Debug.Assert(match != null);

            if (successor == match)
            {
                // This node has no successor. This can only happen if the right child of the match is null.
                Debug.Assert(match.Right == null);
                successor = match.Left!;
            }
            else
            {
                Debug.Assert(parentOfSuccessor != null);
                Debug.Assert(successor.Left == null);
                Debug.Assert((successor.Right == null && successor.IsRed) || (successor.Right!.IsRed && successor.IsBlack));

                successor.Right?.ColorBlack();

                if (parentOfSuccessor != match)
                {
                    // Detach the successor from its parent and set its right child.
                    parentOfSuccessor.Left = successor.Right;
                    successor.Right = match.Right;
                }

                successor.Left = match.Left;
            }

            if (successor != null)
            {
                successor.Color = match.Color;
            }

            ReplaceChildOrRoot(parentOfMatch, match, successor!);
        }

        internal virtual Node? FindNode(T item)
        {
            Node? current = root;
            while (current != null)
            {
                int order = Comparer.Compare(item, current.Item);
                if (order == 0)
                {
                    return current;
                }

                current = order < 0 ? current.Left : current.Right;
            }

            return null;
        }

        /// <summary>
        /// Searches for an item and returns its zero-based index in this set.
        /// </summary>
        /// <param name="item">The item.</param>
        /// <returns>The item's zero-based index in this set, or -1 if it isn't found.</returns>
        /// <remarks>
        /// <para>
        /// This implementation is based off of http://en.wikipedia.org/wiki/Binary_Tree#Methods_for_storing_binary_trees.
        /// </para>
        /// <para>
        /// This method is used with the BitHelper class. Note that this implementation is
        /// completely different from <see cref="TreeSubSet"/>'s, and that the two should not be mixed.
        /// </para>
        /// </remarks>
        internal virtual int InternalIndexOf(T item)
        {
            Node? current = root;
            int count = 0;
            while (current != null)
            {
                int order = Comparer.Compare(item, current.Item);
                if (order == 0)
                {
                    return count;
                }

                current = order < 0 ? current.Left : current.Right;
                count = order < 0 ? (2 * count + 1) : (2 * count + 2);
            }

            return -1;
        }

        internal Node? FindRange(T? from, T? to) => FindRange(from, to, lowerBoundActive: true, upperBoundActive: true);

        internal Node? FindRange(T? from, T? to, bool lowerBoundActive, bool upperBoundActive)
        {
            Node? current = root;
            while (current != null)
            {
                if (lowerBoundActive && Comparer.Compare(from, current.Item) > 0)
                {
                    current = current.Right;
                }
                else
                {
                    if (upperBoundActive && Comparer.Compare(to, current.Item) < 0)
                    {
                        current = current.Left;
                    }
                    else
                    {
                        return current;
                    }
                }
            }

            return null;
        }

        internal void UpdateVersion() => ++version;

        private static Node? ConstructRootFromSortedArray(T[] arr, int startIndex, int endIndex, Node? redNode)
        {
            // You're given a sorted array... say 1 2 3 4 5 6
            // There are 2 cases:
            // -  If there are odd # of elements, pick the middle element (in this case 4), and compute
            //    its left and right branches
            // -  If there are even # of elements, pick the left middle element, save the right middle element
            //    and call the function on the rest
            //    1 2 3 4 5 6 -> pick 3, save 4 and call the fn on 1,2 and 5,6
            //    now add 4 as a red node to the lowest element on the right branch
            //             3                       3
            //         1       5       ->     1        5
            //           2       6             2     4   6
            //    As we're adding to the leftmost of the right branch, nesting will not hurt the red-black properties
            //    Leaf nodes are red if they have no sibling (if there are 2 nodes or if a node trickles
            //    down to the bottom

            // This is done recursively because the iterative way to do this ends up wasting more space than it saves in stack frames
            // Only some base cases are handled below.

            int size = endIndex - startIndex + 1;
            Node root;

            switch (size)
            {
                case 0:
                    return null;
                case 1:
                    root = new Node(arr[startIndex], NodeColor.Black);
                    if (redNode != null)
                    {
                        root.Left = redNode;
                    }
                    break;
                case 2:
                    root = new Node(arr[startIndex], NodeColor.Black);
                    root.Right = new Node(arr[endIndex], NodeColor.Black);
                    root.Right.ColorRed();
                    if (redNode != null)
                    {
                        root.Left = redNode;
                    }
                    break;
                case 3:
                    root = new Node(arr[startIndex + 1], NodeColor.Black);
                    root.Left = new Node(arr[startIndex], NodeColor.Black);
                    root.Right = new Node(arr[endIndex], NodeColor.Black);
                    if (redNode != null)
                    {
                        root.Left.Left = redNode;
                    }
                    break;
                default:
                    int midpt = ((startIndex + endIndex) / 2);
                    root = new Node(arr[midpt], NodeColor.Black);
                    root.Left = ConstructRootFromSortedArray(arr, startIndex, midpt - 1, redNode);
                    root.Right = size % 2 == 0 ?
                        ConstructRootFromSortedArray(arr, midpt + 2, endIndex, new Node(arr[midpt + 1], NodeColor.Red)) :
                        ConstructRootFromSortedArray(arr, midpt + 1, endIndex, null);
                    break;

            }

            return root;
        }

        public virtual T? Min
        {
            get
            {
                if (root == null) return default;

                Node current = root;
                while (current.Left != null)
                {
                    current = current.Left;
                }

                return current.Item;
            }
        }

        public virtual T? Max
        {
            get
            {
                if (root == null) return default;

                Node current = root;
                while (current.Right != null)
                {
                    current = current.Right;
                }

                return current.Item;
            }
        }

        public IEnumerable<T> Reverse()
        {
            Enumerator e = new Enumerator(this, reverse: true);
            while (e.MoveNext())
            {
                yield return e.Current;
            }
        }

        public virtual SortedSet<T> GetViewBetween(T? lowerValue, T? upperValue)
        {
            if (Comparer.Compare(lowerValue, upperValue) > 0)
            {
                throw new ArgumentOutOfRangeException(nameof(lowerValue));
            }
            return new TreeSubSet(this, lowerValue, upperValue, true, true);
        }

        /// <summary>
        /// debug status to be checked whenever any operation is called
        /// </summary>
        /// <returns></returns>
        internal virtual bool versionUpToDate() => true;

        internal sealed class Node
        {
            public Node(T item, NodeColor color)
            {
                Item = item;
                Color = color;
            }

            public static bool IsNonNullBlack(Node? node) => node != null && node.IsBlack;

            public static bool IsNonNullRed(Node? node) => node != null && node.IsRed;

            public static bool IsNullOrBlack(Node? node) => node == null || node.IsBlack;

            public T Item { get; set; }

            public Node? Left { get; set; }

            public Node? Right { get; set; }

            public NodeColor Color { get; set; }

            public bool IsBlack => Color == NodeColor.Black;

            public bool IsRed => Color == NodeColor.Red;

            public bool Is2Node => IsBlack && IsNullOrBlack(Left) && IsNullOrBlack(Right);

            public bool Is4Node => IsNonNullRed(Left) && IsNonNullRed(Right);

            public void ColorBlack() => Color = NodeColor.Black;

            public void ColorRed() => Color = NodeColor.Red;

            public Node DeepClone(int count)
            {
                Debug.Assert(count == GetCount());

                Node newRoot = ShallowClone();

                var pendingNodes = new Stack<(Node source, Node target)>(2 * Log2(count) + 2);
                pendingNodes.Push((this, newRoot));

                while (pendingNodes.TryPop(out var next))
                {
                    Node clonedNode;

                    if (next.source.Left is Node left)
                    {
                        clonedNode = left.ShallowClone();
                        next.target.Left = clonedNode;
                        pendingNodes.Push((left, clonedNode));
                    }

                    if (next.source.Right is Node right)
                    {
                        clonedNode = right.ShallowClone();
                        next.target.Right = clonedNode;
                        pendingNodes.Push((right, clonedNode));
                    }
                }

                return newRoot;
            }

            /// <summary>
            /// Gets the rotation this node should undergo during a removal.
            /// </summary>
            public TreeRotation GetRotation(Node current, Node sibling)
            {
                Debug.Assert(IsNonNullRed(sibling.Left) || IsNonNullRed(sibling.Right));
                Debug.Assert(HasChildren(current, sibling));

                bool currentIsLeftChild = Left == current;
                return IsNonNullRed(sibling.Left) ?
                    (currentIsLeftChild ? TreeRotation.RightLeft : TreeRotation.Right) :
                    (currentIsLeftChild ? TreeRotation.Left : TreeRotation.LeftRight);
            }

            /// <summary>
            /// Gets the sibling of one of this node's children.
            /// </summary>
            public Node GetSibling(Node node)
            {
                Debug.Assert(node != null);
                Debug.Assert(node == Left ^ node == Right);

                return node == Left ? Right! : Left!;
            }

            public Node ShallowClone() => new Node(Item, Color);

            public void Split4Node()
            {
                Debug.Assert(Left != null);
                Debug.Assert(Right != null);

                ColorRed();
                Left.ColorBlack();
                Right.ColorBlack();
            }

            /// <summary>
            /// Does a rotation on this tree. May change the color of a grandchild from red to black.
            /// </summary>
            public Node? Rotate(TreeRotation rotation)
            {
                Node removeRed;
                switch (rotation)
                {
                    case TreeRotation.Right:
                        removeRed = Left!.Left!;
                        Debug.Assert(removeRed.IsRed);
                        removeRed.ColorBlack();
                        return RotateRight();
                    case TreeRotation.Left:
                        removeRed = Right!.Right!;
                        Debug.Assert(removeRed.IsRed);
                        removeRed.ColorBlack();
                        return RotateLeft();
                    case TreeRotation.RightLeft:
                        Debug.Assert(Right!.Left!.IsRed);
                        return RotateRightLeft();
                    case TreeRotation.LeftRight:
                        Debug.Assert(Left!.Right!.IsRed);
                        return RotateLeftRight();
                    default:
                        Debug.Fail($"{nameof(rotation)}: {rotation} is not a defined {nameof(TreeRotation)} value.");
                        return null;
                }
            }

            /// <summary>
            /// Does a left rotation on this tree, making this node the new left child of the current right child.
            /// </summary>
            public Node RotateLeft()
            {
                Node child = Right!;
                Right = child.Left;
                child.Left = this;
                return child;
            }

            /// <summary>
            /// Does a left-right rotation on this tree. The left child is rotated left, then this node is rotated right.
            /// </summary>
            public Node RotateLeftRight()
            {
                Node child = Left!;
                Node grandChild = child.Right!;

                Left = grandChild.Right;
                grandChild.Right = this;
                child.Right = grandChild.Left;
                grandChild.Left = child;
                return grandChild;
            }

            /// <summary>
            /// Does a right rotation on this tree, making this node the new right child of the current left child.
            /// </summary>
            public Node RotateRight()
            {
                Node child = Left!;
                Left = child.Right;
                child.Right = this;
                return child;
            }

            /// <summary>
            /// Does a right-left rotation on this tree. The right child is rotated right, then this node is rotated left.
            /// </summary>
            public Node RotateRightLeft()
            {
                Node child = Right!;
                Node grandChild = child.Left!;

                Right = grandChild.Left;
                grandChild.Left = this;
                child.Left = grandChild.Right;
                grandChild.Right = child;
                return grandChild;
            }

            /// <summary>
            /// Combines two 2-nodes into a 4-node.
            /// </summary>
            public void Merge2Nodes()
            {
                Debug.Assert(IsRed);
                Debug.Assert(Left!.Is2Node);
                Debug.Assert(Right!.Is2Node);

                // Combine two 2-nodes into a 4-node.
                ColorBlack();
                Left.ColorRed();
                Right.ColorRed();
            }

            /// <summary>
            /// Replaces a child of this node with a new node.
            /// </summary>
            /// <param name="child">The child to replace.</param>
            /// <param name="newChild">The node to replace <paramref name="child"/> with.</param>
            public void ReplaceChild(Node child, Node newChild)
            {
                Debug.Assert(HasChild(child));

                if (Left == child)
                {
                    Left = newChild;
                }
                else
                {
                    Right = newChild;
                }
            }

            private int GetCount() => 1 + (Left?.GetCount() ?? 0) + (Right?.GetCount() ?? 0);

            private bool HasChild(Node child) => child == Left || child == Right;

            private bool HasChildren(Node child1, Node child2)
            {
                Debug.Assert(child1 != child2);

                return (Left == child1 && Right == child2)
                    || (Left == child2 && Right == child1);
            }
        }

        public struct Enumerator : IEnumerator<T>
        {
            private readonly SortedSet<T> _tree;
            private readonly int _version;

            private readonly Stack<Node> _stack;
            private Node? _current;

            private readonly bool _reverse;

            internal Enumerator(SortedSet<T> set)
                : this(set, reverse: false)
            {
            }

            internal Enumerator(SortedSet<T> set, bool reverse)
            {
                _tree = set;
                set.VersionCheck();
                _version = set.version;

                // 2 log(n + 1) is the maximum height.
                _stack = new Stack<Node>(2 * Log2(set.TotalCount() + 1));
                _current = null;
                _reverse = reverse;

                Initialize();
            }

            private void Initialize()
            {
                _current = null;
                Node? node = _tree.root;
                Node? next, other;
                while (node != null)
                {
                    (next, other) = _reverse ?
                        (node.Right, node.Left) : (node.Left, node.Right);

                    if (_tree.IsWithinRange(node.Item))
                    {
                        _stack.Push(node);
                        node = next;
                    }
                    else if (next == null || !_tree.IsWithinRange(next.Item))
                    {
                        node = other;
                    }
                    else
                    {
                        node = next;
                    }
                }
            }

            public bool MoveNext()
            {
                // Make sure that the underlying subset has not been changed since
                _tree.VersionCheck();

                if (_version != _tree.version)
                {
                    throw new InvalidOperationException();
                }

                if (_stack.Count == 0)
                {
                    _current = null;
                    return false;
                }

                _current = _stack.Pop();
                Node? node = (_reverse ? _current.Left : _current.Right);
                Node? next, other;
                while (node != null)
                {
                    (next, other) = _reverse ?
                        (node.Right, node.Left) : (node.Left, node.Right);

                    if (_tree.IsWithinRange(node.Item))
                    {
                        _stack.Push(node);
                        node = next;
                    }
                    else if (other == null || !_tree.IsWithinRange(other.Item))
                    {
                        node = next;
                    }
                    else
                    {
                        node = other;
                    }
                }
                return true;
            }

            public void Dispose() { }

            public readonly T Current
            {
                get
                {
                    if (_current != null)
                    {
                        return _current.Item;
                    }
                    return default!; // Should only happen when accessing Current is undefined behavior
                }
            }

            object? IEnumerator.Current
            {
                get
                {
                    if (_current == null)
                    {
                        throw new InvalidOperationException();
                    }

                    return _current.Item;
                }
            }

            internal readonly bool NotStartedOrEnded => _current == null;

            internal void Reset()
            {
                if (_version != _tree.version)
                {
                    throw new InvalidOperationException();
                }

                _stack.Clear();
                Initialize();
            }

            void IEnumerator.Reset() => Reset();
        }

        /// <summary>
        /// Searches the set for a given value and returns the equal value it finds, if any.
        /// </summary>
        /// <param name="equalValue">The value to search for.</param>
        /// <param name="actualValue">The value from the set that the search found, or the default value of <typeparamref name="T"/> when the search yielded no match.</param>
        /// <returns>A value indicating whether the search was successful.</returns>
        /// <remarks>
        /// This can be useful when you want to reuse a previously stored reference instead of
        /// a newly constructed one (so that more sharing of references can occur) or to look up
        /// a value that has more complete data than the value you currently have, although their
        /// comparer functions indicate they are equal.
        /// </remarks>
        public bool TryGetValue(T equalValue, [MaybeNullWhen(false)] out T actualValue)
        {
            Node? node = FindNode(equalValue);
            if (node != null)
            {
                actualValue = node.Item;
                return true;
            }
            actualValue = default;
            return false;
        }

        // Used for set checking operations (using enumerables) that rely on counting
        private static int Log2(int value) => BitOperations.Log2((uint)value);
    }
}