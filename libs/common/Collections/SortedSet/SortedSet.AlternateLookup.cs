// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if NET9_0_OR_GREATER

using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

#nullable enable

namespace Garnet.common.Collections
{
    public partial class SortedSet<T>
    {
        /// <summary>
        /// Gets an instance of a type that may be used to perform operations on the current <see cref="SortedSet{T}"/>
        /// using a <typeparamref name="TAlternate"/> instead of a <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="TAlternate">The alternate type of instance for performing lookups.</typeparam>
        /// <returns>The created lookup instance.</returns>
        /// <exception cref="InvalidOperationException">The set's comparer is not compatible with <typeparamref name="TAlternate"/>.</exception>
        /// <remarks>
        /// The set must be using a comparer that implements <see cref="IAlternateComparer{TAlternate, T}"/> with
        /// <typeparamref name="TAlternate"/> and <typeparamref name="T"/>. If it doesn't, an exception will be thrown.
        /// </remarks>
        public AlternateLookup<TAlternate> GetAlternateLookup<TAlternate>()
            where TAlternate : allows ref struct
        {
            if (!AlternateLookup<TAlternate>.IsCompatibleItem(this))
            {
                throw new InvalidOperationException();
            }

            return new AlternateLookup<TAlternate>(this);
        }

        /// <summary>
        /// Provides a type that may be used to perform operations on a <see cref="SortedSet{T}"/>
        /// using a <typeparamref name="TAlternate"/> instead of a <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="TAlternate">The alternate type of instance for performing lookups.</typeparam>
        public readonly struct AlternateLookup<TAlternate> where TAlternate : allows ref struct
        {
            /// <summary>Initialize the instance. The set must have already been verified to have a compatible comparer.</summary>
            internal AlternateLookup(SortedSet<T> set)
            {
                Debug.Assert(set is not null);
                Debug.Assert(IsCompatibleItem(set));
                Set = set;
            }

            /// <summary>Gets the <see cref="SortedSet{T}"/> against which this instance performs operations.</summary>
            public SortedSet<T> Set { get; }

            /// <summary>Checks whether the set has a comparer compatible with <typeparamref name="TAlternate"/>.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static bool IsCompatibleItem(SortedSet<T> set)
            {
                Debug.Assert(set is not null);
                return set.comparer is IAlternateComparer<TAlternate, T>;
            }

            /// <summary>Gets the set's alternate comparer. The set must have already been verified as compatible.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static IAlternateComparer<TAlternate, T> GetAlternateComparer(SortedSet<T> set)
            {
                Debug.Assert(IsCompatibleItem(set));
                return Unsafe.As<IAlternateComparer<TAlternate, T>>(set.comparer);
            }
            
            /// <summary>Adds the specified element to a set.</summary>
            /// <param name="item">The element to add to the set.</param>
            /// <returns>true if the element is added to the set; false if the element is already present.</returns>
            public bool Add(TAlternate item)
            {
                var set = Set;
                var root = set.root;
                var comparer = GetAlternateComparer(set);

                if (root == null)
                {
                    // The tree is empty and this is the first item.
                    root = new Node(comparer.Create(item), NodeColor.Black);
                    set.count = 1;
                    set.version++;
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
                set.version++;

                int order = 0;
                while (current != null)
                {
                    order = comparer.Compare(item, current.Item);
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
                            set.InsertionBalance(current, ref parent!, grandParent!, greatGrandParent!);
                        }
                    }

                    greatGrandParent = grandParent;
                    grandParent = parent;
                    parent = current;
                    current = (order < 0) ? current.Left : current.Right;
                }

                Debug.Assert(parent != null);
                // We're ready to insert the new node.
                Node node = new Node(comparer.Create(item), NodeColor.Red);
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
                    set.InsertionBalance(node, ref parent!, grandParent!, greatGrandParent!);
                }

                // The root node is always black.
                root.ColorBlack();
                ++set.count;
                return true;
            }

            /// <summary>Removes the specified element from a set.</summary>
            /// <remarks>Alternate comparer variant of <see cref="SortedSet{T}.Remove(T)"/></remarks>
            /// <param name="item">The element to remove.</param>
            /// <returns>true if the element is successfully found and removed; otherwise, false.</returns>
            public bool Remove(TAlternate item)
            {
                var set = Set;
                var root = set.root;

                if (root == null)
                {
                    return false;
                }

                var comparer = GetAlternateComparer(set);

                // Search for a node and then find its successor.
                // Then copy the item from the successor to the matching node, and delete the successor.
                // If a node doesn't have a successor, we can replace it with its left child (if not empty),
                // or delete the matching node.
                //
                // In top-down implementation, it is important to make sure the node to be deleted is not a 2-node.
                // Following code will make sure the node on the path is not a 2-node.

                // Even if we don't actually remove from the set, we may be altering its structure (by doing rotations
                // and such). So update our version to disable any enumerators/subsets working on it.
                set.version++;

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
                                set.ReplaceChildOrRoot(grandParent, parent, sibling);
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

                                set.ReplaceChildOrRoot(grandParent, parent, newGrandParent);
                                if (parent == match)
                                {
                                    parentOfMatch = newGrandParent;
                                }
                            }
                        }
                    }

                    // We don't need to compare after we find the match.
                    int order = foundMatch ? -1 : comparer.Compare(item, current.Item);
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
                    set.ReplaceNode(match, parentOfMatch!, parent!, grandParent!);
                    --set.count;
                }

                root?.ColorBlack();
                return foundMatch;
            }


            /// <summary>Determines whether a set contains the specified element.</summary>
            /// <param name="item">The element to locate in the set.</param>
            /// <returns>true if the set contains the specified element; otherwise, false.</returns>
            public bool Contains(TAlternate item) => FindNode(item) is not null;

            /// <summary>Searches the set for a given value and returns the equal value it finds, if any.</summary>
            /// <param name="equalValue">The value to search for.</param>
            /// <param name="actualValue">The value from the set that the search found, or the default value of <typeparamref name="T"/> when the search yielded no match.</param>
            /// <returns>A value indicating whether the search was successful.</returns>
            public bool TryGetValue(TAlternate equalValue, [MaybeNullWhen(false)] out T actualValue)
            {
                var node = FindNode(equalValue);
                if (node != null)
                {
                    actualValue = node.Item;
                    return true;
                }
                actualValue = default;
                return false;
            }

            internal Node? FindNode(TAlternate item)
            {
                var set = Set;
                var current = set.root;
                var comparer = GetAlternateComparer(set);

                while (current != null)
                {
                    var order = comparer.Compare(item, current.Item);
                    if (order == 0)
                    {
                        return current;
                    }

                    current = order < 0 ? current.Left : current.Right;
                }

                return null;
            }
        }
    }
}

#endif