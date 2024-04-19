// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections;
using System.Collections.Generic;

namespace Garnet.server;

public class NodeAddedEventArgs : EventArgs
{
    public ListObject ParentObject { get; init; }

    public NodeAddedEventArgs(ListObject parentObject)
    {
        this.ParentObject = parentObject;
    }
}

#pragma warning disable CS1591
public class ObservableLinkedList<T> : ICollection<T>
{
    private readonly ListObject listObject;
    private readonly LinkedList<T> linkedList;

    public ObservableLinkedList(ListObject listObject)
    {
        this.linkedList = new LinkedList<T>();
        this.listObject = listObject;
    }

    public delegate void NodeAddedEventHandler(object sender, NodeAddedEventArgs e);

    public event NodeAddedEventHandler NodeAddedEvent;

    public int Count => linkedList.Count;

    public bool IsReadOnly => false;

    public LinkedListNode<T> First => linkedList.First;

    public LinkedListNode<T> Last => linkedList.Last;

    public void AddFirst(T value)
    {
        linkedList.AddFirst(value);
        NodeAddedEvent?.Invoke(this, new NodeAddedEventArgs(listObject));
    }

    public void AddLast(T value)
    {
        linkedList.AddLast(value);
        NodeAddedEvent?.Invoke(this, new NodeAddedEventArgs(listObject));
    }

    public LinkedListNode<T> AddBefore(LinkedListNode<T> node, T value)
    {
        var newNode = linkedList.AddBefore(node, value);
        NodeAddedEvent?.Invoke(this, new NodeAddedEventArgs(listObject));
        return newNode;
    }

    public LinkedListNode<T> AddAfter(LinkedListNode<T> node, T value)
    {
        var newNode = linkedList.AddAfter(node, value);
        NodeAddedEvent?.Invoke(this, new NodeAddedEventArgs(listObject));
        return newNode;
    }

    public void RemoveFirst()
    {
        linkedList.RemoveFirst();
    }

    public void RemoveLast()
    {
        linkedList.RemoveLast();
    }

    public void Add(T item)
    {
        this.AddLast(item);
    }

    public void Clear()
    {
        linkedList.Clear();
    }

    public bool Contains(T item)
    {
        return linkedList.Contains(item);
    }

    public void CopyTo(T[] array, int arrayIndex)
    {
        linkedList.CopyTo(array, arrayIndex);
    }

    public bool Remove(T item)
    {
        return linkedList.Remove(item);
    }

    public IEnumerator<T> GetEnumerator()
    {
        return linkedList.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return linkedList.GetEnumerator();
    }

    public IEnumerable<LinkedListNode<T>> Nodes()
    {
        return linkedList.Nodes();
    }
}