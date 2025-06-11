// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.common
{
    /// <summary>
    /// Stack implementation supporting data access by reference
    /// </summary>
    public class RefStack<T> where T : struct
    {
        // Default initial capacity of stack buffer
        const int DefaultInitialCapacity = 64;

        private readonly int initialCapacity;
        private T[] buffer;
        private int count;

        /// <summary>
        /// Creates an empty stack with initial capacity
        /// </summary>
        /// <param name="initialCapacity">Initial capacity of the underlying stack buffer</param>
        public RefStack(int initialCapacity = DefaultInitialCapacity)
        {
            this.initialCapacity = initialCapacity;
            buffer = [];
            count = 0;
        }

        /// <summary>
        /// Push item to stack
        /// </summary>
        /// <param name="item">Reference to item</param>
        public void Push(T item)
        {
            if (count == buffer.Length)
                ExtendStackBuffer();

            buffer[count++] = item;
        }

        /// <summary>
        /// Peek top item in stack
        /// </summary>
        /// <returns>Reference to top item in stack</returns>
        public ref T Peek()
        {
            if (count == 0) throw new InvalidOperationException("Stack contains no elements.");

            return ref buffer[count - 1];
        }

        /// <summary>
        /// Pop top item in stack
        /// </summary>
        /// <returns>Reference to top item in stack</returns>
        public ref T Pop()
        {
            if (count == 0) throw new InvalidOperationException("Stack contains no elements.");

            return ref buffer[--count];
        }

        /// <summary>
        /// Size of stack
        /// </summary>
        public int Count => count;

        private void ExtendStackBuffer()
        {
            var newBuffer = new T[count == 0 ? initialCapacity : count * 2];
            Array.Copy(buffer, newBuffer, count);
            buffer = newBuffer;
        }
    }
}