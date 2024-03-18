// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Heap container to store keys and values when they go pending
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IHeapContainer<T> : IDisposable
    {
        /// <summary>
        /// Get a reference to the contained object
        /// </summary>
        ref T Get();
    }

    /// <summary>
    /// Heap container for standard C# objects (non-variable-length)
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class StandardHeapContainer<T> : IHeapContainer<T>
    {
        private T obj;

        public StandardHeapContainer(ref T obj)
        {
            this.obj = obj;
        }

        public ref T Get() => ref obj;

        public void Dispose() { }
    }
}