// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

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
    /// Do-nothing heap container for an empty input. An empty input carries no bytes, so there is nothing
    /// to copy onto the heap and nothing to release. Reused per session (see
    /// <c>TsavoriteExecutionContext.EmptyInputContainer</c>) rather than per pending op, so an empty-input
    /// read does not rent a wrapper. <see cref="Get"/> exposes the backing value by <c>ref</c>; although an
    /// empty read input is not expected to be written, <see cref="Dispose"/> resets it to default so a
    /// callback that does write through the ref cannot affect the next op reusing this container.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal sealed class EmptyHeapContainer<T> : IHeapContainer<T>
    {
        private T value;
        public ref T Get() => ref value;
        public void Dispose() => value = default;
    }

    /// <summary>
    /// Heap container for standard C# objects (non-variable-length).
    /// </summary>
    /// <remarks>
    /// Supports the same per-session pooling pattern as
    /// <see cref="SpanByteHeapContainer"/>: when constructed/initialized with a non-null
    /// <c>returnPool</c>, <see cref="Dispose"/> clears state and pushes <c>this</c> back onto
    /// that stack for reuse, eliminating the per-pending-op wrapper allocation when the
    /// session input type is not <see cref="PinnedSpanByte"/>.
    /// </remarks>
    /// <typeparam name="T"></typeparam>
    internal sealed class StandardHeapContainer<T> : IHeapContainer<T>
    {
        private T obj;
        private Stack<IHeapContainer<T>> returnPool;

        public StandardHeapContainer(ref T obj)
        {
            Initialize(ref obj, returnPool: null);
        }

        internal StandardHeapContainer(ref T obj, Stack<IHeapContainer<T>> returnPool)
        {
            Initialize(ref obj, returnPool);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Initialize(ref T obj, Stack<IHeapContainer<T>> returnPool)
        {
            this.obj = obj;
            this.returnPool = returnPool;
        }

        public ref T Get() => ref obj;

        public void Dispose()
        {
            obj = default;
            var pool = returnPool;
            returnPool = null;
            pool?.Push(this);
        }
    }
}