// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Simple queue containing items that may be ref or value types, but does not call Dispose; if TElement is IDisposable
    /// it must be owned/disposed elsewhere.
    /// </summary>
    /// <typeparam name="TElement"></typeparam>
    class SimpleConcurrentStack<TElement>
    {
        public const int DefaultInitialCapacity = 1024;

        /// <summary>The actual stack, as a simple growable vector</summary>
        TElement[] stack;

        TailAndLatch tailAndLatch = new();

        public SimpleConcurrentStack(int initialCapacity = DefaultInitialCapacity)
        {
            stack = new TElement[initialCapacity];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryPush(TElement elem)
        {
            tailAndLatch.AcquireForPush(ref stack);
            stack[tailAndLatch.Tail] = elem;
            _ = tailAndLatch.ClearLatch();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryPop(out TElement element)
        {
            if (!tailAndLatch.TryAcquireForPop())
            {
                element = default;
                return false;
            }

            element = stack[tailAndLatch.Tail];
            _ = tailAndLatch.ClearLatch();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear() => Array.Clear(stack);
    }
}
