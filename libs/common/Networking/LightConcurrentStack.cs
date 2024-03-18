// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;

namespace Garnet.common
{
    internal class LightConcurrentStack<T> : IDisposable
        where T : class, IDisposable
    {
        readonly T[] stack;
        int tail;
        SpinLock latch;
        bool disposed;

        public LightConcurrentStack(int maxCapacity = 128)
        {
            stack = new T[maxCapacity];
            tail = 0;
            latch = new SpinLock();
            disposed = false;
        }

        public void Dispose()
        {
            var lockTaken = false;
            latch.Enter(ref lockTaken);
            Debug.Assert(lockTaken);
            disposed = true;
            while (tail > 0)
            {
                var elem = stack[--tail];
                elem.Dispose();
            }
            latch.Exit();
        }

        public bool TryPush(T elem)
        {
            var lockTaken = false;
            latch.Enter(ref lockTaken);
            Debug.Assert(lockTaken);
            if (disposed || tail == stack.Length)
            {
                latch.Exit();
                return false;
            }
            stack[tail++] = elem;
            latch.Exit();
            return true;
        }

        public bool TryPop(out T elem, out bool disposed)
        {
            elem = null;
            var lockTaken = false;
            latch.Enter(ref lockTaken);
            Debug.Assert(lockTaken);
            disposed = this.disposed;
            if (tail == 0)
            {
                latch.Exit();
                return false;
            }

            elem = stack[--tail];
            latch.Exit();
            return true;
        }
    }
}