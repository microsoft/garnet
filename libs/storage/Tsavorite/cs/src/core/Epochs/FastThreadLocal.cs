// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Fast implementation of instance-thread-local variables
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal sealed class FastThreadLocal<T>
    {
        // Max instances supported
        private const int kMaxInstances = 128;

        [ThreadStatic]
        private static T[] tl_values;
        [ThreadStatic]
        private static int[] tl_iid;

        private readonly int offset;
        private readonly int iid;

        private static readonly int[] instances = new int[kMaxInstances];
        private static int instanceId = 0;

        public FastThreadLocal()
        {
            iid = Interlocked.Increment(ref instanceId);

            for (int i = 0; i < kMaxInstances; i++)
            {
                if (0 == Interlocked.CompareExchange(ref instances[i], iid, 0))
                {
                    offset = i;
                    return;
                }
            }
            throw new TsavoriteException("Unsupported number of simultaneous instances");
        }

        public void InitializeThread()
        {
            if (tl_values == null)
            {
                tl_values = new T[kMaxInstances];
                tl_iid = new int[kMaxInstances];
            }
            if (tl_iid[offset] != iid)
            {
                tl_iid[offset] = iid;
                tl_values[offset] = default(T);
            }
        }

        public void DisposeThread()
        {
            tl_values[offset] = default(T);
            tl_iid[offset] = 0;
        }

        /// <summary>
        /// Dispose instance for all threads
        /// </summary>
        public void Dispose()
        {
            instances[offset] = 0;
        }

        public T Value
        {
            get => tl_values[offset];
            set => tl_values[offset] = value;
        }

        public bool IsInitializedForThread => (tl_values != null) && (iid == tl_iid[offset]);
    }
}