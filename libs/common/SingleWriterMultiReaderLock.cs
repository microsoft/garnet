// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// Single writer multiple readers lock
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 4)]
    public struct SingleWriterMultiReaderLock
    {
        [FieldOffset(0)]
        int _lock;

        /// <summary>
        /// Check if write locked
        /// </summary>
        public bool IsWriteLocked => _lock < 0;

        /// <summary>
        /// Attempt to acquire write lock but do not wait on failure
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryWriteLock()
            => Interlocked.CompareExchange(ref _lock, int.MinValue, 0) == 0;

        /// <summary>
        /// Acquire writer lock or spin wait until it can be acquired
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteLock()
        {
            while (!TryWriteLock())
                Thread.Yield();
        }

        /// <summary>
        /// Release write lock (spin wait until it can be released)
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteUnlock()
        {
            Debug.Assert(_lock < 0);
            while (Interlocked.CompareExchange(ref _lock, 0, int.MinValue) != int.MinValue)
                Thread.Yield();
        }

        /// <summary>
        /// Try acquire read lock but do not wait on failure
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryReadLock()
        {
            if (_lock >= 0)
            {
                if (Interlocked.Increment(ref _lock) > 0)
                    return true;
                Interlocked.Decrement(ref _lock);
            }
            return false;
        }

        /// <summary>
        /// Attempt to acquire read lock or spin wait until it can be acquired
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReadLock()
        {
            while (!TryReadLock())
                Thread.Yield();
        }

        /// <summary>
        /// Release read lock
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReadUnlock()
        {
            Debug.Assert(_lock > 0);
            Interlocked.Decrement(ref _lock);
        }

        /// <inheritdoc />
        public override string ToString()
            => _lock.ToString();
    }
}