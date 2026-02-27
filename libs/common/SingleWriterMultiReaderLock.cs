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
        /// Basic Constructor
        /// </summary>
        public SingleWriterMultiReaderLock() => _lock = 0;

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

        /// <summary>
        /// Attempt to update a held read lock to a write lock.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryUpgradeReadLock()
        {
            Debug.Assert(_lock > 0, "Illegal to call when not holding a read lock");

            // If caller is the only reader (_lock == 1), this will succeed in becoming a write lock (_lock == int.MinValue)
            return Interlocked.CompareExchange(ref _lock, int.MinValue, 1) == 1;
        }

        /// <summary>
        /// Downgrade a held write lock to a read lock.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DowngradeWriteLock()
        {
            Debug.Assert(_lock < 0, "Illegal to call when not holding write lock");

            while (Interlocked.CompareExchange(ref _lock, 1, int.MinValue) != int.MinValue)
            {
                _ = Thread.Yield();
            }
        }

        /// <summary>
        /// Try acquire write lock and spin wait until isWriteLocked
        /// NOTE: once closed this lock should never be unlocked because is considered disposed
        /// </summary>
        /// <returns>
        /// Return true if current thread acquired write lock.
        /// Return false if some other thread acquired write lock.
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryCloseLock()
        {
            while (true)
            {
                var isWriteLocked = IsWriteLocked;
                var acquiredWriteLock = TryWriteLock();
                if (isWriteLocked || acquiredWriteLock)
                {
                    return acquiredWriteLock;
                }
                Thread.Yield();
            }
        }

        /// <inheritdoc />
        public override string ToString()
            => _lock.ToString();
    }
}