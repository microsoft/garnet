// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// Single writer multiple readers lock with lock closure capability
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 4)]
    public struct SingleWriterMultiReaderLock
    {
        [FieldOffset(0)]
        int _lock;

        const int ExclusiveLock = 1 << 31;
        const int ClosedLock = 1 << 30;
        const int MaxReaders = ClosedLock - 1;

        /// <summary>
        /// Basic Constructor
        /// </summary>
        public SingleWriterMultiReaderLock() => _lock = 0;

        /// <summary>
        /// Check if write locked
        /// </summary>
        public bool IsWriteLocked => _lock == ExclusiveLock;

        /// <summary>
        /// Check if closed (no new locks can be acquired)
        /// </summary>
        public bool IsClosed => _lock == ClosedLock;

        /// <summary>
        /// Attempt to acquire write lock but do not wait on failure
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryWriteLock()
        {
            if (Interlocked.CompareExchange(ref _lock, ExclusiveLock, 0) == 0)
                return true;
            if (_lock == ClosedLock)
                ThrowException("Lock is closed; cannot take write lock");
            return false;
        }

        /// <summary>
        /// Acquire writer lock or spin wait until it can be acquired
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteLock()
        {
            while (!TryWriteLock())
                _ = Thread.Yield();
        }

        /// <summary>
        /// Release write lock.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteUnlock()
        {
            Debug.Assert(_lock == ExclusiveLock);
            _lock = 0;
        }

        /// <summary>
        /// Try acquire read lock but do not wait on failure
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryReadLock()
        {
            if (_lock < MaxReaders)
            {
                if (Interlocked.Increment(ref _lock) <= MaxReaders)
                    return true;
                _ = Interlocked.Decrement(ref _lock);
            }

            if (_lock == ClosedLock)
                ThrowException("Lock is closed; cannot take read lock");
            return false;
        }

        /// <summary>
        /// Attempt to acquire read lock or spin wait until it can be acquired
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReadLock()
        {
            while (!TryReadLock())
                _ = Thread.Yield();
        }

        /// <summary>
        /// Release read lock
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReadUnlock()
        {
            Debug.Assert(_lock > 0 && _lock <= MaxReaders);
            _ = Interlocked.Decrement(ref _lock);
        }

        /// <summary>
        /// Attempt to update a held read lock to a write lock.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryUpgradeReadLock()
        {
            Debug.Assert(_lock > 0 && _lock <= MaxReaders, "Illegal to call when not holding a read lock");

            // If caller is the only reader (_lock == 1), this will succeed in becoming a write lock
            return Interlocked.CompareExchange(ref _lock, ExclusiveLock, 1) == 1;
        }

        /// <summary>
        /// Downgrade a held write lock to a read lock.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DowngradeWriteLock()
        {
            Debug.Assert(_lock == ExclusiveLock, "Illegal to call when not holding write lock");
            _lock = 1;
        }

        /// <summary>
        /// Close lock, spin waiting until it is closed.
        /// NOTE: once closed this lock cannot be used for reads or writes as it is considered disposed.
        /// </summary>
        /// <returns>
        /// Return true if current thread closes the lock.
        /// Return false if some other thread closes the lock.
        /// </returns>
        public bool CloseLock()
        {
            while (true)
            {
                if (Interlocked.CompareExchange(ref _lock, ClosedLock, 0) == 0)
                    return true;
                if (_lock == ClosedLock)
                    return false;
                _ = Thread.Yield();
            }
        }

        /// <inheritdoc />
        public override string ToString()
            => _lock.ToString();

        [MethodImpl(MethodImplOptions.NoInlining)]
        static void ThrowException(string str) => throw new GarnetException(str);
    }
}