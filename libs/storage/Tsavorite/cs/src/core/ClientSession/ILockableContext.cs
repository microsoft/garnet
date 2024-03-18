// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Lockable context functions. Useful when doing generic locking across diverse <see cref="LockableUnsafeContext{Key, Value, Input, Output, Context, Functions}"/> and <see cref="LockableContext{Key, Value, Input, Output, Context, Functions}"/> specializations.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    public interface ILockableContext<TKey>
    {
        /// <summary>
        /// Begins a series of lock operations on possibly multiple keys; call before any locks are taken.
        /// </summary>
        void BeginLockable();

        /// <summary>
        /// Ends a series of lock operations on possibly multiple keys; call after all locks are released.
        /// </summary>
        void EndLockable();

        /// <summary>
        /// If true, then keys must use one of the <see cref="ITsavoriteContext{TKey}.GetKeyHash(ref TKey)"/> overloads to obtain a code by which groups of keys will be sorted for manual locking, to avoid deadlocks.
        /// </summary>
        /// <remarks>Whether this returns true depends on the <see cref="ConcurrencyControlMode"/> on <see cref="TsavoriteKVSettings{Key, Value}"/>, or passed to the TsavoriteKV constructor.</remarks>
        bool NeedKeyHash { get; }

        /// <summary>
        /// Compare two structures that implement ILockableKey.
        /// </summary>
        /// <typeparam name="TLockableKey">The type of the app data struct or class containing key info</typeparam>
        /// <param name="key1">The first key to compare</param>
        /// <param name="key2">The first key to compare</param>
        /// <returns>The result of key1.CompareTo(key2)</returns>
        int CompareKeyHashes<TLockableKey>(TLockableKey key1, TLockableKey key2)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Compare two structures that implement ILockableKey.
        /// </summary>
        /// <typeparam name="TLockableKey">The type of the app data struct or class containing key info</typeparam>
        /// <param name="key1">The first key to compare</param>
        /// <param name="key2">The first key to compare</param>
        /// <returns>The result of key1.CompareTo(key2)</returns>
        int CompareKeyHashes<TLockableKey>(ref TLockableKey key1, ref TLockableKey key2)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Sort an array of app data structures (or classes) by lock code and lock type; these will be passed to Lockable*Session.Lock
        /// </summary>
        /// <typeparam name="TLockableKey">The type of the app data struct or class containing key info</typeparam>
        /// <param name="keys">The array of app key data </param>
        void SortKeyHashes<TLockableKey>(TLockableKey[] keys)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Sort an array of app data structures (or classes) by lock code and lock type; these will be passed to Lockable*Session.Lock
        /// </summary>
        /// <typeparam name="TLockableKey">The type of the app data struct or class containing key info</typeparam>
        /// <param name="keys">The array of app key data </param>
        /// <param name="start">The starting key index to sort</param>
        /// <param name="count">The number of keys to sort</param>
        void SortKeyHashes<TLockableKey>(TLockableKey[] keys, int start, int count)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Locks the keys identified in the passed array.
        /// </summary>
        /// <typeparam name="TLockableKey"></typeparam>
        /// <param name="keys">keys to be locked, and whether that locking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TLockableKey}(TLockableKey[])"/>.</param>
        void Lock<TLockableKey>(TLockableKey[] keys)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Locks the keys identified in the passed array.
        /// </summary>
        /// <typeparam name="TLockableKey"></typeparam>
        /// <param name="keys">key hashCodes to be locked, and whether that locking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TLockableKey}(TLockableKey[])"/>.</param>
        /// <param name="start">The starting key index to Lock</param>
        /// <param name="count">The number of keys to Lock</param>
        void Lock<TLockableKey>(TLockableKey[] keys, int start, int count)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Locks the keys identified in the passed array, with retry limits or cancellation.
        /// </summary>
        /// <typeparam name="TLockableKey"></typeparam>
        /// <param name="keys">keys to be locked, and whether that locking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TLockableKey}(TLockableKey[])"/>.</param>
        bool TryLock<TLockableKey>(TLockableKey[] keys)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Locks the keys identified in the passed array, with retry limits or cancellation.
        /// </summary>
        /// <typeparam name="TLockableKey"></typeparam>
        /// <param name="keys">keys to be locked, and whether that locking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TLockableKey}(TLockableKey[])"/>.</param>
        /// <param name="timeout">TimeSpan limiting the duration of the TryLock() call over all keys.</param>
        bool TryLock<TLockableKey>(TLockableKey[] keys, TimeSpan timeout)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Locks the keys identified in the passed array, with retry limits or cancellation.
        /// </summary>
        /// <typeparam name="TLockableKey"></typeparam>
        /// <param name="keys">keys to be locked, and whether that locking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TLockableKey}(TLockableKey[])"/>.</param>
        /// <param name="start">The starting key index to Lock</param>
        /// <param name="count">The number of keys to Lock</param>
        /// <param name="timeout">TimeSpan limiting the duration of the TryLock() call over all keys.</param>
        bool TryLock<TLockableKey>(TLockableKey[] keys, int start, int count, TimeSpan timeout)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Locks the keys identified in the passed array, with retry limits or cancellation.
        /// </summary>
        /// <typeparam name="TLockableKey"></typeparam>
        /// <param name="keys">keys to be locked, and whether that locking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TLockableKey}(TLockableKey[])"/>.</param>
        /// <param name="cancellationToken">The cancellation token</param>
        bool TryLock<TLockableKey>(TLockableKey[] keys, CancellationToken cancellationToken)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Locks the keys identified in the passed array, with retry limits or cancellation.
        /// </summary>
        /// <typeparam name="TLockableKey"></typeparam>
        /// <param name="keys">keys to be locked, and whether that locking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TLockableKey}(TLockableKey[])"/>.</param>
        /// <param name="start">The starting key index to Lock</param>
        /// <param name="count">The number of keys to Lock</param>
        /// <param name="cancellationToken">The cancellation token, if any</param>
        bool TryLock<TLockableKey>(TLockableKey[] keys, int start, int count, CancellationToken cancellationToken)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Locks the keys identified in the passed array, with retry limits or cancellation.
        /// </summary>
        /// <typeparam name="TLockableKey"></typeparam>
        /// <param name="keys">keys to be locked, and whether that locking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TLockableKey}(TLockableKey[])"/>.</param>
        /// <param name="timeout">TimeSpan limiting the duration of the TryLock() call over all keys.</param>
        /// <param name="cancellationToken">The cancellation token</param>
        bool TryLock<TLockableKey>(TLockableKey[] keys, TimeSpan timeout, CancellationToken cancellationToken)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Promotes a shared lock on the key to an exclusive lock, with retry limits or cancellation.
        /// </summary>
        /// <typeparam name="TLockableKey"></typeparam>
        /// <param name="keys">key hashCodes to be locked, and whether that locking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TLockableKey}(TLockableKey[])"/>.</param>
        /// <param name="start">The starting key index to Lock</param>
        /// <param name="count">The number of keys to Lock</param>
        /// <param name="timeout">TimeSpan limiting the duration of the TryLock() call over all keys.</param>
        /// <param name="cancellationToken">The cancellation token, if any</param>
        bool TryLock<TLockableKey>(TLockableKey[] keys, int start, int count, TimeSpan timeout, CancellationToken cancellationToken)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Tries to promote a shared lock the key to an exclusive lock, with retry limits or cancellation.
        /// </summary>
        /// <typeparam name="TLockableKey"></typeparam>
        /// <param name="key">key whose lock is to be promoted.</param>
        /// <remarks>On success, the caller must update the ILockableKey.LockType so the unlock has the right type</remarks>
        bool TryPromoteLock<TLockableKey>(TLockableKey key)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Promotes a shared lock on the key to an exclusive lock, with retry limits or cancellation.
        /// </summary>
        /// <typeparam name="TLockableKey"></typeparam>
        /// <param name="key">key whose lock is to be promoted.</param>
        /// <param name="cancellationToken">The cancellation token</param>
        /// <remarks>On success, the caller must update the ILockableKey.LockType so the unlock has the right type</remarks>
        bool TryPromoteLock<TLockableKey>(TLockableKey key, CancellationToken cancellationToken)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Promotes a shared lock on the key to an exclusive lock, with retry limits or cancellation.
        /// </summary>
        /// <typeparam name="TLockableKey"></typeparam>
        /// <param name="key">key whose lock is to be promoted.</param>
        /// <param name="timeout">TimeSpan limiting the duration of the TryPromoteLock() call.</param>
        /// <param name="cancellationToken">The cancellation token, if any</param>
        /// <remarks>On success, the caller must update the ILockableKey.LockType so the unlock has the right type</remarks>
        bool TryPromoteLock<TLockableKey>(TLockableKey key, TimeSpan timeout, CancellationToken cancellationToken)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Unlocks the keys identified in the passed array.
        /// </summary>
        /// <typeparam name="TLockableKey"></typeparam>
        /// <param name="keys">key hashCodes to be unlocked, and whether that unlocking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TLockableKey}(TLockableKey[])"/>.</param>
        void Unlock<TLockableKey>(TLockableKey[] keys)
        where TLockableKey : ILockableKey;

        /// <summary>
        /// Unlocks the keys identified in the passed array.
        /// </summary>
        /// <typeparam name="TLockableKey"></typeparam>
        /// <param name="keys">key hashCodes to be unlocked, and whether that unlocking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TLockableKey}(TLockableKey[])"/>.</param>
        /// <param name="start">The starting index to Unlock</param>
        /// <param name="count">The number of keys to Unlock</param>
        void Unlock<TLockableKey>(TLockableKey[] keys, int start, int count)
            where TLockableKey : ILockableKey;
    }
}