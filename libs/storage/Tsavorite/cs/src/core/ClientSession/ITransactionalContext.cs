// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Transactional context functions. Useful when doing generic locking across diverse 
    /// <see cref="TransactionalUnsafeContext{Key, Value, Input, Output, Context, Functions, StoreFunctions, Allocator}"/> and 
    /// <see cref="TransactionalContext{Key, Value, Input, Output, Context, Functions, StoreFunctions, Allocator}"/> specializations.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    public interface ITransactionalContext<TKey>
    {
        /// <summary>
        /// Begins a series of lock operations on possibly multiple keys; call before any locks are taken.
        /// </summary>
        void BeginTransaction();

        /// <summary>
        /// Ends a series of lock operations on possibly multiple keys; call after all locks are released.
        /// </summary>
        void EndTransaction();

        /// <summary>
        /// Compare two structures that implement <see name="ITransactionalKey"/>.
        /// </summary>
        /// <typeparam name="TTransactionalKey">The type of the app data struct or class containing key info</typeparam>
        /// <param name="key1">The first key to compare</param>
        /// <param name="key2">The first key to compare</param>
        /// <returns>The result of key1.CompareTo(key2)</returns>
        int CompareKeyHashes<TTransactionalKey>(TTransactionalKey key1, TTransactionalKey key2)
            where TTransactionalKey : ITransactionalKey;

        /// <summary>
        /// Compare two structures that implement <see name="ITransactionalKey"/>.
        /// </summary>
        /// <typeparam name="TTransactionalKey">The type of the app data struct or class containing key info</typeparam>
        /// <param name="key1">The first key to compare</param>
        /// <param name="key2">The first key to compare</param>
        /// <returns>The result of key1.CompareTo(key2)</returns>
        int CompareKeyHashes<TTransactionalKey>(ref TTransactionalKey key1, ref TTransactionalKey key2)
            where TTransactionalKey : ITransactionalKey;

        /// <summary>
        /// Sort an array of app data structures (or classes) by lock code and lock type; these will be passed to Transactional*Session.Lock
        /// </summary>
        /// <typeparam name="TTransactionalKey">The type of the app data struct or class containing key info</typeparam>
        /// <param name="keys">The array of app key data </param>
        void SortKeyHashes<TTransactionalKey>(TTransactionalKey[] keys)
            where TTransactionalKey : ITransactionalKey;

        /// <summary>
        /// Sort an array of app data structures (or classes) by lock code and lock type; these will be passed to Transactional*Session.Lock
        /// </summary>
        /// <typeparam name="TTransactionalKey">The type of the app data struct or class containing key info</typeparam>
        /// <param name="keys">The array of app key data </param>
        /// <param name="start">The starting key index to sort</param>
        /// <param name="count">The number of keys to sort</param>
        void SortKeyHashes<TTransactionalKey>(TTransactionalKey[] keys, int start, int count)
            where TTransactionalKey : ITransactionalKey;

        /// <summary>
        /// Locks the keys identified in the passed array.
        /// </summary>
        /// <typeparam name="TTransactionalKey"></typeparam>
        /// <param name="keys">keys to be locked, and whether that locking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TTransactionalKey}(TTransactionalKey[])"/>.</param>
        void Lock<TTransactionalKey>(TTransactionalKey[] keys)
            where TTransactionalKey : ITransactionalKey;

        /// <summary>
        /// Locks the keys identified in the passed array.
        /// </summary>
        /// <typeparam name="TTransactionalKey"></typeparam>
        /// <param name="keys">key hashCodes to be locked, and whether that locking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TTransactionalKey}(TTransactionalKey[])"/>.</param>
        /// <param name="start">The starting key index to Lock</param>
        /// <param name="count">The number of keys to Lock</param>
        void Lock<TTransactionalKey>(TTransactionalKey[] keys, int start, int count)
            where TTransactionalKey : ITransactionalKey;

        /// <summary>
        /// Locks the keys identified in the passed array, with retry limits or cancellation.
        /// </summary>
        /// <typeparam name="TTransactionalKey"></typeparam>
        /// <param name="keys">keys to be locked, and whether that locking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TTransactionalKey}(TTransactionalKey[])"/>.</param>
        bool TryLock<TTransactionalKey>(TTransactionalKey[] keys)
            where TTransactionalKey : ITransactionalKey;

        /// <summary>
        /// Locks the keys identified in the passed array, with retry limits or cancellation.
        /// </summary>
        /// <typeparam name="TTransactionalKey"></typeparam>
        /// <param name="keys">keys to be locked, and whether that locking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TTransactionalKey}(TTransactionalKey[])"/>.</param>
        /// <param name="timeout">TimeSpan limiting the duration of the TryLock() call over all keys.</param>
        bool TryLock<TTransactionalKey>(TTransactionalKey[] keys, TimeSpan timeout)
            where TTransactionalKey : ITransactionalKey;

        /// <summary>
        /// Locks the keys identified in the passed array, with retry limits or cancellation.
        /// </summary>
        /// <typeparam name="TTransactionalKey"></typeparam>
        /// <param name="keys">keys to be locked, and whether that locking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TTransactionalKey}(TTransactionalKey[])"/>.</param>
        /// <param name="start">The starting key index to Lock</param>
        /// <param name="count">The number of keys to Lock</param>
        /// <param name="timeout">TimeSpan limiting the duration of the TryLock() call over all keys.</param>
        bool TryLock<TTransactionalKey>(TTransactionalKey[] keys, int start, int count, TimeSpan timeout)
            where TTransactionalKey : ITransactionalKey;

        /// <summary>
        /// Locks the keys identified in the passed array, with retry limits or cancellation.
        /// </summary>
        /// <typeparam name="TTransactionalKey"></typeparam>
        /// <param name="keys">keys to be locked, and whether that locking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TTransactionalKey}(TTransactionalKey[])"/>.</param>
        /// <param name="cancellationToken">The cancellation token</param>
        bool TryLock<TTransactionalKey>(TTransactionalKey[] keys, CancellationToken cancellationToken)
            where TTransactionalKey : ITransactionalKey;

        /// <summary>
        /// Locks the keys identified in the passed array, with retry limits or cancellation.
        /// </summary>
        /// <typeparam name="TTransactionalKey"></typeparam>
        /// <param name="keys">keys to be locked, and whether that locking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TTransactionalKey}(TTransactionalKey[])"/>.</param>
        /// <param name="start">The starting key index to Lock</param>
        /// <param name="count">The number of keys to Lock</param>
        /// <param name="cancellationToken">The cancellation token, if any</param>
        bool TryLock<TTransactionalKey>(TTransactionalKey[] keys, int start, int count, CancellationToken cancellationToken)
            where TTransactionalKey : ITransactionalKey;

        /// <summary>
        /// Locks the keys identified in the passed array, with retry limits or cancellation.
        /// </summary>
        /// <typeparam name="TTransactionalKey"></typeparam>
        /// <param name="keys">keys to be locked, and whether that locking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TTransactionalKey}(TTransactionalKey[])"/>.</param>
        /// <param name="timeout">TimeSpan limiting the duration of the TryLock() call over all keys.</param>
        /// <param name="cancellationToken">The cancellation token</param>
        bool TryLock<TTransactionalKey>(TTransactionalKey[] keys, TimeSpan timeout, CancellationToken cancellationToken)
            where TTransactionalKey : ITransactionalKey;

        /// <summary>
        /// Promotes a shared lock on the key to an exclusive lock, with retry limits or cancellation.
        /// </summary>
        /// <typeparam name="TTransactionalKey"></typeparam>
        /// <param name="keys">key hashCodes to be locked, and whether that locking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TTransactionalKey}(TTransactionalKey[])"/>.</param>
        /// <param name="start">The starting key index to Lock</param>
        /// <param name="count">The number of keys to Lock</param>
        /// <param name="timeout">TimeSpan limiting the duration of the TryLock() call over all keys.</param>
        /// <param name="cancellationToken">The cancellation token, if any</param>
        bool TryLock<TTransactionalKey>(TTransactionalKey[] keys, int start, int count, TimeSpan timeout, CancellationToken cancellationToken)
            where TTransactionalKey : ITransactionalKey;

        /// <summary>
        /// Tries to promote a shared lock the key to an exclusive lock, with retry limits or cancellation.
        /// </summary>
        /// <typeparam name="TTransactionalKey"></typeparam>
        /// <param name="key">key whose lock is to be promoted.</param>
        /// <remarks>On success, the caller must update the ITransactionalKey.LockType so the unlock has the right type</remarks>
        bool TryPromoteLock<TTransactionalKey>(TTransactionalKey key)
            where TTransactionalKey : ITransactionalKey;

        /// <summary>
        /// Promotes a shared lock on the key to an exclusive lock, with retry limits or cancellation.
        /// </summary>
        /// <typeparam name="TTransactionalKey"></typeparam>
        /// <param name="key">key whose lock is to be promoted.</param>
        /// <param name="cancellationToken">The cancellation token</param>
        /// <remarks>On success, the caller must update the ITransactionalKey.LockType so the unlock has the right type</remarks>
        bool TryPromoteLock<TTransactionalKey>(TTransactionalKey key, CancellationToken cancellationToken)
            where TTransactionalKey : ITransactionalKey;

        /// <summary>
        /// Promotes a shared lock on the key to an exclusive lock, with retry limits or cancellation.
        /// </summary>
        /// <typeparam name="TTransactionalKey"></typeparam>
        /// <param name="key">key whose lock is to be promoted.</param>
        /// <param name="timeout">TimeSpan limiting the duration of the TryPromoteLock() call.</param>
        /// <param name="cancellationToken">The cancellation token, if any</param>
        /// <remarks>On success, the caller must update the ITransactionalKey.LockType so the unlock has the right type</remarks>
        bool TryPromoteLock<TTransactionalKey>(TTransactionalKey key, TimeSpan timeout, CancellationToken cancellationToken)
            where TTransactionalKey : ITransactionalKey;

        /// <summary>
        /// Unlocks the keys identified in the passed array.
        /// </summary>
        /// <typeparam name="TTransactionalKey"></typeparam>
        /// <param name="keys">key hashCodes to be unlocked, and whether that unlocking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TTransactionalKey}(TTransactionalKey[])"/>.</param>
        void Unlock<TTransactionalKey>(TTransactionalKey[] keys)
        where TTransactionalKey : ITransactionalKey;

        /// <summary>
        /// Unlocks the keys identified in the passed array.
        /// </summary>
        /// <typeparam name="TTransactionalKey"></typeparam>
        /// <param name="keys">key hashCodes to be unlocked, and whether that unlocking is shared or exclusive; must be sorted by <see cref="SortKeyHashes{TTransactionalKey}(TTransactionalKey[])"/>.</param>
        /// <param name="start">The starting index to Unlock</param>
        /// <param name="count">The number of keys to Unlock</param>
        void Unlock<TTransactionalKey>(TTransactionalKey[] keys, int start, int count)
            where TTransactionalKey : ITransactionalKey;
    }
}