// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Decides how a session's operation waits for a key whose hash bucket another session holds latched.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Without a handler, an operation that cannot latch its bucket retries on the thread that issued it, holding the
    /// epoch, until the holder lets go. A holder that keeps the latch across a device read or a long transaction then
    /// occupies that thread for as long.
    /// </para>
    /// <para>
    /// A handler is called with the epoch released, so it may wait. Returning retries the operation from the start;
    /// throwing abandons it, leaving the store as it was before the operation began and the epoch released once the
    /// exception leaves the context call. It is consulted for top-level operations of a session that does not lock
    /// transactionally, and for a transactional <c>Lock</c> taken while the session holds no other locks, which then
    /// holds none of its keys while the handler waits and retries them all when it returns. A session that holds log
    /// pointers across operations, as an unsafe context does, must not install one.
    /// </para>
    /// </remarks>
    public interface ILockContentionHandler
    {
        /// <summary>
        /// Called when an operation has found its key's bucket latched in a mode that conflicts with its own.
        /// </summary>
        /// <param name="contention">The contended operation, and a probe of whether the conflict persists.</param>
        void OnLockContended(in LockContention contention);
    }

    /// <summary>
    /// An operation waiting on a conflicting latch, passed to <see cref="ILockContentionHandler.OnLockContended"/>.
    /// </summary>
    public readonly struct LockContention
    {
        readonly TsavoriteBase store;

        internal LockContention(TsavoriteBase store, long keyHash, bool exclusive)
        {
            this.store = store;
            KeyHash = keyHash;
            Exclusive = exclusive;
        }

        /// <summary>Hash of the key the operation is waiting to latch.</summary>
        public long KeyHash { get; }

        /// <summary>Whether the operation needs the bucket exclusively, as an update does, rather than shared.</summary>
        public bool Exclusive { get; }

        /// <summary>Whether another session still holds the bucket latched in a conflicting mode.</summary>
        public bool IsContended => store.HasConflictingLatch(KeyHash, Exclusive);

        /// <summary>
        /// Whether a key with hash <paramref name="keyHash"/> falls in the bucket the operation is waiting on, so that
        /// releasing that key's latch may end the wait.
        /// </summary>
        /// <param name="keyHash">Hash of a key whose latch is being released.</param>
        public bool SharesBucketWith(long keyHash) => store.InSameBucket(KeyHash, keyHash);
    }
}