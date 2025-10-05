// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// The reason for a call to <see cref="IRecordDisposer.DisposeValueObject(IHeapObject, DisposeReason)"/>
    /// </summary>
    public enum DisposeReason
    {
        /// <summary>
        /// No Dispose() call was made
        /// </summary>
        None,

        /// <summary>
        /// CopyUpdate cleared the object immediately for more efficient size tracking
        /// </summary>
        CopyUpdated,

        /// <summary>
        /// Failure of InitialWriter insertion of a record at the tail of the cache.
        /// </summary>
        InitialWriterCASFailed,

        /// <summary>
        /// Failure of CopyUpdater insertion of a record at the tail of the cache.
        /// </summary>
        CopyUpdaterCASFailed,

        /// <summary>
        /// Failure of InitialUpdater insertion of a record at the tail of the cache.
        /// </summary>
        InitialUpdaterCASFailed,

        /// <summary>
        /// Failure of InitialDeleter insertion of a record at the tail of the cache.
        /// </summary>
        InitialDeleterCASFailed,

        /// <summary>
        /// Some CAS failed and retry could not use the record due to size or address restrictions
        /// </summary>
        CASAndRetryFailed,

        /// <summary>
        /// A record was deserialized from the disk (or network buffer) for a pending Read or RMW operation.
        /// </summary>
        DeserializedFromDisk,

        /// <summary>
        /// A record was retrieved from the revivification freelist, and thus the key space may have to be adjusted as well.
        /// </summary>
        RevivificationFreeList,

        /// <summary>
        /// A new record was created for Upsert or RMW but the InitialWriter or InitialUpdater operation returned false
        /// </summary>
        InsertAbandoned,

        /// <summary>
        /// Deleted but remains in hash chain so Key is unchanged
        /// </summary>
        Deleted,

        /// <summary>
        /// Record expiration
        /// </summary>
        Expired,

        /// <summary>
        /// Elided from hash chain but not put into Revivification free list
        /// </summary>
        Elided,

        /// <summary>
        /// A page was evicted from the in-memory portion of the main log, or from the readcache.
        /// </summary>
        PageEviction
    }
}