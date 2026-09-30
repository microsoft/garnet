// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// One key registered by WATCH, held per session in <see cref="WatchedKeysContainer"/>.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 29)]
    struct WatchedKeySlice
    {
        /// <summary>
        /// The version read from the <see cref="WatchVersionMap"/> slot for <see cref="hash"/> at the time of the
        /// WATCH. EXEC aborts if the slot no longer holds this value.
        /// </summary>
        [FieldOffset(0)]
        public long version;

        /// <summary>
        /// The watched key bytes, copied into the container's own allocator so they stay valid for the life of the
        /// watch. Used for locking, cluster slot verification, and <see cref="WatchedKeysContainer.RemoveWatch"/>,
        /// never for version lookup.
        /// </summary>
        [FieldOffset(8)]
        public PinnedSpanByte slice;

        /// <summary>
        /// <see cref="Utility.HashBytes"/> of the key, and the sole identity used to look up its version.
        /// </summary>
        /// <remarks>
        /// <see cref="WatchVersionMap"/> holds versions in a fixed-size array indexed by <c>hash &amp; sizeMask</c> and
        /// stores no key bytes, so neither the watcher nor the writer that bumps a version ever compares the key
        /// itself. Writers pass the Tsavorite record's key hash, which is the same <see cref="Utility.HashBytes"/>
        /// value, so a modification of this key always lands on the slot recorded here.
        /// <para>
        /// Two distinct keys can share a slot, either through a 64-bit hash collision or, far more commonly, because
        /// their hashes alias under the mask, and a write to either bumps the version both observe. That is accepted by
        /// design: hash-only lookup buys a single interlocked read per key at WATCH and at EXEC with no key storage or
        /// comparison in the map shared by every session, and it errs conservatively. A collision can only add a
        /// spurious conflict, aborting a transaction that would have been safe to run; it can never hide a real
        /// modification, because a write to the watched key always increments the slot the watcher recorded. Clients
        /// must already handle EXEC returning nil and retry.
        /// </para>
        /// </remarks>
        [FieldOffset(20)]
        public long hash;

        /// <summary>
        /// True while this entry takes part in locking and version validation.
        /// <see cref="WatchedKeysContainer.RemoveWatch"/> clears it as a soft delete, leaving the entry in place.
        /// </summary>
        [FieldOffset(28)]
        public bool isWatched;
    }
}