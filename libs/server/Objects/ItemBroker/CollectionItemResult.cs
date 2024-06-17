// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Result of item retrieved from observed collection
    /// </summary>
    internal readonly struct CollectionItemResult(byte[] key, byte[] item)
    {
        /// <summary>
        /// True if item was found
        /// </summary>
        internal bool Found => Key != default;

        /// <summary>
        /// Key of collection from which item was retrieved
        /// </summary>
        internal byte[] Key { get; } = key;

        /// <summary>
        /// Item retrieved from collection
        /// </summary>
        internal byte[] Item { get; } = item;

        /// <summary>
        /// Instance of empty result
        /// </summary>
        internal static readonly CollectionItemResult Empty = new(null, null);
    }
}