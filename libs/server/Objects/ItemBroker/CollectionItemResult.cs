// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Result of item retrieved from observed collection
    /// </summary>
    internal class CollectionItemResult
    {
        /// <summary>
        /// True if item was found
        /// </summary>
        internal bool Found => Key != default;

        /// <summary>
        /// Key of collection from which item was retrieved
        /// </summary>
        internal byte[] Key { get; }

        /// <summary>
        /// Item retrieved from collection
        /// </summary>
        internal byte[] Item { get; }

        /// <summary>
        /// Instance of empty result
        /// </summary>
        internal static readonly CollectionItemResult Empty = new(null, null);

        public CollectionItemResult(byte[] key, byte[] item)
        {
            Key = key;
            Item = item;
        }
    }
}