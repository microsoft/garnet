// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Result of item retrieved from observed collection
    /// </summary>
    internal readonly struct CollectionItemResult
    {
        public CollectionItemResult(byte[] key, byte[] item)
        {
            Key = key;
            Item = item;
        }

        public CollectionItemResult(byte[] key, byte[][] items)
        {
            Key = key;
            Items = items;
        }

        private CollectionItemResult(bool isError)
        {
            IsError = isError;
        }

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
        /// Item retrieved from collection
        /// </summary>
        internal byte[][] Items { get; }

        /// <summary>
        /// Indicates whether the result represents an error.
        /// </summary>
        internal readonly bool IsError { get; }

        /// <summary>
        /// Instance of empty result
        /// </summary>
        internal static readonly CollectionItemResult Empty = new(null, item: null);

        /// <summary>
        /// Instance representing an error result.
        /// </summary>
        internal static readonly CollectionItemResult Error = new(true);
    }
}