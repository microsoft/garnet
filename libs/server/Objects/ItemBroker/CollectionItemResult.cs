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

        public CollectionItemResult(byte[] key, double score, byte[] item)
        {
            Key = key;
            Score = score;
            Item = item;
        }

        public CollectionItemResult(byte[] key, double[] scores, byte[][] items)
        {
            Key = key;
            Scores = scores;
            Items = items;
        }

        private CollectionItemResult(bool isForceUnblocked, bool isTypeMismatch)
        {
            IsForceUnblocked = isForceUnblocked;
            IsTypeMismatch = isTypeMismatch;
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
        /// Score associated with the item retrieved from the collection
        /// </summary>
        internal double Score { get; }

        /// <summary>
        /// Item retrieved from collection
        /// </summary>
        internal byte[][] Items { get; }

        /// <summary>
        /// Scores associated with the items retrieved from the collection
        /// </summary>
        internal double[] Scores { get; }

        /// <summary>
        /// Gets a value indicating whether the item retrieval was force unblocked.
        /// </summary>
        internal bool IsForceUnblocked { get; }

        /// <summary>
        /// Gets a value indicating whether the item retrieval returned a type mismatch.
        /// </summary>
        internal bool IsTypeMismatch { get; }

        /// <summary>
        /// Instance of empty result
        /// </summary>
        internal static readonly CollectionItemResult Empty = new(null, item: null);

        /// <summary>
        /// Instance representing a Force Unblocked result.
        /// </summary>
        internal static readonly CollectionItemResult ForceUnblocked = new(isForceUnblocked: true, isTypeMismatch: false);

        /// <summary>
        /// Instance representing a Type Mismatch result.
        /// </summary>
        internal static readonly CollectionItemResult TypeMismatch = new(isForceUnblocked: false, isTypeMismatch: true);
    }
}