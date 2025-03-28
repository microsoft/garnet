// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Result of item retrieved from observed collection
    /// </summary>
    internal readonly struct CollectionItemResult
    {
        public CollectionItemResult(GarnetStatus status)
        {
            Status = status;
        }

        public CollectionItemResult(byte[] key, byte[] item)
        {
            Key = key;
            Item = item;
            Status = key == default ? GarnetStatus.NOTFOUND : GarnetStatus.OK;
        }

        public CollectionItemResult(byte[] key, byte[][] items)
        {
            Key = key;
            Items = items;
            Status = key == default ? GarnetStatus.NOTFOUND : GarnetStatus.OK;
        }

        public CollectionItemResult(byte[] key, double score, byte[] item)
        {
            Key = key;
            Item = item;
            Score = score;
            Status = key == default ? GarnetStatus.NOTFOUND : GarnetStatus.OK;
        }

        public CollectionItemResult(byte[] key, double[] scores, byte[][] items)
        {
            Key = key;
            Items = items;
            Scores = scores;
            Status = key == default ? GarnetStatus.NOTFOUND : GarnetStatus.OK;
        }

        private CollectionItemResult(bool isForceUnblocked)
        {
            IsForceUnblocked = isForceUnblocked;
        }

        /// <summary>
        /// Result status
        /// </summary>
        internal GarnetStatus Status { get; }

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
        internal readonly bool IsForceUnblocked { get; }

        /// <summary>
        /// Instance of empty result
        /// </summary>
        internal static readonly CollectionItemResult Empty = new(null, item: null);

        /// <summary>
        /// Instance representing an Force Unblocked result.
        /// </summary>
        internal static readonly CollectionItemResult ForceUnblocked = new(true);
    }
}