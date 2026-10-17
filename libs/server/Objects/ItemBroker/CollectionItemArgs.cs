// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// The command-specific arguments a blocking collection command hands to <see cref="CollectionItemBroker"/>,
    /// alongside the keys it observes.
    /// </summary>
    /// <remarks>
    /// Held by value on the observer, which lives on the heap for as long as the command waits. The broker reads
    /// these on its own thread, after the calling session has suspended and its stack has unwound, so the
    /// arguments may not reference the calling frame.
    /// </remarks>
    internal readonly struct CollectionItemArgs
    {
        /// <summary>
        /// Key of the collection to push to, on the pinned object heap. BLMOVE and BRPOPLPUSH only.
        /// </summary>
        internal readonly byte[] DestinationKey;

        /// <summary>
        /// End of the source collection to pop from.
        /// </summary>
        internal readonly OperationDirection SourceDirection;

        /// <summary>
        /// End of the destination collection to push to. BLMOVE and BRPOPLPUSH only.
        /// </summary>
        internal readonly OperationDirection DestinationDirection;

        /// <summary>
        /// Maximum number of elements to pop. BLMPOP and BZMPOP only.
        /// </summary>
        internal readonly int Count;

        /// <summary>
        /// Whether to pop the lowest scores first. BZMPOP only.
        /// </summary>
        internal readonly bool LowScoresFirst;

        private CollectionItemArgs(byte[] destinationKey, OperationDirection sourceDirection,
            OperationDirection destinationDirection, int count, bool lowScoresFirst)
        {
            DestinationKey = destinationKey;
            SourceDirection = sourceDirection;
            DestinationDirection = destinationDirection;
            Count = count;
            LowScoresFirst = lowScoresFirst;
        }

        /// <summary>
        /// Arguments for moving a single element between two lists (BLMOVE, BRPOPLPUSH).
        /// </summary>
        /// <param name="destinationKey">Key of the list to push to, on the pinned object heap.</param>
        /// <param name="sourceDirection">End of the source list to pop from.</param>
        /// <param name="destinationDirection">End of the destination list to push to.</param>
        /// <returns>The arguments.</returns>
        internal static CollectionItemArgs ForMove(byte[] destinationKey, OperationDirection sourceDirection,
            OperationDirection destinationDirection)
            => new(destinationKey, sourceDirection, destinationDirection, count: 1, lowScoresFirst: false);

        /// <summary>
        /// Arguments for popping from a list (BLMPOP).
        /// </summary>
        /// <param name="direction">End of the list to pop from.</param>
        /// <param name="count">Maximum number of elements to pop.</param>
        /// <returns>The arguments.</returns>
        internal static CollectionItemArgs ForListPop(OperationDirection direction, int count)
            => new(destinationKey: null, direction, OperationDirection.Unknown, count, lowScoresFirst: false);

        /// <summary>
        /// Arguments for popping from a sorted set (BZMPOP).
        /// </summary>
        /// <param name="lowScoresFirst">Whether to pop the lowest scores first.</param>
        /// <param name="count">Maximum number of elements to pop.</param>
        /// <returns>The arguments.</returns>
        internal static CollectionItemArgs ForSortedSetPop(bool lowScoresFirst, int count)
            => new(destinationKey: null, OperationDirection.Unknown, OperationDirection.Unknown, count, lowScoresFirst);
    }
}