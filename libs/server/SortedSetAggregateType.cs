// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Specifies the type of aggregation to be used in sorted set operations.
    /// </summary>
    public enum SortedSetAggregateType : byte
    {
        /// <summary>
        /// Sum the values.
        /// </summary>
        Sum,
        /// <summary>
        /// Use the minimum value.
        /// </summary>
        Min,
        /// <summary>
        /// Use the maximum value.
        /// </summary>
        Max
    }
}