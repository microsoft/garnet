// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.networking
{
    /// <summary>
    /// Settings for max sizes of types
    /// </summary>
    public class MaxSizeSettings
    {
        /// <summary>
        /// Max key size
        /// </summary>
        public int MaxKeySize = 4096;

        /// <summary>
        /// Max value size
        /// </summary>
        public int MaxValueSize = 4096;

        /// <summary>
        /// Max input size
        /// </summary>
        public int MaxInputSize = 4096;

        /// <summary>
        /// Max output size
        /// </summary>
        public int MaxOutputSize = 4096;
    }
}