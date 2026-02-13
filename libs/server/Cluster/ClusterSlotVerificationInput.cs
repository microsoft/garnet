// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    public struct ClusterSlotVerificationInput
    {
        /// <summary>
        /// Whether this is a read only command
        /// </summary>
        public bool readOnly;

        /// <summary>
        /// Whether ASKING is enabled for this command
        /// </summary>
        public byte sessionAsking;

        /// <summary>
        /// Offset of first key in the ArgSlice buffer
        /// </summary>
        public int firstKey;

        /// <summary>
        /// Offset of the last key in the ArgSlice buffer
        /// </summary>
        public int lastKey;

        /// <summary>
        /// The step, or increment, between the first key and the position of the next key
        /// </summary>
        public int step;

        /// <summary>
        /// Offset of key num if any
        /// </summary>
        public int keyNumOffset;

        /// <summary>
        /// Check if asking is set
        /// </summary>
        public readonly bool Asking => sessionAsking > 0;
    }
}