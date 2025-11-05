// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Garnet.server
{
    [StructLayout(LayoutKind.Explicit, Size = 20)]
    public struct ReplicaReadSessionContext
    {
        /// <summary>
        /// Last read sublogIdx
        /// </summary>
        [FieldOffset(0)]
        public long sessionVersion;

        /// <summary>
        /// Maximum session sequence number established from all keys read so far
        /// </summary>
        [FieldOffset(8)]
        public long maximumSessionSequenceNumber;

        /// <summary>
        /// Last read sublogIdx
        /// </summary>
        [FieldOffset(16)]
        public int lastSublogIdx;
    }
}