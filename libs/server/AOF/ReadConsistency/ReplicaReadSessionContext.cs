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
        /// Maximum session timestamp
        /// </summary>
        [FieldOffset(8)]
        public long maximumSessionTimestamp;

        /// <summary>
        /// Last read sublogIdx
        /// </summary>
        [FieldOffset(16)]
        public int lastSublogIdx;        
    }
}