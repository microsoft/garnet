// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;
using System.Threading;

namespace Garnet.server
{
    [StructLayout(LayoutKind.Explicit, Size = 12)]
    public struct ReplicaReadContext
    {
        /// <summary>
        /// Last read sublogIdx
        /// </summary>
        [FieldOffset(0)]
        public long lastSublogIdx;

        /// <summary>
        /// Maximum session timestamp
        /// </summary>
        [FieldOffset(4)]
        public long maximumTimestamp;
    }

    public class ReadSessionWaiter
    {
        public ManualResetEventSlim eventSlim;
        public long waitForTimestamp;
        public long hash;
    }
}