// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using Garnet.common;

namespace Garnet.cluster
{
    internal unsafe class ReplayRecordState(int replayTasks)
    {
        public byte* record;
        public int recordLength;
        public long currentAddress;
        public long nextAddress;
        public bool isProtected;
        public ManualResetEventSlim Completed = new(false);
        public EventBarrier eventBarrier = new(replayTasks);

        public void Reset()
        {
            Completed.Reset();
            eventBarrier.Reset();
        }
    }
}
