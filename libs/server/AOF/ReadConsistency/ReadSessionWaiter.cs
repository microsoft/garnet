// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;

namespace Garnet.server
{
    public class ReadSessionWaiter
    {
        public ManualResetEventSlim eventSlim;
        public long waitForTimestamp;
        public byte sublogIdx;
        public int keyOffset;
    }
}