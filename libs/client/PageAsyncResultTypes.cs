// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.client
{
    class CountWrapper
    {
        public int count;
        public long untilAddress;
    }

    /// <summary>
    /// Page async flush result
    /// </summary>
    class PageAsyncFlushResult
    {
        public CountWrapper count;
        public long page;
        internal long fromOffset;
        internal long untilOffset;
    }

    /// <summary>
    /// Out-of-line payload async flush result
    /// </summary>
    class PayloadAsyncFlushResult
    {
        public CountWrapper count;
        public Payload payload;
        public int remainingChunks;
    }
}