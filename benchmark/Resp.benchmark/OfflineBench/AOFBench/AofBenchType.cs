// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Resp.benchmark
{
    public enum AofBenchType
    {
        /// <summary>
        /// Enqueue to sublog randomly.
        /// </summary>
        EnqueueRandom,
        /// <summary>
        /// Enqueue to sublog in a sharded manner.
        /// </summary>
        EnqueueSharded,
        /// <summary>
        /// Simulate AOF replay.
        /// </summary>
        Replay
    }
}