// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// An interface to allow the caller to record start and stop for pending IO completions (for example, to track the time spent there,
    /// or the percentage of operations that went pending.
    /// </summary>
    public interface IPendingMetrics
    {
        /// <summary>Pending completion has been started</summary>
        void StartPendingMetrics();

        /// <summary>Pending completion has ended</summary>
        void StopPendingMetrics();
    }
}
