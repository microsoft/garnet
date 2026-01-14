// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;

namespace Garnet.server
{
    /// <summary>
    /// Represents metadata and control objects for a background maintenance task.
    /// </summary>
    public sealed class TaskMetadata
    {
        /// <summary>
        /// Cancellation token source associated with this specific task
        /// </summary>
        public CancellationTokenSource cts;

        /// <summary>
        /// The running task instance
        /// </summary>
        public Task task;
    }
}