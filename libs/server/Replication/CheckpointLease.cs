// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Garnet.server
{
    /// <summary>
    /// Holds a checkpoint reference until the lease is disposed.
    /// </summary>
    internal sealed class CheckpointLease<T> : IDisposable where T : class
    {
        Action release;

        /// <summary>
        /// Checkpoint information protected by this lease.
        /// </summary>
        public T Value { get; }

        /// <summary>
        /// Creates a checkpoint lease.
        /// </summary>
        public CheckpointLease(T value, Action release)
        {
            Value = value;
            this.release = release;
        }

        /// <inheritdoc />
        public void Dispose()
            => Interlocked.Exchange(ref release, null)?.Invoke();
    }
}