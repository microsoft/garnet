// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// Applies hard byte admission before memory allocation so concurrent producers cannot exceed a configured
    /// in-flight quota.
    /// </summary>
    /// <remarks>
    /// A zero-byte capacity disables accounting and throttling. Applications compose this tracker with a
    /// <c>WaiterQueue&lt;int&gt;</c> to provide admission retries and waiting.
    /// GarnetLightClient integration is intentionally reserved for a separate integration stage; this type
    /// currently provides only the memory-backpressure infrastructure.
    /// </remarks>
    public sealed class MemoryTracker : IResourceTracker<int>
    {
        readonly long capacityBytes;

        long inUseBytes;
        long peakInUseBytes;

        /// <summary>
        /// Configured maximum number of in-flight bytes. Zero means unlimited.
        /// </summary>
        public long CapacityBytes => capacityBytes;

        /// <summary>
        /// Number of bytes currently reserved against this quota.
        /// </summary>
        public long InUseBytes => Interlocked.Read(ref inUseBytes);

        /// <summary>
        /// Highest observed number of bytes reserved against this quota.
        /// </summary>
        public long PeakInUseBytes => Interlocked.Read(ref peakInUseBytes);

        /// <summary>
        /// Creates a memory resource tracker.
        /// </summary>
        /// <param name="capacityBytes">Maximum in-flight bytes. Zero disables throttling.</param>
        public MemoryTracker(long capacityBytes)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(capacityBytes);

            this.capacityBytes = capacityBytes;
        }

        void IResourceTracker<int>.Validate(in int requestResource)
        {
            if (requestResource <= 0)
                throw new ArgumentOutOfRangeException(nameof(requestResource));

            if (capacityBytes != 0 && requestResource > capacityBytes)
            {
                throw new InvalidOperationException(
                    $"Memory reservation of {requestResource} bytes exceeds the configured maximum of {capacityBytes} bytes.");
            }
        }

        bool IResourceTracker<int>.TryReserve(in int requestResource)
        {
            if (capacityBytes == 0)
                return true;

            while (true)
            {
                var current = Interlocked.Read(ref inUseBytes);
                if (current > capacityBytes - requestResource)
                    return false;

                var next = current + requestResource;
                if (Interlocked.CompareExchange(ref inUseBytes, next, current) != current)
                    continue;

                UpdatePeak(next);
                return true;

                void UpdatePeak(long value)
                {
                    var current = Interlocked.Read(ref peakInUseBytes);
                    while (value > current)
                    {
                        var observed = Interlocked.CompareExchange(ref peakInUseBytes, value, current);
                        if (observed == current)
                            return;
                        current = observed;
                    }
                }
            }
        }

        void IResourceTracker<int>.Release(in int requestResource)
        {
            if (capacityBytes == 0)
                return;

            while (true)
            {
                var current = Interlocked.Read(ref inUseBytes);
                if (requestResource > current)
                    throw new InvalidOperationException($"Cannot release {requestResource} bytes when only {current} bytes are reserved.");
                if (Interlocked.CompareExchange(ref inUseBytes, current - requestResource, current) == current)
                    return;
            }
        }
    }
}