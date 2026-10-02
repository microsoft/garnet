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
    /// A zero-byte capacity disables accounting and throttling. Applications compose this throttle with a
    /// <c>WaiterQueue&lt;int&gt;</c> to provide admission retries and waiting.
    /// GarnetLightClient integration is intentionally reserved for a separate integration stage; this type
    /// currently provides only the memory-backpressure infrastructure.
    /// </remarks>
    public readonly struct MemoryThrottle : IResourceThrottle<int>
    {
        sealed class ThrottleState(long capacityBytes)
        {
            internal readonly long capacityBytes = capacityBytes;
            internal long inUseBytes;
            internal long peakInUseBytes;
        }

        readonly ThrottleState state;

        /// <summary>
        /// Configured maximum number of in-flight bytes. Zero means unlimited.
        /// </summary>
        public long CapacityBytes => state?.capacityBytes ?? 0;

        /// <summary>
        /// Number of bytes currently reserved against this quota.
        /// </summary>
        public long InUseBytes => state == null ? 0 : Interlocked.Read(ref state.inUseBytes);

        /// <summary>
        /// Highest observed number of bytes reserved against this quota.
        /// </summary>
        public long PeakInUseBytes => state == null ? 0 : Interlocked.Read(ref state.peakInUseBytes);

        /// <summary>
        /// Creates a memory resource throttle.
        /// </summary>
        /// <param name="capacityBytes">Maximum in-flight bytes. Zero disables throttling.</param>
        public MemoryThrottle(long capacityBytes)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(capacityBytes);

            state = capacityBytes == 0 ? null : new ThrottleState(capacityBytes);
        }

        void IResourceThrottle<int>.Validate(in int requestResource)
            => ValidateRequest(requestResource);

        void ValidateRequest(int requestResource)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(requestResource);

            var capacityBytes = CapacityBytes;
            if (capacityBytes != 0 && requestResource > capacityBytes)
            {
                throw new InvalidOperationException(
                    $"Memory reservation of {requestResource} bytes exceeds the configured maximum of {capacityBytes} bytes.");
            }
        }

        bool IResourceThrottle<int>.TryReserve(in int requestResource)
        {
            ValidateRequest(requestResource);

            var state = this.state;
            if (state == null)
                return true;

            while (true)
            {
                var current = Interlocked.Read(ref state.inUseBytes);
                if (current > state.capacityBytes - requestResource)
                    return false;

                var next = current + requestResource;
                if (Interlocked.CompareExchange(ref state.inUseBytes, next, current) != current)
                    continue;

                UpdatePeak(state, next);
                return true;

                static void UpdatePeak(ThrottleState state, long value)
                {
                    var current = Interlocked.Read(ref state.peakInUseBytes);
                    while (value > current)
                    {
                        var observed = Interlocked.CompareExchange(ref state.peakInUseBytes, value, current);
                        if (observed == current)
                            return;
                        current = observed;
                    }
                }
            }
        }

        void IResourceThrottle<int>.Release(in int requestResource)
        {
            var state = this.state;
            if (state == null)
                return;

            while (true)
            {
                var current = Interlocked.Read(ref state.inUseBytes);
                if (requestResource > current)
                    throw new InvalidOperationException($"Cannot release {requestResource} bytes when only {current} bytes are reserved.");
                if (Interlocked.CompareExchange(ref state.inUseBytes, current - requestResource, current) == current)
                    return;
            }
        }
    }
}