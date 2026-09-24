// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;

namespace Garnet.server
{
    /// <summary>
    /// Coordinates AOF truncation with consumers that require retained log addresses.
    /// </summary>
    internal sealed class AofRetentionManager : IDisposable
    {
        readonly object sync = new();
        readonly List<AofRetentionLease> leases = [];
        AofAddress truncatedUntil;
        bool disposed;

        /// <summary>
        /// Gets the highest address logically truncated by this manager.
        /// </summary>
        public AofAddress TruncatedUntil
        {
            get
            {
                lock (sync)
                    return truncatedUntil;
            }
        }

        /// <summary>
        /// Creates an AOF retention manager.
        /// </summary>
        public AofRetentionManager(int physicalSublogCount)
        {
            truncatedUntil = AofAddress.Create(physicalSublogCount, 0);
        }

        /// <summary>
        /// Attempts to retain AOF from the current addresses supplied by <paramref name="getCurrentAddress"/>.
        /// Registration is serialized with truncation boundary updates.
        /// </summary>
        public bool TryAcquire(
            in AofAddress startAddress,
            Func<int, long> getCurrentAddress,
            bool allowDataLoss,
            out AofRetentionLease lease)
        {
            ArgumentNullException.ThrowIfNull(getCurrentAddress);
            lease = null;

            lock (sync)
            {
                if (disposed || startAddress.Length != truncatedUntil.Length)
                    return false;
                if (startAddress.AnyLesser(truncatedUntil) && !allowDataLoss)
                    return false;

                lease = new AofRetentionLease(this, getCurrentAddress);
                leases.Add(lease);
                return true;
            }
        }

        /// <summary>
        /// Attempts to retain AOF from a fixed address.
        /// </summary>
        public bool TryAcquire(in AofAddress startAddress, bool allowDataLoss, out AofRetentionLease lease)
        {
            var retainedAddress = startAddress;
            return TryAcquire(startAddress, physicalSublogIdx => retainedAddress[physicalSublogIdx], allowDataLoss, out lease);
        }

        /// <summary>
        /// Computes and records a safe truncation boundary for all sublogs.
        /// </summary>
        public AofAddress GetTruncationLimit(in AofAddress requestedAddress)
        {
            lock (sync)
            {
                var result = requestedAddress;
                ApplyRetentionLimits(ref result);
                truncatedUntil.MonotonicUpdate(ref result);
                return result;
            }
        }

        /// <summary>
        /// Computes and records a safe truncation boundary for one sublog.
        /// </summary>
        public long GetTruncationLimit(long requestedAddress, int physicalSublogIdx, long upperBound)
        {
            lock (sync)
            {
                var result = Math.Min(requestedAddress, upperBound);
                foreach (var lease in leases)
                    result = Math.Min(result, lease.GetCurrentAddress(physicalSublogIdx));
                truncatedUntil.MonotonicUpdate(result, physicalSublogIdx);
                return result;
            }
        }

        /// <summary>
        /// Advances the known logical truncation boundary.
        /// </summary>
        public void UpdateTruncatedUntil(in AofAddress address)
        {
            lock (sync)
            {
                var value = address;
                truncatedUntil.MonotonicUpdate(ref value);
            }
        }

        void ApplyRetentionLimits(ref AofAddress address)
        {
            foreach (var lease in leases)
            {
                for (var physicalSublogIdx = 0; physicalSublogIdx < address.Length; physicalSublogIdx++)
                    address[physicalSublogIdx] = Math.Min(address[physicalSublogIdx], lease.GetCurrentAddress(physicalSublogIdx));
            }
        }

        internal void Release(AofRetentionLease lease)
        {
            lock (sync)
                _ = leases.Remove(lease);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            lock (sync)
            {
                if (disposed)
                    return;
                disposed = true;
                foreach (var lease in leases)
                    lease.Detach();
                leases.Clear();
            }
        }
    }

    /// <summary>
    /// Pins the AOF truncation boundary until disposed.
    /// </summary>
    internal sealed class AofRetentionLease : IDisposable
    {
        AofRetentionManager manager;
        readonly Func<int, long> getCurrentAddress;

        internal AofRetentionLease(AofRetentionManager manager, Func<int, long> getCurrentAddress)
        {
            this.manager = manager;
            this.getCurrentAddress = getCurrentAddress;
        }

        internal long GetCurrentAddress(int physicalSublogIdx)
            => getCurrentAddress(physicalSublogIdx);

        internal void Detach()
            => Interlocked.Exchange(ref manager, null);

        /// <inheritdoc />
        public void Dispose()
            => Interlocked.Exchange(ref manager, null)?.Release(this);
    }
}