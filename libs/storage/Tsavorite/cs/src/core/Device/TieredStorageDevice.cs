// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// A <see cref="TieredStorageDevice"/> logically composes multiple <see cref="IDevice"/> into a single storage device. It is assumed
    /// that some <see cref="IDevice"/> are used as caches while there is one that is considered the commit point, i.e. when a write is completed
    /// on the device, it is considered persistent. Reads are served from the closest device with available data. Writes are issued in parallel to
    /// all devices 
    /// </summary>
    sealed class TieredStorageDevice : StorageDeviceBase
    {
        private readonly IList<IDevice> devices;
        private readonly int commitPoint;

        /// <summary>
        /// Constructs a new TieredStorageDevice composed of the given devices.
        /// </summary>
        /// <param name="commitPoint">
        /// The index of an <see cref="IDevice">IDevice</see> in <see cref="devices"/>. When a write has been completed on the device,
        /// the write is considered persistent. It is guaranteed that the callback in <see cref="WriteAsync(IntPtr, int, ulong, uint, DeviceIOCompletionCallback, object)"/>
        /// will not be called until the write is completed on the commit point device.
        /// </param>
        /// <param name="devices">
        /// List of devices to be used. The list should be given in order of hot to cold. Read is served from the
        /// device with smallest index in the list that has the requested data
        /// </param>
        /// <remarks>Each request is forwarded unchanged to the tiers, so this device reports the largest of their sector sizes.</remarks>
        public TieredStorageDevice(int commitPoint, IList<IDevice> devices) : base(ComputeFileString(devices, commitPoint), GetLargestSectorSize(devices), ComputeCapacity(devices))
        {
            Debug.Assert(commitPoint >= 0 && commitPoint < devices.Count, "commit point is out of range");

            this.devices = devices;
            this.commitPoint = commitPoint;
        }

        /// <summary>
        /// Constructs a new TieredStorageDevice composed of the given devices.
        /// </summary>
        /// <param name="commitPoint">
        /// The index of an <see cref="IDevice">IDevice</see> in <see cref="devices">devices</see>. When a write has been completed on the device,
        /// the write is considered persistent. It is guaranteed that the callback in <see cref="WriteAsync(IntPtr, int, ulong, uint, DeviceIOCompletionCallback, object)"/>
        /// will not be called until the write is completed on commit point device and all previous tiers.
        /// </param>
        /// <param name="devices">
        /// List of devices to be used. The list should be given in order of hot to cold. Read is served from the
        /// device with smallest index in the list that has the requested data
        /// </param>
        public TieredStorageDevice(int commitPoint, params IDevice[] devices) : this(commitPoint, (IList<IDevice>)devices)
        {
        }

        public override void Initialize(long segmentSize, LightEpoch epoch, bool omitSegmentIdFromFileName = false)
        {
            base.Initialize(segmentSize, epoch, omitSegmentIdFromFileName);

            foreach (IDevice devices in devices)
            {
                devices.Initialize(segmentSize, epoch);
            }
        }

        public override void Dispose()
        {
            foreach (IDevice device in devices)
            {
                device.Dispose();
            }
        }

        /// <inheritdoc/>
        public override void Reset()
        {
            base.Reset();
            foreach (IDevice device in devices)
            {
                device.Reset();
            }
        }

        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, DeviceIOCompletionCallback callback, object context)
        {
            // This device is epoch-protected and cannot be stale while the operation is in flight
            IDevice closestDevice = devices[FindClosestDeviceContaining(segmentId)];
            // We can directly forward the address, because assuming an inclusive policy, all devices agree on the same address space. The only difference is that some segments may not
            // be present for certain devices. 
            closestDevice.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);
        }

        public override unsafe void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            // A write must reach the fastest tier, as the rest of this method assumes: it waits for every tier through the commit
            // point. So a write to a segment the fastest tier has evicted fails at once. Writing it elsewhere would lose data after a
            // restart, since a tier knows what it holds only in memory or from its files, and the fastest tier can then claim the
            // segment again: data written only to the slower tiers would be hidden, and data also written to the fastest tier would
            // be only part of the segment, the rest reading as zeros. Supporting such writes needs the segment copied to the fastest
            // tier first, or a lasting record of what each tier holds. A log writes near its tail, which the fastest tier holds, and
            // writes below it only after recovering to a commit in a segment the fastest tier evicted.
            if (FindClosestDeviceContaining(segmentId) != 0)
            {
                callback(uint.MaxValue, 0, context, ioException: new TsavoriteIOException($"Cannot write segment {segmentId}: the fastest tier has evicted it", null));
                return;
            }

            var countdown = new CountdownEvent(commitPoint + 1);  // number of devices to wait on
            // Issue writes to all tiers in parallel
            for (int i = 0; i < devices.Count; i++)
            {
                if (i <= commitPoint)
                {

                    // All tiers before the commit point (inclusive) need to be persistent before the callback is invoked.
                    devices[i].WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, (e, n, o, ex) =>
                    {
                        // The last tier to finish invokes the callback
                        if (countdown.Signal())
                        {
                            callback(e, n, o, ioException: ex);
                            countdown.Dispose();
                        }

                    }, context);
                }
                else
                {
                    // Otherwise, simply issue the write without caring about callbacks
                    devices[i].WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, (e, n, o, ex) => { }, null);
                }
            }
        }

        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            int startTier = FindClosestDeviceContaining(segment);

            // A tier before startTier has already evicted the segment, so only the tiers from startTier on are called
            var countdown = new CountdownEvent(devices.Count - startTier);
            for (int i = startTier; i < devices.Count; i++)
            {
                devices[i].RemoveSegmentAsync(segment, r =>
                {
                    if (countdown.Signal())
                    {
                        callback(r);
                        countdown.Dispose();
                    }
                }, result);
            }
        }

        /// <inheritdoc/>
        /// <remarks>
        /// <para>
        /// Every tier removes the segments it holds above <paramref name="segment"/>, whatever its <see cref="IDevice.StartSegment"/>
        /// says: a slower tier can hold segments that a faster tier has evicted, and after <see cref="Reset"/>, which returns each
        /// tier's start segment to 0, a faster tier still holds the old segments it had not evicted.
        /// </para>
        /// <para>
        /// This throws <see cref="TsavoriteException"/>, before removing anything, if the fastest tier has evicted
        /// <paramref name="segment"/>.
        /// </para>
        /// </remarks>
        public override bool TryRemoveSegmentsAbove(int segment, out int removed)
        {
            // If the fastest tier has evicted segment, its segments are all above it, so removing them would empty it, and after a
            // restart an empty fastest tier claims every segment, hiding the slower tiers' data. The log cannot be written at segment
            // anyway (see WriteAsync), so this fails before removing anything.
            if (FindClosestDeviceContaining(segment) != 0)
                throw new TsavoriteException($"Cannot remove the segments above segment {segment}: the fastest tier has evicted it, so the log cannot be written there");
            return TryRemoveSegmentsAboveOnEach(devices, segment, out removed);
        }

        private static long ComputeCapacity(IList<IDevice> devices)
        {
            long result = 0;
            // The capacity of a tiered storage device is the sum of the capacity of its tiers
            foreach (IDevice device in devices)
            {
                // Unless the last tier device has unspecified storage capacity, in which case the tiered storage also has unspecified capacity
                if (device.Capacity == Devices.CAPACITY_UNSPECIFIED)
                {
                    Debug.Assert(device == devices[devices.Count - 1], "Only the last tier storage of a tiered storage device can have unspecified capacity");
                    return Devices.CAPACITY_UNSPECIFIED;
                }
                result = Math.Max(result, device.Capacity);
            }
            return result;
        }

        private static string ComputeFileString(IList<IDevice> devices, int commitPoint)
        {
            StringBuilder result = new();
            foreach (IDevice device in devices)
            {
                string formatString = "{0}, file name {1}, capacity {2} bytes;";
                string capacity = device.Capacity == Devices.CAPACITY_UNSPECIFIED ? "unspecified" : device.Capacity.ToString();
                result.AppendFormat(formatString, device.GetType().Name, device.FileName, capacity);
            }
            result.AppendFormat("commit point: {0} at tier {1}", devices[commitPoint].GetType().Name, commitPoint);
            return result.ToString();
        }

        private int FindClosestDeviceContaining(int segment)
        {
            // Can use binary search, but 1) it might not be faster than linear on a array assumed small, and 2) C# built in does not guarantee first element is returned on duplicates.
            // Therefore we are sticking to the simpler approach at first.
            for (int i = 0; i < devices.Count; i++)
            {
                if (devices[i].StartSegment <= segment)
                    return i;
            }
            throw new ArgumentException("No such address exists");
        }
    }
}