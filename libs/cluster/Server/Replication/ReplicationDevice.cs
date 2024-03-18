// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.client;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal class ReplicationDevice : IDevice
    {
        public uint SectorSize => localDevice.SectorSize;

        public string FileName => localDevice.FileName;

        public long Capacity => localDevice.Capacity;

        public long SegmentSize => localDevice.SegmentSize;

        public int StartSegment => localDevice.StartSegment;

        public int EndSegment => localDevice.EndSegment;

        public int ThrottleLimit { get => localDevice.ThrottleLimit; set => localDevice.ThrottleLimit = value; }

        private readonly IDevice localDevice;

        public GarnetClient GarnetClient = null;

        public long ReplicationStartAddress = 0;

        public ReplicationDevice(IDevice localDevice)
        {
            this.localDevice = localDevice;
        }

        public void Reset()
        {
            localDevice.Reset();
        }

        public void Dispose()
        {
            localDevice.Dispose();
        }

        public long GetFileSize(int segment)
            => localDevice.GetFileSize(segment);

        public void Initialize(long segmentSize, Tsavorite.core.LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
            => localDevice.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);

        public void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, DeviceIOCompletionCallback callback, object context)
            => localDevice.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);

        public void ReadAsync(ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint aligned_read_length, DeviceIOCompletionCallback callback, object context)
            => localDevice.ReadAsync(alignedSourceAddress, alignedDestinationAddress, aligned_read_length, callback, context);

        public void RemoveSegment(int segment)
            => localDevice.RemoveSegment(segment);

        public void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
            => localDevice.RemoveSegmentAsync(segment, callback, result);

        public bool Throttle()
            => localDevice.Throttle();

        public void TruncateUntilAddress(long toAddress)
            => localDevice.TruncateUntilAddress(toAddress);

        public void TruncateUntilAddressAsync(long toAddress, AsyncCallback callback, IAsyncResult result)
            => localDevice.TruncateUntilAddressAsync(toAddress, callback, result);

        public void TruncateUntilSegment(int toSegment)
            => localDevice.TruncateUntilSegment(toSegment);

        public void TruncateUntilSegmentAsync(int toSegment, AsyncCallback callback, IAsyncResult result)
            => localDevice.TruncateUntilSegmentAsync(toSegment, callback, result);

        public bool TryComplete()
            => localDevice.TryComplete();

        public void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
            => localDevice.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);

        public void WriteAsync(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            var _client = GarnetClient;
            if (_client != null)
            {

            }
            localDevice.WriteAsync(alignedSourceAddress, alignedDestinationAddress, numBytesToWrite, callback, context);
        }

        /*
        long ClosedUntilAddress;
        long OngoingCloseUntilAddress;

        void OnPagesClosed(GarnetClient client, long newSafeHeadAddress)
        {
            // This thread is responsible for [oldSafeHeadAddress -> newSafeHeadAddress]
            for (; ; Thread.Yield())
            {
                long _ongoingCloseUntilAddress = OngoingCloseUntilAddress;

                // If we are closing in the middle of an ongoing OPCWorker loop, exit.
                if (_ongoingCloseUntilAddress >= newSafeHeadAddress)
                    break;

                // We'll continue the loop if we fail the CAS here; that means another thread extended the Ongoing range.
                if (Interlocked.CompareExchange(ref OngoingCloseUntilAddress, newSafeHeadAddress, _ongoingCloseUntilAddress) == _ongoingCloseUntilAddress)
                {
                    if (_ongoingCloseUntilAddress == 0)
                    {
                        // There was no other thread running the OPCWorker loop, so this thread is responsible for closing [ClosedUntilAddress -> newSafeHeadAddress]
                        Task.Run(async () => await OnPagesClosedWorker(client));
                    }
                    else
                    {
                        // There was another thread runnning the OPCWorker loop, and its ongoing close operation was successfully extended to include the new safe
                        // head address; we have no further work here.
                    }
                    return;
                }
            }

        }

        async Task OnPagesClosedWorker(GarnetClient garnetClient)
        {
            for (; ; Thread.Yield())
            {
                long closeEndAddress = OngoingCloseUntilAddress;

                // TODO: send to remote

                ClosedUntilAddress = closeEndAddress;

                // End if we have exhausted co-operative work
                if (Interlocked.CompareExchange(ref OngoingCloseUntilAddress, 0, closeEndAddress) == closeEndAddress)
                    break;
            }
        }
        */
    }
}