// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using Tsavorite.core;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Device factory creator that can withhold the write completions of named files, such as a node's cluster
    /// configuration files, so that a write of one does not finish until the test completes it.
    /// </summary>
    /// <remarks>
    /// Injected by wrapping the creator a node's <c>GarnetServerOptions.DeviceFactoryCreator</c> already holds. Every
    /// other file passes through untouched. The data is written; only its completion is withheld.
    /// </remarks>
    internal sealed class HeldWriteDeviceFactoryCreator(INamedDeviceFactoryCreator inner, params string[] fileNames) : INamedDeviceFactoryCreator
    {
        readonly string[] fileNames = fileNames;
        readonly object gate = new();
        readonly List<HeldWrite> heldWrites = [];
        int totalWritesHeld;

        /// <summary>A write's completion, withheld while <see cref="HoldWrites"/> is set.</summary>
        readonly record struct HeldWrite(DeviceIOCompletionCallback Callback, uint ErrorCode, uint NumBytes, object Context, Exception Exception);

        /// <summary>
        /// Set to withhold the completion of every write of the files issued from then on, until <see cref="ReleaseWrites"/>.
        /// </summary>
        internal volatile bool HoldWrites;

        /// <summary>Number of write completions withheld and not yet released.</summary>
        internal int WritesHeld
        {
            get
            {
                lock (gate)
                    return heldWrites.Count;
            }
        }

        /// <summary>Number of write completions withheld since the creator was made, released or not.</summary>
        internal int TotalWritesHeld => Volatile.Read(ref totalWritesHeld);

        /// <summary>Completes every write withheld so far, leaving <see cref="HoldWrites"/> as it is.</summary>
        internal void CompleteHeldWrites() => Complete(holdOn: HoldWrites);

        /// <summary>Clears <see cref="HoldWrites"/> and completes every write it withheld. Idempotent.</summary>
        internal void ReleaseWrites() => Complete(holdOn: false);

        void Complete(bool holdOn)
        {
            HeldWrite[] released;
            lock (gate)
            {
                HoldWrites = holdOn;
                released = [.. heldWrites];
                heldWrites.Clear();
            }

            foreach (var write in released)
                write.Callback(write.ErrorCode, write.NumBytes, write.Context, write.Exception);
        }

        /// <inheritdoc/>
        public INamedDeviceFactory Create(string baseName) => new Factory(inner.Create(baseName), this);

        /// <summary>Wraps <paramref name="callback"/> to be withheld if <see cref="HoldWrites"/> is set when it is issued.</summary>
        DeviceIOCompletionCallback WrapWrite(DeviceIOCompletionCallback callback)
            => !HoldWrites ? callback : (errorCode, numBytes, context, exception) =>
            {
                lock (gate)
                {
                    if (HoldWrites)
                    {
                        heldWrites.Add(new(callback, errorCode, numBytes, context, exception));
                        totalWritesHeld++;
                        return;
                    }
                }

                callback(errorCode, numBytes, context, exception);
            };

        /// <summary>Delegating device factory that wraps the named files' devices and passes everything else through.</summary>
        sealed class Factory(INamedDeviceFactory underlying, HeldWriteDeviceFactoryCreator owner) : INamedDeviceFactory
        {
            /// <inheritdoc/>
            public IDevice Get(FileDescriptor fileInfo)
            {
                var device = underlying.Get(fileInfo);
                return Array.IndexOf(owner.fileNames, fileInfo.fileName) >= 0 ? new Device(device, owner) : device;
            }

            /// <inheritdoc/>
            public void Delete(FileDescriptor fileInfo) => underlying.Delete(fileInfo);

            /// <inheritdoc/>
            public IEnumerable<FileDescriptor> ListContents(string path) => underlying.ListContents(path);
        }

        /// <summary>Delegating device that withholds write completions while its owner holds writes.</summary>
        sealed class Device(IDevice underlying, HeldWriteDeviceFactoryCreator owner) : IDevice
        {
            /// <inheritdoc/>
            public uint SectorSize => underlying.SectorSize;

            /// <inheritdoc/>
            public string FileName => underlying.FileName;

            /// <inheritdoc/>
            public long Capacity => underlying.Capacity;

            /// <inheritdoc/>
            public long SegmentSize => underlying.SegmentSize;

            /// <inheritdoc/>
            public int StartSegment => underlying.StartSegment;

            /// <inheritdoc/>
            public int EndSegment => underlying.EndSegment;

            /// <inheritdoc/>
            public int ThrottleLimit { get => underlying.ThrottleLimit; set => underlying.ThrottleLimit = value; }

            /// <inheritdoc/>
            public void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
                => underlying.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);

            /// <inheritdoc/>
            public bool TryComplete() => underlying.TryComplete();

            /// <inheritdoc/>
            public bool Throttle() => underlying.Throttle();

            /// <inheritdoc/>
            public void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
                DeviceIOCompletionCallback callback, object context)
                => underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, owner.WrapWrite(callback), context);

            /// <inheritdoc/>
            public void WriteAsync(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite,
                DeviceIOCompletionCallback callback, object context)
                => underlying.WriteAsync(alignedSourceAddress, alignedDestinationAddress, numBytesToWrite, owner.WrapWrite(callback), context);

            /// <inheritdoc/>
            public void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
                DeviceIOCompletionCallback callback, object context)
                => underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);

            /// <inheritdoc/>
            public void ReadAsync(ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint alignedReadLength,
                DeviceIOCompletionCallback callback, object context)
                => underlying.ReadAsync(alignedSourceAddress, alignedDestinationAddress, alignedReadLength, callback, context);

            /// <inheritdoc/>
            public void TruncateUntilAddressAsync(long toAddress, AsyncCallback callback, IAsyncResult result)
                => underlying.TruncateUntilAddressAsync(toAddress, callback, result);

            /// <inheritdoc/>
            public void TruncateUntilAddress(long toAddress) => underlying.TruncateUntilAddress(toAddress);

            /// <inheritdoc/>
            public void TruncateUntilSegmentAsync(int toSegment, AsyncCallback callback, IAsyncResult result)
                => underlying.TruncateUntilSegmentAsync(toSegment, callback, result);

            /// <inheritdoc/>
            public void TruncateUntilSegment(int toSegment) => underlying.TruncateUntilSegment(toSegment);

            /// <inheritdoc/>
            public void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
                => underlying.RemoveSegmentAsync(segment, callback, result);

            /// <inheritdoc/>
            public void RemoveSegment(int segment) => underlying.RemoveSegment(segment);

            /// <inheritdoc/>
            public long GetFileSize(int segment) => underlying.GetFileSize(segment);

            /// <inheritdoc/>
            public void Reset() => underlying.Reset();

            /// <inheritdoc/>
            public void Dispose() => underlying.Dispose();
        }
    }
}