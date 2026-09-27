// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Device factory creator that holds back the completion callbacks of writes to a chosen checkpoint file, so a
    /// test can arrange for a checkpoint to abort while the writes it issued are still in flight.
    /// </summary>
    /// <remarks>
    /// The underlying write is issued normally; only the notification back to Tsavorite is withheld. That is exactly
    /// the state a slow device leaves a checkpoint in, and it is the state that matters here: the flush counters,
    /// error slot and completion source the callback reports through are shared across checkpoints, so a completion
    /// that has not been waited for is one that can land on the next checkpoint.
    /// </remarks>
    internal sealed class DeferringCheckpointDeviceFactoryCreator : INamedDeviceFactoryCreator
    {
        readonly INamedDeviceFactoryCreator inner = new LocalStorageNamedDeviceFactoryCreator();
        readonly string storeCheckpointBaseDirectory;
        readonly string deferredFileName;

        readonly object gate = new();
        readonly List<Action> deferred = [];
        int outstanding;

        /// <summary>Number of write completions that have been withheld and not yet released.</summary>
        internal int Outstanding => Volatile.Read(ref outstanding);

        /// <summary>Set once at least one write has been withheld, so a test can tell the injection took effect.</summary>
        internal bool AnyDeferred { get; private set; }

        /// <summary>
        /// Creates a creator that defers write completions for <paramref name="deferredFileName"/> under the given
        /// store checkpoint directory.
        /// </summary>
        /// <param name="storeCheckpointBaseDirectory">Store checkpoint base directory, as given by
        /// <c>GarnetServerOptions.StoreCheckpointBaseDirectory</c>.</param>
        /// <param name="deferredFileName">Checkpoint file whose write completions are withheld, for example
        /// <c>ht.dat</c> or <c>snapshot.dat</c>.</param>
        internal DeferringCheckpointDeviceFactoryCreator(string storeCheckpointBaseDirectory, string deferredFileName)
        {
            this.storeCheckpointBaseDirectory = storeCheckpointBaseDirectory;
            this.deferredFileName = deferredFileName;
        }

        /// <summary>Whether new writes are withheld. Cleared writes complete normally.</summary>
        internal volatile bool Deferring;

        /// <summary>Releases every withheld completion and stops withholding new ones.</summary>
        internal void ReleaseAll()
        {
            Deferring = false;

            Action[] toRun;
            lock (gate)
            {
                toRun = [.. deferred];
                deferred.Clear();
            }

            foreach (var complete in toRun)
                complete();
        }

        /// <inheritdoc/>
        public INamedDeviceFactory Create(string baseName)
        {
            var factory = inner.Create(baseName);
            return baseName is not null && baseName.StartsWith(storeCheckpointBaseDirectory, StringComparison.Ordinal)
                ? new DeferringNamedDeviceFactory(factory, this)
                : factory;
        }

        bool TryDefer(Action complete)
        {
            lock (gate)
            {
                if (!Deferring)
                    return false;

                deferred.Add(complete);
                AnyDeferred = true;
                _ = Interlocked.Increment(ref outstanding);
                return true;
            }
        }

        void Completed() => Interlocked.Decrement(ref outstanding);

        private sealed class DeferringNamedDeviceFactory(INamedDeviceFactory underlying, DeferringCheckpointDeviceFactoryCreator owner)
            : INamedDeviceFactory
        {
            /// <inheritdoc/>
            public IDevice Get(FileDescriptor fileInfo)
            {
                var device = underlying.Get(fileInfo);
                return string.Equals(fileInfo.fileName, owner.deferredFileName, StringComparison.Ordinal)
                    ? new DeferringDevice(device, owner)
                    : device;
            }

            /// <inheritdoc/>
            public void Delete(FileDescriptor fileInfo) => underlying.Delete(fileInfo);

            /// <inheritdoc/>
            public IEnumerable<FileDescriptor> ListContents(string path) => underlying.ListContents(path);
        }

        /// <summary>
        /// Delegating device that withholds write completion callbacks while its owner is deferring.
        /// </summary>
        private sealed class DeferringDevice(IDevice underlying, DeferringCheckpointDeviceFactoryCreator owner) : IDevice
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
                => underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, Wrap(callback), context);

            /// <inheritdoc/>
            public void WriteAsync(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite,
                DeviceIOCompletionCallback callback, object context)
                => underlying.WriteAsync(alignedSourceAddress, alignedDestinationAddress, numBytesToWrite, Wrap(callback), context);

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

            DeviceIOCompletionCallback Wrap(DeviceIOCompletionCallback callback)
                => (errorCode, numBytes, context, exception) =>
                {
                    if (!owner.TryDefer(() =>
                        {
                            try
                            {
                                callback(errorCode, numBytes, context, exception);
                            }
                            finally
                            {
                                owner.Completed();
                            }
                        }))
                    {
                        callback(errorCode, numBytes, context, exception);
                    }
                };
        }
    }
}