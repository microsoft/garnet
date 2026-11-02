// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Device factory creator that places the store's checkpoints on a simulated filesystem whose direct-IO alignment is
    /// <see cref="SectorSize"/> (4 KiB by default, as on 4Kn drives and filesystems such as ZFS): checkpoint devices
    /// reject any request whose buffer address, offset or length is not a multiple of it, and their factory reports it
    /// from <see cref="INamedDeviceFactory.GetSectorSize(FileDescriptor)"/>. Every other device - hybrid log, storage tier
    /// and AOF - is created normally, so the checkpoint devices need more alignment than the store's log devices.
    /// </summary>
    /// <remarks>
    /// Injected by assigning it to <c>GarnetServerOptions.DeviceFactoryCreator</c>, which <c>GarnetServer.CreateStore</c>
    /// hands to the store's checkpoint manager. Enforcing the alignment in the device rather than relying on the host's
    /// disks makes the requirement hold on every platform and filesystem.
    /// </remarks>
    internal sealed class StrictSectorCheckpointDeviceFactoryCreator : INamedDeviceFactoryCreator
    {
        /// <summary>The sector size of 4Kn drives and of most filesystems that require direct-IO alignment above 512 bytes.</summary>
        internal const uint DefaultSectorSize = 4096;

        readonly INamedDeviceFactoryCreator inner = new LocalStorageNamedDeviceFactoryCreator();
        readonly string storeCheckpointBaseDirectory;
        int requestCount, misalignedRequestCount;

        /// <summary>
        /// Creates a creator whose devices under <paramref name="storeCheckpointBaseDirectory"/> have
        /// <paramref name="sectorSize"/>-byte sectors.
        /// </summary>
        /// <param name="storeCheckpointBaseDirectory">Store checkpoint base directory, as given by
        /// <c>GarnetServerOptions.StoreCheckpointBaseDirectory</c>. Every database's checkpoint directory lives beneath
        /// it.</param>
        /// <param name="sectorSize">Sector size of the checkpoint devices.</param>
        internal StrictSectorCheckpointDeviceFactoryCreator(string storeCheckpointBaseDirectory, uint sectorSize = DefaultSectorSize)
        {
            this.storeCheckpointBaseDirectory = storeCheckpointBaseDirectory;
            SectorSize = sectorSize;
        }

        /// <summary>Sector size of the checkpoint devices.</summary>
        internal uint SectorSize { get; }

        /// <summary>Number of aligned requests the checkpoint devices forwarded.</summary>
        internal int RequestCount => Volatile.Read(ref requestCount);

        /// <summary>Number of misaligned requests the checkpoint devices rejected.</summary>
        internal int MisalignedRequestCount => Volatile.Read(ref misalignedRequestCount);

        /// <inheritdoc/>
        public INamedDeviceFactory Create(string baseName)
        {
            var factory = inner.Create(baseName);
            return baseName is not null && baseName.StartsWith(storeCheckpointBaseDirectory, StringComparison.Ordinal)
                ? new StrictSectorNamedDeviceFactory(factory, this)
                : factory;
        }

        /// <summary>
        /// Delegating device factory whose devices enforce the owner's sector size, and which reports it.
        /// </summary>
        private sealed class StrictSectorNamedDeviceFactory(INamedDeviceFactory underlying, StrictSectorCheckpointDeviceFactoryCreator owner)
            : INamedDeviceFactory
        {
            /// <inheritdoc/>
            public IDevice Get(FileDescriptor fileInfo) => new StrictSectorDevice(underlying.Get(fileInfo), owner);

            /// <inheritdoc/>
            public void Delete(FileDescriptor fileInfo) => underlying.Delete(fileInfo);

            /// <inheritdoc/>
            public IEnumerable<FileDescriptor> ListContents(string path) => underlying.ListContents(path);

            /// <inheritdoc/>
            public uint GetSectorSize(FileDescriptor fileInfo) => owner.SectorSize;
        }

        /// <summary>
        /// Delegating device that throws <see cref="TsavoriteException"/> synchronously for a misaligned request, as the
        /// native O_DIRECT device does, and forwards aligned requests.
        /// </summary>
        private sealed class StrictSectorDevice : StorageDeviceBase
        {
            readonly IDevice underlying;
            readonly StrictSectorCheckpointDeviceFactoryCreator owner;

            internal StrictSectorDevice(IDevice underlying, StrictSectorCheckpointDeviceFactoryCreator owner)
                : base(underlying.FileName, owner.SectorSize, underlying.Capacity)
            {
                if (owner.SectorSize % underlying.SectorSize != 0)
                    throw new ArgumentException($"Sector size {owner.SectorSize} is not a multiple of the wrapped device's sector size {underlying.SectorSize}");
                this.underlying = underlying;
                this.owner = owner;
            }

            /// <inheritdoc/>
            public override void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
            {
                base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
                underlying.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
            }

            /// <inheritdoc/>
            public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
                => underlying.RemoveSegmentAsync(segment, callback, result);

            /// <inheritdoc/>
            public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
                DeviceIOCompletionCallback callback, object context)
            {
                ThrowIfMisaligned(sourceAddress, destinationAddress, numBytesToWrite, nameof(WriteAsync));
                _ = Interlocked.Increment(ref owner.requestCount);
                underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);
            }

            /// <inheritdoc/>
            public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
                DeviceIOCompletionCallback callback, object context)
            {
                ThrowIfMisaligned(destinationAddress, sourceAddress, readLength, nameof(ReadAsync));
                _ = Interlocked.Increment(ref owner.requestCount);
                underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);
            }

            void ThrowIfMisaligned(IntPtr buffer, ulong offset, uint length, string op)
            {
                var mask = (ulong)SectorSize - 1;
                if ((((ulong)buffer.ToInt64() | offset | length) & mask) == 0)
                    return;
                _ = Interlocked.Increment(ref owner.misalignedRequestCount);
                throw new TsavoriteException($"{nameof(StrictSectorDevice)}.{op}: misaligned I/O for sector size {SectorSize}: "
                    + $"offset=0x{offset:X}, length={length}, buffer=0x{buffer.ToInt64():X}");
            }

            /// <inheritdoc/>
            public override bool TryComplete() => underlying.TryComplete();

            /// <inheritdoc/>
            public override long GetFileSize(int segment) => underlying.GetFileSize(segment);

            /// <inheritdoc/>
            public override void Dispose() => underlying.Dispose();
        }
    }
}