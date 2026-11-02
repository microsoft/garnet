// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using Tsavorite.core;

namespace Tsavorite.test
{
    /// <summary>
    /// A device with the alignment contract of an O_DIRECT device whose logical sectors are <see cref="IDevice.SectorSize"/>
    /// bytes: every request's buffer address, offset and length must be a multiple of the sector size. A misaligned
    /// request throws <see cref="TsavoriteException"/> synchronously, as <see cref="NativeStorageDevice"/> does, so the
    /// contract is enforced on every platform and filesystem. Aligned requests are forwarded to the wrapped device.
    /// </summary>
    public sealed class StrictSectorDevice : StorageDeviceBase
    {
        private readonly IDevice underlying;
        private readonly StrictSectorNamedDeviceFactoryCreator owner;

        /// <summary>
        /// Wrap <paramref name="underlying"/>, whose sector size must divide <paramref name="sectorSize"/>.
        /// </summary>
        public StrictSectorDevice(IDevice underlying, uint sectorSize, StrictSectorNamedDeviceFactoryCreator owner = null)
            : base(underlying.FileName, sectorSize, underlying.Capacity)
        {
            if (sectorSize < IDevice.MinDeviceSectorSize || !Utility.IsPowerOfTwo(sectorSize) || sectorSize % underlying.SectorSize != 0)
                throw new ArgumentOutOfRangeException(nameof(sectorSize), sectorSize, $"Must be a power of two that is a multiple of the wrapped device's sector size {underlying.SectorSize}");
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
            owner?.CountRequest();
            underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);
        }

        /// <inheritdoc/>
        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
            DeviceIOCompletionCallback callback, object context)
        {
            ThrowIfMisaligned(destinationAddress, sourceAddress, readLength, nameof(ReadAsync));
            owner?.CountRequest();
            underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);
        }

        private void ThrowIfMisaligned(IntPtr buffer, ulong offset, uint length, string op)
        {
            var mask = (ulong)SectorSize - 1;
            if ((((ulong)buffer.ToInt64() | offset | length) & mask) == 0)
                return;
            owner?.CountMisalignedRequest();
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

    /// <summary>
    /// Device factory for a filesystem whose direct-IO alignment is a given sector size: its devices are
    /// <see cref="StrictSectorDevice"/>s over the wrapped factory's devices, and it reports that sector size from
    /// <see cref="INamedDeviceFactory.GetSectorSize(FileDescriptor)"/> unless told not to, which models a factory that
    /// does not implement it.
    /// </summary>
    public sealed class StrictSectorNamedDeviceFactory(INamedDeviceFactory underlying, uint sectorSize, bool reportSectorSize,
            StrictSectorNamedDeviceFactoryCreator owner) : INamedDeviceFactory
    {
        /// <inheritdoc/>
        public IDevice Get(FileDescriptor fileInfo) => new StrictSectorDevice(underlying.Get(fileInfo), sectorSize, owner);

        /// <inheritdoc/>
        public void Delete(FileDescriptor fileInfo) => underlying.Delete(fileInfo);

        /// <inheritdoc/>
        public IEnumerable<FileDescriptor> ListContents(string path) => underlying.ListContents(path);

        /// <inheritdoc/>
        public uint GetSectorSize(FileDescriptor fileInfo) => reportSectorSize ? sectorSize : IDevice.MinDeviceSectorSize;
    }

    /// <summary>
    /// Creator of <see cref="StrictSectorNamedDeviceFactory"/> instances, counting the requests their devices accept and
    /// reject so a test can show its IO actually went through them.
    /// </summary>
    public sealed class StrictSectorNamedDeviceFactoryCreator : INamedDeviceFactoryCreator
    {
        /// <summary>The sector size of 4Kn drives and of most filesystems that require direct-IO alignment above 512 bytes.</summary>
        public const uint DefaultSectorSize = 4096;

        private readonly INamedDeviceFactoryCreator underlying;
        private readonly bool reportSectorSize;
        private int requestCount, misalignedRequestCount;

        /// <summary>
        /// Create a creator whose factories wrap those of <paramref name="underlying"/> (local storage by default).
        /// </summary>
        public StrictSectorNamedDeviceFactoryCreator(uint sectorSize = DefaultSectorSize, bool reportSectorSize = true, INamedDeviceFactoryCreator underlying = null)
        {
            SectorSize = sectorSize;
            this.reportSectorSize = reportSectorSize;
            this.underlying = underlying ?? new LocalStorageNamedDeviceFactoryCreator();
        }

        /// <summary>Sector size of the created devices.</summary>
        public uint SectorSize { get; }

        /// <summary>Number of aligned requests forwarded by the created devices.</summary>
        public int RequestCount => Volatile.Read(ref requestCount);

        /// <summary>Number of misaligned requests rejected by the created devices.</summary>
        public int MisalignedRequestCount => Volatile.Read(ref misalignedRequestCount);

        /// <inheritdoc/>
        public INamedDeviceFactory Create(string baseName)
            => new StrictSectorNamedDeviceFactory(underlying.Create(baseName), SectorSize, reportSectorSize, this);

        internal void CountRequest() => Interlocked.Increment(ref requestCount);

        internal void CountMisalignedRequest() => Interlocked.Increment(ref misalignedRequestCount);
    }
}