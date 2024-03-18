// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using Microsoft.Extensions.Logging;
using Tsavorite.core;
using Tsavorite.devices;

namespace Garnet
{
    internal enum FileLocationType
    {
        Local,
        AzureStorage,
        EmbeddedResource
    }

    /// <summary>
    /// Interface for reading / writing into local / remote files
    /// </summary>
    internal interface IStreamProvider
    {
        /// <summary>
        /// Read data from file specified in path
        /// </summary>
        /// <param name="path">Path to file</param>
        /// <returns>Stream object</returns>
        Stream Read(string path);

        /// <summary>
        /// Write data into file specified in path
        /// </summary>
        /// <param name="path">Path to file</param>
        /// <param name="data">Data to write</param>
        void Write(string path, byte[] data);
    }

    /// <summary>
    /// Base StreamProvider class containing common logic between stream providers 
    /// </summary>
    internal abstract class StreamProviderBase : IStreamProvider
    {
        protected const int MaxConfigFileSizeAligned = 262144;

        public Stream Read(string path)
        {
            using var device = GetDevice(path);
            var pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);
            ReadInto(device, pool, 0, out var buffer, MaxConfigFileSizeAligned);
            pool.Free();

            // Remove trailing zeros
            int lastIndex = Array.FindLastIndex(buffer, b => b != 0);
            var stream = new MemoryStream(buffer, 0, lastIndex + 1);
            return stream;
        }

        public unsafe void Write(string path, byte[] data)
        {
            using var device = GetDevice(path);
            var bytesToWrite = GetBytesToWrite(data, device);
            var pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);

            // Get a sector-aligned buffer from the pool and copy _buffer into it.
            var buffer = pool.Get((int)bytesToWrite);
            fixed (byte* bufferRaw = data)
            {
                Buffer.MemoryCopy(bufferRaw, buffer.aligned_pointer, data.Length, data.Length);
            }

            // Write to the device and wait for the device to signal the semaphore that the write is complete.
            using var semaphore = new SemaphoreSlim(0);
            device.WriteAsync((IntPtr)buffer.aligned_pointer, 0, (uint)bytesToWrite, IOCallback, semaphore);
            semaphore.Wait();

            // Free the sector-aligned buffer
            buffer.Return();
            pool.Free();
        }

        protected abstract IDevice GetDevice(string path);

        protected abstract long GetBytesToWrite(byte[] bytes, IDevice device);

        protected static unsafe void ReadInto(IDevice device, SectorAlignedBufferPool pool, ulong address, out byte[] buffer, int size, ILogger logger = null)
        {
            using var semaphore = new SemaphoreSlim(0);
            long numBytesToRead = size;
            numBytesToRead = ((numBytesToRead + (device.SectorSize - 1)) & ~(device.SectorSize - 1));

            var pbuffer = pool.Get((int)numBytesToRead);
            device.ReadAsync(address, (IntPtr)pbuffer.aligned_pointer,
                (uint)numBytesToRead, IOCallback, semaphore);
            semaphore.Wait();

            buffer = new byte[numBytesToRead];
            fixed (byte* bufferRaw = buffer)
                Buffer.MemoryCopy(pbuffer.aligned_pointer, bufferRaw, numBytesToRead, numBytesToRead);
            pbuffer.Return();
        }

        private static void IOCallback(uint errorCode, uint numBytes, object context)
        {
            ((SemaphoreSlim)context).Release();
        }
    }

    /// <summary>
    /// Provides a StreamProvider instance
    /// </summary>
    internal class StreamProviderFactory
    {
        /// <summary>
        /// Get a StreamProvider instance
        /// </summary>
        /// <param name="locationType">Type of location of files the stream provider reads from / writes to</param>
        /// <param name="connectionString">Connection string to Azure Storage, if applicable</param>
        /// <returns>StreamProvider instance</returns>
        internal static IStreamProvider GetStreamProvider(FileLocationType locationType, string connectionString = null)
        {
            switch (locationType)
            {
                case FileLocationType.AzureStorage:
                    if (string.IsNullOrEmpty(connectionString))
                        throw new ArgumentException("Azure Storage connection string is required to read/write settings to Azure Storage", nameof(connectionString));
                    return new AzureStreamProvider(connectionString);
                case FileLocationType.Local:
                    return new LocalFileStreamProvider();
                case FileLocationType.EmbeddedResource:
                    return new EmbeddedResourceStreamProvider();
                default:
                    throw new NotImplementedException();
            }
        }
    }

    /// <summary>
    /// StreamProvider for reading / writing files in Azure Storage
    /// </summary>
    internal class AzureStreamProvider : StreamProviderBase
    {
        private readonly string _connectionString;

        public AzureStreamProvider(string connectionString)
        {
            this._connectionString = connectionString;
        }

        protected override IDevice GetDevice(string path)
        {
            var fileInfo = new FileInfo(path);
            INamedDeviceFactory settingsDeviceFactoryCreator = new AzureStorageNamedDeviceFactory(this._connectionString, default);

            // Get the container info, if it does not exist it will be created
            settingsDeviceFactoryCreator.Initialize($"{fileInfo.Directory?.Name}");
            var settingsDevice = settingsDeviceFactoryCreator.Get(new FileDescriptor("", fileInfo.Name));
            settingsDevice.Initialize(MaxConfigFileSizeAligned, epoch: null, omitSegmentIdFromFilename: false);
            return settingsDevice;
        }

        protected override long GetBytesToWrite(byte[] bytes, IDevice device)
        {
            long numBytesToWrite = bytes.Length;
            numBytesToWrite = ((numBytesToWrite + (device.SectorSize - 1)) & ~(device.SectorSize - 1));
            if (numBytesToWrite > MaxConfigFileSizeAligned)
                throw new Exception($"Config file size {numBytesToWrite} is larger than the maximum allowed size {MaxConfigFileSizeAligned}");
            return numBytesToWrite;
        }
    }

    /// <summary>
    /// StreamProvider for reading / writing files locally
    /// </summary>
    internal class LocalFileStreamProvider : StreamProviderBase
    {
        protected override IDevice GetDevice(string path)
        {
            var fileInfo = new FileInfo(path);

            INamedDeviceFactory settingsDeviceFactoryCreator = new LocalStorageNamedDeviceFactory(disableFileBuffering: false);
            settingsDeviceFactoryCreator.Initialize("");
            var settingsDevice = settingsDeviceFactoryCreator.Get(new FileDescriptor(fileInfo.DirectoryName, fileInfo.Name));
            settingsDevice.Initialize(-1, epoch: null, omitSegmentIdFromFilename: true);
            return settingsDevice;
        }

        protected override long GetBytesToWrite(byte[] bytes, IDevice device)
        {
            return bytes.Length;
        }
    }

    /// <summary>
    /// StreamProvider for reading / writing files as embedded resources in executing assembly
    /// </summary>
    internal class EmbeddedResourceStreamProvider : IStreamProvider
    {
        public Stream Read(string path)
        {
            var assembly = Assembly.GetExecutingAssembly();
            var resourceName = assembly.GetManifestResourceNames().FirstOrDefault(rn => rn.EndsWith(path));
            if (resourceName == null) return null;

            return Assembly.GetExecutingAssembly().GetManifestResourceStream(resourceName);
        }

        public void Write(string path, byte[] data)
        {
            var assembly = Assembly.GetExecutingAssembly();
            var resourceName = assembly.GetManifestResourceNames().FirstOrDefault(rn => rn.EndsWith(path));
            if (resourceName == null) return;

            using var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream(resourceName);
            if (stream != null)
                stream.Write(data, 0, data.Length);
        }
    }
}