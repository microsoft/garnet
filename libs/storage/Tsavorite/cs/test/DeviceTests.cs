// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test
{
    [TestFixture]
    public class DeviceTests
    {
        const int entryLength = 1024;
        SectorAlignedBufferPool bufferPool;
        readonly byte[] entry = new byte[entryLength];
        string path;
        SemaphoreSlim semaphore;

        [SetUp]
        public void Setup()
        {
            path = TestUtils.MethodTestDir + "/test.log";

            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            // Set entry data
            for (int i = 0; i < entry.Length; i++)
                entry[i] = (byte)i;

            bufferPool = new SectorAlignedBufferPool(1, 512);
            semaphore = new SemaphoreSlim(0);
        }

        [TearDown]
        public void TearDown()
        {
            semaphore.Dispose();
            bufferPool.Free();

            // Clean up log files
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [Test]
        public void NativeDeviceTest1()
        {
            // Create devices \ log for test for in memory device
            using var device = new NativeStorageDevice(path, true); // Devices.CreateLogDevice(path, deleteOnClose: true)

            WriteInto(device, 0, entry, entryLength);
            ReadInto(device, 0, out var readEntry, entryLength);

            Assert.IsTrue(readEntry.SequenceEqual(entry));
        }

        [Test]
        public unsafe void NativeDeviceTest2()
        {
            int size = 1 << 16;
            int sector_size = 512;

            var rbuffer = GC.AllocateArray<byte>(size + sector_size, true);
            new Span<byte>(rbuffer).Clear();
            IntPtr ralignedBufferPtr = (IntPtr)(((long)Unsafe.AsPointer(ref rbuffer[0]) + (sector_size - 1)) & ~(sector_size - 1));

            var buffers = new byte[50][];
            for (int i = 0; i < 50; i++)
            {
                buffers[i] = GC.AllocateArray<byte>(size + sector_size, true);
                var buffer = buffers[i];
                IntPtr alignedBufferPtr = (IntPtr)(((long)Unsafe.AsPointer(ref buffer[0]) + (sector_size - 1)) & ~(sector_size - 1));

                using var device = new NativeStorageDevice(path, true);

                device.WriteAsync(alignedBufferPtr, 0, (uint)size, IOCallback, null);
                semaphore.Wait();

                device.ReadAsync(0, ralignedBufferPtr, (uint)size, IOCallback, null);
                semaphore.Wait();

                Assert.IsTrue(new ReadOnlySpan<byte>((void*)ralignedBufferPtr, size).SequenceEqual(new ReadOnlySpan<byte>((void*)alignedBufferPtr, size)));
                buffer = null;
            }
        }

        void Callback(uint errorCode, uint numBytes, object context)
        {
            semaphore.Release();
        }

        unsafe void WriteInto(IDevice device, ulong address, byte[] buffer, int size)
        {
            long numBytesToWrite = size;
            numBytesToWrite = ((numBytesToWrite + (device.SectorSize - 1)) & ~(device.SectorSize - 1));

            var pbuffer = bufferPool.Get((int)numBytesToWrite);
            fixed (byte* bufferRaw = buffer)
            {
                Buffer.MemoryCopy(bufferRaw, pbuffer.aligned_pointer, size, size);
            }

            device.WriteAsync((IntPtr)pbuffer.aligned_pointer, address, (uint)numBytesToWrite, IOCallback, null);
            semaphore.Wait();

            pbuffer.Return();
        }

        unsafe void ReadInto(IDevice device, ulong address, out byte[] buffer, int size)
        {
            long numBytesToRead = size;
            numBytesToRead = ((numBytesToRead + (device.SectorSize - 1)) & ~(device.SectorSize - 1));

            var pbuffer = bufferPool.Get((int)numBytesToRead);
            device.ReadAsync(address, (IntPtr)pbuffer.aligned_pointer,
                (uint)numBytesToRead, IOCallback, null);
            semaphore.Wait();
            buffer = new byte[numBytesToRead];
            fixed (byte* bufferRaw = buffer)
                Buffer.MemoryCopy(pbuffer.aligned_pointer, bufferRaw, numBytesToRead, numBytesToRead);
            pbuffer.Return();
        }

        void IOCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                Assert.Fail("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
            semaphore.Release();
        }
    }
}