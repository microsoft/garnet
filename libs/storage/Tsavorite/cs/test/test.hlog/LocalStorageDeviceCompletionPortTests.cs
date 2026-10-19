// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.Versioning;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    /// <summary>
    /// Tests the completion port and threads that <see cref="LocalStorageDevice"/> owns on Windows.
    /// </summary>
    [TestFixture]
    [SupportedOSPlatform("windows")]
    public unsafe class LocalStorageDeviceCompletionPortTests : TestBase
    {
        const int SectorSize = 4096;
        const int IoSize = 64 * 1024;
        const int IoCount = 256;
        static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(10);

        [SetUp]
        public void Setup()
        {
            if (!OperatingSystem.IsWindows())
                Assert.Ignore("LocalStorageDevice runs only on Windows");
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown(waitForDelete: true);

        /// <summary>
        /// A synchronous wait for IO on a pool thread must not need another pool thread to complete the IO,
        /// or it starves when the pool is capped.
        /// </summary>
        [Test]
        public void TheDefaultDeviceCompletesIoOffThePool()
        {
            using var device = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "test.log"), DeviceType.Native, deleteOnClose: true);
            var (buffer, pointer) = AllocateAligned(IoSize);
            using var written = new ManualResetEventSlim();
            var onPoolThread = true;
            device.WriteAsync(pointer, 0, 0, IoSize, (_, _, _, _) =>
            {
                onPoolThread = Thread.CurrentThread.IsThreadPoolThread;
                written.Set();
            }, null);

            ClassicAssert.IsTrue(written.Wait(WaitTimeout), "The write did not complete");
            ClassicAssert.IsFalse(onPoolThread, "The write completed on a thread-pool thread");
            GC.KeepAlive(buffer);
        }

        [Test]
        public void DisposeDeliversTheCallbackOfEveryIoInFlight()
        {
            var device = CreateDevice(numCompletionThreads: 1);
            var (buffer, pointer) = AllocateAligned(IoSize * IoCount);
            using (var written = new ManualResetEventSlim())
            {
                device.WriteAsync(pointer, 0, 0, IoSize * IoCount, (_, _, _, _) => written.Set(), null);
                ClassicAssert.IsTrue(written.Wait(WaitTimeout), "The write did not complete");
            }

            var completed = 0;
            for (var i = 0; i < IoCount; i++)
                device.ReadAsync(0, (ulong)(i * IoSize), pointer + i * IoSize, IoSize, (_, _, _, _) => Interlocked.Increment(ref completed), null);
            device.Dispose();

            ClassicAssert.IsTrue(SpinWait.SpinUntil(() => Volatile.Read(ref completed) == IoCount, WaitTimeout),
                $"{Volatile.Read(ref completed)} of {IoCount} callbacks ran");
            GC.KeepAlive(buffer);
        }

        [Test]
        public void DisposeStopsEveryCompletionThread([Values(1, 4)] int numCompletionThreads)
        {
            var device = CreateDevice(numCompletionThreads);
            var threads = device.completionThreads;
            ClassicAssert.AreEqual(numCompletionThreads, threads.Length);

            // A poller takes exit packets meant for the completion threads.
            using var stopPolling = new CancellationTokenSource();
            var poller = new Thread(() =>
            {
                while (!stopPolling.IsCancellationRequested)
                    _ = device.TryComplete();
            });
            poller.Start();

            device.Dispose();
            foreach (var thread in threads)
                ClassicAssert.IsTrue(thread.Join(WaitTimeout), "A completion thread is still running");

            stopPolling.Cancel();
            poller.Join();
            ClassicAssert.IsFalse(device.TryComplete(), "TryComplete polled the port after Dispose");
        }

        [Test]
        public void ACompletionCallbackCanDisposeTheDevice()
        {
            var device = CreateDevice(numCompletionThreads: 1);
            var threads = device.completionThreads;
            var (buffer, pointer) = AllocateAligned(IoSize);
            using var disposed = new ManualResetEventSlim();
            device.WriteAsync(pointer, 0, 0, IoSize, (_, _, _, _) =>
            {
                device.Dispose();
                disposed.Set();
            }, null);

            ClassicAssert.IsTrue(disposed.Wait(WaitTimeout), "Dispose did not return");
            foreach (var thread in threads)
                ClassicAssert.IsTrue(thread.Join(WaitTimeout), "A completion thread is still running");
            GC.KeepAlive(buffer);
        }

        static LocalStorageDevice CreateDevice(int numCompletionThreads)
        {
            var saved = LocalStorageDevice.NumCompletionThreads;
            LocalStorageDevice.NumCompletionThreads = numCompletionThreads;
            try
            {
                // Positional, since the internal constructor also takes useIoCompletionPort by name:
                // preallocateFile, deleteOnClose, disableFileBuffering, capacity, recoverDevice, useIoCompletionPort.
                return new LocalStorageDevice(Path.Join(TestUtils.MethodTestDir, "test.log"), false, true, true, Devices.CAPACITY_UNSPECIFIED, false, true);
            }
            finally
            {
                LocalStorageDevice.NumCompletionThreads = saved;
            }
        }

        static (byte[] buffer, IntPtr pointer) AllocateAligned(int size)
        {
            var buffer = GC.AllocateArray<byte>(size + SectorSize, pinned: true);
            var pointer = (IntPtr)(((long)Unsafe.AsPointer(ref buffer[0]) + SectorSize - 1) & ~(long)(SectorSize - 1));
            return (buffer, pointer);
        }
    }
}