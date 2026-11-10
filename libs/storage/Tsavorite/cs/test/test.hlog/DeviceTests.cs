// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using Tsavorite.devices;

namespace Tsavorite.test
{
    [TestFixture]
    public class DeviceTests : TestBase
    {
        const int entryLength = IDevice.MinDeviceSectorSize * 2;
        SectorAlignedBufferPool bufferPool;
        readonly byte[] entry = new byte[entryLength];
        SemaphoreSlim semaphore;

        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            // Set entry data
            for (int i = 0; i < entry.Length; i++)
                entry[i] = (byte)i;

            // Use 4096 to match the strictest device.SectorSize we expect on any modern
            // hardware (4Kn or 512e drives). Matches HardeningSectorSize used in the
            // IDevice_ contract tests below.
            bufferPool = new SectorAlignedBufferPool(1, 4096);
            semaphore = new SemaphoreSlim(0);
        }

        [TearDown]
        public void TearDown()
        {
            semaphore.Dispose();
            bufferPool.Free();

            // Clean up log files
            TestUtils.OnTearDown(waitForDelete: true);
        }

        [Test]
        public void NativeDeviceTest1()
        {
            // Create devices \ log for test for in memory device
            using var device = new NativeStorageDevice(Path.Join(TestUtils.MethodTestDir, "test.log"), true); // Devices.CreateLogDevice(path, deleteOnClose: true)
            device.Initialize(1L << 30);

            WriteInto(device, 0, entry, entryLength);
            ReadInto(device, 0, out var readEntry, entryLength);

            ClassicAssert.IsTrue(readEntry.SequenceEqual(entry));
        }

        /// <summary>
        /// Linux limits paths to PATH_MAX rather than Windows' 260-character MAX_PATH. The native device accepts a path that
        /// leaves room within PATH_MAX for its ".&lt;segment&gt;" suffix, and rejects a longer one when it is created rather than
        /// on its first IO.
        /// </summary>
        [Test]
        public void NativeDeviceLimitsPathsToPathMaxOnLinux()
        {
            if (!OperatingSystem.IsLinux())
                Assert.Ignore("Windows limits paths that are not extended-length to MAX_PATH.");

            // Directories nearly as deep as PATH_MAX allows; no component may exceed NAME_MAX (255 bytes).
            var dir = TestUtils.MethodTestDir;
            while (dir.Length < Native32.LINUX_PATH_MAX - 200)
                dir = Path.Join(dir, new string('d', 100));

            // The longest path the device accepts leaves 11 bytes for the ".<segment>" suffix.
            var longestPath = Path.Join(dir, new string('f', Native32.LINUX_PATH_MAX - 11 - dir.Length - 1));
            ClassicAssert.AreEqual(Native32.LINUX_PATH_MAX - 11, longestPath.Length);

            using (var device = new NativeStorageDevice(longestPath, deleteOnClose: true))
            {
                device.Initialize(1L << 30);
                WriteInto(device, 0, entry, entryLength);
                ReadInto(device, 0, out var readEntry, entryLength);
                ClassicAssert.IsTrue(readEntry.SequenceEqual(entry));
            }

            var ex = Assert.Throws<TsavoriteException>(() => new NativeStorageDevice(longestPath + "f", deleteOnClose: true));
            StringAssert.Contains("is too long", ex.Message);
        }

        [Test]
        public unsafe void NativeDeviceTest2()
        {
            int size = 1 << 16;
            int sector_size = 4096;

            var rbuffer = GC.AllocateArray<byte>(size + sector_size, true);
            new Span<byte>(rbuffer).Clear();
            IntPtr ralignedBufferPtr = (IntPtr)(((long)Unsafe.AsPointer(ref rbuffer[0]) + (sector_size - 1)) & ~(sector_size - 1));

            var buffers = new byte[50][];
            for (int i = 0; i < 50; i++)
            {
                buffers[i] = GC.AllocateArray<byte>(size + sector_size, true);
                var buffer = buffers[i];
                IntPtr alignedBufferPtr = (IntPtr)(((long)Unsafe.AsPointer(ref buffer[0]) + (sector_size - 1)) & ~(sector_size - 1));

                using var device = new NativeStorageDevice(Path.Join(TestUtils.MethodTestDir, "test.log"), true);
                device.Initialize(1L << 30);

                device.WriteAsync(alignedBufferPtr, 0, (uint)size, IOCallback, null);
                semaphore.Wait();

                device.ReadAsync(0, ralignedBufferPtr, (uint)size, IOCallback, null);
                semaphore.Wait();

                ClassicAssert.IsTrue(new ReadOnlySpan<byte>((void*)ralignedBufferPtr, size).SequenceEqual(new ReadOnlySpan<byte>((void*)alignedBufferPtr, size)));
                buffer = null;
            }
        }

        void Callback(uint errorCode, uint numBytes, object context, Exception ioException)
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
            // Return only the caller-requested logical size, not the sector-rounded read
            // length. Otherwise the returned buffer is `numBytesToRead` (e.g. 4096 when
            // size=1024 and SectorSize=4096), which mismatches the caller's expectation.
            buffer = new byte[size];
            fixed (byte* bufferRaw = buffer)
                Buffer.MemoryCopy(pbuffer.aligned_pointer, bufferRaw, size, size);
            pbuffer.Return();
        }

        void IOCallback(uint errorCode, uint numBytes, object context, Exception ioException)
        {
            if (errorCode != 0)
                Assert.Fail($"OverlappedStream GetQueuedCompletionStatus error: {errorCode}");
            semaphore.Release();
        }

        // ===================================================================================
        // Phase 7 — NativeStorageDevice hardening suite.
        //
        // Exercises lifecycle (dispose/initialize ordering), single-IO round-trips across a
        // range of segment sizes, parallel reads/writes, error injection on invalid segment
        // sizes, and recovery mismatch detection. Designed to be CI-friendly: every test cleans
        // up its own files via TestUtils.MethodTestDir, allocations stay below 16 MB, and the
        // parallel tests cap in-flight IOs well below the device's ThrottleLimit of 120.
        //
        // HardeningSectorSize = 4096 is a safe upper bound: the probe returns the required DIO
        // alignment (512 or 4096 on real hardware), and a 4096-aligned buffer is always >= that.
        // Bump this if an 8K-DIO disk ever appears.
        // ===================================================================================

        const int HardeningSectorSize = 4096;
        const long Mib = 1L << 20;
        const long Gib = 1L << 30;

        /// <summary>
        /// Allocates a pinned, sector-aligned buffer of `size` bytes and fills it with the
        /// pattern produced by `pattern(i)`. Returns the byte[] (rooted so the GC won't move
        /// it) and a sector-aligned pointer into it. Tests must keep `buffer` alive for the
        /// duration of any outstanding I/O on `pointer`.
        /// </summary>
        unsafe (byte[] buffer, IntPtr pointer) AllocateAlignedBuffer(int size, Func<int, byte> pattern)
        {
            var buffer = GC.AllocateArray<byte>(size + HardeningSectorSize, pinned: true);
            var ptr = (IntPtr)(((long)Unsafe.AsPointer(ref buffer[0]) + (HardeningSectorSize - 1)) & ~(HardeningSectorSize - 1));
            byte* p = (byte*)ptr;
            for (int i = 0; i < size; i++) p[i] = pattern(i);
            return (buffer, ptr);
        }

        unsafe void AssertBufferContents(IntPtr pointer, int size, Func<int, byte> expected, string label)
        {
            byte* p = (byte*)pointer;
            for (int i = 0; i < size; i++)
            {
                if (p[i] != expected(i))
                {
                    Assert.Fail($"{label}: mismatch at byte {i}: expected 0x{expected(i):X2}, got 0x{p[i]:X2}");
                }
            }
        }

        // ----- IDevice test fixtures --------------------------------------------------

        /// <summary>
        /// Device kinds parametrized by the cross-device round-trip and parallel tests below.
        /// Native is Linux-only (the C++ shim links against libaio/liburing). RandomAccess and
        /// ManagedLocal work on both Linux (managed RandomAccess / FileStream over P/Invoke
        /// O_DIRECT or page-cache) and Windows (IOCP-bound OVERLAPPED I/O).
        /// </summary>
        public enum DeviceKind { Native, RandomAccess, ManagedLocal }

        /// <summary>
        /// Construct + Initialize a fresh device of the requested kind backed by
        /// <paramref name="path"/>. Skips the test via <see cref="Assert.Ignore(string)"/> when
        /// the kind is unsupported on the current OS (Native on Windows).
        /// </summary>
        static IDevice CreateDeviceForTest(DeviceKind kind, string path, long segmentSize, bool deleteOnClose = true)
        {
            switch (kind)
            {
                case DeviceKind.Native:
                    var nd = new NativeStorageDevice(path, deleteOnClose: deleteOnClose);
                    nd.Initialize(segmentSize);
                    return nd;
                case DeviceKind.RandomAccess:
                    var ra = new RandomAccessLocalStorageDevice(path, preallocateFile: false, deleteOnClose: deleteOnClose);
                    ra.Initialize(segmentSize);
                    return ra;
                case DeviceKind.ManagedLocal:
                    var ml = new ManagedLocalStorageDevice(path, preallocateFile: false, deleteOnClose: deleteOnClose);
                    ml.Initialize(segmentSize);
                    return ml;
                default:
                    throw new ArgumentOutOfRangeException(nameof(kind));
            }
        }

        // ----- IDevice tests (all device kinds) ------------------------------------

        [Test]
        [TestCase(DeviceKind.Native)]
        [TestCase(DeviceKind.RandomAccess)]
        [TestCase(DeviceKind.ManagedLocal)]
        [Category("IDevice")]
        public unsafe void IDevice_RoundTrip_BasicReadWrite(DeviceKind kind)
        {
            const long segmentSize = 64 * Mib;
            const int size = 64 * 1024;
            using var device = CreateDeviceForTest(kind, Path.Join(TestUtils.MethodTestDir, "test.log"), segmentSize);

            var (wbuf, wptr) = AllocateAlignedBuffer(size, i => (byte)((i * 7) & 0xFF));
            device.WriteAsync(wptr, 0, 0, (uint)size, IOCallback, null);
            semaphore.Wait();

            var (rbuf, rptr) = AllocateAlignedBuffer(size, _ => 0);
            device.ReadAsync(0, 0, rptr, (uint)size, IOCallback, null);
            semaphore.Wait();

            AssertBufferContents(rptr, size, i => (byte)((i * 7) & 0xFF), $"{kind} basic round-trip");
            GC.KeepAlive(wbuf); GC.KeepAlive(rbuf);
        }

        [Test]
        [TestCase(DeviceKind.Native)]
        [TestCase(DeviceKind.RandomAccess)]
        [TestCase(DeviceKind.ManagedLocal)]
        [Category("IDevice")]
        public unsafe void IDevice_RoundTrip_AcrossSegmentBoundary(DeviceKind kind)
        {
            const long segmentSize = 64 * Mib;
            const int size = 64 * 1024;
            using var device = CreateDeviceForTest(kind, Path.Join(TestUtils.MethodTestDir, "test.log"), segmentSize);

            var (wbuf, wptr) = AllocateAlignedBuffer(size, i => (byte)((i * 11) & 0xFF));
            device.WriteAsync(wptr, segmentId: 1, destinationAddress: 0, (uint)size, IOCallback, null);
            semaphore.Wait();

            var (rbuf, rptr) = AllocateAlignedBuffer(size, _ => 0);
            device.ReadAsync(segmentId: 1, sourceAddress: 0, rptr, (uint)size, IOCallback, null);
            semaphore.Wait();

            AssertBufferContents(rptr, size, i => (byte)((i * 11) & 0xFF), $"{kind} cross-segment");
            GC.KeepAlive(wbuf); GC.KeepAlive(rbuf);
        }

        [Test]
        [TestCase(DeviceKind.Native)]
        [TestCase(DeviceKind.RandomAccess)]
        [TestCase(DeviceKind.ManagedLocal)]
        [Category("IDevice")]
        public unsafe void IDevice_Parallel_32ConcurrentWrites(DeviceKind kind)
        {
            const long segmentSize = 64 * Mib;
            const int N = 32;
            const int size = 8 * 1024;
            using var device = CreateDeviceForTest(kind, Path.Join(TestUtils.MethodTestDir, "test.log"), segmentSize);

            var roots = new byte[N][];
            for (int i = 0; i < N; i++)
            {
                int id = i;
                var (buf, ptr) = AllocateAlignedBuffer(size, j => (byte)((j ^ (id * 17)) & 0xFF));
                roots[i] = buf;
                device.WriteAsync(ptr, 0, (ulong)(id * size), (uint)size, IOCallback, null);
            }
            for (int i = 0; i < N; i++) semaphore.Wait();

            var (rbuf, rptr) = AllocateAlignedBuffer(size, _ => 0);
            for (int i = 0; i < N; i++)
            {
                device.ReadAsync(0, (ulong)(i * size), rptr, (uint)size, IOCallback, null);
                semaphore.Wait();
                int id = i;
                AssertBufferContents(rptr, size, j => (byte)((j ^ (id * 17)) & 0xFF), $"{kind} block {id}");
            }
            GC.KeepAlive(roots); GC.KeepAlive(rbuf);
        }

        [Test]
        [TestCase(DeviceKind.Native)]
        [TestCase(DeviceKind.RandomAccess)]
        [TestCase(DeviceKind.ManagedLocal)]
        [Category("IDevice")]
        public unsafe void IDevice_Parallel_BurstyTraffic(DeviceKind kind)
        {
            const long segmentSize = 64 * Mib;
            const int Bursts = 10;
            const int Per = 10;
            const int size = 4 * 1024;
            using var device = CreateDeviceForTest(kind, Path.Join(TestUtils.MethodTestDir, "test.log"), segmentSize);

            for (int b = 0; b < Bursts; b++)
            {
                var roots = new byte[Per][];
                for (int i = 0; i < Per; i++)
                {
                    int globalIdx = b * Per + i;
                    var (buf, ptr) = AllocateAlignedBuffer(size, j => (byte)((j + globalIdx) & 0xFF));
                    roots[i] = buf;
                    device.WriteAsync(ptr, 0, (ulong)(globalIdx * size), (uint)size, IOCallback, null);
                }
                for (int i = 0; i < Per; i++) semaphore.Wait();
                GC.KeepAlive(roots);
            }
        }

        [TestCase(DeviceKind.Native, 64L * 1024 * 1024)]
        [TestCase(DeviceKind.Native, 256L * 1024 * 1024)]
        [TestCase(DeviceKind.Native, 1L * 1024 * 1024 * 1024)]
        [TestCase(DeviceKind.RandomAccess, 64L * 1024 * 1024)]
        [TestCase(DeviceKind.RandomAccess, 256L * 1024 * 1024)]
        [TestCase(DeviceKind.RandomAccess, 1L * 1024 * 1024 * 1024)]
        [TestCase(DeviceKind.ManagedLocal, 64L * 1024 * 1024)]
        [TestCase(DeviceKind.ManagedLocal, 256L * 1024 * 1024)]
        [TestCase(DeviceKind.ManagedLocal, 1L * 1024 * 1024 * 1024)]
        [Category("IDevice")]
        public unsafe void IDevice_RoundTrip_VariousSegmentSizes(DeviceKind kind, long segmentSize)
        {
            const int size = 64 * 1024;
            using var device = CreateDeviceForTest(kind, Path.Join(TestUtils.MethodTestDir, "test.log"), segmentSize);

            var (wbuf, wptr) = AllocateAlignedBuffer(size, i => (byte)((i * 7) & 0xFF));
            device.WriteAsync(wptr, 0, 0, (uint)size, IOCallback, null);
            semaphore.Wait();

            var (rbuf, rptr) = AllocateAlignedBuffer(size, _ => 0);
            device.ReadAsync(0, 0, rptr, (uint)size, IOCallback, null);
            semaphore.Wait();

            AssertBufferContents(rptr, size, i => (byte)((i * 7) & 0xFF), $"{kind} segSize={segmentSize}");
            GC.KeepAlive(wbuf); GC.KeepAlive(rbuf);
        }

        [Test]
        [TestCase(DeviceKind.Native)]
        [TestCase(DeviceKind.RandomAccess)]
        [TestCase(DeviceKind.ManagedLocal)]
        [Category("IDevice")]
        public unsafe void IDevice_Parallel_64ConcurrentReads(DeviceKind kind)
        {
            const long segmentSize = 64 * Mib;
            const int N = 64;
            const int size = 4 * 1024;
            using var device = CreateDeviceForTest(kind, Path.Join(TestUtils.MethodTestDir, "test.log"), segmentSize);

            // Pre-write one big buffer covering all N blocks so each block has its own byte pattern.
            var (wbuf, wptr) = AllocateAlignedBuffer(N * size, j =>
            {
                int blk = j / size;
                int off = j % size;
                return (byte)((blk * 31 + off) & 0xFF);
            });
            device.WriteAsync(wptr, 0, 0, (uint)(N * size), IOCallback, null);
            semaphore.Wait();

            var rbufs = new byte[N][];
            var rptrs = new IntPtr[N];
            for (int i = 0; i < N; i++)
            {
                var (rb, rp) = AllocateAlignedBuffer(size, _ => 0);
                rbufs[i] = rb; rptrs[i] = rp;
                device.ReadAsync(0, (ulong)(i * size), rp, (uint)size, IOCallback, null);
            }
            for (int i = 0; i < N; i++) semaphore.Wait();

            for (int i = 0; i < N; i++)
            {
                int blk = i;
                AssertBufferContents(rptrs[i], size, off => (byte)((blk * 31 + off) & 0xFF), $"{kind} block {blk}");
            }
            GC.KeepAlive(wbuf); GC.KeepAlive(rbufs);
        }

        [Test]
        [TestCase(DeviceKind.Native)]
        [TestCase(DeviceKind.RandomAccess)]
        [TestCase(DeviceKind.ManagedLocal)]
        [Category("IDevice")]
        public unsafe void IDevice_Parallel_MixedReadsAndWrites(DeviceKind kind)
        {
            const long segmentSize = 64 * Mib;
            const int N = 16;
            const int size = 4 * 1024;
            const ulong readBase = 0;
            const ulong writeBase = 256UL * 1024; // disjoint
            using var device = CreateDeviceForTest(kind, Path.Join(TestUtils.MethodTestDir, "test.log"), segmentSize);

            // Pre-write the read region.
            var (prewbuf, prewptr) = AllocateAlignedBuffer(N * size, j => (byte)((j * 3) & 0xFF));
            device.WriteAsync(prewptr, 0, readBase, (uint)(N * size), IOCallback, null);
            semaphore.Wait();

            // Fire concurrent reads and writes to disjoint regions.
            var wroots = new byte[N][];
            var rroots = new byte[N][];
            var rptrs = new IntPtr[N];
            for (int i = 0; i < N; i++)
            {
                int id = i;
                var (rb, rp) = AllocateAlignedBuffer(size, _ => 0);
                rroots[i] = rb; rptrs[i] = rp;
                device.ReadAsync(0, readBase + (ulong)(id * size), rp, (uint)size, IOCallback, null);

                var (wb, wp) = AllocateAlignedBuffer(size, j => (byte)((j + id) & 0xFF));
                wroots[i] = wb;
                device.WriteAsync(wp, 0, writeBase + (ulong)(id * size), (uint)size, IOCallback, null);
            }
            for (int i = 0; i < 2 * N; i++) semaphore.Wait();

            for (int i = 0; i < N; i++)
            {
                int id = i;
                int baseIdx = id * size;
                AssertBufferContents(rptrs[i], size, off => (byte)(((baseIdx + off) * 3) & 0xFF), $"{kind} read {id}");
            }
            GC.KeepAlive(prewbuf); GC.KeepAlive(wroots); GC.KeepAlive(rroots);
        }

        [Test]
        [TestCase(DeviceKind.Native)]
        [TestCase(DeviceKind.RandomAccess)]
        [TestCase(DeviceKind.ManagedLocal)]
        [Category("IDevice")]
        public unsafe void IDevice_Parallel_StressBurst_100Writes(DeviceKind kind)
        {
            const long segmentSize = 64 * Mib;
            const int N = 100;
            const int size = 4 * 1024;
            using var device = CreateDeviceForTest(kind, Path.Join(TestUtils.MethodTestDir, "test.log"), segmentSize);

            var roots = new byte[N][];
            for (int i = 0; i < N; i++)
            {
                int id = i;
                var (buf, ptr) = AllocateAlignedBuffer(size, j => (byte)((j * 5 + id) & 0xFF));
                roots[i] = buf;
                device.WriteAsync(ptr, 0, (ulong)(id * size), (uint)size, IOCallback, null);
            }
            for (int i = 0; i < N; i++) semaphore.Wait();

            // Read back every fifth block to spot-check.
            var (rbuf, rptr) = AllocateAlignedBuffer(size, _ => 0);
            for (int i = 0; i < N; i += 5)
            {
                device.ReadAsync(0, (ulong)(i * size), rptr, (uint)size, IOCallback, null);
                semaphore.Wait();
                int id = i;
                AssertBufferContents(rptr, size, j => (byte)((j * 5 + id) & 0xFF), $"{kind} stress block {id}");
            }
            GC.KeepAlive(roots); GC.KeepAlive(rbuf);
        }

        // ----- IDevice contract: a read past the end of a file reads nothing ------------

        /// <summary>
        /// A read that starts at or past the end of a segment's file, including a read of a segment that was never written,
        /// completes with no error and no bytes. (LocalStorageDevice, not covered here, reports Windows' end-of-file error
        /// instead; callers accept both.)
        /// </summary>
        [Test]
        [TestCase(DeviceKind.Native)]
        [TestCase(DeviceKind.RandomAccess)]
        [TestCase(DeviceKind.ManagedLocal)]
        [Category("IDevice")]
        public unsafe void IDevice_ReadPastEndOfFile_CompletesWithNoBytes(DeviceKind kind)
        {
            using var device = CreateDeviceForTest(kind, Path.Join(TestUtils.MethodTestDir, "test.log"), 64 * Mib);

            // Write segment 0's first sector, then read where nothing was written: the next sector, a sector further past the end
            // of the file, and segment 1, which was never written
            var (wbuf, wptr) = AllocateAlignedBuffer(HardeningSectorSize, i => (byte)(i & 0xFF));
            device.WriteAsync(wptr, 0, 0, HardeningSectorSize, IOCallback, null);
            semaphore.Wait();

            var (rbuf, rptr) = AllocateAlignedBuffer(HardeningSectorSize, _ => 0);
            foreach (var (segmentId, address) in new[] { (0, (ulong)HardeningSectorSize), (0, 16UL * HardeningSectorSize), (1, 0UL) })
            {
                var (errorCode, numBytes) = (uint.MaxValue, uint.MaxValue);
                device.ReadAsync(segmentId, address, rptr, HardeningSectorSize, (e, n, _, _) =>
                {
                    (errorCode, numBytes) = (e, n);
                    _ = semaphore.Release();
                }, null);
                semaphore.Wait();
                ClassicAssert.AreEqual(0u, errorCode, $"{kind}: error code reading segment {segmentId} at {address}");
                ClassicAssert.AreEqual(0u, numBytes, $"{kind}: bytes read from segment {segmentId} at {address}");
            }
            GC.KeepAlive(wbuf);
            GC.KeepAlive(rbuf);
        }

        // ----- IDevice contract: Initialize is optional; ctor defaults are valid for IO ------

        [Test]
        [TestCase(DeviceKind.Native)]
        [TestCase(DeviceKind.RandomAccess)]
        [TestCase(DeviceKind.ManagedLocal)]
        [Category("IDevice")]
        public unsafe void IDevice_ReadAsyncBeforeInitialize_UsesCtorDefaults(DeviceKind kind)
        {
            // Uniform IDevice contract: ctor establishes valid defaults (segmentSize = -1,
            // segmentSizeBits = 64, segmentSizeMask = ~0UL) that route every IO to segment 0,
            // which is functionally identical to having called Initialize(-1). IO without an
            // explicit Initialize() call must succeed (single-segment mode is the default).
            var path = Path.Join(TestUtils.MethodTestDir, $"initopt_read_{kind}.log");
            // First write a small block via a separate device so the read below has data to
            // return. Both producer and consumer skip Initialize() — relying on ctor defaults.
            const uint kBlock = 4096;
            {
                IDevice writer = kind switch
                {
                    DeviceKind.Native => new NativeStorageDevice(path, deleteOnClose: false),
                    DeviceKind.RandomAccess => new RandomAccessLocalStorageDevice(path, preallocateFile: false, deleteOnClose: false),
                    DeviceKind.ManagedLocal => new ManagedLocalStorageDevice(path, preallocateFile: false, deleteOnClose: false),
                    _ => throw new ArgumentOutOfRangeException(nameof(kind)),
                };
                using (writer)
                {
                    var (wbuf, wptr) = AllocateAlignedBuffer((int)kBlock, i => (byte)(i & 0xFF));
                    writer.WriteAsync(wptr, 0, 0, kBlock, IOCallback, null);
                    semaphore.Wait();
                    GC.KeepAlive(wbuf);
                }
            }

            // Reader: no Initialize() call. Should succeed using ctor defaults.
            IDevice device = kind switch
            {
                DeviceKind.Native => new NativeStorageDevice(path, deleteOnClose: true),
                DeviceKind.RandomAccess => new RandomAccessLocalStorageDevice(path, preallocateFile: false, deleteOnClose: true),
                DeviceKind.ManagedLocal => new ManagedLocalStorageDevice(path, preallocateFile: false, deleteOnClose: true),
                _ => throw new ArgumentOutOfRangeException(nameof(kind)),
            };
            using (device)
            {
                var (buf, ptr) = AllocateAlignedBuffer((int)kBlock, _ => 0);
                Assert.DoesNotThrow(() => device.ReadAsync(0, 0, ptr, kBlock, IOCallback, null));
                semaphore.Wait();
                AssertBufferContents(ptr, (int)kBlock, i => (byte)(i & 0xFF), "read without Initialize");
                GC.KeepAlive(buf);
            }
        }

        [Test]
        [TestCase(DeviceKind.Native)]
        [TestCase(DeviceKind.RandomAccess)]
        [TestCase(DeviceKind.ManagedLocal)]
        [Category("IDevice")]
        public unsafe void IDevice_WriteAsyncBeforeInitialize_UsesCtorDefaults(DeviceKind kind)
        {
            // Companion of the read test above: IO without an explicit Initialize() call must
            // succeed because ctor defaults already establish unbounded single-segment routing.
            var path = Path.Join(TestUtils.MethodTestDir, $"initopt_write_{kind}.log");
            IDevice device = kind switch
            {
                DeviceKind.Native => new NativeStorageDevice(path, deleteOnClose: true),
                DeviceKind.RandomAccess => new RandomAccessLocalStorageDevice(path, preallocateFile: false, deleteOnClose: true),
                DeviceKind.ManagedLocal => new ManagedLocalStorageDevice(path, preallocateFile: false, deleteOnClose: true),
                _ => throw new ArgumentOutOfRangeException(nameof(kind)),
            };
            using (device)
            {
                var (buf, ptr) = AllocateAlignedBuffer(4096, i => (byte)i);
                Assert.DoesNotThrow(() => device.WriteAsync(ptr, 0, 0, 4096, IOCallback, null));
                semaphore.Wait();
                GC.KeepAlive(buf);
            }
        }

        [Test]
        [TestCase(DeviceKind.Native)]
        [TestCase(DeviceKind.RandomAccess)]
        [TestCase(DeviceKind.ManagedLocal)]
        [Category("IDevice")]
        public unsafe void IDevice_Initialize_SegmentSizeMinusOne_UnboundedSingleSegment(DeviceKind kind)
        {
            // Contract: Initialize(-1) selects unbounded single-segment mode. Every IO routes
            // through segment 0 (no per-segment file rotation). Validates with a write at a
            // high offset followed by readback, which would have crossed a segment boundary
            // under any positive segmentSize but here lives in the single growing segment file.
            const uint kBlock = 4096;
            // Pick a "would-have-crossed-segment-N" offset that is sector-aligned and modest
            // enough to not blow up tmp disk usage in CI. 1 MiB is far past any real segment
            // we use elsewhere in this fixture (64 MiB is our usual segment), but importantly
            // it exercises the (alignedAddress >> segmentSizeBits) math which must produce
            // segment 0 in -1 mode (since segmentSizeBits == 64 / segmentSizeMask == ~0).
            const ulong kHighOffset = 1UL << 20;
            using var device = CreateDeviceForTest(kind, Path.Join(TestUtils.MethodTestDir, $"unbounded_{kind}.log"), segmentSize: -1L);
            var (wbuf, wptr) = AllocateAlignedBuffer((int)kBlock, i => (byte)((i * 11 + 3) & 0xFF));
            var (rbuf, rptr) = AllocateAlignedBuffer((int)kBlock, _ => 0);

            var write = new System.Threading.SemaphoreSlim(0, 1);
            device.WriteAsync(wptr, 0, kHighOffset, kBlock, (e, n, c, _) => write.Release(), null);
            write.Wait();

            var read = new System.Threading.SemaphoreSlim(0, 1);
            device.ReadAsync(0, kHighOffset, rptr, kBlock, (e, n, c, _) => read.Release(), null);
            read.Wait();

            AssertBufferContents(rptr, (int)kBlock, i => (byte)((i * 11 + 3) & 0xFF), $"{kind} unbounded round-trip");
            GC.KeepAlive(wbuf); GC.KeepAlive(rbuf);
        }

        [Test]
        [TestCase(DeviceKind.Native)]
        [TestCase(DeviceKind.RandomAccess)]
        [TestCase(DeviceKind.ManagedLocal)]
        [Category("IDevice")]
        public unsafe void IDevice_Initialize_OmitSegmentIdFromFilename_BareFileName(DeviceKind kind)
        {
            // Contract: Initialize(-1, omitSegmentIdFromFilename: true) makes the device write
            // to the bare basename (no `.0` suffix). All three managed and native device kinds
            // support this; combined with -1 it lets external tooling open the data file by a
            // fixed, predictable name (e.g. log commit-metadata files named just `commit.42`).
            const uint kBlock = 4096;
            var basePath = Path.Join(TestUtils.MethodTestDir, $"omit_{kind}.log");
            IDevice device = kind switch
            {
                DeviceKind.Native => new NativeStorageDevice(basePath, deleteOnClose: false),
                DeviceKind.RandomAccess => new RandomAccessLocalStorageDevice(basePath, preallocateFile: false, deleteOnClose: false),
                DeviceKind.ManagedLocal => new ManagedLocalStorageDevice(basePath, preallocateFile: false, deleteOnClose: false),
                _ => throw new ArgumentOutOfRangeException(nameof(kind)),
            };
            using (device)
            {
                device.Initialize(segmentSize: -1L, omitSegmentIdFromFilename: true);

                var (wbuf, wptr) = AllocateAlignedBuffer((int)kBlock, i => (byte)((i * 7 + 1) & 0xFF));
                var write = new System.Threading.SemaphoreSlim(0, 1);
                device.WriteAsync(wptr, 0, 0, kBlock, (e, n, c, _) => write.Release(), null);
                write.Wait();

                // The on-disk file must be the bare basename (no `.0` suffix).
                Assert.That(File.Exists(basePath), Is.True, $"{kind}: expected bare-named file '{basePath}'");
                Assert.That(File.Exists(basePath + ".0"), Is.False, $"{kind}: unexpected segment-suffixed file '{basePath}.0'");
                GC.KeepAlive(wbuf);
            }
            // Clean up — deleteOnClose=false because the bare-named file isn't tracked by the
            // segment-aware Dispose; remove it explicitly.
            try { File.Delete(basePath); } catch { }
        }

        [Test]
        [TestCase(DeviceKind.Native)]
        [TestCase(DeviceKind.RandomAccess)]
        [TestCase(DeviceKind.ManagedLocal)]
        [Category("IDevice")]
        public void IDevice_Initialize_OmitSegmentIdFromFilename_WithoutMinusOne_Throws(DeviceKind kind)
        {
            // omitSegmentIdFromFilename is only meaningful in unbounded single-segment mode.
            // Combining it with a positive segment size would let segment 1, 2, ... all collapse
            // onto the same on-disk file and clobber each other; reject it at Initialize.
            var path = Path.Join(TestUtils.MethodTestDir, $"omit_pos_{kind}.log");
            IDevice device = kind switch
            {
                DeviceKind.Native => new NativeStorageDevice(path, deleteOnClose: true),
                DeviceKind.RandomAccess => new RandomAccessLocalStorageDevice(path, preallocateFile: false, deleteOnClose: true),
                DeviceKind.ManagedLocal => new ManagedLocalStorageDevice(path, preallocateFile: false, deleteOnClose: true),
                _ => throw new ArgumentOutOfRangeException(nameof(kind)),
            };
            using (device)
                Assert.Throws<TsavoriteException>(() => device.Initialize(64 * Mib, omitSegmentIdFromFilename: true));
        }

        // ----- NativeStorageDevice-specific tests (lifecycle, recovery, validation) -----------------------------------------------------

        // ----- Lifecycle (5 tests) ---------------------------------------------------------

        [Test]
        [Category("NativeStorageDevice")]
        public void NativeStorageDevice_DisposeBeforeInitialize_IsNoOp()
        {
            // Constructing a device without ever invoking IO must NOT crash on Dispose: the
            // native device handle is still IntPtr.Zero (created lazily on first IO) and the
            // Dispose path must skip the NativeDevice_Destroy P/Invoke and the
            // completion-thread join entirely.
            var device = new NativeStorageDevice(Path.Join(TestUtils.MethodTestDir, "test.log"), deleteOnClose: true);
            Assert.DoesNotThrow(() => device.Dispose());
        }

        [Test]
        [Category("NativeStorageDevice")]
        public void NativeStorageDevice_DisposeTwice_IsIdempotent()
        {
            // Dispose() is documented as idempotent — Interlocked.Exchange on disposedFlag is
            // the gate. The second Dispose must short-circuit without touching native code,
            // join threads, or throwing.
            var device = new NativeStorageDevice(Path.Join(TestUtils.MethodTestDir, "test.log"), deleteOnClose: true);
            device.Initialize(64 * Mib);
            device.Dispose();
            Assert.DoesNotThrow(() => device.Dispose());
        }

        [Test]
        [Category("NativeStorageDevice")]
        public void NativeStorageDevice_InitializeTwice_Idempotent()
        {
            // Initialize is idempotent (matches LocalStorageDevice / RandomAccessLocalStorageDevice
            // contract): metadata is updated on each call and the native handle is created
            // lazily on first IO using the final segmentSize / segmentSizeBits / segmentSizeMask.
            // Repeated Initialize calls with the same or different segment sizes are legal as
            // long as no IO has flowed yet.
            using var device = new NativeStorageDevice(Path.Join(TestUtils.MethodTestDir, "test.log"), deleteOnClose: true);
            Assert.DoesNotThrow(() => device.Initialize(64 * Mib));
            Assert.DoesNotThrow(() => device.Initialize(64 * Mib));
        }

        // ----- Recovery (3 tests) ----------------------------------------------------------

        [Test]
        [Category("NativeStorageDevice")]
        public void NativeStorageDevice_NonPowerOfTwoSegmentSize_Throws()
        {
            // 3 MiB is not a power of 2; the managed validation in Initialize rejects it before
            // any P/Invoke happens. (The native side would also reject via std::invalid_argument
            // from ValidatedShift, so this is a belt-and-suspenders test.)
            using var device = new NativeStorageDevice(Path.Join(TestUtils.MethodTestDir, "test.log"), deleteOnClose: true);
            Assert.Throws<TsavoriteException>(() => device.Initialize(3 * Mib));
        }

        [Test]
        [Category("NativeStorageDevice")]
        public void NativeStorageDevice_SegmentSizeSmallerThanSector_Throws()
        {
            // 256 bytes < 512 byte sector size — sub-sector segments make no sense and would
            // produce broken offset math in the upper layers.
            using var device = new NativeStorageDevice(Path.Join(TestUtils.MethodTestDir, "test.log"), deleteOnClose: true);
            Assert.Throws<TsavoriteException>(() => device.Initialize(256));
        }

        [Test]
        [Category("NativeStorageDevice")]
        public void NativeStorageDevice_ZeroSegmentSize_Throws()
        {
            using var device = new NativeStorageDevice(Path.Join(TestUtils.MethodTestDir, "test.log"), deleteOnClose: true);
            Assert.Throws<TsavoriteException>(() => device.Initialize(0));
        }

        // ----- Recovery (3 tests) ----------------------------------------------------------

        [Test]
        [Category("NativeStorageDevice")]
        public unsafe void NativeStorageDevice_Recovery_MatchingSegmentSize_Succeeds()
        {
            // Write some data with 64 MiB segments, dispose, then reopen with the same segment
            // size. The recovery check in NativeDeviceImpl ctor must accept this — existing
            // segment files are <= the configured segment size, so there's no mismatch.
            const long segmentSize = 64 * Mib;
            const int size = 64 * 1024;
            string path = Path.Join(TestUtils.MethodTestDir, "test.log");

            {
                using var device = new NativeStorageDevice(path, deleteOnClose: false);
                device.Initialize(segmentSize);
                var (wbuf, wptr) = AllocateAlignedBuffer(size, i => (byte)((i * 13) & 0xFF));
                device.WriteAsync(wptr, 0, 0, (uint)size, IOCallback, null);
                semaphore.Wait();
                GC.KeepAlive(wbuf);
            }

            {
                using var device2 = new NativeStorageDevice(path, deleteOnClose: true);
                Assert.DoesNotThrow(() => device2.Initialize(segmentSize));
                var (rbuf, rptr) = AllocateAlignedBuffer(size, _ => 0);
                device2.ReadAsync(0, 0, rptr, (uint)size, IOCallback, null);
                semaphore.Wait();
                AssertBufferContents(rptr, size, i => (byte)((i * 13) & 0xFF), "recovery match");
                GC.KeepAlive(rbuf);
            }
        }

        [Test]
        [Category("NativeStorageDevice")]
        public unsafe void NativeStorageDevice_Recovery_LargerExistingSegment_DetectsMismatch()
        {
            // Write data with 256 MiB segments large enough to overflow a 64 MiB segment, then
            // reopen with 64 MiB. The C++ NativeDeviceImpl ctor's ValidateRecoveredSegments
            // walk should find the existing file > 64 MiB and refuse to open (TsavoriteException
            // surfacing the native last_error).
            //
            // Note: we write 65 MiB of data into a 256 MiB segment so segment-0 file grows past
            // 64 MiB.
            const long bigSegment = 256 * Mib;
            const long smallSegment = 64 * Mib;
            const int size = 65 * 1024 * 1024; // 65 MiB
            string path = Path.Join(TestUtils.MethodTestDir, "test.log");

            {
                using var device = new NativeStorageDevice(path, deleteOnClose: false);
                device.Initialize(bigSegment);
                var (wbuf, wptr) = AllocateAlignedBuffer(size, i => (byte)((i & 0xFF)));
                device.WriteAsync(wptr, 0, 0, (uint)size, IOCallback, null);
                semaphore.Wait();
                GC.KeepAlive(wbuf);
            }

            {
                using var device2 = new NativeStorageDevice(path, deleteOnClose: true);
                // Initialize is metadata-only — the native device (and the C++ recovery walk
                // that detects the mismatch) is created lazily on first IO. The mismatch must
                // surface here as a TsavoriteException, not as a silent successful Initialize.
                device2.Initialize(smallSegment);
                var (rbuf, rptr) = AllocateAlignedBuffer(4096, _ => 0);
                var ex = Assert.Throws<TsavoriteException>(() => device2.ReadAsync(0, 0, rptr, 4096, IOCallback, null));
                StringAssert.Contains("segment", ex.Message, "Expected mismatch message to mention segment");
                GC.KeepAlive(rbuf);
            }
        }

        [Test]
        [Category("NativeStorageDevice")]
        public unsafe void NativeStorageDevice_Recovery_SmallerExistingSegment_Succeeds()
        {
            // Inverse of the mismatch case: write a small file with 64 MiB segments, reopen
            // with 256 MiB segments. The existing file is <= the new segment size, so the
            // ValidateRecoveredSegments walk accepts it.
            const long smallSegment = 64 * Mib;
            const long bigSegment = 256 * Mib;
            const int size = 64 * 1024;
            string path = Path.Join(TestUtils.MethodTestDir, "test.log");

            {
                using var device = new NativeStorageDevice(path, deleteOnClose: false);
                device.Initialize(smallSegment);
                var (wbuf, wptr) = AllocateAlignedBuffer(size, i => (byte)((i * 17) & 0xFF));
                device.WriteAsync(wptr, 0, 0, (uint)size, IOCallback, null);
                semaphore.Wait();
                GC.KeepAlive(wbuf);
            }

            {
                using var device2 = new NativeStorageDevice(path, deleteOnClose: true);
                Assert.DoesNotThrow(() => device2.Initialize(bigSegment));
            }
        }

        // ---------------------------------------------------------------------------------
        // Sector / alignment probe & validation tests. These cover the 4K-native plumbing:
        // (a) GetSectorSize uses NativeDevice_ProbeAlignment in the ctor;
        // (b) Initialize cross-checks the C# probe against the kernel's authoritative value;
        // (c) ReadAsync/WriteAsync reject misaligned offsets/lengths/buffers with a clean
        //     TsavoriteException instead of letting the kernel return cryptic EINVAL.
        // ---------------------------------------------------------------------------------

        [Test]
        [Category("NativeStorageDevice")]
        public void NativeStorageDevice_SectorSize_IsPowerOfTwoAtLeast512()
        {
            // The probe path runs in the ctor (via base(..., GetSectorSize(filename), ...)).
            // Whatever it returns must be a usable sector size: a power of two and at least
            // the historical 512 floor. Tsavorite's allocators rely on power-of-two masking.
            using var device = new NativeStorageDevice(Path.Combine(TestUtils.MethodTestDir, "test.log"), deleteOnClose: true);
            uint s = device.SectorSize;
            Assert.That(s, Is.GreaterThanOrEqualTo(512u), "SectorSize must be >= 512 (the floor).");
            Assert.That((s & (s - 1)), Is.EqualTo(0u), $"SectorSize must be a power of two; got {s}.");
        }

        [Test]
        [Category("NativeStorageDevice")]
        public void NativeStorageDevice_SectorSize_IsStableAcrossOpens()
        {
            // The probe is filesystem-level — two devices opened on files in the same dir
            // must agree on the sector size. If they don't, the probe is non-deterministic
            // and the cross-check in Initialize would fire spuriously in production.
            using var d1 = new NativeStorageDevice(Path.Combine(TestUtils.MethodTestDir, "a.log"), deleteOnClose: true);
            using var d2 = new NativeStorageDevice(Path.Combine(TestUtils.MethodTestDir, "b.log"), deleteOnClose: true);
            Assert.That(d2.SectorSize, Is.EqualTo(d1.SectorSize));
        }

        [Test]
        [Category("NativeStorageDevice")]
        public void DeviceSectorSize_IsConsistentAcrossDeviceTypes()
        {
            // All local-disk device types must report the same required DIO alignment from the
            // shared probe. Linux-only: NativeStorageDevice is the Linux native backend.
            if (!OperatingSystem.IsLinux())
                Assert.Ignore("NativeStorageDevice vs RandomAccess comparison is Linux-specific.");

            // Both devices probe the same directory (same filesystem => same alignment).
            using var native = new NativeStorageDevice(Path.Combine(TestUtils.MethodTestDir, "native.log"), deleteOnClose: true);
            using var randomAccess = new RandomAccessLocalStorageDevice(Path.Combine(TestUtils.MethodTestDir, "ra.log"), deleteOnClose: true);

            uint s = native.SectorSize;
            Assert.That(s, Is.GreaterThanOrEqualTo(512u), "SectorSize must be >= 512 (the floor).");
            Assert.That((s & (s - 1)), Is.EqualTo(0u), $"SectorSize must be a power of two; got {s}.");
            Assert.That(randomAccess.SectorSize, Is.EqualTo(s),
                "RandomAccessLocalStorageDevice must report the same required DIO alignment as NativeStorageDevice on the same filesystem.");
        }

        [Test]
        [Category("NativeStorageDevice")]
        public unsafe void NativeStorageDevice_UnalignedOffset_ReadAsync_Throws()
        {
            // Issue a read with an offset that's not a multiple of SectorSize. The libaio path
            // would return -EINVAL via the callback (or assert in debug builds); the managed
            // guard must surface this synchronously with a clean TsavoriteException that names the
            // misaligned input.
            var path = Path.Combine(TestUtils.MethodTestDir, "test.log");
            using var device = new NativeStorageDevice(path, deleteOnClose: true);
            device.Initialize(1L << 30);
            uint sector = device.SectorSize;
            uint length = sector * 4;
            var (buf, ptr) = AllocateAlignedBuffer((int)length, _ => 0);
            try
            {
                ulong unaligned = sector - 1; // smaller than sector, definitely misaligned
                Assert.Throws<TsavoriteException>(() => device.ReadAsync(0, unaligned, ptr, length, (_, _, _, _) => { }, null));
            }
            finally { GC.KeepAlive(buf); }
        }

        [Test]
        [Category("NativeStorageDevice")]
        public unsafe void NativeStorageDevice_UnalignedLength_WriteAsync_Throws()
        {
            var path = Path.Combine(TestUtils.MethodTestDir, "test.log");
            using var device = new NativeStorageDevice(path, deleteOnClose: true);
            device.Initialize(1L << 30);
            uint sector = device.SectorSize;
            // Buffer big enough that we can pretend to write a non-multiple length.
            var (buf, ptr) = AllocateAlignedBuffer((int)(sector * 4), i => (byte)i);
            try
            {
                uint bad = sector + 1; // not a multiple of sector
                Assert.Throws<TsavoriteException>(() => device.WriteAsync(ptr, 0, 0, bad, (_, _, _, _) => { }, null));
            }
            finally { GC.KeepAlive(buf); }
        }

        [Test]
        [Category("NativeStorageDevice")]
        public unsafe void NativeStorageDevice_UnalignedBuffer_WriteAsync_Throws()
        {
            var path = Path.Combine(TestUtils.MethodTestDir, "test.log");
            using var device = new NativeStorageDevice(path, deleteOnClose: true);
            device.Initialize(1L << 30);
            uint sector = device.SectorSize;
            var (buf, ptr) = AllocateAlignedBuffer((int)(sector * 4), _ => 0);
            try
            {
                IntPtr misalignedPtr = ptr + 1; // misaligned buffer pointer
                Assert.Throws<TsavoriteException>(() => device.WriteAsync(misalignedPtr, 0, 0, sector, (_, _, _, _) => { }, null));
            }
            finally { GC.KeepAlive(buf); }
        }

        [Test]
        [TestCase(DeviceKind.Native)]
        [TestCase(DeviceKind.RandomAccess)]
        [TestCase(DeviceKind.ManagedLocal)]
        [Category("IDevice")]
        public unsafe void IDevice_PermissionDeniedAtFirstWrite_CallbackGetsError(DeviceKind kind)
        {
            // Devices open segment files lazily on first I/O. When that open() fails (e.g.
            // a chmod-0 parent directory), the device MUST signal the failure to the caller
            // via the completion callback's errorCode — never swallow it, never hang, and
            // never throw synchronously to the user thread. All three local-storage device
            // implementations catch the open exception in their worker and route it through
            // the same callback contract.
            //
            // Root-skip: chmod has no effect on root, which would produce a false negative.
            // Linux-only: chmod-based permission denial is a POSIX construct.
            if (Environment.UserName == "root") Assert.Ignore("Running as root bypasses POSIX permission checks.");
            if (!OperatingSystem.IsLinux()) Assert.Ignore("chmod-based permission test is Linux-only.");

            var dir = TestUtils.MethodTestDir;
            Directory.CreateDirectory(dir);
            var path = Path.Combine(dir, "test.log");

            // Construct + Initialize BEFORE chmod: the ctor / Initialize may need to probe
            // alignment or open the directory, both of which need at least +x.
            using var device = CreateDeviceForTest(kind, path, 1L << 30);

            // Robust permission-restore guard: even if assertions throw, we restore 0755 so
            // TearDown can wipe the directory. AppDomain.UnhandledException as last resort.
            uint sector = device.SectorSize;
            var (buf, ptr) = AllocateAlignedBuffer((int)sector, _ => 0xAB);
            if (chmod(dir, 0) != 0) Assert.Ignore("chmod failed; cannot run permission test.");
            try
            {
                uint observedError = 0;
                using var done = new SemaphoreSlim(0);

                device.WriteAsync(ptr, 0, 0, sector, (errorCode, _, _, _) =>
                {
                    observedError = errorCode;
                    done.Release();
                }, null);

                Assert.That(done.Wait(TimeSpan.FromSeconds(10)), Is.True, $"{kind}: write callback did not fire within 10s.");
                Assert.That(observedError, Is.Not.EqualTo(0u), $"{kind}: write to chmod-0 dir should produce a non-zero errorCode via callback.");
            }
            finally
            {
                // ALWAYS restore permissions before we leave, even on assertion failure.
                // 0x1ED = 0755. If this fails we can't help TearDown; log and move on.
                _ = chmod(dir, 0x1ED);
                GC.KeepAlive(buf);
            }
        }

        // ===================================================================================
        // Phase 8 — submit-path / backend / sharding / throttle / late-entry-point coverage.
        //
        // Targets the code paths added or rewritten for the high-concurrency hang fix, which the
        // Phase-7 suite above does not exercise:
        //   - the io_uring submission backend (io_uring_submit count handling, get_sqe/submit
        //     unwind) — previously NOT covered by any unit test;
        //   - multiple completion threads / io_contexts (pick_context / pick_ring sharding);
        //   - multiple devices interleaved across threads (the pick_context/pick_ring owner+bounds
        //     thread-local guard, whose absence is an out-of-bounds shard index);
        //   - ThrottleLimit -> aggregate in-flight cap (capped at io-contexts * queue-depth);
        //   - the late P/Invoke entry points GetFileSize / RemoveSegment / Reset / TryComplete.
        //
        // io_uring cases self-skip when the loaded native library / kernel lacks a working io_uring.
        // ===================================================================================

        public enum NativeBackend { Default, Libaio, Uring }

        // One-time probe: does the loaded native library AND the host kernel support a real io_uring
        // submission? (GetAvailableBackends reports only library capability, not kernel support, so
        // we also drive one real submit.) Cached so the probe runs at most once per test process.
        static readonly bool s_uringWorks = ProbeUringWorks();

        static unsafe bool ProbeUringWorks()
        {
            if (!OperatingSystem.IsLinux())
                return false;
            try
            {
                if (!NativeStorageDevice.GetAvailableBackends().uringAvailable)
                    return false;
                var path = Path.Combine(Path.GetTempPath(), $"uring_probe_{Guid.NewGuid():N}.log");
                using var d = new NativeStorageDevice(path, deleteOnClose: true, ioBackend: NativeStorageDevice.IoBackend.Uring);
                d.Initialize(1L << 30);
                const int sz = 4096;
                var buf = GC.AllocateArray<byte>(sz + sz, pinned: true);
                var ptr = (IntPtr)(((long)Unsafe.AsPointer(ref buf[0]) + (sz - 1)) & ~(sz - 1));
                using var sem = new SemaphoreSlim(0);
                uint err = 0;
                d.WriteAsync(ptr, 0, 0, sz, (e, _, _, _) => { err = e; sem.Release(); }, null);
                bool ok = sem.Wait(TimeSpan.FromSeconds(5)) && err == 0;
                GC.KeepAlive(buf);
                return ok;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Build + Initialize a Native device on the requested backend, completion-thread count and
        /// throttle limit. <paramref name="numIoContexts"/> (kernel io_context / io_uring ring count,
        /// decoupled from the drainer count), <paramref name="queueDepth"/> (per-ring submission depth)
        /// and <paramref name="uringSqPoll"/> (io_uring SQPOLL) exercise the corresponding ctor knobs;
        /// each 0/false selects the device default. Self-skips on non-Linux, or when io_uring is
        /// requested but unavailable.
        /// </summary>
        static NativeStorageDevice CreateNativeForTest(string path, long segmentSize, NativeBackend backend,
                                                       int completionThreads = 1, int throttleLimit = 0, bool omitSegmentId = false,
                                                       int numIoContexts = 0, int queueDepth = 0, bool uringSqPoll = false)
        {
            if (!OperatingSystem.IsLinux())
                Assert.Ignore("NativeStorageDevice is Linux-only.");
            if (backend == NativeBackend.Uring && !s_uringWorks)
                Assert.Ignore("io_uring backend is not available/working on this host.");

            var io = backend switch
            {
                NativeBackend.Libaio => NativeStorageDevice.IoBackend.Libaio,
                NativeBackend.Uring => NativeStorageDevice.IoBackend.Uring,
                _ => NativeStorageDevice.IoBackend.Default,
            };
            var d = new NativeStorageDevice(path, deleteOnClose: true, numCompletionThreads: completionThreads, ioBackend: io,
                                            numIoContexts: numIoContexts, queueDepth: queueDepth, uringSqPoll: uringSqPoll);
            if (throttleLimit > 0)
                d.ThrottleLimit = throttleLimit;
            d.Initialize(segmentSize, omitSegmentIdFromFilename: omitSegmentId && segmentSize == -1L);
            return d;
        }

        // ----- backend round-trip / parallel (covers the io_uring submit path) -------------

        [Test]
        [TestCase(NativeBackend.Default)]
        [TestCase(NativeBackend.Libaio)]
        [TestCase(NativeBackend.Uring)]
        [Category("IDevice")]
        public unsafe void Native_Backend_RoundTrip(NativeBackend backend)
        {
            using var device = CreateNativeForTest(Path.Join(TestUtils.MethodTestDir, "test.log"), 64 * Mib, backend);
            const int size = 8 * 1024;
            var (wbuf, wptr) = AllocateAlignedBuffer(size, j => (byte)((j * 7 + 3) & 0xFF));
            device.WriteAsync(wptr, 0, 0, (uint)size, IOCallback, null);
            semaphore.Wait();

            var (rbuf, rptr) = AllocateAlignedBuffer(size, _ => 0);
            device.ReadAsync(0, 0, rptr, (uint)size, IOCallback, null);
            semaphore.Wait();

            AssertBufferContents(rptr, size, j => (byte)((j * 7 + 3) & 0xFF), $"{backend} round-trip");
            GC.KeepAlive(wbuf); GC.KeepAlive(rbuf);
        }

        [Test]
        [TestCase(NativeBackend.Libaio)]
        [TestCase(NativeBackend.Uring)]
        [Category("IDevice")]
        public unsafe void Native_Backend_Parallel_64Reads(NativeBackend backend)
        {
            const int N = 64, size = 4 * 1024;
            using var device = CreateNativeForTest(Path.Join(TestUtils.MethodTestDir, "test.log"), 64 * Mib, backend);

            var (wbuf, wptr) = AllocateAlignedBuffer(N * size, j => (byte)(((j / size) * 31 + (j % size)) & 0xFF));
            device.WriteAsync(wptr, 0, 0, (uint)(N * size), IOCallback, null);
            semaphore.Wait();

            var rbufs = new byte[N][];
            var rptrs = new IntPtr[N];
            for (int i = 0; i < N; i++)
            {
                var (rb, rp) = AllocateAlignedBuffer(size, _ => 0);
                rbufs[i] = rb; rptrs[i] = rp;
                device.ReadAsync(0, (ulong)(i * size), rp, (uint)size, IOCallback, null);
            }
            for (int i = 0; i < N; i++) semaphore.Wait();

            for (int i = 0; i < N; i++)
            {
                int blk = i;
                AssertBufferContents(rptrs[i], size, off => (byte)((blk * 31 + off) & 0xFF), $"{backend} block {blk}");
            }
            GC.KeepAlive(wbuf); GC.KeepAlive(rbufs);
        }

        // ----- multiple completion threads / io_contexts (pick_context / pick_ring sharding) -----

        [Test]
        [TestCase(NativeBackend.Libaio)]
        [TestCase(NativeBackend.Uring)]
        [Category("IDevice")]
        public unsafe void Native_MultiCompletionThreads_ParallelMixed(NativeBackend backend)
        {
            const int N = 48, size = 4 * 1024;
            const ulong readBase = 0, writeBase = 1UL << 20;
            // 4 completion threads => 4 io_contexts/rings => exercises pick_context/pick_ring sharding.
            using var device = CreateNativeForTest(Path.Join(TestUtils.MethodTestDir, "test.log"), 64 * Mib, backend, completionThreads: 4);

            var (prewbuf, prewptr) = AllocateAlignedBuffer(N * size, j => (byte)((j * 5) & 0xFF));
            device.WriteAsync(prewptr, 0, readBase, (uint)(N * size), IOCallback, null);
            semaphore.Wait();

            var rroots = new byte[N][];
            var rptrs = new IntPtr[N];
            var wroots = new byte[N][];
            for (int i = 0; i < N; i++)
            {
                int id = i;
                var (rb, rp) = AllocateAlignedBuffer(size, _ => 0);
                rroots[i] = rb; rptrs[i] = rp;
                device.ReadAsync(0, readBase + (ulong)(id * size), rp, (uint)size, IOCallback, null);

                var (wb, wp) = AllocateAlignedBuffer(size, j => (byte)((j + id) & 0xFF));
                wroots[i] = wb;
                device.WriteAsync(wp, 0, writeBase + (ulong)(id * size), (uint)size, IOCallback, null);
            }
            for (int i = 0; i < 2 * N; i++) semaphore.Wait();

            for (int i = 0; i < N; i++)
            {
                int baseIdx = i * size;
                AssertBufferContents(rptrs[i], size, off => (byte)(((baseIdx + off) * 5) & 0xFF), $"{backend} read {i}");
            }
            GC.KeepAlive(prewbuf); GC.KeepAlive(rroots); GC.KeepAlive(wroots);
        }

        // ----- multiple devices with DIFFERENT shard counts, driven from many threads -----------
        // Regression test for the pick_context / pick_ring thread-local owner+bounds guard: a thread
        // that was assigned a high shard index on a device with many contexts must NOT reuse that
        // index against a different device with fewer contexts (which would be an out-of-bounds read
        // of that device's ring/context array -> crash/corruption).

        [Test]
        [TestCase(NativeBackend.Libaio)]
        [TestCase(NativeBackend.Uring)]
        [Category("IDevice")]
        public unsafe void Native_MultipleDevices_MultiThread_NoCrossShardIndexReuse(NativeBackend backend)
        {
            const int threads = 8, perThread = 24, size = 4 * 1024;
            // Device A has 4 shards, device B has 2. Threads land on A-shards 0..3; those on shard
            // 2 or 3 then submit to B, exercising the per-device shard reassignment.
            using var a = CreateNativeForTest(Path.Join(TestUtils.MethodTestDir, "a.log"), 64 * Mib, backend, completionThreads: 4);
            using var b = CreateNativeForTest(Path.Join(TestUtils.MethodTestDir, "b.log"), 64 * Mib, backend, completionThreads: 2);

            var (wa, wpa) = AllocateAlignedBuffer(size, j => (byte)(j & 0xFF));
            var (wb, wpb) = AllocateAlignedBuffer(size, j => (byte)((j ^ 0x5A) & 0xFF));
            using var done = new SemaphoreSlim(0);
            int errors = 0;
            void Cb(uint e, uint n, object c, Exception ioException) { if (e != 0) Interlocked.Increment(ref errors); done.Release(); }

            var ts = new Thread[threads];
            for (int t = 0; t < threads; t++)
            {
                int tid = t;
                ts[t] = new Thread(() =>
                {
                    for (int i = 0; i < perThread; i++)
                    {
                        ulong addr = (ulong)((tid * perThread + i) * size);
                        a.WriteAsync(wpa, 0, addr, (uint)size, Cb, null);
                        b.WriteAsync(wpb, 0, addr, (uint)size, Cb, null);
                    }
                });
                ts[t].Start();
            }
            foreach (var th in ts) th.Join();
            for (int i = 0; i < threads * perThread * 2; i++)
                Assert.That(done.Wait(TimeSpan.FromSeconds(30)), Is.True, $"{backend}: completion timed out (possible OOB/hang in shard selection).");
            ClassicAssert.AreEqual(0, errors, $"{backend}: write completions reported errors.");

            // Spot-check correctness on both devices.
            var (ra, rpa) = AllocateAlignedBuffer(size, _ => 0);
            a.ReadAsync(0, (ulong)(10 * size), rpa, (uint)size, IOCallback, null); semaphore.Wait();
            AssertBufferContents(rpa, size, j => (byte)(j & 0xFF), $"{backend} device A");
            var (rb, rpb) = AllocateAlignedBuffer(size, _ => 0);
            b.ReadAsync(0, (ulong)(10 * size), rpb, (uint)size, IOCallback, null); semaphore.Wait();
            AssertBufferContents(rpb, size, j => (byte)((j ^ 0x5A) & 0xFF), $"{backend} device B");
            GC.KeepAlive(wa); GC.KeepAlive(wb); GC.KeepAlive(ra); GC.KeepAlive(rb);
        }

        // ----- ThrottleLimit -> submission-ring sizing ----------------------------------------

        [Test]
        [TestCase(NativeBackend.Libaio)]
        [TestCase(NativeBackend.Uring)]
        [Category("IDevice")]
        public unsafe void Native_HighThrottle_HighInFlight(NativeBackend backend)
        {
            // ThrottleLimit 512 sizes the kernel submission ring to 512 (vs the 128 default), and we
            // fire 256 concurrent reads (well past 128) to drive the throttle-sized ring + submit path.
            const int N = 256, size = 4 * 1024;
            using var device = CreateNativeForTest(Path.Join(TestUtils.MethodTestDir, "test.log"), 256 * Mib, backend, completionThreads: 2, throttleLimit: 512);

            var (wbuf, wptr) = AllocateAlignedBuffer(N * size, j => (byte)(((j / size) + (j % size)) & 0xFF));
            device.WriteAsync(wptr, 0, 0, (uint)(N * size), IOCallback, null);
            semaphore.Wait();

            var rbufs = new byte[N][];
            var rptrs = new IntPtr[N];
            for (int i = 0; i < N; i++)
            {
                var (rb, rp) = AllocateAlignedBuffer(size, _ => 0);
                rbufs[i] = rb; rptrs[i] = rp;
                device.ReadAsync(0, (ulong)(i * size), rp, (uint)size, IOCallback, null);
            }
            for (int i = 0; i < N; i++) semaphore.Wait();

            for (int i = 0; i < N; i++)
            {
                int blk = i;
                AssertBufferContents(rptrs[i], size, off => (byte)((blk + off) & 0xFF), $"{backend} block {blk}");
            }
            GC.KeepAlive(wbuf); GC.KeepAlive(rbufs);
        }

        [Test]
        [TestCase(NativeBackend.Libaio)]
        [Category("IDevice")]
        public unsafe void Native_ThrottleAboveMax_IsClampedAndStillWorks(NativeBackend backend)
        {
            // A throttle far above the kernel capacity (io-contexts * queue-depth) is capped (and warned);
            // the device must still round-trip correctly.
            using var device = CreateNativeForTest(Path.Join(TestUtils.MethodTestDir, "test.log"), 64 * Mib, backend, throttleLimit: 100_000);
            const int size = 4 * 1024;
            var (wbuf, wptr) = AllocateAlignedBuffer(size, j => (byte)((j + 9) & 0xFF));
            device.WriteAsync(wptr, 0, 0, (uint)size, IOCallback, null);
            semaphore.Wait();
            var (rbuf, rptr) = AllocateAlignedBuffer(size, _ => 0);
            device.ReadAsync(0, 0, rptr, (uint)size, IOCallback, null);
            semaphore.Wait();
            AssertBufferContents(rptr, size, j => (byte)((j + 9) & 0xFF), $"{backend} clamped-throttle round-trip");
            GC.KeepAlive(wbuf); GC.KeepAlive(rbuf);
        }

        // ----- high submitter concurrency (direct regression for the original production hang) -----
        // The reported failure: a Garnet server's growing thread pool produced enough concurrent
        // submitters (each holding a FASTER epoch + thread-id slot while spinning on a full kernel
        // ring) to exhaust the slot table, which then threw across the P/Invoke boundary and wedged
        // the server. This test drives many concurrent submitter threads against a throttle-sized
        // ring so that: (a) slot acquire/release churns far more than the table has slots, exercising
        // the never-throw yield-spin reuse; (b) throttle == ring (512) means submits never EAGAIN-spin
        // in-epoch. A regression (slot exhaustion or an in-epoch full-ring spin) reappears as a hang,
        // which the per-operation completion timeout converts into a deterministic test failure.

        [Test]
        [TestCase(NativeBackend.Libaio)]
        [TestCase(NativeBackend.Uring)]
        [Category("IDevice")]
        public unsafe void Native_HighConcurrency_ManyThreads_NoHang(NativeBackend backend)
        {
            const int threads = 64, perThread = 32, size = 4 * 1024;
            const int total = threads * perThread;     // 2048 blocks * 4 KiB = 8 MiB (fits one segment)
            using var device = CreateNativeForTest(Path.Join(TestUtils.MethodTestDir, "test.log"), 256 * Mib, backend, completionThreads: 4, throttleLimit: 512);

            // Seed a contiguous, position-derived region so every read targets verifiable data.
            var (wbuf, wptr) = AllocateAlignedBuffer(total * size, j => (byte)(((j / size) * 13 + (j % size)) & 0xFF));
            device.WriteAsync(wptr, 0, 0, (uint)(total * size), IOCallback, null);
            semaphore.Wait();

            var rroots = new byte[total][];
            var rptrs = new IntPtr[total];
            for (int i = 0; i < total; i++)
            {
                var (rb, rp) = AllocateAlignedBuffer(size, _ => 0);
                rroots[i] = rb; rptrs[i] = rp;
            }

            using var done = new SemaphoreSlim(0);
            int errors = 0;
            void Cb(uint e, uint n, object c, Exception ioException) { if (e != 0) Interlocked.Increment(ref errors); done.Release(); }

            var ts = new Thread[threads];
            for (int t = 0; t < threads; t++)
            {
                int tid = t;
                ts[t] = new Thread(() =>
                {
                    for (int i = 0; i < perThread; i++)
                    {
                        int idx = tid * perThread + i;
                        device.ReadAsync(0, (ulong)(idx * size), rptrs[idx], (uint)size, Cb, null);
                    }
                });
                ts[t].Start();
            }
            foreach (var th in ts) th.Join();

            for (int i = 0; i < total; i++)
                Assert.That(done.Wait(TimeSpan.FromSeconds(30)), Is.True,
                    $"{backend}: completion {i}/{total} timed out — possible slot-exhaustion/in-epoch-spin hang regression.");
            ClassicAssert.AreEqual(0, errors, $"{backend}: reads reported errors under high concurrency.");

            // Verify a representative sample (every 37th block) for correctness.
            for (int i = 0; i < total; i += 37)
            {
                int blk = i;
                AssertBufferContents(rptrs[i], size, off => (byte)((blk * 13 + off) & 0xFF), $"{backend} block {blk}");
            }
            GC.KeepAlive(wbuf); GC.KeepAlive(rroots);
        }

        // ----- io_context / ring-count / queue-depth / SQPOLL ctor knobs --------------------------

        /// <summary>
        /// Issues one read per buffer, striped across <paramref name="submitThreads"/> background threads,
        /// and returns once every read has been submitted (completions are awaited by the caller).
        /// Native rings/io_contexts are picked per submitting thread, so a single-threaded issue loop puts
        /// every read on one ring; fanning out is what actually exercises a multi-ring configuration.
        /// </summary>
        void SubmitReadsFromThreads(IDevice device, IntPtr[] rptrs, int size, int submitThreads)
        {
            var submitters = new Thread[submitThreads];
            for (int t = 0; t < submitThreads; t++)
            {
                int start = t;
                submitters[t] = new Thread(() =>
                {
                    for (int i = start; i < rptrs.Length; i += submitThreads)
                        device.ReadAsync(0, (ulong)((long)i * size), rptrs[i], (uint)size, IOCallback, null);
                })
                { IsBackground = true };
                submitters[t].Start();
            }
            foreach (var s in submitters) s.Join();
        }

        [Test]
        [TestCase(NativeBackend.Libaio)]
        [TestCase(NativeBackend.Uring)]
        [Category("IDevice")]
        public unsafe void Native_ExplicitIoContexts_DefaultDepth_MultiRing(NativeBackend backend)
        {
            // Decouple ring count from drainer count: 8 io_contexts served by 2 drainers, default
            // (auto-derived) queue depth. For libaio + default queueDepth this also exercises the
            // ResolveLibaioReservationDepth branch (the reservation is sized from the throttle share
            // rather than the io_uring per-ring ceiling). Rings are assigned per submitting thread
            // (thread-affine), so the reads are issued from 8 concurrent threads to actually fan out
            // across all 8 rings rather than piling onto the single test thread's ring.
            const int N = 128, size = 4 * 1024, submitThreads = 8;
            using var device = CreateNativeForTest(Path.Join(TestUtils.MethodTestDir, "test.log"), 256 * Mib, backend,
                                                   completionThreads: 2, throttleLimit: 1024, numIoContexts: 8);

            var (wbuf, wptr) = AllocateAlignedBuffer(N * size, j => (byte)(((j / size) * 5 + (j % size)) & 0xFF));
            device.WriteAsync(wptr, 0, 0, (uint)(N * size), IOCallback, null);
            semaphore.Wait();

            var rbufs = new byte[N][];
            var rptrs = new IntPtr[N];
            for (int i = 0; i < N; i++)
            {
                var (rb, rp) = AllocateAlignedBuffer(size, _ => 0);
                rbufs[i] = rb; rptrs[i] = rp;
            }

            SubmitReadsFromThreads(device, rptrs, size, submitThreads);
            for (int i = 0; i < N; i++) semaphore.Wait();

            for (int i = 0; i < N; i++)
            {
                int blk = i;
                AssertBufferContents(rptrs[i], size, off => (byte)((blk * 5 + off) & 0xFF), $"{backend} block {blk}");
            }
            GC.KeepAlive(wbuf); GC.KeepAlive(rbufs);
        }

        [Test]
        [TestCase(NativeBackend.Libaio)]
        [TestCase(NativeBackend.Uring)]
        [Category("IDevice")]
        public unsafe void Native_ExplicitQueueDepth_RoundTrips(NativeBackend backend)
        {
            // Explicit shallow per-ring queue depth (64) across 4 rings, and fire more concurrent reads
            // (256) than the aggregate ring capacity so the native ring-full backpressure (submit unwinds
            // to Pending and retries after a completion) is exercised and every read still completes.
            // Reads are issued from 4 concurrent threads so all 4 shallow rings are driven at once.
            const int N = 256, size = 4 * 1024, submitThreads = 4;
            using var device = CreateNativeForTest(Path.Join(TestUtils.MethodTestDir, "test.log"), 256 * Mib, backend,
                                                   completionThreads: 2, throttleLimit: 256, numIoContexts: 4, queueDepth: 64);

            var (wbuf, wptr) = AllocateAlignedBuffer(N * size, j => (byte)(((j / size) * 3 + (j % size)) & 0xFF));
            device.WriteAsync(wptr, 0, 0, (uint)(N * size), IOCallback, null);
            semaphore.Wait();

            var rbufs = new byte[N][];
            var rptrs = new IntPtr[N];
            for (int i = 0; i < N; i++)
            {
                var (rb, rp) = AllocateAlignedBuffer(size, _ => 0);
                rbufs[i] = rb; rptrs[i] = rp;
            }

            SubmitReadsFromThreads(device, rptrs, size, submitThreads);
            for (int i = 0; i < N; i++) semaphore.Wait();

            for (int i = 0; i < N; i++)
            {
                int blk = i;
                AssertBufferContents(rptrs[i], size, off => (byte)((blk * 3 + off) & 0xFF), $"{backend} block {blk}");
            }
            GC.KeepAlive(wbuf); GC.KeepAlive(rbufs);
        }

        [Test]
        [Category("IDevice")]
        public unsafe void Native_Uring_SqPoll_RoundTrips()
        {
            // io_uring SQPOLL is Uring-only (libaio/Windows ignore it). Each ring gets its own kernel
            // poll thread, so the reads are issued from 4 concurrent threads to drive all 4 rings (and
            // their poll threads) through the SQPOLL submit branch.
            const int N = 32, size = 4 * 1024, submitThreads = 4;
            using var device = CreateNativeForTest(Path.Join(TestUtils.MethodTestDir, "test.log"), 128 * Mib, NativeBackend.Uring,
                                                   completionThreads: 2, throttleLimit: 512, numIoContexts: 4, uringSqPoll: true);

            var (wbuf, wptr) = AllocateAlignedBuffer(N * size, j => (byte)(((j / size) * 9 + (j % size)) & 0xFF));
            device.WriteAsync(wptr, 0, 0, (uint)(N * size), IOCallback, null);
            semaphore.Wait();

            var rbufs = new byte[N][];
            var rptrs = new IntPtr[N];
            for (int i = 0; i < N; i++)
            {
                var (rb, rp) = AllocateAlignedBuffer(size, _ => 0);
                rbufs[i] = rb; rptrs[i] = rp;
            }

            SubmitReadsFromThreads(device, rptrs, size, submitThreads);
            for (int i = 0; i < N; i++) semaphore.Wait();

            for (int i = 0; i < N; i++)
            {
                int blk = i;
                AssertBufferContents(rptrs[i], size, off => (byte)((blk * 9 + off) & 0xFF), $"Uring-SQPOLL block {blk}");
            }
            GC.KeepAlive(wbuf); GC.KeepAlive(rbufs);
        }

        // ----- late P/Invoke entry points (GetFileSize / Reset / TryComplete / RemoveSegment) -----

        [Test]
        [Category("IDevice")]
        public unsafe void Native_GetFileSize_ReflectsWrites()
        {
            using var device = CreateNativeForTest(Path.Join(TestUtils.MethodTestDir, "test.log"), 64 * Mib, NativeBackend.Default);
            // No data written yet: segment 0 reports size 0 (file not yet created).
            ClassicAssert.AreEqual(0L, device.GetFileSize(0));

            const int size = 16 * 1024;
            var (wbuf, wptr) = AllocateAlignedBuffer(size, _ => 0xCD);
            device.WriteAsync(wptr, 0, 0, (uint)size, IOCallback, null);
            semaphore.Wait();

            ClassicAssert.GreaterOrEqual(device.GetFileSize(0), (long)size, "GetFileSize should reflect the written segment.");
            GC.KeepAlive(wbuf);
        }

        [Test]
        [Category("IDevice")]
        public unsafe void Native_Reset_ClosesSegments_DeviceRemainsUsable()
        {
            using var device = CreateNativeForTest(Path.Join(TestUtils.MethodTestDir, "test.log"), 64 * Mib, NativeBackend.Default);
            const int size = 4 * 1024;
            var (wbuf, wptr) = AllocateAlignedBuffer(size, _ => 0xEE);
            device.WriteAsync(wptr, 0, 0, (uint)size, IOCallback, null);
            semaphore.Wait();

            // Reset closes the open segment handles; the device must lazily reopen on the next IO.
            ClassicAssert.DoesNotThrow(() => device.Reset());

            var (rbuf, rptr) = AllocateAlignedBuffer(size, _ => 0);
            device.ReadAsync(0, 0, rptr, (uint)size, IOCallback, null);
            semaphore.Wait();
            AssertBufferContents(rptr, size, _ => 0xEE, "post-Reset read");
            GC.KeepAlive(wbuf); GC.KeepAlive(rbuf);
        }

        [Test]
        [Category("IDevice")]
        public unsafe void Native_TryComplete_DoesNotThrow_BeforeAndAfterIO()
        {
            using var device = CreateNativeForTest(Path.Join(TestUtils.MethodTestDir, "test.log"), 64 * Mib, NativeBackend.Default);
            // Before the native handle is even created.
            ClassicAssert.DoesNotThrow(() => device.TryComplete());

            const int size = 4 * 1024;
            var (wbuf, wptr) = AllocateAlignedBuffer(size, _ => 0x11);
            device.WriteAsync(wptr, 0, 0, (uint)size, IOCallback, null);
            semaphore.Wait();

            ClassicAssert.DoesNotThrow(() => device.TryComplete());
            GC.KeepAlive(wbuf);
        }

        /// <summary>
        /// TryComplete and TryCompleteMine return false once every IO has completed. The native functions return a one-byte C++
        /// bool, and read as a four-byte value, its undefined upper bytes can turn false into true.
        /// </summary>
        [Test]
        [Category("IDevice")]
        public unsafe void Native_TryComplete_IsFalseWithNothingToComplete()
        {
            using var device = CreateDeviceForTest(DeviceKind.Native, Path.Join(TestUtils.MethodTestDir, "test.log"), 64 * Mib);
            const int size = 4 * 1024;
            var (wbuf, wptr) = AllocateAlignedBuffer(size, _ => 0x22);
            device.WriteAsync(wptr, 0, 0, (uint)size, IOCallback, null);
            semaphore.Wait();

            // The write's completion has run, so nothing is left to complete. Repeated, since the undefined bytes are only
            // sometimes non-zero.
            for (var i = 0; i < 100; i++)
            {
                ClassicAssert.IsFalse(device.TryComplete(), nameof(device.TryComplete));
                ClassicAssert.IsFalse(device.TryCompleteMine(), nameof(device.TryCompleteMine));
            }
            GC.KeepAlive(wbuf);
        }

        [Test]
        [Category("IDevice")]
        public unsafe void Native_RemoveSegment_RemovesPersistedData()
        {
            // Multi-segment device (segment files <base>.<id>); write segment 1, remove it, confirm gone.
            const long segmentSize = 64 * Mib;
            const int size = 4 * 1024;
            using var device = CreateNativeForTest(Path.Join(TestUtils.MethodTestDir, "test.log"), segmentSize, NativeBackend.Default);

            // Write into segment 1 (segmentId selects the <base>.1 file; the address is the
            // within-segment offset).
            var (wbuf, wptr) = AllocateAlignedBuffer(size, _ => 0x7C);
            device.WriteAsync(wptr, 1, 0, (uint)size, IOCallback, null);
            semaphore.Wait();
            ClassicAssert.GreaterOrEqual(device.GetFileSize(1), (long)size, "segment 1 should exist after write.");

            // RemoveSegment closes and deletes the segment, the lowest the device holds open. Querying it afterwards must not
            // crash and must report empty — this is the exact path (size() -> OpenSegment bundle-expand -> BumpCurrentEpoch) that
            // faulted when GetFileSize ran without epoch protection.
            device.RemoveSegment(1);
            ClassicAssert.AreEqual(0L, device.GetFileSize(1), "segment 1 should report empty after RemoveSegment.");
            GC.KeepAlive(wbuf);
        }

        /// <summary>Read a segment file that a device may hold open</summary>
        static byte[] ReadSegmentFile(string file)
        {
            using var stream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            var bytes = new byte[stream.Length];
            stream.ReadExactly(bytes);
            return bytes;
        }

        /// <summary>Assert that each segment's file begins with the sector that <paramref name="pattern"/> describes</summary>
        static void AssertSegmentFiles(string path, Func<int, byte> pattern, params int[] segments)
        {
            foreach (var segment in segments)
            {
                var file = $"{path}.{segment}";
                ClassicAssert.IsTrue(File.Exists(file), $"segment {segment} should exist");
                var bytes = ReadSegmentFile(file);
                ClassicAssert.GreaterOrEqual(bytes.Length, HardeningSectorSize, $"segment {segment} should hold its data");
                for (var i = 0; i < HardeningSectorSize; i++)
                {
                    if (bytes[i] != pattern(i))
                        Assert.Fail($"segment {segment}: mismatch at byte {i}: expected 0x{pattern(i):X2}, got 0x{bytes[i]:X2}");
                }
            }
        }

        /// <summary>
        /// The native device's RemoveSegment removes only the segment named, wherever that segment lies: below or above the
        /// segments the device holds open, open above the lowest, the lowest open, or with none open. The device keeps the
        /// segments it holds open contiguous, so each case takes a different path. Every other segment keeps its data, and a
        /// segment written again after its removal keeps the new data.
        /// </summary>
        [Test]
        [Category("IDevice")]
        public unsafe void Native_RemoveSegment_RemovesOnlyThatSegment()
        {
            // The files are checked directly, since reading through the device would change which segments it holds open
            const long segmentSize = 1L << 16;
            var path = Path.Join(TestUtils.MethodTestDir, "test.log");
            string SegmentFile(int segment) => $"{path}.{segment}";
            Func<int, byte> written = i => (byte)(i * 3);
            Func<int, byte> rewritten = i => (byte)(i * 5 + 1);
            var (wbuf, wptr) = AllocateAlignedBuffer(HardeningSectorSize, written);
            var (rwbuf, rwptr) = AllocateAlignedBuffer(HardeningSectorSize, rewritten);
            var (rbuf, rptr) = AllocateAlignedBuffer(HardeningSectorSize, _ => 0);

            // Write segments 0 through 7 through one device, then remove them through another, which starts with none open
            using (var writer = CreateDeviceForTest(DeviceKind.Native, path, segmentSize, deleteOnClose: false))
            {
                for (var segment = 0; segment <= 7; segment++)
                {
                    writer.WriteAsync(wptr, segment, 0, HardeningSectorSize, IOCallback, null);
                    semaphore.Wait();
                }
            }

            using (var device = CreateDeviceForTest(DeviceKind.Native, path, segmentSize, deleteOnClose: false))
            {
                // This leaves the device holding segments 2 through 5 open
                foreach (var segment in new[] { 2, 5 })
                {
                    device.ReadAsync(segment, 0, rptr, HardeningSectorSize, IOCallback, null);
                    semaphore.Wait();
                }

                // Not open, below and above the open segments: the file is deleted
                device.RemoveSegment(0);
                ClassicAssert.IsFalse(File.Exists(SegmentFile(0)), "segment 0, below the open segments, should be deleted");
                device.RemoveSegment(7);
                ClassicAssert.IsFalse(File.Exists(SegmentFile(7)), "segment 7, above the open segments, should be deleted");
                AssertSegmentFiles(path, written, 1, 2, 3, 4, 5, 6);

                // Open above the lowest open segment: the file is emptied in place
                device.RemoveSegment(4);
                ClassicAssert.AreEqual(0L, new FileInfo(SegmentFile(4)).Length, "segment 4, open above the lowest, should be emptied");
                AssertSegmentFiles(path, written, 1, 2, 3, 5, 6);

                // The lowest open segment: the file is closed and deleted
                device.RemoveSegment(2);
                ClassicAssert.IsFalse(File.Exists(SegmentFile(2)), "segment 2, the lowest open, should be deleted");
                AssertSegmentFiles(path, written, 1, 3, 5, 6);

                // A segment written again after its removal keeps the new data: the removal waits for the deferred delete, which
                // would otherwise remove the new file or leave the old one
                device.RemoveSegment(3);
                ClassicAssert.IsFalse(File.Exists(SegmentFile(3)), "segment 3, then the lowest open, should be deleted");
                device.WriteAsync(rwptr, 3, 0, HardeningSectorSize, IOCallback, null);
                semaphore.Wait();

                // With no segment open, as after Reset: the file is deleted
                device.Reset();
                device.RemoveSegment(6);
                ClassicAssert.IsFalse(File.Exists(SegmentFile(6)), "segment 6, with no segment open, should be deleted");
            }

            // Segments 1 and 5 keep their data, and segment 3 holds what was written after its removal, also when read through a
            // new device
            AssertSegmentFiles(path, written, 1, 5);
            AssertSegmentFiles(path, rewritten, 3);
            using (var reopened = CreateDeviceForTest(DeviceKind.Native, path, segmentSize, deleteOnClose: false))
            {
                reopened.ReadAsync(3, 0, rptr, HardeningSectorSize, IOCallback, null);
                semaphore.Wait();
                AssertBufferContents(rptr, HardeningSectorSize, rewritten, "segment 3, written after it was removed");
            }
            GC.KeepAlive(wbuf);
            GC.KeepAlive(rwbuf);
            GC.KeepAlive(rbuf);
        }

        /// <summary>
        /// Reading a segment that truncation removed reopens it, and the native device then holds open every segment from there
        /// up, creating their files. The next truncation must delete those files too, which removing one segment at a time could
        /// not: the device keeps the segments it holds open contiguous, so it would only empty them.
        /// </summary>
        [Test]
        [Category("IDevice")]
        public unsafe void Native_Truncation_RemovesSegmentsReopenedBelowIt()
        {
            const long segmentSize = 1L << 16;
            var path = Path.Join(TestUtils.MethodTestDir, "test.log");
            var (wbuf, wptr) = AllocateAlignedBuffer(HardeningSectorSize, i => (byte)(i * 3));
            var (rbuf, rptr) = AllocateAlignedBuffer(HardeningSectorSize, _ => 0);
            var epoch = new LightEpoch();
            try
            {
                // Write segments 0 through 3, and truncate below segment 2
                using var device = new NativeStorageDevice(path, deleteOnClose: false);
                device.Initialize(segmentSize, epoch);
                for (var segment = 0; segment <= 3; segment++)
                {
                    device.WriteAsync(wptr, segment, 0, HardeningSectorSize, IOCallback, null);
                    semaphore.Wait();
                }

                device.TruncateUntilSegment(2);
                foreach (var segment in new[] { 0, 1 })
                    ClassicAssert.IsFalse(File.Exists($"{path}.{segment}"), $"segment {segment} should be truncated");

                // The read's outcome does not matter: segment 0's new file is empty, which older native libraries on Windows
                // report as an IO error
                device.ReadAsync(0, 0, rptr, HardeningSectorSize, (_, _, _, _) => semaphore.Release(), null);
                semaphore.Wait();
                foreach (var segment in new[] { 0, 1 })
                    ClassicAssert.IsTrue(File.Exists($"{path}.{segment}"), $"reading segment 0 should reopen segment {segment}");

                // Truncating below segment 3 deletes the reopened files too, and segment 3 keeps its data
                device.TruncateUntilSegment(3);
                for (var segment = 0; segment < 3; segment++)
                    ClassicAssert.IsFalse(File.Exists($"{path}.{segment}"), $"segment {segment} should be truncated");
                AssertSegmentFiles(path, i => (byte)(i * 3), 3);
            }
            finally
            {
                epoch.Dispose();
            }
            GC.KeepAlive(wbuf);
            GC.KeepAlive(rbuf);
        }

        // ----- Removing the segments above a segment ---------------------------------

        /// <summary>Device kinds that implement <see cref="IDevice.TryRemoveSegmentsAbove"/></summary>
        public enum SegmentRemovingDeviceKind { Native, RandomAccess, ManagedLocal, LocalStorage, LocalMemory }

        /// <summary>
        /// Create and initialize a device of the given kind over <paramref name="path"/>. LocalStorageDevice is Windows-only, so
        /// elsewhere the test is ignored.
        /// </summary>
        static IDevice CreateSegmentRemovingDeviceForTest(SegmentRemovingDeviceKind kind, string path, long segmentSize)
        {
            switch (kind)
            {
                case SegmentRemovingDeviceKind.Native:
                    return CreateDeviceForTest(DeviceKind.Native, path, segmentSize);
                case SegmentRemovingDeviceKind.RandomAccess:
                    return CreateDeviceForTest(DeviceKind.RandomAccess, path, segmentSize);
                case SegmentRemovingDeviceKind.ManagedLocal:
                    return CreateDeviceForTest(DeviceKind.ManagedLocal, path, segmentSize);
                case SegmentRemovingDeviceKind.LocalStorage when OperatingSystem.IsWindows():
                    var localStorage = new LocalStorageDevice(path, false, true, true, Devices.CAPACITY_UNSPECIFIED, false, false);
                    localStorage.Initialize(segmentSize);
                    return localStorage;
                case SegmentRemovingDeviceKind.LocalStorage:
                    Assert.Ignore("LocalStorageDevice is Windows-only.");
                    return null;
                case SegmentRemovingDeviceKind.LocalMemory:
                    var localMemory = new LocalMemoryDevice(capacity: 8 * segmentSize, segmentSize: segmentSize, parallelism: 1);
                    localMemory.Initialize(segmentSize);
                    return localMemory;
                default:
                    throw new ArgumentOutOfRangeException(nameof(kind));
            }
        }

        /// <summary>
        /// TryRemoveSegmentsAbove removes the segments above the one named and counts those that held data, while the segments at
        /// and below it keep their data and files that are not the device's segments remain. An implementation that removes each
        /// segment with RemoveSegment relies on it removing only that segment, which the RemoveSegment of older native libraries
        /// does not do, so this checks every device kind.
        /// </summary>
        [Test]
        [Category("IDevice")]
        public unsafe void IDevice_TryRemoveSegmentsAbove_KeepsTheSegmentsAtAndBelow([Values] SegmentRemovingDeviceKind kind)
        {
            const long segmentSize = 1L << 16;
            using var device = CreateSegmentRemovingDeviceForTest(kind, Path.Join(TestUtils.MethodTestDir, "test.log"), segmentSize);

            // Files that are not this device's segments: its bare name, suffixes that are not segment ids as the device writes
            // them, and another device's segment
            string[] otherFiles = kind == SegmentRemovingDeviceKind.LocalMemory ? []
                : ["test.log", "test.log.x", "test.log.007", "test.log.-1", "test.log.+2", "test.log.3.tmp", "test.logx.4", "other.log.6"];
            foreach (var name in otherFiles)
                File.WriteAllBytes(Path.Join(TestUtils.MethodTestDir, name), [1]);

            var (wbuf, wptr) = AllocateAlignedBuffer(HardeningSectorSize, i => (byte)(i * 3));
            var (rbuf, rptr) = AllocateAlignedBuffer(HardeningSectorSize, _ => 0);

            // Segments 5, 0, 1 and 2 hold data, and 3 and 4 do not. Writing 5 first leaves the native device holding 0 through 5
            // open, with empty files for 3 and 4.
            foreach (var segment in new[] { 5, 0, 1, 2 })
            {
                device.WriteAsync(wptr, segment, 0, HardeningSectorSize, IOCallback, null);
                semaphore.Wait();
            }

            // Removing above segment 1 removes 2 and 5, the two that held data, and then finds nothing more to remove
            ClassicAssert.IsTrue(device.TryRemoveSegmentsAbove(1, out var removed));
            ClassicAssert.AreEqual(2, removed, "segments 2 and 5 held data");
            ClassicAssert.IsTrue(device.TryRemoveSegmentsAbove(1, out removed));
            ClassicAssert.AreEqual(0, removed, "no segment above 1 should hold data");

            // Segments 0 and 1 keep their data, and the other files remain
            foreach (var segment in new[] { 0, 1 })
            {
                device.ReadAsync(segment, 0, rptr, HardeningSectorSize, IOCallback, null);
                semaphore.Wait();
                AssertBufferContents(rptr, HardeningSectorSize, i => (byte)(i * 3), $"{kind} segment {segment}");
            }
            foreach (var name in otherFiles)
                ClassicAssert.IsTrue(File.Exists(Path.Join(TestUtils.MethodTestDir, name)), $"{name} is not a segment of the device");
            GC.KeepAlive(wbuf);
            GC.KeepAlive(rbuf);
        }

        /// <summary>
        /// RemoveSegment removes only the segment named, on every device kind: here segment 2, between segments that keep their
        /// data
        /// </summary>
        [Test]
        [Category("IDevice")]
        public unsafe void IDevice_RemoveSegment_RemovesOnlyThatSegment([Values] SegmentRemovingDeviceKind kind)
        {
            const long segmentSize = 1L << 16;
            var path = Path.Join(TestUtils.MethodTestDir, "test.log");
            using var device = CreateSegmentRemovingDeviceForTest(kind, path, segmentSize);
            var (wbuf, wptr) = AllocateAlignedBuffer(HardeningSectorSize, i => (byte)(i * 3));
            var (rbuf, rptr) = AllocateAlignedBuffer(HardeningSectorSize, _ => 0);

            // Write segments 0 through 3
            for (var segment = 0; segment <= 3; segment++)
            {
                device.WriteAsync(wptr, segment, 0, HardeningSectorSize, IOCallback, null);
                semaphore.Wait();
            }

            // Segment 2 lies between others. The native device holds segments 0 through 3 open, so it is open above the lowest.
            device.RemoveSegment(2);

            if (kind == SegmentRemovingDeviceKind.LocalMemory)
            {
                // LocalMemoryDevice has no files: reading the removed segment fails
                uint readError = 0;
                device.ReadAsync(2, 0, rptr, HardeningSectorSize, (errorCode, _, _, _) => { readError = errorCode; semaphore.Release(); }, null);
                semaphore.Wait();
                ClassicAssert.AreNotEqual(0u, readError, "reading the removed segment should fail");
            }
            else
            {
                // A file device deletes or empties the removed segment's file
                var removed = new FileInfo($"{path}.2");
                ClassicAssert.IsTrue(!removed.Exists || removed.Length == 0, "the removed segment's file should hold no data");

                // Checked before reading through the device: some devices report reading a removed segment as an IO error
                AssertSegmentFiles(path, i => (byte)(i * 3), 0, 1, 3);
            }

            // The other segments read back their data through the device
            foreach (var segment in new[] { 0, 1, 3 })
            {
                device.ReadAsync(segment, 0, rptr, HardeningSectorSize, IOCallback, null);
                semaphore.Wait();
                AssertBufferContents(rptr, HardeningSectorSize, i => (byte)(i * 3), $"{kind} segment {segment}");
            }
            GC.KeepAlive(wbuf);
            GC.KeepAlive(rbuf);
        }

        /// <summary>Device kinds whose <see cref="IDevice.Reset"/> must forget the truncated segments, including composite devices</summary>
        public enum ResettableDeviceKind { Native, RandomAccess, ManagedLocal, LocalStorage, LocalMemory, Tiered, Sharded }

        /// <summary>
        /// Create a device of the given kind, initialized with <paramref name="epoch"/> as truncation requires, and report the files
        /// that hold a segment: none for <see cref="LocalMemoryDevice"/>, and one per child of a composite device
        /// </summary>
        static IDevice CreateResettableDeviceForTest(ResettableDeviceKind kind, string path, long segmentSize, LightEpoch epoch, out Func<int, string[]> segmentFiles)
        {
            segmentFiles = segment => [$"{path}.{segment}"];
            IDevice device;
            switch (kind)
            {
                case ResettableDeviceKind.Native:
                    device = new NativeStorageDevice(path, deleteOnClose: true);
                    break;
                case ResettableDeviceKind.RandomAccess:
                    device = new RandomAccessLocalStorageDevice(path, preallocateFile: false, deleteOnClose: true);
                    break;
                case ResettableDeviceKind.ManagedLocal:
                    device = new ManagedLocalStorageDevice(path, preallocateFile: false, deleteOnClose: true);
                    break;
                case ResettableDeviceKind.LocalStorage when OperatingSystem.IsWindows():
                    device = new LocalStorageDevice(path, false, true, true, Devices.CAPACITY_UNSPECIFIED, false, false);
                    break;
                case ResettableDeviceKind.LocalStorage:
                    Assert.Ignore("LocalStorageDevice is Windows-only.");
                    return null;
                case ResettableDeviceKind.LocalMemory:
                    device = new LocalMemoryDevice(capacity: 8 * segmentSize, segmentSize: segmentSize, parallelism: 1);
                    segmentFiles = _ => [];
                    break;
                case ResettableDeviceKind.Tiered:
                case ResettableDeviceKind.Sharded:
                    string[] childPaths = [$"{path}.a", $"{path}.b"];
                    segmentFiles = segment => childPaths.Select(childPath => $"{childPath}.{segment}").ToArray();

                    // Only the last tier of a tiered device may have unspecified capacity
                    IDevice first = new RandomAccessLocalStorageDevice(childPaths[0], preallocateFile: false, deleteOnClose: true, capacity: 1L << 30);
                    IDevice second = new RandomAccessLocalStorageDevice(childPaths[1], preallocateFile: false, deleteOnClose: true);
                    device = kind == ResettableDeviceKind.Tiered
                        ? new TieredStorageDevice(1, first, second)
                        : new ShardedStorageDevice(new UniformPartitionScheme(Math.Max(first.SectorSize, second.SectorSize), first, second));
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(kind));
            }
            device.Initialize(segmentSize, epoch);
            return device;
        }

        /// <summary>
        /// A log that is reset writes from segment 0 again, below the segments already truncated, and truncation must still be
        /// able to remove those segments: Reset must return the device's start segment to 0, on every device kind, composite
        /// devices included
        /// </summary>
        [Test]
        [Category("IDevice")]
        public unsafe void IDevice_Reset_ForgetsTheTruncatedSegments([Values] ResettableDeviceKind kind)
        {
            const long segmentSize = 1L << 16;
            var path = Path.Join(TestUtils.MethodTestDir, "test.log");
            Func<int, byte> written = i => (byte)(i * 3);
            Func<int, byte> rewritten = i => (byte)(i * 5 + 1);
            var (wbuf, wptr) = AllocateAlignedBuffer(HardeningSectorSize, written);
            var (rwbuf, rwptr) = AllocateAlignedBuffer(HardeningSectorSize, rewritten);
            var (rbuf, rptr) = AllocateAlignedBuffer(HardeningSectorSize, _ => 0);
            var epoch = new LightEpoch();
            try
            {
                using var device = CreateResettableDeviceForTest(kind, path, segmentSize, epoch, out var segmentFiles);

                // Write segments 0 through 2, and truncate below segment 2
                for (var segment = 0; segment <= 2; segment++)
                {
                    device.WriteAsync(wptr, segment, 0, HardeningSectorSize, IOCallback, null);
                    semaphore.Wait();
                }
                device.TruncateUntilSegment(2);
                ClassicAssert.AreEqual(2, device.StartSegment);

                device.Reset();
                ClassicAssert.AreEqual(0, device.StartSegment, "Reset should forget the truncated segments");

                // Write segments 0 and 1 again, with other data, and truncate below segment 1
                foreach (var segment in new[] { 0, 1 })
                {
                    device.WriteAsync(rwptr, segment, 0, HardeningSectorSize, IOCallback, null);
                    semaphore.Wait();
                }
                device.TruncateUntilSegment(1);
                ClassicAssert.AreEqual(1, device.StartSegment);

                // Segment 0 is gone: LocalMemoryDevice fails to read it, and no device keeps its files
                if (kind == ResettableDeviceKind.LocalMemory)
                {
                    uint readError = 0;
                    device.ReadAsync(0, 0, rptr, HardeningSectorSize, (errorCode, _, _, _) => { readError = errorCode; semaphore.Release(); }, null);
                    semaphore.Wait();
                    ClassicAssert.AreNotEqual(0u, readError, "segment 0 should be truncated");
                }
                foreach (var file in segmentFiles(0))
                    ClassicAssert.IsFalse(File.Exists(file), $"{file} should be truncated");

                // Segment 1 holds the data written after the reset
                device.ReadAsync(1, 0, rptr, HardeningSectorSize, IOCallback, null);
                semaphore.Wait();
                AssertBufferContents(rptr, HardeningSectorSize, rewritten, $"{kind} segment 1");
            }
            finally
            {
                epoch.Dispose();
            }
            GC.KeepAlive(wbuf);
            GC.KeepAlive(rwbuf);
            GC.KeepAlive(rbuf);
        }

        /// <summary>
        /// A faster tier holds only recent segments, so it can have evicted a segment that the slower tiers still hold. Removing
        /// that segment calls only the slower tiers, and must still complete, and remove it from them.
        /// </summary>
        [Test]
        [Category("IDevice")]
        public unsafe void TieredDevice_RemoveSegmentAsync_CompletesWhenAFasterTierEvictedTheSegment()
        {
            const long segmentSize = 1L << 16;
            var path = Path.Join(TestUtils.MethodTestDir, "test.log");
            var (wbuf, wptr) = AllocateAlignedBuffer(HardeningSectorSize, i => (byte)(i * 3));
            var epoch = new LightEpoch();
            try
            {
                var fast = new RandomAccessLocalStorageDevice($"{path}.fast", preallocateFile: false, deleteOnClose: true, capacity: 1L << 30);
                var slow = new RandomAccessLocalStorageDevice($"{path}.slow", preallocateFile: false, deleteOnClose: true);
                using var tiered = new TieredStorageDevice(1, fast, slow);
                tiered.Initialize(segmentSize, epoch);

                // Write segments 0 and 1 to both tiers, then evict segment 0 from the faster tier only
                for (var segment = 0; segment <= 1; segment++)
                {
                    tiered.WriteAsync(wptr, segment, 0, HardeningSectorSize, IOCallback, null);
                    semaphore.Wait();
                }
                fast.TruncateUntilSegment(1);

                // Removing segment 0 through the tiered device completes, and the slower tier deletes its file
                using var removed = new ManualResetEventSlim(false);
                tiered.RemoveSegmentAsync(0, _ => removed.Set(), null);
                ClassicAssert.IsTrue(removed.Wait(TimeSpan.FromSeconds(10)), "removing a segment that a faster tier evicted should complete");
                ClassicAssert.IsFalse(File.Exists($"{path}.slow.0"), "the slower tier should remove the segment");
            }
            finally
            {
                epoch.Dispose();
            }
            GC.KeepAlive(wbuf);
        }

        /// <summary>
        /// Deleting a segment file that the native device holds open would lose later writes to it on Linux, and Windows refuses,
        /// so TryRemoveSegmentsAbove empties such a file instead, and a later write through the device still reaches it
        /// </summary>
        [Test]
        [Category("IDevice")]
        public unsafe void Native_TryRemoveSegmentsAbove_EmptiesTheFilesItHoldsOpen()
        {
            const long segmentSize = 1L << 16;
            var path = Path.Join(TestUtils.MethodTestDir, "test.log");
            var (wbuf, wptr) = AllocateAlignedBuffer(HardeningSectorSize, i => (byte)(i * 3));
            var (rbuf, rptr) = AllocateAlignedBuffer(HardeningSectorSize, _ => 0);
            using (var device = CreateDeviceForTest(DeviceKind.Native, path, segmentSize, deleteOnClose: false))
            {
                // This leaves the device holding segments 0 through 5 open
                foreach (var segment in new[] { 5, 0, 1, 2 })
                {
                    device.WriteAsync(wptr, segment, 0, HardeningSectorSize, IOCallback, null);
                    semaphore.Wait();
                }

                // Removing above segment 1 empties the files of segments 2 through 5; segments 2 and 5 held data
                ClassicAssert.IsTrue(device.TryRemoveSegmentsAbove(1, out var removed));
                ClassicAssert.AreEqual(2, removed, "segments 2 and 5 held data");
                for (var segment = 2; segment <= 5; segment++)
                    ClassicAssert.AreEqual(0L, new FileInfo($"{path}.{segment}").Length, $"segment {segment} should be emptied, not deleted");

                // Write segment 3 again through the same device
                device.WriteAsync(wptr, 3, 0, HardeningSectorSize, IOCallback, null);
                semaphore.Wait();
            }

            // A new device reads back that write from the file
            using (var reopened = CreateDeviceForTest(DeviceKind.Native, path, segmentSize, deleteOnClose: false))
            {
                reopened.ReadAsync(3, 0, rptr, HardeningSectorSize, IOCallback, null);
                semaphore.Wait();
                AssertBufferContents(rptr, HardeningSectorSize, i => (byte)(i * 3), "segment 3, written after it was emptied");
            }
            GC.KeepAlive(wbuf);
            GC.KeepAlive(rbuf);
        }

        /// <summary>
        /// TryRemoveSegmentsAbove deletes the files of the segments above the one named that the native device does not hold open,
        /// here after Reset closed them all, and the segments at and below the one named keep their data
        /// </summary>
        [Test]
        [Category("IDevice")]
        public unsafe void Native_TryRemoveSegmentsAbove_DeletesTheFilesItDoesNotHoldOpen()
        {
            const long segmentSize = 1L << 16;
            var path = Path.Join(TestUtils.MethodTestDir, "test.log");
            using var device = CreateDeviceForTest(DeviceKind.Native, path, segmentSize, deleteOnClose: false);
            var (wbuf, wptr) = AllocateAlignedBuffer(HardeningSectorSize, i => (byte)(i * 3));
            var (rbuf, rptr) = AllocateAlignedBuffer(HardeningSectorSize, _ => 0);

            // Segments 5, 0, 1 and 2 hold data
            foreach (var segment in new[] { 5, 0, 1, 2 })
            {
                device.WriteAsync(wptr, segment, 0, HardeningSectorSize, IOCallback, null);
                semaphore.Wait();
            }

            // Reset closes every segment, as TsavoriteLog.Initialize does; reading segment 1 then opens only that one
            device.Reset();
            device.ReadAsync(1, 0, rptr, HardeningSectorSize, IOCallback, null);
            semaphore.Wait();

            // Removing above segment 1 deletes the files of segments 2 through 5; segments 2 and 5 held data
            ClassicAssert.IsTrue(device.TryRemoveSegmentsAbove(1, out var removed));
            ClassicAssert.AreEqual(2, removed, "segments 2 and 5 held data");
            for (var segment = 2; segment <= 5; segment++)
                ClassicAssert.IsFalse(File.Exists($"{path}.{segment}"), $"segment {segment} should be deleted");

            // Segments 0 and 1 keep their data
            foreach (var segment in new[] { 0, 1 })
            {
                device.ReadAsync(segment, 0, rptr, HardeningSectorSize, IOCallback, null);
                semaphore.Wait();
                AssertBufferContents(rptr, HardeningSectorSize, i => (byte)(i * 3), $"segment {segment}");
            }
            GC.KeepAlive(wbuf);
            GC.KeepAlive(rbuf);
        }

        /// <summary>
        /// A device whose file name has no segment id cannot list its segments, so TryRemoveSegmentsAbove returns false and
        /// removes nothing, and clearing falls back to zeroing the data it finds
        /// </summary>
        [Test]
        [Category("IDevice")]
        public void IDevice_TryRemoveSegmentsAbove_IsFalseForAFileNamedWithoutASegmentId()
        {
            using var device = new RandomAccessLocalStorageDevice(Path.Join(TestUtils.MethodTestDir, "test.log"), deleteOnClose: true);
            device.Initialize(segmentSize: -1, omitSegmentIdFromFilename: true);
            ClassicAssert.IsFalse(device.TryRemoveSegmentsAbove(0, out var removed));
            ClassicAssert.AreEqual(0, removed);
        }

        /// <summary>
        /// An Azure device removes the blobs above the segment named that it found when it started, including those another
        /// instance wrote, and keeps the blobs at and below it. Runs only when Azure tests are enabled, against the emulator.
        /// </summary>
        [Test]
        [Category("IDevice")]
        public unsafe void AzureDevice_TryRemoveSegmentsAbove_RemovesTheBlobsItFound()
        {
            TestUtils.IgnoreIfNotRunningAzureTests();
            const long segmentSize = 1L << 16;
            var epoch = new LightEpoch();
            var (wbuf, wptr) = AllocateAlignedBuffer(HardeningSectorSize, i => (byte)(i * 3));
            var (rbuf, rptr) = AllocateAlignedBuffer(HardeningSectorSize, _ => 0);
            try
            {
                // A device that deletes on close removes every blob it found when it started, including any left by an earlier run
                using (new AzureStorageDevice(TestUtils.AzureEmulatedStorageString, TestUtils.AzureTestContainer, TestUtils.AzureTestDirectory, "test.log", deleteOnClose: true))
                {
                }

                // Write segments 5, 0, 1 and 2 through one instance
                using (var writer = new AzureStorageDevice(TestUtils.AzureEmulatedStorageString, TestUtils.AzureTestContainer, TestUtils.AzureTestDirectory, "test.log"))
                {
                    writer.Initialize(segmentSize, epoch);
                    foreach (var segment in new[] { 5, 0, 1, 2 })
                    {
                        writer.WriteAsync(wptr, segment, 0, HardeningSectorSize, IOCallback, null);
                        semaphore.Wait();
                    }
                }

                // Another instance finds the blobs when it starts, and removes those above the segment named
                using (var remover = new AzureStorageDevice(TestUtils.AzureEmulatedStorageString, TestUtils.AzureTestContainer, TestUtils.AzureTestDirectory, "test.log"))
                {
                    remover.Initialize(segmentSize, epoch);
                    ClassicAssert.IsTrue(remover.TryRemoveSegmentsAbove(1, out var removed));
                    ClassicAssert.AreEqual(2, removed, "segments 2 and 5 held data");
                }

                // A last instance, which deletes every blob on close, finds no blob above segment 1, while segments 0 and 1 keep
                // their data
                using (var remaining = new AzureStorageDevice(TestUtils.AzureEmulatedStorageString, TestUtils.AzureTestContainer, TestUtils.AzureTestDirectory, "test.log", deleteOnClose: true))
                {
                    remaining.Initialize(segmentSize, epoch);
                    ClassicAssert.IsTrue(remaining.TryRemoveSegmentsAbove(1, out var removed));
                    ClassicAssert.AreEqual(0, removed, "no blob above segment 1 should remain");
                    foreach (var segment in new[] { 0, 1 })
                    {
                        remaining.ReadAsync(segment, 0, rptr, HardeningSectorSize, IOCallback, null);
                        semaphore.Wait();
                        AssertBufferContents(rptr, HardeningSectorSize, i => (byte)(i * 3), $"segment {segment}");
                    }
                }
            }
            finally
            {
                epoch.Dispose();
                GC.KeepAlive(wbuf);
                GC.KeepAlive(rbuf);
            }
        }

        [System.Runtime.InteropServices.DllImport("libc", SetLastError = true, EntryPoint = "chmod")]
        private static extern int chmod(string pathname, uint mode);
    }
}