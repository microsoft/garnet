// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    /// <summary>
    /// Tests for truncating a log after <see cref="TsavoriteLog.Reset"/> or <see cref="TsavoriteLog.Initialize"/> moves it below
    /// segments that were already truncated
    /// </summary>
    [TestFixture]
    internal class LogTruncationAfterResetTests : TestBase
    {
        const int PageSizeBits = 12;
        const int MemorySizeBits = 14;

        // 4 pages per segment, so a small log spans several segments
        const int SegmentSizeBits = 14;
        const long SegmentSize = 1L << SegmentSizeBits;

        static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

        string logPath;
        DeviceLogCommitCheckpointManager manager;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            logPath = Path.Join(TestUtils.MethodTestDir, "t.log");
            manager = new DeviceLogCommitCheckpointManager(new LocalStorageNamedDeviceFactoryCreator(),
                new DefaultCheckpointNamingScheme(TestUtils.MethodTestDir), removeOutdated: false);
        }

        [TearDown]
        public void TearDown()
        {
            manager?.Dispose();
            manager = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        [Test]
        [Category("TsavoriteLog")]
        public void TruncationRemovesSegmentsWrittenAfterTheLogMovesDown([Values] bool initialize)
        {
            using var device = CreateFileDevice();
            using var log = CreateLog(device);
            FillUntil(log, 4 * SegmentSize + 1);
            TruncateUntil(log, 4 * SegmentSize);
            for (var segment = 0; segment < 4; segment++)
                AssertTruncated(segment);

            // Move the log below the truncated segments: Reset restarts it in segment 0, and Initialize in segment 2, as a replica's
            // full sync does
            var restart = initialize ? 2 * SegmentSize : 0;
            if (initialize)
                log.Initialize(restart, restart);
            else
                log.Reset();

            // Write into the next segment, and truncate below it
            FillUntil(log, restart + SegmentSize + 1);
            var firstSegment = (int)(restart >> SegmentSizeBits);
            ClassicAssert.IsTrue(File.Exists(SegmentPath(firstSegment)), $"segment {firstSegment} should be written");
            TruncateUntil(log, (firstSegment + 1) * SegmentSize);
            AssertTruncated(firstSegment);
        }

        [Test]
        [Category("TsavoriteLog")]
        public void TruncationRequestedBeforeAResetIsSkipped()
        {
            // Truncation runs on a task, which can still be pending when the log is reset. The reset device truncates from segment
            // 0, so such a truncation would then remove the new log's segments.
            using var device = new GatedTruncationDevice(CreateFileDevice());
            using var log = CreateLog(device);
            FillUntil(log, 5 * SegmentSize + 1);

            // A first truncation holds the device, so a second, requested meanwhile, cannot run before the reset
            device.Gate.Reset();
            log.UnsafeShiftBeginAddress(2 * SegmentSize, truncateLog: true);
            ClassicAssert.IsTrue(device.TruncationStarted.Wait(Timeout), "the first truncation should start");
            log.UnsafeShiftBeginAddress(4 * SegmentSize, truncateLog: true);

            // The reset waits for the first truncation to finish before it resets the device
            var reset = Task.Run(log.Reset);
            var resetCountField = GetResetCountField(log, out var allocator);
            ClassicAssert.IsTrue(SpinWait.SpinUntil(() => (long)resetCountField.GetValue(allocator) == 1, Timeout), "the reset should begin");
            device.Gate.Set();
            ClassicAssert.IsTrue(reset.Wait(Timeout), "the reset should finish once the first truncation does");

            // The new log's segments survive the truncation requested before the reset, which is skipped
            FillUntil(log, SegmentSize + 1);
            ClassicAssert.IsFalse(SpinWait.SpinUntil(() => device.Truncations > 1, TimeSpan.FromSeconds(1)), "the second truncation should be skipped");
            foreach (var segment in new[] { 0, 1 })
                ClassicAssert.IsTrue(File.Exists(SegmentPath(segment)), $"segment {segment} of the new log should remain");
        }

        IDevice CreateFileDevice() => new RandomAccessLocalStorageDevice(logPath, disableFileBuffering: false, osReadBuffering: true);

        TsavoriteLog CreateLog(IDevice device) => new(new TsavoriteLogSettings
        {
            LogDevice = device,
            LogCommitManager = manager,
            TryRecoverLatest = false,
            PageSizeBits = PageSizeBits,
            MemorySizeBits = MemorySizeBits,
            SegmentSizeBits = SegmentSizeBits,
        });

        /// <summary>Enqueue entries until the tail reaches <paramref name="address"/>, then commit</summary>
        static void FillUntil(TsavoriteLog log, long address)
        {
            var entry = new byte[100];
            while (log.TailAddress < address)
                _ = log.Enqueue(entry);
            Commit(log);
        }

        /// <summary>Truncate the log below <paramref name="address"/> with a commit, which starts the device truncation</summary>
        static void TruncateUntil(TsavoriteLog log, long address)
        {
            log.TruncateUntil(address);
            Commit(log);
        }

        /// <summary>
        /// Commit with CommitStrongly. After Reset or Initialize, Commit commits nothing until the tail passes the address that the
        /// last earlier commit covered.
        /// </summary>
        static void Commit(TsavoriteLog log) => ClassicAssert.IsTrue(log.CommitStrongly(out _, out _, spinWait: true), "the commit should be made");

        string SegmentPath(int segment) => $"{logPath}.{segment}";

        /// <summary>Assert that the segment's file is deleted; truncation runs on a task, so this waits for it</summary>
        void AssertTruncated(int segment)
        {
            var path = SegmentPath(segment);
            ClassicAssert.IsTrue(SpinWait.SpinUntil(() => !File.Exists(path), Timeout), $"segment {segment} should be truncated");
        }

        /// <summary>The allocator field that counts the log's resets, which the second test watches to know a reset has begun</summary>
        static FieldInfo GetResetCountField(TsavoriteLog log, out object allocator)
        {
            allocator = typeof(TsavoriteLog).GetField("allocator", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(log);
            for (var type = allocator.GetType(); type != null; type = type.BaseType)
            {
                var field = type.GetField("resetCount", BindingFlags.Instance | BindingFlags.NonPublic);
                if (field != null)
                    return field;
            }
            throw new MissingFieldException("AllocatorBase", "resetCount");
        }

        /// <summary>
        /// A device that forwards to another, counts truncations, and while <see cref="Gate"/> is closed, holds each one until it opens
        /// </summary>
        sealed class GatedTruncationDevice(IDevice underlying) : StorageDeviceBase(underlying.FileName, underlying.SectorSize, underlying.Capacity)
        {
            internal readonly ManualResetEventSlim Gate = new(true);
            internal readonly SemaphoreSlim TruncationStarted = new(0);
            int truncations;

            internal int Truncations => Volatile.Read(ref truncations);

            public override void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
            {
                base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
                underlying.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
            }

            public override void TruncateUntilAddress(long toAddress)
            {
                _ = Interlocked.Increment(ref truncations);
                _ = TruncationStarted.Release();

                // Bounded, so a test that fails before opening the gate does not leave this blocked
                _ = Gate.Wait(Timeout);
                base.TruncateUntilAddress(toAddress);
            }

            public override void Reset()
            {
                base.Reset();
                underlying.Reset();
            }

            public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
                => underlying.RemoveSegmentAsync(segment, callback, result);

            public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
                DeviceIOCompletionCallback callback, object context)
                => underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);

            public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
                DeviceIOCompletionCallback callback, object context)
                => underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);

            public override bool TryComplete() => underlying.TryComplete();

            public override long GetFileSize(int segment) => underlying.GetFileSize(segment);

            public override void Dispose()
            {
                underlying.Dispose();
                Gate.Dispose();
                TruncationStarted.Dispose();
            }
        }
    }
}