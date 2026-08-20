// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Backend tuning options for <see cref="DeviceType.Native"/> on Linux (libaio / io_uring).
    /// Ignored for other device types and platforms. Passed to
    /// <see cref="Devices.CreateLogDevice(string, DeviceType, bool, bool, long, bool, bool, bool, bool, int, Microsoft.Extensions.Logging.ILogger, NativeDeviceOptions, LocalMemoryDeviceOptions)"/>.
    /// </summary>
    public sealed class NativeDeviceOptions
    {
        /// <summary>Which IO backend (libaio or io_uring) to use.</summary>
        public NativeStorageDevice.IoBackend IoBackend { get; set; } = NativeStorageDevice.IoBackend.Default;

        /// <summary>
        /// Number of independent kernel io_contexts / io_uring rings (ring count), decoupled from the
        /// completion-drain thread count. Set at or above submitter concurrency to make io_submit
        /// contention-free and spread completion posting across more rings; the drainers each range-drain
        /// a contiguous slice. 0 (default) is backend-specific: io_uring uses a hardware-aware ring count
        /// (min(2 * ProcessorCount, 64), floored at the drainer count), while libaio uses one ring per
        /// drainer. Clamped up to the drainer count.
        /// </summary>
        public int NumIoContexts { get; set; } = 0;

        /// <summary>
        /// Per-ring kernel submission depth D (maxEvents for io_uring_queue_init / libaio io_setup).
        /// Orthogonal to <see cref="NumIoContexts"/> (ring count) and the aggregate throttle.
        /// 0 (default) = the device default depth.
        /// </summary>
        public int QueueDepth { get; set; } = 0;

        /// <summary>
        /// io_uring backend only: enable IORING_SETUP_SQPOLL so a kernel thread polls the submission
        /// queue (syscall-free submits). Each ring gets its own poll thread. Ignored for libaio.
        /// Off by default.
        /// </summary>
        public bool UringSqPoll { get; set; } = false;

        /// <summary>
        /// io_uring SQPOLL poll-thread idle window in milliseconds (sq_thread_idle). Only used when
        /// <see cref="UringSqPoll"/> is true; 0 = native default.
        /// </summary>
        public int UringSqPollIdleMs { get; set; } = 0;
    }

    /// <summary>
    /// Tuning options for <see cref="DeviceType.LocalMemory"/>. Ignored for other device types. Passed to
    /// <see cref="Devices.CreateLogDevice(string, DeviceType, bool, bool, long, bool, bool, bool, bool, int, Microsoft.Extensions.Logging.ILogger, NativeDeviceOptions, LocalMemoryDeviceOptions)"/>.
    /// </summary>
    public sealed class LocalMemoryDeviceOptions
    {
        /// <summary>Segment size in bytes (must divide the device capacity). Default 1 GB.</summary>
        public long SegmentSize { get; set; } = 1L << 30;

        /// <summary>
        /// Per-submitter ring capacity (power of two), which is the device's in-flight bound (the
        /// producer blocks when its ring is full). 0 (default) uses the built-in default. This is how an
        /// in-flight throttle is applied to LocalMemory: its per-ring SPSC backpressure caps in-flight
        /// with no device-wide counter.
        /// </summary>
        public int RingCapacity { get; set; } = 0;
    }
}