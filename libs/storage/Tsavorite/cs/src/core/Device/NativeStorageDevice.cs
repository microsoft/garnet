// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    struct NativeResult
    {
        public DeviceIOCompletionCallback callback;
        public object context;
    }

    /// <summary>
    /// Native version of local storage device
    /// </summary>
    public unsafe class NativeStorageDevice : StorageDeviceBase
    {
        /// <summary>
        /// Default per-ring native submission depth D (io_uring SQ entries / libaio io_setup maxevents)
        /// used when <c>--device-queue-depth</c> is not set. This is the per-ring kernel queue capacity,
        /// one of the two physical device dimensions (ring COUNT = io-contexts, ring DEPTH = this).
        ///
        /// For io_uring this 4096 is a headroom CEILING, not pre-allocated work: the SQ is per-ring mmap
        /// memory with no global budget, a ring never holds more than <c>throttle / io-contexts</c> in-flight,
        /// and deeper rings than the workload uses cost only bounded pinned ring memory (~D*100B/ring), never
        /// extra IO. For libaio it is NOT free: <c>io_setup</c> permanently reserves <c>io-contexts * D</c>
        /// events from the global <c>fs.aio-max-nr</c> budget at creation whether used or not, so reserving the
        /// full 4096/ring wastes budget and, with many coexisting devices (e.g. cluster nodes), exhausts a
        /// stock 65536 budget (io_setup EAGAIN). Therefore, when queue-depth is left at the default, the libaio
        /// io_setup reservation is sized DOWN to the throttle share, capped per-ring (<see cref="ResolveLibaioReservationDepth"/>),
        /// rather than this ceiling. For multi-ring serving devices the full aggregate throttle is preserved
        /// (io-contexts * reservation &gt;= throttle) so there is no IOPS cost; only low-ring-count auxiliary
        /// devices (which do not serve deep queues) have their reservation — and effective throttle — reduced.
        /// An explicit <c>--device-queue-depth</c> is honored verbatim.
        /// </summary>
        const int DefaultQueueDepth = 1 << 12;      // 4096

        /// <summary>
        /// Per-ring headroom multiple applied when sizing the default libaio <c>io_setup</c> reservation to the
        /// throttle share (<see cref="ResolveLibaioReservationDepth"/>): each ring is sized to
        /// <c>headroom * ceil(throttle / io-contexts)</c> so an uneven submitter-&gt;ring distribution rarely
        /// drives a ring to exactly-full (which would trigger a non-fatal io_submit EAGAIN unwind/retry). 2x
        /// eliminates the ~2% ring-full IOPS dip seen at 1x. Combined with <see cref="LibaioReservationCap"/>
        /// (which bounds the per-ring depth) the per-device global-budget reservation is
        /// <c>min(2 * throttle, io-contexts * cap)</c>, so multi-ring serving devices keep full depth headroom
        /// while low-ring-count devices stay small.
        /// </summary>
        const int LibaioReservationHeadroom = 2;

        /// <summary>
        /// Ceiling on the per-ring default libaio <c>io_setup</c> reservation depth
        /// (<see cref="ResolveLibaioReservationDepth"/>). A single libaio io_context (ring) is drained by one
        /// completion thread and submitted through one kernel aio ring lock, so making a SINGLE ring hold the
        /// full 4096-deep throttle is both inefficient (one drainer cannot keep a 4096-deep ring saturated —
        /// the drainer sweep showed a lone drainer collapses) and wasteful of the global <c>fs.aio-max-nr</c>
        /// budget: deep in-flight should come from MORE rings (higher <c>--device-completion-threads</c>), not
        /// one mega-deep ring. Capping the per-ring reservation here keeps low-ring-count devices — auxiliary
        /// logs that do not serve deep random-read queues (AOF append, checkpoint bulk IO, per-node cluster
        /// replication logs), which default to a single ring — small, so many of them coexist in a stock 65536
        /// budget (e.g. a multi-node cluster process opening ~15 such devices reserves ~15*<see cref="LibaioReservationCap"/>
        /// instead of 15*4096, which exhausts the budget). Multi-ring serving devices are unaffected: at
        /// <c>io-contexts &gt;= 4</c> the 2x throttle share (<c>2 * 4096 / io-contexts</c>) is already &lt;= this
        /// cap, so their per-ring depth and full aggregate throttle are preserved. Only applies to the DEFAULT
        /// reservation; an explicit <c>--device-queue-depth</c> is honored verbatim (bypasses this path).
        /// </summary>
        const int LibaioReservationCap = 1 << 11;   // 2048

        /// <summary>
        /// Floor for the default libaio <c>io_setup</c> reservation depth (<see cref="ResolveLibaioReservationDepth"/>),
        /// giving headroom when the throttle share is tiny (high io-contexts). Matches the native default ring depth.
        /// </summary>
        const int LibaioReservationFloor = 1 << 7;  // 128

        /// <summary>
        /// Default for <see cref="AioMaxDevices"/>: the number of libaio Native device instances a single machine
        /// is provisioned to coexist within the global <c>fs.aio-max-nr</c> budget.
        /// </summary>
        const int DefaultAioMaxDevices = 1 << 5;    // 32

        /// <summary>
        /// Target number of libaio Native device instances a single process/machine is provisioned to coexist
        /// within the global <c>fs.aio-max-nr</c> budget. The default per-device <c>io_setup</c> reservation
        /// (<c>io-contexts * queue-depth</c>) is hard-capped at <c>fs.aio-max-nr / this</c>
        /// (<see cref="ResolveLibaioReservationDepth"/>), so at least this many devices can always be created
        /// regardless of <c>--device-completion-threads</c> / <c>--device-throttle-limit</c>. This is a
        /// PROCESS-WIDE setting (not per-device) because <c>fs.aio-max-nr</c> is a machine-global budget shared by
        /// every device in every process; set it once at startup (e.g. from <c>--device-aio-max-devices</c>)
        /// before any device is created. Because it is global, devices created through the raw
        /// <see cref="Devices.CreateLogDevice"/> path (cluster auxiliary logs, AOF) honor it too, without plumbing
        /// it through each call site. 32 keeps a stock 65536 budget at 2048 events/device (matching
        /// <see cref="LibaioReservationCap"/>); a machine that raises <c>fs.aio-max-nr</c> proportionally raises
        /// the per-device ceiling, so serving devices on a well-provisioned host are never starved. libaio only —
        /// io_uring has no global budget (per-ring mmap), so this never applies to it.
        /// </summary>
        public static int AioMaxDevices = DefaultAioMaxDevices;

        /// <summary>
        /// Hard cap on per-ring queue depth: io_uring's <c>IORING_MAX_ENTRIES</c> (32768). libaio additionally
        /// draws <c>io-contexts * queue-depth</c> from the global <c>fs.aio-max-nr</c> budget (distro default
        /// 65536), guarded separately at device creation.
        /// </summary>
        const int MaxQueueDepth = 1 << 15;          // 32768

        /// <summary>
        /// Default aggregate in-flight read throttle T (software backpressure) used when
        /// <c>--device-throttle-limit</c> is not set. This is the maximum number of disk-read IOs the
        /// allocator keeps in flight before it applies backpressure (<see cref="Throttle"/>); its only
        /// physical footprint is the pinned POH read buffers of the in-flight reads (~T * 4KB). It sizes
        /// NOTHING in the kernel — that is <see cref="DefaultQueueDepth"/>'s job.
        ///
        /// 4096 saturates an 8-drive NVMe RAID-0 at the achievable peak (measured neutral vs 65536 there)
        /// while keeping pinned read-buffer memory bounded (~16MB). High-connection deployments may raise
        /// <c>--device-throttle-limit</c> up to the kernel capacity <c>io-contexts * queue-depth</c> for a
        /// measured +5-7% at very high connection counts.
        /// </summary>
        const int DefaultThrottleLimit = 1 << 12;   // 4096

        /// <summary>
        /// Number of per-submitter-thread shards for in-flight tracking. Each submitter thread is assigned one
        /// shard (round-robin) on its first IO, and every per-IO bookkeeping write (slot assignment, in-flight
        /// increment/decrement) then lands on that shard's own cache lines. This removes the cache-line
        /// ping-pong that a single global pending counter plus a shared free-slot queue create when dozens of
        /// submitter and completion threads touch them on every IO — the dominant cost profiled at high IOPS.
        /// Sized above the peak concurrent submitter-thread count so distinct threads almost never share a shard
        /// (sharing is still correct — the counters are interlocked — just slightly more contended).
        /// An internal implementation detail, not a user knob: the shard count has a single correct regime
        /// (comfortably above peak concurrent submitters, with churn headroom).
        /// A performance sweep (device.bench and RESP GET over NumShards 512→16 × threads 32→128) showed
        /// throughput is flat while NumShards ≥ the peak concurrent submitter count, with a knee only at
        /// NumShards ≈ threads/4 — where ≥4 max-in-flight submitters share one <see cref="SlotsPerShard"/>-slot
        /// free-list and RentSlot begins to spin. That peak submitter count is in turn bounded by the number of
        /// logical processors available to the process (threads cannot execute the submit path more concurrently
        /// than there are CPUs; the ThreadPool only transiently overshoots under connections ≫ cores), so the
        /// count is derived from <see cref="Environment.ProcessorCount"/> rather than fitted to one machine —
        /// 2× the processor count gives headroom for that transient overshoot. The floor of 128 is the value
        /// validated on the sweep hardware (never regress below it on smaller boxes, ~1 MB of fixed managed
        /// memory); the cap of 1024 bounds the fixed table to ~8 MB on very large machines.
        /// <see cref="Environment.ProcessorCount"/> honors process CPU affinity and cgroup limits, so a pinned or
        /// containerized server sizes to the cores it can actually run on.
        /// </summary>
        static readonly int NumShards = Math.Clamp(2 * Environment.ProcessorCount, 128, 1024);

        /// <summary>
        /// Number of completion-tracking slots per shard (power of two), i.e. the depth of each shard's
        /// free-list (<see cref="shardFreeSlots"/>). Sized at 2x <see cref="MaxPerThreadInFlight"/> so a shard's
        /// list never empties under the throttle: a submitter rents at most <see cref="MaxPerThreadInFlight"/>
        /// slots before it must wait for completions, leaving ample free slots even accounting for the brief
        /// overshoot where a thread clears the throttle gate and submits before a completion lands.
        /// </summary>
        const int SlotsPerShard = 256;

        /// <summary>
        /// Hard cap on a single shard's (submitter thread's) in-flight IOs, enforced by <see cref="Throttle"/>.
        /// Kept at half of <see cref="SlotsPerShard"/> so each shard's free-list retains 2x headroom over the
        /// throttle gate, absorbing the brief overshoot where a thread clears the gate and submits before a
        /// completion lands. Total in-flight across the device is still bounded by the global
        /// <see cref="StorageDeviceBase.ThrottleLimit"/> — see <see cref="Throttle"/>.
        /// </summary>
        const int MaxPerThreadInFlight = SlotsPerShard / 2;

        /// <summary>
        /// Stride (in <see cref="long"/>s, i.e. 128 bytes) between adjacent shard counters in
        /// <see cref="shardSubmitted"/> / <see cref="shardCompleted"/> so each shard's counter sits on its own
        /// cache line (and the line is not shared with an adjacent shard via the hardware prefetcher).
        /// </summary>
        const int ShardStride = 16;

        /// <summary>
        /// Interval (milliseconds) between <see cref="activeShards"/> reconciliations — see
        /// <see cref="MaybeReconcileActiveShards"/>. Chosen small enough to track submitter-thread churn promptly
        /// yet large enough that the periodic shard scan is negligible (a few hundred scans/second at most).
        /// </summary>
        const long ReconcileIntervalMs = 200;

        /// <summary>
        /// Size of the in-flight completion-tracking pool (the <see cref="results"/> array): one entry per slot
        /// across all shards. Pure managed memory (no kernel cost). Derived from <see cref="NumShards"/>.
        /// </summary>
        static readonly int MaxResults = NumShards * SlotsPerShard;

        /// <summary>
        /// Sentinel returned by an int-valued native entry point (currently NativeDevice_QueueRunFor)
        /// when its body threw a C++ exception that the C ABI firewall caught. Distinct from the
        /// normal failure code (-1) so the completion worker can surface NativeDevice_GetLastError
        /// rather than silently treating it as a benign drain result. Kept in sync with the native
        /// kCABIExceptionSentinel (native_device_wrapper.cc).
        /// </summary>
        const int NativeCABIExceptionSentinel = int.MinValue;

        /// <summary>
        /// Floor sector size used when the alignment probe fails (parent directory missing,
        /// or kernel/filesystem combinations that do not populate statx STATX_DIOALIGN).
        /// </summary>
        const uint MinSectorSize = IDevice.MinDeviceSectorSize;

        readonly ILogger logger;
        NativeResult[] results;

        /// <summary>
        /// Per-shard monotonic count of submitted IOs, sharded by submitter thread and spaced by
        /// <see cref="ShardStride"/> so each shard's counter owns a cache line. Written by the shard's submitter
        /// thread(s). In-flight for a shard is <c>shardSubmitted - shardCompleted</c>; this drives
        /// <see cref="Throttle"/> and <see cref="Dispose()"/>'s drain-wait. Completion-slot assignment is
        /// handled separately by the per-shard free-lists (<see cref="shardFreeSlots"/>).
        /// </summary>
        long[] shardSubmitted;

        /// <summary>
        /// Per-shard count of completed (or failed/aborted) IOs, spaced by <see cref="ShardStride"/>. Bumped by
        /// whichever thread runs the completion (drainer or a <see cref="TryComplete"/> caller) or the submit
        /// error/abort path. Interlocked because more than one completer can touch a shard.
        /// </summary>
        long[] shardCompleted;

        /// <summary>
        /// Per-shard free-list of completion-tracking slot offsets into <see cref="results"/>. Each shard owns
        /// the contiguous block <c>[shard*SlotsPerShard, (shard+1)*SlotsPerShard)</c>; a submit rents a slot from
        /// its shard's list and the slot is returned only when its IO completes (in <see cref="_callback"/> or a
        /// submit error path). This "return only after completion" invariant is what makes reuse safe under the
        /// device's out-of-order completions — a monotonic counter-ring cannot, because a single slow IO can stay
        /// in flight while newer submits wrap the ring back onto its slot and overwrite the still-pending
        /// <see cref="NativeResult"/>, delivering a stale/duplicate context on the late completion. Sharding the
        /// list (rather than one global queue) keeps this off the contended cache lines profiled at high IOPS.
        /// </summary>
        ConcurrentQueue<int>[] shardFreeSlots;

        /// <summary>
        /// Per-device, per-thread shard assignment. The value factory (<see cref="AssignShard"/>) hands out the
        /// next shard round-robin and bumps <see cref="activeShards"/> the first time a thread touches this device.
        /// </summary>
        ThreadLocal<int> shardIndex;

        /// <summary>Round-robin sequence used by <see cref="AssignShard"/> to hand out shard indices.</summary>
        int nextShardSeq;

        /// <summary>
        /// Estimate of the number of CONCURRENTLY-active submitter threads, used by <see cref="PerThreadLimit"/>
        /// to split the global <see cref="StorageDeviceBase.ThrottleLimit"/> into a per-thread in-flight budget
        /// so the device-wide cap is preserved without a global in-flight counter. <see cref="AssignShard"/>
        /// increments it the first time a thread submits (immediate, conservative — new threads instantly get a
        /// fair share), and <see cref="MaybeReconcileActiveShards"/> periodically reconciles it DOWN to actual
        /// shard occupancy so it does not ratchet up as the .NET ThreadPool retires and re-injects submitter
        /// threads. Without that reconcile, a long-lived device under ThreadPool churn would see this grow
        /// unbounded (every fresh thread bumps it, thread exit never decrements it), collapsing the per-thread
        /// budget and progressively starving the device queue depth — a slow, restart-only-recoverable decline.
        /// </summary>
        int activeShards;

        /// <summary>
        /// <see cref="Environment.TickCount64"/> at/after which the next <see cref="activeShards"/> reconciliation
        /// is due. A plain (non-atomic) read gates the reconcile on the hot completion path; concurrent completers
        /// racing to reconcile is harmless (the scan is idempotent). See <see cref="MaybeReconcileActiveShards"/>.
        /// </summary>
        long nextReconcileTicks;

        /// <summary>
        /// Effective aggregate in-flight throttle T used by <see cref="Throttle"/>, captured once when the
        /// native device is created (the same point ring count N and depth D are fixed): the configured
        /// <see cref="StorageDeviceBase.ThrottleLimit"/> (or <see cref="DefaultThrottleLimit"/> when unset),
        /// capped at the kernel capacity <c>N * D</c> so aggregate in-flight can never exceed the rings. The
        /// hot throttle-spin loop reads this field directly rather than recomputing. Defaults to
        /// <see cref="DefaultThrottleLimit"/>; only consulted once reads are in flight, by which point the
        /// native device — and this value — have been established.
        /// </summary>
        int effectiveThrottleLimit = DefaultThrottleLimit;

        /// <summary>
        /// Configuration captured at construction time; the underlying native device is created
        /// lazily on the first IO call via <see cref="EnsureNativeDeviceCreated"/> using
        /// <c>segmentSize</c> as the requested segment size (defaults to -1 = unbounded
        /// single segment unless <see cref="Initialize"/> was called to override). All four
        /// fields are immutable after the constructor returns.
        /// </summary>
        readonly string filename;
        readonly bool deleteOnClose;
        readonly bool disableFileBuffering;
        readonly int numCompletionThreadsConfig;
        readonly int numIoContextsConfig;
        readonly int numQueueDepthConfig;
        readonly IoBackend ioBackendConfig;

        /// <summary>
        /// Runtime segment size in bytes that the native shim was asked to use. Populated by
        /// <see cref="EnsureNativeDeviceCreated"/> the first time the native handle is created.
        /// When the upper-layer requested <c>segmentSize = -1</c> (unbounded single segment)
        /// this is <see cref="UnboundedNativeSegmentSizeBytes"/>, large enough that any
        /// non-negative <c>long</c> upper-layer address routes to segment 0 under the native
        /// shim's <c>shift = log2(segment_size_bytes)</c> math. Used only for diagnostics /
        /// assertions on the C# side; the authoritative value lives inside the native device.
        /// </summary>
        ulong nativeSegmentSizeBytes;

        /// <summary>
        /// Native-side segment size used to represent unbounded single-segment mode (the default
        /// when neither the ctor nor <see cref="Initialize"/> overrides it, equivalent to
        /// <c>Initialize(segmentSize: -1)</c>). 1&lt;&lt;63 = 9.2 EiB; any non-negative
        /// <c>long</c> address is below this and so shifts to segment 0 inside the native
        /// <c>FileSystemSegmentedFile</c>. The C# managed side uses <c>segmentSizeBits = 64</c> /
        /// <c>segmentSizeMask = ~0</c> for its own address math, so segment IDs are always 0 in
        /// this mode on both sides.
        /// </summary>
        const ulong UnboundedNativeSegmentSizeBytes = 1UL << 63;

        /// <summary>
        /// Atomic flag (0 = alive, 1 = disposed) set once <see cref="Dispose"/> has freed
        /// <see cref="nativeDevice"/>. All native dispatch points check this flag before crossing
        /// the P/Invoke boundary, so a late call from a Tsavorite epoch-drain path (e.g.
        /// TryComplete fired after Dispose returned) is a silent no-op instead of a use-after-free.
        /// Using an int + <see cref="Interlocked.Exchange(ref int, int)"/> makes <see cref="Dispose"/>
        /// idempotent: a second call short-circuits before re-running the (non-idempotent) shutdown
        /// sequence.
        /// </summary>
        int disposedFlag;

        #region Native storage interface

        const string NativeLibraryName = "native_device";
        static readonly string NativeLibraryPath = null;
        static readonly string LibaioFallbackLibraryPath = null;

        static NativeStorageDevice()
        {
            // Select the prebuilt for the current OS/architecture/libc. The prebuilts are laid out under
            // runtimes/<rid>/native/ (NuGet RID convention) so a single package can carry per-platform binaries;
            // musl-based Linux (e.g. Alpine) uses the "linux-musl-*" RID because its binaries link the musl libc
            // and the plain "libaio.so.1"/"liburing.so.2" SONAMEs, whereas the glibc "linux-*" build links the
            // t64 "libaio.so.1t64" and cannot load on musl.
            var rid = GetNativeRuntimeIdentifier();
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                NativeLibraryPath = $"runtimes/{rid}/native/native_device.dll";
                LibaioFallbackLibraryPath = null;
            }
            else
            {
                // We ship two Linux native libraries per RID:
                //   * libnative_device.so         — built with USE_URING=ON, links libaio AND
                //     liburing. Used on hosts that have liburing2 installed; exposes both the
                //     Libaio and Uring backends.
                //   * libnative_device_libaio.so  — built with USE_URING=OFF, links libaio only.
                //     Used as a fallback on hosts without liburing2 installed; exposes the
                //     Libaio backend only (selecting Uring at construction time produces a
                //     clear TsavoriteException pointing the user at the install command).
                // The two-binary scheme keeps the Uring hot path zero-overhead (direct calls,
                // no function-pointer indirection) while still giving end-users on stock
                // distributions a libnative_device that loads cleanly without installing
                // liburing manually.
                NativeLibraryPath = $"runtimes/{rid}/native/libnative_device.so";
                LibaioFallbackLibraryPath = $"runtimes/{rid}/native/libnative_device_libaio.so";
            }
            NativeLibrary.SetDllImportResolver(typeof(NativeStorageDevice).Assembly, ImportResolver);
        }

        /// <summary>
        /// Computes the NuGet runtime identifier (RID) subfolder under runtimes/&lt;rid&gt;/native/ from which to load
        /// the prebuilt native device for the current process. Linux distinguishes glibc ("linux-&lt;arch&gt;") from musl
        /// ("linux-musl-&lt;arch&gt;", e.g. Alpine) because the two are not binary-compatible. Only the RIDs whose prebuilts
        /// are actually shipped will resolve; others fail the load and the caller falls back to a managed device.
        /// </summary>
        static string GetNativeRuntimeIdentifier()
        {
            var arch = RuntimeInformation.ProcessArchitecture switch
            {
                Architecture.X64 => "x64",
                Architecture.Arm64 => "arm64",
                Architecture.X86 => "x86",
                Architecture.Arm => "arm",
                var other => other.ToString().ToLowerInvariant()
            };

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return $"win-{arch}";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                return $"osx-{arch}";
            return IsMuslRuntime ? $"linux-musl-{arch}" : $"linux-{arch}";
        }

        static IntPtr ImportResolver(string libraryName, Assembly assembly, DllImportSearchPath? searchPath)
        {
            if (libraryName != NativeLibraryName || NativeLibraryPath == null)
                return IntPtr.Zero;

            var resolvedPath = ResolveNativeLibraryPath(assembly, NativeLibraryPath);

            try
            {
                // Primary (Uring-capable) build. LoadWithLibaioShim also repairs a libaio SONAME
                // mismatch transparently; a missing liburing.so.2 is the one failure it cannot repair
                // and lets propagate, so we can fall back to the libaio-only build below.
                return LoadWithLibaioShim(resolvedPath);
            }
            catch (DllNotFoundException ex) when (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)
                                                  && LibaioFallbackLibraryPath != null
                                                  && ex.Message.Contains("liburing.so.2", StringComparison.Ordinal))
            {
                // Host has no liburing2 installed. Fall back to the libaio-only build so that
                // the Libaio backend (the default) keeps working. Selecting IoBackend.Uring at
                // construction time on the fallback binary throws TsavoriteException with an
                // install-liburing2 instruction; we never silently downgrade Uring to Libaio.
                // The fallback ALSO goes through LoadWithLibaioShim, so a host that needs BOTH the
                // libaio SONAME shim AND the fallback (e.g. Ubuntu 24.04+: libaio.so.1t64 + no
                // liburing2) is repaired here instead of dead-ending — regardless of whether the
                // dynamic loader reported the libaio or the liburing miss first.
                var fallbackPath = ResolveNativeLibraryPath(assembly, LibaioFallbackLibraryPath);
                try
                {
                    return LoadWithLibaioShim(fallbackPath);
                }
                catch (DllNotFoundException fallbackEx)
                {
                    throw new DllNotFoundException(
                        $"Failed to load either '{Path.GetFileName(resolvedPath)}' (needs liburing.so.2) " +
                        $"or fallback '{Path.GetFileName(fallbackPath)}' (libaio-only). " +
                        $"Primary error: {ex.Message}. Fallback error: {fallbackEx.Message}. " +
                        $"On Debian/Ubuntu install with: sudo apt-get install -y libaio1t64 liburing2",
                        ex);
                }
            }
        }

        /// <summary>
        /// Load a native device build, transparently repairing a libaio SONAME mismatch
        /// (libaio.so.1 vs the 64-bit-time_t libaio.so.1t64) by dropping a compatibility symlink next
        /// to the binary and retrying once. A libaio mismatch is the one link failure we can fix in
        /// place; anything else — notably a missing liburing.so.2 — is allowed to propagate so the
        /// caller can fall back to the libaio-only build. Shared by BOTH the primary and the fallback
        /// load so a host that needs the libaio shim AND the fallback (no liburing2) is repaired
        /// regardless of which unresolved SONAME the dynamic loader reports first.
        /// </summary>
        static IntPtr LoadWithLibaioShim(string resolvedPath)
        {
            try
            {
                return NativeLibrary.Load(resolvedPath);
            }
            catch (DllNotFoundException ex) when (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)
                                                  && ex.Message.Contains("libaio.so.1", StringComparison.Ordinal))
            {
                // SONAME-mismatch shim for libaio. Our shipped binaries link libaio using the
                // build box's SONAME: on Debian 13 / Ubuntu 24.04+ the libaio1 package was renamed
                // to libaio1t64 (64-bit time_t ABI transition) and its SONAME became
                // "libaio.so.1t64", so binaries built there carry a DT_NEEDED of libaio.so.1t64.
                // Other glibc distros (Azure Linux, RHEL, Fedora, ...) ship the historical
                // "libaio.so.1" instead. Whichever SONAME the loader could not resolve, drop a
                // symlink of that name -> the libaio the host actually provides, next to the native
                // library; it is built with RPATH=$ORIGIN so it picks the symlink up. The primary and
                // fallback binaries live in the same directory, so a single symlink serves both.
                var missingSoname = ex.Message.Contains("libaio.so.1t64", StringComparison.Ordinal)
                    ? "libaio.so.1t64"
                    : "libaio.so.1";
                if (TryCreateLibaioCompatSymlink(resolvedPath, missingSoname, out var symlinkedPath))
                {
                    // Retry once with the symlink in place. If this build ALSO needs a library we
                    // cannot repair here (e.g. liburing.so.2), that DllNotFoundException propagates so
                    // the caller can try the libaio-only fallback build.
                    return NativeLibrary.Load(resolvedPath);
                }

                throw new DllNotFoundException(BuildLibaioDiagnostic(symlinkedPath, missingSoname, ex), ex);
            }
        }

        /// <summary>
        /// Resolve <paramref name="relativePath"/> (a NuGet-style "runtimes/&lt;rid&gt;/native/&lt;lib&gt;"
        /// relative path) to an absolute filesystem path. We probe (in order) the assembly's own
        /// directory, the application's base directory, and finally the current working directory
        /// when it is available. Falls back to the raw relative path if none of these exist, so
        /// dlopen's error message surfaces as before.
        /// </summary>
        static string ResolveNativeLibraryPath(Assembly assembly, string relativePath)
        {
            string[] searchRoots =
            [
                Path.GetDirectoryName(assembly?.Location),
                AppContext.BaseDirectory,
                TryGetCurrentDirectory(),
            ];

            foreach (var root in searchRoots)
            {
                if (string.IsNullOrEmpty(root))
                    continue;
                var candidate = Path.Combine(root, relativePath);
                if (File.Exists(candidate))
                    return Path.GetFullPath(candidate);
            }

            return relativePath;
        }

        /// <summary>
        /// Returns Directory.GetCurrentDirectory() if it can be obtained, otherwise null. The current
        /// directory can be unavailable (e.g., deleted or inaccessible to the process), which should
        /// not block native library resolution.
        /// </summary>
        static string TryGetCurrentDirectory()
        {
            try
            {
                return Directory.GetCurrentDirectory();
            }
            catch (Exception)
            {
                return null;
            }
        }

        /// <summary>
        /// True on musl-based distros (e.g., Alpine), detected via the musl dynamic loader
        /// (/lib/ld-musl-*.so*). The shipped native device libraries are glibc builds, so on musl
        /// they are unsupported: fabricating a libaio compat symlink and retrying the load would bind
        /// the glibc library against musl's libaio and crash. On musl we skip the compat shim and let
        /// the load fail cleanly so the caller can fall back to a managed device.
        /// Exposed so <see cref="Devices.GetDefaultDeviceType"/> does not pick <see cref="DeviceType.Native"/>
        /// as the default on musl (which would fail on the first storage IO).
        /// </summary>
        internal static readonly bool IsMuslRuntime = DetectMuslRuntime();

        static bool DetectMuslRuntime()
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                return false;
            try
            {
                using var e = Directory.EnumerateFiles("/lib", "ld-musl-*.so*").GetEnumerator();
                return e.MoveNext();
            }
            catch
            {
                // /lib may be absent or inaccessible; treat as non-musl.
                return false;
            }
        }

        /// <summary>
        /// Candidate absolute paths for a host-provided libaio shared object, across the multiarch
        /// and lib64 layouts of the distros we support. The 64-bit time_t SONAME "libaio.so.1t64"
        /// (shipped by Debian 13 / Ubuntu 24.04+ libaio1t64) is listed before the historical
        /// "libaio.so.1" (shipped by Azure Linux, RHEL, Fedora, and pre-t64 Debian/Ubuntu) so that,
        /// when both are present, we prefer linking against the real t64 file. Add more here if
        /// additional architectures or layouts appear.
        /// </summary>
        static readonly string[] LibaioHostCandidatePaths =
        [
            "/usr/lib/x86_64-linux-gnu/libaio.so.1t64",
            "/usr/lib/aarch64-linux-gnu/libaio.so.1t64",
            "/lib/x86_64-linux-gnu/libaio.so.1t64",
            "/lib/aarch64-linux-gnu/libaio.so.1t64",
            "/usr/lib64/libaio.so.1t64",
            "/usr/lib/libaio.so.1t64",
            "/usr/lib/x86_64-linux-gnu/libaio.so.1",
            "/usr/lib/aarch64-linux-gnu/libaio.so.1",
            "/lib/x86_64-linux-gnu/libaio.so.1",
            "/lib/aarch64-linux-gnu/libaio.so.1",
            "/usr/lib64/libaio.so.1",
            "/usr/lib/libaio.so.1",
            "/lib/libaio.so.1",
        ];

        /// <summary>
        /// Finds the first host-provided libaio from <see cref="LibaioHostCandidatePaths"/>, or
        /// returns false if none is installed. The returned path reflects the distro's actual layout
        /// (e.g. /usr/lib64 on RHEL/Fedora/Azure Linux, /usr/lib/&lt;triplet&gt; on Debian/Ubuntu).
        /// </summary>
        static bool TryFindHostLibaio(out string hostLibaioPath)
        {
            hostLibaioPath = null;
            foreach (var candidate in LibaioHostCandidatePaths)
            {
                if (File.Exists(candidate))
                {
                    hostLibaioPath = candidate;
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Locate a host-provided libaio (see <see cref="LibaioHostCandidatePaths"/>) and create a
        /// <paramref name="missingSoname"/> symlink next to libnative_device.so so that the dynamic
        /// linker (searching RPATH=$ORIGIN) can satisfy the DT_NEEDED entry the load failed on. This
        /// bridges the SONAME mismatch in either direction: a t64 binary on a non-t64 distro
        /// (libaio.so.1t64 -> host libaio.so.1) or a pre-t64 binary on a t64 distro
        /// (libaio.so.1 -> host libaio.so.1t64). Returns true when a usable symlink exists at the
        /// expected path after the call - whether we created it or a concurrently-starting process
        /// did. Sets <paramref name="createdSymlink"/> to the link path in that case.
        /// </summary>
        static bool TryCreateLibaioCompatSymlink(string resolvedNativeLibraryPath, string missingSoname, out string createdSymlink)
        {
            createdSymlink = null;

            // On musl (Alpine) the glibc-built native device cannot load; do not fabricate a libaio
            // symlink that would bind it against musl's libaio and segfault - fail cleanly instead so
            // the caller falls back to a managed device (matches pre-shim behavior on musl).
            if (IsMuslRuntime)
                return false;

            if (!TryFindHostLibaio(out var hostLibaioPath))
                return false;

            string shimPath;
            try
            {
                var nativeDir = Path.GetDirectoryName(Path.GetFullPath(resolvedNativeLibraryPath));
                if (string.IsNullOrEmpty(nativeDir) || !Directory.Exists(nativeDir))
                    return false;

                shimPath = Path.Combine(nativeDir, missingSoname);
            }
            catch (Exception)
            {
                return false;
            }

            try
            {
                File.CreateSymbolicLink(shimPath, hostLibaioPath);
                createdSymlink = shimPath;
                return true;
            }
            catch (IOException)
            {
                // Either a concurrently-starting process already created the symlink (common in
                // container fleets where multiple Garnet instances share an image), or a stale file
                // of the same name is present. If it already resolves to a real libaio, treat that
                // as success; otherwise fall through to the diagnostic error.
                if (IsUsableLibaioShim(shimPath))
                {
                    createdSymlink = shimPath;
                    return true;
                }
                return false;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// Returns true if <paramref name="shimPath"/> is an existing symlink whose target is a
        /// supported libaio: the "libaio.so.1t64" or historical "libaio.so.1" SONAME, or a versioned
        /// "libaio.so.1.*" real file (possibly via a relative or absolute target).
        /// </summary>
        static bool IsUsableLibaioShim(string shimPath)
        {
            try
            {
                var info = new FileInfo(shimPath);
                if (!info.Exists) return false;
                var target = info.LinkTarget;
                if (string.IsNullOrEmpty(target)) return false;
                // LinkTarget may be relative (e.g. "libaio.so.1t64") or absolute (e.g.
                // "/usr/lib64/libaio.so.1.0.2"); compare on the file name and accept only the two
                // supported SONAMEs or a versioned "libaio.so.1.*" real file - without matching an
                // unrelated future SONAME such as "libaio.so.10".
                var name = Path.GetFileName(target);
                return name == "libaio.so.1t64"
                    || name == "libaio.so.1"
                    || name.StartsWith("libaio.so.1.", StringComparison.Ordinal);
            }
            catch
            {
                return false;
            }
        }

        static string BuildLibaioDiagnostic(string attemptedSymlinkPath, string missingSoname, Exception inner)
        {
            var attempted = attemptedSymlinkPath == null
                ? "Could not find a host libaio in standard multiarch/lib64 paths; auto-repair skipped."
                : $"Attempted to create '{attemptedSymlinkPath}' -> a host libaio but the load still failed.";

            // Build the compat-symlink hint from the libaio the host actually ships so the suggested
            // path is correct on every distro (e.g. /usr/lib64 on RHEL/Fedora/Azure Linux,
            // /usr/lib/<triplet> on Debian/Ubuntu). Only offer it when a real libaio is present;
            // otherwise the right fix is to install the package.
            string symlinkFix;
            if (TryFindHostLibaio(out var hostLibaioPath))
            {
                var linkPath = Path.Combine(Path.GetDirectoryName(hostLibaioPath) ?? "/usr/lib", missingSoname);
                symlinkFix = $"(b) as root, create the compat symlink 'ln -sf {hostLibaioPath} {linkPath}', ";
            }
            else
            {
                symlinkFix = $"(b) if libaio is installed under a different SONAME, as root create a '{missingSoname}' compat symlink next to it, ";
            }

            return
                $"Failed to load native storage device library '{NativeLibraryPath}' because its dependency '{missingSoname}' " +
                "is not resolvable by the dynamic linker. This is a libaio SONAME mismatch between the build box and the host: " +
                "binaries built on Debian 13 (trixie) / Ubuntu 24.04 (noble) require 'libaio.so.1t64' (the libaio1t64 package, " +
                "64-bit time_t ABI transition), whereas Azure Linux, RHEL, Fedora and pre-t64 Debian/Ubuntu ship 'libaio.so.1'. " +
                attempted + " " +
                "To fix, either (a) install the host's libaio package (for example 'apt-get install -y libaio1t64', " +
                "'tdnf install -y libaio', or 'dnf install -y libaio'), " +
                symlinkFix +
                "or (c) switch to a non-native device by setting '--device-type RandomAccess' (or removing '--use-native-device-linux'). " +
                "Original loader error: " + inner.Message;
        }

        /// <summary>
        /// Async callback delegate
        /// </summary>
        public delegate void AsyncIOCallback(IntPtr context, int result, ulong bytesTransferred);
        IntPtr nativeDevice;

        /// <summary>
        /// Selects the IO backend used by the underlying native device. On Linux,
        /// <see cref="Libaio"/> uses the historical libaio path (the default). <see cref="Uring"/>
        /// uses io_uring. On Windows, only <see cref="Default"/> is supported (Windows ThreadPool).
        /// </summary>
        /// <remarks>
        /// Whether a given backend is actually available at runtime depends on how the loaded
        /// <c>libnative_device.so</c> / <c>native_device.dll</c> was built. Call
        /// <see cref="GetAvailableBackends"/> to probe at runtime.
        /// <para>
        /// The Linux prebuilt shipped under <c>runtimes/linux-x64/native/</c> is built with
        /// <c>USE_URING=ON</c>, so <c>liburing.so.2</c> is a NEEDED ELF entry that the dynamic
        /// linker must resolve at load time even when only <see cref="Libaio"/> is selected.
        /// </para>
        /// Must stay in sync with <c>NativeDeviceBackend</c> in <c>native_device.h</c>.
        /// </remarks>
        public enum IoBackend : int
        {
            /// <summary>Platform default (libaio on Linux, ThreadPool on Windows).</summary>
            Default = 0,
            /// <summary>Linux libaio. Same as Default on Linux.</summary>
            Libaio = 1,
            /// <summary>Linux io_uring. Requires native lib built with FASTER_URING.</summary>
            Uring = 2,
        }

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_CreateWithBackend", CallingConvention = CallingConvention.Cdecl)]
        static extern IntPtr NativeDevice_CreateWithBackend(string file, bool enablePrivileges, bool unbuffered, bool delete_on_close, int backend, ulong segmentSizeBytes, bool omitSegmentIdFromFilename, int numIoContexts, int maxEvents);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_GetSegmentSize", CallingConvention = CallingConvention.Cdecl)]
        static extern ulong NativeDevice_GetSegmentSize(IntPtr device);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_AvailableBackends", CallingConvention = CallingConvention.Cdecl)]
        static extern int NativeDevice_AvailableBackends();

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_Destroy", CallingConvention = CallingConvention.Cdecl)]
        static extern void NativeDevice_Destroy(IntPtr device);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_sector_size", CallingConvention = CallingConvention.Cdecl)]
        static extern uint NativeDevice_sector_size(IntPtr device);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_ProbeAlignment", CallingConvention = CallingConvention.Cdecl)]
        static extern uint NativeDevice_ProbeAlignment(string filename);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_ReadAsync", CallingConvention = CallingConvention.Cdecl)]
        static extern int NativeDevice_ReadAsync(IntPtr device, ulong source, IntPtr dest, uint length, AsyncIOCallback callback, IntPtr context);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_WriteAsync", CallingConvention = CallingConvention.Cdecl)]
        static extern int NativeDevice_WriteAsync(IntPtr device, IntPtr source, ulong dest, uint length, AsyncIOCallback callback, IntPtr context);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_CreateDir", CallingConvention = CallingConvention.Cdecl)]
        static extern int NativeDevice_CreateDir(IntPtr device, string dir, int deleteExisting);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_TryComplete", CallingConvention = CallingConvention.Cdecl)]
        static extern bool NativeDevice_TryComplete(IntPtr device);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_TryCompleteMine", CallingConvention = CallingConvention.Cdecl)]
        static extern bool NativeDevice_TryCompleteMine(IntPtr device);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_QueueRun", CallingConvention = CallingConvention.Cdecl)]
        static extern int NativeDevice_QueueRun(IntPtr device, int timeout_secs);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_QueueRunFor", CallingConvention = CallingConvention.Cdecl)]
        static extern int NativeDevice_QueueRunFor(IntPtr device, int ctxIdx, int timeout_secs);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_WakeCompletionWorker", CallingConvention = CallingConvention.Cdecl)]
        static extern int NativeDevice_WakeCompletionWorker(IntPtr device, int ctxIdx);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_NumIoContexts", CallingConvention = CallingConvention.Cdecl)]
        static extern int NativeDevice_NumIoContexts(IntPtr device);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_GetFileSize", CallingConvention = CallingConvention.Cdecl)]
        static extern ulong NativeDevice_GetFileSize(IntPtr device, ulong segment);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_Reset", CallingConvention = CallingConvention.Cdecl)]
        static extern void NativeDevice_Reset(IntPtr device);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_RemoveSegment", CallingConvention = CallingConvention.Cdecl)]
        static extern void NativeDevice_RemoveSegment(IntPtr device, ulong segment);

        [DllImport(NativeLibraryName, EntryPoint = "NativeDevice_GetLastError", CallingConvention = CallingConvention.Cdecl)]
        static extern IntPtr NativeDevice_GetLastError();
        #endregion

        /// <summary>
        /// Reads the thread-local last-error message produced by the native device. The native
        /// side guarantees that, for synchronous P/Invoke calls, the returned pointer references
        /// storage on the calling managed thread, so it is safe to read here without holding
        /// onto the pointer. Returns an empty string when there is no error.
        /// </summary>
        static string GetNativeLastError()
        {
            try
            {
                var ptr = NativeDevice_GetLastError();
                if (ptr == IntPtr.Zero) return string.Empty;
                return Marshal.PtrToStringUTF8(ptr) ?? string.Empty;
            }
            catch (EntryPointNotFoundException)
            {
                // Older builds of the native library without NativeDevice_GetLastError exported.
                return string.Empty;
            }
        }

        /// <summary>
        /// Returns the native thread-local last-error formatted for appending to an exception
        /// message (": &lt;message&gt;"), or an empty string when the native side reported none.
        /// Must be called on the same managed thread that made the failing P/Invoke call, since the
        /// native last-error storage is thread-local.
        /// </summary>
        static string FormatNativeError()
        {
            var msg = GetNativeLastError();
            return string.IsNullOrEmpty(msg) ? string.Empty : $": {msg}";
        }

        readonly AsyncIOCallback _callbackDelegate;
        CancellationTokenSource completionThreadToken;
        Thread[] completionThreads;
        int numRingsActual;

        // Instrumentation: peak concurrent in-flight writes seen, and submit/complete counters.
        // Set TSAVORITE_DEVICE_INSTRUMENT=1 in the environment to enable.
        static readonly bool s_instrument = Environment.GetEnvironmentVariable("TSAVORITE_DEVICE_INSTRUMENT") == "1";
        int peakNumPending;
        long submitCount;
        long completeCount;
        long submitNanos;

        /// <summary>Shard index for the calling thread on this device (assigned on first access).</summary>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        int GetShard() => shardIndex.Value;

        /// <summary>
        /// <see cref="ThreadLocal{T}"/> value factory: hands out the next shard round-robin and records that a
        /// new submitter thread has appeared (so <see cref="Throttle"/> can split the global budget across them).
        /// The round-robin counter is reduced modulo <see cref="NumShards"/> as <c>uint</c> so that when the
        /// signed <see cref="nextShardSeq"/> eventually wraps past <see cref="int.MaxValue"/> on a long-lived,
        /// thread-churning server the index stays in <c>[0, NumShards)</c> (a signed <c>%</c> would yield a
        /// negative index and an out-of-range shard access). <see cref="NumShards"/> need not be a power of two.
        /// </summary>
        int AssignShard()
        {
            int idx = (int)((uint)(Interlocked.Increment(ref nextShardSeq) - 1) % (uint)NumShards);
            Interlocked.Increment(ref activeShards);
            return idx;
        }

        /// <summary>
        /// Periodically reconciles <see cref="activeShards"/> DOWN to the number of shards currently carrying
        /// in-flight IO (≈ the number of concurrently-active submitter threads). <see cref="AssignShard"/> only
        /// ever increments <see cref="activeShards"/> — a .NET ThreadPool worker that submits once and is later
        /// retired never decrements it — so without this the divisor ratchets up as the pool churns threads,
        /// shrinking <see cref="PerThreadLimit"/> and starving the device queue over the process's lifetime
        /// (a slow decline only a restart recovers). Reconciling from live occupancy makes the divisor track
        /// actual concurrency: births are counted immediately (in <see cref="AssignShard"/>), deaths are reclaimed
        /// here. Time-gated by a cheap non-atomic <see cref="Environment.TickCount64"/> check so it is effectively
        /// free on the hot completion path (a full scan runs at most every <see cref="ReconcileIntervalMs"/> ms).
        /// Concurrent completers racing here is harmless: the scan is idempotent and the write is a plain store.
        /// <para>
        /// KNOWN BOUNDED TRANSIENT (intentional tradeoff): a submitter thread that already has a shard assigned
        /// (its <see cref="ThreadLocal{T}"/> value) does NOT re-run <see cref="AssignShard"/> when it re-activates,
        /// so if this reconciliation has just cratered <see cref="activeShards"/> after an idle gap and a burst of
        /// such reused threads then floods in, they briefly see a stale-low divisor and each provisions up to
        /// <see cref="MaxPerThreadInFlight"/> in-flight — a transient over-subscription bounded at
        /// <c>liveThreads × MaxPerThreadInFlight</c> (≈ 2× the throttle) for at most one <see cref="ReconcileIntervalMs"/>
        /// window, after which the next scan (their now-occupied shards) restores the correct divisor. This is
        /// non-corrupting (it only mis-sizes a perf knob) and is absorbed by the native ring's EAGAIN/unwind+retry.
        /// We deliberately do NOT track exact 0→1 / 1→0 shard-occupancy transitions to keep <see cref="activeShards"/>
        /// perfectly current: in-flight is <c>shardSubmitted − shardCompleted</c> across two independently-updated
        /// counters, so detecting those transitions atomically would require reading both after each increment (a
        /// cross-counter race) or a per-shard lock on the hottest submit/complete paths — a correctness hazard and
        /// a throughput cost that outweigh eliminating a brief, self-correcting, bounded over-subscription.
        /// </para>
        /// </summary>
        void MaybeReconcileActiveShards()
        {
            long now = Environment.TickCount64;
            if (now < Volatile.Read(ref nextReconcileTicks))
                return;
            Volatile.Write(ref nextReconcileTicks, now + ReconcileIntervalMs);
            int occupied = 0;
            for (int s = 0; s < NumShards; s++)
            {
                if (Volatile.Read(ref shardSubmitted[s * ShardStride]) - Volatile.Read(ref shardCompleted[s * ShardStride]) > 0)
                    occupied++;
            }
            Volatile.Write(ref activeShards, occupied < 1 ? 1 : occupied);
        }

        /// <summary>
        /// Rents a free completion-tracking slot offset from the calling submitter's shard. Under the throttle a
        /// shard never holds more than <see cref="MaxPerThreadInFlight"/> slots at once (&lt; <see cref="SlotsPerShard"/>),
        /// so the fast <see cref="ConcurrentQueue{T}.TryDequeue"/> path always succeeds; the spin is a safety net for
        /// the rare unthrottled bulk caller (e.g. page reads on recovery). Completions run on separate drainer
        /// threads that return slots via <see cref="ReturnSlot"/>, so the spin cannot self-deadlock.
        /// </summary>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        int RentSlot(int shard)
        {
            var freeList = shardFreeSlots[shard];
            if (freeList.TryDequeue(out int offset))
                return offset;
            var spin = new SpinWait();
            while (!freeList.TryDequeue(out offset))
                spin.SpinOnce();
            return offset;
        }

        /// <summary>
        /// Returns a completion-tracking slot to its owning shard's free-list. The owning shard is encoded in the
        /// offset (<c>offset / SlotsPerShard</c>), so a completion on any drainer thread returns the slot to the
        /// list the submitter will rent from — no cross-shard mixing. Called only after the slot's IO has
        /// completed (its <see cref="NativeResult"/> has been read), preserving the "reuse only after completion"
        /// invariant that makes slot reuse safe under out-of-order completions.
        /// </summary>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        void ReturnSlot(int offset)
        {
            // Release the completed slot's captured callback delegate and context object before the slot
            // is re-enqueued, so neither is kept rooted until the slot is next rented (otherwise up to
            // MaxResults user contexts stay reachable indefinitely on a mostly-idle device). Every caller
            // has already consumed the NativeResult first (the `var result = results[offset]` copy in
            // _callback, and the local callback/context parameters on the error/disposed paths), and clearing
            // BEFORE the enqueue means a concurrent RentSlot that dequeues this offset and writes its own
            // NativeResult cannot be clobbered by this clear.
            results[offset] = default;
            shardFreeSlots[offset / SlotsPerShard].Enqueue(offset);
        }

        /// <summary>Current in-flight IO count for a shard (submitted minus completed).</summary>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        long InFlightInShard(int shard)
            => Volatile.Read(ref shardSubmitted[shard * ShardStride]) - Volatile.Read(ref shardCompleted[shard * ShardStride]);

        /// <summary>Sum of in-flight IOs across all shards. Cold path only (Dispose drain, instrumentation).</summary>
        long TotalInFlight()
        {
            long total = 0;
            for (int s = 0; s < NumShards; s++)
                total += Volatile.Read(ref shardSubmitted[s * ShardStride]) - Volatile.Read(ref shardCompleted[s * ShardStride]);
            return total;
        }

        /// <summary>
        /// Per-thread in-flight budget = global throttle split across the distinct submitter threads seen so far,
        /// clamped to <see cref="MaxPerThreadInFlight"/>. Splitting this way keeps the device-wide cap equal to
        /// the configured <see cref="StorageDeviceBase.ThrottleLimit"/> (total ≈ perThread × activeShards) while
        /// the hot <see cref="Throttle"/> check only reads the calling thread's own shard counters.
        /// </summary>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        int PerThreadLimit()
        {
            int global = Volatile.Read(ref nativeDevice) == IntPtr.Zero
                ? (ThrottleLimit > 0 ? ThrottleLimit : DefaultThrottleLimit)
                : effectiveThrottleLimit;
            int active = Volatile.Read(ref activeShards);
            if (active < 1) active = 1;
            int perThread = global / active;
            if (perThread < 1) perThread = 1;
            if (perThread > MaxPerThreadInFlight) perThread = MaxPerThreadInFlight;
            return perThread;
        }

        /// <summary>
        /// Leases the native handle for a non-IO native call (TryComplete / Reset / RemoveSegment / GetFileSize)
        /// so a concurrent <see cref="Dispose"/> cannot free it mid-call. Returns false (without leasing) once
        /// disposal has begun. On success the caller MUST call <see cref="ReleaseLease"/> in a finally. The lease
        /// reuses the shard in-flight counters, so Dispose's drain-wait covers leased native calls automatically.
        /// </summary>
        bool TryLease(out int shard)
        {
            shard = GetShard();
            if (Volatile.Read(ref disposedFlag) != 0)
                return false;
            Interlocked.Increment(ref shardSubmitted[shard * ShardStride]);
            // Re-check after publishing the lease: if Dispose set the flag concurrently, its drain-wait either
            // already observed this lease (and is waiting) or will not — either way we must not touch the handle.
            if (Volatile.Read(ref disposedFlag) != 0)
            {
                Interlocked.Increment(ref shardCompleted[shard * ShardStride]);
                return false;
            }
            return true;
        }

        /// <summary>Releases a lease taken by <see cref="TryLease"/>.</summary>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        void ReleaseLease(int shard)
            => Interlocked.Increment(ref shardCompleted[shard * ShardStride]);

        void _callback(IntPtr context, int errorCode, ulong numBytes)
        {
            if (s_instrument) Interlocked.Increment(ref completeCount);
            MaybeReconcileActiveShards();
            int offset = (int)context;
            var result = results[offset];
            // CRITICAL: this method is invoked via a function pointer from native code (libaio /
            // io_uring completion drainer thread) across the C ABI boundary. ANY managed
            // exception that escapes this method propagates back into the native dispatch
            // loop and, when it crosses the ABI boundary, causes the .NET runtime to
            // terminate the drainer thread (silently, since it's a background thread). That
            // leaves the device with no completion processor: all subsequent IOs are
            // submitted but never completed, in-flight grows unbounded, device.Throttle()
            // stays true forever, and the next worker thread to call ReadAsync deadlocks
            // spinning in the throttle-wait loop.
            //
            // Tsavorite's user callback (AsyncGetFromDiskCallback) re-throws in several
            // error paths (no completionEvent set, exception during validation, etc.). So we
            // MUST catch absolutely everything here and never let it escape — even fatal
            // exceptions like OOM should be swallowed and the slot/bookkeeping cleaned up.
            // Errors that need surfacing should go through the result.callback's own error
            // channel (numBytes=0 + errorCode), not via a throw.
            //
            // try/finally also ensures that on a throwing user callback the shard's completed
            // counter is still bumped. Dispose() spins until in-flight (submitted-completed)
            // reaches 0, so bumping here (after the callback returns) guarantees Dispose waits
            // for all in-flight user callbacks to finish before destroying the native device
            // underneath them.
            try
            {
                result.callback((uint)errorCode, (uint)numBytes, result.context, ioException: default);
            }
            catch (Exception ex)
            {
                logger?.LogCritical(ex, "Unhandled exception in user IO completion callback (suppressed to keep drainer alive)");
            }
            finally
            {
                Interlocked.Increment(ref shardCompleted[(offset / SlotsPerShard) * ShardStride]);
                ReturnSlot(offset);
            }
        }

        /// <summary>Diagnostic: snapshot and reset per-second submit/complete counters and peak in-flight.
        /// Set environment variable <c>TSAVORITE_DEVICE_INSTRUMENT=1</c> to enable population.</summary>
        public (int curPending, int peakPending, long submits, long completes, long submitNs) GetAndResetStats()
        {
            int cur = (int)TotalInFlight();
            var stats = (cur, peakNumPending, submitCount, completeCount, submitNanos);
            peakNumPending = cur;
            submitCount = 0;
            completeCount = 0;
            submitNanos = 0;
            return stats;
        }

        /// <inheritdoc />
        /// <remarks>
        /// In-flight is tracked per submitter thread (shard) to avoid the cache-line contention a single global
        /// counter creates at high IOPS. Each thread throttles on its own shard's in-flight count against a
        /// per-thread budget (<see cref="PerThreadLimit"/>) derived from the effective aggregate throttle
        /// (<see cref="StorageDeviceBase.ThrottleLimit"/>, or <see cref="DefaultThrottleLimit"/> when unset,
        /// capped at the kernel capacity io-contexts * queue-depth) split across the active submitter threads —
        /// so the device-wide in-flight cap still equals the effective throttle, while this hot check only reads
        /// the calling thread's own cache lines.
        /// <para>
        /// Before the native device is lazily created (cold start), <see cref="PerThreadLimit"/> gates on the live
        /// clamped limit rather than the seeded <see cref="effectiveThrottleLimit"/>, so a startup burst of
        /// concurrent submitters cannot bypass the configured throttle and flood the just-sized ring.
        /// </para>
        /// </remarks>
        public override bool Throttle()
        {
            int shard = GetShard();
            return InFlightInShard(shard) > PerThreadLimit();
        }

        /// <summary>
        /// Resolves the per-ring native submission depth D (maxEvents passed to io_uring_queue_init /
        /// libaio io_setup). Returns <c>--device-queue-depth</c> when set (clamped to
        /// <see cref="MaxQueueDepth"/>), else <see cref="DefaultQueueDepth"/>. This is the ring DEPTH knob —
        /// orthogonal to the ring COUNT (io-contexts) and the aggregate in-flight throttle. Read at
        /// native-device creation (first IO). Ignored by the Windows (IOCP) backend.
        /// </summary>
        int ResolveQueueDepth()
        {
            int d = numQueueDepthConfig > 0 ? numQueueDepthConfig : DefaultQueueDepth;
            if (d > MaxQueueDepth)
            {
                logger?.LogWarning(
                    "NativeStorageDevice: queue-depth ({depth}) exceeds the io_uring maximum ({max}); clamping.",
                    d, MaxQueueDepth);
                d = MaxQueueDepth;
            }
            return d;
        }

        /// <summary>
        /// Per-ring libaio <c>io_setup</c> reservation depth when <c>--device-queue-depth</c> is left at the
        /// default. Unlike io_uring (per-ring mmap, no global budget), libaio permanently reserves
        /// <c>io-contexts * depth</c> events from the shared <c>fs.aio-max-nr</c> budget at creation, so the
        /// <see cref="DefaultQueueDepth"/> ceiling (4096) over-reserves: a libaio ring never holds more than the
        /// aggregate throttle spread across the rings. We size the reservation to that share —
        /// <c>NextPow2(headroom * ceil(throttle / io-contexts))</c>, floored at <see cref="LibaioReservationFloor"/>,
        /// capped at <see cref="LibaioReservationCap"/> (a single ring should not hold the full deep queue — see
        /// that constant) and at the <paramref name="ceilingDepth"/> (the resolved queue-depth) — so the per-device
        /// reservation is <c>io-contexts * result = min(headroom * throttle, io-contexts * cap)</c>. Multi-ring
        /// serving devices (io-contexts &gt;= 4) keep <c>io-contexts * result &gt;= throttle</c> by construction (the
        /// throttle share is &lt;= the cap), so the full aggregate throttle stays usable (effectiveThrottleLimit is
        /// NOT reduced =&gt; no IOPS cost); low-ring-count auxiliary devices drop to <c>~= io-contexts * cap</c>,
        /// letting many coexist in a stock 65536 budget. Finally the WHOLE-device reservation is hard-capped at
        /// <c>fs.aio-max-nr / AioMaxDevices</c> (default <see cref="DefaultAioMaxDevices"/>) so at least that many
        /// devices always fit the kernel budget regardless of io-contexts / throttle; on a stock 65536 budget this
        /// bounds each device to 2048 events, while a host that sizes fs.aio-max-nr for its workload keeps serving
        /// devices at full depth (e.g. 4194304 / 32 = 131072 per device, which never binds).
        /// </summary>
        int ResolveLibaioReservationDepth(int ringCount, int throttle, int ceilingDepth)
        {
            long share = ((long)throttle + ringCount - 1) / ringCount;   // ceil(throttle / ringCount)
            long depth = Utility.NextPowerOf2(share * LibaioReservationHeadroom);
            if (depth < LibaioReservationFloor) depth = LibaioReservationFloor;
            if (depth > LibaioReservationCap) depth = LibaioReservationCap;
            if (depth > ceilingDepth) depth = ceilingDepth;

            // Hard per-device AIO budget: guarantee at least AioMaxDevices libaio devices fit the global
            // fs.aio-max-nr budget by bounding this device's WHOLE reservation (ringCount * depth), independent
            // of ring count or throttle. Halve the depth (staying a power of two) until it fits; this wins over
            // the soft floor above. The caller then caps effectiveThrottleLimit at ringCount * depth, so
            // aggregate in-flight tracks the (possibly reduced) reservation.
            int maxDevices = AioMaxDevices < 1 ? DefaultAioMaxDevices : AioMaxDevices;
            long perDeviceBudget = GetAioMaxNr() / maxDevices;
            while (depth > 1 && (long)ringCount * depth > perDeviceBudget)
                depth >>= 1;

            return (int)depth;
        }

        /// <summary>
        /// Best-effort read of the global libaio event budget <c>fs.aio-max-nr</c> (distro default 65536, shared
        /// across every process on the machine). Returns the stock 65536 default if the /proc entry is unreadable
        /// (e.g. non-Linux). Never throws. Read at device creation (rare), so not cached.
        /// </summary>
        static long GetAioMaxNr()
        {
            try
            {
                return long.Parse(System.IO.File.ReadAllText("/proc/sys/fs/aio-max-nr").Trim());
            }
            catch
            {
                return 1 << 16; // stock default fallback (best-effort)
            }
        }

        /// <summary>
        /// Best-effort libaio guard: <c>io_setup</c> draws <c>io-contexts * queue-depth</c> events from the
        /// global <c>fs.aio-max-nr</c> budget (distro default 65536, shared across every process). If the
        /// requested total exceeds that budget, warn up front so the operator sees an actionable message
        /// rather than a cryptic <c>io_setup</c> EAGAIN at device creation. io_uring uses per-ring mmap memory
        /// only (no global budget), so this applies to libaio just. Never throws (best-effort /proc read).
        /// </summary>
        void WarnIfLibaioAioBudgetExceeded(int numContexts, int queueDepth)
        {
            if (ioBackendConfig != IoBackend.Libaio && ioBackendConfig != IoBackend.Default)
                return;
            long requested = (long)numContexts * queueDepth;
            long budget = GetAioMaxNr();
            if (requested > budget)
            {
                logger?.LogWarning(
                    "NativeStorageDevice: libaio io-contexts*queue-depth ({req} = {n}*{d}) exceeds the system fs.aio-max-nr budget ({budget}); io_setup may fail. Lower --device-io-contexts or --device-queue-depth, or raise fs.aio-max-nr.",
                    requested, numContexts, queueDepth, budget);
            }
        }

        /// <summary>
        /// Returns the set of IO backends that the currently-loaded native library was built
        /// with. Always includes <see cref="IoBackend.Default"/>; on Linux may also include
        /// <see cref="IoBackend.Uring"/> if the native lib was compiled with FASTER_URING.
        /// </summary>
        public static (bool defaultAvailable, bool uringAvailable) GetAvailableBackends()
        {
            int mask = NativeDevice_AvailableBackends();
            return ((mask & 1) != 0, (mask & 2) != 0);
        }

        /// <summary>
        /// Constructor with more options for derived classes.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The constructor only captures configuration; the underlying native device is created
        /// lazily on the first IO call via <see cref="EnsureNativeDeviceCreated"/>. This lets
        /// callers configure the segment size (if non-default) by calling <see cref="Initialize"/>
        /// in between construction and the first IO, without paying any cost for the native
        /// device creation up-front. Callers that do not call <see cref="Initialize"/> get the
        /// ctor defaults (unbounded single segment, equivalent to <c>Initialize(-1)</c>).
        /// </para>
        /// </remarks>
        /// <param name="filename">File name (or prefix) with path</param>
        /// <param name="deleteOnClose"></param>
        /// <param name="disableFileBuffering"></param>
        /// <param name="capacity">The maximum number of bytes this storage device can accommodate, or CAPACITY_UNSPECIFIED if there is no such limit </param>
        /// <param name="numCompletionThreads">Number of IO completion threads (drainers).
        /// Each drainer is bound 1:1 to its own io_context (libaio) or io_uring ring; the
        /// number of rings equals numCompletionThreads on both backends. Submitters distribute
        /// across rings via per-thread affinity. Ignored on Windows (IOCP). When &lt; 1,
        /// treated as 1.</param>
        /// <param name="ioBackend">IO backend to use (default platform backend, or explicit libaio / io_uring on Linux).</param>
        /// <param name="logger"></param>
        public NativeStorageDevice(string filename,
                                      bool deleteOnClose = false,
                                      bool disableFileBuffering = true,
                                      long capacity = Devices.CAPACITY_UNSPECIFIED,
                                      int numCompletionThreads = 1,
                                      IoBackend ioBackend = IoBackend.Default,
                                      ILogger logger = null,
                                      int numIoContexts = 0,
                                      int queueDepth = 0)
                : base(filename, EnsureParentDirectoryAndProbeSectorSize(filename), capacity)
        {
            Debug.Assert(numCompletionThreads >= 1);

            // The 260-char MAX_PATH limit does not apply to extended-length paths (those prefixed with
            // "\\?\"); only enforce it for normal paths.
            if (!Native32.IsExtendedLengthPath(filename) && filename.Length > Native32.WIN32_MAX_PATH - 11)     // -11 to allow for ".<segment>"
                throw new TsavoriteException($"Path {filename} is too long");

            // Configuration is captured here; the native device handle (and its completion-drainer
            // thread, libaio / io_uring rings, etc.) is created lazily on the first IO call.
            this.filename = filename;
            this.deleteOnClose = deleteOnClose;
            this.disableFileBuffering = disableFileBuffering;
            this.numCompletionThreadsConfig = numCompletionThreads < 1 ? 1 : numCompletionThreads;
            // The number of io_contexts (rings) is decoupled from the number of drainer threads.
            // Each submitter maps to its own ring via the native pick_context thread-affinity, so
            // giving the device more rings than concurrent submitters makes io_submit contention-free
            // (no shared per-context aio ring/completion lock across unrelated submitters) and spreads
            // completion posting across more rings. A small pool of numCompletionThreads drainers then
            // range-drains contiguous slices of the rings. Any explicit value is clamped up to
            // numCompletionThreads so every drainer owns at least one ring.
            //
            // Smart default when the caller leaves numIoContexts unset (<= 0): io_uring is ring-STARVED
            // when rings < submitter concurrency — many submitters serialize on the per-ring submit lock
            // (~3x slower), the single biggest io_uring foot-gun. So default io_uring to a hardware-aware
            // ring count that covers typical submitter concurrency (2x cores, capped at 64 to bound ring
            // memory at ~400 KB/ring). libaio is ring-count-neutral (its kernel io_context mutex is cheap)
            // and its io-contexts x queue-depth draws from the global fs.aio-max-nr budget, so it keeps the
            // conservative rings = drainers default.
            int defaultIoContexts = ioBackend == IoBackend.Uring
                ? Math.Max(this.numCompletionThreadsConfig, Math.Min(2 * Environment.ProcessorCount, 64))
                : this.numCompletionThreadsConfig;
            int requestedIoContexts = numIoContexts <= 0 ? defaultIoContexts : numIoContexts;
            if (requestedIoContexts < this.numCompletionThreadsConfig)
                requestedIoContexts = this.numCompletionThreadsConfig;
            // Optional override to decouple the io_uring ring count from the completion-thread
            // (drainer) count. Giving each submitter thread its own ring (rings >= submitter
            // threads) avoids the shared-ring non-owner submit path. Clamped up to
            // numCompletionThreads so every drainer owns at least one ring.
            if (Environment.GetEnvironmentVariable("GARNET_DEVICE_IO_CONTEXTS") is string ioCtxEnv &&
                int.TryParse(ioCtxEnv, out var envIoContexts) && envIoContexts > 0)
            {
                requestedIoContexts = envIoContexts < this.numCompletionThreadsConfig
                    ? this.numCompletionThreadsConfig : envIoContexts;
                Console.Error.WriteLine($"[io-contexts] GARNET_DEVICE_IO_CONTEXTS override: rings={requestedIoContexts} drainers={this.numCompletionThreadsConfig}");
            }
            this.numIoContextsConfig = requestedIoContexts;
            // Per-ring kernel submission depth D (maxEvents). 0 => DefaultQueueDepth at creation.
            // Orthogonal to ring count (numIoContexts) and aggregate throttle; see ResolveQueueDepth.
            this.numQueueDepthConfig = queueDepth > 0 ? queueDepth : 0;
            this.ioBackendConfig = ioBackend;
            this.logger = logger;

            // Default aggregate in-flight throttle. Unlike the managed in-box devices (which cap at 120),
            // the native device is built for deep NVMe queues, so it defaults to DefaultThrottleLimit (4096).
            // The factory only overrides this when --device-throttle-limit is set (> 0); leaving it here means
            // PerThreadLimit()/init resolve the 4096 default (their `ThrottleLimit > 0 ? ... : DefaultThrottleLimit`
            // fallback is otherwise unreachable). No external consumer reads ThrottleLimit — Throttle() uses the
            // sharded PerThreadLimit() path — so setting it to the intended default here is safe.
            ThrottleLimit = DefaultThrottleLimit;
            _callbackDelegate = _callback;

            // In-flight accounting is sharded per submitter thread to avoid the global cache-line
            // contention profiled at high IOPS. The counter arrays are allocated up front (small,
            // ~32 KB total) because Throttle() may run before the first IO creates the native device.
            shardSubmitted = new long[NumShards * ShardStride];
            shardCompleted = new long[NumShards * ShardStride];
            shardIndex = new ThreadLocal<int>(AssignShard);

            // Per-shard free-list of completion slots, each pre-populated with its shard's contiguous block of
            // slot offsets into results[]. Allocated up front (not lazily per shard) so a completion drainer can
            // safely return a slot to its owning shard's list without racing that list's construction. See
            // shardFreeSlots. results[] itself is allocated lazily on first IO (EnsureNativeDeviceCreated).
            shardFreeSlots = new ConcurrentQueue<int>[NumShards];
            for (int s = 0; s < NumShards; s++)
            {
                var freeList = new ConcurrentQueue<int>();
                int baseOffset = s * SlotsPerShard;
                for (int k = 0; k < SlotsPerShard; k++)
                    freeList.Enqueue(baseOffset + k);
                shardFreeSlots[s] = freeList;
            }
        }

        /// <inheritdoc />
        /// <remarks>
        /// Validates that segmentSize is a positive power of two and at least the device sector
        /// size — the native side enforces the same invariant when it later creates the device,
        /// but failing fast in managed code keeps the error message close to the caller. Like
        /// the base implementation this is purely a configuration call (the ctor already
        /// establishes valid defaults); the underlying native handle is created lazily on the
        /// first IO via <see cref="EnsureNativeDeviceCreated"/> using the final
        /// <c>base.segmentSizeBits</c>, so subsequent calls that change the segment size are
        /// honoured as long as no IO has flowed yet.
        /// <para>
        /// Passing <c>segmentSize = -1</c> selects unbounded single-segment mode: the native
        /// shim is asked to use <see cref="UnboundedNativeSegmentSizeBytes"/> (1&lt;&lt;63) so
        /// every non-negative upper-layer address routes to segment 0 in both the C++ and
        /// managed bit-shift math, and the on-disk layout is a single segment file
        /// (<c>&lt;basename&gt;.0</c>) that grows on demand. When combined with
        /// <paramref name="omitSegmentIdFromFilename"/> = true, the file is named
        /// just <c>&lt;basename&gt;</c> (no segment suffix) — only allowed with
        /// <paramref name="segmentSize"/> = -1, matching the managed devices' behaviour.
        /// </para>
        /// </remarks>
        public override void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
            // Metadata only — matches LocalStorageDevice / RandomAccessLocalStorageDevice. The
            // native handle is created lazily on first IO via EnsureNativeDeviceCreated() using
            // the current base.segmentSizeBits, so repeated calls before the first IO end up
            // creating a native device with the most-recently-requested segment size.
            if (omitSegmentIdFromFilename && segmentSize != -1)
                throw new TsavoriteException("omitSegmentIdFromFilename requires segmentSize = -1 (single unbounded segment); multiple segments would all map to the same on-disk path and clobber each other.");
            if (segmentSize != -1)
            {
                if (segmentSize <= 0 || (segmentSize & (segmentSize - 1)) != 0)
                    throw new TsavoriteException($"Native device segment size must be a positive power of two (or -1 for unbounded); got {segmentSize}.");
                if (segmentSize < SectorSize)
                    throw new TsavoriteException($"Segment size {segmentSize} must be at least the device sector size {SectorSize}.");
            }
            base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
        }

        readonly object nativeCreateLock = new();

        /// <summary>
        /// Lazily creates the native device, spawns completion-drainer threads, and runs the
        /// startup ABI / segment-size / sector-size cross-checks. Uses
        /// <c>segmentSize</c> as the requested segment size — callers may override
        /// the default (-1, unbounded single segment) by calling <see cref="Initialize"/>
        /// before the first IO. Idempotent: subsequent calls are a single non-locking read once
        /// the native handle exists. Thread-safe via double-checked locking. Throws if the
        /// device has been disposed or if the native shim rejects the configuration.
        /// </summary>
        void EnsureNativeDeviceCreated()
        {
            // Pair Volatile.Read here with Volatile.Write below (line ~764). The fast path
            // skips the lock; without an acquire barrier on ARM the reader can observe a
            // non-null nativeDevice while still seeing a stale `results` array reference or
            // partially-initialised completion-thread state. Volatile.Read on weak-memory
            // hosts costs a single ldar instruction, no cost on x86 (plain mov).
            if (Volatile.Read(ref nativeDevice) != IntPtr.Zero) return;
            if (Volatile.Read(ref disposedFlag) != 0)
                throw new ObjectDisposedException(nameof(NativeStorageDevice));
            lock (nativeCreateLock)
            {
                // Inside the lock the acquire fence guarantees we see writes from the prior
                // owner, so a plain field read is safe here.
                if (nativeDevice != IntPtr.Zero) return;
                if (Volatile.Read(ref disposedFlag) != 0)
                    throw new ObjectDisposedException(nameof(NativeStorageDevice));

                ulong sizeForNative = segmentSize == -1
                    ? UnboundedNativeSegmentSizeBytes
                    : (ulong)segmentSize;

                nativeSegmentSizeBytes = sizeForNative;

                // Capture the effective aggregate in-flight throttle T once, here, where ring count N and
                // depth D are also fixed — ThrottleLimit has been applied by the factory before the first IO.
                // Clean split (one duty each): N = io-contexts (ring count), D = queue-depth (per-ring kernel
                // depth), T = throttle-limit (aggregate software backpressure). T is capped at the kernel
                // capacity N*D so aggregate in-flight can never exceed the rings (the correctness invariant
                // that prevents the ring-full submit spin). Throttle() reads effectiveThrottleLimit directly.
                int ringCount = numIoContextsConfig < 1 ? 1 : numIoContextsConfig;
                int requestedThrottle = ThrottleLimit > 0 ? ThrottleLimit : DefaultThrottleLimit;
                int ringDepth = ResolveQueueDepth();

                // libaio io_setup PERMANENTLY reserves ringCount*ringDepth events from the GLOBAL fs.aio-max-nr
                // budget at creation, used or not; the DefaultQueueDepth ceiling (right for io_uring's per-ring
                // mmap SQ) over-reserves for libaio and, with many coexisting devices (e.g. cluster nodes),
                // exhausts a stock 65536 budget => io_setup EAGAIN. When queue-depth is left at the default,
                // size the libaio reservation to the throttle share (ringCount*reservation >= throttle) so the
                // full aggregate throttle is preserved at no IOPS cost while the per-device reservation drops.
                if ((ioBackendConfig == IoBackend.Libaio || ioBackendConfig == IoBackend.Default) && numQueueDepthConfig <= 0)
                    ringDepth = ResolveLibaioReservationDepth(ringCount, requestedThrottle, ringDepth);

                WarnIfLibaioAioBudgetExceeded(ringCount, ringDepth);
                long kernelCapacity = (long)ringCount * ringDepth;
                if (requestedThrottle > kernelCapacity)
                {
                    logger?.LogWarning(
                        "NativeStorageDevice: throttle-limit ({throttle}) exceeds kernel capacity io-contexts*queue-depth ({n}*{d}={cap}); capping aggregate in-flight at that capacity.",
                        requestedThrottle, ringCount, ringDepth, kernelCapacity);
                    requestedThrottle = (int)kernelCapacity;
                }
                effectiveThrottleLimit = requestedThrottle;

                var newDevice = NativeDevice_CreateWithBackend(filename, false, disableFileBuffering, deleteOnClose, (int)ioBackendConfig, sizeForNative, OmitSegmentIdFromFileName, numIoContextsConfig, ringDepth);
                if (newDevice == IntPtr.Zero)
                {
                    var nativeMessage = GetNativeLastError();
                    var available = GetAvailableBackends();
                    var detail = string.IsNullOrEmpty(nativeMessage)
                        ? $"Requested IO backend '{ioBackendConfig}' is not available in the loaded native_device library."
                        : $"Native device initialization failed: {nativeMessage}";
                    throw new TsavoriteException(
                        $"{detail} " +
                        $"Available backends: default={available.defaultAvailable}, io_uring={available.uringAvailable}. " +
                        (ioBackendConfig == IoBackend.Uring
                            ? "The io_uring backend requires liburing.so.2 to be present at process start. " +
                              "Install it (Debian/Ubuntu: 'sudo apt-get install -y liburing2'; Fedora/RHEL: 'sudo dnf install -y liburing'; Alpine: 'apk add liburing') and restart the process. " +
                              "Note: many container runtimes block io_uring_setup via their default seccomp profile (init fails with EPERM); run with a profile that permits io_uring, or use the libaio backend. " +
                              "The libaio backend (selected with IoBackend.Default / IoBackend.Libaio) is always available and does not require liburing."
                            : "Verify the native library matches the requested backend."));
                }

                ulong actualSegmentSize = NativeDevice_GetSegmentSize(newDevice);
                if (actualSegmentSize != sizeForNative)
                {
                    NativeDevice_Destroy(newDevice);
                    throw new TsavoriteException(
                        $"Native device segment size mismatch: requested {sizeForNative}, native returned {actualSegmentSize}. " +
                        "This indicates an ABI mismatch between the loaded native_device library and the managed wrapper. " +
                        "Ensure libnative_device.so matches the current build.");
                }

                uint nativeSectorSize = NativeDevice_sector_size(newDevice);
                if (nativeSectorSize != SectorSize)
                {
                    // Both sides (managed probe in EnsureParentDirectoryAndProbeSectorSize,
                    // native probe in NativeDeviceImpl's field initializer) go through the
                    // same ProbeDioAlignment routine on the same filename with the parent
                    // directory pre-materialised, so the two values are guaranteed to agree
                    // on every well-formed host. A drift here is a real ABI / loaded-library
                    // mismatch — e.g. the shipped libnative_device.so was rebuilt from a
                    // different branch than the managed wrapper, or the host kernel changed
                    // STATX_DIOALIGN semantics between the two calls. Hard-fail so it is
                    // caught at the first I/O rather than silently mis-aligning every write.
                    NativeDevice_Destroy(newDevice);
                    throw new TsavoriteException(
                        $"Native device sector-size mismatch on '{filename}': managed wrapper probed {SectorSize} bytes but the kernel reports {nativeSectorSize} bytes for the actual file. " +
                        "The most likely cause is a stale libnative_device.so or a managed/native version skew. " +
                        "Rebuild the native library from this branch (libs/storage/Tsavorite/cc) and reinstall the resulting binary into libs/storage/Tsavorite/cs/src/core/Device/runtimes/<rid>/native/.");
                }

                if (results == null) results = new NativeResult[MaxResults];

                if (NativeDevice_QueueRun(newDevice, 0) >= 0)
                {
                    try
                    {
                        _ = NativeDevice_NumIoContexts(newDevice);
                        _ = NativeDevice_QueueRunFor(newDevice, 0, 0);
                        _ = NativeDevice_TryCompleteMine(newDevice);
                    }
                    catch (EntryPointNotFoundException ex)
                    {
                        NativeDevice_Destroy(newDevice);
                        throw new TsavoriteException(
                            "Loaded libnative_device.so/dll is missing the sharded-ABI exports " +
                            "NativeDevice_NumIoContexts / NativeDevice_QueueRunFor / NativeDevice_TryCompleteMine. " +
                            "The shared library predates the multi-io-context change and must be rebuilt from this branch " +
                            "(libs/storage/Tsavorite/cc) and the resulting binary installed to " +
                            "libs/storage/Tsavorite/cs/src/core/Device/runtimes/<rid>/native/.", ex);
                    }

                    completionThreadToken = new();
                    int actualIoContexts = NativeDevice_NumIoContexts(newDevice);
                    if (actualIoContexts < 1) actualIoContexts = 1;
                    numRingsActual = actualIoContexts;
                    // Partition the io_contexts (rings) across a small pool of drainer threads. When
                    // actualIoContexts == numCompletionThreadsConfig this reduces to the legacy 1:1
                    // ring-to-drainer binding; when there are more rings than drainers (to de-contend
                    // io_submit), each drainer range-drains a contiguous slice so every ring is still
                    // reaped promptly. Rings are split as evenly as possible; the first `remainder`
                    // drainers get one extra ring.
                    int numDrainers = numCompletionThreadsConfig;
                    if (numDrainers > actualIoContexts) numDrainers = actualIoContexts;
                    completionThreads = new Thread[numDrainers];
                    int baseCount = actualIoContexts / numDrainers;
                    int remainder = actualIoContexts % numDrainers;
                    int nextStart = 0;
                    for (int i = 0; i < numDrainers; i++)
                    {
                        int startCtx = nextStart;
                        int count = baseCount + (i < remainder ? 1 : 0);
                        nextStart += count;
                        completionThreads[i] = new Thread(() => CompletionWorker(startCtx, count))
                        {
                            IsBackground = true
                        };
                        completionThreads[i].Start();
                    }
                }

                // Publish last: a reader observing nativeDevice != IntPtr.Zero is guaranteed to
                // see a fully-initialised handle with completion threads already running.
                Volatile.Write(ref nativeDevice, newDevice);
            }
        }

        /// <inheritdoc />
        /// <remarks>
        /// Mirrors the contract used by <see cref="LocalStorageDevice"/> and
        /// <see cref="ManagedLocalStorageDevice"/>: closes all open segment handles and forgets
        /// them. Subsequent writes lazily reopen segments via the native
        /// <c>FileSystemSegmentedFile::OpenSegment</c> path, so the device remains usable.
        /// </remarks>
        public override void Reset()
        {
            // Lease the native handle (same protocol as ReadAsync/WriteAsync) so a concurrent
            // Dispose() cannot free it while we are inside the native call. A false result means
            // disposal has begun; no-op.
            if (!TryLease(out int shard)) return;
            try
            {
                // No-op if the native device has not been created yet (no handles to reset).
                var dev = Volatile.Read(ref nativeDevice);
                if (dev != IntPtr.Zero)
                    NativeDevice_Reset(dev);
            }
            finally
            {
                ReleaseLease(shard);
            }
        }

        /// <summary>
        /// Asserts that an I/O request is properly aligned for the underlying O_DIRECT / aligned
        /// path. The libaio and io_uring submission paths require that the file offset, the
        /// transfer length, and the user buffer pointer all be multiples of the device's sector
        /// size; misaligned requests fail with EINVAL at completion time.
        /// </summary>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private void ThrowIfMisaligned(ulong offset, uint length, IntPtr buffer, string op)
        {
            uint mask = SectorSize - 1;
            if ((offset & mask) != 0 || (length & mask) != 0 || ((ulong)buffer.ToInt64() & mask) != 0)
                ThrowMisaligned(offset, length, buffer, op);
        }

        // Cold path; NoInlining keeps the AggressiveInlining guard small.
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
        private void ThrowMisaligned(ulong offset, uint length, IntPtr buffer, string op)
        {
            // TsavoriteException (not IOException): this is a precondition/validation failure on the
            // caller-supplied arguments before any kernel submission, consistent with the other
            // configuration guards in this class (segment/sector-size checks). IOException here is
            // reserved for actual kernel I/O completion failures (see the Read/Write callbacks).
            throw new TsavoriteException(
                $"NativeStorageDevice.{op}: misaligned I/O — sector size is {SectorSize}, but " +
                $"offset=0x{offset:X16}, length={length}, buffer=0x{buffer.ToInt64():X16}. " +
                "All three values must be a multiple of the device sector size for the " +
                "O_DIRECT / libaio / io_uring path. This usually indicates an upper-layer " +
                "staging buffer was allocated with the wrong alignment or a flush boundary " +
                "is not on a sector multiple.");
        }

        /// <inheritdoc />
        public override void ReadAsync(int segmentId, ulong sourceAddress,
                                     IntPtr destinationAddress,
                                     uint readLength,
                                     DeviceIOCompletionCallback callback,
                                     object context)
        {
            if (Volatile.Read(ref disposedFlag) != 0)
                throw new ObjectDisposedException(nameof(NativeStorageDevice));
            EnsureNativeDeviceCreated();

            // The libaio/io_uring path requires O_DIRECT-aligned offset, length, AND buffer.
            // Misalignment in release builds would otherwise produce a cryptic EINVAL from the
            // kernel (read returns -EINVAL via the completion callback), or in debug builds
            // hit an assert in DCHECK_ALIGNMENT inside file_linux.cc. Three predicated AND
            // operations is negligible vs the syscall itself.
            ThrowIfMisaligned(sourceAddress, readLength, destinationAddress, nameof(ReadAsync));

            // Sharded, contention-free slot + in-flight accounting. The slot is rented from this submitter's
            // shard free-list and returned only when the IO completes (in _callback or an error path below), so
            // a slow IO's slot is never reused while it is still in flight — the correctness the counter-ring it
            // replaced could not provide under out-of-order completions. The shardSubmitted bump is the in-flight
            // "lease" the Dekker-style Dispose fence and Throttle() observe. See RentSlot / shardFreeSlots.
            int shard = GetShard();
            int offset = RentSlot(shard);
            Interlocked.Increment(ref shardSubmitted[shard * ShardStride]);
            // Fence against a concurrent Dispose that began after the EnsureNativeDeviceCreated check above:
            // if disposal is now visible, balance the submit bump and route the error callback rather than
            // touching a handle Dispose may be about to free. (Dekker-style: Dispose sets the flag then drains
            // in-flight, so either it observes this bump and waits, or we observe the flag here and back out.)
            if (Volatile.Read(ref disposedFlag) != 0)
            {
                Interlocked.Increment(ref shardCompleted[shard * ShardStride]);
                ReturnSlot(offset);
                callback(uint.MaxValue, 0, context);
                return;
            }
            ref var result = ref results[offset];
            result.context = context;
            result.callback = callback;

            try
            {
                int _result = NativeDevice_ReadAsync(nativeDevice, ((ulong)segmentId << segmentSizeBits) | sourceAddress, destinationAddress, readLength, _callbackDelegate, (IntPtr)offset);

                if (_result != 0)
                    throw new IOException($"Error reading from log file (status {_result}){FormatNativeError()}", _result);
            }
            catch (IOException e)
            {
                logger?.LogCritical(e, $"{nameof(ReadAsync)}");
                try
                {
                    callback((uint)(e.HResult & 0x0000FFFF), 0, context, ioException: e);
                }
                finally
                {
                    Interlocked.Increment(ref shardCompleted[shard * ShardStride]);
                    ReturnSlot(offset);
                }
            }
            catch (Exception e)
            {
                logger?.LogCritical(e, $"{nameof(ReadAsync)}");
                try
                {
                    callback(uint.MaxValue, 0, context, ioException: e);
                }
                finally
                {
                    Interlocked.Increment(ref shardCompleted[shard * ShardStride]);
                    ReturnSlot(offset);
                }
            }
        }

        /// <inheritdoc />
        public override unsafe void WriteAsync(IntPtr sourceAddress,
                                      int segmentId,
                                      ulong destinationAddress,
                                      uint numBytesToWrite,
                                      DeviceIOCompletionCallback callback,
                                      object context)
        {
            if (Volatile.Read(ref disposedFlag) != 0)
                throw new ObjectDisposedException(nameof(NativeStorageDevice));
            EnsureNativeDeviceCreated();

            // Same rationale as ReadAsync — see the comment there. Kernel rejects misaligned
            // O_DIRECT writes with EINVAL; we want to surface this in managed code with the
            // actual offsets/lengths/buffer pointers visible so the caller can diagnose
            // whichever upper-layer staging buffer is misaligned.
            ThrowIfMisaligned(destinationAddress, numBytesToWrite, sourceAddress, nameof(WriteAsync));

            // Sharded slot + in-flight accounting; see ReadAsync for the full rationale.
            int shard = GetShard();
            int offset = RentSlot(shard);
            Interlocked.Increment(ref shardSubmitted[shard * ShardStride]);
            if (Volatile.Read(ref disposedFlag) != 0)
            {
                Interlocked.Increment(ref shardCompleted[shard * ShardStride]);
                ReturnSlot(offset);
                callback(uint.MaxValue, 0, context);
                return;
            }
            ref var result = ref results[offset];
            result.context = context;
            result.callback = callback;

            try
            {
                if (s_instrument)
                {
                    Interlocked.Increment(ref submitCount);
                    long inflight = TotalInFlight();
                    var prevPeak = peakNumPending;
                    while (inflight > prevPeak)
                    {
                        var actual = Interlocked.CompareExchange(ref peakNumPending, (int)inflight, prevPeak);
                        if (actual == prevPeak) break;
                        prevPeak = actual;
                    }
                }
                long ts0 = s_instrument ? Stopwatch.GetTimestamp() : 0;
                int _result = NativeDevice_WriteAsync(nativeDevice, sourceAddress, ((ulong)segmentId << segmentSizeBits) | destinationAddress, numBytesToWrite, _callbackDelegate, (IntPtr)offset);
                if (s_instrument)
                {
                    var elapsed = Stopwatch.GetTimestamp() - ts0;
                    Interlocked.Add(ref submitNanos, (long)(elapsed * 1_000_000_000.0 / Stopwatch.Frequency));
                }

                if (_result != 0)
                {
                    throw new IOException($"Error writing to log file (status {_result}){FormatNativeError()}", _result);
                }
            }
            catch (IOException e)
            {
                logger?.LogCritical(e, $"{nameof(WriteAsync)}");
                try
                {
                    callback((uint)(e.HResult & 0x0000FFFF), 0, context, ioException: e);
                }
                finally
                {
                    Interlocked.Increment(ref shardCompleted[shard * ShardStride]);
                    ReturnSlot(offset);
                }
            }
            catch (Exception e)
            {
                logger?.LogCritical(e, $"{nameof(WriteAsync)}");
                try
                {
                    callback(uint.MaxValue, 0, context, ioException: e);
                }
                finally
                {
                    Interlocked.Increment(ref shardCompleted[shard * ShardStride]);
                    ReturnSlot(offset);
                }
            }
        }

        /// <summary>
        /// <see cref="IDevice.RemoveSegment(int)"/>
        /// </summary>
        /// <param name="segment"></param>
        public override void RemoveSegment(int segment)
        {
            if (Volatile.Read(ref disposedFlag) != 0) return;
            // Lease the native handle so a concurrent Dispose() can't free it mid-call.
            if (TryLease(out int shard))
            {
                try
                {
                    var dev = Volatile.Read(ref nativeDevice);
                    if (dev != IntPtr.Zero)
                    {
                        // Native owns the open handle; let it close+unlink.
                        NativeDevice_RemoveSegment(dev, (ulong)segment);
                        return;
                    }
                }
                finally
                {
                    ReleaseLease(shard);
                }
            }
            else
            {
                // Disposal began — match the disposed-at-entry behavior above and no-op.
                return;
            }
            // No native handle yet — delete the on-disk segment file directly so callers
            // observe the same semantics as LocalStorageDevice / RandomAccessLocalStorageDevice
            // (best-effort unlink that ignores ENOENT).
            try { File.Delete(GetSegmentName(segment)); }
            catch { }
        }

        /// <summary>
        /// <see cref="IDevice.RemoveSegmentAsync(int, AsyncCallback, IAsyncResult)"/>
        /// </summary>
        /// <param name="segment"></param>
        /// <param name="callback"></param>
        /// <param name="result"></param>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            RemoveSegment(segment);
            callback(result);
        }

        /// <summary>
        /// Close device. Shutdown ordering matters: any in-flight IOs must complete first so the
        /// in-flight drain terminates; the completion threads must exit BEFORE we destroy the native
        /// device, otherwise they can dereference a freed io_uring/libaio ring inside
        /// <see cref="NativeDevice_QueueRun"/>.
        /// </summary>
        /// <remarks>
        /// <para>Idempotent — multiple calls are safe; only the first does work.</para>
        /// <para>
        /// User IO callbacks fire either on a completion-worker (drainer) thread or inline on a
        /// submitter thread that reaps its own completions via <see cref="TryComplete"/> /
        /// TryCompleteMine (the default affine inline-drain path). Dispose() must NOT be called from
        /// within any such callback: the in-flight drain below waits for that very callback's completion
        /// bump (issued in <c>_callback</c>'s <c>finally</c>), so a self-dispose would spin forever. The
        /// drainer-thread case is detected and thrown as <see cref="InvalidOperationException"/>; the
        /// inline-submitter-thread case cannot be cheaply detected on the hot completion path, so it is
        /// the caller's contract (matching the IDevice lifecycle contract) not to dispose the device
        /// from inside an IO completion callback — post the disposal to a separate thread instead.
        /// </para>
        /// <para>
        /// Worst-case shutdown stall is bounded by the duration of the longest in-flight user
        /// callback: blocked completion drainers are woken immediately by
        /// <see cref="NativeDevice_WakeCompletionWorker"/> rather than waiting on
        /// <see cref="CompletionWorkerTimeoutSecs"/> to fire. If callbacks are slow,
        /// Dispose() waits for them.
        /// </para>
        /// </remarks>
        public override void Dispose()
        {
            // Self-join deadlock guard MUST run before we touch disposedFlag: if a user IO
            // callback (running on a completion thread) calls Dispose(), joining the caller would
            // deadlock. We surface this as InvalidOperationException so disposedFlag stays 0 and
            // a subsequent Dispose() from a different thread can still proceed.
            if (completionThreads != null)
            {
                var self = Thread.CurrentThread;
                foreach (var t in completionThreads)
                {
                    if (ReferenceEquals(t, self))
                    {
                        throw new InvalidOperationException(
                            "NativeStorageDevice.Dispose() called from an IO completion thread. "
                            + "User callbacks must not dispose the device synchronously; "
                            + "post the disposal to a separate thread.");
                    }
                }
            }

            // Idempotent: second and subsequent calls short-circuit. Setting the flag here gates
            // the late P/Invoke entry points (TryComplete, GetFileSize, Reset, RemoveSegment) on
            // their first line; the per-shard lease they then take closes the race where this
            // drain frees the handle between that check and the native call.
            if (Interlocked.Exchange(ref disposedFlag, 1) != 0)
                return;

            // Drain in-flight ops: wait until every shard's submitted count is matched by its completed
            // count. disposedFlag was published above (full barrier); submit/lease paths bump their shard
            // (full barrier) then re-check the flag, so — Dekker-style — either they observe disposal and
            // back out without touching the handle, or this drain observes their bump and waits. The
            // shardCompleted bump for an accepted IO runs in _callback's `finally` after the user callback,
            // so once in-flight reaches 0 all completions (and their user callbacks) have finished.
            while (TotalInFlight() != 0)
                Thread.Yield();

            // Cancel and Join every completion thread, then destroy the native device.
            // Take nativeCreateLock so a concurrent EnsureNativeDeviceCreated cannot publish a
            // brand-new native handle after we have already torn down (which would leak it).
            lock (nativeCreateLock)
            {
                if (completionThreads != null)
                {
                    completionThreadToken.Cancel();
                    // Wake every blocked completion drainer by submitting a no-op IO to each
                    // io_context. A drainer parks in NativeDevice_QueueRunFor on the first ring of
                    // its range, so wake every ring [0, numRingsActual): waking a ring that no
                    // drainer is currently parked on is harmless (the no-op event is reaped on the
                    // next drain pass). Best-effort: on submit failure the drainer still wakes when
                    // its QueueRunFor timeout fires.
                    for (int i = 0; i < numRingsActual; i++)
                        _ = NativeDevice_WakeCompletionWorker(nativeDevice, i);
                    foreach (var t in completionThreads) t.Join();
                    completionThreadToken.Dispose();
                    completionThreads = null;
                }

                var dev = Interlocked.Exchange(ref nativeDevice, IntPtr.Zero);
                if (dev != IntPtr.Zero)
                {
                    NativeDevice_Destroy(dev);
                    // NativeDevice_Destroy runs log_.Close() under the C ABI firewall; if that threw
                    // it was caught and recorded rather than crashing teardown. Surface it so a
                    // teardown failure is reported instead of looking like "the server hung".
                    var err = GetNativeLastError();
                    if (!string.IsNullOrEmpty(err))
                        logger?.LogError("NativeStorageDevice native teardown reported an error: {error}", err);
                }
            }
        }

        /// <inheritdoc/>
        public override bool TryComplete()
        {
            // Lease the native handle so a concurrent Dispose() can't free it mid-call. TryLease
            // rejects the post-dispose case and closes the race where Dispose frees the handle
            // between the flag check and the native call.
            if (!TryLease(out int shard))
                return false;
            try
            {
                var dev = Volatile.Read(ref nativeDevice);
                if (dev == IntPtr.Zero)
                    return false;
                return NativeDevice_TryComplete(dev);
            }
            finally
            {
                ReleaseLease(shard);
            }
        }

        /// <summary>
        /// Drain only the calling thread's affine native context/ring (the one its submits land on),
        /// instead of walking every context like <see cref="TryComplete"/>. The inline submitter-thread
        /// completion path (Tsavorite CompletePending / AsyncGetFromDisk throttle-wait) is the primary
        /// reaper at high IOPS; polling just this thread's own context issues one io_getevents per poll
        /// rather than one per context, cutting completion-drain syscalls (and the cross-context aio
        /// ring-lock contention) by roughly the context count. All contexts stay covered because each
        /// has sharing submitters and/or a dedicated completion (drainer) thread.
        /// </summary>
        public override bool TryCompleteMine()
        {
            if (!TryLease(out int shard))
                return false;
            try
            {
                var dev = Volatile.Read(ref nativeDevice);
                if (dev == IntPtr.Zero)
                    return false;
                return NativeDevice_TryCompleteMine(dev);
            }
            finally
            {
                ReleaseLease(shard);
            }
        }

        /// <inheritdoc/>
        public override long GetFileSize(int segment)
        {
            if (Volatile.Read(ref disposedFlag) != 0) return 0;
            // Lease the native handle so a concurrent Dispose() can't free it mid-call.
            if (TryLease(out int shard))
            {
                try
                {
                    var dev = Volatile.Read(ref nativeDevice);
                    if (dev != IntPtr.Zero)
                        return (long)NativeDevice_GetFileSize(dev, (ulong)segment);
                }
                finally
                {
                    ReleaseLease(shard);
                }
            }
            // No native handle yet (or disposed) — stat the on-disk segment file directly. Matches
            // LocalStorageDevice / RandomAccessLocalStorageDevice semantics where size is
            // observable before any IO has flowed through the device. Returns 0 for missing
            // files (the cluster manager and checkpoint-recovery code rely on this to decide
            // whether to recover persisted config without first opening the device).
            try
            {
                var fi = new FileInfo(GetSegmentName(segment));
                return fi.Exists ? fi.Length : 0;
            }
            catch
            {
                return 0;
            }
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="segmentId"></param>
        /// <returns></returns>
        protected string GetSegmentName(int segmentId) => GetSegmentFilename(FileName, segmentId);

        /// <summary>
        /// Cold-path probe of the kernel's required direct-I/O alignment for the target file.
        /// Called from the ctor (via base.ctor) before the native device is created;
        /// StorageDeviceBase.SectorSize is set in the base ctor and immutable thereafter.
        /// </summary>
        /// <remarks>
        /// Never throws: on any failure returns <see cref="MinSectorSize"/> (512). A stale or
        /// wrong probe is caught when <see cref="EnsureNativeDeviceCreated"/> later cross-checks
        /// the managed SectorSize against the value the native shim reports for the actual file.
        /// </remarks>
        private static uint GetSectorSize(string filename)
        {
            // The probe is a no-op on Windows (returns MinSectorSize) because the
            // libnative_device.so / .dll on Windows uses the ThreadPool backend and the
            // file_windows.cc path already queries the actual sector size via
            // GetDiskFreeSpace. Linux is where the 4K-native distinction matters.
            try
            {
                // The probe walks up to the nearest existing ancestor, so it's safe to call
                // even when `filename` itself doesn't exist yet (which is the common case at
                // startup before any segment has been written).
                uint probed = NativeDevice_ProbeAlignment(filename);
                if (probed >= MinSectorSize && (probed & (probed - 1)) == 0)
                    return probed;
            }
            catch (DllNotFoundException) { }
            catch (EntryPointNotFoundException) { }
            return MinSectorSize;
        }

        /// <summary>
        /// Materializes the parent directory of <paramref name="filename"/> (if missing) and
        /// then probes the kernel's required direct-I/O alignment for the eventual data
        /// file. Returns the value once and for all — the result is passed to the
        /// <see cref="StorageDeviceBase"/> ctor argument and the resulting
        /// <see cref="IDevice.SectorSize"/> is immutable for the lifetime of the device.
        /// </summary>
        /// <remarks>
        /// The probe is deterministic by construction:
        ///   1. <see cref="Directory.CreateDirectory(string)"/> ensures the parent exists on
        ///      the target filesystem before the probe runs.
        ///   2. The probe call is given the parent directory's path (not the not-yet-existing
        ///      data file), so <c>stat()</c> succeeds on the first try and never walks up to
        ///      a grandparent on a different filesystem.
        /// Together these collapse both the managed-side probe and the later native-side
        /// probe (run from the <c>NativeDeviceImpl</c> field initializer with the same
        /// parent dir present) onto the same kernel STATX_DIOALIGN / sysfs queue-block-size
        /// value, so the cross-check in <see cref="EnsureNativeDeviceCreated"/> is a real
        /// ABI / loaded-library drift detector with no path-resolution false positives.
        /// </remarks>
        private static uint EnsureParentDirectoryAndProbeSectorSize(string filename)
            => GetSectorSize(MaterializeParentDirForProbe(filename));

        /// <summary>
        /// Creates <paramref name="filename"/>'s parent directory (if missing) and returns it as
        /// the probe path — probing the existing parent (not the lazily-created data file) keeps
        /// the probe on the target filesystem.
        /// </summary>
        private static string MaterializeParentDirForProbe(string filename)
        {
            try
            {
                var parent = new FileInfo(filename).Directory?.FullName;
                if (!string.IsNullOrEmpty(parent))
                {
                    Directory.CreateDirectory(parent);
                    return parent;
                }
            }
            catch
            {
                // Not fatal — fall back to the filename; the probe walks up to an ancestor.
            }
            return filename;
        }

        /// <summary>
        /// Required O_DIRECT alignment shared by all local-disk devices (native, RandomAccess,
        /// Managed, Local) so they agree per host. Power of two &gt;= <see cref="IDevice.MinDeviceSectorSize"/>.
        /// </summary>
        /// <remarks>
        /// Linux: the native probe (<c>NativeDevice_ProbeAlignment</c>), falling back to
        /// <see cref="Native32.GetDeviceSectorSize"/> (512) when the native library can't load
        /// (e.g. musl). Windows: <see cref="Native32.GetDeviceSectorSize"/> (GetDiskFreeSpace
        /// logical), taking no native-library dependency.
        /// </remarks>
        internal static uint ProbeSectorSize(string filename)
        {
            var probePath = MaterializeParentDirForProbe(filename);

            uint result = 0;
            if (OperatingSystem.IsLinux())
            {
                try
                {
                    uint probed = NativeDevice_ProbeAlignment(probePath);
                    if (probed >= MinSectorSize && (probed & (probed - 1)) == 0)
                        result = probed;
                }
                catch (DllNotFoundException) { }
                catch (EntryPointNotFoundException) { }
                // A present-but-broken native lib (wrong-arch/corrupt) throws these; managed
                // devices don't require it, so fall back to the managed probe instead of failing.
                catch (BadImageFormatException) { }
                catch (FileLoadException) { }
            }

            if (result == 0)
                result = Native32.GetDeviceSectorSize(probePath);

            // Final guard: power-of-two floor at MinDeviceSectorSize.
            if (result < MinSectorSize || (result & (result - 1)) != 0)
                result = MinSectorSize;
            return result;
        }

        /// <summary>
        /// Drain loop for one completion thread. The thread owns the contiguous range of ring
        /// shards <c>[startCtx, startCtx + ctxCount)</c>. For a single ring it blocks in
        /// <c>NativeDevice_QueueRunFor</c> with a long timeout (legacy fast path). For a range it
        /// polls every ring non-blocking (timeout 0) and, only when the whole pass is idle, blocks
        /// on the first ring with the timeout so it sleeps instead of busy-spinning. Dispose() wakes
        /// blocked workers via <c>NativeDevice_WakeCompletionWorker</c> rather than relying on the
        /// timeout to fire.
        /// </summary>
        void CompletionWorker(int startCtx, int ctxCount)
        {
            // Defense-in-depth: catch around the whole drain loop. _callback already swallows
            // all exceptions from the user callback (see its big comment), but if anything
            // else managed-side throws here (e.g. nativeDevice goes IntPtr.Zero mid-call
            // during a race with Dispose, or a P/Invoke marshalling exception), losing the
            // drainer thread silently is catastrophic: no completions ever fire, in-flight
            // grows unbounded, the next submitter spins forever in device.Throttle() and the
            // whole engine deadlocks. So if anything escapes, log it loudly and exit cleanly.
            try
            {
                // Consecutive idle poll-passes for the multi-ring path below; reset on any drained event.
                long idleSpins = 0;
                while (true)
                {
                    if (completionThreadToken.IsCancellationRequested) break;

                    if (ctxCount <= 1)
                    {
                        // Single ring: block directly on it with the timeout (legacy fast path).
                        int rc = NativeDevice_QueueRunFor(nativeDevice, startCtx, CompletionWorkerTimeoutSecs);
                        if (rc == NativeCABIExceptionSentinel)
                        {
                            // The native drain threw and was firewalled by the C ABI guard (instead of
                            // unwinding across P/Invoke and terminating the process). Surface the message
                            // so it can be reported, then pause briefly to avoid a hot error loop if the
                            // fault is persistent. The drainer keeps running so Dispose can still proceed.
                            logger?.LogError("NativeStorageDevice completion drainer (startCtx={startCtx}) hit a native exception: {error}", startCtx, GetNativeLastError());
                            Thread.Sleep(10);
                        }
                        Thread.Yield();
                        continue;
                    }

                    // Range of rings: poll each one non-blocking (timeout 0) so a completion on any
                    // ring in the range is reaped promptly. A blocking io_getevents parks on a SINGLE
                    // context, which would hide completions on the sibling rings for the whole timeout
                    // — fatal for the low-in-flight write/flush path (a stalled flush completion blocks
                    // the memory-bounded log). So this path NEVER blocks on one ring: under saturation
                    // every pass drains events and we re-poll immediately (max throughput, lowest
                    // latency); on a brief idle we yield; only on sustained idle do we sleep 1ms to
                    // release the core, and even then the next pass re-polls the ENTIRE range so no ring
                    // waits more than ~1ms. Dispose is observed within one pass (no Wake dependency here).
                    int drained = 0;
                    bool faulted = false;
                    for (int k = 0; k < ctxCount; k++)
                    {
                        int rc = NativeDevice_QueueRunFor(nativeDevice, startCtx + k, 0);
                        if (rc == NativeCABIExceptionSentinel)
                            faulted = true;
                        else if (rc > 0)
                            drained += rc;
                    }
                    if (faulted)
                    {
                        logger?.LogError("NativeStorageDevice completion drainer (startCtx={startCtx}, count={ctxCount}) hit a native exception: {error}", startCtx, ctxCount, GetNativeLastError());
                        Thread.Sleep(10);
                        idleSpins = 0;
                    }
                    else if (drained > 0)
                    {
                        idleSpins = 0;      // stay hot: re-poll immediately
                    }
                    else if (++idleSpins < CompletionWorkerIdleSpinBudget)
                    {
                        Thread.Yield();     // brief idle: stay responsive, keep every ring visible
                    }
                    else
                    {
                        Thread.Sleep(1);    // sustained idle: release the core (re-polls whole range on wake)
                    }
                }
            }
            catch (Exception ex)
            {
                logger?.LogCritical(ex, "NativeStorageDevice completion drainer (startCtx={startCtx}) terminated by unhandled exception", startCtx);
            }
        }

        // Per-iteration timeout for completion workers. Long enough that the idle syscall rate is
        // negligible; Dispose() does not rely on this firing because it submits a synthetic wake-up
        // event via NativeDevice_WakeCompletionWorker to unblock the worker immediately.
        const int CompletionWorkerTimeoutSecs = 1;

        // Multi-ring drainers (numIoContexts > numCompletionThreads) never block in a syscall (that
        // would hide sibling rings). Instead they poll; after this many consecutive fully-idle passes
        // they switch from Thread.Yield() to Thread.Sleep(1) to release the core. Under a saturated
        // read workload the idle branch is never taken, so this only bounds CPU when the device is
        // quiescent (e.g. between log-flush completions during load).
        const long CompletionWorkerIdleSpinBudget = 1024;
    }
}