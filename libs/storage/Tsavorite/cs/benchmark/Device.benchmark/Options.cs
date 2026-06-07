// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using CommandLine;
using Tsavorite.core;

namespace Device.benchmark
{
    public class Options
    {
        [Option("file-size", Required = false, Default = 1L << 30, HelpText = "File size (bytes)")]
        public long FileSize { get; set; }

        [Option("sector-size", Required = false, Default = 512, HelpText = "Sector size")]
        public int SectorSize { get; set; }

        [Option("file-name", Required = false, Default = "c:/data/test.dat", HelpText = "File name")]
        public string FileName { get; set; }

        [Option("device-type", Required = false, Default = DeviceType.Native, HelpText = "Device type (Native, FileStream, RandomAccess, LocalMemory). For LocalMemory, --file-name and --io-backend are ignored.")]
        public DeviceType DeviceType { get; set; }

        [Option("throttle-limit", Required = false, Default = 0, HelpText = "Max device-level in-flight ops (0 = no throttle). Note: for Native libaio the kernel io_context is only 128 slots wide — running with --throttle-limit 0 plus high QD (threads × batch > 128) floods the ring and the kernel returns EAGAIN per request (surfaced as Status::IOError=4). The benchmark reports these as errors; throughput uses successful completions only. Set to 128 (matches both the libaio io_context capacity and the io_uring SQ depth this build uses) to avoid flood.")]
        public int ThrottleLimit { get; set; }

        [Option("completion-threads", Required = false, Default = 0, HelpText = "Number of background drainer threads that wait on IO completions (0 = processor count on Windows, 1 on Linux). On Linux Native, each drainer is bound 1:1 to its own kernel io_context (libaio) or io_uring ring; submitters distribute across contexts/rings via per-thread affinity. For DeviceType.LocalMemory, each drainer owns one SPSC ring fed by one submitter via per-thread routing. Throughput scales with this value up to the available submitter concurrency.")]
        public int CompletionThreads { get; set; }

        [Option("io-backend", Required = false, Default = "default", HelpText = "Linux Native IO backend: default, libaio, uring. Ignored on other devices/OSes. Unknown values are rejected at startup.")]
        public string IoBackend { get; set; }

        [Option("segment-size", Required = false, Default = 1L << 30, HelpText = "Segment size (bytes)")]
        public long SegmentSize { get; set; }

        [Option('b', "batch-size", Separator = ',', Required = false, Default = new[] { 1024 }, HelpText = "Batch size, number of requests (comma separated)")]
        public IEnumerable<int> BatchSize { get; set; }

        [Option("runtime", Required = false, Default = 15, HelpText = "Run time per benchmark (seconds)")]
        public int Runtime { get; set; }

        [Option('t', "threads", Separator = ',', Default = new[] { 1, 2, 4, 8, 16, 32 }, HelpText = "Number of threads (comma separated)")]
        public IEnumerable<int> NumThreads { get; set; }

        [Option("mode", Required = false, Default = "raw", HelpText = "Worker model: 'raw' (random-sector reads, device ceiling) or 'keyed' (dictionary key->address + readyResponses handoff modelling Tsavorite Read()->CompletePending(), no Tsavorite engine).")]
        public string Mode { get; set; }

        [Option("keys", Required = false, Default = 10_000_000L, HelpText = "Keyed mode: number of keys in the shared key->address dictionary.")]
        public long Keys { get; set; }

        [Option("copy-out", Required = false, Default = false, HelpText = "Keyed mode: memcpy the read sector into a per-op output buffer in the completion continuation (models Tsavorite's Reader copy).")]
        public bool CopyOut { get; set; }

        [Option("epoch", Required = false, Default = false, HelpText = "Keyed mode: Resume/Suspend a shared LightEpoch per op (and ProtectAndDrain in CompletePending), exactly like BasicContext.Read. Isolates the epoch-protection cost.")]
        public bool Epoch { get; set; }

        [Option("epoch-hold", Required = false, Default = false, HelpText = "Keyed mode: hold the shared LightEpoch across the whole chunk (Resume once per chunk, ProtectAndDrain per drain, Suspend once) instead of per op. Tests whether batching epoch protection restores scalability.")]
        public bool EpochHold { get; set; }

        [Option("hash-index", Required = false, Default = false, HelpText = "Keyed mode: resolve key->address through a shared open-addressing hash table (models Tsavorite's hash index walk) instead of a direct array.")]
        public bool HashIndex { get; set; }

        [Option("big-ctx", Required = false, Default = false, HelpText = "Keyed mode: insert/remove a ~192-byte struct per op in a per-worker dictionary (models the PendingContext stored in ioPendingRequests).")]
        public bool BigCtx { get; set; }

        [Option("pool-ctx", Required = false, Default = false, HelpText = "Keyed mode: rent/return a per-op context object from a per-worker pool (models the pooled AsyncIOContext).")]
        public bool PoolCtx { get; set; }

        [Option("buf-pool", Required = false, Default = false, HelpText = "Keyed mode: rent/return the read buffer from a SHARED pool per op (models Tsavorite's single per-allocator SectorAlignedBufferPool) instead of a per-op fixed buffer.")]
        public bool BufPool { get; set; }

        [Option("buf-pool-tls", Required = false, Default = false, HelpText = "Keyed mode: like --buf-pool but the pool is PER-WORKER (models a per-session buffer pool). Tests the fix: same Get/Return churn, no cross-thread sharing.")]
        public bool BufPoolTls { get; set; }
    }
}