// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using BenchmarkDotNet.Attributes;
using Embedded.server;
using Garnet.server;
using Tsavorite.core;

namespace BDN.benchmark.Operations.LTM
{
    /// <summary>
    /// Larger-than-memory equivalent of <see cref="BDN.benchmark.Operations.RawStringOperations"/>.
    /// <para>
    /// The store is configured with a tiny in-memory log (a buffer of <see cref="MemoryPages"/> pages of
    /// <see cref="PageSizeBytes"/> bytes each) tiered to a <see cref="DeviceType.LocalMemory"/> device. Setup then populates
    /// <see cref="PopulatePageCount"/> pages worth of fresh keys, so all but the last few pages are evicted to the device.
    /// Each benchmarked batch issues its operations across the entire populated key range, so almost every operation
    /// touches a record that resides on the device — exercising the larger-than-memory (pending IO) path.
    /// </para>
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class RawStringOperations : OperationsBase
    {
        /// <summary>Main-log page size. 4 KB is the minimum supported page size.</summary>
        const int PageSizeBytes = 4096;

        /// <summary>Number of pages kept in memory (the in-memory buffer). 4 pages * 4 KB = 16 KB.</summary>
        const int MemoryPages = 4;

        /// <summary>Number of pages worth of records to populate, so ~96% of the data is evicted to the device.</summary>
        const int PopulatePageCount = 100;

        /// <summary>Prefix for populated (present) keys.</summary>
        const string KeyPrefix = "k:";

        /// <summary>Prefix for keys that are never populated (used by the not-found benchmark).</summary>
        const string MissingPrefix = "z:";

        /// <summary>Number of distinct keys that were populated to fill <see cref="PopulatePageCount"/> pages.</summary>
        int keyCount;

        /// <summary>Number of digits used to format every key id as a fixed-width string (sized to hold <see cref="keyCount"/>).
        /// Fixed-width keys let us overwrite a command's key in place with a freshly chosen random id without shifting bytes.</summary>
        int keyDigits;

        /// <summary>xorshift64* state for fast per-operation random key selection. BDN is single-threaded, so no locking is needed.</summary>
        ulong rngState = 0x9E3779B97F4A7C15UL;

        RandomKeyBatch set, setex, setnx, setxx, getf, getnf, incr, decr, incrby, decrby;

        /// <summary>
        /// A pre-built batch of <see cref="OperationsBase.batchSize"/> identical fixed-width commands whose key digits are
        /// rewritten with random ids before each Send, so almost every operation misses memory and goes pending.
        /// </summary>
        struct RandomKeyBatch
        {
            public Request request;
            /// <summary>Byte offset of the first key digit within the first command in <see cref="request"/>.</summary>
            public int firstKeyDigitOffset;
            /// <summary>Byte length of one command (the stride between successive keys in the batch buffer).</summary>
            public int commandLength;
        }

        protected override void ConfigureServerOptions(GarnetServerOptions opts)
        {
            // Larger-than-memory: a tiny in-memory log (MemoryPages * PageSizeBytes) tiered to a LocalMemoryDevice.
            // The bulk of the populated records live on the device, so operations exercise the device IO path.
            opts.EnableStorageTier = true;
            opts.DeviceType = DeviceType.LocalMemory;
            opts.PageSize = $"{PageSizeBytes}";
            opts.LogMemorySize = $"{MemoryPages * PageSizeBytes}";
            // Note: SegmentSize is left at its default; for LocalMemory the device segment size matches the log segment
            // size, and the populated working set (~PopulatePageCount pages) occupies a single segment.
        }

        public override void GlobalSetup()
        {
            // Pin the process to a single NUMA socket BEFORE base.GlobalSetup() creates the server (and the
            // device's completion thread), so the run-thread and that completion thread stay co-located.
            PinProcessToSingleSocket();

            base.GlobalSetup();

            // Fixed-width keys must be sized before populating (Populate uses Key(id)). Use an upper bound on the key
            // count: every record is at least MinRecordBytes (the RecordInfo header alone is 8 bytes), so
            // targetBytes / MinRecordBytes is >= the number of keys that fit, and its digit count is therefore >= the
            // digit count of the actual keyCount. So every id in [0, keyCount) formats to exactly keyDigits characters.
            const int MinRecordBytes = 8;
            keyDigits = NumDigits((long)PopulatePageCount * PageSizeBytes / MinRecordBytes);

            // Populate PopulatePageCount pages of fresh keys; once memory (MemoryPages) is full the older pages are
            // evicted to the device, so only the last few pages remain in memory.
            keyCount = Populate();

            // Evict everything below the tail to the device so the benchmark starts from a cold, device-resident state.
            server.StoreWrapper.store.Log.FlushAndEvict(wait: true);

            SetupBatch(ref set, KeyPrefix, id => Resp("SET", Key(id), "0"));
            SetupBatch(ref setex, KeyPrefix, id => Resp("SETEX", Key(id), "9", "0"));
            SetupBatch(ref setnx, KeyPrefix, id => Resp("SET", Key(id), "0", "NX"));   // Becomes SETEXNX rather than SETNX
            SetupBatch(ref setxx, KeyPrefix, id => Resp("SET", Key(id), "0", "XX"));   // Becomes SETEXXX rather than SETXX
            SetupBatch(ref getf, KeyPrefix, id => Resp("GET", Key(id)));
            SetupBatch(ref getnf, MissingPrefix, id => Resp("GET", MissingKey(id)));
            SetupBatch(ref incr, KeyPrefix, id => Resp("INCR", Key(id)));
            SetupBatch(ref decr, KeyPrefix, id => Resp("DECR", Key(id)));
            SetupBatch(ref incrby, KeyPrefix, id => Resp("INCRBY", Key(id), "1234567890"));
            SetupBatch(ref decrby, KeyPrefix, id => Resp("DECRBY", Key(id), "1234567890"));
        }

        /// <summary>
        /// Pin the whole benchmark process to a single NUMA socket so the run-thread and the
        /// <see cref="DeviceType.LocalMemory"/> device's completion ("processor") thread stay co-located on
        /// cores that share an L3 cache.
        /// <para>
        /// Why: each pending IO is handed off run-thread -&gt; SPSC ring -&gt; device completion thread -&gt;
        /// readyResponses drain (back on the run-thread). If the OS places those two threads on different
        /// sockets, every op's handoff bounces cache lines across the socket interconnect (~1.4-1.9x slower),
        /// which shows up as bimodal, sticky-per-launch run-to-run variance in these LTM benchmarks.
        /// </para>
        /// <para>
        /// Setting <em>process</em> affinity (rather than just the run-thread) is required: the device creates
        /// its completion thread separately, and a new thread inherits the process affinity mask, not the
        /// creating thread's. Setting the process mask here (before the server/device is created) constrains
        /// the current run-thread and every thread created afterwards (the completion thread, GC threads) to
        /// the one socket. Windows-only (the affinity API is unsupported elsewhere); a no-op on other OSes.
        /// </para>
        /// <para>
        /// TODO: Investigate Option B — pin individual threads to specific cores instead of the whole process,
        /// via <see cref="Native32.AffinitizeThreadShardedNuma"/>: call it on the run-thread here, and add a
        /// LocalMemoryDevice option to affinitize its ProcessorLoop thread to a different core on the same
        /// socket. That avoids constraining unrelated threads (e.g. GC), but needs a device change to expose
        /// completion-thread affinity.
        /// </para>
        /// <para>
        /// TODO: Because this pin is Windows-only, the LTM benchmark has been removed from the BDN CI perf gate
        /// (ci-bdnbenchmark.yml test matrix + BDN_Benchmark_Config.json) — on the Linux CI it would run unpinned
        /// and the bimodal cross-socket variance would flake the gate. Restore it to CI once cross-platform
        /// pinning (Option B and/or Linux affinity support) keeps the LTM numbers stable on the CI runners.
        /// </para>
        /// </summary>
        private static void PinProcessToSingleSocket()
        {
            if (!OperatingSystem.IsWindows())
                return;

            // Assume two NUMA sockets with contiguous logical-processor enumeration (procs [0, N/2) = socket 0),
            // matching Tsavorite's Native32.AffinitizeThreadShardedNuma(_, 2) convention. Assumes a single
            // processor group (<= 64 logical processors), so a single IntPtr mask suffices.
            var socketProcs = Environment.ProcessorCount / 2;
            if (socketProcs is < 1 or > 63)
                return;

            var mask = (1L << socketProcs) - 1;
            using var process = Process.GetCurrentProcess();
            process.ProcessorAffinity = (nint)mask;
        }

        /// <summary>
        /// Populate fresh keys (each set to "0") until <see cref="PopulatePageCount"/> pages have been appended to the log.
        /// Returns the number of keys populated.
        /// </summary>
        int Populate()
        {
            var log = server.StoreWrapper.store.Log;
            var initialTail = log.TailAddress;
            var targetBytes = (long)PopulatePageCount * PageSizeBytes;

            var id = 0;
            while (log.TailAddress - initialTail < targetBytes)
            {
                SlowConsumeMessage(Encoding.ASCII.GetBytes(Resp("SET", Key(id), "0")));
                id++;
            }
            return id;
        }

        /// <summary>Fixed-width present key, e.g. "k:00042"; used by both populate and query so the bytes match exactly.</summary>
        string Key(long id) => KeyPrefix + id.ToString("D" + keyDigits);

        /// <summary>Fixed-width never-populated key, e.g. "z:00042"; used by the not-found benchmark.</summary>
        string MissingKey(long id) => MissingPrefix + id.ToString("D" + keyDigits);

        /// <summary>Build a RESP array command from bulk-string arguments.</summary>
        static string Resp(params string[] args)
        {
            var sb = new StringBuilder();
            _ = sb.Append('*').Append(args.Length).Append("\r\n");
            foreach (var arg in args)
                _ = sb.Append('$').Append(arg.Length).Append("\r\n").Append(arg).Append("\r\n");
            return sb.ToString();
        }

        /// <summary>
        /// Build a request buffer containing <see cref="OperationsBase.batchSize"/> identical fixed-width commands (key id 0),
        /// and record where each command's key digits live so <see cref="SendRandomized"/> can overwrite them with random ids
        /// before each Send. Because keys are fixed-width every command has the same length, so the key sits at a constant
        /// offset within the first command and recurs at a constant stride.
        /// </summary>
        void SetupBatch(ref RandomKeyBatch batch, string keyPrefix, Func<long, string> makeCommand)
        {
            // id 0 formats to all-zero key digits; this is the placeholder we locate now and overwrite per Send.
            var template = makeCommand(0);
            var keyPlaceholder = keyPrefix + new string('0', keyDigits);
            var keyIndex = template.IndexOf(keyPlaceholder, StringComparison.Ordinal);
            Debug.Assert(keyIndex >= 0, "Key placeholder not found in command template");

            var bytes = Encoding.ASCII.GetBytes(template);
            batch.commandLength = bytes.Length;
            batch.firstKeyDigitOffset = keyIndex + keyPrefix.Length;

            batch.request.buffer = GC.AllocateArray<byte>(bytes.Length * batchSize, pinned: true);
            for (var i = 0; i < batchSize; i++)
                bytes.CopyTo(batch.request.buffer.AsSpan(i * bytes.Length));
            batch.request.bufferPtr = (byte*)Unsafe.AsPointer(ref batch.request.buffer[0]);
        }

        [Benchmark]
        public void Set() => SendRandomized(ref set);

        [Benchmark]
        public void SetEx() => SendRandomized(ref setex);

        [Benchmark]
        public void SetNx() => SendRandomized(ref setnx);

        [Benchmark]
        public void SetXx() => SendRandomized(ref setxx);

        [Benchmark]
        public void GetFound() => SendRandomized(ref getf);

        [Benchmark]
        public void GetNotFound() => SendRandomized(ref getnf);

        [Benchmark]
        public void Increment() => SendRandomized(ref incr);

        [Benchmark]
        public void Decrement() => SendRandomized(ref decr);

        [Benchmark]
        public void IncrementBy() => SendRandomized(ref incrby);

        [Benchmark]
        public void DecrementBy() => SendRandomized(ref decrby);

        /// <summary>Overwrite every command's key in the batch with a freshly chosen random id, then send the batch.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SendRandomized(ref RandomKeyBatch batch)
        {
            var p = batch.request.bufferPtr + batch.firstKeyDigitOffset;
            for (var i = 0; i < batchSize; i++)
            {
                WriteKeyDigits(p, NextRandomKeyId());
                p += batch.commandLength;
            }
            Send(batch.request);
        }

        /// <summary>Write <see cref="keyDigits"/> decimal digits of <paramref name="id"/> (zero-padded) starting at <paramref name="p"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void WriteKeyDigits(byte* p, long id)
        {
            for (var i = keyDigits - 1; i >= 0; i--)
            {
                p[i] = (byte)('0' + (int)(id % 10));
                id /= 10;
            }
        }

        /// <summary>Fast single-threaded random key id in [0, keyCount) via xorshift64*.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long NextRandomKeyId()
        {
            var x = rngState;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            rngState = x;
            return (long)((x * 0x2545F4914F6CDD1DUL) % (ulong)keyCount);
        }

        /// <summary>Number of decimal digits needed to represent <paramref name="value"/> (minimum 1).</summary>
        static int NumDigits(long value)
        {
            var digits = 1;
            while (value >= 10)
            {
                value /= 10;
                digits++;
            }
            return digits;
        }
    }
}