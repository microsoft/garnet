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
    /// Cost of a <c>GET</c> that misses memory and has to complete a pending read.
    /// <para>
    /// This is the path <see cref="BDN.benchmark.Operations.LTM.RawStringOperations"/> cannot measure. That
    /// benchmark sets <see cref="GarnetServerOptions.DeviceCompletionThreads"/> to 0, which completes every
    /// read inline on the submitting thread, so the session's pending completion never actually waits and the
    /// suspension machinery is compiled in but never exercised. Here completions are delivered by a device
    /// thread, so the read genuinely completes elsewhere and the session genuinely suspends and resumes.
    /// </para>
    /// <para>
    /// <see cref="Reads"/> sweeps the number of pipelined misses in one batch. <c>Reads=1</c> is an isolated
    /// miss: one whole suspend/resume round trip, including the cross-thread handoff, which is the latency a
    /// client sees for a single miss. Larger values are the pipelined case. Dividing the reported time by
    /// <see cref="Reads"/> gives the per-operation cost, and how that cost moves across the sweep is the
    /// question: if the reads of a batch overlapped, per-operation cost would fall as the batch grows, because
    /// several device round trips would be in flight at once. A flat curve means they do not overlap.
    /// </para>
    /// <para>
    /// No latency is injected into the device. The point here is the efficiency of the pending machinery
    /// itself, so the device is as fast as possible and what is left to measure is our own cost. Simulated
    /// device latency would only add a constant that swamps it.
    /// </para>
    /// <para>
    /// Both drive <see cref="RespServerSession.TryConsumeMessagesAsync"/>, which is the entry point a real
    /// network transport uses. The synchronous <see cref="RespServerSession.TryConsumeMessages"/> is not used
    /// here: it exists for consumers with no receive loop to unwind to, and it blocks the calling thread on the
    /// suspension rather than returning to it, which is the behaviour this work exists to avoid measuring.
    /// </para>
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class PendingGetOperations : OperationsBase
    {
        /// <summary>Main-log page size. 4 KB is the minimum supported page size.</summary>
        const int PageSizeBytes = 4096;

        /// <summary>Number of pages kept in memory (the in-memory buffer). 4 pages * 4 KB = 16 KB.</summary>
        const int MemoryPages = 4;

        /// <summary>Number of pages worth of records to populate, so almost all of the data is on the device.</summary>
        const int PopulatePageCount = 100;

        /// <summary>
        /// Number of pipelined <c>GET</c>s in one batch, every one of them a miss. 1 is the isolated-miss
        /// case. Reported time is for the whole batch, so divide by this for per-operation cost.
        /// </summary>
        [Params(1, 2, 4, 8, 16, 32, 64)]
        public int Reads { get; set; }

        /// <summary>Prefix for populated (present) keys.</summary>
        const string KeyPrefix = "k:";

        /// <summary>Number of distinct keys that were populated.</summary>
        int keyCount;

        /// <summary>Fixed-width key id length, so a key can be overwritten in place without shifting bytes.</summary>
        int keyDigits;

        /// <summary>xorshift64* state for per-operation random key selection. BDN is single-threaded.</summary>
        ulong rngState = 0x9E3779B97F4A7C15UL;

        RandomKeyBatch batch;

        /// <summary>A pre-built batch of fixed-width <c>GET</c>s whose key digits are rewritten before each send.</summary>
        struct RandomKeyBatch
        {
            public Request request;
            /// <summary>Byte offset of the first key digit within the first command.</summary>
            public int firstKeyDigitOffset;
            /// <summary>Byte length of one command, i.e. the stride between successive keys.</summary>
            public int commandLength;
            /// <summary>Number of commands in the batch.</summary>
            public int count;
        }

        protected override void ConfigureServerOptions(GarnetServerOptions opts)
        {
            opts.EnableStorageTier = true;
            opts.DeviceType = DeviceType.LocalMemory;
            opts.PageSize = $"{PageSizeBytes}";
            opts.LogMemorySize = $"{MemoryPages * PageSizeBytes}";

            // The point of this benchmark. A non-zero completion thread count builds the LocalMemoryDevice with
            // parallelism > 0, so a read is handed to a device thread and its completion comes back through the
            // SPSC ring rather than running inline on the submitting thread. That is what makes the session
            // actually suspend. It also means the measurement includes a cross-thread handoff, which is real
            // cost on this path but does make run-to-run variance higher than the inline-completion benchmarks.
            opts.DeviceCompletionThreads = 1;
        }

        public override void GlobalSetup()
        {
            base.GlobalSetup();

            const int MinRecordBytes = 8;
            keyDigits = NumDigits((long)PopulatePageCount * PageSizeBytes / MinRecordBytes);
            keyCount = Populate();

            // Start from a cold store: everything below the tail goes to the device, so a GET against a random
            // key misses memory and goes pending.
            server.StoreWrapper.store.Log.FlushAndEvict(wait: true);

            SetupBatch(ref batch, Reads);
        }

        /// <summary>Populate fresh keys until <see cref="PopulatePageCount"/> pages have been appended.</summary>
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

        /// <summary>Fixed-width present key, e.g. "k:00042".</summary>
        string Key(long id) => KeyPrefix + id.ToString("D" + keyDigits);

        /// <summary>Build a RESP array command from bulk-string arguments.</summary>
        static string Resp(params string[] args)
        {
            var sb = new StringBuilder();
            _ = sb.Append('*').Append(args.Length).Append("\r\n");
            foreach (var arg in args)
                _ = sb.Append('$').Append(arg.Length).Append("\r\n").Append(arg).Append("\r\n");
            return sb.ToString();
        }

        /// <summary>Build a request buffer of <paramref name="count"/> identical fixed-width <c>GET</c>s.</summary>
        void SetupBatch(ref RandomKeyBatch batch, int count)
        {
            var template = Resp("GET", Key(0));
            var keyPlaceholder = KeyPrefix + new string('0', keyDigits);
            var keyIndex = template.IndexOf(keyPlaceholder, StringComparison.Ordinal);
            Debug.Assert(keyIndex >= 0, "Key placeholder not found in command template");

            var bytes = Encoding.ASCII.GetBytes(template);
            batch.commandLength = bytes.Length;
            batch.firstKeyDigitOffset = keyIndex + KeyPrefix.Length;
            batch.count = count;

            batch.request.buffer = GC.AllocateArray<byte>(bytes.Length * count, pinned: true);
            for (var i = 0; i < count; i++)
                bytes.CopyTo(batch.request.buffer.AsSpan(i * bytes.Length));
            batch.request.bufferPtr = (byte*)Unsafe.AsPointer(ref batch.request.buffer[0]);
        }

        /// <summary>A batch of <see cref="Reads"/> pipelined <c>GET</c>s, every one of which misses memory.</summary>
        [Benchmark]
        [BenchmarkCategory(BenchmarkCategories.Read)]
        public ValueTask<int> GetPending() => SendRandomized(ref batch);

        /// <summary>
        /// Rewrite every key in the batch with a fresh random id and hand the buffer to the session. Returns
        /// the session's task rather than awaiting it, so this method compiles to no async state machine of
        /// its own and the reported allocation is the session's rather than the harness's.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask<int> SendRandomized(ref RandomKeyBatch batch)
        {
            var p = batch.request.bufferPtr + batch.firstKeyDigitOffset;
            for (var i = 0; i < batch.count; i++)
            {
                WriteKeyDigits(p, NextRandomKeyId());
                p += batch.commandLength;
            }
            return session.TryConsumeMessagesAsync(batch.request.bufferPtr, batch.request.buffer.Length);
        }

        /// <summary>Write <see cref="keyDigits"/> zero-padded decimal digits of <paramref name="id"/> at <paramref name="p"/>.</summary>
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