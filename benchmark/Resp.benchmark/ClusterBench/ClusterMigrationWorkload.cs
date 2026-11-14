// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers.Binary;
using System.Diagnostics;
using System.Text.Json;
using HdrHistogram;
using StackExchange.Redis;
using StackExchange.Redis.Profiling;

namespace Resp.benchmark
{
    /// <summary>
    /// Controlled cluster-aware traffic against an existing numeric-key dataset.
    /// </summary>
    public sealed class ClusterMigrationWorkload
    {
        static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };
        readonly Options options;
        readonly CancellationTokenSource stop = new();
        readonly AsyncLocal<ProfilingSession> profiler = new();
        readonly long[] versions;
        readonly WorkerState[] workers;
        Phase phase = new(0, "paused", false);
        string failure;

        sealed record Phase(int Sequence, string Name, bool Running);

        /// <summary>
        /// An atomic file-based control request. Pause drains before validation or analysis.
        /// </summary>
        public sealed record Control(int Sequence, string Phase, bool Running = false,
            bool Validate = false, DateTime? WindowStartUtc = null, DateTime? WindowEndUtc = null,
            bool Stop = false);

        sealed class Distribution
        {
            public readonly LongHistogram Histogram = new(1, 1_000_000_000, 3);
            public long Errors;
            public void Record(long micros, bool success)
            {
                Histogram.RecordValue(Math.Max(1, micros));
                if (!success) Errors++;
            }
            public void Add(Distribution other)
            {
                Histogram.Add(other.Histogram);
                Errors += other.Errors;
            }
            public object Export() => new
            {
                Count = Histogram.TotalCount,
                Successes = Histogram.TotalCount - Errors,
                Errors,
                P50Microseconds = Histogram.GetValueAtPercentile(50),
                P95Microseconds = Histogram.GetValueAtPercentile(95),
                P99Microseconds = Histogram.GetValueAtPercentile(99),
                P999Microseconds = Histogram.GetValueAtPercentile(99.9),
                MaximumMicroseconds = Histogram.GetValueAtPercentile(100),
                Bins = Histogram.RecordedValues().Select(v => new
                {
                    Value = v.ValueIteratedTo,
                    Count = v.CountAddedInThisIterationStep
                }).ToArray()
            };
        }

        sealed class Measurements
        {
            public readonly Distribution Reads = new();
            public readonly Distribution Writes = new();
            public long ProfiledOperations;
            public long SampledAsk;
            public long SampledMoved;
            public long FirstIssuedTicks = long.MaxValue;
            public long LastCompletedTicks;
            public void Add(Measurements other)
            {
                Reads.Add(other.Reads);
                Writes.Add(other.Writes);
                ProfiledOperations += other.ProfiledOperations;
                SampledAsk += other.SampledAsk;
                SampledMoved += other.SampledMoved;
                FirstIssuedTicks = Math.Min(FirstIssuedTicks, other.FirstIssuedTicks);
                LastCompletedTicks = Math.Max(LastCompletedTicks, other.LastCompletedTicks);
            }
            public object Export() => new
            {
                GET = Reads.Export(),
                SET = Writes.Export(),
                ProfiledOperations,
                SampledAsk,
                SampledMoved,
                StartedUtc = FirstIssuedTicks == long.MaxValue ? (DateTime?)null : new DateTime(FirstIssuedTicks, DateTimeKind.Utc),
                CompletedUtc = LastCompletedTicks == 0 ? (DateTime?)null : new DateTime(LastCompletedTicks, DateTimeKind.Utc),
                Seconds = LastCompletedTicks == 0 ? 0 : (LastCompletedTicks - FirstIssuedTicks) / 10_000_000.0
            };
        }

        sealed class WorkerState : IDisposable
        {
            public ConnectionMultiplexer Client;
            public FileStream Trace;
            public StreamWriter Intervals;
            public readonly Dictionary<int, Measurements> Metrics = new();
            public readonly Dictionary<int, string> Names = new();
            public int Active;
            public int ObservedSequence;
            public void Dispose()
            {
                Trace?.Dispose();
                Intervals?.Dispose();
                Client?.Dispose();
            }
        }

        readonly int readPercent;

        ClusterMigrationWorkload(Options options)
        {
            this.options = options;
            if (!options.ClusterBench || !options.Online || !options.SkipLoad ||
                options.Client != ClientType.SERedis || options.IntraThreadParallelism != 1 ||
                options.NumThreads.Count() != 1 || options.NumThreads.First() <= 0 ||
                options.DbSize < options.NumThreads.First() || options.DbSize > 100_000_000 ||
                options.KeyLength != 8 || options.ValueLength != 2048 ||
                !options.OpWorkload.SequenceEqual([OpType.GET, OpType.SET]) ||
                options.OpPercent.Count() != 2 || options.OpPercent.Any(percent => percent <= 0) ||
                options.OpPercent.Sum() != 100 ||
                options.ReplicaOpsPercent != 0 || options.EnableTLS ||
                string.IsNullOrEmpty(options.MigrationTarget) ||
                options.MigrationRequestTimeoutMs <= 0 || options.MigrationProfileEvery <= 0)
                throw new ArgumentException("Migration workload requires online cluster SERedis, skipload, one thread-count, itp=1, numeric 8-byte keys, 2048-byte values, positive GET/SET percentages totaling 100, and a target primary.");
            readPercent = options.OpPercent.First();
            versions = new long[options.DbSize];
            workers = Enumerable.Range(0, options.NumThreads.First()).Select(_ => new WorkerState()).ToArray();
        }

        /// <summary>
        /// Run the controlled workload until a stop request or an explicit failure.
        /// </summary>
        public static async Task RunAsync(Options options)
        {
            var runner = new ClusterMigrationWorkload(options);
            try
            {
                await runner.RunAsync();
            }
            finally
            {
                runner.stop.Cancel();
                foreach (var worker in runner.workers) worker.Dispose();
                runner.stop.Dispose();
            }
        }

        async Task RunAsync()
        {
            var directory = options.MigrationWorkloadDirectory;
            Directory.CreateDirectory(directory);
            if (File.Exists(Path.Combine(directory, "ready.json")))
                throw new IOException("Workload directory has already been used.");
            foreach (var worker in workers)
            {
                var config = BenchUtils.GetConfig(options.Address, options.Port);
                config.EndPoints.Add(options.MigrationTarget, options.Port);
                config.AsyncTimeout = options.MigrationRequestTimeoutMs;
                config.SyncTimeout = options.MigrationRequestTimeoutMs;
                config.ConnectTimeout = options.MigrationRequestTimeoutMs;
                config.AbortOnConnectFail = true;
                if (options.Auth != null) config.Password = options.Auth;
                worker.Client = await ConnectionMultiplexer.ConnectAsync(config);
                worker.Client.RegisterProfiler(() => profiler.Value);
                await worker.Client.GetServer(options.Address, options.Port).PingAsync();
                await worker.Client.GetServer(options.MigrationTarget, options.Port).PingAsync();
                var id = Array.IndexOf(workers, worker);
                worker.Trace = new FileStream(Path.Combine(directory, $"worker-{id}.bin"),
                    FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, 131072);
                worker.Intervals = new StreamWriter(Path.Combine(directory, $"worker-{id}-intervals.jsonl"), false);
            }
            WriteJson(Path.Combine(directory, "ready.json"), new
            {
                Utc = DateTime.UtcNow,
                Workers = workers.Length,
                options.DbSize,
                ReadPercent = readPercent,
                WritePercent = 100 - readPercent,
                options.MigrationProfileEvery,
                options.MigrationRequestTimeoutMs,
                options.WorkloadSeed,
                ClientVersion = typeof(ConnectionMultiplexer).Assembly.GetName().Version.ToString(),
                Runtime = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription,
                TraceFormat = "16 bytes: UTC issue ticks Int64, elapsed microseconds Int32, write Byte, error Byte, phase UInt16; little endian",
                ProcessId = Environment.ProcessId
            });
            var tasks = workers.Select((worker, id) => RunWorkerAsync(worker, id)).ToArray();
            var lastSequence = 0;
            var telemetryTime = Stopwatch.GetTimestamp();
            using var telemetry = new StreamWriter(Path.Combine(directory, "telemetry.jsonl"), false);
            try
            {
                while (!stop.IsCancellationRequested)
                {
                    if (Stopwatch.GetElapsedTime(telemetryTime).TotalSeconds >= 1)
                    {
                        using var process = Process.GetCurrentProcess();
                        telemetry.WriteLine(JsonSerializer.Serialize(new
                        {
                            Utc = DateTime.UtcNow,
                            CpuSeconds = process.TotalProcessorTime.TotalSeconds,
                            process.WorkingSet64,
                            AllocatedBytes = GC.GetTotalAllocatedBytes(),
                            Gen0 = GC.CollectionCount(0),
                            Gen1 = GC.CollectionCount(1),
                            Gen2 = GC.CollectionCount(2)
                        }));
                        telemetry.Flush();
                        telemetryTime = Stopwatch.GetTimestamp();
                    }
                    var path = Path.Combine(directory, "request.json");
                    if (File.Exists(path))
                    {
                        var request = JsonSerializer.Deserialize<Control>(await File.ReadAllTextAsync(path));
                        if (request.Sequence > lastSequence)
                        {
                            if (request.Sequence > ushort.MaxValue) throw new InvalidDataException("Too many phase transitions.");
                            Interlocked.Exchange(ref phase, new(request.Sequence, request.Phase, false));
                            while (workers.Any(w => Volatile.Read(ref w.Active) != 0 ||
                                Volatile.Read(ref w.ObservedSequence) != request.Sequence))
                            {
                                if (failure != null) throw new InvalidOperationException(failure);
                                await Task.Delay(10);
                            }
                            foreach (var worker in workers)
                            {
                                worker.Trace.Flush();
                                worker.Intervals.Flush();
                            }
                            long verified = 0;
                            if (request.Validate)
                                verified = (await Task.WhenAll(workers.Select((worker, id) => VerifyAsync(worker, id)))).Sum();
                            object window = null;
                            if (request.WindowStartUtc.HasValue && request.WindowEndUtc.HasValue)
                                window = AnalyzeTraces(workers.Select(w => w.Trace.Name),
                                    request.WindowStartUtc.Value, request.WindowEndUtc.Value);
                            var all = new SortedDictionary<int, Measurements>();
                            foreach (var worker in workers)
                                foreach (var entry in worker.Metrics)
                                {
                                    if (!all.TryGetValue(entry.Key, out var aggregate))
                                        all.Add(entry.Key, aggregate = new());
                                    aggregate.Add(entry.Value);
                                }
                            var phases = all.Select(entry => new
                            {
                                Sequence = entry.Key,
                                Name = workers.Select(w => w.Names.GetValueOrDefault(entry.Key)).First(n => n != null),
                                Measurements = entry.Value.Export()
                            }).ToArray();
                            Interlocked.Exchange(ref phase, new(request.Sequence, request.Phase, request.Running));
                            WriteJson(Path.Combine(directory, $"reply-{request.Sequence}.json"), new
                            {
                                request.Sequence,
                                request.Phase,
                                Utc = DateTime.UtcNow,
                                VerifiedWrites = verified,
                                Window = window,
                                Phases = phases
                            });
                            lastSequence = request.Sequence;
                            if (request.Stop) break;
                        }
                    }
                    await Task.Delay(25);
                }
                if (failure != null) throw new InvalidOperationException(failure);
            }
            finally
            {
                stop.Cancel();
                await Task.WhenAll(tasks);
            }
        }

        async Task RunWorkerAsync(WorkerState worker, int id)
        {
            var random = new Random(unchecked(options.WorkloadSeed * 397 + id));
            var key = new byte[8];
            var value = Enumerable.Repeat((byte)'v', 2048).ToArray();
            var db = worker.Client.GetDatabase(0);
            long sequence = 1, operations = 0, reads = 0, writes = 0, errors = 0, maxMicros = 0;
            var intervalStart = DateTime.UtcNow;
            var intervalEnd = intervalStart;
            var intervalPhase = phase;
            void FlushInterval()
            {
                if (reads + writes + errors == 0) return;
                worker.Intervals.WriteLine(JsonSerializer.Serialize(new
                {
                    StartUtc = intervalStart,
                    EndUtc = intervalEnd,
                    intervalPhase.Sequence,
                    intervalPhase.Name,
                    Reads = reads,
                    Writes = writes,
                    Errors = errors,
                    MaximumMicroseconds = maxMicros
                }));
                reads = writes = errors = maxMicros = 0;
            }
            try
            {
                while (!stop.IsCancellationRequested)
                {
                    Interlocked.Exchange(ref worker.Active, 1);
                    var current = Volatile.Read(ref phase);
                    if (intervalPhase != current)
                    {
                        FlushInterval();
                        intervalPhase = current;
                        intervalStart = DateTime.UtcNow;
                    }
                    if (!current.Running)
                    {
                        Volatile.Write(ref worker.ObservedSequence, current.Sequence);
                        Interlocked.Exchange(ref worker.Active, 0);
                        await Task.Delay(10, stop.Token);
                        continue;
                    }
                    try
                    {
                        if (!worker.Metrics.TryGetValue(current.Sequence, out var metrics))
                        {
                            worker.Metrics.Add(current.Sequence, metrics = new());
                            worker.Names.Add(current.Sequence, current.Name);
                        }
                        var write = random.Next(100) >= readPercent;
                        var index = write
                            ? id + random.Next((options.DbSize - 1 - id) / workers.Length + 1) * workers.Length
                            : random.Next(options.DbSize);
                        FillKey(key, index);
                        var expected = Volatile.Read(ref versions[index]);
                        var version = (sequence++ << 6) | (uint)id;
                        if (write) EncodeValue(value, index, version);
                        var sampled = operations++ % options.MigrationProfileEvery == 0;
                        ProfilingSession session = null;
                        if (sampled) profiler.Value = session = new();
                        var utc = DateTime.UtcNow;
                        var started = Stopwatch.GetTimestamp();
                        var success = false;
                        try
                        {
                            if (write)
                            {
                                if (!await db.StringSetAsync(key, value)) throw new InvalidDataException("SET did not acknowledge success.");
                                Volatile.Write(ref versions[index], version);
                                writes++;
                            }
                            else
                            {
                                var result = await db.StringGetAsync(key);
                                if (result.IsNull || result.Length() != 2048) throw new InvalidDataException($"Missing or invalid GET: {index}");
                                if (expected > 0) ValidateValue((byte[])result, index, expected, false);
                                reads++;
                            }
                            success = true;
                        }
                        finally
                        {
                            var micros = Math.Max(1, (Stopwatch.GetTimestamp() - started) * 1_000_000 / Stopwatch.Frequency);
                            (write ? metrics.Writes : metrics.Reads).Record(micros, success);
                            metrics.FirstIssuedTicks = Math.Min(metrics.FirstIssuedTicks, utc.Ticks);
                            metrics.LastCompletedTicks = Math.Max(metrics.LastCompletedTicks, utc.Ticks + micros * 10);
                            WriteTrace(worker.Trace, utc.Ticks, checked((int)micros), write, !success, (ushort)current.Sequence);
                            maxMicros = Math.Max(maxMicros, micros);
                            if (!success) errors++;
                            intervalEnd = DateTime.UtcNow;
                            if (session != null)
                            {
                                metrics.ProfiledOperations++;
                                foreach (var command in session.FinishProfiling())
                                {
                                    var reason = command.RetransmissionReason?.ToString();
                                    if (string.Equals(reason, "Ask", StringComparison.OrdinalIgnoreCase)) metrics.SampledAsk++;
                                    if (string.Equals(reason, "Moved", StringComparison.OrdinalIgnoreCase)) metrics.SampledMoved++;
                                }
                                profiler.Value = null;
                            }
                            if ((DateTime.UtcNow - intervalStart).TotalMilliseconds >= 100)
                            {
                                FlushInterval();
                                intervalStart = DateTime.UtcNow;
                            }
                        }
                    }
                    finally
                    {
                        Interlocked.Exchange(ref worker.Active, 0);
                    }
                }
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
            catch (Exception ex)
            {
                Interlocked.CompareExchange(ref failure, ex.ToString(), null);
                WriteJson(Path.Combine(options.MigrationWorkloadDirectory, $"worker-{id}-failure.json"), new { Utc = DateTime.UtcNow, Error = ex.ToString() });
                stop.Cancel();
                throw;
            }
            finally
            {
                FlushInterval();
            }
        }

        async Task<long> VerifyAsync(WorkerState worker, int id)
        {
            long count = 0;
            var key = new byte[8];
            var db = worker.Client.GetDatabase(0);
            for (var index = id; index < versions.Length; index += workers.Length)
            {
                var version = versions[index];
                if (version == 0) continue;
                FillKey(key, index);
                var result = await db.StringGetAsync(key);
                if (result.IsNull) throw new InvalidDataException($"Missing acknowledged SET: {index}");
                ValidateValue((byte[])result, index, version, true);
                count++;
            }
            return count;
        }

        /// <summary>Encode the existing preload key without changing the keyspace.</summary>
        public static void FillKey(Span<byte> key, int index)
        {
            if (key.Length != 8 || index < 0 || index >= 100_000_000) throw new ArgumentOutOfRangeException(nameof(index));
            key.Fill((byte)'X');
            var position = 7;
            do
            {
                key[position--] = (byte)('0' + index % 10);
                index /= 10;
            } while (index != 0);
        }

        /// <summary>Keep fixed-size writes distinguishable by key and acknowledged version.</summary>
        public static void EncodeValue(Span<byte> value, int key, long version)
        {
            if (value.Length != 2048) throw new ArgumentException("Expected 2048 bytes.", nameof(value));
            BinaryPrimitives.WriteInt64LittleEndian(value, key);
            BinaryPrimitives.WriteInt64LittleEndian(value[8..], version);
        }

        /// <summary>Check acknowledged versions without rejecting a newer concurrent value.</summary>
        public static void ValidateValue(ReadOnlySpan<byte> value, int key, long version, bool exact)
        {
            if (value.Length != 2048 || BinaryPrimitives.ReadInt64LittleEndian(value) != key)
                throw new InvalidDataException($"Corrupt value for key {key}.");
            var observed = BinaryPrimitives.ReadInt64LittleEndian(value[8..]);
            if (exact ? observed != version : observed < version)
                throw new InvalidDataException($"Version mismatch for key {key}: expected {version}, received {observed}.");
        }

        /// <summary>Write a compact logical-operation record, including failed outcomes.</summary>
        public static void WriteTrace(Stream stream, long utcTicks, int micros, bool write, bool error, ushort phase)
        {
            Span<byte> record = stackalloc byte[16];
            BinaryPrimitives.WriteInt64LittleEndian(record, utcTicks);
            BinaryPrimitives.WriteInt32LittleEndian(record[8..], micros);
            record[12] = (byte)(write ? 1 : 0);
            record[13] = (byte)(error ? 1 : 0);
            BinaryPrimitives.WriteUInt16LittleEndian(record[14..], phase);
            stream.Write(record);
        }

        /// <summary>Aggregate exact issue-window latencies, retaining requests completed after migration.</summary>
        public static object AnalyzeTraces(IEnumerable<string> paths, DateTime startUtc, DateTime endUtc)
        {
            if (endUtc <= startUtc) throw new ArgumentException("Invalid migration window.");
            var metrics = new Measurements();
            long carryIn = 0, carryOut = 0;
            foreach (var path in paths)
            {
                using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 131072);
                if (stream.Length % 16 != 0) throw new InvalidDataException("Incomplete workload trace.");
                using var reader = new BinaryReader(stream);
                while (stream.Position < stream.Length)
                {
                    var issue = reader.ReadInt64();
                    var micros = reader.ReadInt32();
                    var write = reader.ReadByte() != 0;
                    var error = reader.ReadByte() != 0;
                    _ = reader.ReadUInt16();
                    if (issue < startUtc.Ticks && issue + micros * 10L >= startUtc.Ticks) carryIn++;
                    if (issue < startUtc.Ticks || issue >= endUtc.Ticks) continue;
                    (write ? metrics.Writes : metrics.Reads).Record(micros, !error);
                    metrics.FirstIssuedTicks = Math.Min(metrics.FirstIssuedTicks, issue);
                    metrics.LastCompletedTicks = Math.Max(metrics.LastCompletedTicks, issue + micros * 10L);
                    if (issue + micros * 10L > endUtc.Ticks) carryOut++;
                }
            }
            return new
            {
                StartUtc = startUtc,
                EndUtc = endUtc,
                Seconds = (endUtc - startUtc).TotalSeconds,
                CarryIn = carryIn,
                CarryOut = carryOut,
                Measurements = metrics.Export()
            };
        }

        static void WriteJson(string path, object value)
        {
            var temporary = path + ".tmp";
            File.WriteAllText(temporary, JsonSerializer.Serialize(value, JsonOptions));
            File.Move(temporary, path, true);
        }
    }
}