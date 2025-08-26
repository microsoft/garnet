// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Garnet server entry point
    /// </summary>
    public class Program
    {
        static void Main(string[] args)
        {
            try
            {
                using var server = new GarnetServer(args);

                // Optional: register custom extensions
                RegisterExtensions(server);

                // Start the server
                server.Start();

                Thread.Sleep(Timeout.Infinite);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unable to initialize server due to exception: {ex.Message}");
            }
        }

        /// <summary>
        /// Register new commands with the server. You can access these commands from clients using
        /// commands such as db.Execute in StackExchange.Redis. Example:
        ///   db.Execute("SETIFPM", key, value, prefix);
        /// </summary>
        static void RegisterExtensions(GarnetServer server)
        {
            // Register custom command on raw strings (SETIFPM = "set if prefix match")
            // Add RESP command info to registration for command to appear when client runs COMMAND / COMMAND INFO
            var setIfPmCmdInfo = new RespCommandsInfo
            {
                Name = "SETIFPM",
                Arity = 4,
                FirstKey = 1,
                LastKey = 1,
                Step = 1,
                Flags = RespCommandFlags.DenyOom | RespCommandFlags.Write,
                AclCategories = RespAclCategories.String | RespAclCategories.Write,
            };
            server.Register.NewCommand("SETIFPM", CommandType.ReadModifyWrite, new SetIfPMCustomCommand(), setIfPmCmdInfo);

            // Register custom command on raw strings (SETWPIFPGT = "set with prefix, if prefix greater than")
            server.Register.NewCommand("SETWPIFPGT", CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand());

            // Register custom command on raw strings (DELIFM = "delete if value matches")
            server.Register.NewCommand("DELIFM", CommandType.ReadModifyWrite, new DeleteIfMatchCustomCommand());

            // Register custom commands on objects
            var factory = new MyDictFactory();
            server.Register.NewType(factory);
            server.Register.NewCommand("MYDICTSET", CommandType.ReadModifyWrite, factory, new MyDictSet(), new RespCommandsInfo { Arity = 4 });
            server.Register.NewCommand("MYDICTGET", CommandType.Read, factory, new MyDictGet(), new RespCommandsInfo { Arity = 3 });

            // Register stored procedure to run a transactional command
            // Add RESP command info to registration for command to appear when client runs COMMAND / COMMAND INFO
            var readWriteTxCmdInfo = new RespCommandsInfo
            {
                Name = "READWRITETX",
                Arity = 4,
                FirstKey = 1,
                LastKey = 3,
                Step = 1,
                Flags = RespCommandFlags.DenyOom | RespCommandFlags.Write,
                AclCategories = RespAclCategories.Write,
            };
            server.Register.NewTransactionProc("READWRITETX", () => new ReadWriteTxn(), readWriteTxCmdInfo);

            // Register stored procedure to run a transactional command
            server.Register.NewTransactionProc("MSETPX", () => new MSetPxTxn());

            // Register stored procedure to run a transactional command
            server.Register.NewTransactionProc("MGETIFPM", () => new MGetIfPM());

            // Register stored procedure to run a non-transactional command
            server.Register.NewTransactionProc("GETTWOKEYSNOTXN", () => new GetTwoKeysNoTxn(), new RespCommandsInfo { Arity = 3 });

            // Register sample transactional procedures
            server.Register.NewTransactionProc("SAMPLEUPDATETX", () => new SampleUpdateTxn(), new RespCommandsInfo { Arity = 9 });
            server.Register.NewTransactionProc("SAMPLEDELETETX", () => new SampleDeleteTxn(), new RespCommandsInfo { Arity = 6 });

            server.Register.NewProcedure("SUM", () => new Sum());
            server.Register.NewProcedure("SETMAINANDOBJECT", () => new SetStringAndList());

            RegisterHackyBenchmarkCommands(server);
        }

        // Hack Hack - this had better not be in main
        public static void RegisterHackyBenchmarkCommands(GarnetServer server)
        {
            server.Register.NewProcedure("FILLBENCH", () => FillBenchCommand.Instance, new RespCommandsInfo() { Arity = 3 });
            server.Register.NewProcedure("BENCHRWMIX", () => BenchmarkReadWriteMixCommand.Instance, new RespCommandsInfo() { Arity = 9 });
        }
    }

    // FOR HORRIBLE DEMONSTRATION PURPOSES -- this had better not be in main
    internal sealed class BenchmarkReadWriteMixCommand : CustomProcedure
    {
        public static readonly BenchmarkReadWriteMixCommand Instance = new();

        /// <summary>
        /// BENCHRWMIX (VECTOR SET) (PATH FOR READ VECTORS) (PATH FOR WRITE VECTORS) (RESULTS PER QUERY) (DELTA) (SEARCH EXPLORATION FACTOR) (ROLL OUT OF 1_000 TO WRITE) (DURATION SECS)
        /// 
        /// Returns "(duration in milliseconds) (search count) (inserted count) (True|False if we ran out of write data)"
        /// </summary>
        public override unsafe bool Execute<TGarnetApi>(TGarnetApi garnetApi, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            if (procInput.parseState.Count != 8)
            {
                WriteError(ref output, "BAD ARG");
                return true;
            }

            ref ArgSlice vectorSet = ref procInput.parseState.GetArgSliceByRef(0);
            string readPath = procInput.parseState.GetString(1);
            string writePath = procInput.parseState.GetString(2);
            int resultsPerQuery = procInput.parseState.GetInt(3);
            float delta = procInput.parseState.GetFloat(4);
            int searchExplorationFactor = procInput.parseState.GetInt(5);
            int writePerc = procInput.parseState.GetInt(6);
            int durationSecs = procInput.parseState.GetInt(7);
            long durationMillis = durationSecs * 1_000;

            //if (!File.Exists(readPath))
            //{
            //    WriteError(ref output, "READ PATH NOT FOUND");
            //    return true;
            //}

            //if (!File.Exists(writePath))
            //{
            //    WriteError(ref output, "WRITE PATH NOT FOUND");
            //    return true;
            //}

            ReadOnlyMemory<float>[] randomReadVecs = GetReadVectors(readPath).ToArray();
            List<(ReadOnlyMemory<byte> Element, ReadOnlyMemory<float> Values)> writeVecs = GetWriteVectors(writePath).ToList();
            int writeVecNextIx = 0;

            Random r = Random.Shared;

            long startTimestamp = Stopwatch.GetTimestamp();

            long reads = 0;
            long writes = 0;

            // Reuse result space for all queries
            Span<byte> idSpace = GC.AllocateArray<byte>(resultsPerQuery * (sizeof(int) + sizeof(int)), pinned: true);
            Span<float> distanceSpace = GC.AllocateArray<float>(resultsPerQuery, pinned: true);

            Stopwatch sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < durationMillis)
            {
                if (r.Next(1_000) < writePerc && writeVecNextIx < writeVecs.Count)
                {
                    // Write a vec
                    (ReadOnlyMemory<byte> Element, ReadOnlyMemory<float> Values) vec = writeVecs[writeVecNextIx];
                    writeVecNextIx++;

                    GarnetStatus writeRes;
                    VectorManagerResult vecRes;
                    fixed (byte* elemPtr = vec.Element.Span)
                    {
                        writeRes = garnetApi.VectorSetAdd(vectorSet, 0, vec.Values.Span, new ArgSlice(elemPtr, vec.Element.Length), VectorQuantType.NoQuant, 64, default, 64, out vecRes);
                    }

                    if (writeRes != GarnetStatus.OK || vecRes != VectorManagerResult.OK)
                    {
                        WriteError(ref output, $"FAILED WRITE {writeRes} -> {vecRes} for 0x{string.Join("", vec.Element.ToArray().Select(static x => x.ToString("X2")))})");
                        return true;
                    }

                    writes++;
                }
                else
                {
                    // Read a vec
                    long readIx = r.NextInt64(randomReadVecs.Length);
                    ReadOnlyMemory<float> values = randomReadVecs[readIx];
                    SpanByteAndMemory idResults = SpanByteAndMemory.FromPinnedSpan(idSpace);
                    SpanByteAndMemory distanceResults = SpanByteAndMemory.FromPinnedSpan(MemoryMarshal.Cast<float, byte>(distanceSpace));

                    GarnetStatus readRes = garnetApi.VectorSetValueSimilarity(vectorSet, values.Span, resultsPerQuery, delta, searchExplorationFactor, default, 0, ref idResults, ref distanceResults, out VectorManagerResult vecRes);
                    Debug.Assert(idResults.IsSpanByte && distanceResults.IsSpanByte, "Shouldn't have resized, allocations will tank perf");

                    if (readRes != GarnetStatus.OK || vecRes != VectorManagerResult.OK)
                    {
                        WriteError(ref output, $"FAILED READ {readRes} -> {vecRes} for values [{string.Join(", ", values.ToArray())}]");
                        return true;
                    }

                    reads++;
                }
            }

            sw.Stop();
            double durationMilliseconds = sw.ElapsedMilliseconds;

            WriteBulkString(ref output, Encoding.UTF8.GetBytes($"{durationMilliseconds} {reads} {writes} {writeVecNextIx == writeVecs.Count}"));
            return true;
        }

        private static IEnumerable<ReadOnlyMemory<float>> GetReadVectors(string path)
        {
            // TODO: load from disk

            yield return (new float[] { 7f, 8f, 9f });
            yield return (new float[] { 10f, 11f, 12f });
            yield return (new float[] { 13f, 14f, 15f });
        }

        private IEnumerable<(ReadOnlyMemory<byte> Element, ReadOnlyMemory<float> Values)> GetWriteVectors(string path)
        {
            // TODO: load from disk

            yield return ("123"u8.ToArray(), new float[] { 1f, 2f, 3f });
            yield return ("456"u8.ToArray(), new float[] { 4f, 5f, 6f });
            yield return ("789"u8.ToArray(), new float[] { 7f, 8f, 9f });
        }
    }

    // FOR HORRIBLE DEMONSTRATION PURPOSES -- this had better not be in main
    internal sealed class FillBenchCommand : CustomProcedure
    {
        public static readonly FillBenchCommand Instance = new();

        /// <summary>
        /// FILLBENCH (LOCAL PATH TO DATA) (VECTOR SET KEY)
        /// 
        /// Returns "(duration in milliseconds) (inserted count)"
        /// </summary>
        public override unsafe bool Execute<TGarnetApi>(TGarnetApi garnetApi, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            if (procInput.parseState.Count != 2)
            {
                WriteError(ref output, "BAD ARG");
                return true;
            }

            long startTimeStamp = Stopwatch.GetTimestamp();

            string path = procInput.parseState.GetString(0);
            ref ArgSlice key = ref procInput.parseState.GetArgSliceByRef(1);

            //if (!File.Exists(path))
            //{
            //    WriteError(ref output, "PATH NOT FOUND");
            //    return true;
            //}

            long inserts = 0;

            foreach ((ReadOnlyMemory<byte> Element, ReadOnlyMemory<float> Values) vector in ReadAllVectors(path))
            {
                GarnetStatus res;
                VectorManagerResult vecRes;
                fixed (byte* elem = vector.Element.Span)
                {
                    ArgSlice element = new ArgSlice(elem, vector.Element.Length);

                    res = garnetApi.VectorSetAdd(key, 0, vector.Values.Span, element, VectorQuantType.NoQuant, 64, default, 64, out vecRes);
                }

                if (res != GarnetStatus.OK || vecRes != VectorManagerResult.OK)
                {
                    WriteError(ref output, $"FAILED {res} -> {vecRes} for 0x{string.Join("", vector.Element.ToArray().Select(static x => x.ToString("X2")))})");
                    return true;
                }

                inserts++;
            }

            double durationMilliseconds = Stopwatch.GetElapsedTime(startTimeStamp).TotalMilliseconds;

            WriteBulkString(ref output, Encoding.UTF8.GetBytes($"{durationMilliseconds} {inserts}"));
            return true;
        }

        private IEnumerable<(ReadOnlyMemory<byte> Element, ReadOnlyMemory<float> Values)> ReadAllVectors(string path)
        {
            // TODO: load from disk

            yield return ("123"u8.ToArray(), new float[] { 1f, 2f, 3f });
            yield return ("456"u8.ToArray(), new float[] { 4f, 5f, 6f });
            yield return ("789"u8.ToArray(), new float[] { 7f, 8f, 9f });
        }
    }
}