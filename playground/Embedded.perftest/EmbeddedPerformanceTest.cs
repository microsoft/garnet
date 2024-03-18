// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Embedded.perftest
{
    /// <summary>
    /// Simple embedded online stress-test for Garnet.
    /// </summary>
    internal class EmbeddedPerformanceTest
    {
        /// <summary>
        /// Options configured for this test instance
        /// </summary>
        readonly Options opts;

        /// <summary>
        /// Embedded server instance to issue commands to
        /// </summary>
        readonly EmbeddedRespServer server;

        /// <summary>
        /// Logger used to log test progress
        /// </summary>
        readonly ILogger logger;

        /// <summary>
        /// Operations to execute in the workload
        /// </summary>
        OperationType[] opWorkload;

        /// <summary>
        /// Probability of each respective operation in opWorkload
        /// </summary>
        int[] opPercent;

        /// <summary>
        /// Creates a new embedded Garnet stress-test instance.
        /// </summary>
        /// <param name="opts">Test options.</param>
        /// <param name="loggerFactory">Factory to create the status logger for the test</param>
        /// <exception cref="Exception">Thrown if given configuration is invalid.</exception>
        public EmbeddedPerformanceTest(EmbeddedRespServer server, Options opts, ILoggerFactory loggerFactory)
        {
            this.server = server;
            this.opts = opts;
            logger = loggerFactory.CreateLogger("EmbeddedBench");
            opPercent = opts.OpPercent?.ToArray();
            opWorkload = opts.OpWorkload?.ToArray();

            if (opPercent.Length != opWorkload.Length)
                throw new Exception($"opPercent {opWorkload.Length} and opWorkload {opWorkload.Length} mismatch!");

            for (int i = 1; i < opPercent.Length; i++)
            {
                opPercent[i] += opPercent[i - 1];
            }
            if (opPercent[^1] != 100)
                throw new Exception($"opPercent must sum to 100, distribution: {String.Join(',', opPercent)}");
        }

        /// <summary>
        /// Returns the Garnet operation type matching the given percentage according to the workload.
        /// </summary>
        /// <param name="percent">Percentage in the percentage range specified by the workload</param>
        /// <returns>The matching operation type.</returns>
        /// <exception cref="Exception">Thrown if input percentage is outside of the defined percentage range for the workload.</exception>
        OperationType SelectOpType(int percent)
        {
            for (int i = 0; i < opPercent.Length; i++)
                if (percent <= opPercent[i])
                    return opWorkload[i];
            throw new Exception($"Invalid input percentage {percent}");
        }

        // Barrier used to synchronize benchmark worker threads
        static Barrier barrier;

        /// <summary>
        /// Executes the main benchmark worker loop. Runs for opts.RunTime seconds.
        /// </summary>
        /// <param name="threadId">The id of the thread running the benchmark loop.</param>
        /// <returns>The average operation throughput during the loop. In kOps/sec.</returns>
        public unsafe double WorkerLoop(int threadId)
        {
            // Maximum number of bytes per RESP-encoded command
            const int kMaxCommandLength = 64;

            // Operations per batch that is sent to Garnet
            int batchSize = opts.BatchSize;

            // Runtime in ms
            int runTimeMs = 1000 * opts.RunTime;

            // Local RESP package buffer that is handed off to Garnet
            // NOTE: Garnet might modify this buffer during processing.
            byte[] respBuffer = GC.AllocateArray<byte>(kMaxCommandLength * batchSize, pinned: true);
            byte* respBufferPtr = (byte*)Unsafe.AsPointer(ref respBuffer[0]);

            // Buffer containing the whole generated batch in RESP format to re-initialize respBuffer in between executions
            byte[] batchBuffer = GC.AllocateArray<byte>(kMaxCommandLength * batchSize, pinned: true);
            byte* batchBufferPtr = (byte*)Unsafe.AsPointer(ref batchBuffer[0]);

            // Create RESP-encoded command invocations for each command type
            var operations = new Dictionary<OperationType, byte[]>()
            {
                { OperationType.PING,   System.Text.Encoding.ASCII.GetBytes("PING\r\n") },
                { OperationType.DBSIZE, System.Text.Encoding.ASCII.GetBytes("*1\r\n$6\r\nDBSIZE\r\n") },
                { OperationType.CLIENT, System.Text.Encoding.ASCII.GetBytes("*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n")},
                { OperationType.ECHO,   System.Text.Encoding.ASCII.GetBytes("*2\r\n$4\r\nECHO\r\n$1\r\na\r\n")},
                { OperationType.GET,    System.Text.Encoding.ASCII.GetBytes($"*2\r\n$3\r\nGET\r\n$3\r\n{threadId:000}\r\n")},
                { OperationType.SET,    System.Text.Encoding.ASCII.GetBytes($"*3\r\n$3\r\nSET\r\n$3\r\n{threadId:000}\r\n$3\r\n{threadId:000}\r\n")}
            };

            //
            // Pre-generate a random operation sequence (to exclude RNG from main benchmark loop)
            //

            logger?.LogInformation($"[{threadId:00}] Generating operation sequence with {batchSize} operations per batch");

            Random rng = new(opts.Seed + threadId);

            int batchBufferLength = 0;

            for (int i = 0; i < batchSize; i++)
            {
                int percent = rng.Next(100);
                OperationType op = SelectOpType(percent);

                Debug.Assert(batchBufferLength + operations[op].Length <= kMaxCommandLength * batchSize);
                Array.Copy(operations[op], 0, batchBuffer, batchBufferLength, operations[op].Length);
                batchBufferLength += operations[op].Length;
            }

            //
            // Main benchmark loop
            //

            // Create a separate session for the thread
            using var session = server.GetRespSession();

            barrier.SignalAndWait();

            // Insert the test key/value pair for this thread
            Array.Copy(operations[OperationType.SET], respBuffer, operations[OperationType.SET].Length);
            session.TryConsumeMessages(respBufferPtr, operations[OperationType.SET].Length);

            logger?.LogInformation("[{threadId:00}] Starting benchmark loop ({runtime} seconds)", threadId, runTimeMs / 1000.0);

            barrier.SignalAndWait();

            // Loop until time limit is reached
            long numBatches = 0;

            Stopwatch sw = Stopwatch.StartNew();

            while (true)
            {
                // Reset the contents of the request buffer and send to Garnet
                Array.Copy(batchBuffer, respBuffer, batchBufferLength);
                session.TryConsumeMessages(respBufferPtr, batchBufferLength);

                if (numBatches++ % 8192 == 0 && sw.ElapsedMilliseconds > runTimeMs)
                {
                    break;
                }
            }

            sw.Stop();

            barrier.SignalAndWait();

            // Print results for this thread
            double throughput = numBatches * batchSize / (double)sw.ElapsedMilliseconds;

            logger?.LogInformation("[{threadId:00}] Executed {numBatches} batches in {seconds} seconds", threadId, numBatches, sw.Elapsed.TotalSeconds);
            logger?.LogInformation("[{threadId:00}] Throughput: {throughput} Kops/sec", threadId, throughput);

            return throughput;
        }

        /// <summary>
        /// Run the main test loop
        /// </summary>
        public unsafe void Run()
        {
            // Runtime in ms
            int runTimeMs = 1000 * opts.RunTime;

            // Execute stress-test individually for each configured #threads
            foreach (int NumThreads in opts.NumThreads)
            {
                Thread[] threads = new Thread[NumThreads];
                double[] throughput = new double[NumThreads];

                Console.WriteLine($"Running with {NumThreads} threads...");

                barrier = new Barrier(NumThreads);

                // Create and start test threads
                for (int i = 0; i < NumThreads; i++)
                {
                    int idx = i;
                    threads[idx] = new Thread(() => { throughput[idx] = WorkerLoop(idx); });
                }

                foreach (Thread thread in threads)
                {
                    thread.Start();
                }

                // Wait for execution of test threads
                Thread.Sleep(runTimeMs);

                foreach (Thread thread in threads)
                {
                    thread.Join();
                }

                // Print results
                Console.WriteLine($"Avg. throughput: {Math.Round(throughput.Sum(), 2)} Kops/sec; {Math.Round(throughput.Average(), 2)} Kops/sec/thread (Max: {Math.Round(throughput.Max(), 2)}, Min: {Math.Round(throughput.Min(), 2)})");
            }
        }
    }
}