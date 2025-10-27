// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;
using Embedded.server;
using System.Text;
using System.Diagnostics;
using System.Net;

namespace Resp.benchmark
{
    public class AofBench
    {
        readonly Options options;
        readonly AofGen aofGen;
        readonly AofSync[] aofSync;
        StringBuilder stats = new();
        long total_bytes_processed = 0;
        long total_pages_processed = 0;

        internal EmbeddedRespServer server;
        internal RespServerSession[] sessions;

        volatile bool done = false;

        public AofBench(Options options)
        {
            this.options = options;

            if (options.Client == ClientType.InProc && !options.EnableCluster)
                throw new Exception("InProc AofBench requires --cluster!");

            var serverOptions = new GarnetServerOptions
            {
                ClusterAnnounceEndpoint = new IPEndPoint(IPAddress.Loopback, 6379),
                QuietMode = true,
                EnableAOF = options.EnableAOF,
                EnableCluster = options.EnableCluster,
                IndexSize = options.IndexSize,
                ClusterConfigFlushFrequencyMs = -1,
                UseAofNullDevice = options.UseAofNullDevice,
                CommitFrequencyMs = options.CommitFrequencyMs,
                AofPageSize = options.AofPageSize,
                AofMemorySize = options.CalculateAofMemorySizeForLoad(),
                AofSublogCount = options.AofSublogCount
            };
            server = new EmbeddedRespServer(serverOptions, null, new GarnetServerEmbedded());
            sessions = server.GetRespSessions(options.AofSublogCount);
            aofGen = new AofGen(options, stats);
            aofSync = [.. Enumerable.Range(0, options.AofSublogCount).Select(x => new AofSync(this, threadId: x, startAddress: 0, options, aofGen, stats))];
        }

        public void PrintStats()
        {
            Console.WriteLine(stats.ToString());
        }

        public void GenerateData() => aofGen.GenerateData();

        public void Run()
        {
            var threads = options.AofSublogCount;
            var workers = new Thread[threads];
            total_bytes_processed = 0;

            Console.WriteLine($"Run AOFSync using {threads} thread(s)");

            // Run the experiment.
            for (var idx = 0; idx < threads; ++idx)
            {
                var x = idx;
                workers[idx] = new Thread(() => RunAOFBench(x));
            }

            Stopwatch swatch = new();
            swatch.Start();

            // Start threads.
            foreach (var worker in workers)
                worker.Start();

            // Let workers operate for a specific RunTime
            Thread.Sleep(TimeSpan.FromSeconds(options.RunTime));
            done = true;

            // Wait for AOF load to complete
            foreach (var worker in workers)
                worker.Join();

            swatch.Stop();

            var seconds = swatch.ElapsedMilliseconds / 1000.0;
            var bytesPerSecond = (total_bytes_processed / seconds) / (double)1_000_000_000;

            Console.WriteLine($"Total time: {swatch.ElapsedMilliseconds:N2}ms for {total_bytes_processed:N2} bytes");
            Console.WriteLine($"Bandwidth: {bytesPerSecond:N2} GiB/sec");
            Console.WriteLine($"Total pages processed {total_pages_processed:N0}");

            unsafe void RunAOFBench(int threadId)
            {
                var buffers = aofGen.GetPageBuffers(threadId);
                var offset = 0;
                var currentAddress = 64L;
                var nextAddress = 64L;
                var pagesSend = 0L;
                var totalBytes = 0L;
                while (!done)
                {
                    var pos = offset++ % buffers.Length;
                    var currPage = buffers[pos];
                    fixed (byte* payloadPtr = currPage.payload)
                    {
                        nextAddress = currentAddress + currPage.payloadLength;
                        aofSync[threadId].Consume(payloadPtr, currPage.payloadLength, currentAddress, nextAddress, isProtected: false);

                        // First page has a valid address from 64.
                        // After that currentAddress starts from beginning of bage (i.e. multiple of page size)
                        currentAddress = currentAddress == 64 ? currPage.Length : currentAddress + currPage.Length;
                        pagesSend++;
                        totalBytes += currPage.payloadLength;
                    }
                }

                _ = Interlocked.Add(ref total_pages_processed, pagesSend);
                _ = Interlocked.Add(ref total_bytes_processed, totalBytes);
            }
        }
    }
}