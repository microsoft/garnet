// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using HdrHistogram;

namespace GarnetClientStress
{
    public class TaskScaling
    {
        readonly ManualResetEventSlim waiter = new();

        GarnetClient[] gclientArray;
        readonly Options opts;

        /// <summary>
        /// GarnetClientStress constructor
        /// </summary>
        /// <param name="opts"></param>
        public TaskScaling(Options opts)
        {
            this.opts = StressTestUtils.NotNull(opts, nameof(opts));
            this.gclientArray = new GarnetClient[opts.PartitionCount];
        }

        private async Task<bool> CreateClients()
        {
            var timeout = TimeSpan.FromSeconds(5);
            for (int i = 0; i < gclientArray.Length; i++)
            {
                gclientArray[i] = new GarnetClient(
                    opts.Address,
                    opts.Port,
                    opts.EnableTLS ? StressTestUtils.GetTlsOptions("GarnetTest") : null,
                    timeoutMilliseconds: (int)timeout.TotalMilliseconds,
                    maxOutstandingTasks: opts.ClientMaxOutstandingTasks,
                    recordLatency: true);
                Console.WriteLine($"Connecting to {opts.Address}:{opts.Port}");
                try
                {
                    await gclientArray[i].ConnectAsync().WaitAsync(timeout);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    return false;
                }
                //gclientArray[i].Connect();
                Console.WriteLine($"Connected to {opts.Address}:{opts.Port}");
            }
            return true;
        }

        public void Run()
        {
            if (!Task.Run(CreateClients).Result) return;

            Thread operationThread = new(() => OperationThread(opts.MaxOutstandingRequests));
            Thread monitorThread = new(MonitorThread);

            Console.WriteLine("Starting stress test");
            Console.WriteLine($"Starting operation thread with {gclientArray.Length} client instance(s)");
            operationThread.Start();
            monitorThread.Start();

            operationThread.Join();
            monitorThread.Join();
            while (outstandingTasks > 0) { Thread.Yield(); }
            Console.WriteLine("Finished stress test");
        }

        private bool PrintHistogram(int iter, int resetInterval)
        {
            bool printHeader = false;
            for (int i = 0; i < gclientArray.Length; i++)
            {
                var histogram = gclientArray[i]?.GetLatencyHistogram;
                if (histogram != null && histogram.TotalCount > 0)
                {
                    Console.WriteLine("{0,12:0.0}; {1,12:0.0}; {2,12:0.0}; {3,12:0.0}; {4,12:0.0}; {5,12:0.0}; {6,12:0.0}; {7,16:N0}; {8,12:N0}; {9,12:N0}; {10,12:N0}; {11,14:N0}",
                        histogram.GetValueAtPercentile(0) / OutputScalingFactor.TimeStampToMicroseconds,
                        histogram.GetValueAtPercentile(5) / OutputScalingFactor.TimeStampToMicroseconds,
                        histogram.GetValueAtPercentile(50) / OutputScalingFactor.TimeStampToMicroseconds,
                        histogram.GetMean() / OutputScalingFactor.TimeStampToMicroseconds,
                        histogram.GetValueAtPercentile(95) / OutputScalingFactor.TimeStampToMicroseconds,
                        histogram.GetValueAtPercentile(99) / OutputScalingFactor.TimeStampToMicroseconds,
                        histogram.GetValueAtPercentile(99.9) / OutputScalingFactor.TimeStampToMicroseconds,
                        histogram.TotalCount,
                        outstandingTasks,
                        gclientArray[i]?.PipelineLength(),
                        gclientArray[i]?.GetOutstandingTasksLimit,
                        numCanceled);
                    if (iter % resetInterval == 0)
                    {
                        gclientArray[i].ResetLatencyHistogram();
                        printHeader = true;
                    }
                }
            }
            return printHeader;
        }

        private void MonitorThread()
        {
            Console.WriteLine("Starting monitor thread");
            bool printHeader = true;

            int iter = 0;
            int resetInterval = 30;
            int delay = 1000;
            while (!waiter.IsSet)
            {
                if (!CheckConnection())
                {
                    //Console.WriteLine("Connection closed");
                    //PrintHistogram(iter, resetInterval);
                    return;
                }
                if (printHeader)
                {
                    Console.WriteLine(
                        "{0,12}; {1,12}; {2,12}; {3,12}; {4,12}; {5,12}; {6,12}; {7,16}; {8,12}; {9,12}; {10,12}; {11,12}",
                        "min(us)",
                        "5th(us)",
                        "median(us)",
                        "avg(us)",
                        "95th(us)",
                        "99th(us)",
                        "99.9th(us)",
                        "total-count",
                        "issued-req",
                        "pipe-len",
                        "tasks-limit",
                        "tasks-canceled"
                        );
                }
                printHeader = PrintHistogram(iter, resetInterval);
                iter++;
                Thread.Sleep(delay);
            }
        }

        volatile int outstandingTasks = 0;

        private bool CheckConnection()
        {
            for (int i = 0; i < gclientArray.Length; i++)
                if (gclientArray[i] != null && !gclientArray[i].IsConnected)
                    return false;
            return true;
        }

        private void OperationThread(int maxOutstandingTasks)
        {
            Random r = new(Guid.NewGuid().GetHashCode());
            try
            {
                int tasksRun = 0;
                while (true)
                {
                    if (!CheckConnection()) return;
                    //limit externally how much outstanding tasks to be created
                    while (outstandingTasks >= maxOutstandingTasks) Thread.Yield();
                    Interlocked.Increment(ref outstandingTasks);
                    int taskId = r.Next(0, int.MaxValue);
                    Task.Run(async () => await ShortRunningTask(taskId, opts.DbSize, opts.ValueLength));

                    //Specify maximum task spawned
                    //if (tasksRun > opts.RunTime * maxOutstandingTasks) break;
                    tasksRun++;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"OperationThread Exception: {ex.Message}");
            }

            while (outstandingTasks > 0)
            {
                Thread.Yield();
            }
            waiter.Set();
        }

        int numCanceled;

        public async Task ShortRunningTask(int taskId, int keyRange, int valueSize)
        {
            var clientId = taskId % gclientArray.Length;
            var gclient = gclientArray[clientId];
            Random r = new(taskId);

            bool read = r.Next(0, 100) < opts.ReadPercent;
            string key = r.Next(0, keyRange).ToString();
            string value = StressTestUtils.RandomValue(r, valueSize);
            bool success = true;

            TimeSpan timeout = TimeSpan.FromSeconds(1);
            var cts = new CancellationTokenSource(timeout);
            try
            {
                if (read)
                {
                    var returnValue = await gclient.StringGetAsync(key, cts.Token);
                    if (returnValue == null)
                    {
                        success = await gclient.StringSetAsync(key, value, cts.Token);
                        if (!success) goto terminate;
                    }
                    else
                    {
                        if (value.CompareTo(returnValue) < 0)
                        {
                            success = await gclient.StringSetAsync(key, value, cts.Token);
                            if (!success) goto terminate;
                        }
                    }
                }
                else
                {
                    success = await gclient.StringSetAsync(key, value, cts.Token);
                    if (!success) goto terminate;
                }

            terminate:
                if (!success)
                {
                    string msg = "An insert failed";
                    Console.WriteLine(msg);
                    throw new Exception(msg);
                }
            }
            catch
            {
                Interlocked.Increment(ref numCanceled);
                // Console.WriteLine($"ShortRunningTask threw exception: {ex.Message}");
                //if (numCanceled % 100 == 0)
                //    Console.WriteLine($"{numCanceled} tasks canceled");
            }
            finally
            {
                Interlocked.Decrement(ref outstandingTasks);
            }
        }
    }
}