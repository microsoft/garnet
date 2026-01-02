// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net;
using System.Text;
using Embedded.server;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using StackExchange.Redis;

namespace Resp.benchmark
{
    /// <summary>
    /// Dummy clients issuing commands as fast as possible, with varying number of
    /// threads, to stress server side.
    /// </summary>
    public partial class RespPerfBench
    {
        readonly int Start;
        readonly ManualResetEventSlim waiter = new();
        readonly Options opts;
        readonly IConnectionMultiplexer redis;
        internal EmbeddedRespServer server;
        internal RespServerSession[] sessions;

        KeyValuePair<RedisKey, RedisValue>[] database;

        ReqGen load_rg;
        ReqGen run_rg;

        volatile bool done = false;
        long total_ops_done = 0;
        long total_bytes_consumed = 0;

        public RespPerfBench(Options opts, int Start, IConnectionMultiplexer redis)
        {
            this.opts = opts;
            this.Start = Start;
            if (opts.Client == ClientType.SERedis)
                this.redis = redis;

            if (opts.Client == ClientType.InProc)
            {
                if (opts.EnableCluster && !opts.SkipLoad && !opts.LSet)
                    throw new Exception("Use --lset when running InProc and with cluster enabled to load data!");

                var serverOptions = AofBench.GetServerOptions(opts);
                server = new EmbeddedRespServer(serverOptions, Program.loggerFactory, new GarnetServerEmbedded());
                sessions = server.GetRespSessions(opts.NumThreads.Max());

                if (opts.EnableCluster)
                {
                    AddSlotRange([(0, 16383)]);
                    unsafe void AddSlotRange(List<(int, int)> slotRanges)
                    {
                        foreach (var slotRange in slotRanges)
                        {
                            var clusterAddSlotsRange = Encoding.ASCII.GetBytes($"*4\r\n$7\r\nCLUSTER\r\n$13\r\nADDSLOTSRANGE\r\n" +
                                $"${Garnet.common.NumUtils.CountDigits(slotRange.Item1)}\r\n{slotRange.Item1}\r\n" +
                                $"${Garnet.common.NumUtils.CountDigits(slotRange.Item2)}\r\n{slotRange.Item2}\r\n");
                            fixed (byte* req = clusterAddSlotsRange)
                                _ = sessions[0].TryConsumeMessages(req, clusterAddSlotsRange.Length);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Load DB with DbSize keys, starting from Start; specified #threads
        /// e.g., "0" => "0", "1" => "1", and so on
        /// </summary>
        /// <param name="loadDbThreads"></param>
        /// <param name="BatchSize"></param>
        /// <param name="keyLen"></param>
        /// <param name="valueLen"></param>
        /// <param name="numericValue"></param>
        public void LoadData(
            int loadDbThreads = 8,
            int BatchSize = 1 << 12,
            int keyLen = default,
            int valueLen = default,
            bool numericValue = false)
        {
            if (opts.Client == ClientType.InProc && loadDbThreads > sessions.Length)
            {
                foreach (var session in sessions)
                    session.Dispose();
                sessions = server.GetRespSessions(loadDbThreads);
            }

            if (load_rg != null)
                opts.DbSize = load_rg.DbSize;

            if (opts.DbSize < loadDbThreads)
                loadDbThreads = opts.DbSize;

            if (opts.DbSize % loadDbThreads != 0)
                throw new Exception($"DbSize {opts.DbSize} must be divisible by number of loading threads {loadDbThreads}");

            int loadBatchSize = opts.DbSize / loadDbThreads;
            loadBatchSize = loadBatchSize < BatchSize ? loadBatchSize : BatchSize;
            if (!opts.LSet)
                LightOperate(OpType.MSET, opts.DbSize, loadBatchSize, loadDbThreads, opts.DbSize / loadDbThreads, default, load_rg, false, false, keyLen, valueLen, numericValue: numericValue);
            else
                LightOperate(OpType.SET, opts.DbSize, loadBatchSize, loadDbThreads, opts.DbSize / loadDbThreads, default, load_rg, false, false, keyLen, valueLen, numericValue: numericValue);
            load_rg = null;

            if (opts.Client != ClientType.InProc)
                GetDBSIZE(loadDbThreads);
        }

        private unsafe void GetDBSIZE(int loadDbThreads)
        {
            if (opts.DbSize > (1 << 20)) return;
            string dbSize;
            if (opts.Client != ClientType.SERedis)
            {
                var req = Encoding.ASCII.GetBytes("*1\r\n$6\r\nDBSIZE\r\n");
                var lighClientOnResponseDelegate = new LightClient.OnResponseDelegateUnsafe(ReqGen.OnResponse);
                using LightClient client = new(new IPEndPoint(IPAddress.Parse(opts.Address), opts.Port), (int)OpType.DBSIZE, lighClientOnResponseDelegate, 128, opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);

                client.Connect();
                client.Authenticate(opts.Auth);
                client.Send(req, req.Length, 1);
                client.CompletePendingRequests();

                fixed (byte* buf = client.ResponseBuffer)
                {
                    byte* ptr = buf;
                    RespReadResponseUtils.TryReadIntegerAsString(out dbSize, ref ptr, ptr + client.ResponseBuffer.Length);
                }
            }
            else
            {
                var db = redis.GetDatabase(0);
                var keyCount = db.Execute("DBSIZE");
                dbSize = keyCount.ToString();
            }
            Console.WriteLine($">>> Loaded {dbSize} keys into the DB >>>");
        }

        public void LoadHLLData(
            int loadDbThreads = 32,
            int BatchSize = 1 << 12,
            int keyLen = default,
            int valueLen = default)
        {
            if (opts.DbSize / BatchSize == 0)
            {
                BatchSize = opts.DbSize;
                loadDbThreads = 1;
            }
            Console.WriteLine($"Generating {OpType.MPFADD} request batches");
            LightOperate(
                OpType.MPFADD, //OpType
                opts.DbSize, //TotalOps
                BatchSize, //BatchSize
                loadDbThreads, //NumThreads
                opts.DbSize / loadDbThreads, //OpsPerThread
                default,//runTime
                load_rg,//ReqGen
                false, //randomGen
                false, //randomServe
                keyLen, //keyLen
                valueLen //valueLen
                );
            load_rg = null;
        }

        /// <summary>
        /// Perform async GET ops on same thread, for a total of NumOps ops
        /// </summary>
        public void PerformAsyncGET(int NumOps, int BatchSize = 1 << 12)
        {
            if (database == null)
                CreateLocalDB();

            using var redis = ConnectionMultiplexer.Connect($"{opts.Address}:{opts.Port},connectTimeout=999999,syncTimeout=999999");
            var db = redis.GetDatabase(0);

            int DbSize = database.Length;

            bool checkResults = false;

            // Query database
            Random r = new(0);
            Random r2 = new(0);

            Stopwatch sw = new();
            sw.Start();
            int idx = 0;
            var tasks = new Task[BatchSize];
            for (int b = 0; b < NumOps; b++)
            {
                tasks[idx] = db.StringGetAsync(database[r.Next(DbSize)].Key);
                idx++;
                if (idx == BatchSize)
                {
                    Task.WaitAll(tasks);
                    if (checkResults)
                    {
                        for (int k = 0; k < idx; k++)
                        {
                            if (database[r2.Next(DbSize)].Value != ((Task<RedisValue>)tasks[k]).Result)
                                Console.WriteLine("BatchedAsyncGet: Error");
                        }
                    }
                    idx = 0;
                }
            }
            if (idx > 0)
            {
                Task.WaitAll(tasks);
                if (checkResults)
                {
                    for (int k = 0; k < idx; k++)
                    {
                        if (database[r2.Next(DbSize)].Value != ((Task<RedisValue>)tasks[k]).Result)
                            Console.WriteLine("BatchedAsyncGet: Error");
                    }
                }
            }
            sw.Stop();

            Console.WriteLine("Total time: {0}ms for {1} ops", sw.ElapsedMilliseconds, NumOps);

            Console.WriteLine("AsyncGet: Success");
        }

        /// <summary>
        /// Perform real MGET ops from NumThreads threads, with NumOps ops/thread
        /// </summary>
        public void PerformMGET(int NumOps, int NumThreads, int BatchSize = 1 << 12)
        {
            if (database == null)
                CreateLocalDB();

            // Query database
            Thread[] workers = new Thread[NumThreads];

            // Run the experiment.
            for (int idx = 0; idx < NumThreads; idx++)
            {
                int x = idx;
                workers[idx] = new Thread(() => MGetThreadRunner(x, NumOps, BatchSize));
            }

            // Start threads.
            foreach (Thread worker in workers)
                worker.Start();

            Stopwatch swatch = new();
            swatch.Start();
            foreach (Thread worker in workers)
            {
                worker.Join();
            }
            swatch.Stop();

            Console.WriteLine("Total time: {0}ms for {1} gets", swatch.ElapsedMilliseconds, NumOps * NumThreads);
            Console.WriteLine("MGet: Success");
        }

        /// <summary>
        /// Perform lightweight ops using NumThreads threads, NumOps ops/thread
        /// </summary>
        /// <param name="TotalOps"></param>
        /// <param name="NumThreads"></param>
        /// <param name="BatchSize"></param>
        /// <param name="runTime"></param>
        /// <param name="randomGen"></param>
        /// <param name="randomServe"></param>
        /// <param name="keyLen"></param>
        /// <param name="valueLen"></param>
        /// <param name="ttl"></param>
        /// <param name="opType"></param>
        public void Run(
            OpType opType,
            int TotalOps,
            int[] NumThreads,
            int BatchSize = 1 << 12,
            TimeSpan runTime = default,
            bool randomGen = true,
            bool randomServe = true,
            int keyLen = default,
            int valueLen = default,
            int ttl = 0)
        {
            ReqGen rg;

            if (run_rg != null)
                rg = run_rg;
            else
            {
                rg = new ReqGen(Start, opts.DbSize, TotalOps, BatchSize, opType, randomGen, randomServe, keyLen, valueLen, ttl: ttl);
                rg.Generate();
            }

            foreach (var numThread in NumThreads)
            {
                GC.Collect();
                GC.WaitForFullGCComplete();
                LightOperate(opType, TotalOps, BatchSize, numThread, 0, runTime, rg, randomGen, randomServe);
            }
            run_rg = null;
        }

        /// <summary>
        /// Perform lightweight ops using NumThreads threads, NumOps ops/thread
        /// </summary>
        /// <param name="TotalOps"></param>
        /// <param name="BatchSize"></param>
        /// <param name="NumThreads"></param>
        /// <param name="OpsPerThread"></param>
        /// <param name="runTime"></param>
        /// <param name="rg"></param>
        /// <param name="randomGen"></param>
        /// <param name="randomServe"></param>
        /// <param name="keyLen"></param>
        /// <param name="valueLen"></param>
        /// <param name="numericValue"></param>
        /// <param name="verbose"></param>
        /// <param name="opType"></param>
        public ReqGen LightOperate(
            OpType opType,
            int TotalOps,
            int BatchSize,
            int NumThreads,
            int OpsPerThread = 0,
            TimeSpan runTime = default,
            ReqGen rg = null,
            bool randomGen = true,
            bool randomServe = true,
            int keyLen = default,
            int valueLen = default,
            bool numericValue = false,
            bool verbose = true)
        {
            if (rg == null)
            {
                rg = new ReqGen(Start, opts.DbSize, TotalOps, BatchSize, opType, randomGen, randomServe, keyLen, valueLen, numericValue, verbose, flatBufferClient: (opts.Client == ClientType.SERedis || opts.Client == ClientType.GarnetClientSession), ttl: opts.Ttl);
                rg.Generate();
            }

            if (verbose)
            {
                Console.WriteLine();
                Console.WriteLine($"Operation type: {opType}");
                Console.WriteLine($"Num threads: {NumThreads}");
            }

            // Query database
            var workers = new Thread[NumThreads];

            // Run the experiment.
            for (var idx = 0; idx < NumThreads; idx++)
            {
                var x = idx;
                workers[idx] = opts.Client switch
                {

                    ClientType.LightClient => new Thread(() => LightOperateThreadRunner(x, OpsPerThread, opType, rg)),
                    ClientType.GarnetClientSession => new Thread(() => GarnetClientSessionOperateThreadRunner(OpsPerThread, opType, rg)),
                    ClientType.SERedis => new Thread(() => SERedisOperateThreadRunner(OpsPerThread, opType, rg)),
                    ClientType.InProc => new Thread(() => InProcOperateThreadRunner(x, OpsPerThread, opType, rg)),
                    _ => throw new Exception($"ClientType {opts.Client} not supported"),
                };
            }

            AofAddress beginAddress = default;
            if (opts.Client == ClientType.InProc && sessions[0].StoreWrapper.appendOnlyFile != null)
                beginAddress = sessions[0].StoreWrapper.TailAddress;

            // Start threads.
            foreach (var worker in workers)
                worker.Start();

            waiter.Set();

            Stopwatch swatch = new();
            swatch.Start();
            if (OpsPerThread == 0)
            {
                if (runTime == default) runTime = TimeSpan.FromSeconds(15); // default
                Thread.Sleep(runTime);
                done = true;
            }
            foreach (var worker in workers)
                worker.Join();

            swatch.Stop();

            var seconds = swatch.ElapsedMilliseconds / 1000.0;
            var opsPerSecond = total_ops_done / seconds;
            var byteConsumerPerSecond = (total_bytes_consumed / seconds) / (double)1_000_000_000;

            if (verbose)
            {
                Console.WriteLine($"[Total time]: {swatch.ElapsedMilliseconds:N2}ms for {total_ops_done:N2} ops");
                Console.WriteLine($"[Throughput]: {opsPerSecond:N2} ops/sec");
                if (ClientType.InProc == opts.Client)
                {
                    var count = sessions[0].DbSize();
                    Console.WriteLine($"[BytesConsumed]: {total_bytes_consumed:N0} bytes");
                    Console.WriteLine($"[BytesConsumedPerSecond]: {byteConsumerPerSecond:N2} GiB/sec");
                    Console.WriteLine($"[DB Size]: {count}");
                    if (sessions[0].StoreWrapper.appendOnlyFile != null)
                    {
                        var tailAddress = sessions[0].StoreWrapper.TailAddress;
                        var aofSize = tailAddress.AggregateDiff(beginAddress);

                        var tpt = (aofSize / seconds) / (double)1_000_000_000;
                        Console.WriteLine($"[AOF Total Size]: {aofSize:N2} bytes");
                        Console.WriteLine($"[AOF Append Tpt]: {tpt:N2} GiB/sec");
                    }
                }
            }

            done = false;
            total_ops_done = 0;
            total_bytes_consumed = 0;
            waiter.Reset();

            return rg;
        }

        private unsafe void LightOperateThreadRunner(int threadId, int NumOps, OpType opType, ReqGen rg)
        {
            var lighClientOnResponseDelegate = new LightClient.OnResponseDelegateUnsafe(ReqGen.OnResponse);
            using ClientBase client = new LightClient(new IPEndPoint(IPAddress.Parse(opts.Address), opts.Port), (int)opType, lighClientOnResponseDelegate, rg.GetBufferSize(), opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);

            client.Connect();
            client.Authenticate(opts.Auth);

            var maxReqs = (NumOps / rg.BatchCount);
            var numReqs = 0;

            waiter.Wait();

            Stopwatch sw = new();
            sw.Start();
            while (!done)
            {
                byte[] buf = rg.GetRequest(out var len, threadId);
                client.Send(buf, len, (opType == OpType.MSET || opType == OpType.MPFADD) ? 1 : rg.BatchCount);
                client.CompletePendingRequests();
                numReqs++;
                if (numReqs == maxReqs) break;
            }
            sw.Stop();

            Interlocked.Add(ref total_ops_done, numReqs * rg.BatchCount);
        }

        private void GarnetClientSessionOperateThreadRunner(int NumOps, OpType opType, ReqGen rg)
        {
            switch (opType)
            {
                case OpType.MSET:
                    break;
                default:
                    throw new Exception($"opType: {opType} benchmark not supported with GarnetClientSession!");
            }
            var c = new GarnetClientSession(new IPEndPoint(IPAddress.Parse(opts.Address), opts.Port), new(), tlsOptions: opts.EnableTLS ? BenchUtils.GetTlsOptions(opts.TlsHost, opts.CertFileName, opts.CertPassword) : null);
            c.Connect();
            if (opts.Auth != null)
            {
                c.Execute("AUTH", opts.Auth);
                c.CompletePending();
            }

            var maxReqs = NumOps / rg.BatchCount;
            var numReqs = 0;

            waiter.Wait();

            Stopwatch sw = new();
            sw.Start();
            while (!done)
            {
                var reqArgs = rg.GetRequestArgs();
                reqArgs.Insert(0, "MSET");
                c.Execute([.. reqArgs]);
                c.CompletePending(true);
                numReqs++;
                if (numReqs == maxReqs) break;
            }
            sw.Stop();

            Interlocked.Add(ref total_ops_done, numReqs * rg.BatchCount);
        }

        private void SERedisOperateThreadRunner(int NumOps, OpType opType, ReqGen rg)
        {
            switch (opType)
            {
                case OpType.MSET:
                    break;
                default:
                    throw new Exception($"opType: {opType} benchmark not supported with SERedis!");
            }
            var db = redis.GetDatabase(0);

            var maxReqs = NumOps / rg.BatchCount;
            var numReqs = 0;

            waiter.Wait();

            Stopwatch sw = new();
            sw.Start();
            while (!done)
            {
                var reqArgs = rg.GetRequestArgs();
                for (var i = 0; i < reqArgs.Count; i += 2)
                    db.StringSet(reqArgs[i], reqArgs[i + 1]);
                numReqs++;
                if (numReqs == maxReqs) break;
            }
            sw.Stop();

            Interlocked.Add(ref total_ops_done, numReqs * rg.BatchCount);
        }

        private unsafe void InProcOperateThreadRunner(int threadId, int NumOps, OpType opType, ReqGen rg)
        {
            var maxReqs = NumOps / rg.BatchCount;
            var numReqs = 0;

            waiter.Wait();

            Stopwatch sw = new();
            var bytesConsumed = 0L;
            sw.Start();
            while (!done)
            {
                var buf = rg.GetRequest(out var len, threadId);
                fixed (byte* ptr = buf)
                    _ = sessions[threadId].TryConsumeMessages(ptr, len);

                bytesConsumed += len;
                numReqs++;
                if (numReqs == maxReqs) break;
            }
            sw.Stop();

            Interlocked.Add(ref total_ops_done, numReqs * rg.BatchCount);
            Interlocked.Add(ref total_bytes_consumed, bytesConsumed);
        }

        private void MGetThreadRunner(int threadid, int NumOps, int BatchSize = 1 << 12)
        {
            var checkResults = false;
            var DbSize = database.Length;

            using var redis = ConnectionMultiplexer.Connect($"{opts.Address}:{opts.Port},connectTimeout=999999,syncTimeout=999999");
            var db = redis.GetDatabase(0);

            Random r = new(threadid);
            Random r2 = new(threadid);

            Stopwatch sw = new();
            sw.Start();
            var idx = 0;
            var getBatch = new RedisKey[BatchSize];
            for (var b = 0; b < NumOps; b++)
            {
                getBatch[idx++] = database[r.Next(DbSize)].Key;
                if (idx == BatchSize)
                {
                    var result = db.StringGet(getBatch);
                    if (checkResults)
                    {
                        for (var k = 0; k < idx; k++)
                        {
                            if (database[r2.Next(DbSize)].Value != result[k])
                                Console.WriteLine("OperateThreadRunner: Error");
                        }
                    }
                    idx = 0;
                }
            }
            if (idx > 0)
            {
                var result = db.StringGet([.. getBatch.Take(idx)]);
                if (checkResults)
                {
                    for (var k = 0; k < idx; k++)
                    {
                        if (database[r2.Next(DbSize)].Value != result[k])
                            Console.WriteLine("OperateThreadRunner: Error");
                    }
                }
            }
            sw.Stop();

            Console.WriteLine("MGetThreadRunner: Thread {0} - Total time: {1}ms for {2} gets", threadid, sw.ElapsedMilliseconds, NumOps);
        }

        private void CreateLocalDB()
        {
            Console.WriteLine($"Creating database of size {opts.DbSize}");
            database = new KeyValuePair<RedisKey, RedisValue>[opts.DbSize];
            for (var k = 0; k < opts.DbSize; k++)
            {
                database[k] = new KeyValuePair<RedisKey, RedisValue>(new RedisKey(k.ToString()), new RedisValue(k.ToString()));
            }
            Console.WriteLine("Create completed");
        }

        private void LoadDatabaseStringSet(int BatchSize = 1 << 12)
        {
            using var redis = ConnectionMultiplexer.Connect($"{opts.Address}:{opts.Port},connectTimeout=999999,syncTimeout=999999");
            var db = redis.GetDatabase(0);

            var DbSize = database.Length;

            Console.WriteLine($"Loading database of size {database.Length}");

            Stopwatch sw = new();
            sw.Start();
            var MSet = true;
            if (MSet)
            {
                for (var b = 0; b < DbSize; b += BatchSize)
                {
                    db.StringSet([.. database.Skip(b).Take(BatchSize)]);
                    if (b > 0 && b % 1000000 == 0)
                        Console.WriteLine(b);
                }
            }
            else
            {
                var tasks = new Task[BatchSize];
                var idx = 0;
                for (var b = 0; b < DbSize; b++)
                {
                    tasks[idx] = db.StringSetAsync(database[b].Key, database[b].Value);
                    idx++;
                    if (idx == BatchSize)
                    {
                        Task.WaitAll(tasks);
                        idx = 0;
                    }
                    if (b > 0 && b % 1000000 == 0)
                        Console.WriteLine(b);
                }
                if (idx > 0)
                    Task.WaitAll(tasks);
            }
            sw.Stop();
            Console.WriteLine("Load: Total time: {0}ms for {1} set operations", sw.ElapsedMilliseconds, DbSize);
        }
    }
}