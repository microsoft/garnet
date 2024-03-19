---
id: resp-bench
sidebar_label: Resp.benchmark
title: The Resp.benchmark Tool
---

# The Resp.benchmark Tool

**Garnet** project contains a Benchmark tool for running RESP benchmarking using different clients, different workloads and different strategies for measuring throughput, performance and latency.

This tool exists under the folder: 

```bash 
> Garnet\benchmark\Resp.benchmark
```

## Usage

The options available with Resp.benchmark can be seen by running the benchmark tool using  the `--help` command line switch. Example:

```bash
> cd Garnet\benchmark\Resp.benchmark
> dotnet run -c Release -f net8.0 -- --help
```
---

## Implementation Details

The benchmark operates in two phases.

- Load key in database
- Run benchmark for a single command
	
In both phases ReqGen is used to generate the requests for either loading data
or performing the benchmark.

There are several options to generate requests as indicated by the constructor of the ReqGen class.


```csharp
        public ReqGen(
            int Start,
            int DbSize,
            int NumOps,
            int BatchSize,
            OpType opType,
            bool randomGen = true,
            bool randomServe = true,
            int keyLen = default,
            int valueLen = default)
```

- Start: Key offset.
- DbSize: Number of keys in the database
- NumOps: Total number of operations to be perfomed
- BatchSize: Total number of ops in a batch.
- opType: Operation to be performed (e.g. GET, MSET, INCR)
- randomGen: Whether to generate keys in sequence or randomly.
- randomServe: Whether serve the requests randomly or in the sequence they were generated.
- keyLen: The least number of bytes in a key. Keys are assigned from (Start,DbSize) range and padded accordingly to build a set of at least. The default size equals the maximum number of digits of DbSize.
- valueLen: The total number of bytes in a value.


The following method is used to load the data into the database
```csharp
        public void LoadData(
            int loadDbThreads = 8, 
            int BatchSize = 1 << 12, 
            int keyLen = default, 
            int valueLen = default)
```	
- loadDbThreads: Total number of threads used for loading
- BatchSize: Total number of key-value pair in a batch
- keyLen: length of a key in bytes.
- valueLen: length of a value in bytes.

The following method is used to run an iteration of the benchmark for a given operation with the specified parameters

```csharp
        public void Run(
            OpType opType,
            int TotalOps,
            int[] NumThreads,
            int BatchSize = 1 << 12,
            TimeSpan runTime = default,
            bool randomGen = true,
            bool randomServe = true,
            int keyLen = default,
            int valueLen = default)
```

- opType: operation to benchmark
- totalops: total ops executed for the given benchmark.
- NumThreads: Total number of clients used to run the benchmark
- BatchSize: Total number of operations in a single batch.
- runTime: Minimum duration to run the benchmark.
- randomGen: The order in which keys are generated in the requests.
- randomServe: The order in which the request buffers are accessed.
- keyLen: least number of bytes in a single key.
- valueLen: total number of bytes in a value.

## Commands benchmarked

The **commands** that can be benchmarked from each category are:

### Raw strings commands

* SET
* GET
* MGET
* INCR 
* SETEX

### Bitmaps commands

* SETBIT
* BITOP
* GETBIT
* BITCOUNT
* BITPOS
* BITOP_AND
* BITOP_OR 
* BITOP_XOR
* BITOP_NOT
* BITFIELD_GET
* BITFIELD_SET 
* BITFIELD_INCR

### Hyperloglog commands

* PFADD
* PFCOUNT
* PFMERGE

### Sorted Sets & Geo commands

* ZADD 
* GEOADD
* ZREM 
* ZCARD 

### Server commands

* PING

## Type of benchmarks

### Online benchmarks

These options measure performance of the server in a continuous online mode.

From the folder:
```
Garnet/benchmark/Resp.benchmark
```

For a run with 1 client session: 

```bash
dotnet run -c Release -f net8.0 -- --online --op-workload GET,SET --op-percent 50,50 -b 1  -t 1
```

For a run with 16 client sessions:

```bash
dotnet run -c Release -f net8.0 -- --online --op-workload GET,SET --op-percent 50,50 -b 1  -t 16
```

For a run using ZADD and ZCARD commands:

```bash
dotnet run -c Release -f net8.0 -- --online --op-workload ZADD,ZCARD --op-percent 50,50 -b 1  -t 1 --keylength --client SERedis
```

**Where:**

* --op-workload: the different commands to send.
* --online: The type of the benchmark to do.
* --op-percent: The weight of the load for each command.
* --t or threads: The number of conexions the tool will create to send the load.
* --client: The client to use. GarnetClientSession is good option to stress the system to its maximum. SERedis stands for StackExchange.Redis library.
* --keylength: The length for the size of the key.


### Offline benchmarks

This category of benchmark use our `LightClient` with pre-created batches, to measure how much throughput the server can achieve for a given period of time.

Some examples of running offline benchmarks are:

```bash
dotnet run -c Release -f net8.0 -- --op GET -t 2,4,8,16 -b 512 --dbsize 10241024 --keylength 1024
```

```bash
dotnet run -c Release -f net8.0 -- --op ZADDCARD -t 64 --b 512 --dbsize 16777216 --keylength 512
```
**Where:**

* --op: the command to send. ZADDCARD will exercise the ZADD and ZCARD in the same run.
* --t or threads: The number of threads to use.
* --dbsize: The amount of keys to create in the store

## Preloading data for benchmarks

You can pre-populate the store with keys before running GET benchmarks by performing the following steps:

- Execute MSET with the Resp Benchmark tool:

```bash
dotnet run -c Release -f net8.0 -- --op MSET --dbsize 16777216 --keylength 512
```

Then execute the GET command in online mode:

```bash
dotnet run -c Release -f net8.0 -- --op-workload GET --online -b 1 --op-percent 100 -t 64 --client GarnetClientSession --itp 1024 --runtime -1 --dbsize 16777216 --keylength 512
```

If you want to know how high the hit rate you can execute from any client CLI tool:

```
INFO stats
```

Look at the **garnet_hit_rate** metric, the value ideally, should be close to 100.

Metrics need to be enabled in the Garnet server for getting stats with the INFO command:

```
 --latency-monitor --metrics-sampling-freq 5
```
