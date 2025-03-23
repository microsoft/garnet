// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.server;
using Garnet.server.BTreeIndex;
using Tsavorite.core;
class Program
{
    /// <summary>
    /// Playground for the B+tree index implementation
    /// </summary>
    /// <param name="args"></param>
    /// 
    static unsafe void something(SectorAlignedBufferPool pool, SectorAlignedMemory* ptr)
    {
        pool.Return(*ptr);
    }
    static unsafe void Main(string[] args)
    {
        var tree = new BTree((uint)BTreeNode.PAGE_SIZE);
        ulong N = 400000;
        bool verbose = true;
        bool sanity = false;
        if (args.Length > 0)
        {
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i] == "--verb")
                {
                    verbose = true;
                }
                else if (args[i] == "-s")
                {
                    sanity = true;
                }
                else if (args[i] == "-N")
                {
                    N = ulong.Parse(args[i + 1]);
                    break;
                }
            }
        }
        StreamID[] streamIDs = new StreamID[N];
        long duration = 0;
        long dur2 = 0;
        for (ulong i = 0; i < N; i++)
        {
            StreamID x = new StreamID(i + 1, 0);
            Debug.Assert(x.ms > 0);
            streamIDs[i] = x;
        }
        long start = Stopwatch.GetTimestamp();
        Stopwatch sw = new Stopwatch();
        sw.Start();
        for (ulong i = 0; i < N; i++)
        {
            tree.Insert((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0]), new Value(i + 1));
            var value = tree.Get((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0]));
            Debug.Assert(value.address == i + 1);
        }
        sw.Stop();
        dur2 = sw.ElapsedTicks;
        duration += Stopwatch.GetTimestamp() - start;
        Console.WriteLine(" Number of Fast Inserts = " + tree.FastInserts);
        double nanosecondsPerTick = (1_000_000_000.0) / Stopwatch.Frequency;
        if (verbose)
        {
            Console.WriteLine("Insertion done");
            Console.WriteLine(" Number of Fast Inserts = " + tree.FastInserts);
            Console.WriteLine("Number of Leaves = " + tree.LeafCount);
            Console.WriteLine("Number of Internal Nodes = " + tree.InternalCount);
            Console.WriteLine("Time for insertion = " + (double)dur2 * nanosecondsPerTick + " ns");
        }
        long insertion_time = (long)(dur2 * nanosecondsPerTick);
        sw.Reset();

        // point lookups
        sw.Start();
        for (ulong i = 0; i < N; i++)
        {
            var value = tree.Get((byte*)Unsafe.AsPointer(ref streamIDs[i].idBytes[0]));
            Debug.Assert(value.address == i + 1);
        }
        sw.Stop();
        long query_time = (long)(sw.ElapsedTicks * nanosecondsPerTick);
        if (verbose)
        {
            Console.WriteLine("Time for querying = " + query_time + " ns");
        }
        sw.Reset();

        // forward range query 
        double[] selectivities = [0.01, 0.05, 0.1];
        long[] range_query_times = new long[selectivities.Length];
        Value[] startVal = new Value[selectivities.Length];
        Value[] endVal = new Value[selectivities.Length];
        List<Value>[] list = new List<Value>[selectivities.Length];
        for (int i = 0; i < selectivities.Length; i++)
        {
            double selectivity = selectivities[i];
            ulong startIdx, endIdx;
            do
            {
                // get a random start index from 0 to N
                startIdx = (ulong)new Random().Next(0, (int)N);
                endIdx = (ulong)(startIdx + (N * selectivity));
            } while (endIdx >= N);
            sw.Start();
            var count = tree.Get((byte*)Unsafe.AsPointer(ref streamIDs[startIdx].idBytes[0]), (byte*)Unsafe.AsPointer(ref streamIDs[endIdx].idBytes[0]), out startVal[i], out endVal[i], out list[i]);
            Debug.Assert(count == (int)(endIdx - startIdx + 1));
            sw.Stop();
            range_query_times[i] = (long)(sw.ElapsedTicks * nanosecondsPerTick);
            if (verbose)
            {
                Console.WriteLine("Time for range query " + (i + 1) + " = " + range_query_times[i] + " ns");
            }
            sw.Reset();
        }
        if (verbose)
            Console.WriteLine("Range query check passed ");

        // now let's delete some keys 
        sw.Reset();
        int num_deletes = 100;
        int num_successfully_deleted = 0;
        for (int i = 0; i < num_deletes; i++)
        {
            // generate a random index to delete
            int idx = new Random().Next(0, (int)N);
            sw.Start();
            bool val = false;
            // bool val = tree.Delete((byte*)Unsafe.AsPointer(ref streamIDs[idx].idBytes[0]));
            sw.Stop();
            if (val)
            {
                num_successfully_deleted++;
            }
        }
        long deleteTime = (long)(sw.ElapsedTicks * nanosecondsPerTick);
        if (verbose)
        {
            Console.WriteLine("Number of keys deleted = " + num_successfully_deleted);
            Console.WriteLine("Time for deletion = " + deleteTime + " ns");
        }

        tree.Delete((byte*)Unsafe.AsPointer(ref streamIDs[N - 400].idBytes[0]));
        tree.Delete((byte*)Unsafe.AsPointer(ref streamIDs[N - 300].idBytes[0]));
        tree.Delete((byte*)Unsafe.AsPointer(ref streamIDs[N - 200].idBytes[0]));
        tree.Delete((byte*)Unsafe.AsPointer(ref streamIDs[N - 100].idBytes[0]));

        // do a range query to check again 
        tree.Get((byte*)Unsafe.AsPointer(ref streamIDs[N - 500].idBytes[0]), (byte*)Unsafe.AsPointer(ref streamIDs[N - 1].idBytes[0]), out Value startVal1, out Value endVal1, out List<Value> tombstones);
        Debug.Assert(tombstones.Count == 4);    
        Console.WriteLine("Delete check passed ");

        // print all times collected in a csv format 
        Console.WriteLine(insertion_time + ", " + query_time + ", " + range_query_times[0] + ", " + range_query_times[1] + ", " + range_query_times[2] + ", " + deleteTime);
        tree.Deallocate();
        Console.WriteLine("Num allocates = " + tree.stats.numAllocates);
        Console.WriteLine("Num deallocates = " + tree.stats.numDeallocates);
        Console.WriteLine("All checks passed");
    }
}