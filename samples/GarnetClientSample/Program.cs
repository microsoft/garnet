// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;

namespace GarnetClientSample
{
    /// <summary>
    /// Use Garnet with GarnetClient and StackExchange.Redis clients
    /// </summary>
    class Program
    {
      

        static async Task Main()
        {
            BenchmarkRunner.Run<SampleGet>();
          

            // await new SERedisSamples(address, port).RunAll();
        }

    }
    public class SampleGet
    {
        static readonly string address = "127.0.0.1";
        static readonly int port = 6379;
        static readonly bool useTLS = false;
        GarnetClientSamples samples;

        [GlobalSetup]
        public void Setup()
        {
            samples =  new GarnetClientSamples(address, port, useTLS);
        }
        [Benchmark]
        public void RunGet()
        {
            for (int i = 0; i < 100; i++)
            {
                samples.RunAll();
            }
        }
    }
}