// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Tsavorite.core;

namespace BenchmarkDotNetTests
{
    public class EpochTests
    {
        LightEpoch epoch;

        [GlobalSetup]
        public void Setup()
        {
            epoch = new LightEpoch();
        }

        [GlobalCleanup]
        public void TearDown()
        {
            epoch.Dispose();
        }

        [Benchmark]
        public void ResumeSuspend()
        {
            for (var i = 0; i < 1_000_000; i++)
            {
                epoch.Resume();
                epoch.Suspend();
            }
        }

        [Benchmark]
        public void ProtectAndDrain()
        {
            epoch.Resume();
            for (var i = 0; i < 1_000_000; i++)
            {
                epoch.ProtectAndDrain();
            }
            epoch.Suspend();
        }

        void MyAction() { }

        [Benchmark]
        public void BumpCurrentEpoch()
        {
            epoch.Resume();
            for (var i = 0; i < 100_000; i++)
            {
                epoch.BumpCurrentEpoch(MyAction);
            }
            epoch.Suspend();
        }
    }
}