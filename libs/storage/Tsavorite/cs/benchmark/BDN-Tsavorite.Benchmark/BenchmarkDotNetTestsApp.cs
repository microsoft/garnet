// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using BenchmarkDotNet.Running;
using System.IO;

namespace BenchmarkDotNetTests
{
    public class BenchmarkDotNetTestsApp
    {
        public static string TestDirectory => Path.Combine(Path.GetDirectoryName(typeof(BenchmarkDotNetTestsApp).Assembly.Location), "Tests");

        public static void Main(string[] args)
        {
            BenchmarkSwitcher.FromAssembly(typeof(BenchmarkDotNetTestsApp).Assembly).Run(args);
        }
    }
}
