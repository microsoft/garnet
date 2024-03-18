// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Bitmap
{
    public static class Common
    {
        public static void PrintElapsedTime(TimeSpan tspan, int iter, long bytes, String msg)
        {
            double elapsedSeconds = tspan.TotalSeconds;
            double elapsedTimeMillis = tspan.TotalMilliseconds;
            double bytesAccessed = iter * bytes;
            double bytesPerSecond = (((double)(bytesAccessed) / (1024L * 1024L)) / elapsedSeconds);

            Console.WriteLine($"Total number of iterations: {iter}");
            Console.WriteLine("Total time:{0}", tspan);
            Console.WriteLine("Total time:{0} ms for {1}", elapsedTimeMillis, msg);
            Console.WriteLine("Total time:{0} s for {1}", elapsedSeconds, msg);
            Console.WriteLine($"Throughtput: {bytesPerSecond} MB/s");
            Console.WriteLine($"Operations: {iter / elapsedSeconds} Op/s");
            Console.WriteLine("Average latency: {0} ms", elapsedTimeMillis / (double)iter);
        }
    }
}