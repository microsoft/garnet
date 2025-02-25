// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.server;

namespace Garnet.test.server
{
    /// <summary>
    /// Garnet server entry point
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                using var server = new GarnetServer(args);

                // Start the server
                server.Start();

                Thread.Sleep(Timeout.Infinite);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unable to initialize server due to exception: {ex.Message}");
            }
        }
    }
}