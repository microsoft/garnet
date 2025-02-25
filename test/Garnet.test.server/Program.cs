// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.server;

namespace Garnet.test.server
{
    /// <summary>
    /// Garnet test server entry point
    /// </summary>
    public class Program
    {
        public static void Main(string[] args)
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