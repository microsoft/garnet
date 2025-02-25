// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Threading;
using Garnet.server;
using Garnet.server.Auth.Settings;

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
                var port = int.Parse(Environment.GetEnvironmentVariable("GARNET_TEST_PORT"));

                using var server = new GarnetServer(new GarnetServerOptions()
                {
                    DisableObjects = true,
                    DisablePubSub = true,
                    EndPoint = new IPEndPoint(IPAddress.Loopback, port),
                    EnableDebugCommand = ConnectionProtectionOption.Local
                });

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