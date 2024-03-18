// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;

namespace GarnetClientSample
{
    /// <summary>
    /// Use Garnet with GarnetClient and StackExchange.Redis clients
    /// </summary>
    class Program
    {
        static readonly string address = "127.0.0.1";
        static readonly int port = 3278;
        static readonly bool useTLS = false;

        static async Task Main()
        {
            await new GarnetClientSamples(address, port, useTLS).RunAll();

            // await new SERedisSamples(address, port).RunAll();
        }
    }
}