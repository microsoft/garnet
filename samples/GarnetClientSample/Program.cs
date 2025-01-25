// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using System.Threading.Tasks;

namespace GarnetClientSample
{
    /// <summary>
    /// Use Garnet with GarnetClient and StackExchange.Redis clients
    /// </summary>
    class Program
    {
        static readonly EndPoint endpoint = new IPEndPoint(IPAddress.Loopback, 6379);

        static readonly bool useTLS = false;

        static async Task Main()
        {
            await new GarnetClientSamples(endpoint, useTLS).RunAll();

            // await new SERedisSamples(address, port).RunAll();
        }
    }
}