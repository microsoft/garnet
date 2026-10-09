// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using System.Text;
using Garnet.client;

namespace Client.benchmark
{
    /// <summary>
    /// A benchmark arm: wraps a single connected client instance and exposes a uniform,
    /// allocation-light way to seed a key and issue the measured operation. Implemented as
    /// a <see langword="readonly struct"/> so the workload runner can be generic over the
    /// concrete driver (<c>where TDriver : struct, IOpDriver</c>) and dispatch the per-op
    /// call through a constrained (devirtualized) invocation rather than an interface call.
    /// The struct holds only references, so copying it across worker threads still shares
    /// the one underlying client.
    /// </summary>
    internal interface IOpDriver : IDisposable
    {
        /// <summary>Establishes the client connection to the server.</summary>
        void Connect();

        /// <summary>Seeds the key read by <see cref="IssueAsync"/> (not measured).</summary>
        Task SeedAsync();

        /// <summary>Issues a single measured operation and completes when its reply is parsed.</summary>
        Task IssueAsync();
    }

    /// <summary>
    /// Driver for the original <see cref="GarnetClient"/> (inline send path). The measured
    /// operation is a GET of the seeded key.
    /// </summary>
    internal readonly struct GarnetClientDriver : IOpDriver
    {
        readonly GarnetClient client;
        readonly string key;
        readonly string value;

        public GarnetClientDriver(EndPoint endpoint, string key, string value)
        {
            client = new GarnetClient(endpoint);
            this.key = key;
            this.value = value;
        }

        public void Connect() => client.Connect();

        public Task SeedAsync() => client.StringSetAsync(key, value);

        public Task IssueAsync() => client.StringGetAsync(key);

        public void Dispose() => client.Dispose();
    }

    /// <summary>
    /// Driver for the <see cref="GarnetLightClient"/> (duplex-ring send path). The measured
    /// operation is a GET of the seeded key, issued through the raw RESP execute API so the
    /// wire format is identical to <see cref="GarnetClientDriver"/>.
    /// </summary>
    internal readonly struct GarnetLightClientDriver : IOpDriver
    {
        static readonly Memory<byte> GET = "$3\r\nGET\r\n"u8.ToArray();
        static readonly Memory<byte> SET = "$3\r\nSET\r\n"u8.ToArray();

        readonly GarnetLightClient client;
        readonly Memory<byte>[] getArgs;
        readonly Memory<byte>[] setArgs;

        public GarnetLightClientDriver(EndPoint endpoint, string key, string value)
        {
            client = new GarnetLightClient(endpoint);
            var keyBytes = Encoding.UTF8.GetBytes(key);
            var valueBytes = Encoding.UTF8.GetBytes(value);
            getArgs = [keyBytes];
            setArgs = [keyBytes, valueBytes];
        }

        public void Connect() => client.Connect();

        public Task SeedAsync() => client.ExecuteForStringResultAsync(SET, setArgs);

        public Task IssueAsync() => client.ExecuteForStringResultAsync(GET, getArgs);

        public void Dispose() => client.Dispose();
    }
}