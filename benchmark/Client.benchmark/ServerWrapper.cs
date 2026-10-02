// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using System.Net.Sockets;
using Garnet;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Client.benchmark
{
    /// <summary>
    /// Hosts a single real <see cref="GarnetServer"/> in-process, bound to a loopback
    /// endpoint on an ephemeral port, configured for a pure in-memory workload (no
    /// storage tier, no AOF). The benchmark client connects to <see cref="EndPoint"/>
    /// over a real TCP socket, so the full network and server stack participates in the
    /// measurement.
    /// </summary>
    internal sealed class ServerWrapper : IDisposable
    {
        readonly GarnetServer server;

        /// <summary>
        /// The loopback endpoint the server is listening on.
        /// </summary>
        public IPEndPoint EndPoint { get; }

        /// <summary>
        /// The loopback host the server is listening on.
        /// </summary>
        public string Host => EndPoint.Address.ToString();

        /// <summary>
        /// The ephemeral port the server is listening on.
        /// </summary>
        public int Port => EndPoint.Port;

        /// <summary>
        /// Creates and starts an in-process server on a free loopback port.
        /// </summary>
        /// <param name="disableObjects">Disable the object (collection) types; the benchmark drives raw-string ops.</param>
        /// <param name="loggerFactory">Optional logger factory for server diagnostics.</param>
        public ServerWrapper(bool disableObjects = true, ILoggerFactory loggerFactory = null)
        {
            EndPoint = new IPEndPoint(IPAddress.Loopback, GetFreeLoopbackPort());

            var opts = new GarnetServerOptions(loggerFactory?.CreateLogger("ServerWrapper"))
            {
                EndPoints = [EndPoint],
                EnableStorageTier = false,
                IndexMemorySize = "1g",
                DisableObjects = disableObjects,
                QuietMode = true,
                DeviceFactoryCreator = new LocalStorageNamedDeviceFactoryCreator(logger: loggerFactory?.CreateLogger("ServerWrapper")),
            };

            server = new GarnetServer(opts, loggerFactory);
            server.Start();
        }

        /// <summary>
        /// Reserves a free TCP port on the loopback interface.
        /// </summary>
        static int GetFreeLoopbackPort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            try
            {
                return ((IPEndPoint)listener.LocalEndpoint).Port;
            }
            finally
            {
                listener.Stop();
            }
        }

        public void Dispose() => server.Dispose();
    }
}