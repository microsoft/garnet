// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Net;
using System.Net.Sockets;

namespace Garnet.test.sentinel.Fixtures
{
    /// <summary>
    /// Allocates free TCP ports for spawned subprocesses. Mirrors the helper
    /// used in Garnet.test.TestUtils; duplicated here so this project doesn't
    /// need InternalsVisibleTo into Garnet.test.
    /// </summary>
    internal static class TestPorts
    {
        /// <summary>
        /// Returns an unused TCP port by briefly binding a TcpListener on port 0.
        /// There is a small race window between close and use; callers should
        /// hold the port through Process.Start, which we do inside ProcessWrapper.
        /// </summary>
        public static int AllocateFreePort()
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
    }
}
